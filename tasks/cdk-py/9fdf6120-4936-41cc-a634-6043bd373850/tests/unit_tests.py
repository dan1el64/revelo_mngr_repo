#!/usr/bin/env python3

import inspect
import json
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import (
    STAGE_CONFIG,
    build_app,
    _lambda_code_enrichment,
    _lambda_code_get_status,
    _lambda_code_worker,
    create_stack,
)


TEST_CONNECTION_TARGET = "configured-target"


def synthesize_stack(
    stage="dev",
    connection_target=TEST_CONNECTION_TARGET,
    region=None,
    name_prefix="",
):
    context = {
        "stage": stage,
        "aws_endpoint": connection_target,
        "name_prefix": name_prefix,
    }
    if region is not None:
        context["aws_region"] = region

    with mock.patch.dict(os.environ, {}, clear=True):
        app = cdk.App(context=context)
        stack = create_stack(app)
    return stack, Template.from_stack(stack)


def synthesize_template(stage="dev", connection_target=TEST_CONNECTION_TARGET, region=None):
    return synthesize_stack(stage=stage, connection_target=connection_target, region=region)[1]


def application_lambda_resources(template):
    lambda_functions = template.find_resources("AWS::Lambda::Function")
    return [
        definition
        for definition in lambda_functions.values()
        if "Environment" in definition["Properties"]
    ]


class TestOrderIntakeUnit(unittest.TestCase):
    def test_app_is_self_contained_and_uses_only_prompt_configuration_inputs(self):
        create_stack_source = inspect.getsource(create_stack)
        build_app_source = inspect.getsource(build_app)
        combined_source = create_stack_source + build_app_source

        expected_inputs = {
            "stage",
            "aws_region",
            "aws_endpoint",
            "name_prefix",
            "STAGE",
            "AWS_REGION",
            "CDK_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT",
            "NAME_PREFIX",
        }
        for token in expected_inputs:
            self.assertIn(token, combined_source)

        unexpected_inputs = {
            "VPC_ID",
            "SUBNET_ID",
            "SECURITY_GROUP_ID",
            "TABLE_NAME",
            "BUCKET_NAME",
            "QUEUE_NAME",
            "TOPIC_NAME",
            "SECRET_NAME",
        }
        for token in unexpected_inputs:
            self.assertNotIn(token, combined_source)

    def test_build_app_synthesizes_all_stages_without_manual_preprovisioning_inputs(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            app = build_app()

        stack_ids = sorted(
            child.node.id
            for child in app.node.children
            if isinstance(child, cdk.Stack)
        )
        self.assertEqual(
            stack_ids,
            ["OrderIntake-dev", "OrderIntake-prod", "OrderIntake-test"],
        )

    def test_context_validation_and_default_region(self):
        with self.assertRaisesRegex(ValueError, "stage"):
            with mock.patch.dict(os.environ, {}, clear=True):
                create_stack(cdk.App(context={"aws_endpoint": TEST_CONNECTION_TARGET}))

        with self.assertRaisesRegex(ValueError, "stage"):
            with mock.patch.dict(os.environ, {}, clear=True):
                create_stack(
                    cdk.App(
                        context={
                            "stage": "qa",
                            "aws_endpoint": TEST_CONNECTION_TARGET,
                        }
                    )
                )

        with self.assertRaisesRegex(ValueError, "aws_endpoint"):
            with mock.patch.dict(os.environ, {}, clear=True):
                create_stack(cdk.App(context={"stage": "dev"}))

        stack, _ = synthesize_stack(stage="dev", region=None)
        self.assertEqual(stack.region, "us-east-1")

    def test_template_uses_requested_aws_region_without_hardcoded_region_values(self):
        stack, template = synthesize_stack(stage="dev", region="eu-west-1")
        self.assertEqual(stack.region, "eu-west-1")

        lambda_functions = application_lambda_resources(template)
        for definition in lambda_functions:
            environment = definition["Properties"]["Environment"]["Variables"]
            self.assertEqual(environment["APP_AWS_REGION"], "eu-west-1")

        template_text = json.dumps(template.to_json())
        self.assertNotIn("us-east-1", template_text)

    def test_stack_contains_exact_prompt_resource_counts(self):
        template = synthesize_template("dev")

        template.resource_count_is("AWS::S3::Bucket", 1)
        template.resource_count_is("AWS::Glue::Database", 1)
        template.resource_count_is("AWS::Glue::Crawler", 1)
        template.resource_count_is("AWS::Athena::WorkGroup", 1)
        template.resource_count_is("AWS::ApiGateway::RestApi", 1)
        self.assertEqual(len(application_lambda_resources(template)), 3)
        template.resource_count_is("AWS::SQS::Queue", 1)
        template.resource_count_is("AWS::SNS::Topic", 1)
        template.resource_count_is("AWS::DynamoDB::Table", 1)
        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
        template.resource_count_is("AWS::Events::EventBus", 1)
        template.resource_count_is("AWS::Events::Rule", 2)
        template.resource_count_is("AWS::Pipes::Pipe", 1)
        template.resource_count_is("AWS::SecretsManager::Secret", 1)
        template.resource_count_is("AWS::EC2::VPC", 1)
        template.resource_count_is("AWS::EC2::SecurityGroup", 1)
        template.resource_count_is("AWS::EC2::Subnet", 4)
        template.resource_count_is("AWS::EC2::NatGateway", 0)

    def test_stage_specific_schedule_and_prefixes_are_deterministic(self):
        for stage, config in STAGE_CONFIG.items():
            template = synthesize_template(stage)
            template.has_resource_properties(
                "AWS::Glue::Crawler",
                {
                    "Schedule": {"ScheduleExpression": config["crawler_schedule"]},
                },
            )
            template.has_resource_properties(
                "AWS::Athena::WorkGroup",
                {
                    "WorkGroupConfiguration": {
                        "EnforceWorkGroupConfiguration": True,
                    }
                },
            )

            crawler = next(iter(template.find_resources("AWS::Glue::Crawler").values()))
            self.assertEqual(crawler["Properties"]["Name"], f"orders-{stage}-crawler")
            crawler_path = crawler["Properties"]["Targets"]["S3Targets"][0]["Path"]
            self.assertIn(config["archive_prefix"], json.dumps(crawler_path))

            athena_workgroup = next(
                iter(template.find_resources("AWS::Athena::WorkGroup").values())
            )
            athena_path = athena_workgroup["Properties"]["WorkGroupConfiguration"][
                "ResultConfiguration"
            ]["OutputLocation"]
            self.assertIn(config["athena_prefix"], json.dumps(athena_path))

    def test_glue_crawler_targets_archive_prefix_and_writes_to_glue_database(self):
        template = synthesize_template("dev")

        crawler = next(iter(template.find_resources("AWS::Glue::Crawler").values()))
        self.assertEqual(crawler["Properties"]["Name"], "orders-dev-crawler")
        self.assertEqual(crawler["Properties"]["DatabaseName"], {"Ref": "GlueDatabase"})
        self.assertIn("archive/dev/", json.dumps(crawler["Properties"]["Targets"]["S3Targets"]))

    def test_athena_workgroup_is_explicitly_associated_with_aws_data_catalog(self):
        template = synthesize_template("dev")

        athena_workgroup = next(iter(template.find_resources("AWS::Athena::WorkGroup").values()))
        workgroup_configuration = athena_workgroup["Properties"]["WorkGroupConfiguration"]
        self.assertTrue(workgroup_configuration["EnforceWorkGroupConfiguration"])
        self.assertIn("AwsDataCatalog", workgroup_configuration["AdditionalConfiguration"])

    def test_api_routes_logging_and_lambda_runtime_settings(self):
        template = synthesize_template("dev")

        template.has_resource_properties(
            "AWS::ApiGateway::Stage",
            {"StageName": "v1", "AccessLogSetting": Match.any_value()},
        )
        template.resource_count_is("AWS::Logs::LogGroup", 5)

        methods = template.find_resources("AWS::ApiGateway::Method")
        http_methods = {
            props["Properties"]["HttpMethod"] for props in methods.values()
        }
        self.assertEqual(http_methods, {"GET", "POST"})

        lambda_functions = application_lambda_resources(template)
        self.assertEqual(len(lambda_functions), 3)
        for definition in lambda_functions:
            properties = definition["Properties"]
            self.assertEqual(properties["Runtime"], "python3.11")
            self.assertEqual(properties["MemorySize"], 256)
            self.assertEqual(properties["Timeout"], 10)
            self.assertIn("VpcConfig", properties)
            self.assertIn("SecurityGroupIds", properties["VpcConfig"])
            self.assertIn("SubnetIds", properties["VpcConfig"])

        log_groups = template.find_resources("AWS::Logs::LogGroup")
        lambda_log_groups = [
            resource
            for resource in log_groups.values()
            if "/aws/lambda/" in json.dumps(resource["Properties"].get("LogGroupName", ""))
        ]
        self.assertEqual(len(lambda_log_groups), 3)
        for log_group in lambda_log_groups:
            self.assertEqual(log_group["Properties"]["RetentionInDays"], 7)

        methods = template.find_resources("AWS::ApiGateway::Method")
        post_method = next(
            resource
            for resource in methods.values()
            if resource["Properties"]["HttpMethod"] == "POST"
        )
        get_method = next(
            resource
            for resource in methods.values()
            if resource["Properties"]["HttpMethod"] == "GET"
        )

        post_integration = json.dumps(post_method["Properties"]["Integration"])
        self.assertIn("states:action/StartExecution", post_integration)
        self.assertIn("stateMachineArn", post_integration)
        self.assertIn("executionId", post_integration)
        self.assertIn("executionArn", post_integration)
        self.assertIn('"application/json"', post_integration)
        self.assertIn('$input.path', post_integration)

        get_integration = json.dumps(get_method["Properties"]["Integration"])
        self.assertIn("lambda:path", get_integration)

    def test_api_gateway_excludes_custom_domains_usage_plans_and_api_keys(self):
        template = synthesize_template("dev")
        for resource_type in (
            "AWS::ApiGateway::DomainName",
            "AWS::ApiGateway::BasePathMapping",
            "AWS::ApiGateway::UsagePlan",
            "AWS::ApiGateway::UsagePlanKey",
            "AWS::ApiGateway::ApiKey",
        ):
            template.resource_count_is(resource_type, 0)

    def test_state_machine_and_security_controls_match_prompt(self):
        template = synthesize_template("dev")

        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                    ]
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "VersioningConfiguration": Match.absent(),
            },
        )
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [{"AttributeName": "orderId", "KeyType": "HASH"}],
                "PointInTimeRecoverySpecification": {
                    "PointInTimeRecoveryEnabled": False
                },
            },
        )
        template.has_resource_properties(
            "AWS::SQS::Queue",
            {
                "VisibilityTimeout": 30,
                "SqsManagedSseEnabled": True,
            },
        )
        template.has_resource_properties(
            "AWS::SNS::Topic",
            {
                "KmsMasterKeyId": Match.any_value(),
            },
        )

        state_machine = next(
            iter(template.find_resources("AWS::StepFunctions::StateMachine").values())
        )
        definition = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
        definition_text = "".join(part for part in definition if isinstance(part, str))

        self.assertIn("WriteReceivedStatus", definition_text)
        self.assertIn("SendToIntakeQueue", definition_text)
        self.assertIn("arn:aws:states:::aws-sdk:dynamodb:putItem", definition_text)
        self.assertIn("arn:aws:states:::sqs:sendMessage", definition_text)
        self.assertIn("orderId", definition_text)
        self.assertIn("payload", definition_text)

    def test_dynamodb_table_does_not_enable_streams(self):
        template = synthesize_template("dev")
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {"StreamSpecification": Match.absent()},
        )

    def test_aws_endpoint_input_is_propagated_to_custom_sdk_clients(self):
        template = synthesize_template("dev")

        lambda_functions = application_lambda_resources(template)
        for definition in lambda_functions:
            environment = definition["Properties"]["Environment"]["Variables"]
            self.assertEqual(environment["AWS_ENDPOINT_URL"], TEST_CONNECTION_TARGET)
            self.assertEqual(environment["APP_AWS_REGION"], "us-east-1")

        get_code = _lambda_code_get_status()
        worker_code = _lambda_code_worker()
        self.assertIn('endpoint_url = os.environ.get("AWS_ENDPOINT_URL")', get_code)
        self.assertIn('region_name=os.environ["APP_AWS_REGION"]', get_code.replace(" ", ""))
        self.assertIn('endpoint_url=os.environ.get("AWS_ENDPOINT_URL")', worker_code.replace(" ", ""))
        self.assertIn('region_name=os.environ["APP_AWS_REGION"]', worker_code.replace(" ", ""))

    def test_iam_policies_are_minimal_and_scoped_to_required_resources(self):
        template = synthesize_template("dev")
        policies = template.find_resources("AWS::IAM::Policy")

        api_policy = next(
            policy["Properties"]["PolicyDocument"]["Statement"]
            for logical_id, policy in policies.items()
            if "ApiStartExecutionRole" in logical_id
        )
        self.assertEqual(len(api_policy), 1)
        self.assertEqual(api_policy[0]["Action"], "states:StartExecution")
        self.assertIsInstance(api_policy[0]["Resource"], dict)

        state_machine_statements = [
            statement
            for logical_id, policy in policies.items()
            if "StateMachineRole" in logical_id
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        ]
        dynamodb_statement = next(
            statement
            for statement in state_machine_statements
            if statement["Action"] == ["dynamodb:PutItem", "dynamodb:UpdateItem"]
        )
        self.assertIsInstance(dynamodb_statement["Resource"], dict)
        sqs_statement = next(
            statement
            for statement in state_machine_statements
            if statement["Action"] == "sqs:SendMessage"
        )
        self.assertIsInstance(sqs_statement["Resource"], dict)
        lambda_statement = next(
            statement
            for statement in state_machine_statements
            if statement["Action"] == "lambda:InvokeFunction"
        )
        self.assertIsInstance(lambda_statement["Resource"], dict)

        worker_statements = [
            statement
            for logical_id, policy in policies.items()
            if "WorkerFunctionServiceRole" in logical_id
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        ]
        events_statement = next(
            statement
            for statement in worker_statements
            if statement["Action"] == "events:PutEvents"
        )
        self.assertIsInstance(events_statement["Resource"], dict)
        sns_statement = next(
            statement
            for statement in worker_statements
            if statement["Action"] == "sns:Publish"
        )
        self.assertIsInstance(sns_statement["Resource"], dict)
        secrets_statement = next(
            statement
            for statement in worker_statements
            if statement["Action"] == ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        )
        self.assertIsInstance(secrets_statement["Resource"], dict)
        self.assertNotEqual(secrets_statement["Resource"], "*")

    def test_enrichment_lambda_permissions_are_minimal_and_exclude_writes(self):
        template = synthesize_template("dev")
        policies = template.find_resources("AWS::IAM::Policy")

        enrichment_statements = [
            statement
            for logical_id, policy in policies.items()
            if "EnrichmentFunctionServiceRole" in logical_id
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        ]
        self.assertEqual(enrichment_statements, [])

        roles = template.find_resources("AWS::IAM::Role")
        enrichment_role = next(
            role["Properties"]
            for logical_id, role in roles.items()
            if "EnrichmentFunctionServiceRole" in logical_id
        )
        self.assertIn("ManagedPolicyArns", enrichment_role)
        self.assertEqual(len(enrichment_role["ManagedPolicyArns"]), 2)
        self.assertNotIn("Policies", enrichment_role)

        enrichment_code = _lambda_code_enrichment()
        self.assertIn("json.loads", enrichment_code)
        self.assertIn('"source": "pipe"', enrichment_code)

    def test_worker_publishes_sns_notification_on_success(self):
        worker_code = _lambda_code_worker()
        self.assertIn("sns.publish(", worker_code)
        self.assertIn("TopicArn=topic_arn", worker_code)
        self.assertIn('"status": "PROCESSED"', worker_code)

    def test_worker_updates_dynamodb_status_for_success_and_failure(self):
        worker_code = _lambda_code_worker()
        self.assertEqual(worker_code.count("table.update_item("), 2)
        self.assertIn('":status": "PROCESSED"', worker_code)
        self.assertIn('":status": "FAILED"', worker_code)
        self.assertIn("updatedAt", worker_code)
        self.assertIn('":error": str(exc)', worker_code)

    def test_worker_puts_processed_and_failed_events_with_required_detail_types(self):
        worker_code = _lambda_code_worker()
        self.assertEqual(worker_code.count("eventbridge.put_events("), 2)
        self.assertIn('"DetailType": "order.processed"', worker_code)
        self.assertIn('"DetailType": "order.failed"', worker_code)
        self.assertIn('"EventBusName": event_bus_name', worker_code)
        self.assertIn('"Source": "order-intake.worker"', worker_code)

    def test_eventbridge_rules_route_processed_and_failed_events_to_sns(self):
        template = synthesize_template("dev")
        rules = template.find_resources("AWS::Events::Rule")
        topic_logical_id = next(iter(template.find_resources("AWS::SNS::Topic").keys()))

        self.assertEqual(len(rules), 2)
        rule_bodies = [rule["Properties"] for rule in rules.values()]

        processed_rule = next(
            rule for rule in rule_bodies
            if rule["EventPattern"]["detail-type"] == ["order.processed"]
        )
        failed_rule = next(
            rule for rule in rule_bodies
            if rule["EventPattern"]["detail-type"] == ["order.failed"]
        )

        for rule in (processed_rule, failed_rule):
            self.assertEqual(len(rule["Targets"]), 1)
            self.assertEqual(rule["Targets"][0]["Arn"], {"Ref": topic_logical_id})

    def test_eventbridge_rules_do_not_target_prohibited_services(self):
        template = synthesize_template("dev")
        rules = template.find_resources("AWS::Events::Rule")
        prohibited_markers = (
            "dynamodb",
            "ecs",
            "batch",
            "redshift",
            "sagemaker",
        )
        for rule in rules.values():
            target_arn_text = json.dumps(rule["Properties"]["Targets"][0]["Arn"]).lower()
            for marker in prohibited_markers:
                self.assertNotIn(marker, target_arn_text)

    def test_enrichment_lambda_transforms_pipe_body_to_order_payload(self):
        enrichment_code = _lambda_code_enrichment()
        self.assertIn('body = event.get("body")', enrichment_code)
        self.assertIn('body = json.loads(body)', enrichment_code)
        self.assertIn('"source": "pipe"', enrichment_code)
        self.assertIn('"orderId": body["orderId"]', enrichment_code)
        self.assertIn('"payload": body["payload"]', enrichment_code)

    def test_state_machine_writes_received_status_before_queueing_order(self):
        template = synthesize_template("dev")
        state_machine = next(
            iter(template.find_resources("AWS::StepFunctions::StateMachine").values())
        )
        definition = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
        definition_text = "".join(part for part in definition if isinstance(part, str))

        self.assertIn('"WriteReceivedStatus"', definition_text)
        self.assertIn('"Resource":"arn:aws:states:::aws-sdk:dynamodb:putItem"', definition_text.replace(" ", ""))
        self.assertIn('"status":{"S":"RECEIVED"}', definition_text.replace(" ", ""))
        self.assertIn('"ResultPath":null', definition_text.replace(" ", ""))
        self.assertIn('"Next":"SendToIntakeQueue"', definition_text.replace(" ", ""))

    def test_state_machine_passes_order_id_and_payload_through_workflow_steps(self):
        template = synthesize_template("dev")
        state_machine = next(
            iter(template.find_resources("AWS::StepFunctions::StateMachine").values())
        )
        definition = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
        definition_text = "".join(part for part in definition if isinstance(part, str)).replace(" ", "")

        self.assertIn('"orderId":{"S.$":"$.orderId"}', definition_text)
        self.assertIn('"orderId.$":"$.orderId"', definition_text)
        self.assertIn('"payload.$":"$.payload"', definition_text)

    def test_pipe_maps_sqs_body_into_state_machine_input(self):
        template = synthesize_template("dev")
        pipe = next(iter(template.find_resources("AWS::Pipes::Pipe").values()))
        properties = pipe["Properties"]

        self.assertIn("Enrichment", properties)
        self.assertEqual(
            properties["EnrichmentParameters"]["InputTemplate"],
            '{"body": <$.body>}',
        )
        target_parameters = properties["TargetParameters"]
        self.assertEqual(
            target_parameters["StepFunctionStateMachineParameters"]["InvocationType"],
            "FIRE_AND_FORGET",
        )

    def test_post_orders_starts_execution_and_returns_execution_identifier(self):
        template = synthesize_template("dev")
        post_method = next(
            resource["Properties"]
            for resource in template.find_resources("AWS::ApiGateway::Method").values()
            if resource["Properties"]["HttpMethod"] == "POST"
        )

        integration = post_method["Integration"]
        self.assertEqual(integration["IntegrationHttpMethod"], "POST")
        self.assertIn("states:action/StartExecution", json.dumps(integration["Uri"]))

        request_template = integration["RequestTemplates"]["application/json"]
        request_template_text = json.dumps(request_template)
        self.assertIn("stateMachineArn", request_template_text)
        self.assertIn("$util.escapeJavaScript($input.body)", request_template_text)
        self.assertNotIn("StartSyncExecution", request_template_text)

        policies = template.find_resources("AWS::IAM::Policy")
        api_statements = next(
            policy["Properties"]["PolicyDocument"]["Statement"]
            for logical_id, policy in policies.items()
            if "ApiStartExecutionRole" in logical_id
        )
        self.assertEqual(
            [statement["Action"] for statement in api_statements],
            ["states:StartExecution"],
        )

        state_machine_statements = [
            statement
            for logical_id, policy in policies.items()
            if "StateMachineRole" in logical_id
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        ]
        self.assertTrue(
            any(statement["Action"] == "sqs:SendMessage" for statement in state_machine_statements)
        )

        response_template = integration["IntegrationResponses"][0]["ResponseTemplates"]["application/json"]
        self.assertIn("executionArn", response_template)
        self.assertIn("executionId", response_template)

    def test_worker_secret_access_is_scoped_to_created_secret_only(self):
        template = synthesize_template("dev")
        policies = template.find_resources("AWS::IAM::Policy")
        secret_logical_id = next(iter(template.find_resources("AWS::SecretsManager::Secret").keys()))

        secrets_statement = next(
            statement
            for logical_id, policy in policies.items()
            if "WorkerFunctionServiceRole" in logical_id
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
            if statement["Action"] == ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        )
        self.assertEqual(secrets_statement["Resource"], {"Ref": secret_logical_id})

    def test_all_integrations_are_wired_with_cdk_references_instead_of_hard_coded_names(self):
        template = synthesize_template("dev")

        crawler = next(iter(template.find_resources("AWS::Glue::Crawler").values()))
        crawler_path = crawler["Properties"]["Targets"]["S3Targets"][0]["Path"]
        self.assertEqual(crawler["Properties"]["DatabaseName"], {"Ref": "GlueDatabase"})
        self.assertIn("Fn::Join", crawler_path)
        self.assertTrue(
            any(
                isinstance(part, dict) and "Ref" in part
                for part in crawler_path["Fn::Join"][1]
            )
        )

        athena_workgroup = next(iter(template.find_resources("AWS::Athena::WorkGroup").values()))
        output_location = athena_workgroup["Properties"]["WorkGroupConfiguration"][
            "ResultConfiguration"
        ]["OutputLocation"]
        self.assertIn("Fn::Join", output_location)
        self.assertTrue(
            any(
                isinstance(part, dict) and "Ref" in part
                for part in output_location["Fn::Join"][1]
            )
        )

        worker_function = next(
            definition
            for definition in application_lambda_resources(template)
            if definition["Properties"]["Environment"]["Variables"].get("ARCHIVE_BUCKET")
        )
        worker_variables = worker_function["Properties"]["Environment"]["Variables"]
        for variable_name in (
            "ARCHIVE_BUCKET",
            "EVENT_BUS_NAME",
            "SECRET_ARN",
            "TABLE_NAME",
            "TOPIC_ARN",
        ):
            self.assertIsInstance(worker_variables[variable_name], dict)

        rules = template.find_resources("AWS::Events::Rule")
        for rule in rules.values():
            properties = rule["Properties"]
            self.assertIsInstance(properties["EventBusName"], dict)
            self.assertIsInstance(properties["Targets"][0]["Arn"], dict)

        pipe = next(iter(template.find_resources("AWS::Pipes::Pipe").values()))
        pipe_properties = pipe["Properties"]
        self.assertIsInstance(pipe_properties["Source"], dict)
        self.assertIsInstance(pipe_properties["Enrichment"], dict)
        self.assertIsInstance(pipe_properties["Target"], dict)

        state_machine = next(
            iter(template.find_resources("AWS::StepFunctions::StateMachine").values())
        )
        definition_parts = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
        referenced_parts = [part for part in definition_parts if isinstance(part, dict)]
        self.assertGreaterEqual(len(referenced_parts), 3)
        for part in referenced_parts:
            self.assertIn("Ref", part)

        api_policy = next(
            policy["Properties"]["PolicyDocument"]["Statement"][0]
            for logical_id, policy in template.find_resources("AWS::IAM::Policy").items()
            if "ApiStartExecutionRole" in logical_id
        )
        self.assertIsInstance(api_policy["Resource"], dict)

    def test_resources_are_fully_destructible_and_network_is_restricted(self):
        _, template = synthesize_stack("dev")
        template_json = template.to_json()

        for logical_id, resource in template_json["Resources"].items():
            if "DeletionPolicy" in resource:
                self.assertNotEqual(
                    resource["DeletionPolicy"],
                    "Retain",
                    f"{logical_id} uses Retain deletion policy",
                )
            if "UpdateReplacePolicy" in resource:
                self.assertNotEqual(
                    resource["UpdateReplacePolicy"],
                    "Retain",
                    f"{logical_id} uses Retain update replace policy",
                )

        security_group_ingress = template.find_resources("AWS::EC2::SecurityGroupIngress")
        self.assertEqual(security_group_ingress, {})

        security_group = next(
            iter(template.find_resources("AWS::EC2::SecurityGroup").values())
        )
        self.assertEqual(security_group["Properties"]["SecurityGroupEgress"][0]["CidrIp"], "0.0.0.0/0")

    def test_catalog_and_lambda_code_contracts_match_prompt(self):
        template = synthesize_template("dev")

        crawler = next(iter(template.find_resources("AWS::Glue::Crawler").values()))
        self.assertIn("DatabaseName", crawler["Properties"])
        self.assertIn("archive/dev/", json.dumps(crawler["Properties"]["Targets"]))

        athena = next(iter(template.find_resources("AWS::Athena::WorkGroup").values()))
        athena_config = athena["Properties"]["WorkGroupConfiguration"]
        self.assertTrue(athena_config["EnforceWorkGroupConfiguration"])
        self.assertIn("AwsDataCatalog", athena_config["AdditionalConfiguration"])

        get_code = _lambda_code_get_status()
        self.assertIn('dynamodb.Table(table_name).get_item', get_code)
        self.assertIn('"orderId"', get_code)
        self.assertIn('"status"', get_code)

        worker_code = _lambda_code_worker()
        self.assertIn('s3.put_object', worker_code)
        self.assertIn('sns.publish', worker_code)
        self.assertIn('eventbridge.put_events', worker_code)
        self.assertIn('ARCHIVE_PREFIX', worker_code)


if __name__ == "__main__":
    unittest.main()
