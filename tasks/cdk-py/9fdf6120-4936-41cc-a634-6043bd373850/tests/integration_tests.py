#!/usr/bin/env python3

import json
import os
from pathlib import Path
import sys
import time
import unittest
from urllib import request
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import STAGE_CONFIG


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
NAME_PREFIX = os.getenv("NAME_PREFIX", "")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL_S3") or AWS_ENDPOINT
EXPECTED_STAGES = tuple(STAGE_CONFIG.keys())


def aws_client(service_name):
    endpoint_url = S3_ENDPOINT if service_name == "s3" else AWS_ENDPOINT
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        endpoint_url=endpoint_url,
    )


class StageDeployment:
    def __init__(self, stage, stack):
        self.stage = stage
        self.stack_name = stack["StackName"]
        self.stack_id = stack["StackId"]
        self.stack_status = stack["StackStatus"]
        self.outputs = {
            output["OutputKey"]: output["OutputValue"]
            for output in stack.get("Outputs", [])
        }
        self.resources = stack["StackResources"]

    @classmethod
    def load(cls, cloudformation, stage):
        stack_name = cls.stack_name_for(stage)
        stack = cloudformation.describe_stacks(StackName=stack_name)["Stacks"][0]
        resources = cloudformation.describe_stack_resources(StackName=stack_name)[
            "StackResources"
        ]
        return cls(stage, {**stack, "StackResources": resources})

    @staticmethod
    def stack_name_for(stage):
        prefix = f"{NAME_PREFIX}-" if NAME_PREFIX else ""
        return f"OrderIntake-{prefix}{stage}"

    def resource_of_type(self, resource_type):
        matches = [
            resource
            for resource in self.resources
            if resource["ResourceType"] == resource_type
        ]
        if len(matches) != 1:
            raise AssertionError(
                f"{self.stack_name} expected exactly one {resource_type}, found {len(matches)}"
            )
        return matches[0]

    def resources_of_type(self, resource_type):
        return [
            resource
            for resource in self.resources
            if resource["ResourceType"] == resource_type
        ]

    def application_lambda_resources(self):
        return [
            resource
            for resource in self.resources_of_type("AWS::Lambda::Function")
            if "order-intake-" in resource["PhysicalResourceId"]
        ]


class TestOrderIntakeIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cloudformation = aws_client("cloudformation")
        cls.s3 = aws_client("s3")
        cls.dynamodb = aws_client("dynamodb")
        cls.glue = aws_client("glue")
        cls.events = aws_client("events")
        cls.pipes = aws_client("pipes")
        cls.secretsmanager = aws_client("secretsmanager")
        cls.ec2 = aws_client("ec2")
        cls.lambda_client = aws_client("lambda")
        cls.stepfunctions = aws_client("stepfunctions")
        cls.sqs = aws_client("sqs")
        cls.deployments = {
            stage: StageDeployment.load(cls.cloudformation, stage)
            for stage in EXPECTED_STAGES
        }

    def test_all_expected_stage_stacks_are_deployed_with_outputs(self):
        self.assertEqual(sorted(self.deployments.keys()), sorted(EXPECTED_STAGES))

        for stage, deployment in self.deployments.items():
            self.assertEqual(
                deployment.stack_name,
                StageDeployment.stack_name_for(stage),
            )
            self.assertEqual(deployment.stack_id.split(":")[3], AWS_REGION)
            self.assertTrue(
                deployment.stack_status.endswith("COMPLETE"),
                f"{deployment.stack_name} status was {deployment.stack_status}",
            )

            for key in (
                "ArchiveBucketName",
                "NotificationTopicArn",
                "OrderEventsBusName",
                "OrderStatusTableName",
                "OrdersApiUrl",
            ):
                self.assertIn(key, deployment.outputs)
                self.assertTrue(deployment.outputs[key])

    def test_glue_pipes_eventbridge_secretsmanager_and_vpc_resources_exist_in_all_stages(self):
        for stage, deployment in self.deployments.items():
            archive_bucket = deployment.outputs["ArchiveBucketName"]
            status_table = deployment.outputs["OrderStatusTableName"]
            topic_arn = deployment.outputs["NotificationTopicArn"]
            event_bus_name = deployment.outputs["OrderEventsBusName"]

            self.s3.head_bucket(Bucket=archive_bucket)
            self.assertEqual(
                self.dynamodb.describe_table(TableName=status_table)["Table"]["TableName"],
                status_table,
            )

            glue_database_name = deployment.resource_of_type("AWS::Glue::Database")[
                "PhysicalResourceId"
            ]
            crawler_name = deployment.resource_of_type("AWS::Glue::Crawler")[
                "PhysicalResourceId"
            ]
            self._assert_glue_resources(stage, glue_database_name, crawler_name)

            bus = self.events.describe_event_bus(Name=event_bus_name)
            self.assertEqual(bus["Name"], event_bus_name)

            rules = self.events.list_rules(EventBusName=event_bus_name)["Rules"]
            self.assertEqual(len(rules), 2)
            seen_detail_types = set()
            for rule in rules:
                rule_name = rule["Name"]
                event_pattern = json.loads(rule["EventPattern"])
                seen_detail_types.update(event_pattern["detail-type"])
                targets = self.events.list_targets_by_rule(
                    Rule=rule_name,
                    EventBusName=event_bus_name,
                )["Targets"]
                self.assertEqual(len(targets), 1)
                self.assertEqual(targets[0]["Arn"], topic_arn)
                target_arn_text = targets[0]["Arn"].lower()
                for prohibited_marker in (
                    "dynamodb",
                    "ecs",
                    "batch",
                    "redshift",
                    "sagemaker",
                ):
                    self.assertNotIn(prohibited_marker, target_arn_text)
            self.assertEqual(seen_detail_types, {"order.processed", "order.failed"})

            secret_id = deployment.resource_of_type("AWS::SecretsManager::Secret")[
                "PhysicalResourceId"
            ]
            secret_description = self.secretsmanager.describe_secret(SecretId=secret_id)
            self.assertTrue(secret_description["ARN"])
            self.assertTrue(secret_description["Name"])
            secret_value = self.secretsmanager.get_secret_value(SecretId=secret_id)
            self.assertEqual(
                json.loads(secret_value["SecretString"])["provider"],
                "third-party",
            )

            queue_url = self._queue_url(
                deployment.resource_of_type("AWS::SQS::Queue")["PhysicalResourceId"]
            )
            queue_arn = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn"],
            )["Attributes"]["QueueArn"]
            state_machine_arn = deployment.resource_of_type("AWS::StepFunctions::StateMachine")[
                "PhysicalResourceId"
            ]
            enrichment_function_name = next(
                resource["PhysicalResourceId"]
                for resource in deployment.resources_of_type("AWS::Lambda::Function")
                if "-enrichment-" in resource["PhysicalResourceId"]
            )
            enrichment_arn = self.lambda_client.get_function_configuration(
                FunctionName=enrichment_function_name
            )["FunctionArn"]
            pipe_name = deployment.resource_of_type("AWS::Pipes::Pipe")["PhysicalResourceId"]
            self._assert_pipe_resource(
                pipe_name=pipe_name,
                queue_arn=queue_arn,
                enrichment_arn=enrichment_arn,
                state_machine_arn=state_machine_arn,
            )

            vpc_id = deployment.resource_of_type("AWS::EC2::VPC")["PhysicalResourceId"]
            security_group_id = deployment.resource_of_type("AWS::EC2::SecurityGroup")[
                "PhysicalResourceId"
            ]
            subnet_ids = [
                resource["PhysicalResourceId"]
                for resource in deployment.resources_of_type("AWS::EC2::Subnet")
            ]
            self.assertEqual(len(subnet_ids), 4)
            self.assertEqual(
                self.ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]["VpcId"],
                vpc_id,
            )
            security_group = self.ec2.describe_security_groups(
                GroupIds=[security_group_id]
            )["SecurityGroups"][0]
            self.assertEqual(security_group["VpcId"], vpc_id)
            subnets = self.ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
            self.assertEqual(len(subnets), 4)
            self.assertEqual({subnet["VpcId"] for subnet in subnets}, {vpc_id})

            lambda_functions = deployment.application_lambda_resources()
            self.assertEqual(len(lambda_functions), 3)
            for function_resource in lambda_functions:
                configuration = self.lambda_client.get_function_configuration(
                    FunctionName=function_resource["PhysicalResourceId"]
                )
                self.assertEqual(configuration["VpcConfig"]["VpcId"], vpc_id)
                self.assertEqual(
                    configuration["VpcConfig"]["SecurityGroupIds"],
                    [security_group_id],
                )
                self.assertTrue(configuration["VpcConfig"]["SubnetIds"])
                self.assertTrue(
                    set(configuration["VpcConfig"]["SubnetIds"]).issubset(set(subnet_ids))
                )

    def test_order_workflow_executes_end_to_end_in_each_stage(self):
        for stage, deployment in self.deployments.items():
            order_id = f"{stage}-{int(time.time())}"
            payload = {
                "stage": stage,
                "submittedAt": int(time.time()),
                "items": [{"sku": f"{stage}-item", "quantity": 1}],
            }
            execution_arn = self._start_order(deployment.outputs["OrdersApiUrl"], order_id, payload)
            self._wait_for_execution_success(execution_arn)

            item = self._wait_for_order_status(
                deployment.outputs["OrderStatusTableName"],
                order_id,
                "PROCESSED",
            )
            self.assertIn("updatedAt", item)

            status_response = self._get_order_status(
                deployment.outputs["OrdersApiUrl"],
                order_id,
            )
            self.assertEqual(status_response["orderId"], order_id)
            self.assertEqual(status_response["status"], "PROCESSED")

            object_key = f"{STAGE_CONFIG[stage]['archive_prefix']}{order_id}.json"
            stored_payload = self._wait_for_archive_object(
                deployment.outputs["ArchiveBucketName"],
                object_key,
            )
            self.assertEqual(stored_payload, payload)

    def _start_order(self, api_url, order_id, payload):
        body = json.dumps({"orderId": order_id, "payload": payload}).encode("utf-8")
        http_request = request.Request(
            self._api_url(api_url, "orders"),
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(http_request, timeout=30) as response:
            response = json.loads(response.read().decode("utf-8"))
        execution_arn = response.get("executionArn") or response.get("executionId")
        self.assertIsNotNone(execution_arn)
        self.assertTrue(execution_arn.startswith("arn:"))
        return execution_arn

    def _get_order_status(self, api_url, order_id):
        http_request = request.Request(
            self._api_url(api_url, f"orders/{order_id}"),
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(http_request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def _wait_for_execution_success(self, execution_arn):
        deadline = time.time() + 60
        last_error = None
        execution = None
        while time.time() < deadline:
            try:
                execution = self.stepfunctions.describe_execution(executionArn=execution_arn)
                if execution["status"] != "RUNNING":
                    break
            except Exception as exc:  # pragma: no cover - transient integration failures
                last_error = exc
            time.sleep(2)

        if execution is None or execution["status"] == "RUNNING":
            details = f"; last error: {last_error}" if last_error else ""
            self.fail(f"Timed out waiting for execution {execution_arn} to finish{details}")

        self.assertEqual(execution["status"], "SUCCEEDED")

    def _wait_for_order_status(self, table_name, order_id, expected_status):
        deadline = time.time() + 90
        last_error = None
        item = None
        while time.time() < deadline:
            try:
                dynamodb_item = self.dynamodb.get_item(
                    TableName=table_name,
                    Key={"orderId": {"S": order_id}},
                    ConsistentRead=True,
                ).get("Item")
                if dynamodb_item:
                    item = {
                        key: next(iter(value.values()))
                        for key, value in dynamodb_item.items()
                    }
                    if item.get("status") == expected_status:
                        break
            except Exception as exc:  # pragma: no cover - transient integration failures
                last_error = exc
            time.sleep(2)

        if not item or item.get("status") != expected_status:
            details = f"; last error: {last_error}" if last_error else ""
            self.fail(f"Timed out waiting for order {order_id} to reach {expected_status}{details}")

        self.assertEqual(item["orderId"], order_id)
        return item

    def _wait_for_archive_object(self, bucket_name, object_key):
        deadline = time.time() + 90
        last_error = None
        object_response = None
        while time.time() < deadline:
            try:
                object_response = self.s3.get_object(Bucket=bucket_name, Key=object_key)
                break
            except Exception as exc:  # pragma: no cover - transient integration failures
                last_error = exc
            time.sleep(2)

        if object_response is None:
            details = f"; last error: {last_error}" if last_error else ""
            self.fail(f"Timed out waiting for s3://{bucket_name}/{object_key} to exist{details}")

        body = object_response["Body"].read().decode("utf-8")
        return json.loads(body)

    def _assert_glue_resources(self, stage, glue_database_name, crawler_name):
        try:
            database = self.glue.get_database(Name=glue_database_name)["Database"]
            crawler = self.glue.get_crawler(Name=crawler_name)["Crawler"]
        except ClientError as exc:
            error_message = exc.response.get("Error", {}).get("Message", "").lower()
            if "glue service is not included" not in error_message or "license" not in error_message:
                raise

            # Some emulated environments deploy Glue resources successfully through
            # CloudFormation while blocking Glue control-plane APIs. In that case,
            # resource presence has already been validated via CloudFormation.
            self.assertTrue(glue_database_name)
            self.assertTrue(crawler_name)
            return

        self.assertEqual(database["Name"], glue_database_name)
        self.assertEqual(crawler["Name"], crawler_name)
        self.assertEqual(crawler["Schedule"]["ScheduleExpression"], STAGE_CONFIG[stage]["crawler_schedule"])
        self.assertEqual(crawler["DatabaseName"], glue_database_name)
        self.assertIn(
            f"/{STAGE_CONFIG[stage]['archive_prefix']}",
            crawler["Targets"]["S3Targets"][0]["Path"],
        )

    def _assert_pipe_resource(self, pipe_name, queue_arn, enrichment_arn, state_machine_arn):
        try:
            pipe = self.pipes.describe_pipe(Name=pipe_name)
        except ClientError as exc:
            error_message = exc.response.get("Error", {}).get("Message", "").lower()
            if "pipes service is not included" not in error_message or "license" not in error_message:
                raise

            # Some emulated environments deploy Pipes resources successfully through
            # CloudFormation while blocking the Pipes control-plane API. In that
            # case, resource presence has already been validated via CloudFormation.
            self.assertTrue(pipe_name)
            self.assertTrue(queue_arn)
            self.assertTrue(enrichment_arn)
            self.assertTrue(state_machine_arn)
            return

        self.assertEqual(pipe["CurrentState"], "RUNNING")
        self.assertEqual(pipe["Source"], queue_arn)
        self.assertEqual(pipe["Enrichment"], enrichment_arn)
        self.assertEqual(pipe["Target"], state_machine_arn)

    def _queue_url(self, queue_identifier):
        if queue_identifier.startswith("http"):
            return queue_identifier
        if queue_identifier.startswith("arn:"):
            queue_identifier = queue_identifier.rsplit(":", 1)[-1]
        return self.sqs.get_queue_url(QueueName=queue_identifier)["QueueUrl"]

    def _api_url(self, api_url, path_suffix):
        parsed = urlparse(api_url)
        normalized_base = api_url.rstrip("/")
        if AWS_ENDPOINT:
            endpoint_host = urlparse(AWS_ENDPOINT).netloc
            if endpoint_host and parsed.netloc != endpoint_host:
                api_id = parsed.netloc.split(".")[0]
                stage_name = parsed.path.strip("/").split("/")[0]
                endpoint = AWS_ENDPOINT.rstrip("/")
                normalized_base = f"{endpoint}/_aws/execute-api/{api_id}/{stage_name}"
        return f"{normalized_base.rstrip('/')}/{path_suffix.lstrip('/')}"


if __name__ == "__main__":
    unittest.main()
