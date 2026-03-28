#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    App,
    CfnOutput,
    CfnDeletionPolicy,
    Duration,
    Environment,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_athena as athena
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_pipes as pipes
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct


STAGE_CONFIG = {
    "dev": {
        "crawler_schedule": "cron(0/30 * * * ? *)",
        "archive_prefix": "archive/dev/",
        "athena_prefix": "athena/dev/",
    },
    "test": {
        "crawler_schedule": "cron(0 * * * ? *)",
        "archive_prefix": "archive/test/",
        "athena_prefix": "athena/test/",
    },
    "prod": {
        "crawler_schedule": "cron(0/15 * * * ? *)",
        "archive_prefix": "archive/prod/",
        "athena_prefix": "athena/prod/",
    },
}


def _context_or_env(app: App, context_key: str, *env_keys: str) -> str | None:
    value = app.node.try_get_context(context_key)
    if value:
        return value

    for env_key in env_keys:
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

    return None


def _asset_dir(name: str, code: str) -> str:
    digest = hashlib.sha256(code.encode("utf-8")).hexdigest()[:16]
    base_dir = Path("/tmp/order-intake-cdk-assets") / f"{name}-{digest}"
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "index.py").write_text(code, encoding="utf-8")
    return str(base_dir)


def _lambda_code_get_status() -> str:
    return """
import json
import os
import boto3


def lambda_handler(event, _context):
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    table_name = os.environ["TABLE_NAME"]
    order_id = event["pathParameters"]["orderId"]

    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.environ["APP_AWS_REGION"],
        endpoint_url=endpoint_url,
    )
    item = dynamodb.Table(table_name).get_item(Key={"orderId": order_id}).get("Item")

    if item is None:
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"orderId": order_id, "status": "NOT_FOUND"}),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {"orderId": item["orderId"], "status": item["status"]}
        ),
    }
"""


def _lambda_code_worker() -> str:
    return """
import json
import os
from datetime import datetime, timezone

import boto3


def _client(service_name):
    return boto3.client(
        service_name,
        region_name=os.environ["APP_AWS_REGION"],
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
    )


def _resource(service_name):
    return boto3.resource(
        service_name,
        region_name=os.environ["APP_AWS_REGION"],
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
    )


def _normalize_event(event):
    if "Records" in event and event["Records"]:
        body = event["Records"][0]["body"]
        if isinstance(body, str):
            body = json.loads(body)
        return {
            "orderId": body["orderId"],
            "payload": body["payload"],
        }
    return event


def lambda_handler(event, _context):
    event = _normalize_event(event)
    now = datetime.now(timezone.utc).isoformat()
    order_id = event["orderId"]
    payload = event["payload"]
    bucket = os.environ["ARCHIVE_BUCKET"]
    prefix = os.environ["ARCHIVE_PREFIX"]
    topic_arn = os.environ["TOPIC_ARN"]
    event_bus_name = os.environ["EVENT_BUS_NAME"]
    secret_arn = os.environ["SECRET_ARN"]

    secrets = _client("secretsmanager")
    secrets.get_secret_value(SecretId=secret_arn)

    table = _resource("dynamodb").Table(os.environ["TABLE_NAME"])
    s3 = _client("s3")
    sns = _client("sns")
    eventbridge = _client("events")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}{order_id}.json",
            Body=json.dumps(payload).encode("utf-8"),
            ServerSideEncryption="AES256",
        )
        table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #status = :status, updatedAt = :updated_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "PROCESSED",
                ":updated_at": now,
            },
        )
        sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps({"orderId": order_id, "status": "PROCESSED"}),
        )
        eventbridge.put_events(
            Entries=[
                {
                    "Source": "order-intake.worker",
                    "DetailType": "order.processed",
                    "Detail": json.dumps({"orderId": order_id, "status": "PROCESSED"}),
                    "EventBusName": event_bus_name,
                }
            ]
        )
        return {"orderId": order_id, "status": "PROCESSED"}
    except Exception as exc:
        table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #status = :status, updatedAt = :updated_at, error = :error",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "FAILED",
                ":updated_at": now,
                ":error": str(exc),
            },
        )
        eventbridge.put_events(
            Entries=[
                {
                    "Source": "order-intake.worker",
                    "DetailType": "order.failed",
                    "Detail": json.dumps({"orderId": order_id, "status": "FAILED"}),
                    "EventBusName": event_bus_name,
                }
            ]
        )
        raise
"""


def _lambda_code_enrichment() -> str:
    return """
import json


def lambda_handler(event, _context):
    body = event.get("body")
    if isinstance(body, str):
        body = json.loads(body)

    return {
        "source": "pipe",
        "orderId": body["orderId"],
        "payload": body["payload"],
    }
"""


class OrderIntakeStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        stage_name: str,
        aws_region: str,
        aws_endpoint: str,
        name_prefix: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=Environment(region=aws_region), **kwargs)

        config = STAGE_CONFIG[stage_name]
        archive_prefix = config["archive_prefix"]
        athena_prefix = config["athena_prefix"]
        resource_prefix = f"{name_prefix}-" if name_prefix else ""

        vpc = ec2.Vpc(
            self,
            "OrderVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        lambda_security_group = ec2.SecurityGroup(
            self,
            "LambdaSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Lambda egress-only security group",
        )

        archive_bucket = s3.Bucket(
            self,
            "ArchiveBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
        )

        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{resource_prefix}orders_{stage_name}",
            ),
        )
        glue_database.apply_removal_policy(RemovalPolicy.DESTROY)

        glue_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        archive_bucket.grant_read(glue_role, f"{archive_prefix}*")
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartitions",
                    "glue:BatchCreatePartition",
                    "glue:BatchUpdatePartition",
                ],
                resources=["*"],
            )
        )

        crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=f"{resource_prefix}orders-{stage_name}-crawler",
            role=glue_role.role_arn,
            database_name=glue_database.ref,
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression=config["crawler_schedule"]),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{archive_bucket.bucket_name}/{archive_prefix}",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING",
            ),
        )
        crawler.add_dependency(glue_database)
        crawler.cfn_options.deletion_policy = CfnDeletionPolicy.DELETE
        crawler.cfn_options.update_replace_policy = CfnDeletionPolicy.DELETE

        athena_workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkGroup",
            name=f"{resource_prefix}orders-{stage_name}",
            recursive_delete_option=True,
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                additional_configuration=json.dumps({"catalog": "AwsDataCatalog"}),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{archive_bucket.bucket_name}/{athena_prefix}",
                ),
            ),
        )
        athena_workgroup.cfn_options.deletion_policy = CfnDeletionPolicy.DELETE
        athena_workgroup.cfn_options.update_replace_policy = CfnDeletionPolicy.DELETE

        status_table = dynamodb.Table(
            self,
            "OrderStatusTable",
            partition_key=dynamodb.Attribute(
                name="orderId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=False
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        topic = sns.Topic(
            self,
            "NotificationTopic",
            master_key=kms.Alias.from_alias_name(self, "AwsManagedSnsKey", "alias/aws/sns"),
        )

        intake_queue = sqs.Queue(
            self,
            "OrderIntakeQueue",
            visibility_timeout=Duration.seconds(30),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        order_events_bus = events.EventBus(self, "OrderEventsBus")

        third_party_secret = secretsmanager.Secret(
            self,
            "ThirdPartyApiKeySecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"provider": "third-party"}),
                generate_string_key="apiKey",
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        lambda_common = {
            "runtime": lambda_.Runtime.PYTHON_3_11,
            "memory_size": 256,
            "timeout": Duration.seconds(10),
            "vpc": vpc,
            "vpc_subnets": ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            "security_groups": [lambda_security_group],
        }

        api_get_function = lambda_.Function(
            self,
            "ApiGetFunction",
            function_name=f"{resource_prefix}order-intake-get-{stage_name}",
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset(
                _asset_dir("api-get", _lambda_code_get_status())
            ),
            environment={
                "AWS_ENDPOINT_URL": aws_endpoint,
                "APP_AWS_REGION": aws_region,
                "TABLE_NAME": status_table.table_name,
            },
            **lambda_common,
        )

        worker_function = lambda_.Function(
            self,
            "WorkerFunction",
            function_name=f"{resource_prefix}order-intake-worker-{stage_name}",
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset(
                _asset_dir("worker", _lambda_code_worker())
            ),
            environment={
                "ARCHIVE_BUCKET": archive_bucket.bucket_name,
                "ARCHIVE_PREFIX": archive_prefix,
                "AWS_ENDPOINT_URL": aws_endpoint,
                "APP_AWS_REGION": aws_region,
                "EVENT_BUS_NAME": order_events_bus.event_bus_name,
                "SECRET_ARN": third_party_secret.secret_arn,
                "TABLE_NAME": status_table.table_name,
                "TOPIC_ARN": topic.topic_arn,
            },
            **lambda_common,
        )

        enrichment_function = lambda_.Function(
            self,
            "EnrichmentFunction",
            function_name=f"{resource_prefix}order-intake-enrichment-{stage_name}",
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset(
                _asset_dir("enrichment", _lambda_code_enrichment())
            ),
            environment={
                "AWS_ENDPOINT_URL": aws_endpoint,
                "APP_AWS_REGION": aws_region,
            },
            **lambda_common,
        )

        for logical_name, function in (
            ("ApiGetLogGroup", api_get_function),
            ("WorkerLogGroup", worker_function),
            ("EnrichmentLogGroup", enrichment_function),
        ):
            logs.LogGroup(
                self,
                logical_name,
                log_group_name=f"/aws/lambda/{function.function_name}",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            )

        status_table.grant_read_data(api_get_function)

        archive_bucket.grant_put(worker_function, f"{archive_prefix}*")
        status_table.grant(worker_function, "dynamodb:PutItem", "dynamodb:UpdateItem")
        order_events_bus.grant_put_events_to(worker_function)
        topic.grant_publish(worker_function)
        third_party_secret.grant_read(worker_function)
        intake_queue.grant_consume_messages(worker_function)
        worker_function.add_event_source(
            lambda_event_sources.SqsEventSource(
                intake_queue,
                batch_size=1,
            )
        )

        state_machine_role = iam.Role(
            self,
            "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:PutItem", "dynamodb:UpdateItem"],
                resources=[status_table.table_arn],
            )
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[intake_queue.queue_arn],
            )
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[worker_function.function_arn],
            )
        )

        state_machine_definition = {
            "StartAt": "RouteExecution",
            "States": {
                "RouteExecution": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.source",
                            "StringEquals": "pipe",
                            "Next": "ProcessQueuedOrder",
                        }
                    ],
                    "Default": "WriteReceivedStatus",
                },
                "WriteReceivedStatus": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
                    "Parameters": {
                        "TableName": status_table.table_name,
                        "Item": {
                            "orderId": {"S.$": "$.orderId"},
                            "status": {"S": "RECEIVED"},
                        },
                    },
                    "ResultPath": None,
                    "Next": "SendToIntakeQueue",
                },
                "SendToIntakeQueue": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": intake_queue.queue_url,
                        "MessageBody": {
                            "orderId.$": "$.orderId",
                            "payload.$": "$.payload",
                        },
                    },
                    "End": True,
                },
                "ProcessQueuedOrder": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": worker_function.function_name,
                        "Payload": {
                            "orderId.$": "$.orderId",
                            "payload.$": "$.payload",
                        },
                    },
                    "OutputPath": "$.Payload",
                    "End": True,
                },
            },
        }

        state_machine_log_group = logs.LogGroup(
            self,
            "StateMachineLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        state_machine = sfn.StateMachine(
            self,
            "OrderStateMachine",
            state_machine_type=sfn.StateMachineType.STANDARD,
            definition_body=sfn.DefinitionBody.from_string(
                json.dumps(state_machine_definition)
            ),
            role=state_machine_role,
            logs=sfn.LogOptions(
                destination=state_machine_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        api_integration_role = iam.Role(
            self,
            "ApiStartExecutionRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
        )
        api_integration_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        api_access_logs = logs.LogGroup(
            self,
            "ApiAccessLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        api = apigateway.RestApi(
            self,
            "OrdersApi",
            cloud_watch_role=False,
            deploy_options=apigateway.StageOptions(
                stage_name="v1",
                access_log_destination=apigateway.LogGroupLogDestination(api_access_logs),
                access_log_format=apigateway.AccessLogFormat.json_with_standard_fields(
                    caller=True,
                    http_method=True,
                    ip=True,
                    protocol=True,
                    request_time=True,
                    resource_path=True,
                    response_length=True,
                    status=True,
                    user=True,
                ),
            ),
        )

        orders_resource = api.root.add_resource("orders")
        order_id_resource = orders_resource.add_resource("{orderId}")

        start_execution_integration = apigateway.AwsIntegration(
            service="states",
            action="StartExecution",
            integration_http_method="POST",
            options=apigateway.IntegrationOptions(
                credentials_role=api_integration_role,
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_templates={
                    "application/json": json.dumps(
                        {
                            "stateMachineArn": state_machine.state_machine_arn,
                            "input": "$util.escapeJavaScript($input.body)",
                        }
                    ),
                },
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_templates={
                            "application/json": '{"executionArn": "$input.path(\'$.executionArn\')", "executionId": "$input.path(\'$.executionArn\')"}'
                        },
                    )
                ],
            ),
        )
        orders_resource.add_method(
            "POST",
            start_execution_integration,
            method_responses=[apigateway.MethodResponse(status_code="200")],
        )
        order_id_resource.add_method(
            "GET",
            apigateway.LambdaIntegration(api_get_function),
            method_responses=[apigateway.MethodResponse(status_code="200")],
        )

        processed_rule = events.Rule(
            self,
            "OrderProcessedRule",
            event_bus=order_events_bus,
            event_pattern=events.EventPattern(detail_type=["order.processed"]),
        )
        processed_rule.add_target(events_targets.SnsTopic(topic))

        failed_rule = events.Rule(
            self,
            "OrderFailedRule",
            event_bus=order_events_bus,
            event_pattern=events.EventPattern(detail_type=["order.failed"]),
        )
        failed_rule.add_target(events_targets.SnsTopic(topic))

        pipe_role = iam.Role(
            self,
            "PipeRole",
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:ChangeMessageVisibility",
                ],
                resources=[intake_queue.queue_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[enrichment_function.function_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        pipes.CfnPipe(
            self,
            "OrderPipe",
            role_arn=pipe_role.role_arn,
            source=intake_queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=1
                )
            ),
            enrichment=enrichment_function.function_arn,
            enrichment_parameters=pipes.CfnPipe.PipeEnrichmentParametersProperty(
                input_template='{"body": <$.body>}'
            ),
            target=state_machine.state_machine_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                step_function_state_machine_parameters=pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                )
            ),
        )

        CfnOutput(self, "OrdersApiUrl", value=api.url)
        CfnOutput(self, "ArchiveBucketName", value=archive_bucket.bucket_name)
        CfnOutput(self, "OrderStatusTableName", value=status_table.table_name)
        CfnOutput(self, "NotificationTopicArn", value=topic.topic_arn)
        CfnOutput(self, "OrderEventsBusName", value=order_events_bus.event_bus_name)


def create_stack(app: App) -> OrderIntakeStack:
    stage = _context_or_env(app, "stage", "STAGE")
    if stage not in STAGE_CONFIG:
        raise ValueError("Context 'stage' is required and must be one of: dev, test, prod")

    aws_region = _context_or_env(app, "aws_region", "AWS_REGION", "CDK_DEFAULT_REGION") or "us-east-1"
    aws_endpoint = _context_or_env(app, "aws_endpoint", "AWS_ENDPOINT_URL", "AWS_ENDPOINT")
    if not aws_endpoint:
        raise ValueError("Context 'aws_endpoint' is required")

    name_prefix = _context_or_env(app, "name_prefix", "NAME_PREFIX") or ""

    return OrderIntakeStack(
        app,
        f"OrderIntake-{name_prefix + '-' if name_prefix else ''}{stage}",
        stage_name=stage,
        aws_region=aws_region,
        aws_endpoint=aws_endpoint,
        name_prefix=name_prefix,
    )


def build_app() -> App:
    app = App()
    stage = _context_or_env(app, "stage", "STAGE")
    if stage in STAGE_CONFIG:
        create_stack(app)
        return app

    # Allow plain `cdk synth` / `cdklocal deploy` in harness flows by
    # synthesizing all deterministic stage variants when stage is not provided.
    aws_region = _context_or_env(app, "aws_region", "AWS_REGION", "CDK_DEFAULT_REGION") or "us-east-1"
    aws_endpoint = _context_or_env(
        app,
        "aws_endpoint",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT",
    ) or "https://endpoint.example.invalid"
    name_prefix = _context_or_env(app, "name_prefix", "NAME_PREFIX") or ""
    for stage_name in ("dev", "test", "prod"):
        OrderIntakeStack(
            app,
            f"OrderIntake-{name_prefix + '-' if name_prefix else ''}{stage_name}",
            stage_name=stage_name,
            aws_region=aws_region,
            aws_endpoint=aws_endpoint,
            name_prefix=name_prefix,
        )
    return app


if __name__ == "__main__":
    build_app().synth()
