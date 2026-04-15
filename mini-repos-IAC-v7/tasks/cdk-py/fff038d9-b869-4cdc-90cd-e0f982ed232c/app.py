#!/usr/bin/env python3
"""Single-file CDK app for the event-driven ingestion platform."""

import json
import os
from typing import Dict, Optional

_jsii_cache_dir = os.path.expanduser("~/Library/Caches")
if not os.access(_jsii_cache_dir, os.W_OK):
    os.environ["HOME"] = "/tmp"

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_apigateway as apigw,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_events as events,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_pipes as pipes,
    aws_rds as rds,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_stepfunctions as stepfunctions,
)

SOURCE_NAME = "app.ingestion"
DETAIL_TYPE = "ProcessingComplete"
STATUS_DB_NAME = "event_ingestion_status"
INGEST_FUNCTION_NAME = "event-ingestion-ingest"
WORKER_FUNCTION_NAME = "event-ingestion-worker"
ENRICHER_FUNCTION_NAME = "event-ingestion-enricher"


INGEST_LAMBDA_CODE = """
import json
import os
import time
import uuid

import boto3


def _client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.client(service_name, **kwargs)


def _resource(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.resource(service_name, **kwargs)


def handler(event, _context):
    body = event.get("body") or ""
    if len(body.encode("utf-8")) > 262144:
        return {
            "statusCode": 413,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "payload exceeds 256 KB"}),
        }

    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "body must be valid JSON"}),
        }

    request_id = str(uuid.uuid4())
    ttl = int(time.time()) + 86400

    _client("sqs").send_message(
        QueueUrl=os.environ["INGESTION_QUEUE_URL"],
        MessageBody=json.dumps({"requestId": request_id, "payload": payload}),
    )

    _resource("dynamodb").Table(os.environ["AUDIT_TABLE_NAME"]).put_item(
        Item={
            "pk": request_id,
            "ttl": ttl,
            "status": "RECEIVED",
        }
    )

    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"requestId": request_id}),
    }
"""


WORKER_LAMBDA_CODE = """
import json
import os
from decimal import Decimal
from datetime import datetime, timezone

import boto3


def _client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.client(service_name, **kwargs)


def _resource(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.resource(service_name, **kwargs)


def _json_default(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def handler(event, _context):
    dynamodb = _resource("dynamodb")
    s3_client = _client("s3")
    events_client = _client("events")
    audit_table = dynamodb.Table(os.environ["AUDIT_TABLE_NAME"])

    for record in event.get("Records", []):
        message = json.loads(record["body"])
        request_id = message["requestId"]
        payload = message["payload"]

        audit_item = audit_table.get_item(Key={"pk": request_id}).get("Item", {})
        processed_at = datetime.now(timezone.utc).isoformat()
        object_key = f"processed/{request_id}.json"
        processed_record = {
            "requestId": request_id,
            "payload": payload,
            "audit": audit_item,
            "processedAt": processed_at,
        }

        s3_client.put_object(
            Bucket=os.environ["PROCESSED_BUCKET_NAME"],
            Key=object_key,
            Body=json.dumps(processed_record, default=_json_default).encode("utf-8"),
            ContentType="application/json",
        )

        audit_table.update_item(
            Key={"pk": request_id},
            UpdateExpression="SET processedAt = :processed_at, objectKey = :object_key",
            ExpressionAttributeValues={
                ":processed_at": processed_at,
                ":object_key": object_key,
            },
        )

        events_client.put_events(
            Entries=[
                {
                    "EventBusName": os.environ["EVENT_BUS_NAME"],
                    "Source": os.environ["EVENT_SOURCE"],
                    "DetailType": os.environ["EVENT_DETAIL_TYPE"],
                    "Detail": json.dumps(
                        {
                            "requestId": request_id,
                            "bucket": os.environ["PROCESSED_BUCKET_NAME"],
                            "key": object_key,
                        }
                    ),
                }
            ]
        )

    return {"batchItemFailures": []}
"""


ENRICHER_LAMBDA_CODE = """
import json
from datetime import datetime, timezone


def _decode_body(record):
    body = record.get("body")
    if body is None:
        return record
    return json.loads(body)


def handler(event, _context):
    payload = event
    if isinstance(event, list) and event:
        payload = _decode_body(event[0])
    elif isinstance(event, dict) and "Records" in event and event["Records"]:
        payload = _decode_body(event["Records"][0])
    if isinstance(event, dict) and "body" in event:
        payload = _decode_body(event)

    if not isinstance(payload, dict):
        payload = {"payload": payload}

    detail = payload.get("detail") if isinstance(payload.get("detail"), dict) else payload
    detail["enriched"] = True
    detail["timestamp"] = datetime.now(timezone.utc).isoformat()
    payload["detail"] = detail
    return payload
"""


def load_config() -> Dict[str, Optional[str]]:
    """Read the allowed environment variables only."""
    return {
        "endpoint": os.getenv("AWS_ENDPOINT"),
        "region": os.getenv("AWS_REGION", "us-east-1"),
        "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }


def configure_environment(config: Dict[str, Optional[str]]) -> None:
    """Keep the process environment aligned with the allowed inputs."""
    os.environ["AWS_REGION"] = config["region"] or "us-east-1"
    if config["endpoint"]:
        os.environ["AWS_ENDPOINT_URL"] = config["endpoint"]
    if config["access_key_id"]:
        os.environ["AWS_ACCESS_KEY_ID"] = config["access_key_id"]
    if config["secret_access_key"]:
        os.environ["AWS_SECRET_ACCESS_KEY"] = config["secret_access_key"]


def _log_stream_arn(log_group: logs.LogGroup) -> str:
    return f"{log_group.log_group_arn}:*"


def build_stack(app: cdk.App, config: Dict[str, Optional[str]]) -> cdk.Stack:
    """Create the infrastructure stack."""
    stack = cdk.Stack(
        app,
        "EventDrivenIngestionStack",
        env=cdk.Environment(region=config["region"] or "us-east-1"),
    )

    vpc = ec2.Vpc(
        stack,
        "ApplicationVpc",
        max_azs=2,
        nat_gateways=1,
        subnet_configuration=[
            ec2.SubnetConfiguration(
                name="Public",
                subnet_type=ec2.SubnetType.PUBLIC,
                cidr_mask=24,
            ),
            ec2.SubnetConfiguration(
                name="Private",
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                cidr_mask=24,
            ),
        ],
    )

    lambda_security_group = ec2.SecurityGroup(
        stack,
        "LambdaRunspacesSecurityGroup",
        vpc=vpc,
        description="Security group for Lambda runspaces",
        allow_all_outbound=True,
    )

    database_security_group = ec2.SecurityGroup(
        stack,
        "DatabaseTierSecurityGroup",
        vpc=vpc,
        description="Security group for the database tier",
        allow_all_outbound=True,
    )
    database_security_group.add_ingress_rule(
        peer=lambda_security_group,
        connection=ec2.Port.tcp(5432),
        description="PostgreSQL from Lambda runspaces only",
    )

    ingestion_queue = sqs.Queue(
        stack,
        "IngestionQueue",
        visibility_timeout=Duration.seconds(60),
        retention_period=Duration.days(4),
    )

    notifications_queue = sqs.Queue(stack, "NotificationsQueue")

    audit_table = dynamodb.Table(
        stack,
        "AuditTable",
        partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
        billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        time_to_live_attribute="ttl",
        stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        removal_policy=RemovalPolicy.DESTROY,
    )

    status_table = dynamodb.Table(
        stack,
        "StatusTable",
        partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
        sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
        billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        removal_policy=RemovalPolicy.DESTROY,
    )

    processed_bucket = s3.Bucket(
        stack,
        "ProcessedBucket",
        versioned=True,
        lifecycle_rules=[
            s3.LifecycleRule(
                prefix="processed/",
                expiration=Duration.days(30),
            )
        ],
        removal_policy=RemovalPolicy.DESTROY,
    )

    application_bus = events.EventBus(
        stack,
        "ApplicationEventBus",
        event_bus_name="event-ingestion-bus",
    )

    notifications_topic = sns.Topic(
        stack,
        "NotificationsTopic",
        topic_name="event-ingestion-notifications",
    )

    notifications_queue_policy = sqs.QueuePolicy(
        stack,
        "NotificationsQueuePolicy",
        queues=[notifications_queue],
    )
    notifications_queue_policy.document.add_statements(
        iam.PolicyStatement(
            sid="AllowSnsFanout",
            effect=iam.Effect.ALLOW,
            principals=[iam.ServicePrincipal("sns.amazonaws.com")],
            actions=["sqs:SendMessage"],
            resources=[notifications_queue.queue_arn],
            conditions={"ArnEquals": {"aws:SourceArn": notifications_topic.topic_arn}},
        )
    )

    sns.CfnSubscription(
        stack,
        "NotificationsSubscription",
        protocol="sqs",
        topic_arn=notifications_topic.topic_arn,
        endpoint=notifications_queue.queue_arn,
    )

    ingest_log_group = logs.LogGroup(
        stack,
        "IngestLambdaLogGroup",
        log_group_name=f"/aws/lambda/{INGEST_FUNCTION_NAME}",
        retention=logs.RetentionDays.TWO_WEEKS,
        removal_policy=RemovalPolicy.DESTROY,
    )
    worker_log_group = logs.LogGroup(
        stack,
        "WorkerLambdaLogGroup",
        log_group_name=f"/aws/lambda/{WORKER_FUNCTION_NAME}",
        retention=logs.RetentionDays.TWO_WEEKS,
        removal_policy=RemovalPolicy.DESTROY,
    )
    enricher_log_group = logs.LogGroup(
        stack,
        "EnricherLambdaLogGroup",
        log_group_name=f"/aws/lambda/{ENRICHER_FUNCTION_NAME}",
        retention=logs.RetentionDays.TWO_WEEKS,
        removal_policy=RemovalPolicy.DESTROY,
    )

    common_vpc_permissions = iam.PolicyStatement(
        actions=[
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:AssignPrivateIpAddresses",
            "ec2:UnassignPrivateIpAddresses",
        ],
        resources=["*"],
    )

    ingest_role = iam.Role(
        stack,
        "IngestLambdaRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        inline_policies={
            "IngestPermissions": iam.PolicyDocument(
                statements=[
                    common_vpc_permissions,
                    iam.PolicyStatement(
                        actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                        resources=[_log_stream_arn(ingest_log_group)],
                    ),
                    iam.PolicyStatement(
                        actions=["sqs:SendMessage"],
                        resources=[ingestion_queue.queue_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["dynamodb:PutItem"],
                        resources=[audit_table.table_arn],
                    ),
                ]
            )
        },
    )

    worker_role = iam.Role(
        stack,
        "WorkerLambdaRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        inline_policies={
            "WorkerPermissions": iam.PolicyDocument(
                statements=[
                    common_vpc_permissions,
                    iam.PolicyStatement(
                        actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                        resources=[_log_stream_arn(worker_log_group)],
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ChangeMessageVisibility",
                        ],
                        resources=[ingestion_queue.queue_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["dynamodb:GetItem", "dynamodb:UpdateItem"],
                        resources=[audit_table.table_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["s3:PutObject"],
                        resources=[processed_bucket.arn_for_objects("processed/*")],
                    ),
                    iam.PolicyStatement(
                        actions=["events:PutEvents"],
                        resources=[application_bus.event_bus_arn],
                    ),
                ]
            )
        },
    )

    enricher_role = iam.Role(
        stack,
        "EnricherLambdaRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        inline_policies={
            "EnricherPermissions": iam.PolicyDocument(
                statements=[
                    common_vpc_permissions,
                    iam.PolicyStatement(
                        actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                        resources=[_log_stream_arn(enricher_log_group)],
                    ),
                ]
            )
        },
    )

    lambda_environment = {
        "AWS_ENDPOINT": config["endpoint"] or "",
        "AUDIT_TABLE_NAME": audit_table.table_name,
        "INGESTION_QUEUE_URL": ingestion_queue.queue_url,
        "PROCESSED_BUCKET_NAME": processed_bucket.bucket_name,
        "EVENT_BUS_NAME": application_bus.event_bus_name,
        "EVENT_SOURCE": SOURCE_NAME,
        "EVENT_DETAIL_TYPE": DETAIL_TYPE,
    }

    ingest_lambda = lambda_.Function(
        stack,
        "IngestLambda",
        function_name=INGEST_FUNCTION_NAME,
        runtime=lambda_.Runtime.PYTHON_3_11,
        architecture=lambda_.Architecture.X86_64,
        handler="index.handler",
        code=lambda_.Code.from_inline(INGEST_LAMBDA_CODE),
        timeout=Duration.seconds(30),
        memory_size=256,
        role=ingest_role,
        vpc=vpc,
        vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        security_groups=[lambda_security_group],
        environment=lambda_environment,
    )

    worker_lambda = lambda_.Function(
        stack,
        "WorkerLambda",
        function_name=WORKER_FUNCTION_NAME,
        runtime=lambda_.Runtime.PYTHON_3_11,
        architecture=lambda_.Architecture.X86_64,
        handler="index.handler",
        code=lambda_.Code.from_inline(WORKER_LAMBDA_CODE),
        timeout=Duration.seconds(30),
        memory_size=256,
        role=worker_role,
        vpc=vpc,
        vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        security_groups=[lambda_security_group],
        environment=lambda_environment,
    )

    enricher_lambda = lambda_.Function(
        stack,
        "EnricherLambda",
        function_name=ENRICHER_FUNCTION_NAME,
        runtime=lambda_.Runtime.PYTHON_3_11,
        architecture=lambda_.Architecture.X86_64,
        handler="index.handler",
        code=lambda_.Code.from_inline(ENRICHER_LAMBDA_CODE),
        timeout=Duration.seconds(30),
        memory_size=256,
        role=enricher_role,
        vpc=vpc,
        vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        security_groups=[lambda_security_group],
        environment={"AWS_ENDPOINT": config["endpoint"] or ""},
    )

    worker_event_source_mapping = lambda_.EventSourceMapping(
        stack,
        "WorkerQueueMapping",
        target=worker_lambda,
        event_source_arn=ingestion_queue.queue_arn,
        batch_size=10,
        max_batching_window=Duration.seconds(5),
        enabled=True,
    )
    worker_event_source_mapping.node.add_dependency(ingestion_queue)

    api = apigw.RestApi(
        stack,
        "IngestionApi",
        rest_api_name="event-ingestion-api",
        cloud_watch_role=False,
        endpoint_types=[apigw.EndpointType.REGIONAL],
        deploy_options=apigw.StageOptions(stage_name="v1"),
    )
    if api.node.try_find_child("Endpoint") is not None:
        api.node.try_remove_child("Endpoint")
    ingest_resource = api.root.add_resource("ingest")
    ingest_request_model = apigw.Model(
        stack,
        "IngestRequestModel",
        rest_api=api,
        content_type="application/json",
        schema=apigw.JsonSchema(
            schema=apigw.JsonSchemaVersion.DRAFT4,
            max_length=262144,
            one_of=[
                apigw.JsonSchema(
                    type=apigw.JsonSchemaType.OBJECT, additional_properties=True
                ),
                apigw.JsonSchema(type=apigw.JsonSchemaType.ARRAY),
                apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                apigw.JsonSchema(type=apigw.JsonSchemaType.NUMBER),
                apigw.JsonSchema(type=apigw.JsonSchemaType.INTEGER),
                apigw.JsonSchema(type=apigw.JsonSchemaType.BOOLEAN),
                apigw.JsonSchema(type=apigw.JsonSchemaType.NULL),
            ],
        ),
    )
    post_method = ingest_resource.add_method(
        "POST",
        apigw.LambdaIntegration(ingest_lambda, proxy=True),
        authorization_type=apigw.AuthorizationType.NONE,
        request_models={"application/json": ingest_request_model},
        request_validator_options=apigw.RequestValidatorOptions(
            validate_request_body=True
        ),
    )

    status_table_arn = status_table.table_arn
    state_machine_role = iam.Role(
        stack,
        "StateMachineRole",
        assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        inline_policies={
            "StateMachinePermissions": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["dynamodb:PutItem", "dynamodb:UpdateItem"],
                        resources=[status_table_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["sns:Publish"],
                        resources=[notifications_topic.topic_arn],
                    ),
                ]
            )
        },
    )

    definition = {
        "StartAt": "NormalizeInput",
        "States": {
            "NormalizeInput": {
                "Type": "Pass",
                "Parameters": {
                    "requestId.$": "$.detail.requestId",
                    "status": "PROCESSED",
                    "detail.$": "$.detail",
                },
                "Next": "WriteStatusRow",
            },
            "WriteStatusRow": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:putItem",
                "Parameters": {
                    "TableName": "${StatusTableName}",
                    "Item": {
                        "pk": {"S.$": "$.requestId"},
                        "sk": {"S": "workflow"},
                        "status": {"S.$": "$.status"},
                        "payload": {"S.$": "States.JsonToString($.detail)"},
                    },
                },
                "ResultPath": None,
                "Next": "PublishNotification",
            },
            "PublishNotification": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "${NotificationsTopicArn}",
                    "Message.$": "States.JsonToString($)",
                },
                "ResultPath": None,
                "End": True,
            },
        },
    }

    state_machine = stepfunctions.CfnStateMachine(
        stack,
        "ProcessingStateMachine",
        role_arn=state_machine_role.role_arn,
        state_machine_type="STANDARD",
        definition_string=json.dumps(definition),
        definition_substitutions={
            "StatusTableName": status_table.table_name,
            "NotificationsTopicArn": notifications_topic.topic_arn,
        },
    )

    event_rule_role = iam.Role(
        stack,
        "ApplicationEventRuleRole",
        assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        inline_policies={
            "ApplicationEventRulePermissions": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["states:StartExecution"],
                        resources=[state_machine.attr_arn],
                    )
                ]
            )
        },
    )

    processing_event_rule = events.CfnRule(
        stack,
        "ApplicationEventRule",
        event_bus_name=application_bus.event_bus_name,
        event_pattern={
            "source": [SOURCE_NAME],
            "detail-type": [DETAIL_TYPE],
        },
        targets=[
            events.CfnRule.TargetProperty(
                arn=state_machine.attr_arn,
                id="ProcessingStateMachineTarget",
                role_arn=event_rule_role.role_arn,
            )
        ],
    )

    pipe_role = iam.Role(
        stack,
        "ProcessingPipeRole",
        assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
        inline_policies={
            "PipePermissions": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ChangeMessageVisibility",
                        ],
                        resources=[ingestion_queue.queue_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["lambda:InvokeFunction"],
                        resources=[enricher_lambda.function_arn],
                    ),
                    iam.PolicyStatement(
                        actions=["states:StartExecution"],
                        resources=[state_machine.attr_arn],
                    ),
                ]
            )
        },
    )

    pipes.CfnPipe(
        stack,
        "ProcessingPipe",
        role_arn=pipe_role.role_arn,
        source=ingestion_queue.queue_arn,
        source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
            sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                batch_size=1
            )
        ),
        enrichment=enricher_lambda.function_arn,
        target=state_machine.attr_arn,
        target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
            step_function_state_machine_parameters=(
                pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                )
            )
        ),
    )

    database_credentials = secretsmanager.Secret(
        stack,
        "DatabaseCredentialsSecret",
        secret_name="event-ingestion-db-credentials",
        generate_secret_string=secretsmanager.SecretStringGenerator(
            secret_string_template=json.dumps({"username": "dbadmin"}),
            generate_string_key="password",
            exclude_punctuation=True,
            include_space=False,
        ),
    )
    database_credentials.apply_removal_policy(RemovalPolicy.DESTROY)

    database_subnet_group = rds.CfnDBSubnetGroup(
        stack,
        "ApplicationDatabaseSubnetGroup",
        db_subnet_group_description="Subnets for the application database",
        subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets],
    )

    database = rds.CfnDBInstance(
        stack,
        "ApplicationDatabase",
        db_instance_class="db.t3.micro",
        allocated_storage="20",
        db_subnet_group_name=database_subnet_group.ref,
        engine="postgres",
        engine_version="15.12",
        master_username=database_credentials.secret_value_from_json("username").unsafe_unwrap(),
        master_user_password=database_credentials.secret_value_from_json("password").unsafe_unwrap(),
        vpc_security_groups=[database_security_group.security_group_id],
        publicly_accessible=False,
        storage_type="gp2",
        deletion_protection=False,
        delete_automated_backups=True,
    )
    database.apply_removal_policy(RemovalPolicy.DESTROY)
    database.add_dependency(database_subnet_group)

    glue_database = glue.CfnDatabase(
        stack,
        "GlueDatabase",
        catalog_id=stack.account,
        database_input=glue.CfnDatabase.DatabaseInputProperty(name=STATUS_DB_NAME),
    )

    glue_role = iam.Role(
        stack,
        "GlueCrawlerRole",
        assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        inline_policies={
            "GlueCrawlerPermissions": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["s3:GetObject"],
                        resources=[processed_bucket.arn_for_objects("processed/*")],
                    ),
                    iam.PolicyStatement(
                        actions=["s3:ListBucket"],
                        resources=[processed_bucket.bucket_arn],
                        conditions={"StringLike": {"s3:prefix": ["processed/*"]}},
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "glue:GetDatabase",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:CreateTable",
                            "glue:UpdateTable",
                            "glue:BatchCreatePartition",
                            "glue:CreatePartition",
                            "glue:UpdatePartition",
                            "glue:GetPartitions",
                        ],
                        resources=[
                            stack.format_arn(service="glue", resource="catalog"),
                            stack.format_arn(
                                service="glue",
                                resource="database",
                                resource_name=STATUS_DB_NAME,
                            ),
                            stack.format_arn(
                                service="glue",
                                resource="table",
                                resource_name=f"{STATUS_DB_NAME}/*",
                            ),
                        ],
                    ),
                ]
            )
        },
    )

    crawler = glue.CfnCrawler(
        stack,
        "ProcessedRecordsCrawler",
        role=glue_role.role_arn,
        database_name=glue_database.ref,
        targets=glue.CfnCrawler.TargetsProperty(
            s3_targets=[
                glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{processed_bucket.bucket_name}/processed/"
                )
            ]
        ),
    )
    crawler.add_dependency(glue_database)

    cdk.CfnOutput(
        stack,
        "ApiInvokeUrl",
        value=cdk.Fn.join(
            "",
            [
                "https://",
                api.rest_api_id,
                ".execute-api.",
                stack.region,
                ".",
                stack.url_suffix,
                "/v1/ingest",
            ],
        ),
    )
    cdk.CfnOutput(stack, "IngestionQueueUrl", value=ingestion_queue.queue_url)
    cdk.CfnOutput(stack, "EventBusName", value=application_bus.event_bus_name)
    cdk.CfnOutput(stack, "ProcessedBucketName", value=processed_bucket.bucket_name)

    return stack


def main() -> cdk.App:
    """Entrypoint used by tests and synthesis."""
    config = load_config()
    configure_environment(config)
    app = cdk.App()
    build_stack(app, config)
    return app


if __name__ == "__main__":
    main().synth()
