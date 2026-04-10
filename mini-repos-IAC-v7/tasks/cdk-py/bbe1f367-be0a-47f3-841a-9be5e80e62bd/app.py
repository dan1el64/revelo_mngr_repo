#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigwv2,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_pipes as pipes,
    aws_rds as rds,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
)
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct


def get_aws_region() -> str:  # pragma: no cover
    return os.getenv("AWS_REGION", "us-east-1")


def get_aws_endpoint() -> str:
    return os.getenv("AWS_ENDPOINT", "")


def build_lambda_environment(extra: dict[str, str]) -> dict[str, str]:  # pragma: no cover
    environment = dict(extra)
    aws_endpoint = get_aws_endpoint()
    if aws_endpoint:
        environment["AWS_ENDPOINT"] = aws_endpoint
    environment.update(extra)
    return environment


def generated_name(scope: Construct, suffix: str, max_length: int = 64) -> str:
    name = f"{cdk.Names.unique_id(scope)}-{suffix}".lower()
    return name[:max_length]


def ingest_handler_code() -> str:
    return """
import json
import logging
import os

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT") or None
QUEUE_URL = os.environ["QUEUE_URL"]
METRIC_NAMESPACE = "SecurityBaseline/Ingest"
METRIC_NAME = "AcceptedRequests"


def _client(service_name):
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        endpoint_url=AWS_ENDPOINT,
    )


def _emit_metric():
    _client("cloudwatch").put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": METRIC_NAME,
                "Value": 1,
                "Unit": "Count",
            }
        ],
    )


def _extract_payload(event):
    if "requestContext" in event:
        body = event.get("body") or "{}"
        return json.loads(body) if isinstance(body, str) else body

    if "body" in event:
        body = event["body"]
        return json.loads(body) if isinstance(body, str) else body

    records = event.get("Records") or []
    if records:
        body = records[0].get("body") or "{}"
        return json.loads(body) if isinstance(body, str) else body

    return event


def handler(event, _context):
    payload = _extract_payload(event)
    LOGGER.info("ingest event received")
    _emit_metric()

    if "requestContext" in event:
        _client("sqs").send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(payload),
        )
        return {
            "statusCode": 202,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "accepted"}),
        }

    return {
        "source": "pipe-enrichment",
        "payload": payload,
    }
"""


def workflow_worker_code() -> str:
    return """
import json
import logging
import os
import socket

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT") or None
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = 5432
METRIC_NAMESPACE = "SecurityBaseline/Workflow"
METRIC_NAME = "TasksProcessed"


def _client(service_name):
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        endpoint_url=AWS_ENDPOINT,
    )


def _emit_metric():
    _client("cloudwatch").put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": METRIC_NAME,
                "Value": 1,
                "Unit": "Count",
            }
        ],
    )


def handler(event, _context):
    LOGGER.info("workflow task received")
    secret_value = _client("secretsmanager").get_secret_value(SecretId=DB_SECRET_ARN)
    credentials = json.loads(secret_value["SecretString"])

    try:
        with socket.create_connection((DB_HOST, DB_PORT), timeout=1):
            LOGGER.info("database port reachable")
    except OSError as exc:
        LOGGER.warning("database connectivity check did not complete: %s", exc)

    _emit_metric()
    return {
        "status": "processed",
        "username": credentials["username"],
        "input": event,
    }
"""


def add_lambda_vpc_permissions(role: iam.Role) -> None:
    role.add_to_policy(
        iam.PolicyStatement(
            sid="VpcNetworking",
            actions=[
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcs",
                "ec2:AssignPrivateIpAddresses",
                "ec2:UnassignPrivateIpAddresses",
            ],
            resources=["*"],
        )
    )


def add_log_write_permissions(role: iam.Role, log_group: logs.LogGroup, sid: str) -> None:
    role.add_to_policy(
        iam.PolicyStatement(
            sid=sid,
            actions=[
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            resources=[
                log_group.log_group_arn,
                f"{log_group.log_group_arn}:*",
            ],
        )
    )


class SecurityBaselineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_vpc_subnets = ec2.SubnetSelection(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        )

        vpc = ec2.Vpc(
            self,
            "SecurityBaselineVpc",
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
            self,
            "LambdaSecurityGroup",
            vpc=vpc,
            allow_all_outbound=False,
            description="Security group for Lambda Runspaces",
        )

        stateful_store_security_group = ec2.SecurityGroup(
            self,
            "StatefulStoreSecurityGroup",
            vpc=vpc,
            description="Security group for the Stateful Store",
        )

        lambda_security_group.add_egress_rule(
            stateful_store_security_group,
            ec2.Port.tcp(5432),
            "Allow Postgres traffic to the database security group",
        )
        lambda_security_group.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "Allow HTTPS egress for AWS API access",
        )
        stateful_store_security_group.add_ingress_rule(
            lambda_security_group,
            ec2.Port.tcp(5432),
            "Allow Postgres only from Lambda Runspaces",
        )

        ingest_queue = sqs.Queue(
            self,
            "IngestQueue",
            visibility_timeout=Duration.seconds(30),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        database_secret = secretsmanager.Secret(
            self,
            "DatabaseSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"app_user"}',
                generate_string_key="password",
                exclude_characters='"/@\\',
                password_length=24,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        database_subnet_group = rds.CfnDBSubnetGroup(
            self,
            "StatefulStoreSubnetGroup",
            db_subnet_group_description="Private subnets for the stateful store",
            subnet_ids=vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnet_ids,
        )

        database = rds.CfnDBInstance(
            self,
            "StatefulStore",
            engine="postgres",
            db_instance_class="db.t3.micro",
            allocated_storage="20",
            storage_encrypted=True,
            multi_az=False,
            publicly_accessible=False,
            deletion_protection=False,
            delete_automated_backups=True,
            backup_retention_period=0,
            db_subnet_group_name=database_subnet_group.ref,
            vpc_security_groups=[stateful_store_security_group.security_group_id],
            master_username=database_secret.secret_value_from_json(
                "username"
            ).unsafe_unwrap(),
            master_user_password=database_secret.secret_value_from_json(
                "password"
            ).unsafe_unwrap(),
        )
        database.apply_removal_policy(RemovalPolicy.DESTROY)

        crawler_bucket = s3.Bucket(
            self,
            "CrawlerBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        ingest_function_name = generated_name(self, "ingest-handler")
        workflow_function_name = generated_name(self, "workflow-worker")

        ingest_role = iam.Role(
            self,
            "IngestHandlerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        workflow_role = iam.Role(
            self,
            "WorkflowWorkerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        pipe_role = iam.Role(
            self,
            "EventBridgePipeRole",
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
        )
        state_machine_role = iam.Role(
            self,
            "WorkflowStateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        ingest_handler = lambda_.Function(
            self,
            "IngestHandler",
            function_name=ingest_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(ingest_handler_code()),
            memory_size=256,
            timeout=Duration.seconds(10),
            role=ingest_role,
            environment=build_lambda_environment(
                {
                    "QUEUE_URL": ingest_queue.queue_url,
                }
            ),
            vpc=vpc,
            vpc_subnets=lambda_vpc_subnets,
            security_groups=[lambda_security_group],
        )

        workflow_worker = lambda_.Function(
            self,
            "WorkflowWorker",
            function_name=workflow_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(workflow_worker_code()),
            memory_size=256,
            timeout=Duration.seconds(15),
            role=workflow_role,
            environment=build_lambda_environment(
                {
                    "DB_SECRET_ARN": database_secret.secret_arn,
                    "DB_HOST": database.attr_endpoint_address,
                }
            ),
            vpc=vpc,
            vpc_subnets=lambda_vpc_subnets,
            security_groups=[lambda_security_group],
        )

        ingest_log_group = logs.LogGroup(
            self,
            "IngestHandlerLogGroup",
            log_group_name=f"/aws/lambda/{ingest_function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )
        workflow_log_group = logs.LogGroup(
            self,
            "WorkflowWorkerLogGroup",
            log_group_name=f"/aws/lambda/{workflow_function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        add_lambda_vpc_permissions(ingest_role)
        add_lambda_vpc_permissions(workflow_role)
        add_log_write_permissions(ingest_role, ingest_log_group, "WriteIngestLogs")
        add_log_write_permissions(workflow_role, workflow_log_group, "WriteWorkflowLogs")

        ingest_role.add_to_policy(
            iam.PolicyStatement(
                sid="SendToIngressQueue",
                actions=["sqs:SendMessage"],
                resources=[ingest_queue.queue_arn],
            )
        )
        workflow_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadDatabaseSecret",
                actions=["secretsmanager:GetSecretValue"],
                resources=[database_secret.secret_arn],
            )
        )

        workflow_definition = sfn.Chain.start(
            sfn_tasks.LambdaInvoke(
                self,
                "RunWorkflowWorker",
                lambda_function=workflow_worker,
                payload_response_only=True,
            )
        ).next(sfn.Succeed(self, "WorkflowSucceeded"))

        workflow_state_machine = sfn.StateMachine(
            self,
            "WorkflowStateMachine",
            state_machine_type=sfn.StateMachineType.STANDARD,
            definition_body=sfn.DefinitionBody.from_chainable(workflow_definition),
            role=state_machine_role,
        )

        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                sid="InvokeWorkflowWorker",
                actions=["lambda:InvokeFunction"],
                resources=[workflow_worker.function_arn],
            )
        )

        pipe_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadIngressQueue",
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:GetQueueAttributes",
                ],
                resources=[ingest_queue.queue_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                sid="InvokeEnrichmentLambda",
                actions=["lambda:InvokeFunction"],
                resources=[ingest_handler.function_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                sid="StartWorkflowExecution",
                actions=["states:StartExecution"],
                resources=[workflow_state_machine.state_machine_arn],
            )
        )

        api = apigwv2.HttpApi(
            self,
            "IngressHttpApi",
            create_default_stage=False,
        )

        lambda_integration = apigwv2.CfnIntegration(
            self,
            "IngestLambdaIntegration",
            api_id=api.api_id,
            integration_type="AWS_PROXY",
            integration_method="POST",
            integration_uri=(
                f"arn:{cdk.Aws.PARTITION}:apigateway:{cdk.Aws.REGION}:"
                f"lambda:path/2015-03-31/functions/{ingest_handler.function_arn}/invocations"
            ),
            payload_format_version="2.0",
        )

        apigwv2.CfnRoute(
            self,
            "IngestRoute",
            api_id=api.api_id,
            route_key="POST /ingest",
            target=f"integrations/{lambda_integration.ref}",
        )

        apigwv2.CfnStage(
            self,
            "IngressDefaultStage",
            api_id=api.api_id,
            stage_name="$default",
            auto_deploy=True,
            access_log_settings=apigwv2.CfnStage.AccessLogSettingsProperty(
                destination_arn=ingest_log_group.log_group_arn,
                format=(
                    '{"requestId":"$context.requestId",'
                    '"routeKey":"$context.routeKey",'
                    '"status":"$context.status",'
                    '"ip":"$context.identity.sourceIp"}'
                ),
            ),
        )

        ingest_handler.add_permission(
            "AllowApiGatewayInvoke",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            source_arn=Stack.of(self).format_arn(
                service="execute-api",
                resource=api.api_id,
                resource_name="*/POST/ingest",
                arn_format=cdk.ArnFormat.SLASH_RESOURCE_NAME,
            ),
        )

        pipes.CfnPipe(
            self,
            "IngestPipe",
            role_arn=pipe_role.role_arn,
            source=ingest_queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=1
                )
            ),
            enrichment=ingest_handler.function_arn,
            target=workflow_state_machine.state_machine_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                step_function_state_machine_parameters=(
                    pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                        invocation_type="FIRE_AND_FORGET"
                    )
                )
            ),
        )

        glue_database = glue.CfnDatabase(
            self,
            "CrawlerCatalogDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{cdk.Names.unique_id(self)}catalog"
            ),
        )

        crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadCrawlerBucket",
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                resources=[
                    crawler_bucket.bucket_arn,
                    crawler_bucket.arn_for_objects("*"),
                ],
            )
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        glue.CfnCrawler(
            self,
            "SecurityInventoryCrawler",
            role=crawler_role.role_arn,
            database_name=glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{crawler_bucket.bucket_name}"
                    )
                ]
            ),
        )


def build_app() -> cdk.App:  # pragma: no cover
    app = cdk.App()
    SecurityBaselineStack(
        app,
        "SecurityBaselineStack",
        env=cdk.Environment(region=get_aws_region()),
    )
    return app


def main() -> None:  # pragma: no cover
    app = build_app()
    app.synth()


if __name__ == "__main__":  # pragma: no cover
    main()
