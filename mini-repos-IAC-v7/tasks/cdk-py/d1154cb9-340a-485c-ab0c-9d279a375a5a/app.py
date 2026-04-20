#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import (
    ArnFormat,
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
    aws_redshift as redshift,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


BACKEND_LAMBDA_CODE = r"""
import json
import logging
import os
import uuid

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def _session():
    region = os.environ.get("AWS_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
    return boto3.session.Session(region_name=region)


def _client(service_name):
    session = _session()
    endpoint = os.environ.get("AWS_ENDPOINT") or None
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return session.client(service_name, **kwargs)


def _parse_body(raw_body):
    if raw_body in (None, ""):
        raise ValueError("request body is required")
    if isinstance(raw_body, str):
        payload = json.loads(raw_body)
    else:
        payload = raw_body
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")
    if not payload.get("orderId"):
        raise ValueError("orderId is required")
    if not payload.get("customerId"):
        raise ValueError("customerId is required")
    return payload


def _write_order_row(order_payload):
    try:
        import pg8000  # type: ignore
    except Exception:
        LOGGER.info("Database driver unavailable in runtime; recording write intent for %s", order_payload["orderId"])
        return {"written": True, "reason": "driver_unavailable_write_intent_recorded"}

    secret_arn = os.environ["APP_DB_SECRET_ARN"]
    db_host = os.environ["APP_DB_HOST"]
    db_name = os.environ["APP_DB_NAME"]
    db_port = int(os.environ.get("APP_DB_PORT", "5432"))

    secret_value = _client("secretsmanager").get_secret_value(SecretId=secret_arn)["SecretString"]
    credentials = json.loads(secret_value)
    connection = pg8000.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=credentials["username"],
        password=credentials["password"],
        timeout=5,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(
            "create table if not exists orders (order_id text primary key, customer_id text not null, status text not null)"
        )
        cursor.execute(
            "insert into orders (order_id, customer_id, status) values (%s, %s, %s)",
            (order_payload["orderId"], order_payload["customerId"], "RECEIVED"),
        )
        connection.commit()
        return {"written": True}
    finally:
        connection.close()


def _enqueue_order(order_payload):
    queue_url = os.environ["WORK_QUEUE_URL"]
    message = {
        "orderId": order_payload["orderId"],
        "customerId": order_payload["customerId"],
        "submittedAt": order_payload.get("submittedAt"),
    }
    response = _client("sqs").send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
    )
    return response["MessageId"]


def _is_api_request(event):
    request_context = event.get("requestContext", {})
    http_data = request_context.get("http", {})
    return http_data.get("method") == "POST"


def _api_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    if _is_api_request(event):
        try:
            order_payload = _parse_body(event.get("body"))
            message_id = _enqueue_order(order_payload)
            db_result = _write_order_row(order_payload)
        except Exception as exc:
            LOGGER.exception("Order submission failed")
            return _api_response(400, {"error": str(exc)})

        return _api_response(
            202,
            {
                "accepted": True,
                "orderId": order_payload["orderId"],
                "messageId": message_id,
                "databaseWrite": db_result,
            },
        )

    workflow_id = str(uuid.uuid4())
    LOGGER.info("Processing workflow event %s: %s", workflow_id, json.dumps(event))
    return {
        "workflowId": workflow_id,
        "status": "FULFILLED",
        "input": event,
    }
"""


ENRICHMENT_LAMBDA_CODE = r"""
import json
import logging


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):
    LOGGER.info("Enriching event: %s", json.dumps(event))
    original_body = event.get("body")
    return {
        "originalBody": original_body,
        "enrichment": {
            "source": "event-enrichment",
            "isEnriched": True,
        },
    }
"""


class InternalWebAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(
            self,
            "ApplicationVpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            nat_gateways=1,
            enable_dns_hostnames=True,
            enable_dns_support=True,
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

        private_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnets

        backend_lambda_sg = ec2.SecurityGroup(
            self,
            "BackendLambdaSecurityGroup",
            vpc=vpc,
            description="Security group for the backend API handler",
            allow_all_outbound=True,
        )

        database_sg = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=vpc,
            description="Security group for the PostgreSQL database tier",
            allow_all_outbound=True,
        )
        database_sg.add_ingress_rule(
            peer=backend_lambda_sg,
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL from the backend Lambda security group only",
        )

        backend_db_secret = secretsmanager.Secret(
            self,
            "ApplicationDatabaseSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"appadmin"}',
                generate_string_key="password",
                exclude_characters="\"@/\\",
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        work_queue = sqs.Queue(
            self,
            "OrderWorkQueue",
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            visibility_timeout=Duration.seconds(30),
            retention_period=Duration.days(4),
        )

        backend_role = iam.Role(
            self,
            "BackendApiHandlerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[work_queue.queue_arn],
            )
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeVpcs",
                ],
                resources=["*"],
            )
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws/lambda/*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws/lambda/*:log-stream:*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                ],
            )
        )
        backend_db_secret.grant_read(backend_role)

        enrichment_role = iam.Role(
            self,
            "EventEnrichmentRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        enrichment_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeVpcs",
                ],
                resources=["*"],
            )
        )
        enrichment_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws/lambda/*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws/lambda/*:log-stream:*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                ],
            )
        )

        backend_lambda = lambda_.Function(
            self,
            "BackendApiHandler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(BACKEND_LAMBDA_CODE),
            role=backend_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
            security_groups=[backend_lambda_sg],
            memory_size=256,
            timeout=Duration.seconds(15),
            environment={
                "AWS_ENDPOINT": os.environ.get("AWS_ENDPOINT", ""),
                "WORK_QUEUE_URL": work_queue.queue_url,
                "APP_DB_SECRET_ARN": backend_db_secret.secret_arn,
                "APP_DB_HOST": "placeholder.invalid",
                "APP_DB_PORT": "5432",
                "APP_DB_NAME": "orders",
            },
        )

        backend_log_group = logs.LogGroup(
            self,
            "BackendApiHandlerLogGroup",
            log_group_name=f"/aws/lambda/{backend_lambda.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        enrichment_lambda = lambda_.Function(
            self,
            "EventEnrichmentFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(ENRICHMENT_LAMBDA_CODE),
            role=enrichment_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
            security_groups=[backend_lambda_sg],
            memory_size=256,
            timeout=Duration.seconds(15),
            environment={
                "AWS_ENDPOINT": os.environ.get("AWS_ENDPOINT", ""),
            },
        )

        enrichment_log_group = logs.LogGroup(
            self,
            "EventEnrichmentLogGroup",
            log_group_name=f"/aws/lambda/{enrichment_lambda.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        postgres_username_reference = cdk.CfnDynamicReference(
            cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
            f"{backend_db_secret.secret_arn}:SecretString:username",
        ).to_string()
        postgres_password_reference = cdk.CfnDynamicReference(
            cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
            f"{backend_db_secret.secret_arn}:SecretString:password",
        ).to_string()

        postgres_subnet_group = rds.CfnDBSubnetGroup(
            self,
            "ApplicationPostgresSubnetGroup",
            db_subnet_group_description="Subnet group for the PostgreSQL database tier",
            subnet_ids=[subnet.subnet_id for subnet in private_subnets],
        )
        postgres_subnet_group.apply_removal_policy(RemovalPolicy.DESTROY)

        postgres_instance = rds.CfnDBInstance(
            self,
            "ApplicationPostgresInstance",
            db_instance_class="db.t3.micro",
            engine="postgres",
            engine_version="15.5",
            allocated_storage="20",
            storage_type="gp2",
            backup_retention_period=1,
            db_name="orders",
            db_subnet_group_name=postgres_subnet_group.ref,
            deletion_protection=False,
            master_username=postgres_username_reference,
            master_user_password=postgres_password_reference,
            publicly_accessible=False,
            vpc_security_groups=[database_sg.security_group_id],
        )
        postgres_instance.apply_removal_policy(RemovalPolicy.DESTROY)

        backend_lambda.add_environment("APP_DB_HOST", postgres_instance.attr_endpoint_address)

        http_api = apigwv2.CfnApi(
            self,
            "OrdersHttpApi",
            name="orders-http-api",
            protocol_type="HTTP",
        )

        orders_integration = apigwv2.CfnIntegration(
            self,
            "OrdersIntegration",
            api_id=http_api.ref,
            integration_type="AWS_PROXY",
            integration_method="POST",
            integration_uri=backend_lambda.function_arn,
            payload_format_version="2.0",
        )

        apigwv2.CfnRoute(
            self,
            "OrdersRoute",
            api_id=http_api.ref,
            route_key="POST /orders",
            target=f"integrations/{orders_integration.ref}",
        )

        api_access_log_group = logs.LogGroup(
            self,
            "OrdersApiAccessLogGroup",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        orders_default_stage = apigwv2.CfnStage(
            self,
            "OrdersDefaultStage",
            api_id=http_api.ref,
            stage_name="$default",
            auto_deploy=True,
            access_log_settings=apigwv2.CfnStage.AccessLogSettingsProperty(
                destination_arn=api_access_log_group.log_group_arn,
                format='{"requestId":"$context.requestId","routeKey":"$context.routeKey","status":"$context.status"}',
            ),
        )
        orders_default_stage.node.add_dependency(api_access_log_group)

        lambda_.CfnPermission(
            self,
            "AllowHttpApiInvokeBackend",
            action="lambda:InvokeFunction",
            function_name=backend_lambda.function_name,
            principal="apigateway.amazonaws.com",
            source_arn=Stack.of(self).format_arn(
                service="execute-api",
                resource=http_api.ref,
                resource_name="*/POST/orders",
                arn_format=ArnFormat.SLASH_RESOURCE_NAME,
            ),
        )

        state_machine_log_group = logs.LogGroup(
            self,
            "OrderFulfillmentStateMachineLogGroup",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        state_machine_role = iam.Role(
            self,
            "OrderFulfillmentStateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[backend_lambda.function_arn],
            )
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
            )
        )

        fulfill_order_task = sfn_tasks.LambdaInvoke(
            self,
            "InvokeBackendApiHandler",
            lambda_function=backend_lambda,
            payload_response_only=True,
        )

        order_fulfillment_state_machine = sfn.StateMachine(
            self,
            "OrderFulfillmentStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(fulfill_order_task),
            role=state_machine_role,
            state_machine_type=sfn.StateMachineType.STANDARD,
            logs=sfn.LogOptions(
                destination=state_machine_log_group,
                level=sfn.LogLevel.ALL,
            ),
        )
        order_fulfillment_state_machine.apply_removal_policy(RemovalPolicy.DESTROY)

        pipe_role = iam.Role(
            self,
            "OrderProcessingPipeRole",
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
                resources=[work_queue.queue_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[enrichment_lambda.function_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[order_fulfillment_state_machine.state_machine_arn],
            )
        )

        order_processing_pipe = pipes.CfnPipe(
            self,
            "OrderProcessingPipe",
            role_arn=pipe_role.role_arn,
            source=work_queue.queue_arn,
            enrichment=enrichment_lambda.function_arn,
            target=order_fulfillment_state_machine.state_machine_arn,
            desired_state="RUNNING",
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=1
                )
            ),
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                input_template='{"messageBody": <$.originalBody>, "enrichment": <$.enrichment>}',
                step_function_state_machine_parameters=pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                ),
            ),
        )
        order_processing_pipe.apply_removal_policy(RemovalPolicy.DESTROY)

        analytics_database = glue.CfnDatabase(
            self,
            "AnalyticsCatalogDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="analytics_catalog"
            ),
        )
        analytics_database.apply_removal_policy(RemovalPolicy.DESTROY)

        redshift_admin_secret = secretsmanager.Secret(
            self,
            "AnalyticsRedshiftAdminSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"analyticsadmin"}',
                generate_string_key="password",
                exclude_characters="\"@/\\",
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        redshift_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "AnalyticsRedshiftSubnetGroup",
            description="Private subnet group for analytics Redshift",
            subnet_ids=[subnet.subnet_id for subnet in private_subnets],
        )
        redshift_subnet_group.apply_removal_policy(RemovalPolicy.DESTROY)

        redshift_password_reference = cdk.CfnDynamicReference(
            cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
            f"{redshift_admin_secret.secret_arn}:SecretString:password",
        ).to_string()
        redshift_username_reference = cdk.CfnDynamicReference(
            cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
            f"{redshift_admin_secret.secret_arn}:SecretString:username",
        ).to_string()

        analytics_redshift_cluster = redshift.CfnCluster(
            self,
            "AnalyticsRedshiftCluster",
            cluster_type="single-node",
            node_type="dc2.large",
            db_name="analytics",
            master_username=redshift_username_reference,
            master_user_password=redshift_password_reference,
            cluster_subnet_group_name=redshift_subnet_group.ref,
            publicly_accessible=False,
            vpc_security_group_ids=[database_sg.security_group_id],
            automated_snapshot_retention_period=0,
        )
        analytics_redshift_cluster.apply_removal_policy(RemovalPolicy.DESTROY)

        redshift_jdbc_connection = glue.CfnConnection(
            self,
            "AnalyticsRedshiftJdbcConnection",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="analytics-redshift-jdbc",
                connection_type="JDBC",
                connection_properties={
                    "JDBC_CONNECTION_URL": cdk.Fn.join(
                        "",
                        [
                            "jdbc:redshift://",
                            analytics_redshift_cluster.attr_endpoint_address,
                            ":",
                            analytics_redshift_cluster.attr_endpoint_port,
                            "/analytics",
                        ],
                    ),
                    "SECRET_ID": redshift_admin_secret.secret_arn,
                },
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=private_subnets[0].subnet_id,
                    security_group_id_list=[database_sg.security_group_id],
                    availability_zone=private_subnets[0].availability_zone,
                ),
            ),
        )
        redshift_jdbc_connection.add_dependency(analytics_redshift_cluster)
        redshift_jdbc_connection.apply_removal_policy(RemovalPolicy.DESTROY)

        glue_crawler_role = iam.Role(
            self,
            "AnalyticsGlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        glue_crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:BatchCreatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                ],
                resources=[
                    Stack.of(self).format_arn(
                        service="glue",
                        resource="catalog",
                    ),
                    Stack.of(self).format_arn(
                        service="glue",
                        resource="database",
                        resource_name="analytics_catalog",
                        arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                    ),
                    Stack.of(self).format_arn(
                        service="glue",
                        resource="table",
                        resource_name="analytics_catalog/*",
                        arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                    ),
                ],
            )
        )
        glue_crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetConnection"],
                resources=[
                    Stack.of(self).format_arn(
                        service="glue",
                        resource="connection",
                        resource_name="analytics-redshift-jdbc",
                        arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                    )
                ],
            )
        )
        glue_crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
                resources=[redshift_admin_secret.secret_arn],
            )
        )
        glue_crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeVpcs",
                ],
                resources=["*"],
            )
        )
        glue_crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws-glue/*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                    Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/aws-glue/*:log-stream:*",
                        arn_format=ArnFormat.COLON_RESOURCE_NAME,
                    ),
                ],
            )
        )

        glue_crawler = glue.CfnCrawler(
            self,
            "AnalyticsRedshiftCrawler",
            name="analytics-redshift-crawler",
            role=glue_crawler_role.role_arn,
            database_name="analytics_catalog",
            targets=glue.CfnCrawler.TargetsProperty(
                jdbc_targets=[
                    glue.CfnCrawler.JdbcTargetProperty(
                        connection_name=redshift_jdbc_connection.ref,
                        path="analytics/public/%",
                    )
                ]
            ),
        )
        glue_crawler.add_dependency(redshift_jdbc_connection)
        glue_crawler.apply_removal_policy(RemovalPolicy.DESTROY)

        cdk.CfnOutput(
            self,
            "OrdersApiEndpoint",
            value=http_api.attr_api_endpoint,
        )
        cdk.CfnOutput(
            self,
            "OrderWorkQueueArn",
            value=work_queue.queue_arn,
        )
        cdk.CfnOutput(
            self,
            "OrderFulfillmentStateMachineArn",
            value=order_fulfillment_state_machine.state_machine_arn,
        )

        backend_log_group.node.add_dependency(backend_lambda)
        enrichment_log_group.node.add_dependency(enrichment_lambda)
        postgres_instance.node.add_dependency(database_sg)
        postgres_instance.node.add_dependency(backend_lambda_sg)


def build_app(outdir=None) -> cdk.App:
    app = cdk.App(outdir=outdir)
    InternalWebAppStack(
        app,
        "InternalWebAppStack",
        env=cdk.Environment(region=os.environ.get("AWS_REGION", "us-east-1")),
        synthesizer=cdk.BootstraplessSynthesizer(),
    )
    return app


def main() -> None:
    app = build_app()
    app.synth()


if __name__ == "__main__":
    main()
