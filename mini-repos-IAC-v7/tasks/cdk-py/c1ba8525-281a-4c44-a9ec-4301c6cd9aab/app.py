#!/usr/bin/env python3

import os
import textwrap
from typing import Any, Optional

import aws_cdk as cdk
from constructs import Construct

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigw,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_rds as rds,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
)


def _read_input(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _build_backend_handler_code(db_host: str) -> str:
    code = textwrap.dedent(
        """
        import json
        import os

        import boto3

        DB_HOST = "__DB_HOST__"


        def _response(status_code, payload):
            return {
                "statusCode": status_code,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload),
            }


        def _load_db_secret(secret_arn):
            client = boto3.client("secretsmanager")
            secret_value = client.get_secret_value(SecretId=secret_arn)
            return json.loads(secret_value["SecretString"])


        def _open_connection():
            try:
                import pg8000
            except ImportError as error:
                raise RuntimeError("pg8000 dependency is not available in the Lambda runtime") from error

            credentials = _load_db_secret(os.environ["DB_SECRET_ARN"])
            connection = pg8000.connect(
                host=DB_HOST,
                port=5432,
                database=os.environ["DB_NAME"],
                user=credentials["username"],
                password=credentials["password"],
                timeout=5,
            )
            cursor = connection.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "id SERIAL PRIMARY KEY, "
                "value TEXT NOT NULL)"
            )
            connection.commit()
            return connection, cursor


        def handler(event, context):
            method = event.get("httpMethod")
            path = event.get("path", "")

            if method == "GET" and path.endswith("/health"):
                return _response(200, {"ok": True})

            try:
                connection, cursor = _open_connection()
            except Exception as error:
                return _response(500, {"error": str(error)})

            try:
                if method == "GET" and path.endswith("/items"):
                    cursor.execute("SELECT id, value FROM items ORDER BY id LIMIT 20")
                    rows = cursor.fetchall()
                    items = [{"id": row[0], "value": row[1]} for row in rows]
                    return _response(200, items)

                if method == "POST" and path.endswith("/items"):
                    body = json.loads(event.get("body") or "{}")
                    value = body.get("value", "")
                    cursor.execute(
                        "INSERT INTO items (value) VALUES (%s) RETURNING id",
                        (value,),
                    )
                    new_id = cursor.fetchone()[0]
                    connection.commit()

                    boto3.client("events").put_events(
                        Entries=[
                            {
                                "Source": os.environ["EVENTS_SOURCE"],
                                "DetailType": os.environ["EVENTS_DETAIL_TYPE"],
                                "Detail": json.dumps({"id": new_id}),
                                "EventBusName": "default",
                            }
                        ]
                    )
                    return _response(201, {"id": new_id})

                return _response(404, {"error": "not_found"})
            finally:
                cursor.close()
                connection.close()
        """
    )
    return code.replace("__DB_HOST__", db_host)


def _build_event_processor_code() -> str:
    return textwrap.dedent(
        """
        import json


        def handler(event, context):
            for record in event.get("Records", []):
                print(
                    json.dumps(
                        {
                            "messageId": record.get("messageId"),
                            "body": record.get("body"),
                        }
                    )
                )
        """
    )


class PocStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        aws_region: str = "us-east-1",
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.20.0.0/16"),
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

        vpc.add_gateway_endpoint(
            "S3GatewayEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[
                ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            ],
        )

        frontend_alb_sg = ec2.SecurityGroup(
            self,
            "FrontendAlbSecurityGroup",
            vpc=vpc,
            description="Frontend ALB security group",
            allow_all_outbound=False,
        )
        frontend_alb_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(80),
            "Allow HTTP from the internet",
        )

        backend_sg = ec2.SecurityGroup(
            self,
            "BackendServiceSecurityGroup",
            vpc=vpc,
            description="Backend service security group",
            allow_all_outbound=False,
        )
        db_sg = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=vpc,
            description="Database security group",
        )

        frontend_alb_sg.add_egress_rule(
            backend_sg,
            ec2.Port.tcp(3000),
            "Allow frontend-to-backend traffic",
        )
        frontend_alb_sg.add_egress_rule(
            frontend_alb_sg,
            ec2.Port.tcp(80),
            "Allow ALB-to-frontend task traffic",
        )

        backend_sg.add_ingress_rule(
            frontend_alb_sg,
            ec2.Port.tcp(3000),
            "Allow traffic from the frontend tier",
        )
        backend_sg.add_egress_rule(
            db_sg,
            ec2.Port.tcp(5432),
            "Allow PostgreSQL access",
        )
        backend_sg.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "Allow AWS API calls",
        )

        db_sg.add_ingress_rule(
            backend_sg,
            ec2.Port.tcp(5432),
            "Allow PostgreSQL from the backend tier",
        )

        queue = sqs.Queue(
            self,
            "BackgroundWorkQueue",
            visibility_timeout=Duration.seconds(30),
            retention_period=Duration.days(4),
            removal_policy=RemovalPolicy.DESTROY,
        )

        db_secret = secretsmanager.Secret(
            self,
            "DbCredentialsSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"appuser"}',
                generate_string_key="password",
                exclude_punctuation=True,
            ),
        )
        db_secret.apply_removal_policy(RemovalPolicy.DESTROY)

        db_subnet_group = rds.CfnDBSubnetGroup(
            self,
            "ApplicationDatabaseSubnetGroup",
            db_subnet_group_description="Subnet group for ApplicationDatabase database",
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets],
        )
        db_subnet_group.apply_removal_policy(RemovalPolicy.DESTROY)

        database = rds.CfnDBInstance(
            self,
            "ApplicationDatabase",
            allocated_storage="20",
            backup_retention_period=0,
            copy_tags_to_snapshot=True,
            db_instance_class="db.t3.micro",
            db_name="appdb",
            db_subnet_group_name=db_subnet_group.ref,
            delete_automated_backups=True,
            deletion_protection=False,
            engine="postgres",
            engine_version="16",
            master_username=db_secret.secret_value_from_json("username").unsafe_unwrap(),
            master_user_password=db_secret.secret_value_from_json("password").unsafe_unwrap(),
            publicly_accessible=False,
            storage_type="gp2",
            vpc_security_groups=[db_sg.security_group_id],
        )
        database.apply_removal_policy(RemovalPolicy.DESTROY)

        backend_log_group = logs.LogGroup(
            self,
            "BackendApiLogGroup",
            log_group_name="/aws/lambda/backend-api-handler",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        event_processor_log_group = logs.LogGroup(
            self,
            "EventProcessorLogGroup",
            log_group_name="/aws/lambda/event-processor-handler",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        api_access_log_group = logs.LogGroup(
            self,
            "ApiAccessLogGroup",
            log_group_name="/aws/apigateway/poc-backend-access",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        frontend_log_group = logs.LogGroup(
            self,
            "FrontendLogGroup",
            log_group_name="/ecs/poc-frontend",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        backend_role = iam.Role(
            self,
            "BackendApiLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[db_secret.secret_arn],
            )
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=["events:PutEvents"],
                resources=[
                    f"arn:{Aws.PARTITION}:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:event-bus/default"
                ],
            )
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                ],
                resources=["*"],
            )
        )
        backend_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    backend_log_group.log_group_arn,
                    f"{backend_log_group.log_group_arn}:*",
                ],
            )
        )

        backend_lambda = lambda_.Function(
            self,
            "BackendApiHandler",
            function_name="backend-api-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.Code.from_inline(
                _build_backend_handler_code(database.attr_endpoint_address)
            ),
            handler="index.handler",
            role=backend_role,
            memory_size=1024,
            timeout=Duration.seconds(10),
            reserved_concurrent_executions=20,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[backend_sg],
            environment={
                "DB_SECRET_ARN": db_secret.secret_arn,
                "DB_NAME": "appdb",
                "EVENTS_SOURCE": "app.backend",
                "EVENTS_DETAIL_TYPE": "item.created",
                "QUEUE_URL": queue.queue_url,
            },
        )

        api = apigw.RestApi(
            self,
            "BackendApi",
            rest_api_name="poc-backend-api",
            endpoint_types=[apigw.EndpointType.REGIONAL],
            cloud_watch_role=True,
            cloud_watch_role_removal_policy=RemovalPolicy.DESTROY,
            deploy_options=apigw.StageOptions(
                stage_name="prod",
                logging_level=apigw.MethodLoggingLevel.INFO,
                access_log_destination=apigw.LogGroupLogDestination(
                    api_access_log_group
                ),
                access_log_format=apigw.AccessLogFormat.custom(
                    '{"requestId":"$context.requestId","status":"$context.status"}'
                ),
            ),
        )
        lambda_integration = apigw.LambdaIntegration(backend_lambda, proxy=True)
        health_resource = api.root.add_resource("health")
        health_resource.add_method("GET", lambda_integration)
        items_resource = api.root.add_resource("items")
        items_resource.add_method("GET", lambda_integration)
        items_resource.add_method("POST", lambda_integration)

        event_rule = events.Rule(
            self,
            "ItemCreatedRule",
            event_pattern=events.EventPattern(
                source=["app.backend"],
                detail_type=["item.created"],
            ),
        )
        event_rule.add_target(events_targets.SqsQueue(queue))

        event_processor_role = iam.Role(
            self,
            "EventProcessorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        event_processor_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                resources=[queue.queue_arn],
            )
        )
        event_processor_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    event_processor_log_group.log_group_arn,
                    f"{event_processor_log_group.log_group_arn}:*",
                ],
            )
        )
        event_processor_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                ],
                resources=["*"],
            )
        )

        event_processor = lambda_.Function(
            self,
            "EventProcessor",
            function_name="event-processor-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.Code.from_inline(_build_event_processor_code()),
            handler="index.handler",
            role=event_processor_role,
            memory_size=512,
            timeout=Duration.seconds(10),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[backend_sg],
        )

        lambda_.EventSourceMapping(
            self,
            "QueueEventSourceMapping",
            target=event_processor,
            event_source_arn=queue.queue_arn,
            batch_size=10,
        )

        cluster = ecs.Cluster(self, "FrontendCluster", vpc=vpc)

        task_definition = ecs.FargateTaskDefinition(
            self,
            "FrontendTaskDefinition",
            cpu=512,
            memory_limit_mib=1024,
        )
        container = task_definition.add_container(
            "FrontendContainer",
            image=ecs.ContainerImage.from_registry("public.ecr.aws/nginx/nginx:stable"),
            environment={"BACKEND_API_BASE_URL": api.url},
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="frontend", log_group=frontend_log_group
            ),
        )
        container.add_port_mappings(ecs.PortMapping(container_port=80))

        alb = elbv2.ApplicationLoadBalancer(
            self,
            "FrontendAlb",
            vpc=vpc,
            internet_facing=True,
            security_group=frontend_alb_sg,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )
        listener = alb.add_listener("HttpListener", port=80, open=False)
        target_group = elbv2.ApplicationTargetGroup(
            self,
            "FrontendTargetGroup",
            vpc=vpc,
            port=80,
            protocol=elbv2.ApplicationProtocol.HTTP,
            target_type=elbv2.TargetType.IP,
            health_check=elbv2.HealthCheck(
                path="/",
                healthy_http_codes="200-399",
            ),
        )
        listener.add_target_groups("FrontendTgAttachment", target_groups=[target_group])

        # Reuse the three required security groups without introducing a fourth task SG.
        frontend_service = ecs.FargateService(
            self,
            "FrontendService",
            cluster=cluster,
            task_definition=task_definition,
            desired_count=2,
            assign_public_ip=False,
            security_groups=[frontend_alb_sg],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
        )
        frontend_service.attach_to_application_target_group(target_group)

        analytics_bucket = s3.Bucket(
            self,
            "AnalyticsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
        )

        glue_database_name = "poc_analytics"
        glue_crawler_name = "poc-analytics-crawler"
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=glue_database_name
            ),
        )

        glue_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    analytics_bucket.bucket_arn,
                    analytics_bucket.arn_for_objects("*"),
                ],
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                    "glue:BatchUpdatePartition",
                    "glue:GetCrawler",
                    "glue:StartCrawler",
                    "glue:StopCrawler",
                    "glue:UpdateCrawler",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{glue_database_name}",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{glue_database_name}/*",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:crawler/{glue_crawler_name}",
                ],
            )
        )

        glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=glue_crawler_name,
            role=glue_role.role_arn,
            database_name=glue_database_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{analytics_bucket.bucket_name}/data/"
                    )
                ]
            ),
        ).add_dependency(glue_database)

        CfnOutput(
            self,
            "FrontendAlbDns",
            value=alb.load_balancer_dns_name,
            export_name="FrontendAlbDns",
        )


def main() -> None:
    aws_region = _read_input("aws_region", "us-east-1") or "us-east-1"

    app = cdk.App()
    PocStack(
        app,
        "PocStack",
        env=cdk.Environment(region=aws_region),
        aws_region=aws_region,
    )
    app.synth()


if __name__ == "__main__":
    main()
