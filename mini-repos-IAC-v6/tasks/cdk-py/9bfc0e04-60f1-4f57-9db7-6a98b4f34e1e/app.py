#!/usr/bin/env python3

import os
from textwrap import dedent

from aws_cdk import (
    App,
    Aws,
    CfnOutput,
    Duration,
    Environment,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigwv2,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_events as events,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_pipes as pipes,
    aws_rds as rds,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


BACKEND_PORT = 80
NAME_PREFIX = "infra-analysis"
INGEST_SOURCE = "infrastructure-analysis.ingest"
INGEST_DETAIL_TYPE = "IngestAccepted"


def configure_sdk_context(app: App) -> dict:
    aws_region = app.node.try_get_context("AWS_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
    aws_endpoint = app.node.try_get_context("AWS_ENDPOINT") or os.environ.get("AWS_ENDPOINT")
    aws_access_key_id = app.node.try_get_context("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = (
        app.node.try_get_context("AWS_SECRET_ACCESS_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    os.environ["AWS_REGION"] = aws_region
    os.environ["AWS_DEFAULT_REGION"] = aws_region
    if aws_access_key_id:
        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    if aws_secret_access_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    return {
        "AWS_REGION": aws_region,
        "AWS_ENDPOINT": aws_endpoint,
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
    }


def log_stream_resources(log_group: logs.LogGroup) -> list[str]:
    # CloudWatch Logs stream ARNs are only addressable as children of a log group ARN.
    return [log_group.log_group_arn, f"{log_group.log_group_arn}:*"]


class InfrastructureAnalysisStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, sdk_context: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_region = sdk_context["AWS_REGION"]
        aws_endpoint = sdk_context["AWS_ENDPOINT"] or ""

        vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.42.0.0/16"),
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

        alb_sg = ec2.SecurityGroup(
            self,
            "AlbSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Public ALB security group",
            security_group_name=f"{NAME_PREFIX}-alb-sg",
        )
        alb_sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(80), "Allow HTTP from the internet")

        backend_sg = ec2.SecurityGroup(
            self,
            "BackendSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Backend Processing Units security group",
            security_group_name=f"{NAME_PREFIX}-backend-sg",
        )
        backend_sg.add_ingress_rule(
            alb_sg,
            ec2.Port.tcp(BACKEND_PORT),
            "Allow backend traffic only from the ALB security group",
        )

        db_sg = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Storage Layer database security group",
            security_group_name=f"{NAME_PREFIX}-db-sg",
        )
        db_sg.add_ingress_rule(
            backend_sg,
            ec2.Port.tcp(5432),
            "Allow PostgreSQL only from the backend security group",
        )

        ecs_log_group = logs.LogGroup(
            self,
            "EcsLogGroup",
            log_group_name=f"/ecs/{NAME_PREFIX}-backend",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        ingest_log_group = logs.LogGroup(
            self,
            "IngestLogGroup",
            log_group_name=f"/aws/lambda/{NAME_PREFIX}-ingest",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        analyzer_log_group = logs.LogGroup(
            self,
            "AnalyzerLogGroup",
            log_group_name=f"/aws/lambda/{NAME_PREFIX}-analyzer",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        queue = sqs.Queue(
            self,
            "TelemetryQueue",
            queue_name=f"{NAME_PREFIX}-queue",
            visibility_timeout=Duration.seconds(30),
            removal_policy=RemovalPolicy.DESTROY,
        )

        table = dynamodb.Table(
            self,
            "TelemetryLedger",
            table_name=f"{NAME_PREFIX}-ledger",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        event_bus = events.EventBus(
            self,
            "CustomEventBus",
            event_bus_name=f"{NAME_PREFIX}-bus",
        )

        ingest_role = iam.Role(
            self,
            "IngestFunctionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"{NAME_PREFIX}-ingest-role",
        )
        ingest_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[queue.queue_arn],
            )
        )
        ingest_role.add_to_policy(
            iam.PolicyStatement(
                actions=["events:PutEvents"],
                resources=[event_bus.event_bus_arn],
            )
        )
        ingest_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=log_stream_resources(ingest_log_group),
            )
        )

        analyzer_role = iam.Role(
            self,
            "AnalyzerFunctionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"{NAME_PREFIX}-analyzer-role",
        )
        analyzer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:PutItem"],
                resources=[table.table_arn],
            )
        )
        analyzer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=log_stream_resources(analyzer_log_group),
            )
        )

        ingest_function = lambda_.Function(
            self,
            "IngestFunction",
            function_name=f"{NAME_PREFIX}-ingest",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            code=lambda_.Code.from_inline(
                dedent(
                    f"""
                    import json
                    import os
                    import boto3

                    REGION = os.environ["APP_REGION"]
                    ENDPOINT = os.environ.get("AWS_ENDPOINT") or None
                    QUEUE_URL = os.environ["QUEUE_URL"]
                    EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
                    SOURCE = os.environ["INGEST_SOURCE"]
                    DETAIL_TYPE = os.environ["INGEST_DETAIL_TYPE"]


                    def client(service_name):
                        kwargs = {{"region_name": REGION}}
                        if ENDPOINT:
                            kwargs["endpoint_url"] = ENDPOINT
                        return boto3.client(service_name, **kwargs)


                    def lambda_handler(event, context):
                        raw_body = event.get("body") or "{{}}"
                        body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
                        payload = body if isinstance(body, dict) else {{"value": body}}

                        detail = {{
                            "request_id": context.aws_request_id,
                            "payload": payload,
                        }}
                        put_events_response = client("events").put_events(
                            Entries=[
                                {{
                                    "Source": SOURCE,
                                    "DetailType": DETAIL_TYPE,
                                    "Detail": json.dumps(detail),
                                    "EventBusName": EVENT_BUS_NAME,
                                }}
                            ]
                        )
                        event_bridge_event_id = put_events_response["Entries"][0]["EventId"]

                        direct_message = {{
                            "message_kind": "direct-ingest",
                            "request_id": context.aws_request_id,
                            "event_bridge_event_id": event_bridge_event_id,
                            "payload": payload,
                        }}
                        client("sqs").send_message(
                            QueueUrl=QUEUE_URL,
                            MessageBody=json.dumps(direct_message),
                        )

                        print(
                            json.dumps(
                                {{
                                    "request_id": context.aws_request_id,
                                    "event_bridge_event_id": event_bridge_event_id,
                                    "status": "accepted",
                                }}
                            )
                        )

                        return {{
                            "statusCode": 200,
                            "headers": {{"content-type": "application/json"}},
                            "body": json.dumps(
                                {{
                                    "message": "accepted",
                                    "request_id": context.aws_request_id,
                                    "event_bridge_event_id": event_bridge_event_id,
                                }}
                            ),
                        }}
                    """
                )
            ),
            memory_size=256,
            timeout=Duration.seconds(10),
            role=ingest_role,
            environment={
                "APP_REGION": aws_region,
                "AWS_ENDPOINT": aws_endpoint,
                "QUEUE_URL": queue.queue_url,
                "EVENT_BUS_NAME": event_bus.event_bus_name,
                "INGEST_SOURCE": INGEST_SOURCE,
                "INGEST_DETAIL_TYPE": INGEST_DETAIL_TYPE,
            },
        )
        ingest_function.node.add_dependency(ingest_log_group)

        analyzer_function = lambda_.Function(
            self,
            "AnalyzerFunction",
            function_name=f"{NAME_PREFIX}-analyzer",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            code=lambda_.Code.from_inline(
                dedent(
                    """
                    import json
                    import os
                    import boto3

                    REGION = os.environ["APP_REGION"]
                    ENDPOINT = os.environ.get("AWS_ENDPOINT") or None
                    TABLE_NAME = os.environ["TABLE_NAME"]


                    def dynamodb_resource():
                        kwargs = {"region_name": REGION}
                        if ENDPOINT:
                            kwargs["endpoint_url"] = ENDPOINT
                        return boto3.resource("dynamodb", **kwargs)


                    def normalize_payload(event):
                        if isinstance(event, dict) and "message" in event and isinstance(event["message"], dict):
                            message = event["message"]
                            message.setdefault("receipt_timestamp", event.get("receipt_timestamp"))
                            return message, message.get("sqs_message_id")

                        if isinstance(event, dict) and "Records" in event:
                            record = event["Records"][0]
                            body = record.get("body") or "{}"
                            message = json.loads(body) if isinstance(body, str) else body
                            return message, record.get("messageId")

                        if isinstance(event, dict) and "body" in event:
                            body = event.get("body") or "{}"
                            message = json.loads(body) if isinstance(body, str) else body
                            return message, event.get("messageId") or event.get("message_id")

                        return event if isinstance(event, dict) else {"raw": str(event)}, None


                    def lambda_handler(event, _context):
                        message, sqs_message_id = normalize_payload(event)

                        detail = message.get("detail") if isinstance(message.get("detail"), dict) else {}
                        payload = message.get("payload")
                        if payload is None and detail:
                            payload = detail.get("payload")

                        event_bridge_event_id = (
                            message.get("event_bridge_event_id")
                            or message.get("id")
                            or detail.get("event_bridge_event_id")
                        )
                        if event_bridge_event_id is None:
                            event_bridge_event_id = "missing-event-bridge-id"

                        item_key = f"event#{event_bridge_event_id}"
                        item = {
                            "pk": item_key,
                            "event_bridge_event_id": event_bridge_event_id,
                            "sqs_message_id": sqs_message_id or "n/a",
                            "message_kind": message.get("message_kind", "unknown"),
                            "payload": json.dumps(payload if payload is not None else {}),
                        }

                        dynamodb_resource().Table(TABLE_NAME).put_item(Item=item)
                        print(
                            json.dumps(
                                {
                                    "sqs_message_id": sqs_message_id or "n/a",
                                    "event_bridge_event_id": event_bridge_event_id,
                                    "dynamodb_item_key": item_key,
                                }
                            )
                        )

                        return {
                            "message_kind": message.get("message_kind", "unknown"),
                            "event_bridge_event_id": event_bridge_event_id,
                            "sqs_message_id": sqs_message_id or "n/a",
                            "detail": detail,
                            "payload": payload if payload is not None else {},
                            "ddb_item_key": item_key,
                        }
                    """
                )
            ),
            memory_size=256,
            timeout=Duration.seconds(15),
            role=analyzer_role,
            environment={
                "APP_REGION": aws_region,
                "AWS_ENDPOINT": aws_endpoint,
                "TABLE_NAME": table.table_name,
            },
        )
        analyzer_function.node.add_dependency(analyzer_log_group)

        api = apigwv2.CfnApi(
            self,
            "HttpApi",
            name=f"{NAME_PREFIX}-http-api",
            protocol_type="HTTP",
        )
        api_integration = apigwv2.CfnIntegration(
            self,
            "IngestIntegration",
            api_id=api.ref,
            integration_type="AWS_PROXY",
            integration_method="POST",
            integration_uri=(
                f"arn:{Aws.PARTITION}:apigateway:{Aws.REGION}:lambda:path/2015-03-31/"
                f"functions/{ingest_function.function_arn}/invocations"
            ),
            payload_format_version="2.0",
        )
        api_route = apigwv2.CfnRoute(
            self,
            "IngestRoute",
            api_id=api.ref,
            route_key="POST /ingest",
            target=f"integrations/{api_integration.ref}",
        )
        api_stage = apigwv2.CfnStage(
            self,
            "DefaultStage",
            api_id=api.ref,
            stage_name="$default",
            auto_deploy=True,
        )
        api_stage.add_dependency(api_route)

        lambda_.CfnPermission(
            self,
            "AllowHttpApiInvokeIngest",
            action="lambda:InvokeFunction",
            function_name=ingest_function.function_name,
            principal="apigateway.amazonaws.com",
            source_arn=(
                f"arn:{Aws.PARTITION}:execute-api:{Aws.REGION}:{Aws.ACCOUNT_ID}:{api.ref}/*/POST/ingest"
            ),
        )

        rule = events.CfnRule(
            self,
            "IngestEventRule",
            name=f"{NAME_PREFIX}-ingest-rule",
            event_bus_name=event_bus.event_bus_name,
            event_pattern={
                "source": [INGEST_SOURCE],
                "detail-type": [INGEST_DETAIL_TYPE],
            },
            state="ENABLED",
            targets=[
                events.CfnRule.TargetProperty(
                    arn=queue.queue_arn,
                    id="Target0",
                    input_transformer=events.CfnRule.InputTransformerProperty(
                        input_paths_map={
                            "id": "$.id",
                            "source": "$.source",
                            "detail-type": "$.detail-type",
                            "detail": "$.detail",
                        },
                        input_template='{"message_kind":"rule-delivery","event_bridge_event_id":<id>,"source":<source>,"detail_type":<detail-type>,"detail":<detail>}',
                    ),
                )
            ],
        )
        queue.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal("events.amazonaws.com")],
                actions=["sqs:SendMessage"],
                resources=[queue.queue_arn],
                conditions={"ArnEquals": {"aws:SourceArn": rule.attr_arn}},
            )
        )

        lambda_.CfnEventSourceMapping(
            self,
            "AnalyzerSqsEventSource",
            function_name=analyzer_function.function_name,
            event_source_arn=queue.queue_arn,
            batch_size=1,
            enabled=True,
            filter_criteria=lambda_.CfnEventSourceMapping.FilterCriteriaProperty(
                filters=[
                    lambda_.CfnEventSourceMapping.FilterProperty(
                        pattern='{"body":{"message_kind":["direct-ingest"]}}'
                    )
                ]
            ),
        )

        state_machine_role = iam.Role(
            self,
            "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            role_name=f"{NAME_PREFIX}-sfn-role",
        )
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[analyzer_function.function_arn],
            )
        )

        state_machine_definition = sfn.Pass(
            self,
            "RecordReceiptTimestamp",
            parameters={
                "message.$": "$",
                "receipt_timestamp.$": "$$.State.EnteredTime",
            },
        ).next(
            sfn_tasks.LambdaInvoke(
                self,
                "InvokeAnalyzerFromStateMachine",
                lambda_function=analyzer_function,
                payload_response_only=True,
            )
        )

        state_machine = sfn.StateMachine(
            self,
            "StateMachine",
            state_machine_name=f"{NAME_PREFIX}-state-machine",
            state_machine_type=sfn.StateMachineType.STANDARD,
            definition_body=sfn.DefinitionBody.from_chainable(state_machine_definition),
            role=state_machine_role,
        )

        pipe_role = iam.Role(
            self,
            "PipeRole",
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
            role_name=f"{NAME_PREFIX}-pipe-role",
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:GetQueueAttributes",
                ],
                resources=[queue.queue_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[analyzer_function.function_arn],
            )
        )
        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        pipe = pipes.CfnPipe(
            self,
            "EventBridgePipe",
            name=f"{NAME_PREFIX}-pipe",
            role_arn=pipe_role.role_arn,
            source=queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                filter_criteria=pipes.CfnPipe.FilterCriteriaProperty(
                    filters=[
                        pipes.CfnPipe.FilterProperty(
                            pattern='{"body":{"message_kind":["rule-delivery"]}}'
                        )
                    ]
                ),
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=1
                ),
            ),
            enrichment=analyzer_function.function_arn,
            target=state_machine.state_machine_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                step_function_state_machine_parameters=pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                )
            ),
        )

        db_secret = secretsmanager.Secret(
            self,
            "DatabaseSecret",
            secret_name=f"{NAME_PREFIX}-db-secret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"postgres"}',
                generate_string_key="password",
                exclude_punctuation=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        db_subnet_group = rds.CfnDBSubnetGroup(
            self,
            "DatabaseSubnetGroup",
            db_subnet_group_description="Private subnets for the analysis database",
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets],
            db_subnet_group_name=f"{NAME_PREFIX}-db-subnet-group",
        )

        db_instance = rds.CfnDBInstance(
            self,
            "PostgresInstance",
            db_instance_identifier=f"{NAME_PREFIX}-db",
            engine="postgres",
            engine_version="16.3",
            db_instance_class="db.t3.micro",
            allocated_storage="20",
            publicly_accessible=False,
            multi_az=False,
            deletion_protection=False,
            delete_automated_backups=True,
            db_name="analysis",
            db_subnet_group_name=db_subnet_group.ref,
            vpc_security_groups=[db_sg.security_group_id],
            master_username=f"{{{{resolve:secretsmanager:{db_secret.secret_arn}:SecretString:username}}}}",
            master_user_password=f"{{{{resolve:secretsmanager:{db_secret.secret_arn}:SecretString:password}}}}",
        )
        db_instance.apply_removal_policy(RemovalPolicy.DESTROY)

        cluster = ecs.Cluster(
            self,
            "EcsCluster",
            vpc=vpc,
            cluster_name=f"{NAME_PREFIX}-cluster",
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "BackendTaskDefinition",
            family=f"{NAME_PREFIX}-task",
            cpu=256,
            memory_limit_mib=512,
        )
        task_definition.add_container(
            "BackendContainer",
            container_name="backend",
            image=ecs.ContainerImage.from_registry("public.ecr.aws/docker/library/nginx:1.25-alpine"),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="backend",
                log_group=ecs_log_group,
            ),
            environment={
                "APP_REGION": aws_region,
                "AWS_ENDPOINT": aws_endpoint,
                "TELEMETRY_QUEUE_URL": queue.queue_url,
                "TELEMETRY_EVENT_BUS_NAME": event_bus.event_bus_name,
                "DB_HOST": db_instance.attr_endpoint_address,
                "DB_PORT": db_instance.attr_endpoint_port,
            },
            secrets={
                "DB_USERNAME": ecs.Secret.from_secrets_manager(db_secret, field="username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db_secret, field="password"),
            },
            port_mappings=[ecs.PortMapping(container_port=BACKEND_PORT)],
        )

        # Internet -> ALB:80 -> backend Processing Units:80 -> Storage Layer (RDS PostgreSQL:5432)
        # The backend task role can publish telemetry to the shared queue and custom bus if the
        # container image is later replaced with an application that emits those events.
        queue.grant_send_messages(task_definition.task_role)
        event_bus.grant_put_events_to(task_definition.task_role)
        db_secret.grant_read(task_definition.task_role)
        task_definition.add_to_execution_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[db_secret.secret_arn],
            )
        )

        service = ecs.FargateService(
            self,
            "BackendService",
            service_name=f"{NAME_PREFIX}-backend",
            cluster=cluster,
            task_definition=task_definition,
            desired_count=1,
            security_groups=[backend_sg],
            assign_public_ip=False,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        )

        alb = elbv2.ApplicationLoadBalancer(
            self,
            "ApplicationLoadBalancer",
            load_balancer_name=f"{NAME_PREFIX}-alb",
            vpc=vpc,
            internet_facing=True,
            security_group=alb_sg,
        )
        target_group = elbv2.ApplicationTargetGroup(
            self,
            "BackendTargetGroup",
            target_group_name=f"{NAME_PREFIX}-tg",
            vpc=vpc,
            protocol=elbv2.ApplicationProtocol.HTTP,
            port=BACKEND_PORT,
            target_type=elbv2.TargetType.IP,
            health_check=elbv2.HealthCheck(path="/", healthy_http_codes="200-399"),
        )
        target_group.add_target(service)
        listener = alb.add_listener("HttpListener", port=80, open=False)
        listener.add_target_groups("BackendForwarding", target_groups=[target_group])

        api_endpoint_url = f"https://{api.ref}.execute-api.{Aws.REGION}.{Aws.URL_SUFFIX}"

        CfnOutput(self, "HttpApiUrl", value=api_endpoint_url)
        CfnOutput(self, "AlbDnsName", value=alb.load_balancer_dns_name)
        CfnOutput(self, "SqsQueueUrl", value=queue.queue_url)
        CfnOutput(self, "SqsQueueArn", value=queue.queue_arn)
        CfnOutput(self, "EventBusName", value=event_bus.event_bus_name)
        CfnOutput(self, "EventBusArn", value=event_bus.event_bus_arn)
        CfnOutput(self, "EventRuleArn", value=rule.attr_arn)
        CfnOutput(self, "EventPipeArn", value=pipe.attr_arn)
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "DynamoDBTableName", value=table.table_name)
        CfnOutput(
            self,
            "RdsEndpointAddress",
            value=db_instance.attr_endpoint_address,
        )
        CfnOutput(
            self,
            "RdsEndpointPort",
            value=db_instance.attr_endpoint_port,
        )
        CfnOutput(self, "DbSecretArn", value=db_secret.secret_arn)


app = App()
sdk_context = configure_sdk_context(app)

InfrastructureAnalysisStack(
    app,
    "InfrastructureAnalysisStack",
    sdk_context=sdk_context,
    env=Environment(region=sdk_context["AWS_REGION"]),
)

app.synth()
