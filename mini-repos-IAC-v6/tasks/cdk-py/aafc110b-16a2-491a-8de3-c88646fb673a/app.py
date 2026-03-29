#!/usr/bin/env python3

import os
from urllib.parse import urlparse

from aws_cdk import (
    App,
    CfnCondition,
    CfnOutput,
    CfnResource,
    Duration,
    Environment,
    Fn,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigw,
    aws_cloudwatch as cloudwatch,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_pipes as pipes,
    aws_rds as rds,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


DEFAULT_REGION = "us-east-1"


def configure_aws_endpoint_environment() -> str:
    aws_endpoint = os.getenv("AWS_ENDPOINT", "").strip()
    if not aws_endpoint:
        return ""

    endpoint_vars = [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_APIGATEWAY",
        "AWS_ENDPOINT_URL_CLOUDFORMATION",
        "AWS_ENDPOINT_URL_CLOUDWATCH",
        "AWS_ENDPOINT_URL_EC2",
        "AWS_ENDPOINT_URL_EVENTS",
        "AWS_ENDPOINT_URL_IAM",
        "AWS_ENDPOINT_URL_LAMBDA",
        "AWS_ENDPOINT_URL_LOGS",
        "AWS_ENDPOINT_URL_PIPES",
        "AWS_ENDPOINT_URL_RDS",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL_SECRETSMANAGER",
        "AWS_ENDPOINT_URL_SQS",
        "AWS_ENDPOINT_URL_STATES",
    ]

    for name in endpoint_vars:
        os.environ[name] = aws_endpoint

    return aws_endpoint


def log_stream_arns(log_group: logs.ILogGroup) -> list[str]:
    return [log_group.log_group_arn, f"{log_group.log_group_arn}:*"]


def is_aws_endpoint(endpoint: str) -> bool:
    if not endpoint:
        return True

    parsed = urlparse(endpoint)
    host = (parsed.hostname or "").lower()
    return host.endswith("amazonaws.com")


class SecureNotificationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        db_username = "notifications_admin"
        deploy_database = is_aws_endpoint(os.getenv("AWS_ENDPOINT", ""))
        deploy_database_condition = CfnCondition(
            self,
            "DeployDatabaseCondition",
            expression=Fn.condition_equals(
                "true",
                "true" if deploy_database else "false",
            ),
        )

        vpc = ec2.Vpc(
            self,
            "NotificationVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="PrivateWithEgress",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        compute_security_group = ec2.SecurityGroup(
            self,
            "ComputeSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Security group shared by the Lambda functions",
        )

        database_security_group = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=vpc,
            allow_all_outbound=False,
            description="Security group for the PostgreSQL instance",
        )
        database_security_group.add_ingress_rule(
            compute_security_group,
            ec2.Port.tcp(5432),
            "Allow PostgreSQL access only from compute resources",
        )

        artifacts_bucket = s3.Bucket(
            self,
            "ArtifactsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        queue = sqs.Queue(
            self,
            "NotificationQueue",
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            visibility_timeout=Duration.seconds(30),
            retention_period=Duration.days(4),
        )

        api_handler_log_group = logs.LogGroup(
            self,
            "ApiHandlerLogGroup",
            log_group_name="/aws/lambda/secure-notification-api-handler",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )
        worker_log_group = logs.LogGroup(
            self,
            "WorkerLogGroup",
            log_group_name="/aws/lambda/secure-notification-worker",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )
        api_access_log_group = logs.LogGroup(
            self,
            "ApiAccessLogGroup",
            log_group_name="/aws/apigateway/secure-notification-access",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )
        state_machine_log_group = logs.LogGroup(
            self,
            "StateMachineLogGroup",
            log_group_name="/aws/vendedlogs/states/secure-notification-state-machine",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        api_handler_role = iam.Role(
            self,
            "ApiHandlerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "ApiHandlerPermissions": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["sqs:SendMessage"],
                            resources=[queue.queue_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                            resources=log_stream_arns(api_handler_log_group),
                        ),
                    ]
                )
            },
        )

        worker_role = iam.Role(
            self,
            "WorkerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "WorkerPermissions": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                            resources=log_stream_arns(worker_log_group),
                        ),
                    ]
                )
            },
        )

        api_handler_function = _lambda.Function(
            self,
            "ApiHandlerFunction",
            function_name="secure-notification-api-handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(
                """
import json
import os

import boto3


_sqs = boto3.client("sqs")
_queue_url = os.environ["QUEUE_URL"]


def handler(event, _context):
    records = event.get("Records")
    if isinstance(records, list):
        return {
            "records": [
                {
                    "messageId": record.get("messageId"),
                    "body": record.get("body"),
                }
                for record in records
            ]
        }

    body = event.get("body")
    if isinstance(body, str):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"message": body}
    elif body is None:
        payload = event
    else:
        payload = body

    _sqs.send_message(
        QueueUrl=_queue_url,
        MessageBody=json.dumps(payload),
    )
    return {
        "statusCode": 202,
        "body": json.dumps({"accepted": True}),
    }
"""
            ),
            memory_size=256,
            timeout=Duration.seconds(10),
            environment={"QUEUE_URL": queue.queue_url},
            log_group=api_handler_log_group,
            role=api_handler_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[compute_security_group],
        )

        db_secret = secretsmanager.Secret(
            self,
            "DatabaseSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=f'{{"username":"{db_username}"}}',
                generate_string_key="password",
                exclude_punctuation=True,
            ),
        )

        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[db_secret.secret_arn],
            )
        )

        worker_function = _lambda.Function(
            self,
            "WorkerFunction",
            function_name="secure-notification-worker",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(
                """
import json
import os

import boto3


_secrets = boto3.client("secretsmanager")
_db_credentials_arn = os.environ["DB_CREDENTIALS_ARN"]


def handler(event, _context):
    secret_value = _secrets.get_secret_value(SecretId=_db_credentials_arn)
    records = event.get("Records")
    if not isinstance(records, list):
        records = event.get("records", [])
    return {
        "processedRecords": len(records),
        "secretLoaded": bool(secret_value.get("ARN")),
        "event": event if not records else {"records": len(records)},
    }
"""
            ),
            memory_size=256,
            timeout=Duration.seconds(20),
            environment={"DB_CREDENTIALS_ARN": db_secret.secret_arn},
            log_group=worker_log_group,
            role=worker_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[compute_security_group],
        )

        database = rds.DatabaseInstance(
            self,
            "NotificationDatabase",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3,
                ec2.InstanceSize.MICRO,
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[database_security_group],
            credentials=rds.Credentials.from_password(
                username=db_username,
                password=db_secret.secret_value_from_json("password"),
            ),
            allocated_storage=20,
            backup_retention=Duration.days(1),
            storage_encrypted=True,
            publicly_accessible=False,
            deletion_protection=False,
            delete_automated_backups=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        database_instance_resource = database.node.default_child
        if isinstance(database_instance_resource, rds.CfnDBInstance):
            database_instance_resource.master_username = Fn.join(
                "",
                [
                    "{{resolve:secretsmanager:",
                    db_secret.secret_arn,
                    ":SecretString:username::}}",
                ],
            )
        for child in database.node.find_all():
            if isinstance(child, CfnResource) and child.cfn_resource_type.startswith(
                "AWS::RDS::"
            ):
                child.cfn_options.condition = deploy_database_condition

        process_notification_task = sfn_tasks.LambdaInvoke(
            self,
            "ProcessNotificationTask",
            lambda_function=worker_function,
            payload_response_only=True,
        )

        state_machine = sfn.StateMachine(
            self,
            "NotificationStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(process_notification_task),
            state_machine_type=sfn.StateMachineType.STANDARD,
            logs=sfn.LogOptions(
                destination=state_machine_log_group,
                include_execution_data=True,
                level=sfn.LogLevel.ALL,
            ),
        )

        pipe_role = iam.Role(
            self,
            "PipeRole",
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
                            resources=[queue.queue_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["lambda:InvokeFunction"],
                            resources=[api_handler_function.function_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["states:StartExecution"],
                            resources=[state_machine.state_machine_arn],
                        ),
                    ]
                )
            },
        )

        pipes.CfnPipe(
            self,
            "QueueToStateMachinePipe",
            role_arn=pipe_role.role_arn,
            source=queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=10
                )
            ),
            enrichment=api_handler_function.function_arn,
            target=state_machine.state_machine_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                step_function_state_machine_parameters=pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                )
            ),
            desired_state="RUNNING",
        )

        api = apigw.RestApi(
            self,
            "NotificationApi",
            cloud_watch_role=False,
            deploy_options=apigw.StageOptions(
                access_log_destination=apigw.LogGroupLogDestination(
                    api_access_log_group
                ),
                access_log_format=apigw.AccessLogFormat.json_with_standard_fields(
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
        api.root.add_resource("events").add_method(
            "POST",
            apigw.LambdaIntegration(api_handler_function),
        )

        cloudwatch.Alarm(
            self,
            "ApiHandlerErrorAlarm",
            metric=api_handler_function.metric_errors(period=Duration.seconds(60)),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        cloudwatch.Alarm(
            self,
            "QueueBacklogAlarm",
            metric=queue.metric_approximate_number_of_messages_visible(
                period=Duration.seconds(60),
                statistic="Maximum",
            ),
            threshold=10,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        CfnOutput(self, "ApiUrl", value=api.url)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "BucketName", value=artifacts_bucket.bucket_name)
        CfnOutput(
            self,
            "DatabaseEndpoint",
            value=Fn.condition_if(
                deploy_database_condition.logical_id,
                database.instance_endpoint.hostname,
                "database-not-deployed",
            ).to_string(),
        )


aws_region = os.getenv("AWS_REGION", DEFAULT_REGION)
os.environ.setdefault("AWS_REGION", aws_region)
configure_aws_endpoint_environment()

app = App()
SecureNotificationStack(
    app,
    "SecureNotificationStack",
    env=Environment(region=aws_region),
)
app.synth()
