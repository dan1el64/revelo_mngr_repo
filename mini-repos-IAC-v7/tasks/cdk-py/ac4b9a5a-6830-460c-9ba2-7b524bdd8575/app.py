#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
    from aws_cdk import (
        App,
        CfnParameter,
        Duration,
        RemovalPolicy,
        Stack,
        aws_apigateway as apigateway,
        aws_dynamodb as dynamodb,
        aws_iam as iam,
        aws_lambda as lambda_,
        aws_logs as logs,
        aws_s3 as s3,
        aws_scheduler as scheduler,
    )
    from constructs import Construct


PK_VALUE = "account"
SERVICE_MAP = {
    "iam": "IAM",
    "ec2": "EC2",
    "s3": "S3",
    "lambda": "Lambda",
    "eventbridge": "EventBridge",
    "rds": "RDS",
    "glue": "Glue",
}
SERIALIZER = TypeSerializer()
DESERIALIZER = TypeDeserializer()


def _runtime_client(service_name: str):
    return boto3.client(
        service_name,
        region_name=os.environ["aws_region"],
        endpoint_url=os.environ["aws_endpoint"],
    )


def _paginate(client, operation_name, result_key, **kwargs):
    paginator = client.get_paginator(operation_name)
    for page in paginator.paginate(**kwargs):
        for item in page.get(result_key, []):
            yield item


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _serialize_item(item):
    return SERIALIZER.serialize(item)["M"]


def _deserialize_item(item):
    return {key: DESERIALIZER.deserialize(value) for key, value in item.items()}


def _json_default(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    raise TypeError(f"Unsupported type: {type(value)!r}")


def _collect_inventory():
    iam_client = _runtime_client("iam")
    ec2_client = _runtime_client("ec2")
    s3_client = _runtime_client("s3")
    lambda_client = _runtime_client("lambda")
    events_client = _runtime_client("events")
    rds_client = _runtime_client("rds")
    glue_client = _runtime_client("glue")

    roles = list(_paginate(iam_client, "list_roles", "Roles"))
    users = list(_paginate(iam_client, "list_users", "Users"))
    vpcs = list(_paginate(ec2_client, "describe_vpcs", "Vpcs"))
    subnets = list(_paginate(ec2_client, "describe_subnets", "Subnets"))
    security_groups = list(
        _paginate(ec2_client, "describe_security_groups", "SecurityGroups")
    )
    buckets = s3_client.list_buckets().get("Buckets", [])
    functions = list(_paginate(lambda_client, "list_functions", "Functions"))
    rules = list(
        _paginate(events_client, "list_rules", "Rules", EventBusName="default")
    )
    db_instances = list(_paginate(rds_client, "describe_db_instances", "DBInstances"))
    databases = list(_paginate(glue_client, "get_databases", "DatabaseList"))
    crawlers = list(_paginate(glue_client, "get_crawlers", "Crawlers"))

    return [
        {
            "service": "IAM",
            "counts": {"roles": len(roles), "users": len(users)},
            "sample": {
                "first_role_name": roles[0]["RoleName"] if roles else None,
                "first_user_name": users[0]["UserName"] if users else None,
            },
        },
        {
            "service": "EC2",
            "counts": {
                "vpcs": len(vpcs),
                "subnets": len(subnets),
                "security_groups": len(security_groups),
            },
            "sample": {
                "first_vpc_id": vpcs[0]["VpcId"] if vpcs else None,
                "first_subnet_id": subnets[0]["SubnetId"] if subnets else None,
                "first_security_group_id": (
                    security_groups[0]["GroupId"] if security_groups else None
                ),
            },
        },
        {
            "service": "S3",
            "counts": {"buckets": len(buckets)},
            "sample": {
                "first_bucket_name": buckets[0]["Name"] if buckets else None,
            },
        },
        {
            "service": "Lambda",
            "counts": {"functions": len(functions)},
            "sample": {
                "first_function_name": functions[0]["FunctionName"] if functions else None,
            },
        },
        {
            "service": "EventBridge",
            "counts": {"default_bus_rules": len(rules)},
            "sample": {
                "first_rule_name": rules[0]["Name"] if rules else None,
            },
        },
        {
            "service": "RDS",
            "counts": {"db_instances": len(db_instances)},
            "sample": {
                "first_db_instance_identifier": (
                    db_instances[0]["DBInstanceIdentifier"] if db_instances else None
                ),
            },
        },
        {
            "service": "Glue",
            "counts": {"databases": len(databases), "crawlers": len(crawlers)},
            "sample": {
                "first_database_name": databases[0]["Name"] if databases else None,
                "first_crawler_name": crawlers[0]["Name"] if crawlers else None,
            },
        },
    ]


def _collector_handler():
    collected_at = _iso_now()
    ttl = int(datetime.now(timezone.utc).timestamp()) + 86400
    table_name = os.environ["inventory_table_name"]
    bucket_name = os.environ["inventory_bucket_name"]
    snapshot_key = "inventory/latest.json"

    dynamodb_client = _runtime_client("dynamodb")
    s3_client = _runtime_client("s3")
    findings = _collect_inventory()

    for finding in findings:
        item = {
            "pk": PK_VALUE,
            "sk": f"service#{finding['service']}",
            "service": finding["service"],
            "collected_at": collected_at,
            "counts": finding["counts"],
            "sample": finding["sample"],
            "ttl": ttl,
        }
        dynamodb_client.put_item(TableName=table_name, Item=_serialize_item(item))
        print(
            json.dumps(
                {
                    "service": finding["service"],
                    "collected_at": collected_at,
                    "counts": finding["counts"],
                    "sample": finding["sample"],
                },
                sort_keys=True,
            )
        )

    snapshot = {"collected_at": collected_at, "services": findings}
    s3_client.put_object(
        Bucket=bucket_name,
        Key=snapshot_key,
        Body=json.dumps(snapshot, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return {
        "statusCode": 200,
        "body": json.dumps(
            {"services_written": len(findings), "snapshot_key": snapshot_key},
            sort_keys=True,
        ),
    }


def _query_handler(event):
    table_name = os.environ["inventory_table_name"]
    dynamodb_client = _runtime_client("dynamodb")
    path = event.get("path", "")
    path_parameters = event.get("pathParameters") or {}
    service_param = path_parameters.get("service")

    if service_param:
        canonical_service = SERVICE_MAP.get(service_param.lower())
        if canonical_service is None:
            response = {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Service not found"}, sort_keys=True),
            }
        else:
            result = dynamodb_client.get_item(
                TableName=table_name,
                Key={
                    "pk": {"S": PK_VALUE},
                    "sk": {"S": f"service#{canonical_service}"},
                },
            )
            if "Item" not in result:
                response = {
                    "statusCode": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"message": "Service not found"}, sort_keys=True),
                }
            else:
                response = {
                    "statusCode": 200,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(
                        _deserialize_item(result["Item"]),
                        default=_json_default,
                        sort_keys=True,
                    ),
                }
    else:
        result = dynamodb_client.scan(TableName=table_name, Limit=200)
        items = [_deserialize_item(item) for item in result.get("Items", [])]
        items.sort(key=lambda entry: entry.get("service", ""))
        response = {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(items, default=_json_default, sort_keys=True),
        }

    print(json.dumps({"path": path, "status_code": response["statusCode"]}, sort_keys=True))
    return response


def handler(event, context):
    function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
    if function_name.endswith("-collector"):
        return _collector_handler()
    return _query_handler(event)


class InventoryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        collector_function_name = f"{self.stack_name}-collector"
        query_function_name = f"{self.stack_name}-query"

        aws_region = CfnParameter(
            self,
            "aws_region",
            type="String",
            default="us-east-1",
        )
        aws_endpoint = CfnParameter(
            self,
            "aws_endpoint",
            type="String",
        )

        inventory_bucket = s3.Bucket(
            self,
            "InventoryBucket",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(
                    enabled=True,
                    noncurrent_version_expiration=Duration.days(30),
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )

        inventory_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="DenyInsecureBucketAccess",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=[
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketVersions",
                ],
                resources=[inventory_bucket.bucket_arn],
                conditions={"Bool": {"aws:SecureTransport": "false"}},
            )
        )
        inventory_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="DenyInsecureObjectAccess",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                ],
                resources=[inventory_bucket.arn_for_objects("*")],
                conditions={"Bool": {"aws:SecureTransport": "false"}},
            )
        )

        inventory_table = dynamodb.Table(
            self,
            "InventoryTable",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="sk",
                type=dynamodb.AttributeType.STRING,
            ),
            time_to_live_attribute="ttl",
            point_in_time_recovery_specification=(
                dynamodb.PointInTimeRecoverySpecification(
                    point_in_time_recovery_enabled=False
                )
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        collector_role = iam.Role(
            self,
            "CollectorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "CollectorAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:PutObject"],
                            resources=[inventory_bucket.arn_for_objects("*")],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:ListBucket"],
                            resources=[inventory_bucket.bucket_arn],
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "dynamodb:PutItem",
                                "dynamodb:UpdateItem",
                            ],
                            resources=[inventory_table.table_arn],
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "ec2:DescribeSecurityGroups",
                                "ec2:DescribeSubnets",
                                "ec2:DescribeVpcs",
                                "events:ListRules",
                                "glue:GetCrawlers",
                                "glue:GetDatabases",
                                "iam:ListRoles",
                                "iam:ListUsers",
                                "lambda:ListFunctions",
                                "rds:DescribeDBInstances",
                                "s3:ListAllMyBuckets",
                            ],
                            resources=["*"],
                        ),
                    ]
                )
            },
        )

        query_role = iam.Role(
            self,
            "QueryRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "QueryAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "dynamodb:GetItem",
                                "dynamodb:Scan",
                            ],
                            resources=[inventory_table.table_arn],
                        )
                    ]
                )
            },
        )

        collector_lambda = lambda_.Function(
            self,
            "CollectorLambda",
            function_name=collector_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                ".",
                exclude=[
                    "cdk.out",
                    "tests",
                    "lambda_collector",
                    "lambda_query",
                    "__pycache__",
                    ".pytest_cache",
                ],
            ),
            role=collector_role,
            memory_size=256,
            timeout=Duration.seconds(60),
            reserved_concurrent_executions=1,
            environment={
                "aws_region": aws_region.value_as_string,
                "aws_endpoint": aws_endpoint.value_as_string,
                "inventory_bucket_name": inventory_bucket.bucket_name,
                "inventory_table_name": inventory_table.table_name,
            },
        )

        query_lambda = lambda_.Function(
            self,
            "QueryLambda",
            function_name=query_function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                ".",
                exclude=[
                    "cdk.out",
                    "tests",
                    "lambda_collector",
                    "lambda_query",
                    "__pycache__",
                    ".pytest_cache",
                ],
            ),
            role=query_role,
            memory_size=256,
            timeout=Duration.seconds(15),
            reserved_concurrent_executions=5,
            environment={
                "aws_region": aws_region.value_as_string,
                "aws_endpoint": aws_endpoint.value_as_string,
                "inventory_table_name": inventory_table.table_name,
            },
        )

        collector_log_group = logs.LogGroup(
            self,
            "CollectorLogGroup",
            log_group_name=f"/aws/lambda/{collector_function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )
        query_log_group = logs.LogGroup(
            self,
            "QueryLogGroup",
            log_group_name=f"/aws/lambda/{query_function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        collector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[f"{collector_log_group.log_group_arn}:*"],
            )
        )
        query_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[f"{query_log_group.log_group_arn}:*"],
            )
        )

        api = apigateway.RestApi(
            self,
            "InventoryApi",
            endpoint_configuration=apigateway.EndpointConfiguration(
                types=[apigateway.EndpointType.REGIONAL]
            ),
            cloud_watch_role=False,
            deploy_options=apigateway.StageOptions(
                access_log_destination=apigateway.LogGroupLogDestination(
                    query_log_group
                ),
                access_log_format=apigateway.AccessLogFormat.custom(
                    '{"requestId":"$context.requestId",'
                    '"ip":"$context.identity.sourceIp",'
                    '"requestTime":"$context.requestTime",'
                    '"httpMethod":"$context.httpMethod",'
                    '"resourcePath":"$context.resourcePath",'
                    '"status":"$context.status",'
                    '"responseLength":"$context.responseLength"}'
                ),
                logging_level=apigateway.MethodLoggingLevel.INFO,
            ),
        )

        inventory_resource = api.root.add_resource("inventory")
        inventory_resource.add_method(
            "GET",
            apigateway.LambdaIntegration(query_lambda, proxy=True),
            authorization_type=apigateway.AuthorizationType.NONE,
        )
        inventory_service_resource = inventory_resource.add_resource("{service}")
        inventory_service_resource.add_method(
            "GET",
            apigateway.LambdaIntegration(query_lambda, proxy=True),
            authorization_type=apigateway.AuthorizationType.NONE,
        )

        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            inline_policies={
                "InvokeCollector": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["lambda:InvokeFunction"],
                            resources=[collector_lambda.function_arn],
                        )
                    ]
                )
            },
        )

        collection_schedule = scheduler.CfnSchedule(
            self,
            "CollectorSchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF"
            ),
            schedule_expression="rate(15 minutes)",
            target=scheduler.CfnSchedule.TargetProperty(
                arn=collector_lambda.function_arn,
                role_arn=scheduler_role.role_arn,
            ),
        )

        collector_lambda.add_permission(
            "AllowSchedulerInvoke",
            principal=iam.ServicePrincipal("scheduler.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=collection_schedule.attr_arn,
        )


if __name__ == "__main__":
    app = App()
    InventoryStack(app, "InventoryStack")
    app.synth()
