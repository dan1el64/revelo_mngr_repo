import json
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pytest
from boto3.dynamodb.types import TypeDeserializer


STACK_NAME = "InventoryStack"
DESERIALIZER = TypeDeserializer()


def _region():
    return os.environ.get("AWS_REGION", "us-east-1")


def _client(service_name):
    return boto3.client(service_name, region_name=_region())


def _stack():
    response = _client("cloudformation").describe_stacks(StackName=STACK_NAME)
    stacks = response.get("Stacks", [])
    assert len(stacks) == 1, f"Expected deployed stack {STACK_NAME}"
    return stacks[0]


def _stack_template():
    template_body = _client("cloudformation").get_template(StackName=STACK_NAME)["TemplateBody"]
    if not isinstance(template_body, str):
        template_body = json.dumps(template_body)
    return json.loads(template_body)


def _stack_resources():
    paginator = _client("cloudformation").get_paginator("list_stack_resources")
    resources = []
    for page in paginator.paginate(StackName=STACK_NAME):
        resources.extend(page.get("StackResourceSummaries", []))
    assert resources, f"No resources returned for stack {STACK_NAME}"
    return resources


def _resources_by_type(resource_type):
    return [
        resource for resource in _stack_resources() if resource["ResourceType"] == resource_type
    ]


def _single_resource(resource_type):
    resources = _resources_by_type(resource_type)
    assert len(resources) == 1, f"Expected exactly 1 {resource_type}, found {len(resources)}"
    return resources[0]


def _resource_id(resource_type):
    return _single_resource(resource_type)["PhysicalResourceId"]


def _template_resources(resource_type):
    template = _stack_template()
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def _single_template_resource(resource_type):
    resources = _template_resources(resource_type)
    assert len(resources) == 1, f"Expected exactly 1 template {resource_type}, found {len(resources)}"
    return next(iter(resources.items()))


def _lambda_name(suffix):
    for resource in _resources_by_type("AWS::Lambda::Function"):
        physical_id = resource["PhysicalResourceId"]
        if physical_id.endswith(suffix):
            return physical_id
    raise AssertionError(f"Lambda with suffix {suffix!r} not found")


def _role_name_from_arn(role_arn):
    return role_arn.rsplit("/", 1)[-1]


def _invoke_lambda(function_name, payload):
    response = _client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    body = response["Payload"].read().decode("utf-8")
    assert response["StatusCode"] == 200, body
    assert not response.get("FunctionError"), body
    return json.loads(body) if body else {}



@pytest.fixture(scope="module")
def deployed_inventory():
    collector_name = _lambda_name("-collector")
    response = _invoke_lambda(collector_name, {})
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["services_written"] == 7
    assert body["snapshot_key"] == "inventory/latest.json"
    return {
        "collector_name": collector_name,
        "query_name": _lambda_name("-query"),
        "bucket_name": _resource_id("AWS::S3::Bucket"),
        "table_name": _resource_id("AWS::DynamoDB::Table"),
        "rest_api_id": _resource_id("AWS::ApiGateway::RestApi"),
        "schedule_name": _resource_id("AWS::Scheduler::Schedule"),
    }


def test_stack_is_deployed_with_expected_resource_inventory():
    stack = _stack()
    assert stack["StackName"] == STACK_NAME

    assert len(_resources_by_type("AWS::S3::Bucket")) == 1
    assert len(_resources_by_type("AWS::DynamoDB::Table")) == 1
    assert len(_resources_by_type("AWS::Logs::LogGroup")) == 2
    assert len(_resources_by_type("AWS::Lambda::Function")) == 2
    assert len(_resources_by_type("AWS::ApiGateway::RestApi")) == 1
    assert len(_resources_by_type("AWS::Scheduler::Schedule")) == 1
    assert len(_resources_by_type("AWS::IAM::Role")) == 3
    assert len(_resources_by_type("AWS::EC2::VPC")) == 0
    assert len(_resources_by_type("AWS::EC2::Subnet")) == 0
    assert len(_resources_by_type("AWS::EC2::SecurityGroup")) == 0


def test_deployed_bucket_matches_contract():
    bucket_name = _resource_id("AWS::S3::Bucket")
    s3 = _client("s3")

    assert s3.get_bucket_versioning(Bucket=bucket_name)["Status"] == "Enabled"
    assert s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }

    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    rule = encryption["ServerSideEncryptionConfiguration"]["Rules"][0]
    assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"

    policy = json.loads(s3.get_bucket_policy(Bucket=bucket_name)["Policy"])
    statements = policy["Statement"]
    tls_deny_statements = [
        s for s in statements
        if s["Effect"] == "Deny"
        and s.get("Condition") == {"Bool": {"aws:SecureTransport": "false"}}
    ]
    assert tls_deny_statements, "Bucket policy must deny non-TLS access (aws:SecureTransport = false)"

    _, bucket_resource = _single_template_resource("AWS::S3::Bucket")
    lifecycle_rule = bucket_resource["Properties"]["LifecycleConfiguration"]["Rules"][0]
    assert lifecycle_rule["Status"] == "Enabled"
    assert lifecycle_rule["NoncurrentVersionExpiration"] == {"NoncurrentDays": 30}
    assert lifecycle_rule["AbortIncompleteMultipartUpload"] == {"DaysAfterInitiation": 7}


def test_deployed_dynamodb_table_matches_contract():
    table_name = _resource_id("AWS::DynamoDB::Table")
    dynamodb = _client("dynamodb")

    description = dynamodb.describe_table(TableName=table_name)["Table"]
    assert description["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
    assert description["KeySchema"] == [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]
    assert description["AttributeDefinitions"] == [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
    ]
    assert "LatestStreamArn" not in description

    ttl = dynamodb.describe_time_to_live(TableName=table_name)["TimeToLiveDescription"]
    assert ttl["AttributeName"] == "ttl"
    assert ttl["TimeToLiveStatus"] in {"ENABLED", "ENABLING"}

    backups = dynamodb.describe_continuous_backups(TableName=table_name)[
        "ContinuousBackupsDescription"
    ]
    assert backups["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"] == "DISABLED"


def test_deployed_lambda_configs_and_roles_match_contract():
    lambda_client = _client("lambda")
    iam = _client("iam")

    collector_name = _lambda_name("-collector")
    query_name = _lambda_name("-query")

    collector_cfg = lambda_client.get_function_configuration(FunctionName=collector_name)
    query_cfg = lambda_client.get_function_configuration(FunctionName=query_name)

    assert collector_cfg["Runtime"] == "python3.12"
    assert collector_cfg["Handler"] == "app.handler"
    assert collector_cfg["MemorySize"] == 256
    assert collector_cfg["Timeout"] == 60

    assert query_cfg["Runtime"] == "python3.12"
    assert query_cfg["Handler"] == "app.handler"
    assert query_cfg["MemorySize"] == 256
    assert query_cfg["Timeout"] == 15

    assert collector_cfg["Environment"]["Variables"]["aws_region"] == _region()
    assert "aws_endpoint" in collector_cfg["Environment"]["Variables"]
    assert "inventory_bucket_name" in collector_cfg["Environment"]["Variables"]
    assert "inventory_table_name" in collector_cfg["Environment"]["Variables"]

    assert query_cfg["Environment"]["Variables"]["aws_region"] == _region()
    assert "aws_endpoint" in query_cfg["Environment"]["Variables"]
    assert "inventory_table_name" in query_cfg["Environment"]["Variables"]

    for cfg in (collector_cfg, query_cfg):
        role_name = _role_name_from_arn(cfg["Role"])
        assert iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"] == []
        inline_policies = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
        assert inline_policies

    lambda_resources = _template_resources("AWS::Lambda::Function")
    collector_resource = next(
        resource
        for resource in lambda_resources.values()
        if resource["Properties"]["FunctionName"] == collector_name
    )
    query_resource = next(
        resource
        for resource in lambda_resources.values()
        if resource["Properties"]["FunctionName"] == query_name
    )
    assert collector_resource["Properties"]["ReservedConcurrentExecutions"] == 1
    assert query_resource["Properties"]["ReservedConcurrentExecutions"] == 5


def test_scheduler_and_api_gateway_are_wired_to_expected_targets():
    schedule_name = _resource_id("AWS::Scheduler::Schedule")
    schedule = _client("scheduler").get_schedule(Name=schedule_name)
    collector_name = _lambda_name("-collector")
    collector_arn = _client("lambda").get_function(FunctionName=collector_name)["Configuration"][
        "FunctionArn"
    ]

    assert schedule["ScheduleExpression"] == "rate(15 minutes)"
    assert schedule["FlexibleTimeWindow"]["Mode"] == "OFF"
    assert schedule["Target"]["Arn"] == collector_arn

    rest_api_id = _resource_id("AWS::ApiGateway::RestApi")
    apigateway = _client("apigateway")
    rest_api = apigateway.get_rest_api(restApiId=rest_api_id)
    assert rest_api["endpointConfiguration"]["types"] == ["REGIONAL"]

    resources = apigateway.get_resources(restApiId=rest_api_id)["items"]
    path_parts = {resource.get("pathPart") for resource in resources if "pathPart" in resource}
    assert path_parts == {"inventory", "{service}"}

    _, stage_resource = _single_template_resource("AWS::ApiGateway::Stage")
    deployed_method_settings = stage_resource["Properties"].get("MethodSettings", [])
    assert any(
        s.get("LoggingLevel") == "INFO" and s.get("DataTraceEnabled") is False
        for s in deployed_method_settings
    ), "API Gateway stage must have execution logging at INFO level with data tracing disabled"


def test_collector_populates_snapshot_and_latest_dynamodb_rows(deployed_inventory):
    bucket_name = deployed_inventory["bucket_name"]
    table_name = deployed_inventory["table_name"]
    s3 = _client("s3")
    dynamodb = _client("dynamodb")

    snapshot = json.loads(
        s3.get_object(Bucket=bucket_name, Key="inventory/latest.json")["Body"].read().decode("utf-8")
    )
    collected_at = snapshot["collected_at"]
    assert collected_at.endswith("Z")
    datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    assert len(snapshot["services"]) == 7

    services = {entry["service"] for entry in snapshot["services"]}
    assert services == {"IAM", "EC2", "S3", "Lambda", "EventBridge", "RDS", "Glue"}

    rows = dynamodb.scan(TableName=table_name, Limit=200)["Items"]
    assert len(rows) == 7
    deserialized = [app_item(item) for item in rows]
    assert {item["service"] for item in deserialized} == services

    for item in deserialized:
        assert item["pk"] == "account"
        assert item["sk"] == f"service#{item['service']}"
        item_collected_at = item["collected_at"]
        assert item_collected_at.endswith("Z")
        datetime.fromisoformat(item_collected_at.replace("Z", "+00:00"))
        assert isinstance(item["counts"], dict)
        assert isinstance(item["sample"], dict)
        assert len(item["sample"]) <= 5
        assert isinstance(item["ttl"], (int, Decimal))
        assert int(item["ttl"]) > 0


def test_query_lambda_and_api_surface_return_expected_payloads(deployed_inventory):
    query_name = deployed_inventory["query_name"]

    lambda_inventory = _invoke_lambda(query_name, {"path": "/inventory"})
    assert lambda_inventory["statusCode"] == 200
    lambda_inventory_body = json.loads(lambda_inventory["body"])
    assert len(lambda_inventory_body) == 7

    lambda_service = _invoke_lambda(
        query_name,
        {"path": "/inventory/s3", "pathParameters": {"service": "s3"}},
    )
    assert lambda_service["statusCode"] == 200
    assert json.loads(lambda_service["body"])["service"] == "S3"

    lambda_missing = _invoke_lambda(
        query_name,
        {"path": "/inventory/unknown", "pathParameters": {"service": "unknown"}},
    )
    assert lambda_missing["statusCode"] == 404
    assert json.loads(lambda_missing["body"]) == {"message": "Service not found"}


def app_item(item):
    return {key: DESERIALIZER.deserialize(value) for key, value in item.items()}
