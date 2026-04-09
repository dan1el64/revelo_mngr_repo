import json
import os
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/aws-jsii-package-cache")

from aws_cdk import App, assertions

import app as app_module


InventoryStack = app_module.InventoryStack


def _template(stack_id="InventoryStackTest"):
    app = App()
    stack = InventoryStack(app, stack_id)
    return assertions.Template.from_stack(stack).to_json()


def _resources(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def _single_resource(template, resource_type):
    resources = _resources(template, resource_type)
    assert len(resources) == 1, f"Expected exactly 1 {resource_type}, found {len(resources)}"
    return next(iter(resources.items()))


def _resource_by_property(resources, property_name, expected_value):
    for logical_id, resource in resources.items():
        if resource["Properties"].get(property_name) == expected_value:
            return logical_id, resource
    raise AssertionError(
        f"Resource with {property_name}={expected_value!r} not found in {list(resources)}"
    )


def _lambda_function(template, function_name):
    return _resource_by_property(
        _resources(template, "AWS::Lambda::Function"),
        "FunctionName",
        function_name,
    )


def _log_group(template, log_group_name):
    return _resource_by_property(
        _resources(template, "AWS::Logs::LogGroup"),
        "LogGroupName",
        log_group_name,
    )


def _role_id_from_lambda(lambda_resource):
    return lambda_resource["Properties"]["Role"]["Fn::GetAtt"][0]


def _policy_for_role(template, role_logical_id):
    statements = []
    policies = _resources(template, "AWS::IAM::Policy")
    for _, policy in policies.items():
        if policy["Properties"]["Roles"] == [{"Ref": role_logical_id}]:
            statements.extend(policy["Properties"]["PolicyDocument"]["Statement"])
    role = template["Resources"].get(role_logical_id)
    if role and role["Type"] == "AWS::IAM::Role":
        inline_policies = role["Properties"].get("Policies", [])
        for policy in inline_policies:
            statements.extend(policy["PolicyDocument"]["Statement"])
    if statements:
        return {"Version": "2012-10-17", "Statement": statements}
    raise AssertionError(f"No IAM policy statements found for role {role_logical_id}")


def _statement_by_actions(policy_document, actions):
    expected = actions if isinstance(actions, list) else [actions]
    for statement in policy_document["Statement"]:
        actual = statement["Action"]
        actual_list = actual if isinstance(actual, list) else [actual]
        if actual_list == expected:
            return statement
        if sorted(actual_list) == sorted(expected):
            return statement
    raise AssertionError(f"No statement found for actions {actions}")


def _statement_containing_actions(policy_document, actions):
    expected = set(actions if isinstance(actions, list) else [actions])
    for statement in policy_document["Statement"]:
        actual = statement["Action"]
        actual_list = actual if isinstance(actual, list) else [actual]
        if expected.issubset(set(actual_list)):
            return statement
    raise AssertionError(f"No statement found containing actions {actions}")


def _all_policy_statements(template):
    statements = []

    for resource in _resources(template, "AWS::IAM::Policy").values():
        statements.extend(resource["Properties"]["PolicyDocument"]["Statement"])

    for resource in _resources(template, "AWS::IAM::Role").values():
        for policy in resource["Properties"].get("Policies", []):
            statements.extend(policy["PolicyDocument"]["Statement"])

    return statements


def _deletion_policies(template):
    for logical_id, resource in template["Resources"].items():
        yield logical_id, resource.get("DeletionPolicy"), resource.get("UpdateReplacePolicy")


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class FakeClient:
    def __init__(self, paginated=None, methods=None):
        self.paginated = paginated or {}
        self.methods = methods or {}
        self.method_calls = []
        self.paginators = {}

    def get_paginator(self, operation_name):
        paginator = FakePaginator(self.paginated[operation_name])
        self.paginators[operation_name] = paginator
        return paginator

    def __getattr__(self, name):
        if name not in self.methods:
            raise AttributeError(name)

        def _method(**kwargs):
            self.method_calls.append((name, kwargs))
            result = self.methods[name]
            if callable(result):
                return result(**kwargs)
            return result

        return _method


def _runtime_clients():
    dynamodb_client = FakeClient(
        methods={
            "put_item": lambda **kwargs: {"ResponseMetadata": {"HTTPStatusCode": 200}},
            "get_item": lambda **kwargs: {
                "Item": app_module._serialize_item(
                    {
                        "pk": "account",
                        "sk": "service#S3",
                        "service": "S3",
                        "collected_at": "2024-01-02T03:04:05Z",
                        "counts": {"buckets": 3},
                        "sample": {"first_bucket_name": "alpha-bucket"},
                        "ttl": 1704251045,
                    }
                )
            },
            "scan": lambda **kwargs: {
                "Items": [
                    app_module._serialize_item(
                        {
                            "pk": "account",
                            "sk": "service#S3",
                            "service": "S3",
                            "collected_at": "2024-01-02T03:04:05Z",
                            "counts": {"buckets": 3},
                            "sample": {"first_bucket_name": "alpha-bucket"},
                            "ttl": 1704251045,
                        }
                    ),
                    app_module._serialize_item(
                        {
                            "pk": "account",
                            "sk": "service#EC2",
                            "service": "EC2",
                            "collected_at": "2024-01-02T03:04:05Z",
                            "counts": {"vpcs": 2, "subnets": 4, "security_groups": 5},
                            "sample": {"first_vpc_id": "vpc-123"},
                            "ttl": 1704251045,
                        }
                    ),
                ]
            },
        }
    )
    s3_client = FakeClient(
        methods={
            "list_buckets": {
                "Buckets": [
                    {"Name": "alpha-bucket"},
                    {"Name": "beta-bucket"},
                    {"Name": "gamma-bucket"},
                ]
            },
            "put_object": lambda **kwargs: {"ETag": "etag"},
        }
    )
    clients = {
        "iam": FakeClient(
            paginated={
                "list_roles": [
                    {
                        "Roles": [
                            {"RoleName": "Admin"},
                            {"RoleName": "ReadOnly"},
                        ]
                    }
                ],
                "list_users": [{"Users": [{"UserName": "alice"}]}],
            }
        ),
        "ec2": FakeClient(
            paginated={
                "describe_vpcs": [{"Vpcs": [{"VpcId": "vpc-123"}, {"VpcId": "vpc-456"}]}],
                "describe_subnets": [
                    {
                        "Subnets": [
                            {"SubnetId": "subnet-a"},
                            {"SubnetId": "subnet-b"},
                            {"SubnetId": "subnet-c"},
                            {"SubnetId": "subnet-d"},
                        ]
                    }
                ],
                "describe_security_groups": [
                    {
                        "SecurityGroups": [
                            {"GroupId": "sg-1"},
                            {"GroupId": "sg-2"},
                            {"GroupId": "sg-3"},
                            {"GroupId": "sg-4"},
                            {"GroupId": "sg-5"},
                        ]
                    }
                ],
            }
        ),
        "s3": s3_client,
        "lambda": FakeClient(
            paginated={
                "list_functions": [
                    {
                        "Functions": [
                            {"FunctionName": "collector"},
                            {"FunctionName": "query"},
                        ]
                    }
                ]
            }
        ),
        "events": FakeClient(
            paginated={
                "list_rules": [
                    {
                        "Rules": [
                            {"Name": "every-15m"},
                            {"Name": "manual"},
                        ]
                    }
                ]
            }
        ),
        "rds": FakeClient(
            paginated={
                "describe_db_instances": [
                    {"DBInstances": [{"DBInstanceIdentifier": "inventory-db"}]}
                ]
            }
        ),
        "glue": FakeClient(
            paginated={
                "get_databases": [{"DatabaseList": [{"Name": "catalog"}]}],
                "get_crawlers": [{"Crawlers": [{"Name": "crawler-a"}]}],
            }
        ),
        "dynamodb": dynamodb_client,
    }
    return clients


def test_single_file_delivery_and_input_contract(monkeypatch):
    python_files = sorted(
        path.relative_to(Path.cwd()).as_posix()
        for path in Path.cwd().glob("*.py")
    )
    assert python_files == ["app.py"]

    template = _template()
    parameters = template["Parameters"]

    assert {"awsregion", "awsendpoint"} <= set(parameters)
    assert parameters["awsregion"]["Default"] == "us-east-1"
    assert "Default" not in parameters["awsendpoint"]

    app = App()
    stack = InventoryStack(app, "InventoryContractStack")
    assert stack.termination_protection is False


def test_s3_bucket_and_bucket_policy_contract():
    template = _template()

    bucket_id, bucket = _single_resource(template, "AWS::S3::Bucket")
    assert "BucketName" not in bucket["Properties"]
    assert bucket["Properties"]["VersioningConfiguration"] == {"Status": "Enabled"}
    assert bucket["Properties"]["BucketEncryption"] == {
        "ServerSideEncryptionConfiguration": [
            {
                "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
            }
        ]
    }
    assert bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    rule = bucket["Properties"]["LifecycleConfiguration"]["Rules"][0]
    assert rule["Status"] == "Enabled"
    assert rule["NoncurrentVersionExpiration"] == {"NoncurrentDays": 30}
    assert rule["AbortIncompleteMultipartUpload"] == {"DaysAfterInitiation": 7}
    assert bucket.get("DeletionPolicy") != "Retain"
    assert bucket.get("UpdateReplacePolicy") != "Retain"

    _, bucket_policy = _single_resource(template, "AWS::S3::BucketPolicy")
    statements = bucket_policy["Properties"]["PolicyDocument"]["Statement"]

    tls_deny_statements = [
        s for s in statements
        if s["Effect"] == "Deny"
        and s.get("Condition") == {"Bool": {"aws:SecureTransport": "false"}}
    ]
    assert tls_deny_statements, "Bucket policy must deny non-TLS access (aws:SecureTransport = false)"
    for statement in tls_deny_statements:
        assert statement["Principal"] == {"AWS": "*"}


def test_dynamodb_table_contract():
    template = _template()

    _, table = _single_resource(template, "AWS::DynamoDB::Table")
    assert "TableName" not in table["Properties"]
    assert table["Properties"]["BillingMode"] == "PAY_PER_REQUEST"
    assert table["Properties"]["AttributeDefinitions"] == [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
    ]
    assert table["Properties"]["KeySchema"] == [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]
    assert table["Properties"]["TimeToLiveSpecification"] == {
        "AttributeName": "ttl",
        "Enabled": True,
    }
    assert table["Properties"]["PointInTimeRecoverySpecification"] == {
        "PointInTimeRecoveryEnabled": False
    }
    assert "StreamSpecification" not in table["Properties"]
    assert table.get("DeletionPolicy") != "Retain"
    assert table.get("UpdateReplacePolicy") != "Retain"


def test_log_group_contract():
    stack_id = "InventoryStackTest"
    template = _template(stack_id=stack_id)

    log_groups = _resources(template, "AWS::Logs::LogGroup")
    assert len(log_groups) == 2

    for name in (
        f"/aws/lambda/{stack_id}-collector",
        f"/aws/lambda/{stack_id}-query",
    ):
        _, log_group = _log_group(template, name)
        assert log_group["Properties"]["RetentionInDays"] == 14
        assert "KmsKeyId" not in log_group["Properties"]
        assert log_group.get("DeletionPolicy") != "Retain"
        assert log_group.get("UpdateReplacePolicy") != "Retain"


def test_lambda_function_contracts_and_internal_wiring():
    stack_id = "InventoryStackTest"
    template = _template(stack_id=stack_id)

    _, bucket = _single_resource(template, "AWS::S3::Bucket")
    _, table = _single_resource(template, "AWS::DynamoDB::Table")
    _, collector = _lambda_function(template, f"{stack_id}-collector")
    _, query = _lambda_function(template, f"{stack_id}-query")

    assert len(_resources(template, "AWS::Lambda::Function")) == 2

    assert collector["Properties"]["Runtime"] == "python3.12"
    assert collector["Properties"]["Handler"] == "app.handler"
    assert collector["Properties"]["MemorySize"] == 256
    assert collector["Properties"]["Timeout"] == 60
    assert collector["Properties"]["ReservedConcurrentExecutions"] == 1
    assert collector["Properties"]["Environment"]["Variables"] == {
        "aws_region": {"Ref": "awsregion"},
        "aws_endpoint": {"Ref": "awsendpoint"},
        "inventory_bucket_name": {"Ref": next(iter(_resources(template, "AWS::S3::Bucket")))},
        "inventory_table_name": {"Ref": next(iter(_resources(template, "AWS::DynamoDB::Table")))},
    }
    assert "VpcConfig" not in collector["Properties"]

    assert query["Properties"]["Runtime"] == "python3.12"
    assert query["Properties"]["Handler"] == "app.handler"
    assert query["Properties"]["MemorySize"] == 256
    assert query["Properties"]["Timeout"] == 15
    assert query["Properties"]["ReservedConcurrentExecutions"] == 5
    assert query["Properties"]["Environment"]["Variables"] == {
        "aws_region": {"Ref": "awsregion"},
        "aws_endpoint": {"Ref": "awsendpoint"},
        "inventory_table_name": {"Ref": next(iter(_resources(template, "AWS::DynamoDB::Table")))},
    }
    assert "VpcConfig" not in query["Properties"]

    assert "BucketName" not in bucket["Properties"]
    assert "TableName" not in table["Properties"]


def test_api_gateway_contract_and_query_lambda_wiring():
    stack_id = "InventoryStackTest"
    template = _template(stack_id=stack_id)

    query_log_group_id, _ = _log_group(template, f"/aws/lambda/{stack_id}-query")
    query_lambda_id, _ = _lambda_function(template, f"{stack_id}-query")

    _, api = _single_resource(template, "AWS::ApiGateway::RestApi")
    assert api["Properties"]["EndpointConfiguration"] == {"Types": ["REGIONAL"]}

    resources = _resources(template, "AWS::ApiGateway::Resource")
    assert len(resources) == 2
    inventory_id, inventory_resource = _resource_by_property(resources, "PathPart", "inventory")
    _, service_resource = _resource_by_property(resources, "PathPart", "{service}")
    assert service_resource["Properties"]["ParentId"] == {"Ref": inventory_id}

    methods = list(_resources(template, "AWS::ApiGateway::Method").values())
    assert len(methods) == 2
    for method in methods:
        assert method["Properties"]["HttpMethod"] == "GET"
        assert method["Properties"]["AuthorizationType"] == "NONE"
        assert method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        assert method["Properties"]["Integration"]["IntegrationHttpMethod"] == "POST"
        integration_uri = json.dumps(method["Properties"]["Integration"]["Uri"], sort_keys=True)
        assert query_lambda_id in integration_uri

    _, stage = _single_resource(template, "AWS::ApiGateway::Stage")
    assert stage["Properties"]["MethodSettings"] == [
        {
            "DataTraceEnabled": False,
            "HttpMethod": "*",
            "LoggingLevel": "INFO",
            "ResourcePath": "/*",
        }
    ]
    assert stage["Properties"]["AccessLogSetting"]["DestinationArn"] == {
        "Fn::GetAtt": [query_log_group_id, "Arn"]
    }

    permissions = _resources(template, "AWS::Lambda::Permission")
    api_permissions = [
        resource
        for resource in permissions.values()
        if resource["Properties"]["Principal"] == "apigateway.amazonaws.com"
    ]
    assert len(api_permissions) >= 2
    for permission in api_permissions:
        function_name_json = json.dumps(permission["Properties"]["FunctionName"], sort_keys=True)
        assert query_lambda_id in function_name_json


def test_scheduler_contract_and_collector_wiring():
    stack_id = "InventoryStackTest"
    template = _template(stack_id=stack_id)

    collector_lambda_id, collector_lambda = _lambda_function(template, f"{stack_id}-collector")
    collector_role_id = _role_id_from_lambda(collector_lambda)

    roles = _resources(template, "AWS::IAM::Role")
    assert len(roles) == 3

    _, schedule = _single_resource(template, "AWS::Scheduler::Schedule")
    assert schedule["Properties"]["ScheduleExpression"] == "rate(15 minutes)"
    assert schedule["Properties"]["FlexibleTimeWindow"] == {"Mode": "OFF"}
    assert schedule["Properties"]["Target"]["Arn"] == {"Fn::GetAtt": [collector_lambda_id, "Arn"]}

    scheduler_role_id = schedule["Properties"]["Target"]["RoleArn"]["Fn::GetAtt"][0]
    scheduler_policy = _policy_for_role(template, scheduler_role_id)
    scheduler_statement = scheduler_policy["Statement"][0]
    scheduler_actions = scheduler_statement["Action"]
    scheduler_actions = scheduler_actions if isinstance(scheduler_actions, list) else [scheduler_actions]
    assert scheduler_actions == ["lambda:InvokeFunction"]
    assert scheduler_statement["Effect"] == "Allow"
    assert scheduler_statement["Resource"] == {"Fn::GetAtt": [collector_lambda_id, "Arn"]}

    permissions = _resources(template, "AWS::Lambda::Permission")
    scheduler_permissions = [
        resource
        for resource in permissions.values()
        if resource["Properties"]["Principal"] == "scheduler.amazonaws.com"
    ]
    assert len(scheduler_permissions) == 1
    assert scheduler_permissions[0]["Properties"]["Action"] == "lambda:InvokeFunction"
    assert scheduler_permissions[0]["Properties"]["Principal"] == "scheduler.amazonaws.com"
    assert collector_lambda_id in json.dumps(
        scheduler_permissions[0]["Properties"]["FunctionName"],
        sort_keys=True,
    )
    assert scheduler_permissions[0]["Properties"]["SourceArn"] == {
        "Fn::GetAtt": [next(iter(_resources(template, "AWS::Scheduler::Schedule"))), "Arn"]
    }
    assert collector_role_id != scheduler_role_id


def test_iam_policies_are_inline_scoped_and_without_wildcard_actions():
    template = _template()

    assert len(_resources(template, "AWS::IAM::ManagedPolicy")) == 0

    roles = _resources(template, "AWS::IAM::Role")
    assert len(roles) == 3
    for role in roles.values():
        assert "ManagedPolicyArns" not in role["Properties"]

    statements = _all_policy_statements(template)
    assert statements

    for statement in statements:
        assert statement["Action"] != "*"

    wildcard_resource_statements = [
        statement for statement in statements if statement["Resource"] == "*"
    ]
    assert len(wildcard_resource_statements) == 1
    wildcard_actions = wildcard_resource_statements[0]["Action"]
    if isinstance(wildcard_actions, str):
        wildcard_actions = [wildcard_actions]
    assert set(wildcard_actions) == {
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
    }


def test_collector_and_query_iam_permissions_are_exact():
    stack_id = "InventoryStackTest"
    template = _template(stack_id=stack_id)

    bucket_id, _ = _single_resource(template, "AWS::S3::Bucket")
    table_id, _ = _single_resource(template, "AWS::DynamoDB::Table")
    collector_lambda_id, collector_lambda = _lambda_function(template, f"{stack_id}-collector")
    query_lambda_id, query_lambda = _lambda_function(template, f"{stack_id}-query")
    collector_log_group_id, _ = _log_group(template, f"/aws/lambda/{stack_id}-collector")
    query_log_group_id, _ = _log_group(template, f"/aws/lambda/{stack_id}-query")

    collector_policy = _policy_for_role(template, _role_id_from_lambda(collector_lambda))
    collector_policy_json = json.dumps(collector_policy, sort_keys=True)
    assert "s3:PutObject" in collector_policy_json
    assert bucket_id in collector_policy_json

    put_object_stmt = _statement_containing_actions(collector_policy, ["s3:PutObject"])
    put_object_resources = put_object_stmt["Resource"]
    if not isinstance(put_object_resources, list):
        put_object_resources = [put_object_resources]
    put_object_resources_json = json.dumps(put_object_resources, sort_keys=True)
    assert bucket_id in put_object_resources_json, "s3:PutObject must reference bucket"
    assert "/*" in put_object_resources_json, "s3:PutObject must be granted on objects ARN (bucket/*)"

    assert _statement_by_actions(
        collector_policy,
        ["dynamodb:PutItem", "dynamodb:UpdateItem"],
    )["Resource"] == {"Fn::GetAtt": [table_id, "Arn"]}
    assert _statement_by_actions(
        collector_policy,
        ["logs:CreateLogStream", "logs:PutLogEvents"],
    )["Resource"] == {
        "Fn::Join": ["", [{"Fn::GetAtt": [collector_log_group_id, "Arn"]}, ":*"]]
    }

    assert "s3:DeleteObject" not in collector_policy_json
    assert "dynamodb:DeleteItem" not in collector_policy_json

    query_policy = _policy_for_role(template, _role_id_from_lambda(query_lambda))
    assert _statement_by_actions(
        query_policy,
        ["dynamodb:GetItem", "dynamodb:Scan"],
    )["Resource"] == {"Fn::GetAtt": [table_id, "Arn"]}
    assert _statement_by_actions(
        query_policy,
        ["logs:CreateLogStream", "logs:PutLogEvents"],
    )["Resource"] == {
        "Fn::Join": ["", [{"Fn::GetAtt": [query_log_group_id, "Arn"]}, ":*"]]
    }

    query_policy_json = json.dumps(query_policy, sort_keys=True)
    assert "dynamodb:PutItem" not in query_policy_json
    assert "dynamodb:UpdateItem" not in query_policy_json
    assert "dynamodb:DeleteItem" not in query_policy_json
    assert collector_lambda_id != query_lambda_id


def test_no_vpc_kms_or_retain_semantics_anywhere():
    template = _template()

    assert len(_resources(template, "AWS::EC2::VPC")) == 0
    assert len(_resources(template, "AWS::EC2::Subnet")) == 0
    assert len(_resources(template, "AWS::EC2::SecurityGroup")) == 0
    assert len(_resources(template, "AWS::KMS::Key")) == 0

    for logical_id, deletion_policy, update_replace_policy in _deletion_policies(template):
        assert deletion_policy not in {"Retain", "Snapshot"}, logical_id
        assert update_replace_policy not in {"Retain", "Snapshot"}, logical_id


def test_dynamic_names_follow_stack_name():
    template = _template(stack_id="AlternateInventoryStack")

    _, collector = _lambda_function(template, "AlternateInventoryStack-collector")
    _, query = _lambda_function(template, "AlternateInventoryStack-query")
    assert collector["Properties"]["FunctionName"] == "AlternateInventoryStack-collector"
    assert query["Properties"]["FunctionName"] == "AlternateInventoryStack-query"

    _log_group(template, "/aws/lambda/AlternateInventoryStack-collector")
    _log_group(template, "/aws/lambda/AlternateInventoryStack-query")


def test_collect_inventory_returns_required_services_counts_and_samples(monkeypatch):
    clients = _runtime_clients()
    monkeypatch.setattr(app_module, "_runtime_client", lambda service_name: clients[service_name])

    findings = app_module._collect_inventory()

    assert [entry["service"] for entry in findings] == [
        "IAM",
        "EC2",
        "S3",
        "Lambda",
        "EventBridge",
        "RDS",
        "Glue",
    ]

    by_service = {entry["service"]: entry for entry in findings}
    assert by_service["IAM"]["counts"] == {"roles": 2, "users": 1}
    assert by_service["EC2"]["counts"] == {
        "vpcs": 2,
        "subnets": 4,
        "security_groups": 5,
    }
    assert by_service["S3"]["counts"] == {"buckets": 3}
    assert by_service["Lambda"]["counts"] == {"functions": 2}
    assert by_service["EventBridge"]["counts"] == {"default_bus_rules": 2}
    assert by_service["RDS"]["counts"] == {"db_instances": 1}
    assert by_service["Glue"]["counts"] == {"databases": 1, "crawlers": 1}

    assert by_service["IAM"]["sample"] == {
        "first_role_name": "Admin",
        "first_user_name": "alice",
    }
    assert by_service["EC2"]["sample"] == {
        "first_vpc_id": "vpc-123",
        "first_subnet_id": "subnet-a",
        "first_security_group_id": "sg-1",
    }
    for entry in findings:
        assert len(entry["sample"]) <= 5

    assert clients["events"].paginators["list_rules"].calls == [{"EventBusName": "default"}]


def test_collector_handler_writes_latest_records_snapshot_and_structured_logs(monkeypatch, capsys):
    frozen_now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    expected_ttl = int(frozen_now.timestamp()) + 86400
    clients = _runtime_clients()

    class FrozenDateTime:
        @classmethod
        def now(cls, tz=None):
            return frozen_now

    monkeypatch.setattr(app_module, "datetime", FrozenDateTime)
    monkeypatch.setattr(app_module, "_runtime_client", lambda service_name: clients[service_name])
    monkeypatch.setenv("aws_region", "us-east-1")
    monkeypatch.setenv("aws_endpoint", "https://example.invalid")
    monkeypatch.setenv("inventory_table_name", "inventory-table")
    monkeypatch.setenv("inventory_bucket_name", "inventory-bucket")

    response = app_module._collector_handler()

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {
        "services_written": 7,
        "snapshot_key": "inventory/latest.json",
    }

    put_item_calls = [
        kwargs
        for method_name, kwargs in clients["dynamodb"].method_calls
        if method_name == "put_item"
    ]
    assert len(put_item_calls) == 7
    for call in put_item_calls:
        assert call["TableName"] == "inventory-table"
        item = app_module._deserialize_item(call["Item"])
        assert item["pk"] == "account"
        assert item["sk"].startswith("service#")
        assert item["service"]
        assert item["collected_at"] == "2024-01-02T03:04:05Z"
        assert item["ttl"] == expected_ttl
        assert isinstance(item["counts"], dict)
        assert isinstance(item["sample"], dict)
        assert len(item["sample"]) <= 5

    put_object_calls = [
        kwargs
        for method_name, kwargs in clients["s3"].method_calls
        if method_name == "put_object"
    ]
    assert put_object_calls == [
        {
            "Bucket": "inventory-bucket",
            "Key": "inventory/latest.json",
            "Body": json.dumps(
                {
                    "collected_at": "2024-01-02T03:04:05Z",
                    "services": app_module._collect_inventory(),
                },
                sort_keys=True,
            ).encode("utf-8"),
            "ContentType": "application/json",
        }
    ]

    log_lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert len(log_lines) == 7
    assert {line["service"] for line in log_lines} == {
        "IAM",
        "EC2",
        "S3",
        "Lambda",
        "EventBridge",
        "RDS",
        "Glue",
    }
    for line in log_lines:
        assert line["collected_at"] == "2024-01-02T03:04:05Z"
        assert isinstance(line["counts"], dict)
        assert isinstance(line["sample"], dict)


def test_query_handler_lists_inventory_with_hard_limit_and_sorted_output(monkeypatch, capsys):
    clients = _runtime_clients()
    monkeypatch.setattr(app_module, "_runtime_client", lambda service_name: clients[service_name])
    monkeypatch.setenv("inventory_table_name", "inventory-table")

    response = app_module._query_handler({"path": "/inventory"})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert [item["service"] for item in body] == ["EC2", "S3"]
    assert clients["dynamodb"].method_calls[-1] == (
        "scan",
        {"TableName": "inventory-table", "Limit": 200},
    )

    assert json.loads(capsys.readouterr().out.strip()) == {
        "path": "/inventory",
        "status_code": 200,
    }


def test_query_handler_gets_single_service_entry_and_404s(monkeypatch, capsys):
    clients = _runtime_clients()
    monkeypatch.setattr(app_module, "_runtime_client", lambda service_name: clients[service_name])
    monkeypatch.setenv("inventory_table_name", "inventory-table")

    ok_response = app_module._query_handler(
        {"path": "/inventory/s3", "pathParameters": {"service": "s3"}}
    )
    assert ok_response["statusCode"] == 200
    assert json.loads(ok_response["body"]) == {
        "collected_at": "2024-01-02T03:04:05Z",
        "counts": {"buckets": 3},
        "pk": "account",
        "sample": {"first_bucket_name": "alpha-bucket"},
        "service": "S3",
        "sk": "service#S3",
        "ttl": 1704251045,
    }
    assert clients["dynamodb"].method_calls[-1] == (
        "get_item",
        {
            "TableName": "inventory-table",
            "Key": {"pk": {"S": "account"}, "sk": {"S": "service#S3"}},
        },
    )
    assert json.loads(capsys.readouterr().out.strip()) == {
        "path": "/inventory/s3",
        "status_code": 200,
    }

    clients["dynamodb"].methods["get_item"] = lambda **kwargs: {}
    not_found_response = app_module._query_handler(
        {"path": "/inventory/iam", "pathParameters": {"service": "iam"}}
    )
    assert not_found_response["statusCode"] == 404
    assert json.loads(not_found_response["body"]) == {"message": "Service not found"}
    assert json.loads(capsys.readouterr().out.strip()) == {
        "path": "/inventory/iam",
        "status_code": 404,
    }

    unknown_response = app_module._query_handler(
        {"path": "/inventory/unknown", "pathParameters": {"service": "unknown"}}
    )
    assert unknown_response["statusCode"] == 404
    assert json.loads(unknown_response["body"]) == {"message": "Service not found"}
    assert json.loads(capsys.readouterr().out.strip()) == {
        "path": "/inventory/unknown",
        "status_code": 404,
    }


def test_runtime_client_reads_env_vars_for_endpoint_and_region(monkeypatch):
    from unittest.mock import MagicMock

    monkeypatch.setenv("aws_region", "eu-west-2")
    monkeypatch.setenv("aws_endpoint", "https://custom.endpoint.example.com")

    captured = {}

    def fake_boto3_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return MagicMock()

    monkeypatch.setattr(app_module.boto3, "client", fake_boto3_client)

    app_module._runtime_client("s3")

    assert captured["kwargs"]["region_name"] == "eu-west-2"
    assert captured["kwargs"]["endpoint_url"] == "https://custom.endpoint.example.com"


def test_handler_routes_events_between_collector_and_query(monkeypatch):
    monkeypatch.setattr(app_module, "_collector_handler", lambda: {"handler": "collector"})
    monkeypatch.setattr(
        app_module,
        "_query_handler",
        lambda event: {"handler": "query", "path": event["path"]},
    )

    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "inventory-collector")
    assert app_module.handler({}, None) == {"handler": "collector"}

    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "inventory-query")
    assert app_module.handler({"path": "/inventory"}, None) == {
        "handler": "query",
        "path": "/inventory",
    }
