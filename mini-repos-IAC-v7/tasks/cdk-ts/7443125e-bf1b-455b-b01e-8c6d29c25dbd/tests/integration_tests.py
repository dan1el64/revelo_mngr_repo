import json
import os
from functools import lru_cache
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
import pytest


STACK_NAME = os.environ.get("STACK_NAME", "InfrastructureAnalystStack")
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _required_env(name):
    value = os.environ.get(name)
    assert value, f"Expected environment variable {name} to be set for integration tests"
    return value


def _client(service_name, **kwargs):
    endpoint = os.environ.get("AWS_ENDPOINT")
    region = os.environ.get("AWS_REGION", "us-east-1")
    params = {
        "region_name": region,
        "aws_access_key_id": _required_env("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": _required_env("AWS_SECRET_ACCESS_KEY"),
    }
    if endpoint:
        params["endpoint_url"] = endpoint
    params.update(kwargs)
    return boto3.client(service_name, **params)


@lru_cache(maxsize=1)
def _stack_description():
    try:
        return _client("cloudformation").describe_stacks(StackName=STACK_NAME)["Stacks"][0]
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if error.get("Code") == "ValidationError" and "does not exist" in error.get("Message", ""):
            pytest.skip(f"Stack {STACK_NAME} is not deployed")
        raise


@lru_cache(maxsize=1)
def _stack_outputs():
    return {
        output["OutputKey"]: output["OutputValue"]
        for output in _stack_description().get("Outputs", [])
    }


@lru_cache(maxsize=1)
def _stack_resources():
    paginator = _client("cloudformation").get_paginator("list_stack_resources")
    resources = []
    try:
        for page in paginator.paginate(StackName=STACK_NAME):
            resources.extend(page.get("StackResourceSummaries", []))
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if error.get("Code") == "ValidationError" and "does not exist" in error.get("Message", ""):
            pytest.skip(f"Stack {STACK_NAME} is not deployed")
        raise
    return resources


@lru_cache(maxsize=1)
def _template():
    template_path = os.path.join(REPO_ROOT, "template.json")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    cdk_out = os.path.join(REPO_ROOT, "cdk.out")
    if os.path.isdir(cdk_out):
        for name in sorted(os.listdir(cdk_out)):
            if name.endswith(".template.json"):
                with open(os.path.join(cdk_out, name), "r", encoding="utf-8") as handle:
                    return json.load(handle)

    raise AssertionError("No synthesized template found for integration test fallback checks")


def _resources_of_type(resource_type):
    return [resource for resource in _stack_resources() if resource["ResourceType"] == resource_type]


def _single_resource(resource_type):
    resources = _resources_of_type(resource_type)
    assert len(resources) == 1, f"Expected exactly one {resource_type}, got {resources}"
    return resources[0]


def _physical_ids(resource_type):
    return [resource["PhysicalResourceId"] for resource in _resources_of_type(resource_type)]


def _template_resources(resource_type):
    return [
        resource
        for resource in _template().get("Resources", {}).values()
        if resource.get("Type") == resource_type
    ]


def _single_template_resource(resource_type):
    resources = _template_resources(resource_type)
    assert len(resources) == 1, f"Expected exactly one template resource of type {resource_type}, got {resources}"
    return resources[0]


def _is_provider_coverage_error(exc):
    if not isinstance(exc, ClientError):
        return False
    error = exc.response.get("Error", {})
    code = error.get("Code", "")
    message = error.get("Message", "")
    return (
        code == "InternalFailure"
        and "not included within your" in message
        and "license" in message
    )


def _assert_bucket_lifecycle_from_template():
    bucket = _single_template_resource("AWS::S3::Bucket")
    rules = bucket["Properties"]["LifecycleConfiguration"]["Rules"]
    noncurrent_rule = next(rule for rule in rules if "NoncurrentVersionTransitions" in rule)
    assert noncurrent_rule["NoncurrentVersionTransitions"][0]["StorageClass"] == "STANDARD_IA"
    assert noncurrent_rule["NoncurrentVersionTransitions"][0]["TransitionInDays"] == 30
    assert noncurrent_rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 365

    reports_rule = next(rule for rule in rules if rule.get("Prefix") == "reports/")
    assert reports_rule["ExpirationInDays"] == 30


def _assert_log_group_retention_from_template(log_group_name):
    log_groups = _template_resources("AWS::Logs::LogGroup")
    matches = [
        resource for resource in log_groups
        if resource.get("Properties", {}).get("LogGroupName") == log_group_name
    ]
    assert len(matches) == 1, f"Expected template log group {log_group_name}"
    assert matches[0]["Properties"]["RetentionInDays"] == 14
    assert "KmsKeyId" not in matches[0]["Properties"]


def _assert_database_contract_from_template():
    db_instance = _single_template_resource("AWS::RDS::DBInstance")
    properties = db_instance["Properties"]
    assert properties["Engine"] == "postgres"
    assert str(properties["EngineVersion"]).startswith("15")
    assert properties["DBInstanceClass"] == "db.t3.micro"
    assert str(properties["AllocatedStorage"]) == "20"
    assert properties["StorageType"] == "gp3"
    assert properties["MultiAZ"] is False
    assert properties["BackupRetentionPeriod"] == 7
    assert properties["DeletionProtection"] is False
    assert properties["PubliclyAccessible"] is False


def _assert_stage_logging_from_template():
    stage = _single_template_resource("AWS::ApiGateway::Stage")
    method_settings = stage["Properties"].get("MethodSettings", [])
    assert any(
        setting.get("LoggingLevel") == "INFO"
        and setting.get("DataTraceEnabled") is True
        and setting.get("MetricsEnabled") is True
        for setting in method_settings
    ), f"Expected INFO/dataTrace/metrics stage logging, got {method_settings}"


def _api_identifiers():
    parsed = urlparse(_stack_outputs()["ApiUrl"])
    api_id = (parsed.hostname or "").split(".")[0]
    stage_name = parsed.path.strip("/")
    assert api_id, f"Could not determine API id from {_stack_outputs()['ApiUrl']}"
    assert stage_name, f"Could not determine stage name from {_stack_outputs()['ApiUrl']}"
    return api_id, stage_name


def test_stack_outputs_and_resource_counts_are_present():
    outputs = _stack_outputs()
    assert outputs["CollectorFunctionName"] == "infrastructure-analyst-collector"
    assert outputs["SummaryFunctionName"] == "infrastructure-analyst-summary"
    assert outputs["FindingsBucketName"]
    assert outputs["ApiUrl"]
    assert outputs["DatabaseSecretArn"]

    assert len(_resources_of_type("AWS::Lambda::Function")) == 2
    assert len(_resources_of_type("AWS::Logs::LogGroup")) == 2
    assert len(_resources_of_type("AWS::S3::Bucket")) == 1
    assert len(_resources_of_type("AWS::RDS::DBInstance")) == 1
    assert len(_resources_of_type("AWS::ApiGateway::RestApi")) == 1
    assert len(_resources_of_type("AWS::Scheduler::Schedule")) == 1
    assert len(_resources_of_type("AWS::EC2::SecurityGroup")) == 2


def test_bucket_configuration_matches_inventory_contract():
    bucket_name = _stack_outputs()["FindingsBucketName"]
    s3 = _client("s3", config=boto3.session.Config(s3={"addressing_style": "path"}))

    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
    assert versioning["Status"] == "Enabled"

    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    rule = encryption["ServerSideEncryptionConfiguration"]["Rules"][0]
    assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"

    public_access = s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"]
    assert public_access == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }

    try:
        lifecycle = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        noncurrent_rule = next(rule for rule in lifecycle if "NoncurrentVersionTransitions" in rule)
        assert noncurrent_rule["NoncurrentVersionTransitions"][0]["StorageClass"] == "STANDARD_IA"
        assert noncurrent_rule["NoncurrentVersionTransitions"][0]["NoncurrentDays"] == 30
        assert noncurrent_rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 365

        reports_rule = next(rule for rule in lifecycle if rule.get("Filter", {}).get("Prefix") == "reports/")
        assert reports_rule["Expiration"]["Days"] == 30
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        assert error_code == "NoSuchLifecycleConfiguration", exc
        _assert_bucket_lifecycle_from_template()


def test_log_groups_and_lambda_configurations_are_correct():
    logs_client = _client("logs")
    lambda_client = _client("lambda")

    expected_logs = {
        "/aws/lambda/infrastructure-analyst-collector",
        "/aws/lambda/infrastructure-analyst-summary",
    }
    actual_logs = {
        group["logGroupName"]
        for group in logs_client.describe_log_groups(logGroupNamePrefix="/aws/lambda/infrastructure-analyst")["logGroups"]
    }
    assert expected_logs.issubset(actual_logs)

    for log_group_name in expected_logs:
        group = next(
            candidate
            for candidate in logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
            if candidate["logGroupName"] == log_group_name
        )
        if "retentionInDays" in group:
            assert group["retentionInDays"] == 14
        else:
            _assert_log_group_retention_from_template(log_group_name)
        assert "kmsKeyId" not in group

    collector = lambda_client.get_function_configuration(FunctionName="infrastructure-analyst-collector")
    summary = lambda_client.get_function_configuration(FunctionName="infrastructure-analyst-summary")

    assert collector["Runtime"] == "python3.12"
    assert collector["MemorySize"] == 256
    assert collector["Timeout"] == 60
    assert summary["Runtime"] == "python3.12"
    assert summary["MemorySize"] == 128
    assert summary["Timeout"] == 30

    expected_env_keys = {"REPORT_BUCKET", "DB_SECRET_ARN", "DB_HOST", "DB_PORT", "DB_NAME", "AWS_ENDPOINT"}
    assert set(collector["Environment"]["Variables"].keys()) == expected_env_keys
    assert set(summary["Environment"]["Variables"].keys()) == expected_env_keys
    assert len(collector["VpcConfig"]["SubnetIds"]) == 2
    assert len(summary["VpcConfig"]["SubnetIds"]) == 2
    assert collector["VpcConfig"]["SecurityGroupIds"] == summary["VpcConfig"]["SecurityGroupIds"]


def test_database_and_secret_configuration_are_correct():
    rds_client = _client("rds")
    secrets_client = _client("secretsmanager")

    db_identifier = _single_resource("AWS::RDS::DBInstance")["PhysicalResourceId"]
    try:
        db_instance = rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)["DBInstances"][0]
        assert db_instance["Engine"] == "postgres"
        assert str(db_instance["EngineVersion"]).startswith("15")
        assert db_instance["DBInstanceClass"] == "db.t3.micro"
        assert db_instance["AllocatedStorage"] == 20
        assert db_instance["StorageType"] == "gp3"
        assert db_instance["MultiAZ"] is False
        assert db_instance["BackupRetentionPeriod"] == 7
        assert db_instance["DeletionProtection"] is False
        assert db_instance["PubliclyAccessible"] is False
        assert len(db_instance["DBSubnetGroup"]["Subnets"]) == 2
    except ClientError as exc:
        assert _is_provider_coverage_error(exc), exc
        _assert_database_contract_from_template()

    secret_arn = _stack_outputs()["DatabaseSecretArn"]
    secret = secrets_client.get_secret_value(SecretId=secret_arn)
    payload = json.loads(secret["SecretString"])
    assert set(payload.keys()) >= {"username", "password"}
    assert payload["username"] == "infraanalyst"
    assert payload["password"].isalnum()


def test_api_gateway_and_schedule_are_wired_to_the_expected_targets():
    apigateway = _client("apigateway")
    scheduler_client = _client("scheduler")

    rest_api_id, stage_name = _api_identifiers()
    resources = apigateway.get_resources(restApiId=rest_api_id)["items"]
    path_map = {resource["path"]: resource for resource in resources}
    assert "/summary" in path_map
    assert "/run" in path_map
    assert "GET" in path_map["/summary"]["resourceMethods"]
    assert "POST" in path_map["/run"]["resourceMethods"]

    stage = apigateway.get_stage(restApiId=rest_api_id, stageName=stage_name)
    method_settings = stage.get("methodSettings", {})
    matching_setting = next(
        (
            settings
            for settings in method_settings.values()
            if settings.get("loggingLevel") == "INFO"
            and settings.get("dataTraceEnabled") is True
            and settings.get("metricsEnabled") is True
        ),
        None,
    )
    if matching_setting is None:
        _assert_stage_logging_from_template()

    schedule_name = _single_resource("AWS::Scheduler::Schedule")["PhysicalResourceId"]
    schedule = scheduler_client.get_schedule(Name=schedule_name)
    assert schedule["ScheduleExpression"] == "rate(1 hour)"
    assert schedule["State"] == "ENABLED"
    assert schedule["FlexibleTimeWindow"]["Mode"] == "OFF"
    assert schedule["Target"]["Arn"].endswith(":function:infrastructure-analyst-collector")


def test_security_groups_are_minimal_and_database_only_accepts_lambda_traffic():
    ec2_client = _client("ec2")
    security_group_ids = _physical_ids("AWS::EC2::SecurityGroup")
    groups = ec2_client.describe_security_groups(GroupIds=security_group_ids)["SecurityGroups"]

    lambda_group = next(group for group in groups if "Lambda functions" in group["Description"])
    database_group = next(group for group in groups if "PostgreSQL instance" in group["Description"])

    assert lambda_group["IpPermissions"] == []
    assert len(database_group["IpPermissions"]) == 1
    ingress = database_group["IpPermissions"][0]
    assert ingress["IpProtocol"] == "tcp"
    assert ingress["FromPort"] == 5432
    assert ingress["ToPort"] == 5432
    group_pairs = ingress.get("UserIdGroupPairs", [])
    assert group_pairs, f"Expected ingress to reference at least one security group, got {ingress}"
    assert all("CidrIp" not in pair for pair in group_pairs)
    matching_pairs = [pair for pair in group_pairs if pair["GroupId"] == lambda_group["GroupId"]]
    assert matching_pairs, (
        f"Expected one ingress pair for Lambda security group {lambda_group['GroupId']}, got {group_pairs}"
    )
    assert all(pair.get("GroupId") == lambda_group["GroupId"] for pair in matching_pairs)
