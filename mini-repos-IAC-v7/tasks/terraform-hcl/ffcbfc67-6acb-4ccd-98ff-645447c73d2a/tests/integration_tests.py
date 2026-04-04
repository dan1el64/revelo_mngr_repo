"""Integration tests that verify deployed resources through boto3."""

import json
import os
from pathlib import Path

import boto3
import pytest


ROOT = Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"


def load_state():
    if not STATE_JSON.exists():
        pytest.skip(
            f"state.json not found at {STATE_JSON}. Run 'terraform apply' and then "
            "'terraform show -json > state.json' before integration tests."
        )

    state = json.loads(STATE_JSON.read_text())
    root_module = state.get("values", {}).get("root_module")
    if root_module is None:
        pytest.fail("state.json is missing values.root_module")
    return state


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def state_resources():
    state = load_state()
    root_module = state["values"]["root_module"]
    return list(iter_resources(root_module))


def matching_resources(resource_type, name=None):
    matches = [resource for resource in state_resources() if resource.get("type") == resource_type]
    if name is not None:
        matches = [resource for resource in matches if resource.get("name") == name]
    return matches


def single_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert matches, f"Missing {resource_type}.{name} in state.json"
    assert len(matches) == 1, f"Expected a single {resource_type}.{name} in state.json"
    return matches[0]["values"]


def optional_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert len(matches) <= 1, f"Expected at most one {resource_type}.{name} in state.json"
    return matches[0]["values"] if matches else None


def aws_client(service_name):
    endpoint = (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
        or os.environ.get("TF_VAR_aws_endpoint")
    )
    region = os.environ.get("AWS_REGION") or os.environ.get("TF_VAR_aws_region") or "us-east-1"
    return boto3.client(
        service_name,
        region_name=region,
        endpoint_url=endpoint or None,
        aws_access_key_id=(
            os.environ.get("AWS_ACCESS_KEY_ID")
            or os.environ.get("TF_VAR_aws_access_key_id")
            or "test"
        ),
        aws_secret_access_key=(
            os.environ.get("AWS_SECRET_ACCESS_KEY")
            or os.environ.get("TF_VAR_aws_secret_access_key")
            or "test"
        ),
    )


def test_queue_and_secret_are_live():
    queue = single_resource_values("aws_sqs_queue", "intake")
    secret = single_resource_values("aws_secretsmanager_secret", "database_credentials")

    sqs = aws_client("sqs")
    queue_url = queue.get("url") or queue.get("id")
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod", "Policy"],
    )["Attributes"]

    assert attributes["VisibilityTimeout"] == "60"
    assert attributes["MessageRetentionPeriod"] == "1209600"
    if "Policy" in attributes:
        assert "sqs:SendMessage" in attributes["Policy"]
        assert queue["arn"] in attributes["Policy"]

    secretsmanager = aws_client("secretsmanager")
    secret_value = secretsmanager.get_secret_value(SecretId=secret["arn"])
    payload = json.loads(secret_value["SecretString"])
    assert payload["username"] == "payments_app"
    assert payload["password"]


def test_lambda_configurations_and_log_groups_are_live():
    worker = single_resource_values("aws_lambda_function", "worker")
    health = single_resource_values("aws_lambda_function", "health")
    enrichment = single_resource_values("aws_lambda_function", "enrichment")
    application_logs = single_resource_values("aws_cloudwatch_log_group", "application")
    api_access_logs = single_resource_values("aws_cloudwatch_log_group", "api_access")

    lambda_client = aws_client("lambda")

    worker_cfg = lambda_client.get_function_configuration(FunctionName=worker["function_name"])
    health_cfg = lambda_client.get_function_configuration(FunctionName=health["function_name"])
    enrichment_cfg = lambda_client.get_function_configuration(FunctionName=enrichment["function_name"])

    for cfg in [worker_cfg, health_cfg, enrichment_cfg]:
        assert cfg["Runtime"] == "python3.12"
        assert cfg["Handler"] == "app.handler"
        assert cfg["MemorySize"] == 256
        assert cfg["Timeout"] == 15

    worker_vpc = worker_cfg.get("VpcConfig", {})
    assert sorted(worker_vpc.get("SubnetIds", [])) == sorted(worker["vpc_config"][0]["subnet_ids"])
    assert sorted(worker_vpc.get("SecurityGroupIds", [])) == sorted(worker["vpc_config"][0]["security_group_ids"])

    assert health_cfg.get("VpcConfig", {}).get("SubnetIds", []) == []
    assert health_cfg.get("VpcConfig", {}).get("SecurityGroupIds", []) == []
    assert enrichment_cfg.get("VpcConfig", {}).get("SubnetIds", []) == []
    assert enrichment_cfg.get("VpcConfig", {}).get("SecurityGroupIds", []) == []

    logs = aws_client("logs")
    for log_group in [application_logs["name"], api_access_logs["name"]]:
        groups = logs.describe_log_groups(logGroupNamePrefix=log_group)["logGroups"]
        match = next((group for group in groups if group["logGroupName"] == log_group), None)
        assert match is not None, f"Missing live log group {log_group}"
        assert match["retentionInDays"] == 14
        assert "kmsKeyId" not in match


def test_state_machine_and_optional_live_resources_are_queryable():
    state_machine = single_resource_values("aws_sfn_state_machine", "processing")

    sfn = aws_client("stepfunctions")
    description = sfn.describe_state_machine(stateMachineArn=state_machine["arn"])
    assert description["type"] == "STANDARD"
    assert "Prepare" in description["definition"]
    assert "Complete" in description["definition"]

    api = optional_resource_values("aws_apigatewayv2_api", "front_door")
    stage = optional_resource_values("aws_apigatewayv2_stage", "default")
    if api and stage:
        apigateway = aws_client("apigatewayv2")
        live_api = apigateway.get_api(ApiId=api["api_id"])
        live_stage = apigateway.get_stage(ApiId=api["api_id"], StageName=stage["name"])
        assert live_api["ProtocolType"] == "HTTP"
        assert live_stage["AutoDeploy"] is True

    db_instance = optional_resource_values("aws_db_instance", "storage_layer")
    if db_instance:
        rds = aws_client("rds")
        live_db = rds.describe_db_instances(DBInstanceIdentifier=db_instance["identifier"])["DBInstances"][0]
        assert live_db["Engine"] == "postgres"
        assert live_db["DBInstanceClass"] == "db.t3.micro"
        assert live_db["PubliclyAccessible"] is False

    pipe = optional_resource_values("aws_pipes_pipe", "processing")
    if pipe:
        pipes = aws_client("pipes")
        live_pipe = pipes.describe_pipe(Name=pipe["name"])
        assert live_pipe["CurrentState"] == "RUNNING"
        assert live_pipe["Source"] == pipe["source"]
        assert live_pipe["Target"] == pipe["target"]
