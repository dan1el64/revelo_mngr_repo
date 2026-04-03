"""Integration tests that query deployed resources via boto3."""

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
            f"state.json not found at {STATE_JSON}. Run 'terraform apply' and 'terraform show -json > state.json' first."
        )
    return json.loads(STATE_JSON.read_text())


def all_resources(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(all_resources(child))
    return resources


def state_resources():
    state = load_state()
    root_module = state.get("values", {}).get("root_module")
    if root_module is None:
        pytest.fail("state.json is missing values.root_module")
    return all_resources(root_module)


def matching_resources(resource_type, name):
    return [
        resource
        for resource in state_resources()
        if resource.get("type") == resource_type and resource.get("name") == name
    ]


def single_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert matches, f"Missing {resource_type}.{name} in state.json"
    assert len(matches) == 1, f"Expected a single {resource_type}.{name} in state.json"
    return matches[0]["values"]


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
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("TF_VAR_aws_access_key_id") or "test",
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
        or os.environ.get("TF_VAR_aws_secret_access_key")
        or "test",
    )


def test_queue_and_eventbridge_rule_are_live():
    queue = single_resource_values("aws_sqs_queue", "intake")
    event_rule = single_resource_values("aws_cloudwatch_event_rule", "intake_requested")

    queue_url = queue.get("url") or queue.get("id")
    assert queue_url, "Queue URL must be present in state.json"

    sqs = aws_client("sqs")
    queue_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod", "Policy"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "60"
    assert queue_attrs["MessageRetentionPeriod"] == "345600"

    policy = json.loads(queue_attrs["Policy"])
    statements = policy.get("Statement", [])
    assert any(
        statement.get("Principal", {}).get("Service") == "events.amazonaws.com"
        and statement.get("Action") == "sqs:SendMessage"
        and statement.get("Resource") == queue["arn"]
        for statement in statements
    )

    events = aws_client("events")
    targets = events.list_targets_by_rule(
        Rule=event_rule["name"],
        EventBusName=event_rule.get("event_bus_name", "default"),
    )["Targets"]
    assert any(target["Arn"] == queue["arn"] for target in targets)


def test_lambda_functions_and_logs_are_live():
    enrichment = single_resource_values("aws_lambda_function", "enrichment")
    validation = single_resource_values("aws_lambda_function", "validation")
    enrichment_log_group = single_resource_values("aws_cloudwatch_log_group", "enrichment")
    validation_log_group = single_resource_values("aws_cloudwatch_log_group", "validation")

    lambda_client = aws_client("lambda")
    enrichment_cfg = lambda_client.get_function_configuration(FunctionName=enrichment["function_name"])
    validation_cfg = lambda_client.get_function_configuration(FunctionName=validation["function_name"])

    assert enrichment_cfg["Runtime"] == "python3.12"
    assert enrichment_cfg["MemorySize"] == 256
    assert enrichment_cfg["Timeout"] == 10
    assert sorted(enrichment_cfg["VpcConfig"]["SubnetIds"]) == sorted(enrichment["vpc_config"][0]["subnet_ids"])
    assert enrichment_cfg["VpcConfig"]["SecurityGroupIds"] == enrichment["vpc_config"][0]["security_group_ids"]

    assert validation_cfg["Runtime"] == "python3.12"
    assert validation_cfg["MemorySize"] == 256
    assert validation_cfg["Timeout"] == 15
    env_vars = validation_cfg["Environment"]["Variables"]
    assert env_vars["SECRET_ARN"]
    assert env_vars["DB_HOST"]

    logs = aws_client("logs")
    log_groups = {
        group["logGroupName"]: group
        for group in logs.describe_log_groups(logGroupNamePrefix="/aws/")["logGroups"]
    }
    assert log_groups[enrichment_log_group["name"]]["retentionInDays"] == 14
    assert log_groups[validation_log_group["name"]]["retentionInDays"] == 14


def test_secret_and_state_machine_are_live():
    secret = single_resource_values("aws_secretsmanager_secret", "database")
    secret_version = single_resource_values("aws_secretsmanager_secret_version", "database")
    state_machine = single_resource_values("aws_sfn_state_machine", "processing")
    step_functions_log_group = single_resource_values("aws_cloudwatch_log_group", "step_functions")

    secretsmanager = aws_client("secretsmanager")
    secret_value = secretsmanager.get_secret_value(SecretId=secret["arn"])
    payload = json.loads(secret_value["SecretString"])
    assert payload["username"] == "intake_admin"
    assert payload["password"]
    assert secret_version["secret_string"]

    sfn = aws_client("stepfunctions")
    description = sfn.describe_state_machine(stateMachineArn=state_machine["arn"])
    assert description["type"] == "STANDARD"
    assert description["loggingConfiguration"]["level"] == "ALL"
    assert description["loggingConfiguration"]["includeExecutionData"] is True
    assert step_functions_log_group["arn"] in description["loggingConfiguration"]["destinations"][0]["cloudWatchLogsLogGroup"][
        "logGroupArn"
    ]
    assert "ValidationFailed" in description["definition"]
