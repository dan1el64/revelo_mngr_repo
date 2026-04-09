"""Integration tests that verify deployed AWS resources via boto3."""

import json
import os
import time
from pathlib import Path

import boto3
import pytest
from botocore.config import Config


ROOT = Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"


def load_state():
    if not STATE_JSON.exists():
        pytest.skip(
            f"state.json not found at {STATE_JSON}. Run 'terraform apply' and "
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
    root_module = load_state()["values"]["root_module"]
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
    kwargs = {
        "service_name": service_name,
        "region_name": region,
        "endpoint_url": endpoint or None,
        "aws_access_key_id": (
            os.environ.get("AWS_ACCESS_KEY_ID")
            or os.environ.get("TF_VAR_aws_access_key_id")
            or "test"
        ),
        "aws_secret_access_key": (
            os.environ.get("AWS_SECRET_ACCESS_KEY")
            or os.environ.get("TF_VAR_aws_secret_access_key")
            or "test"
        ),
    }
    if service_name == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(**kwargs)


def eventually(callback, timeout=20, interval=1):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return callback()
        except Exception as exc:  # pragma: no cover - exercised through retries
            last_error = exc
            time.sleep(interval)
    if last_error is not None:
        raise last_error
    raise AssertionError("eventually() reached an impossible state")


def test_network_foundation_and_security_groups_are_live():
    vpc = single_resource_values("aws_vpc", "connectivity_mesh")
    public_rt = single_resource_values("aws_route_table", "public")
    private_rt = single_resource_values("aws_route_table", "private")
    endpoint = single_resource_values("aws_vpc_endpoint", "s3")
    lambda_sg = single_resource_values("aws_security_group", "lambda")
    rds_sg = single_resource_values("aws_security_group", "rds")

    ec2 = aws_client("ec2")

    live_vpc = ec2.describe_vpcs(VpcIds=[vpc["id"]])["Vpcs"][0]
    assert live_vpc["CidrBlock"] == "10.42.0.0/16"
    assert (
        ec2.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsHostnames")
        ["EnableDnsHostnames"]["Value"]
        is True
    )
    assert (
        ec2.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsSupport")
        ["EnableDnsSupport"]["Value"]
        is True
    )

    live_endpoint = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint["id"]])["VpcEndpoints"][0]
    assert live_endpoint["VpcEndpointType"] == "Gateway"
    assert live_endpoint["ServiceName"].endswith(".s3")
    assert set(live_endpoint["RouteTableIds"]) == {public_rt["id"], private_rt["id"]}

    live_lambda_sg = ec2.describe_security_groups(GroupIds=[lambda_sg["id"]])["SecurityGroups"][0]
    assert live_lambda_sg["IpPermissions"] == []

    live_rds_sg = ec2.describe_security_groups(GroupIds=[rds_sg["id"]])["SecurityGroups"][0]
    assert len(live_rds_sg["IpPermissions"]) == 1
    ingress = live_rds_sg["IpPermissions"][0]
    assert ingress["IpProtocol"] == "tcp"
    assert ingress["FromPort"] == 5432
    assert ingress["ToPort"] == 5432
    assert ingress["UserIdGroupPairs"][0]["GroupId"] == lambda_sg["id"]


def test_storage_and_messaging_resources_are_live():
    bucket = single_resource_values("aws_s3_bucket", "event_archive")
    dead_letter = single_resource_values("aws_sqs_queue", "dead_letter")
    primary = single_resource_values("aws_sqs_queue", "primary")

    bucket_name = bucket.get("bucket") or bucket["id"]
    dead_letter_url = dead_letter.get("url") or dead_letter["id"]
    primary_url = primary.get("url") or primary["id"]

    s3 = aws_client("s3")
    s3.head_bucket(Bucket=bucket_name)

    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    default_rule = encryption["ServerSideEncryptionConfiguration"]["Rules"][0]
    assert default_rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"

    public_access = s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"]
    assert public_access == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }

    bucket_policy = json.loads(s3.get_bucket_policy(Bucket=bucket_name)["Policy"])
    statements = bucket_policy["Statement"]
    assert any(
        statement.get("Sid") == "DenyInsecureTransport"
        and statement.get("Condition", {}).get("Bool", {}).get("aws:SecureTransport") == "false"
        and set(statement.get("Resource", [])) == {bucket["arn"], f"{bucket['arn']}/*"}
        for statement in statements
    )

    sqs = aws_client("sqs")
    dlq_attributes = sqs.get_queue_attributes(
        QueueUrl=dead_letter_url,
        AttributeNames=["MessageRetentionPeriod"],
    )["Attributes"]
    assert dlq_attributes["MessageRetentionPeriod"] == "1209600"

    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=primary_url,
        AttributeNames=[
            "VisibilityTimeout",
            "MessageRetentionPeriod",
            "RedrivePolicy",
            "SqsManagedSseEnabled",
            "Policy",
        ],
    )["Attributes"]
    assert queue_attributes["VisibilityTimeout"] == "60"
    assert queue_attributes["MessageRetentionPeriod"] == "345600"
    assert queue_attributes["SqsManagedSseEnabled"] == "true"

    redrive_policy = json.loads(queue_attributes["RedrivePolicy"])
    assert redrive_policy == {
        "deadLetterTargetArn": dead_letter["arn"],
        "maxReceiveCount": 3,
    }


def test_eventbridge_rule_and_queue_target_are_live():
    bus = single_resource_values("aws_cloudwatch_event_bus", "ingest")
    rule = single_resource_values("aws_cloudwatch_event_rule", "ingest_work_item")
    primary = single_resource_values("aws_sqs_queue", "primary")
    primary_url = primary.get("url") or primary["id"]

    events = aws_client("events")

    live_bus = events.describe_event_bus(Name=bus["name"])
    assert live_bus["Name"] == bus["name"]

    live_rule = events.describe_rule(Name=rule["name"], EventBusName=bus["name"])
    assert json.loads(live_rule["EventPattern"]) == {
        "source": ["app.ingest"],
        "detail-type": ["work-item"],
    }

    targets = eventually(
        lambda: events.list_targets_by_rule(Rule=rule["name"], EventBusName=bus["name"])["Targets"]
    )
    assert any(target["Arn"] == primary["arn"] for target in targets)

    sqs = aws_client("sqs")
    policy = json.loads(
        sqs.get_queue_attributes(QueueUrl=primary_url, AttributeNames=["Policy"])["Attributes"]["Policy"]
    )
    assert any(
        statement.get("Principal", {}).get("Service") == "events.amazonaws.com"
        and statement.get("Action") == "sqs:SendMessage"
        and statement.get("Resource") == primary["arn"]
        and statement.get("Condition", {}).get("ArnEquals", {}).get("aws:SourceArn") == rule["arn"]
        for statement in policy.get("Statement", [])
    )


def test_lambda_secret_state_machine_and_optional_pipe_are_live():
    bucket = single_resource_values("aws_s3_bucket", "event_archive")
    secret = single_resource_values("aws_secretsmanager_secret", "db_credentials")
    lambda_fn = single_resource_values("aws_lambda_function", "worker")
    lambda_log_group = single_resource_values("aws_cloudwatch_log_group", "lambda")
    state_machine = single_resource_values("aws_sfn_state_machine", "worker")
    sfn_log_group = single_resource_values("aws_cloudwatch_log_group", "step_functions")
    db_instance = optional_resource_values("aws_db_instance", "postgres")
    pipe = optional_resource_values("aws_pipes_pipe", "ingest")

    lambda_client = aws_client("lambda")
    lambda_config = lambda_client.get_function_configuration(FunctionName=lambda_fn["function_name"])

    assert lambda_config["Runtime"] == "python3.11"
    assert lambda_config["MemorySize"] == 256
    assert lambda_config["Timeout"] == 20
    assert sorted(lambda_config["VpcConfig"]["SubnetIds"]) == sorted(lambda_fn["vpc_config"][0]["subnet_ids"])
    assert lambda_config["VpcConfig"]["SecurityGroupIds"] == lambda_fn["vpc_config"][0]["security_group_ids"]

    environment = lambda_config["Environment"]["Variables"]
    assert environment["BUCKET_NAME"] == (bucket.get("bucket") or bucket["id"])
    assert environment["DB_SECRET_ARN"] == secret["arn"]
    assert environment["DB_WRITE_MODE"] == ("enabled" if db_instance else "disabled")

    secretsmanager = aws_client("secretsmanager")
    secret_value = json.loads(
        secretsmanager.get_secret_value(SecretId=secret["arn"])["SecretString"]
    )
    assert secret_value["username"] == "ingest_admin"
    assert secret_value["dbname"] == "appdb"
    assert secret_value["port"] == 5432
    assert secret_value["password"]

    logs = aws_client("logs")
    lambda_groups = logs.describe_log_groups(logGroupNamePrefix=lambda_log_group["name"])["logGroups"]
    step_function_groups = logs.describe_log_groups(logGroupNamePrefix=sfn_log_group["name"])["logGroups"]
    assert any(group["logGroupName"] == lambda_log_group["name"] and group["retentionInDays"] == 14 for group in lambda_groups)
    assert any(group["logGroupName"] == sfn_log_group["name"] and group["retentionInDays"] == 14 for group in step_function_groups)

    sfn = aws_client("stepfunctions")
    description = sfn.describe_state_machine(stateMachineArn=state_machine["arn"])
    definition = json.loads(description["definition"])
    invoke_worker = definition["States"]["InvokeWorker"]
    assert description["type"] == "STANDARD"
    assert invoke_worker["Parameters"]["FunctionName"] == lambda_fn["arn"]
    assert invoke_worker["Parameters"]["Payload"] == {
        "payload.$": "$",
        "execution_id.$": "$$.Execution.Id",
        "timestamp.$": "$$.State.EnteredTime",
    }

    if pipe:
        live_pipe = aws_client("pipes").describe_pipe(Name=pipe["name"])
        assert live_pipe["Source"] == pipe["source"]
        assert live_pipe["Target"] == pipe["target"]


def test_observability_and_notifications_are_live():
    topic = single_resource_values("aws_sns_topic", "alarms")
    lambda_fn = single_resource_values("aws_lambda_function", "worker")
    state_machine = single_resource_values("aws_sfn_state_machine", "worker")
    db_instance = optional_resource_values("aws_db_instance", "postgres")
    lambda_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "lambda_errors")
    sfn_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "step_functions_failures")
    rds_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "rds_cpu")

    sns = aws_client("sns")
    topic_attributes = sns.get_topic_attributes(TopicArn=topic["arn"])["Attributes"]
    assert topic_attributes["TopicArn"] == topic["arn"]

    subscriptions = eventually(lambda: sns.list_subscriptions_by_topic(TopicArn=topic["arn"])["Subscriptions"])
    assert any(
        subscription["Protocol"] == "email" and subscription["Endpoint"] == "alerts@example.com"
        for subscription in subscriptions
    )

    cloudwatch = aws_client("cloudwatch")
    live_alarms = cloudwatch.describe_alarms(
        AlarmNames=[
            lambda_alarm["alarm_name"],
            sfn_alarm["alarm_name"],
            rds_alarm["alarm_name"],
        ]
    )["MetricAlarms"]
    assert len(live_alarms) == 3

    alarms_by_name = {alarm["AlarmName"]: alarm for alarm in live_alarms}

    live_lambda_alarm = alarms_by_name[lambda_alarm["alarm_name"]]
    assert live_lambda_alarm["Namespace"] == "AWS/Lambda"
    assert live_lambda_alarm["MetricName"] == "Errors"
    assert live_lambda_alarm["Threshold"] == 1.0
    assert live_lambda_alarm["Dimensions"] == [{"Name": "FunctionName", "Value": lambda_fn["function_name"]}]

    live_sfn_alarm = alarms_by_name[sfn_alarm["alarm_name"]]
    assert live_sfn_alarm["Namespace"] == "AWS/States"
    assert live_sfn_alarm["MetricName"] == "ExecutionsFailed"
    assert live_sfn_alarm["Threshold"] == 1.0
    assert live_sfn_alarm["Dimensions"] == [{"Name": "StateMachineArn", "Value": state_machine["arn"]}]

    live_rds_alarm = alarms_by_name[rds_alarm["alarm_name"]]
    assert live_rds_alarm["Namespace"] == "AWS/RDS"
    assert live_rds_alarm["MetricName"] == "CPUUtilization"
    assert live_rds_alarm["Threshold"] == 80.0
    assert live_rds_alarm["Dimensions"] == [
        {
            "Name": "DBInstanceIdentifier",
            "Value": db_instance["identifier"] if db_instance else "pilot-landing-zone-postgres",
        }
    ]
