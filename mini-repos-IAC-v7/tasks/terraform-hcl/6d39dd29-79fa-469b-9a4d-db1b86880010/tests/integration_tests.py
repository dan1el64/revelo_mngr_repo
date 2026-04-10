"""Integration tests that verify deployed AWS resources via boto3."""

import json
import os
import struct
import sys
import textwrap
import time
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
from botocore.config import Config


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
STATE_JSON = ROOT / "state.json"


def load_state():
    if not STATE_JSON.exists():
        pytest.fail(
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


def maybe_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert len(matches) <= 1, f"Expected at most one {resource_type}.{name} in state.json"
    return matches[0]["values"] if matches else None


def using_custom_endpoint():
    return bool(
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
        or os.environ.get("TF_VAR_aws_endpoint")
    )


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


def eventually(callback, timeout=90, interval=2):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return callback()
        except Exception as exc:  # pragma: no cover
            last_error = exc
            time.sleep(interval)
    if last_error is not None:
        raise last_error
    raise AssertionError("eventually() reached an impossible state")


def as_list(value):
    if isinstance(value, list):
        return value
    return [value]


def statement_by_sid(policy_document, sid):
    for statement in as_list(policy_document["Statement"]):
        if statement.get("Sid") == sid:
            return statement
    raise AssertionError(f"Missing policy statement with Sid={sid}")


def extract_heredoc(body, attribute):
    import re

    match = re.search(rf"{attribute}\s*=\s*<<-?(?P<delimiter>[A-Z0-9_]+)\n", body)
    assert match, f"{attribute} must use a heredoc"
    delimiter = match.group("delimiter")
    lines = body[match.end() :].splitlines()
    content_lines = []
    for line in lines:
        if line.strip() == delimiter:
            return "\n".join(content_lines) + "\n"
        content_lines.append(line)
    raise AssertionError(f"unterminated heredoc for {attribute}")


def data_block(data_type, name):
    import re

    tf = MAIN_TF.read_text()
    match = re.search(rf'data\s+"{data_type}"\s+"{name}"\s*\{{', tf)
    assert match, f"missing data block: {data_type}.{name}"
    start = match.end() - 1
    depth = 0
    for index in range(start, len(tf)):
        if tf[index] == "{":
            depth += 1
        elif tf[index] == "}":
            depth -= 1
            if depth == 0:
                return tf[start : index + 1]
    raise AssertionError(f"unterminated data block: {data_type}.{name}")


def lambda_source():
    return textwrap.dedent(extract_heredoc(data_block("archive_file", "worker_zip"), "content"))


def load_lambda_module():
    source = lambda_source()
    namespace = {"__name__": "lambda_function"}
    original_boto3 = sys.modules.get("boto3")
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_args, **_kwargs: None)
    try:
        exec(compile(source, "lambda_function.py", "exec"), namespace)
    finally:
        if original_boto3 is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = original_boto3
    return namespace


def query_ingest_payload(db_host, secret, event_id):
    lambda_module = load_lambda_module()
    conn = lambda_module["_connect_postgres"]({**secret, "host": db_host})
    try:
        query = (
            "SELECT payload::text FROM ingest_events "
            "WHERE id = {};"
        ).format(lambda_module["_quote_literal"](event_id))
        lambda_module["_send_message"](conn, b"Q", query.encode("utf-8") + b"\x00")
        row_value = None
        while True:
            message_type, payload = lambda_module["_read_message"](conn)
            if message_type == b"E":
                raise RuntimeError(lambda_module["_error_message"](payload))
            if message_type == b"D":
                column_count = struct.unpack("!H", payload[:2])[0]
                cursor = 2
                values = []
                for _ in range(column_count):
                    length = struct.unpack("!I", payload[cursor : cursor + 4])[0]
                    cursor += 4
                    if length == 0xFFFFFFFF:
                        values.append(None)
                    else:
                        values.append(payload[cursor : cursor + length].decode("utf-8"))
                        cursor += length
                row_value = values[0]
            if message_type == b"Z":
                return None if row_value is None else json.loads(row_value)
    finally:
        lambda_module["_send_message"](conn, b"X", b"")
        conn.close()


def test_network_foundation_and_security_groups_are_live():
    vpc = single_resource_values("aws_vpc", "connectivity_mesh")
    public_rt = single_resource_values("aws_route_table", "public")
    private_rt = single_resource_values("aws_route_table", "private")
    endpoint = single_resource_values("aws_vpc_endpoint", "s3")
    lambda_sg = single_resource_values("aws_security_group", "lambda")
    rds_sg = single_resource_values("aws_security_group", "rds")

    ec2 = aws_client("ec2")

    ec2.describe_vpcs(VpcIds=[vpc["id"]])

    live_endpoint = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint["id"]])["VpcEndpoints"][0]
    assert live_endpoint["VpcEndpointType"] == "Gateway"
    assert live_endpoint["ServiceName"].endswith(".s3")
    assert set(live_endpoint["RouteTableIds"]) == {public_rt["id"], private_rt["id"]}

    live_lambda_sg = ec2.describe_security_groups(GroupIds=[lambda_sg["id"]])["SecurityGroups"][0]
    assert live_lambda_sg["IpPermissions"] == []
    assert len(live_lambda_sg["IpPermissionsEgress"]) == 1

    live_rds_sg = ec2.describe_security_groups(GroupIds=[rds_sg["id"]])["SecurityGroups"][0]
    assert len(live_rds_sg["IpPermissions"]) == 1
    ingress = live_rds_sg["IpPermissions"][0]
    assert ingress["IpProtocol"] == "tcp"
    assert ingress["FromPort"] == 5432
    assert ingress["ToPort"] == 5432
    assert ingress["UserIdGroupPairs"][0]["GroupId"] == lambda_sg["id"]


def test_storage_and_queue_protection_are_live():
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
    deny_transport = next(
        statement
        for statement in bucket_policy["Statement"]
        if statement.get("Sid") == "DenyInsecureTransport"
    )
    assert deny_transport["Action"] == "s3:*"
    assert set(as_list(deny_transport["Resource"])) == {bucket["arn"], f"{bucket['arn']}/*"}
    assert deny_transport["Condition"]["Bool"]["aws:SecureTransport"] == "false"

    sqs = aws_client("sqs")
    dead_letter_attributes = sqs.get_queue_attributes(
        QueueUrl=dead_letter_url,
        AttributeNames=["MessageRetentionPeriod", "RedrivePolicy"],
    )["Attributes"]
    assert dead_letter_attributes["MessageRetentionPeriod"] == "1209600"
    assert "RedrivePolicy" not in dead_letter_attributes

    primary_attributes = sqs.get_queue_attributes(
        QueueUrl=primary_url,
        AttributeNames=[
            "VisibilityTimeout",
            "MessageRetentionPeriod",
            "RedrivePolicy",
            "SqsManagedSseEnabled",
            "Policy",
        ],
    )["Attributes"]
    assert primary_attributes["VisibilityTimeout"] == "60"
    assert primary_attributes["MessageRetentionPeriod"] == "345600"
    assert primary_attributes["SqsManagedSseEnabled"] == "true"
    assert json.loads(primary_attributes["RedrivePolicy"]) == {
        "deadLetterTargetArn": dead_letter["arn"],
        "maxReceiveCount": 3,
    }


def test_eventbridge_rule_and_queue_permissions_are_live():
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
        lambda: events.list_targets_by_rule(Rule=rule["name"], EventBusName=bus["name"])["Targets"],
        timeout=60,
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
        for statement in policy["Statement"]
    )


def test_iam_roles_and_inline_policies_are_live_and_scoped():
    lambda_role = single_resource_values("aws_iam_role", "lambda")
    sfn_role = single_resource_values("aws_iam_role", "step_functions")
    pipes_role = single_resource_values("aws_iam_role", "pipes")
    lambda_policy = single_resource_values("aws_iam_role_policy", "lambda_execution")
    sfn_policy = single_resource_values("aws_iam_role_policy", "step_functions_execution")
    pipes_policy = single_resource_values("aws_iam_role_policy", "pipes_execution")
    lambda_log_group = single_resource_values("aws_cloudwatch_log_group", "lambda")
    bucket = single_resource_values("aws_s3_bucket", "event_archive")
    secret = single_resource_values("aws_secretsmanager_secret", "db_credentials")
    queue = single_resource_values("aws_sqs_queue", "primary")
    lambda_fn = single_resource_values("aws_lambda_function", "worker")
    state_machine = single_resource_values("aws_sfn_state_machine", "worker")

    iam = aws_client("iam")

    live_lambda_role = iam.get_role(RoleName=lambda_role["name"])["Role"]
    live_sfn_role = iam.get_role(RoleName=sfn_role["name"])["Role"]
    live_pipes_role = iam.get_role(RoleName=pipes_role["name"])["Role"]

    assert live_lambda_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "lambda.amazonaws.com"
    assert live_sfn_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "states.amazonaws.com"
    assert live_pipes_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "pipes.amazonaws.com"

    lambda_policy_document = iam.get_role_policy(
        RoleName=lambda_role["name"],
        PolicyName=lambda_policy["name"],
    )["PolicyDocument"]
    sfn_policy_document = iam.get_role_policy(
        RoleName=sfn_role["name"],
        PolicyName=sfn_policy["name"],
    )["PolicyDocument"]
    pipes_policy_document = iam.get_role_policy(
        RoleName=pipes_role["name"],
        PolicyName=pipes_policy["name"],
    )["PolicyDocument"]

    logs_statement = statement_by_sid(lambda_policy_document, "WriteOwnLogs")
    assert set(as_list(logs_statement["Action"])) == {"logs:CreateLogStream", "logs:PutLogEvents"}
    assert logs_statement["Resource"] == f"{lambda_log_group['arn']}:*"

    archive_statement = statement_by_sid(lambda_policy_document, "ArchivePayloads")
    assert archive_statement["Action"] == "s3:PutObject"
    assert archive_statement["Resource"] == f"{bucket['arn']}/*"

    secret_statement = statement_by_sid(lambda_policy_document, "ReadDatabaseSecret")
    assert secret_statement["Action"] == "secretsmanager:GetSecretValue"
    assert secret_statement["Resource"] == secret["arn"]

    eni_statement = statement_by_sid(lambda_policy_document, "ManageVpcNetworkInterfaces")
    assert set(as_list(eni_statement["Action"])) == {
        "ec2:AssignPrivateIpAddresses",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:UnassignPrivateIpAddresses",
    }
    assert eni_statement["Resource"] == "*"

    invoke_statement = statement_by_sid(sfn_policy_document, "InvokeWorkerLambda")
    assert invoke_statement["Action"] == "lambda:InvokeFunction"
    assert invoke_statement["Resource"] == lambda_fn["arn"]

    log_delivery_statement = statement_by_sid(sfn_policy_document, "WriteStepFunctionLogs")
    assert set(as_list(log_delivery_statement["Action"])) == {
        "logs:CreateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:DescribeLogGroups",
        "logs:DescribeResourcePolicies",
        "logs:GetLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:UpdateLogDelivery",
    }
    assert log_delivery_statement["Resource"] == "*"

    read_queue_statement = statement_by_sid(pipes_policy_document, "ReadPrimaryQueue")
    assert set(as_list(read_queue_statement["Action"])) == {
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ReceiveMessage",
    }
    assert read_queue_statement["Resource"] == queue["arn"]

    enrichment_statement = statement_by_sid(pipes_policy_document, "InvokeEnrichmentLambda")
    assert enrichment_statement["Action"] == "lambda:InvokeFunction"
    assert enrichment_statement["Resource"] == lambda_fn["arn"]

    start_execution_statement = statement_by_sid(pipes_policy_document, "StartWorkerStateMachine")
    assert start_execution_statement["Action"] == "states:StartExecution"
    assert start_execution_statement["Resource"] == state_machine["arn"]


def test_compute_workflow_observability_and_notifications_are_live():
    lambda_fn = single_resource_values("aws_lambda_function", "worker")
    lambda_log_group = single_resource_values("aws_cloudwatch_log_group", "lambda")
    state_machine = single_resource_values("aws_sfn_state_machine", "worker")
    sfn_log_group = single_resource_values("aws_cloudwatch_log_group", "step_functions")
    pipe = maybe_resource_values("aws_pipes_pipe", "ingest")
    topic = single_resource_values("aws_sns_topic", "alarms")
    lambda_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "lambda_errors")
    sfn_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "step_functions_failures")
    rds_alarm = single_resource_values("aws_cloudwatch_metric_alarm", "rds_cpu")

    lambda_client = aws_client("lambda")
    lambda_config = lambda_client.get_function_configuration(FunctionName=lambda_fn["function_name"])
    assert lambda_config["Runtime"] == "python3.11"
    assert lambda_config["MemorySize"] == 256
    assert lambda_config["Timeout"] == 20
    assert sorted(lambda_config["VpcConfig"]["SubnetIds"]) == sorted(lambda_fn["vpc_config"][0]["subnet_ids"])
    assert lambda_config["VpcConfig"]["SecurityGroupIds"] == lambda_fn["vpc_config"][0]["security_group_ids"]

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

    if pipe is not None:
        live_pipe = aws_client("pipes").describe_pipe(Name=pipe["name"])
        assert live_pipe["Source"] == pipe["source"]
        assert live_pipe["Enrichment"] == pipe["enrichment"]
        assert live_pipe["Target"] == pipe["target"]
        assert live_pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
        assert live_pipe["SourceParameters"]["SqsQueueParameters"]["MaximumBatchingWindowInSeconds"] == 1
        assert (
            live_pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
            == "FIRE_AND_FORGET"
        )
    elif not using_custom_endpoint():
        pytest.fail("Missing aws_pipes_pipe.ingest in state.json")

    sns = aws_client("sns")
    topic_attributes = sns.get_topic_attributes(TopicArn=topic["arn"])["Attributes"]
    assert topic_attributes["TopicArn"] == topic["arn"]
    subscriptions = eventually(
        lambda: sns.list_subscriptions_by_topic(TopicArn=topic["arn"])["Subscriptions"],
        timeout=60,
    )
    assert any(
        subscription["Protocol"] == "email"
        and subscription["Endpoint"] == "alerts@example.com"
        and subscription["SubscriptionArn"] != "PendingConfirmation"
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
    assert live_lambda_alarm["Period"] == 300
    assert live_lambda_alarm["EvaluationPeriods"] == 1
    assert live_lambda_alarm["Dimensions"] == [{"Name": "FunctionName", "Value": lambda_fn["function_name"]}]

    live_sfn_alarm = alarms_by_name[sfn_alarm["alarm_name"]]
    assert live_sfn_alarm["Namespace"] == "AWS/States"
    assert live_sfn_alarm["MetricName"] == "ExecutionsFailed"
    assert live_sfn_alarm["Threshold"] == 1.0
    assert live_sfn_alarm["Period"] == 300
    assert live_sfn_alarm["EvaluationPeriods"] == 1
    assert live_sfn_alarm["Dimensions"] == [{"Name": "StateMachineArn", "Value": state_machine["arn"]}]

    live_rds_alarm = alarms_by_name[rds_alarm["alarm_name"]]
    assert live_rds_alarm["Namespace"] == "AWS/RDS"
    assert live_rds_alarm["MetricName"] == "CPUUtilization"
    assert live_rds_alarm["Threshold"] == 80.0
    assert live_rds_alarm["Period"] == 300
    assert live_rds_alarm["EvaluationPeriods"] == 1
    assert len(live_rds_alarm["Dimensions"]) == 1
    assert live_rds_alarm["Dimensions"][0]["Name"] == "DBInstanceIdentifier"
    assert live_rds_alarm["Dimensions"][0]["Value"]


def test_rds_attributes_are_live_when_supported():
    db_instance = maybe_resource_values("aws_db_instance", "postgres")
    db_subnet_group = maybe_resource_values("aws_db_subnet_group", "rds")
    if db_instance is None or db_subnet_group is None:
        if using_custom_endpoint():
            assert db_instance is None
            assert db_subnet_group is None
            return
        pytest.fail("RDS resources are missing from state.json")

    rds = aws_client("rds")
    live_db = rds.describe_db_instances(DBInstanceIdentifier=db_instance["identifier"])["DBInstances"][0]
    assert live_db["Engine"] == "postgres"
    assert live_db["EngineVersion"] == "15.4"
    assert live_db["DBInstanceClass"] == "db.t3.micro"
    assert live_db["AllocatedStorage"] == 20
    assert live_db["StorageType"] == "gp2"
    assert live_db["MultiAZ"] is False
    assert live_db["PubliclyAccessible"] is False
    assert live_db["StorageEncrypted"] is True
    assert live_db["DBSubnetGroup"]["DBSubnetGroupName"] == db_subnet_group["name"]
    assert sorted(subnet["SubnetIdentifier"] for subnet in live_db["DBSubnetGroup"]["Subnets"]) == sorted(
        db_subnet_group["subnet_ids"]
    )


def test_end_to_end_event_flow_archives_payload_and_persists_database_row():
    pipe = maybe_resource_values("aws_pipes_pipe", "ingest")
    db_instance = maybe_resource_values("aws_db_instance", "postgres")
    db_subnet_group = maybe_resource_values("aws_db_subnet_group", "rds")
    bus = single_resource_values("aws_cloudwatch_event_bus", "ingest")
    bucket = single_resource_values("aws_s3_bucket", "event_archive")
    state_machine = single_resource_values("aws_sfn_state_machine", "worker")
    secret = single_resource_values("aws_secretsmanager_secret", "db_credentials")
    primary = single_resource_values("aws_sqs_queue", "primary")

    event_id = f"evt-{uuid.uuid4()}"
    payload = {
        "id": event_id,
        "kind": "end-to-end",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if pipe is None or db_instance is None or db_subnet_group is None:
        if not using_custom_endpoint():
            pytest.fail("Pipe and RDS resources are required outside custom-endpoint test environments")

        assert pipe is None
        assert db_instance is None
        assert db_subnet_group is None

        put_result = aws_client("events").put_events(
            Entries=[
                {
                    "EventBusName": bus["name"],
                    "Source": "app.ingest",
                    "DetailType": "work-item",
                    "Detail": json.dumps(payload),
                }
            ]
        )
        assert put_result["FailedEntryCount"] == 0

        sqs = aws_client("sqs")
        queue_url = primary.get("url") or primary["id"]

        def receive_eventbridge_message():
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
            )
            for message in response.get("Messages", []):
                body = json.loads(message["Body"])
                detail = body.get("detail")
                if detail == payload or detail == json.dumps(payload):
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
                    return body
            raise AssertionError("EventBridge message has not reached SQS yet")

        queued_event = eventually(receive_eventbridge_message, timeout=60, interval=2)
        assert queued_event["source"] == "app.ingest"
        assert queued_event["detail-type"] == "work-item"
        return

    bucket_name = bucket.get("bucket") or bucket["id"]
    sfn = aws_client("stepfunctions")
    existing_execution_arns = {
        execution["executionArn"]
        for execution in sfn.list_executions(stateMachineArn=state_machine["arn"], maxResults=100)["executions"]
    }
    put_result = aws_client("events").put_events(
        Entries=[
            {
                "EventBusName": bus["name"],
                "Source": "app.ingest",
                "DetailType": "work-item",
                "Detail": json.dumps(payload),
            }
        ]
    )
    assert put_result["FailedEntryCount"] == 0

    execution_arn = eventually(
        lambda: next(
            execution["executionArn"]
            for execution in sfn.list_executions(stateMachineArn=state_machine["arn"], maxResults=100)["executions"]
            if execution["executionArn"] not in existing_execution_arns
        ),
        timeout=120,
        interval=3,
    )

    execution = eventually(
        lambda: sfn.describe_execution(executionArn=execution_arn),
        timeout=120,
        interval=3,
    )
    while execution["status"] == "RUNNING":
        time.sleep(3)
        execution = sfn.describe_execution(executionArn=execution_arn)

    assert execution["status"] == "SUCCEEDED"
    output = json.loads(execution["output"])
    assert output["id"] == event_id
    assert output["payload"] == payload
    assert output["s3_key"].startswith("executions/")

    s3 = aws_client("s3")
    archived_object = json.loads(
        s3.get_object(Bucket=bucket_name, Key=output["s3_key"])["Body"].read().decode("utf-8")
    )
    assert archived_object == {"id": event_id, "payload": payload}

    live_db = aws_client("rds").describe_db_instances(DBInstanceIdentifier=db_instance["identifier"])["DBInstances"][0]
    db_host = live_db["Endpoint"]["Address"]
    secret_value = json.loads(aws_client("secretsmanager").get_secret_value(SecretId=secret["arn"])["SecretString"])

    try:
        persisted_payload = eventually(
            lambda: query_ingest_payload(db_host, secret_value, event_id),
            timeout=60,
            interval=3,
        )
    except OSError as exc:
        pytest.fail(f"Integration runner cannot reach the database endpoint: {exc}")

    assert persisted_payload == payload
