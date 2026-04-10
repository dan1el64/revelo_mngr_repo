import json
import os
import re
import sys
import textwrap
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
TF = MAIN_TF.read_text()


RESOURCE_RE = re.compile(r'resource\s+"([^"]+)"\s+"([^"]+)"\s*\{')
DATA_RE = re.compile(r'data\s+"([^"]+)"\s+"([^"]+)"\s*\{')


def block(prefix, *labels):
    quoted_labels = "".join(rf'\s+"{re.escape(label)}"' for label in labels)
    match = re.search(rf'{prefix}{quoted_labels}\s*\{{', TF)
    assert match, f"missing block: {prefix} {' '.join(labels)}"
    start = match.end() - 1
    depth = 0
    for index in range(start, len(TF)):
        if TF[index] == "{":
            depth += 1
        elif TF[index] == "}":
            depth -= 1
            if depth == 0:
                return TF[start : index + 1]
    raise AssertionError(f"unterminated block: {prefix} {' '.join(labels)}")


def resource_block(resource_type, name):
    return block("resource", resource_type, name)


def data_block(data_type, name):
    return block("data", data_type, name)


def provider_block(name):
    return block("provider", name)


def variable_block(name):
    return block("variable", name)


def resource_names(resource_type):
    return [name for found_type, name in RESOURCE_RE.findall(TF) if found_type == resource_type]


def assert_resource_count(resource_type, expected):
    names = resource_names(resource_type)
    assert len(names) == expected, f"{resource_type}: expected {expected}, found {names}"


def extract_heredoc(body, attribute):
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


def lambda_source():
    return textwrap.dedent(extract_heredoc(data_block("archive_file", "worker_zip"), "content"))


def load_lambda_module(client_factory):
    source = lambda_source()
    namespace = {"__name__": "lambda_function"}
    fake_boto3 = types.SimpleNamespace(client=client_factory)
    original_boto3 = sys.modules.get("boto3")
    sys.modules["boto3"] = fake_boto3
    try:
        exec(compile(source, "lambda_function.py", "exec"), namespace)
    finally:
        if original_boto3 is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = original_boto3
    return namespace


def with_env(**updates):
    previous = os.environ.copy()
    os.environ.update(updates)
    return previous


def restore_env(previous):
    os.environ.clear()
    os.environ.update(previous)


def iam_policy_blocks():
    return [
        resource_block("aws_iam_role_policy", "lambda_execution"),
        resource_block("aws_iam_role_policy", "step_functions_execution"),
        resource_block("aws_iam_role_policy", "pipes_execution"),
    ]


def test_single_terraform_file_and_expected_variables_only():
    assert sorted(path.name for path in ROOT.glob("*.tf")) == ["main.tf"]
    variables = set(re.findall(r'variable\s+"([^"]+)"', TF))
    assert {"aws_region", "aws_access_key_id", "aws_secret_access_key"}.issubset(variables)
    assert variables.issubset(
        {"aws_endpoint", "aws_region", "aws_access_key_id", "aws_secret_access_key"}
    )
    assert re.search(r'\bdefault\s*=\s*"us-east-1"', variable_block("aws_region"))


def test_aws_provider_is_parameterized_for_region_credentials_and_endpoint_override():
    assert TF.count('provider "aws"') == 1
    provider = provider_block("aws")
    for attribute, variable in [
        ("region", "aws_region"),
        ("access_key", "aws_access_key_id"),
        ("secret_key", "aws_secret_access_key"),
    ]:
        assert re.search(rf"\b{attribute}\s*=\s*var\.{variable}\b", provider)

    for service in [
        "cloudwatch",
        "ec2",
        "events",
        "iam",
        "lambda",
        "logs",
        "pipes",
        "rds",
        "s3",
        "secretsmanager",
        "sns",
        "sqs",
        "stepfunctions",
        "sts",
    ]:
        assert re.search(rf"\b{service}\s*=\s*var\.aws_endpoint\b", provider)


def test_network_layout_models_public_and_private_routing():
    for resource_type, expected in [
        ("aws_vpc", 1),
        ("aws_subnet", 4),
        ("aws_internet_gateway", 1),
        ("aws_eip", 1),
        ("aws_nat_gateway", 1),
        ("aws_route_table", 2),
        ("aws_route_table_association", 4),
        ("aws_vpc_endpoint", 1),
    ]:
        assert_resource_count(resource_type, expected)

    for subnet_name in ["public_a", "public_b"]:
        assert re.search(
            r"\bmap_public_ip_on_launch\s*=\s*true\b",
            resource_block("aws_subnet", subnet_name),
        )
    for subnet_name in ["private_a", "private_b"]:
        assert re.search(
            r"\bmap_public_ip_on_launch\s*=\s*false\b",
            resource_block("aws_subnet", subnet_name),
        )

    public_route_table = resource_block("aws_route_table", "public")
    private_route_table = resource_block("aws_route_table", "private")
    assert re.search(r'\bcidr_block\s*=\s*"0\.0\.0\.0/0"', public_route_table)
    assert re.search(r"\bgateway_id\s*=\s*aws_internet_gateway\.connectivity_mesh\.id\b", public_route_table)
    assert re.search(r'\bcidr_block\s*=\s*"0\.0\.0\.0/0"', private_route_table)
    assert re.search(r"\bnat_gateway_id\s*=\s*aws_nat_gateway\.public_a\.id\b", private_route_table)

    s3_endpoint = resource_block("aws_vpc_endpoint", "s3")
    route_table_ids = re.search(r"route_table_ids\s*=\s*\[(?P<body>.*?)\]", s3_endpoint, re.DOTALL)
    assert route_table_ids, "aws_vpc_endpoint.s3 must declare route_table_ids"
    assert set(re.findall(r"aws_route_table\.[^.]+\.id", route_table_ids.group("body"))) == {
        "aws_route_table.public.id",
        "aws_route_table.private.id",
    }


def test_security_groups_and_database_are_constrained_to_expected_access():
    lambda_sg = resource_block("aws_security_group", "lambda")
    rds_sg = resource_block("aws_security_group", "rds")
    db_instance = resource_block("aws_db_instance", "postgres")

    assert lambda_sg.count("ingress {") == 0
    assert lambda_sg.count("egress {") == 1
    assert re.search(r'\bprotocol\s*=\s*"-1"', lambda_sg)
    assert re.search(r'\bcidr_blocks\s*=\s*\["0\.0\.0\.0/0"\]', lambda_sg)

    ingress_sections = re.findall(r"ingress\s*\{.*?\n\s*\}", rds_sg, re.DOTALL)
    assert len(ingress_sections) == 1
    assert re.search(r"\bfrom_port\s*=\s*5432\b", ingress_sections[0])
    assert re.search(r"\bto_port\s*=\s*5432\b", ingress_sections[0])
    assert re.search(r'\bprotocol\s*=\s*"tcp"', ingress_sections[0])
    assert re.search(
        r"\bsecurity_groups\s*=\s*\[aws_security_group\.lambda\.id\]",
        ingress_sections[0],
    )
    assert re.search(r"\bpublicly_accessible\s*=\s*false\b", db_instance)
    assert re.search(r"\bstorage_encrypted\s*=\s*true\b", db_instance)


def test_archive_bucket_and_queues_enforce_encryption_transport_and_redrive():
    encryption = resource_block("aws_s3_bucket_server_side_encryption_configuration", "event_archive")
    public_access = resource_block("aws_s3_bucket_public_access_block", "event_archive")
    bucket_policy = resource_block("aws_s3_bucket_policy", "event_archive_tls")
    dead_letter_queue = resource_block("aws_sqs_queue", "dead_letter")
    primary_queue = resource_block("aws_sqs_queue", "primary")

    assert re.search(r'\bsse_algorithm\s*=\s*"AES256"', encryption)
    for setting in [
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ]:
        assert re.search(rf"\b{setting}\s*=\s*true\b", public_access)

    assert '"aws:SecureTransport" = "false"' in bucket_policy
    assert re.search(r"\bResource\s*=\s*\[\s*aws_s3_bucket\.event_archive\.arn,", bucket_policy, re.DOTALL)
    assert '"${aws_s3_bucket.event_archive.arn}/*"' in bucket_policy

    assert re.search(r"\bmessage_retention_seconds\s*=\s*1209600\b", dead_letter_queue)
    assert "redrive_policy" not in dead_letter_queue

    assert re.search(r"\bvisibility_timeout_seconds\s*=\s*60\b", primary_queue)
    assert re.search(r"\bmessage_retention_seconds\s*=\s*345600\b", primary_queue)
    assert re.search(r"\bsqs_managed_sse_enabled\s*=\s*true\b", primary_queue)
    assert "deadLetterTargetArn = aws_sqs_queue.dead_letter.arn" in primary_queue
    assert "maxReceiveCount     = 3" in primary_queue


def test_event_routing_resources_wire_bus_queue_pipe_and_state_machine():
    rule = resource_block("aws_cloudwatch_event_rule", "ingest_work_item")
    target = resource_block("aws_cloudwatch_event_target", "primary_queue")
    queue_policy = resource_block("aws_sqs_queue_policy", "allow_eventbridge_ingest_rule")
    state_machine = resource_block("aws_sfn_state_machine", "worker")
    pipe = resource_block("aws_pipes_pipe", "ingest")

    assert 'source        = ["app.ingest"]' in rule
    assert '"detail-type" = ["work-item"]' in rule
    assert re.search(r"\barn\s*=\s*aws_sqs_queue\.primary\.arn\b", target)
    assert 'Action   = "sqs:SendMessage"' in queue_policy
    assert '"aws:SourceArn" = aws_cloudwatch_event_rule.ingest_work_item.arn' in queue_policy

    assert 'FunctionName = aws_lambda_function.worker.arn' in state_machine

    assert re.search(r"\bsource\s*=\s*aws_sqs_queue\.primary\.arn\b", pipe)
    assert re.search(r"\benrichment\s*=\s*aws_lambda_function\.worker\.arn\b", pipe)
    assert re.search(r"\btarget\s*=\s*aws_sfn_state_machine\.worker\.arn\b", pipe)
    assert re.search(r"\bbatch_size\s*=\s*1\b", pipe)
    assert re.search(r"\bmaximum_batching_window_in_seconds\s*=\s*1\b", pipe)
    assert "aws_lambda_function.worker.arn" not in target
    assert "aws_sfn_state_machine.worker.arn" not in target


def test_lambda_source_derives_s3_key_and_reads_secret_at_runtime():
    captured = {}

    class FakeS3:
        def put_object(self, **kwargs):
            captured["s3_put"] = kwargs

    class FakeSecretsManager:
        def get_secret_value(self, SecretId):
            captured["secret_id"] = SecretId
            return {
                "SecretString": json.dumps(
                    {
                        "username": "ingest_admin",
                        "password": "super-secret",
                        "host": "db.internal",
                    }
                )
            }

    def fake_client(service_name):
        if service_name == "s3":
            return FakeS3()
        if service_name == "secretsmanager":
            return FakeSecretsManager()
        raise AssertionError(f"unexpected boto3 client: {service_name}")

    previous = with_env(
        BUCKET_NAME="archive-bucket",
        DB_SECRET_ARN="arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        DB_WRITE_MODE="disabled",
    )
    try:
        module = load_lambda_module(fake_client)
        result = module["handler"](
            {
                "payload": {"id": "evt-001", "kind": "demo"},
                "execution_id": "exec/segment",
                "timestamp": "2026-04-10T09:15:00Z",
            },
            types.SimpleNamespace(aws_request_id="unused"),
        )
    finally:
        restore_env(previous)

    assert captured["secret_id"].endswith(":secret:test")
    assert captured["s3_put"]["Bucket"] == "archive-bucket"
    assert captured["s3_put"]["Key"] == "executions/exec_segment/2026-04-10T09:15:00Z.json"
    assert json.loads(captured["s3_put"]["Body"]) == {
        "id": "evt-001",
        "payload": {"id": "evt-001", "kind": "demo"},
    }
    assert result["s3_key"] == "executions/exec_segment/2026-04-10T09:15:00Z.json"
    assert result["db_write"] == "disabled"


def test_lambda_source_uses_runtime_secret_to_open_database_connection():
    secret_value = {
        "username": "dbuser",
        "password": "dbpass",
        "host": "db.internal",
        "dbname": "appdb",
    }
    captured = {}

    class FakeS3:
        def put_object(self, **kwargs):
            captured["s3_put"] = kwargs

    class FakeSecretsManager:
        def get_secret_value(self, SecretId):
            captured["secret_id"] = SecretId
            return {"SecretString": json.dumps(secret_value)}

    def fake_client(service_name):
        if service_name == "s3":
            return FakeS3()
        if service_name == "secretsmanager":
            return FakeSecretsManager()
        raise AssertionError(f"unexpected boto3 client: {service_name}")

    previous = with_env(
        BUCKET_NAME="archive-bucket",
        DB_SECRET_ARN="arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        DB_WRITE_MODE="enabled",
    )
    try:
        module = load_lambda_module(fake_client)

        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        fake_connection = FakeConnection()

        def fake_connect(secret):
            captured["connected_with"] = secret
            return fake_connection

        def fake_execute(connection, event_id, payload):
            captured["execute"] = {
                "connection": connection,
                "event_id": event_id,
                "payload": payload,
            }

        def fake_send_message(connection, message_type, payload):
            captured.setdefault("messages", []).append((connection, message_type, payload))

        module["_connect_postgres"] = fake_connect
        module["_execute_postgres"] = fake_execute
        module["_send_message"] = fake_send_message

        result = module["handler"](
            {
                "payload": {"id": "evt-002", "kind": "db"},
                "execution_id": "exec-002",
                "timestamp": "2026-04-10T10:00:00Z",
            },
            types.SimpleNamespace(aws_request_id="unused"),
        )
    finally:
        restore_env(previous)

    assert captured["secret_id"].endswith(":secret:test")
    assert captured["connected_with"] == secret_value
    assert captured["execute"]["connection"] is fake_connection
    assert captured["execute"]["event_id"] == "evt-002"
    assert captured["execute"]["payload"] == {"id": "evt-002", "kind": "db"}
    assert any(message_type == b"X" for _, message_type, _ in captured["messages"])
    assert fake_connection.closed is True
    assert result["s3_key"] == "executions/exec-002/2026-04-10T10:00:00Z.json"


def test_lambda_environment_uses_secret_reference_without_inline_credentials():
    lambda_function = resource_block("aws_lambda_function", "worker")
    environment_block = re.search(r"environment\s*\{(?P<body>.*?)\n\s*\}", lambda_function, re.DOTALL)
    assert environment_block, "lambda function must declare an environment block"
    body = environment_block.group("body")
    for forbidden_pattern in [
        r"AKIA[0-9A-Z]{16}",
        r'password\s*=\s*"[^"]+"',
        r'username\s*=\s*"[^"]+"',
        r'aws_access_key_id\s*=\s*"[^"]+"',
        r'aws_secret_access_key\s*=\s*"[^"]+"',
    ]:
        assert not re.search(forbidden_pattern, body, re.IGNORECASE)


def test_iam_policies_avoid_service_wildcards_and_scope_sensitive_access():
    lambda_policy = resource_block("aws_iam_role_policy", "lambda_execution")
    joined_policies = "\n".join(iam_policy_blocks())

    assert 'Action   = "s3:PutObject"' in lambda_policy
    assert 'Resource = "${aws_s3_bucket.event_archive.arn}/*"' in lambda_policy
    assert 'Action   = "secretsmanager:GetSecretValue"' in lambda_policy
    assert "Resource = aws_secretsmanager_secret.db_credentials.arn" in lambda_policy
    assert 'Resource = "${aws_cloudwatch_log_group.lambda.arn}:*"' in lambda_policy
    for action in [
        "ec2:AssignPrivateIpAddresses",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:UnassignPrivateIpAddresses",
    ]:
        assert f'"{action}"' in lambda_policy
    assert re.search(r"\bResource\s*=\s*\"\*\"", lambda_policy)

    assert "data \"archive_file\" \"worker_zip\"" in TF
    assert DATA_RE.findall(TF) == [("archive_file", "worker_zip")]

    for service in ["lambda", "s3", "sqs", "states", "secretsmanager", "ec2", "logs", "iam"]:
        assert f'"{service}:*"' not in joined_policies


def test_only_expected_iam_statements_use_wildcard_resource():
    wildcard_locations = []
    for policy_name in ["lambda_execution", "step_functions_execution", "pipes_execution"]:
        policy = resource_block("aws_iam_role_policy", policy_name)
        if re.search(r'\bResource\s*=\s*"\*"', policy):
            wildcard_locations.append(policy_name)

    assert wildcard_locations == ["lambda_execution", "step_functions_execution"]


def test_step_functions_and_pipe_policies_target_only_their_runtime_dependencies():
    step_functions_policy = resource_block("aws_iam_role_policy", "step_functions_execution")
    pipes_policy = resource_block("aws_iam_role_policy", "pipes_execution")

    assert 'Action   = "lambda:InvokeFunction"' in step_functions_policy
    assert "Resource = aws_lambda_function.worker.arn" in step_functions_policy
    assert '"logs:CreateLogDelivery"' in step_functions_policy
    assert re.search(r"\bResource\s*=\s*\"\*\"", step_functions_policy)

    assert '"sqs:ReceiveMessage"' in pipes_policy
    assert '"sqs:DeleteMessage"' in pipes_policy
    assert '"sqs:GetQueueAttributes"' in pipes_policy
    assert "Resource = aws_sqs_queue.primary.arn" in pipes_policy
    assert 'Action   = "lambda:InvokeFunction"' in pipes_policy
    assert "Resource = aws_lambda_function.worker.arn" in pipes_policy
    assert 'Action   = "states:StartExecution"' in pipes_policy
    assert "Resource = aws_sfn_state_machine.worker.arn" in pipes_policy


def test_log_groups_keep_retention_without_customer_managed_kms_keys():
    for log_group_name in ["lambda", "step_functions"]:
        log_group = resource_block("aws_cloudwatch_log_group", log_group_name)
        assert re.search(r"\bretention_in_days\s*=\s*14\b", log_group)
        assert "kms_key_id" not in log_group


def test_observability_resources_track_failures_and_notify_via_sns():
    assert_resource_count("aws_sns_topic", 1)
    assert_resource_count("aws_sns_topic_subscription", 1)
    assert_resource_count("aws_cloudwatch_metric_alarm", 3)

    lambda_alarm = resource_block("aws_cloudwatch_metric_alarm", "lambda_errors")
    step_functions_alarm = resource_block("aws_cloudwatch_metric_alarm", "step_functions_failures")
    rds_alarm = resource_block("aws_cloudwatch_metric_alarm", "rds_cpu")

    for block_body, namespace, metric_name, threshold in [
        (lambda_alarm, "AWS/Lambda", "Errors", "1"),
        (step_functions_alarm, "AWS/States", "ExecutionsFailed", "1"),
        (rds_alarm, "AWS/RDS", "CPUUtilization", "80"),
    ]:
        assert f'namespace           = "{namespace}"' in block_body
        assert f'metric_name         = "{metric_name}"' in block_body
        assert re.search(r"\bperiod\s*=\s*300\b", block_body)
        assert re.search(r"\bevaluation_periods\s*=\s*1\b", block_body)
        assert re.search(rf"\bthreshold\s*=\s*{threshold}\b", block_body)
        assert "alarm_actions       = [aws_sns_topic.alarms.arn]" in block_body
