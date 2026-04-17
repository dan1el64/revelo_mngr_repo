import json
import logging
import os
import re
import sys
import textwrap
import types
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
from botocore.stub import Stubber


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
TF = MAIN_TF.read_text()

def provider_block(name):
    return block("provider", name)


def variable_block(name):
    return block("variable", name)


def resource_names(resource_type):
    return re.findall(rf'^resource "{re.escape(resource_type)}" "([^"]+)"', TF, re.MULTILINE)


def variable_names():
    return re.findall(r'^variable "([^"]+)"', TF, re.MULTILINE)


def all_resource_blocks(resource_type):
    return [resource_block(resource_type, name) for name in resource_names(resource_type)]


def count_pattern(pattern):
    return len(re.findall(pattern, TF, re.MULTILINE))


def block(block_type, *labels):
    label_expr = "".join(rf'\s+"{re.escape(label)}"' for label in labels)
    match = re.search(rf"^{block_type}{label_expr}\s*\{{", TF, re.MULTILINE)
    assert match, f"Missing {block_type} {' '.join(labels)}"
    start = match.end() - 1
    depth = 0
    for index in range(start, len(TF)):
        if TF[index] == "{":
            depth += 1
        elif TF[index] == "}":
            depth -= 1
            if depth == 0:
                return TF[start : index + 1]
    raise AssertionError(f"Unterminated {block_type} {' '.join(labels)}")


def resource_block(resource_type, name):
    return block("resource", resource_type, name)


def data_block(data_type, name):
    return block("data", data_type, name)


def extract_heredoc(text, *attributes):
    for attribute in attributes:
        match = re.search(rf"{attribute}\s*=\s*<<-?(?P<marker>[A-Z0-9_]+)\n", text)
        if not match:
            continue
        marker = match.group("marker")
        lines = text[match.end() :].splitlines()
        content = []
        for line in lines:
            if line.strip() == marker:
                return "\n".join(content)
            content.append(line)
    raise AssertionError(f"Expected heredoc for one of: {', '.join(attributes)}")


def lambda_source_for(lambda_resource_name):
    lambda_block = resource_block("aws_lambda_function", lambda_resource_name)
    archive_match = re.search(r"data\.archive_file\.([^.]+)\.", lambda_block)
    assert archive_match, f"{lambda_resource_name} must be packaged with archive_file"
    archive_name = archive_match.group(1)
    archive_block = data_block("archive_file", archive_name)
    source = extract_heredoc(archive_block, "source_content", "content")
    return textwrap.dedent(source)


class FakeLogger:
    def __init__(self, name="lambda-tests"):
        self._logger = logging.getLogger(name)

    def info(self, message, *args, **kwargs):
        self._logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self._logger.error(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self._logger.warning(message, *args, **kwargs)


@contextmanager
def patched_environment(**values):
    previous = os.environ.copy()
    os.environ.update(values)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def load_lambda_module(lambda_resource_name, boto3_client_factory, psycopg2_module=None):
    source = lambda_source_for(lambda_resource_name)
    namespace = {"__name__": f"{lambda_resource_name}_module"}
    originals = {}
    fake_modules = {
        "boto3": types.SimpleNamespace(client=boto3_client_factory),
        "aws_lambda_powertools": types.SimpleNamespace(Logger=lambda *args, **kwargs: FakeLogger()),
    }
    if psycopg2_module is not None:
        fake_modules["psycopg2"] = psycopg2_module

    try:
        for module_name, module_value in fake_modules.items():
            originals[module_name] = sys.modules.get(module_name)
            sys.modules[module_name] = module_value
        exec(compile(source, f"{lambda_resource_name}.py", "exec"), namespace)
    except SyntaxError as exc:
        pytest.fail(f"Inline code for {lambda_resource_name} is not valid Python: {exc}")
    finally:
        for module_name, previous in originals.items():
            if previous is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = previous

    assert "handler" in namespace, f"{lambda_resource_name} source must define handler()"
    return namespace


def stubbed_client(service_name):
    client = boto3.client(
        service_name,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(client)
    stubber.activate()
    return client, stubber


def lambda_context():
    return types.SimpleNamespace(aws_request_id="unit-test-request")


def build_fake_psycopg2(connect_impl):
    return types.SimpleNamespace(connect=connect_impl)


def test_main_tf_is_the_only_terraform_file_and_variable_contract_is_exact():
    tf_files = sorted(path.name for path in ROOT.glob("*.tf"))
    assert tf_files == ["main.tf"]
    assert set(variable_names()) == {
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    }
    assert re.search(r'default\s*=\s*"us-east-1"', variable_block("aws_region"))
    assert "default" not in variable_block("aws_endpoint")
    assert "default" not in variable_block("aws_access_key_id")
    assert "default" not in variable_block("aws_secret_access_key")


def test_provider_contract_and_endpoint_override_are_explicit():
    provider = provider_block("aws")
    assert count_pattern(r'^provider "aws"') == 1
    assert re.search(r"\bregion\s*=\s*var\.aws_region\b", provider)
    assert re.search(r"\baccess_key\s*=\s*var\.aws_access_key_id\b", provider)
    assert re.search(r"\bsecret_key\s*=\s*var\.aws_secret_access_key\b", provider)
    assert re.search(r"\baws_api_endpoint\s*=\s*trimspace\(var\.aws_endpoint\)", TF)
    assert "terraform_data" not in TF
    assert "aws_endpoint_proxy" not in TF
    assert "endpoint_proxy" not in TF
    assert "community_control_plane.py" not in TF
    assert "127.0.0.1" not in TF
    assert "(40 + 6) * 100 + 1" not in TF
    for service in [
        "apigateway",
        "cloudwatch",
        "ec2",
        "elbv2",
        "iam",
        "lambda",
        "logs",
        "pipes",
        "rds",
        "s3",
        "secretsmanager",
        "sqs",
        "sfn",
        "sts",
    ]:
        assert re.search(rf"\b{service}\s*=\s*local\.aws_api_endpoint\b", provider), service
    assert "aws.community_services" not in TF


def test_no_deletion_protection_retain_or_termination_protection_is_enabled():
    assert "prevent_destroy = true" not in TF
    assert "retain_on_delete" not in TF
    assert "termination_protection = true" not in TF
    assert "deletion_protection    = true" not in TF
    assert "deletion_protection = true" not in TF


def test_vpc_and_network_contract_is_declared_in_main_tf():
    vpc = resource_block("aws_vpc", "main")
    assert 'cidr_block           = "10.0.0.0/16"' in vpc
    assert "enable_dns_hostnames = true" in vpc
    assert "enable_dns_support   = true" in vpc
    assert len(resource_names("aws_internet_gateway")) == 1
    assert len(resource_names("aws_subnet")) == 4
    assert len(resource_names("aws_route_table")) == 1
    assert len(resource_names("aws_route_table_association")) == 2
    assert len(resource_names("aws_nat_gateway")) == 0
    assert 'cidr_block              = "10.0.1.0/24"' in resource_block("aws_subnet", "public_1")
    assert 'cidr_block              = "10.0.2.0/24"' in resource_block("aws_subnet", "public_2")
    assert 'cidr_block        = "10.0.101.0/24"' in resource_block("aws_subnet", "private_1")
    assert 'cidr_block        = "10.0.102.0/24"' in resource_block("aws_subnet", "private_2")
    assert "map_public_ip_on_launch = true" in resource_block("aws_subnet", "public_1")
    assert "map_public_ip_on_launch = true" in resource_block("aws_subnet", "public_2")
    route_table = resource_block("aws_route_table", "public")
    assert 'cidr_block = "0.0.0.0/0"' in route_table
    assert "gateway_id = aws_internet_gateway.main.id" in route_table
    assoc_1 = resource_block("aws_route_table_association", "public_1")
    assoc_2 = resource_block("aws_route_table_association", "public_2")
    assert "subnet_id      = aws_subnet.public_1.id" in assoc_1
    assert "subnet_id      = aws_subnet.public_2.id" in assoc_2


def test_security_groups_and_endpoints_cover_required_rules():
    alb_sg = resource_block("aws_security_group", "alb_sg")
    backend_sg = resource_block("aws_security_group", "backend_sg")
    db_sg = resource_block("aws_security_group", "db_sg")
    endpoint_sg = resource_block("aws_security_group", "endpoint_sg")
    alb_http = resource_block("aws_vpc_security_group_ingress_rule", "alb_http")
    alb_to_backend = resource_block("aws_vpc_security_group_egress_rule", "alb_to_backend")
    backend_from_alb = resource_block("aws_vpc_security_group_ingress_rule", "backend_from_alb")
    backend_to_endpoints = resource_block("aws_vpc_security_group_egress_rule", "backend_to_endpoints")
    backend_to_db = resource_block("aws_vpc_security_group_egress_rule", "backend_to_db")
    endpoints_https_from_backend = resource_block(
        "aws_vpc_security_group_ingress_rule",
        "endpoints_https_from_backend",
    )
    db_postgres_from_backend = resource_block("aws_vpc_security_group_ingress_rule", "db_postgres_from_backend")

    assert set(resource_names("aws_security_group")) == {"alb_sg", "backend_sg", "db_sg", "endpoint_sg"}
    assert set(resource_names("aws_vpc_security_group_ingress_rule")) == {
        "alb_http",
        "backend_from_alb",
        "endpoints_https_from_backend",
        "db_postgres_from_backend",
    }
    assert set(resource_names("aws_vpc_security_group_egress_rule")) == {
        "alb_to_backend",
        "backend_to_endpoints",
        "backend_to_db",
    }
    assert "egress = []" in db_sg
    assert "egress = []" in endpoint_sg
    assert "egress = []" in alb_sg
    assert "egress = []" in backend_sg
    assert 'cidr_ipv4         = "0.0.0.0/0"' in alb_http
    assert "security_group_id = aws_security_group.alb_sg.id" in alb_http
    assert "referenced_security_group_id = aws_security_group.backend_sg.id" in alb_to_backend
    assert "from_port                    = 8080" in alb_to_backend
    assert "security_group_id            = aws_security_group.backend_sg.id" in backend_from_alb
    assert "referenced_security_group_id = aws_security_group.alb_sg.id" in backend_from_alb
    assert "security_group_id            = aws_security_group.backend_sg.id" in backend_to_endpoints
    assert "from_port                    = 443" in backend_to_endpoints
    assert "referenced_security_group_id = aws_security_group.endpoint_sg.id" in backend_to_endpoints
    assert "security_group_id            = aws_security_group.backend_sg.id" in backend_to_db
    assert "from_port                    = 5432" in backend_to_db
    assert "referenced_security_group_id = aws_security_group.db_sg.id" in backend_to_db
    assert "security_group_id            = aws_security_group.endpoint_sg.id" in endpoints_https_from_backend
    assert "from_port                    = 443" in endpoints_https_from_backend
    assert "referenced_security_group_id = aws_security_group.backend_sg.id" in endpoints_https_from_backend
    assert "security_group_id            = aws_security_group.db_sg.id" in db_postgres_from_backend
    assert "from_port                    = 5432" in db_postgres_from_backend
    assert "referenced_security_group_id = aws_security_group.backend_sg.id" in db_postgres_from_backend
    assert "443" not in db_sg
    assert "5432" not in endpoint_sg
    assert 'cidr_ipv4         = "0.0.0.0/0"' not in "\n".join(all_resource_blocks("aws_vpc_security_group_egress_rule"))
    assert count_pattern(r'^resource "aws_vpc_endpoint" "') == 4
    for endpoint_name, suffix in [
        ("secretsmanager", ".secretsmanager"),
        ("sqs", ".sqs"),
        ("sfn", ".states"),
        ("logs", ".logs"),
    ]:
        endpoint = resource_block("aws_vpc_endpoint", endpoint_name)
        assert 'vpc_endpoint_type   = "Interface"' in endpoint
        assert suffix in endpoint
        assert "security_group_ids  = [aws_security_group.endpoint_sg.id]" in endpoint
        assert "aws_security_group.db_sg.id" not in endpoint


def test_lambda_resource_contracts_handlers_and_permissions_are_declared():
    frontend = resource_block("aws_lambda_function", "frontend_fn")
    backend = resource_block("aws_lambda_function", "backend_fn")
    worker = resource_block("aws_lambda_function", "worker_fn")
    alb = resource_block("aws_lb", "main")
    target_group = resource_block("aws_lb_target_group", "frontend")
    alb_permission = resource_block("aws_lambda_permission", "alb_frontend")
    listener = resource_block("aws_lb_listener", "http")
    assert 'handler          = "index.handler"' in frontend
    assert 'runtime          = "python3.12"' in frontend
    assert "memory_size      = 256" in frontend
    assert "timeout          = 10" in frontend
    assert 'handler          = "app.handler"' in backend
    assert 'runtime          = "python3.12"' in backend
    assert "memory_size      = 512" in backend
    assert "timeout          = 15" in backend
    assert "security_group_ids = [aws_security_group.backend_sg.id]" in backend
    assert 'handler          = "worker.handler"' in worker
    assert 'runtime          = "python3.12"' in worker
    assert "memory_size      = 256" in worker
    assert "timeout          = 10" in worker
    assert "security_group_ids = [aws_security_group.backend_sg.id]" in worker
    assert "DB_DISABLED   = local.db_disabled" in backend
    assert "count = local.supports_alb ? 1 : 0" in alb
    assert "count = local.supports_alb ? 1 : 0" in target_group
    assert "count = local.supports_alb ? 1 : 0" in alb_permission
    assert "count = local.supports_alb ? 1 : 0" in listener
    assert 'target_type = "lambda"' in target_group
    assert "principal     = \"elasticloadbalancing.amazonaws.com\"" in alb_permission
    assert "principal     = \"apigateway.amazonaws.com\"" in resource_block("aws_lambda_permission", "apigw_backend")


def test_api_gateway_integrations_and_stage_target_backend_lambda():
    assert len(resource_names("aws_api_gateway_method")) == 3
    assert len(resource_names("aws_api_gateway_integration")) == 3
    methods = "\n".join(all_resource_blocks("aws_api_gateway_method"))
    assert 'http_method   = "GET"' in methods
    assert 'http_method   = "POST"' in methods
    integrations = "\n".join(all_resource_blocks("aws_api_gateway_integration"))
    assert integrations.count('type                    = "AWS_PROXY"') == 3
    assert integrations.count("aws_lambda_function.backend_fn.invoke_arn") == 3
    assert 'stage_name    = "dev"' in resource_block("aws_api_gateway_stage", "dev")


def test_database_secret_rds_and_schema_contract_are_declared():
    secret = resource_block("aws_secretsmanager_secret", "db_credentials")
    secret_version = resource_block("aws_secretsmanager_secret_version", "db_credentials")
    generated_password = data_block("aws_secretsmanager_random_password", "db_password")
    db_subnet_group = resource_block("aws_db_subnet_group", "rds")
    db_instance = resource_block("aws_db_instance", "main")
    backend_source = lambda_source_for("backend_fn")

    assert "recovery_window_in_days = 0" in secret
    assert 'username = "appuser"' in secret_version
    assert "password = data.aws_secretsmanager_random_password.db_password.random_password" in secret_version
    assert re.search(r"\bpassword_length\s*=\s*20\b", generated_password)
    assert re.search(r"\bexclude_numbers\s*=\s*false\b", generated_password)
    assert re.search(r"\bexclude_punctuation\s*=\s*true\b", generated_password)
    assert re.search(r"\binclude_space\s*=\s*false\b", generated_password)
    assert 'resource "random_password"' not in TF
    assert 'provider "random"' not in TF
    assert "hashicorp/random" not in TF
    assert "subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]" in db_subnet_group
    assert "count = local.supports_rds ? 1 : 0" in db_subnet_group
    assert 'engine                 = "postgres"' in db_instance
    assert 'engine_version         = "15.4"' in db_instance
    assert 'instance_class         = "db.t3.micro"' in db_instance
    assert "allocated_storage      = 20" in db_instance
    assert "multi_az               = false" in db_instance
    assert "publicly_accessible    = false" in db_instance
    assert "port                   = 5432" in db_instance
    assert "vpc_security_group_ids = [aws_security_group.db_sg.id]" in db_instance
    assert "count                  = local.supports_rds ? 1 : 0" in db_instance
    assert "id serial primary key" in backend_source
    assert "value text not null" in backend_source
    assert "created_at timestamp default now()" in backend_source
    assert "DB_DISABLED" in backend_source


def test_state_machine_pipe_and_iam_policies_are_scoped():
    sfn = resource_block("aws_sfn_state_machine", "ingest_sm")
    pipe = resource_block("aws_pipes_pipe", "ingest_pipe")
    backend_policy = resource_block("aws_iam_role_policy", "backend_policy")
    pipes_sqs = resource_block("aws_iam_role_policy", "pipes_sqs")
    pipes_lambda = resource_block("aws_iam_role_policy", "pipes_lambda")
    pipes_states = resource_block("aws_iam_role_policy", "pipes_states")

    assert '"Type": "Task"' in sfn
    assert '"Type": "Succeed"' in sfn
    assert '"FunctionName": "${aws_lambda_function.worker_fn.arn}"' in sfn
    assert "count = local.supports_pipes ? 1 : 0" in pipe
    assert "source   = aws_sqs_queue.ingest_queue.arn" in pipe
    assert "enrichment = aws_lambda_function.worker_fn.arn" in pipe
    assert "target   = aws_sfn_state_machine.ingest_sm.arn" in pipe
    assert "sqs:SendMessage" in backend_policy
    assert 'Resource = aws_sqs_queue.ingest_queue.arn' in backend_policy
    assert "secretsmanager:GetSecretValue" in backend_policy
    assert 'Resource = aws_secretsmanager_secret.db_credentials.arn' in backend_policy
    assert "sqs:ReceiveMessage" in pipes_sqs
    assert "sqs:DeleteMessage" in pipes_sqs
    assert "sqs:GetQueueAttributes" in pipes_sqs
    assert 'Resource = aws_sqs_queue.ingest_queue.arn' in pipes_sqs
    assert "lambda:InvokeFunction" in pipes_lambda
    assert 'Resource = aws_lambda_function.worker_fn.arn' in pipes_lambda
    assert "states:StartExecution" in pipes_states
    assert 'Resource = aws_sfn_state_machine.ingest_sm.arn' in pipes_states


def test_exactly_two_backend_alarms_are_declared():
    alarms = all_resource_blocks("aws_cloudwatch_metric_alarm")
    assert len(alarms) == 2
    combined = "\n".join(alarms)
    assert combined.count('metric_name         = "Errors"') == 1
    assert combined.count('metric_name         = "Duration"') == 1
    assert combined.count("aws_lambda_function.backend_fn.function_name") == 2


def test_log_groups_are_explicit_and_not_conditionally_skipped():
    frontend_log_group = resource_block("aws_cloudwatch_log_group", "frontend_fn")
    backend_log_group = resource_block("aws_cloudwatch_log_group", "backend_fn")
    worker_log_group = resource_block("aws_cloudwatch_log_group", "worker_fn")
    assert 'name              = "/aws/lambda/frontend_fn"' in frontend_log_group
    assert 'name              = "/aws/lambda/backend_fn"' in backend_log_group
    assert 'name              = "/aws/lambda/worker_fn"' in worker_log_group
    assert "count             = local.endpoint_override_enabled ? 0 : 1" not in frontend_log_group
    assert "count             = local.endpoint_override_enabled ? 0 : 1" not in backend_log_group
    assert "count             = local.endpoint_override_enabled ? 0 : 1" not in worker_log_group


def test_endpoint_override_compatibility_flags_are_explicit():
    assert re.search(r"\bendpoint_override_enabled\s*=\s*local\.has_custom_endpoint\b", TF)
    assert re.search(r"\bsupports_rds\s*=\s*!local\.endpoint_override_enabled\b", TF)
    assert re.search(r"\bsupports_pipes\s*=\s*!local\.endpoint_override_enabled\b", TF)
    assert re.search(r"\bsupports_apigateway\s*=\s*true\b", TF)
    assert re.search(r"\bsupports_alb\s*=\s*!local\.endpoint_override_enabled\b", TF)


def test_frontend_lambda_returns_html_with_health_link():
    module = load_lambda_module("frontend_fn", boto3_client_factory=lambda *_args, **_kwargs: None)
    response = module["handler"]({}, lambda_context())

    assert response["statusCode"] == 200
    assert "/api/health" in response["body"]
    assert "<html" in response["body"].lower()


def test_backend_health_uses_boto3_to_read_secret_and_checks_database():
    secrets_client, secrets_stubber = stubbed_client("secretsmanager")
    secrets_stubber.add_response(
        "get_secret_value",
        {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db",
            "Name": "db-secret",
            "SecretString": json.dumps({"username": "appuser", "password": "safePass123"}),
        },
        {"SecretId": "db-secret"},
    )

    captured = {}

    class FakeCursor:
        def execute(self, query, params=None):
            captured.setdefault("queries", []).append((query, params))

        def fetchone(self):
            return (1,)

        def close(self):
            captured["cursor_closed"] = True

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            captured["connection_closed"] = True

    def fake_connect(**kwargs):
        captured["connect_kwargs"] = kwargs
        return FakeConnection()

    module = load_lambda_module(
        "backend_fn",
        boto3_client_factory=lambda service_name, **_kwargs: secrets_client,
        psycopg2_module=build_fake_psycopg2(fake_connect),
    )

    with patched_environment(
        DB_HOST="db.internal",
        DB_PORT="5432",
        DB_USER="appuser",
        DB_SECRET="db-secret",
        SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789012/ingest_queue",
    ):
        response = module["handler"](
            {"httpMethod": "GET", "resource": "/api/health", "path": "/api/health"},
            lambda_context(),
        )

    secrets_stubber.assert_no_pending_responses()
    assert captured["connect_kwargs"]["host"] == "db.internal"
    assert str(captured["connect_kwargs"]["port"]) == "5432"
    assert captured["connect_kwargs"]["user"] == "appuser"
    assert captured["connect_kwargs"]["password"] == "safePass123"
    assert any("SELECT 1" in query for query, _ in captured["queries"])

    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert body["status"] == "ok"
    assert body["db"] in {"ok", "connected"}


def test_backend_post_items_inserts_row_and_sends_message_with_boto3():
    secrets_client, secrets_stubber = stubbed_client("secretsmanager")
    secrets_stubber.add_response(
        "get_secret_value",
        {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db",
            "Name": "db-secret",
            "SecretString": json.dumps({"username": "appuser", "password": "safePass123"}),
        },
        {"SecretId": "db-secret"},
    )

    sqs_client, sqs_stubber = stubbed_client("sqs")
    queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/ingest_queue"
    sqs_stubber.add_response(
        "send_message",
        {"MessageId": "msg-123"},
        {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps({"id": 42, "value": "demo-item"}),
        },
    )

    captured = {"queries": []}

    class FakeCursor:
        lastrowid = 42

        def execute(self, query, params=None):
            captured["queries"].append((query, params))

        def fetchone(self):
            return (42,)

        def close(self):
            captured["cursor_closed"] = True

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            captured["committed"] = True

        def close(self):
            captured["connection_closed"] = True

    def fake_connect(**kwargs):
        captured["connect_kwargs"] = kwargs
        return FakeConnection()

    def boto_factory(service_name, **_kwargs):
        if service_name == "secretsmanager":
            return secrets_client
        if service_name == "sqs":
            return sqs_client
        raise AssertionError(f"Unexpected boto3 client request: {service_name}")

    module = load_lambda_module(
        "backend_fn",
        boto3_client_factory=boto_factory,
        psycopg2_module=build_fake_psycopg2(fake_connect),
    )

    with patched_environment(
        DB_HOST="db.internal",
        DB_PORT="5432",
        DB_USER="appuser",
        DB_SECRET="db-secret",
        SQS_QUEUE_URL=queue_url,
    ):
        response = module["handler"](
            {
                "httpMethod": "POST",
                "resource": "/api/items",
                "path": "/api/items",
                "body": json.dumps({"value": "demo-item"}),
            },
            lambda_context(),
        )

    secrets_stubber.assert_no_pending_responses()
    sqs_stubber.assert_no_pending_responses()
    assert captured["committed"] is True
    assert any("INSERT INTO items" in query for query, _ in captured["queries"])

    body = json.loads(response["body"])
    assert response["statusCode"] in {200, 201}
    assert body["id"] == 42
    assert body["value"] == "demo-item"


def test_backend_db_disabled_path_uses_local_items_and_sqs(tmp_path):
    sqs_client, sqs_stubber = stubbed_client("sqs")
    queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/ingest_queue"
    sqs_stubber.add_response(
        "send_message",
        {"MessageId": "msg-local"},
        {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps({"id": 1, "value": "offline-item"}),
        },
    )

    def boto_factory(service_name, **_kwargs):
        if service_name == "sqs":
            return sqs_client
        raise AssertionError(f"Unexpected boto3 client request while DB is disabled: {service_name}")

    module = load_lambda_module("backend_fn", boto3_client_factory=boto_factory)
    module["LOCAL_ITEMS_PATH"] = str(tmp_path / "backend_items.json")

    with patched_environment(
        DB_DISABLED="true",
        DB_HOST="localhost",
        DB_PORT="5432",
        DB_SECRET="db-secret",
        SQS_QUEUE_URL=queue_url,
    ):
        create_response = module["handler"](
            {
                "httpMethod": "POST",
                "resource": "/api/items",
                "path": "/api/items",
                "body": json.dumps({"value": "offline-item"}),
            },
            lambda_context(),
        )
        list_response = module["handler"](
            {"httpMethod": "GET", "resource": "/api/items", "path": "/api/items"},
            lambda_context(),
        )

    sqs_stubber.assert_no_pending_responses()
    create_body = json.loads(create_response["body"])
    list_body = json.loads(list_response["body"])
    assert create_response["statusCode"] == 201
    assert create_body == {"id": 1, "value": "offline-item"}
    assert list_body["items"][0]["value"] == "offline-item"


def test_backend_get_items_returns_recent_rows():
    secrets_client, secrets_stubber = stubbed_client("secretsmanager")
    secrets_stubber.add_response(
        "get_secret_value",
        {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db",
            "Name": "db-secret",
            "SecretString": json.dumps({"username": "appuser", "password": "safePass123"}),
        },
        {"SecretId": "db-secret"},
    )

    rows = [
        (2, "newer", datetime(2026, 4, 15, 10, 30, tzinfo=timezone.utc)),
        (1, "older", datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc)),
    ]
    captured = {"queries": []}

    class FakeCursor:
        def execute(self, query, params=None):
            captured["queries"].append(query)

        def fetchall(self):
            return rows

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            return None

    module = load_lambda_module(
        "backend_fn",
        boto3_client_factory=lambda service_name, **_kwargs: secrets_client,
        psycopg2_module=build_fake_psycopg2(lambda **_kwargs: FakeConnection()),
    )

    with patched_environment(
        DB_HOST="db.internal",
        DB_PORT="5432",
        DB_USER="appuser",
        DB_SECRET="db-secret",
        SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789012/ingest_queue",
    ):
        response = module["handler"](
            {"httpMethod": "GET", "resource": "/api/items", "path": "/api/items"},
            lambda_context(),
        )

    secrets_stubber.assert_no_pending_responses()
    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert any("CREATE TABLE IF NOT EXISTS items" in query for query in captured["queries"])
    assert any("ORDER BY id DESC LIMIT 10" in query for query in captured["queries"])
    assert len(body["items"]) == 2
    assert body["items"][0]["id"] == 2
    assert body["items"][0]["value"] == "newer"


def test_worker_logs_the_payload_received_from_the_state_machine(caplog):
    caplog.set_level(logging.INFO)
    module = load_lambda_module("worker_fn", boto3_client_factory=lambda *_args, **_kwargs: None)

    payload = {"id": 42, "value": "demo-item"}
    response = module["handler"]({"payload": payload}, lambda_context())

    assert response["statusCode"] == 200
    assert "demo-item" in json.dumps(response)
    assert any("demo-item" in record.getMessage() for record in caplog.records), (
        "worker_fn must log the payload it receives"
    )
