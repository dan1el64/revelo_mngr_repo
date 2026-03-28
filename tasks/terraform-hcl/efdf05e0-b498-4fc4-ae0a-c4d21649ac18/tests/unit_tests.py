import json
import os
import re
import sys
import textwrap
import types
from collections import Counter
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plan.json"
MAIN_TF_PATH = ROOT / "main.tf"
REQUIRED_ENDPOINT_SERVICES = (
    "sts",
    "iam",
    "ec2",
    "logs",
    "s3",
    "dynamodb",
    "lambda",
    "apigateway",
    "events",
    "sns",
    "sqs",
    "secretsmanager",
    "rds",
    "elasticloadbalancingv2",
)


def _load_plan() -> dict:
    if not PLAN_PATH.exists():
        pytest.skip("plan.json is required for unit tests")
    return json.loads(PLAN_PATH.read_text())


@pytest.fixture(scope="module")
def terraform_plan() -> dict:
    return _load_plan()


def _module_resources(module: dict) -> list[dict]:
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_module_resources(child))
    return resources


def _planned_resources(terraform_plan: dict) -> list[dict]:
    return [
        resource
        for resource in _module_resources(terraform_plan["planned_values"]["root_module"])
        if resource.get("mode") == "managed" and "values" in resource
    ]


def _resource_map(terraform_plan: dict) -> dict[str, dict]:
    return {resource["address"]: resource for resource in _planned_resources(terraform_plan)}


def _resource_values(resources: dict[str, dict], prefix: str) -> dict:
    for address, resource in resources.items():
        if address == prefix or address.startswith(f"{prefix}["):
            return resource["values"]
    raise KeyError(prefix)


def _configuration_json(terraform_plan: dict) -> str:
    return json.dumps(terraform_plan.get("configuration", {}), sort_keys=True)


def _resource_changes_map(terraform_plan: dict) -> dict[str, dict]:
    return {
        change["address"]: change
        for change in terraform_plan.get("resource_changes", [])
        if change.get("mode") == "managed"
    }


def _main_tf_text() -> str:
    return MAIN_TF_PATH.read_text()


def _resource_block_text(resource_type: str, name: str) -> str:
    text = _main_tf_text()
    anchor = f'resource "{resource_type}" "{name}"'
    start = text.index(anchor)
    end_candidates = [
        pos
        for pos in (
            text.find('\nresource "', start + 1),
            text.find("\noutput ", start + 1),
            text.find("\ndata ", start + 1),
        )
        if pos != -1
    ]
    end = min(end_candidates) if end_candidates else len(text)
    return text[start:end]


def _extract_inline_lambda_code(name: str) -> str:
    text = MAIN_TF_PATH.read_text()
    resource_anchor = f'data "archive_file" "{name}"'
    resource_start = text.index(resource_anchor)
    source_anchor = "content  = <<-PY"
    source_start = text.index(source_anchor, resource_start) + len(source_anchor)
    source_end = text.index("\n    PY", source_start)
    return textwrap.dedent(text[source_start:source_end]).strip()


def _load_lambda_module(name: str, boto3_module, extra_modules=None):
    module = types.ModuleType(name)
    previous_modules = {"boto3": sys.modules.get("boto3")}
    sys.modules["boto3"] = boto3_module

    extra_modules = extra_modules or {}
    for module_name, module_value in extra_modules.items():
        previous_modules[module_name] = sys.modules.get(module_name)
        sys.modules[module_name] = module_value

    try:
        exec(_extract_inline_lambda_code(name), module.__dict__)
    finally:
        for module_name, module_value in previous_modules.items():
            if module_value is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = module_value

    return module


def _assert_create_only(change: dict):
    assert change["change"]["actions"] in (["create"], ["read"])


def _policy_values(value):
    if isinstance(value, list):
        return value
    return [value]


def _endpoint_plan(terraform_plan: dict) -> bool:
    outputs = terraform_plan["planned_values"]["outputs"]
    address = outputs.get("rds_endpoint_address", {}).get("value")
    port = outputs.get("rds_endpoint_port", {}).get("value")
    return address is not None and port is not None and str(port) != "5432"


def test_all_infrastructure_is_defined_in_a_single_main_tf():
    terraform_files = sorted(path.name for path in ROOT.glob("*.tf"))
    assert terraform_files == ["main.tf"]


def test_provider_configuration_uses_only_required_inputs_and_defaults(terraform_plan):
    configuration = terraform_plan["configuration"]
    configuration_json = _configuration_json(terraform_plan)
    variables = configuration["root_module"]["variables"]
    main_tf_text = _main_tf_text()
    provider_block = main_tf_text[main_tf_text.index('provider "aws"'):main_tf_text.index('data "aws_availability_zones" "available"')]
    referenced_vars = set(re.findall(r"\bvar\.([A-Za-z0-9_]+)\b", main_tf_text))

    assert set(variables) == {"aws_endpoint", "aws_region"}
    assert referenced_vars == {"aws_endpoint", "aws_region"}
    assert "us-east-1" in configuration_json
    assert "aws_endpoint" in configuration_json
    for service in REQUIRED_ENDPOINT_SERVICES:
        assert service in configuration_json
    assert "AWS_ACCESS_KEY_ID" not in main_tf_text
    assert "AWS_SECRET_ACCESS_KEY" not in main_tf_text
    assert "access_key" not in configuration_json
    assert "secret_key" not in configuration_json
    assert "shared_credentials" not in configuration_json
    assert "profile" not in configuration_json
    assert "access_key" not in provider_block
    assert "secret_key" not in provider_block
    assert "shared_credentials_files" not in provider_block
    assert "shared_config_files" not in provider_block
    assert "profile" not in provider_block
    assert 'data "external"' not in main_tf_text
    assert "hashicorp/external" not in main_tf_text
    assert "file(" not in configuration_json
    assert "templatefile(" not in configuration_json
    assert "filebase64" not in configuration_json
    assert 'resource "local_file"' not in main_tf_text
    assert 'data "local_file"' not in main_tf_text


def test_plan_creates_expected_resource_topology(terraform_plan):
    endpoint_plan = _endpoint_plan(terraform_plan)
    counts = Counter(
        change["type"]
        for change in terraform_plan["resource_changes"]
        if change.get("mode") == "managed"
    )

    expected_counts = {
        "aws_api_gateway_deployment": 1,
        "aws_api_gateway_integration": 1,
        "aws_api_gateway_method": 1,
        "aws_api_gateway_resource": 1,
        "aws_api_gateway_rest_api": 1,
        "aws_api_gateway_stage": 1,
        "aws_cloudwatch_event_rule": 1,
        "aws_cloudwatch_event_target": 1,
        "aws_cloudwatch_log_group": 2,
        "aws_dynamodb_table": 1,
        "aws_eip": 1,
        "aws_iam_role": 2,
        "aws_iam_role_policy": 2,
        "aws_internet_gateway": 1,
        "aws_lambda_function": 2,
        "aws_lambda_permission": 2,
        "aws_nat_gateway": 1,
        "aws_route": 2,
        "aws_route_table": 2,
        "aws_route_table_association": 4,
        "aws_s3_bucket": 1,
        "aws_s3_bucket_lifecycle_configuration": 1,
        "aws_s3_bucket_ownership_controls": 1,
        "aws_s3_bucket_policy": 1,
        "aws_s3_bucket_public_access_block": 1,
        "aws_s3_bucket_server_side_encryption_configuration": 1,
        "aws_s3_bucket_versioning": 1,
        "aws_secretsmanager_secret": 2,
        "aws_secretsmanager_secret_version": 2,
        "aws_security_group": 2,
        "aws_sns_topic": 1,
        "aws_sns_topic_subscription": 1,
        "aws_sqs_queue": 1,
        "aws_sqs_queue_policy": 1,
        "aws_subnet": 4,
        "aws_vpc": 1,
        "aws_vpc_endpoint": 1,
    }
    if not endpoint_plan:
        expected_counts["aws_db_instance"] = 1
        expected_counts["aws_db_subnet_group"] = 1
    assert counts == expected_counts

    for change in terraform_plan["resource_changes"]:
        if change.get("mode") in {"managed", "data"}:
            _assert_create_only(change)


def test_plan_does_not_create_additional_managed_resources(terraform_plan):
    endpoint_plan = _endpoint_plan(terraform_plan)
    actual_addresses = {resource["address"] for resource in _planned_resources(terraform_plan)}
    expected_addresses = {
        "aws_api_gateway_deployment.order_intake",
        "aws_api_gateway_integration.ingest_post",
        "aws_api_gateway_method.ingest_post",
        "aws_api_gateway_resource.ingest",
        "aws_api_gateway_rest_api.order_intake",
        "aws_api_gateway_stage.v1",
        "aws_cloudwatch_event_rule.analytics",
        "aws_cloudwatch_event_target.analytics",
        "aws_cloudwatch_log_group.analytics_fn",
        "aws_cloudwatch_log_group.ingest_fn",
        "aws_dynamodb_table.order_metadata",
        "aws_eip.nat",
        "aws_iam_role.analytics_fn",
        "aws_iam_role.ingest_fn",
        "aws_iam_role_policy.analytics_fn",
        "aws_iam_role_policy.ingest_fn",
        "aws_internet_gateway.order_intake",
        "aws_lambda_function.analytics_fn",
        "aws_lambda_function.ingest_fn",
        "aws_lambda_permission.api_gateway_ingest",
        "aws_lambda_permission.eventbridge_analytics",
        "aws_nat_gateway.order_intake",
        "aws_route.private_default",
        "aws_route.public_default",
        "aws_route_table.private",
        "aws_route_table.public",
        "aws_route_table_association.private_a",
        "aws_route_table_association.private_b",
        "aws_route_table_association.public_a",
        "aws_route_table_association.public_b",
        "aws_s3_bucket.archive",
        "aws_s3_bucket_lifecycle_configuration.archive",
        "aws_s3_bucket_ownership_controls.archive",
        "aws_s3_bucket_policy.archive",
        "aws_s3_bucket_public_access_block.archive",
        "aws_s3_bucket_server_side_encryption_configuration.archive",
        "aws_s3_bucket_versioning.archive",
        "aws_secretsmanager_secret.api_key",
        "aws_secretsmanager_secret.db_app_user",
        "aws_secretsmanager_secret_version.api_key",
        "aws_secretsmanager_secret_version.db_app_user",
        "aws_security_group.database",
        "aws_security_group.lambda",
        "aws_sns_topic.order_events",
        "aws_sns_topic_subscription.order_events_queue",
        "aws_sqs_queue.order_events",
        "aws_sqs_queue_policy.order_events",
        "aws_subnet.private_a",
        "aws_subnet.private_b",
        "aws_subnet.public_a",
        "aws_subnet.public_b",
        "aws_vpc.order_intake",
        "aws_vpc_endpoint.dynamodb",
    }
    if not endpoint_plan:
        expected_addresses.add("aws_db_instance.orders")
        expected_addresses.add("aws_db_subnet_group.orders")
    assert actual_addresses == expected_addresses


def test_plan_enforces_tags_network_and_database_shape(terraform_plan):
    endpoint_plan = _endpoint_plan(terraform_plan)
    resources = _resource_map(terraform_plan)
    configuration_json = _configuration_json(terraform_plan)
    main_tf_text = _main_tf_text()
    public_default_route = _resource_block_text("aws_route", "public_default")
    private_default_route = _resource_block_text("aws_route", "private_default")

    for resource in _planned_resources(terraform_plan):
        if "tags_all" in resource["values"]:
            assert resource["values"]["tags_all"]["Project"] == "OrderIntake", resource["address"]
            assert resource["values"]["tags_all"]["ManagedBy"] == "Terraform", resource["address"]

    vpc = resources["aws_vpc.order_intake"]["values"]
    public_a = resources["aws_subnet.public_a"]["values"]
    public_b = resources["aws_subnet.public_b"]["values"]
    private_a = resources["aws_subnet.private_a"]["values"]
    private_b = resources["aws_subnet.private_b"]["values"]
    lambda_sg = resources["aws_security_group.lambda"]["values"]
    database_sg = resources["aws_security_group.database"]["values"]
    dynamodb = resources["aws_dynamodb_table.order_metadata"]["values"]
    dynamodb_endpoint = resources["aws_vpc_endpoint.dynamodb"]["values"]
    queue = resources["aws_sqs_queue.order_events"]["values"]
    private_route_table = resources["aws_route_table.private"]["values"]
    outputs = terraform_plan["planned_values"]["outputs"]

    assert vpc["cidr_block"] == "10.20.0.0/16"
    assert vpc["enable_dns_hostnames"] is True
    assert vpc["enable_dns_support"] is True
    assert public_a["cidr_block"] == "10.20.0.0/24"
    assert public_b["cidr_block"] == "10.20.1.0/24"
    assert private_a["cidr_block"] == "10.20.10.0/24"
    assert private_b["cidr_block"] == "10.20.11.0/24"
    assert public_a["map_public_ip_on_launch"] is True
    assert public_b["map_public_ip_on_launch"] is True
    assert private_a["map_public_ip_on_launch"] is False
    assert private_b["map_public_ip_on_launch"] is False
    assert public_a["availability_zone"] != public_b["availability_zone"]
    assert private_a["availability_zone"] != private_b["availability_zone"]
    assert 'destination_cidr_block = "0.0.0.0/0"' in public_default_route
    assert "gateway_id             = aws_internet_gateway.order_intake.id" in public_default_route
    assert 'destination_cidr_block = "0.0.0.0/0"' in private_default_route
    assert "nat_gateway_id         = aws_nat_gateway.order_intake.id" in private_default_route
    assert "aws_db_subnet_group" in configuration_json
    assert "aws_subnet.private_a.id" in configuration_json
    assert "aws_subnet.private_b.id" in configuration_json
    assert "aws_security_group.lambda.id" in configuration_json
    assert 'resource "aws_lambda_function" "ingest_fn"' in main_tf_text
    assert 'resource "aws_lambda_function" "analytics_fn"' in main_tf_text
    assert "security_group_ids = [aws_security_group.lambda.id]" in main_tf_text
    assert 'data "external"' not in main_tf_text
    assert 'SKIP_DB_CONNECTIVITY' not in main_tf_text

    assert dynamodb["billing_mode"] == "PAY_PER_REQUEST"
    assert dynamodb["hash_key"] == "pk"
    assert dynamodb["range_key"] == "sk"
    attributes = {attribute["name"]: attribute["type"] for attribute in dynamodb["attribute"]}
    assert attributes["pk"] == "S"
    assert attributes["sk"] == "S"
    assert dynamodb["point_in_time_recovery"][0]["enabled"] is True
    assert "attribute_name = \"ttl\"" in main_tf_text
    assert "enabled        = true" in main_tf_text
    assert '"ttl"' in main_tf_text

    assert queue["message_retention_seconds"] == 345600
    assert queue["visibility_timeout_seconds"] == 60
    assert dynamodb_endpoint["vpc_endpoint_type"] == "Gateway"
    assert "route_table_ids   = [aws_route_table.private.id]" in main_tf_text
    assert dynamodb_endpoint["service_name"] == "com.amazonaws.us-east-1.dynamodb"

    assert lambda_sg.get("ingress", []) == []
    assert len(lambda_sg.get("egress", [])) == 1
    assert lambda_sg["egress"][0]["protocol"] == "-1"
    assert lambda_sg["egress"][0]["from_port"] == 0
    assert lambda_sg["egress"][0]["to_port"] == 0
    assert lambda_sg["egress"][0]["cidr_blocks"] == ["0.0.0.0/0"]

    ingress_rules = [
        rule for rule in database_sg["ingress"]
        if rule["from_port"] == 5432 and rule["to_port"] == 5432 and rule["protocol"] == "tcp"
    ]
    assert len(ingress_rules) == 1
    assert "security_groups = [aws_security_group.lambda.id]" in main_tf_text
    assert ingress_rules[0].get("cidr_blocks", []) == []
    assert ingress_rules[0].get("ipv6_cidr_blocks", []) == []
    assert "vpc_security_group_ids  = [aws_security_group.database.id]" in main_tf_text
    assert "data.aws_secretsmanager_secret_version.db_app_user" in configuration_json
    assert "jsondecode(data.aws_secretsmanager_secret_version.db_app_user.secret_string)" in main_tf_text
    if endpoint_plan:
        assert "local.db_app_user_secret_arn" in main_tf_text
        assert outputs["rds_endpoint_address"]["value"] is not None
        assert int(outputs["rds_endpoint_port"]["value"]) > 0
    else:
        assert "data.aws_secretsmanager_secret_version.db_app_user" in configuration_json
        assert "jsondecode(data.aws_secretsmanager_secret_version.db_app_user[0].secret_string)" in main_tf_text
        db_instance = resources["aws_db_instance.orders"]["values"]
        assert db_instance["instance_class"] == "db.t3.micro"
        assert db_instance["engine"] == "postgres"
        assert db_instance["engine_version"] == "15.4"
        assert db_instance["db_name"] == "orders"
        assert db_instance["allocated_storage"] == 20
        assert db_instance["storage_type"] == "gp2"
        assert db_instance["storage_encrypted"] is True
        assert db_instance["backup_retention_period"] == 1
        assert db_instance["deletion_protection"] is False
        assert db_instance["publicly_accessible"] is False


def test_plan_enforces_storage_messaging_logging_and_iam_controls(terraform_plan):
    resources = _resource_map(terraform_plan)
    resource_changes = _resource_changes_map(terraform_plan)
    configuration_json = _configuration_json(terraform_plan)
    main_tf_text = _main_tf_text()

    lifecycle = resources["aws_s3_bucket_lifecycle_configuration.archive"]["values"]
    ingest_log = resources["aws_cloudwatch_log_group.ingest_fn"]["values"]
    analytics_log = resources["aws_cloudwatch_log_group.analytics_fn"]["values"]
    ingest = resources["aws_lambda_function.ingest_fn"]["values"]
    analytics = resources["aws_lambda_function.analytics_fn"]["values"]

    assert len(lifecycle["rule"]) == 1
    assert lifecycle["rule"][0]["status"] == "Enabled"
    assert lifecycle["rule"][0]["abort_incomplete_multipart_upload"][0]["days_after_initiation"] == 7

    assert "aws_s3_bucket_policy" in configuration_json
    assert "DenyInsecureTransport" in main_tf_text
    assert "AllowAnalyticsRead" in main_tf_text
    assert "AllowAnalyticsGetObject" in main_tf_text
    assert "AllowIngestRawWrites" in main_tf_text
    assert "analytics-fn-inline-policy" in main_tf_text
    assert "ingest-fn-inline-policy" in main_tf_text
    assert "local.api_key_secret_arn" in main_tf_text
    assert "local.db_app_user_secret_arn" in main_tf_text
    assert '"s3:ListBucket"' in main_tf_text
    assert '"s3:GetObject"' in main_tf_text
    assert '"s3:PutObject"' in main_tf_text
    assert '"sns:Publish"' in main_tf_text
    assert '"secretsmanager:GetSecretValue"' in main_tf_text

    queue_policy_after = resource_changes["aws_sqs_queue_policy.order_events"]["change"]["after"]
    assert "aws_sqs_queue.order_events.id" in configuration_json
    assert "sns.amazonaws.com" in main_tf_text
    assert "sqs:SendMessage" in main_tf_text
    assert "aws_sns_topic.order_events.arn" in configuration_json

    assert ingest_log["retention_in_days"] == 14
    assert analytics_log["retention_in_days"] == 14
    assert ingest["handler"] == "index.lambda_handler"
    assert analytics["handler"] == "index.lambda_handler"
    assert ingest["runtime"] == "python3.11"
    assert analytics["runtime"] == "python3.11"
    assert ingest["architectures"] == ["x86_64"]
    assert analytics["architectures"] == ["x86_64"]
    assert ingest["timeout"] == 10
    assert analytics["timeout"] == 10
    assert ingest["memory_size"] == 256
    assert analytics["memory_size"] == 256


def test_plan_enforces_api_gateway_stage_and_lambda_permission_scoping(terraform_plan):
    resources = _resource_map(terraform_plan)
    main_tf_text = _main_tf_text()
    stage = resources["aws_api_gateway_stage.v1"]["values"]

    assert stage["stage_name"] == "v1"
    assert 'resource "aws_api_gateway_deployment" "order_intake"' in main_tf_text
    assert "create_before_destroy = true" in main_tf_text

    api_permission = _resource_block_text("aws_lambda_permission", "api_gateway_ingest")
    assert 'principal     = "apigateway.amazonaws.com"' in api_permission
    assert 'function_name = aws_lambda_function.ingest_fn.function_name' in api_permission
    assert 'source_arn    = "${aws_api_gateway_rest_api.order_intake.execution_arn}/${aws_api_gateway_stage.v1.stage_name}/POST/ingest"' in api_permission

    eventbridge_permission = _resource_block_text("aws_lambda_permission", "eventbridge_analytics")
    assert 'principal     = "events.amazonaws.com"' in eventbridge_permission
    assert 'function_name = aws_lambda_function.analytics_fn.function_name' in eventbridge_permission
    assert "source_arn    = aws_cloudwatch_event_rule.analytics.arn" in eventbridge_permission


def test_plan_declares_dependencies_for_single_run_apply():
    main_tf_text = _main_tf_text()
    subscription = _resource_block_text("aws_sns_topic_subscription", "order_events_queue")
    event_target = _resource_block_text("aws_cloudwatch_event_target", "analytics")
    nat_gateway = _resource_block_text("aws_nat_gateway", "order_intake")
    ingest_lambda = _resource_block_text("aws_lambda_function", "ingest_fn")
    analytics_lambda = _resource_block_text("aws_lambda_function", "analytics_fn")

    assert "depends_on = [aws_sqs_queue_policy.order_events]" in subscription
    assert "depends_on = [aws_lambda_permission.eventbridge_analytics]" in event_target
    assert "depends_on = [aws_internet_gateway.order_intake]" in nat_gateway
    assert "aws_cloudwatch_log_group.ingest_fn" in ingest_lambda
    assert "aws_iam_role_policy.ingest_fn" in ingest_lambda
    assert "aws_cloudwatch_log_group.analytics_fn" in analytics_lambda
    assert "aws_iam_role_policy.analytics_fn" in analytics_lambda
    assert "create_before_destroy = true" in main_tf_text


def test_plan_explicitly_disables_rds_deletion_protection(terraform_plan):
    if _endpoint_plan(terraform_plan):
        outputs = terraform_plan["planned_values"]["outputs"]
        assert outputs["rds_endpoint_address"]["value"] is not None
        assert int(outputs["rds_endpoint_port"]["value"]) > 0
        return
    db_instance = _resource_map(terraform_plan)["aws_db_instance.orders"]["values"]
    assert db_instance["deletion_protection"] is False


def test_plan_enforces_iam_policy_specificity_without_standalone_wildcards():
    ingest_policy = _resource_block_text("aws_iam_role_policy", "ingest_fn")
    analytics_policy = _resource_block_text("aws_iam_role_policy", "analytics_fn")

    for policy_block in (ingest_policy, analytics_policy):
        assert 'Action = "*"' not in policy_block
        assert 'Action   = "*"' not in policy_block
        assert 'Resource = "*"' not in policy_block
        assert 'Resource   = "*"' not in policy_block

    assert "${aws_cloudwatch_log_group.ingest_fn.arn}:*" in ingest_policy
    assert "${aws_cloudwatch_log_group.analytics_fn.arn}:*" in analytics_policy
    assert '"dynamodb:*"' not in ingest_policy
    assert '"sns:*"' not in ingest_policy
    assert '"secretsmanager:*"' not in ingest_policy
    assert '"sqs:*"' not in analytics_policy
    assert '"s3:*"' not in analytics_policy
    assert '"secretsmanager:*"' not in analytics_policy


def test_all_expected_outputs_are_present(terraform_plan):
    outputs = terraform_plan["planned_values"]["outputs"]
    assert set(outputs) == {
        "api_invoke_base_url_v1",
        "api_key_secret_arn",
        "db_app_user_secret_arn",
        "dynamodb_table_name",
        "rds_endpoint_address",
        "rds_endpoint_port",
        "s3_bucket_name",
        "sns_topic_arn",
        "sqs_queue_url",
    }


def test_ingest_lambda_handler_reads_secret_publishes_and_persists_ttl(monkeypatch):
    published = {}
    written_item = {}
    secret_requests = []

    class FakeSNS:
        def publish(self, **kwargs):
            published.update(kwargs)

    class FakeSecretsManager:
        def get_secret_value(self, **kwargs):
            secret_requests.append(kwargs)
            return {"SecretString": "CHANGE_ME"}

    class FakeTable:
        def put_item(self, Item):
            written_item.update(Item)

    class FakeDynamoDB:
        def Table(self, name):
            assert name == "order-metadata"
            return FakeTable()

    boto3_module = types.ModuleType("boto3")
    boto3_module.client = lambda service, **kwargs: {
        "sns": FakeSNS(),
        "secretsmanager": FakeSecretsManager(),
    }[service]
    boto3_module.resource = lambda service, **kwargs: {"dynamodb": FakeDynamoDB()}[service]

    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:order-events")
    monkeypatch.setenv("TABLE_NAME", "order-metadata")
    monkeypatch.setenv("API_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:orderintake/api_key")

    module = _load_lambda_module("ingest_fn", boto3_module)
    response = module.lambda_handler({}, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"ok": True}
    assert secret_requests == [{"SecretId": os.environ["API_SECRET_ARN"]}]
    assert json.loads(published["Message"]) == {"event": "order_received", "source": "api"}
    assert written_item["pk"] == "ORDER"
    assert written_item["sk"] == "STATIC"
    assert written_item["source"] == "api"
    assert "ttl" in written_item


def test_analytics_lambda_handler_reads_secret_consumes_queue_and_writes_marker(monkeypatch):
    deleted = []
    written_object = {}
    secret_requests = []

    class FakeSQS:
        def receive_message(self, **kwargs):
            return {"Messages": [{"ReceiptHandle": "abc"}]}

        def delete_message(self, **kwargs):
            deleted.append(kwargs)

    class FakeS3:
        def put_object(self, **kwargs):
            written_object.update(kwargs)

    class FakeSecretsManager:
        def get_secret_value(self, **kwargs):
            secret_requests.append(kwargs)
            return {"SecretString": json.dumps({"username": "appuser", "password": "CHANGE_ME"})}

    boto3_module = types.ModuleType("boto3")
    boto3_module.client = lambda service, **kwargs: {
        "sqs": FakeSQS(),
        "s3": FakeS3(),
        "secretsmanager": FakeSecretsManager(),
    }[service]

    monkeypatch.setenv("QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/order-events-queue")
    monkeypatch.setenv("BUCKET_NAME", "order-intake-archive")
    monkeypatch.setenv("DB_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:orderintake/db_app_user")

    module = _load_lambda_module("analytics_fn", boto3_module)
    response = module.lambda_handler({}, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"processed": True}
    assert secret_requests == [{"SecretId": os.environ["DB_SECRET_ARN"]}]
    assert deleted == [{"QueueUrl": os.environ["QUEUE_URL"], "ReceiptHandle": "abc"}]
    assert written_object["Bucket"] == os.environ["BUCKET_NAME"]
    assert written_object["Key"] == "raw/analytics-marker.txt"
