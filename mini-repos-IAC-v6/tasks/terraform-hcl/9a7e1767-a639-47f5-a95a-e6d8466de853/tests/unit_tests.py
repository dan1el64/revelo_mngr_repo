import json
import re
from pathlib import Path

import pytest


REPO_DIR = Path(__file__).resolve().parent.parent
PLAN_PATH = REPO_DIR / "plan.json"
MAIN_TF_PATH = REPO_DIR / "main.tf"


def load_plan():
    if not PLAN_PATH.exists():
        pytest.fail("plan.json must exist before running unit tests")
    return json.loads(PLAN_PATH.read_text())


def load_main_tf():
    return MAIN_TF_PATH.read_text()


def planned_resources(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(planned_resources(child))
    return resources


def resources_by_type(plan, resource_type):
    root_module = plan["planned_values"]["root_module"]
    return [
        resource
        for resource in planned_resources(root_module)
        if resource["type"] == resource_type and resource.get("mode", "managed") == "managed"
    ]


def one_resource(plan, resource_type):
    resources = resources_by_type(plan, resource_type)
    assert len(resources) == 1, f"expected exactly one {resource_type}, found {len(resources)}"
    return resources[0]


def resource_values(plan, resource_type, resource_name):
    for resource in resources_by_type(plan, resource_type):
        if resource["name"] == resource_name:
            return resource["values"]
    raise AssertionError(f"{resource_type}.{resource_name} not found")


def is_allow_all_egress(rule):
    return (
        rule.get("protocol") == "-1"
        and rule.get("cidr_blocks") == ["0.0.0.0/0"]
        and rule.get("from_port") == 0
        and rule.get("to_port") == 0
    )


def has_endpoint_gate(main_tf, resource_type, resource_name):
    pattern = (
        rf'resource "{resource_type}" "{resource_name}" \{{'
        r'\s*count\s*=\s*can\(regex\("amazonaws\\\\.com", var\.aws_endpoint\)\) \? 1 : 0'
    )
    return re.search(pattern, main_tf, re.DOTALL) is not None


def test_contract_inputs_provider_and_naming():
    plan = load_plan()
    main_tf = load_main_tf()

    assert sorted(path.name for path in REPO_DIR.glob("*.tf")) == ["main.tf"]
    assert set(plan["configuration"]["root_module"]["variables"].keys()) == {"aws_region", "aws_endpoint"}
    assert re.search(r'variable "aws_endpoint"\s*\{[^}]*type\s*=\s*string', main_tf, re.DOTALL)
    assert not re.search(r'variable "aws_endpoint"\s*\{[^}]*default\s*=', main_tf, re.DOTALL)
    assert 'default     = "us-east-1"' in main_tf
    assert 'region                      = var.aws_region' in main_tf
    assert "endpoints {" in main_tf
    provider_match = re.search(r'provider "aws"\s*\{(.*?)\n\}', main_tf, re.DOTALL)
    assert provider_match is not None
    assert "var.aws_endpoint" in provider_match.group(1)

    assert "locals {" not in main_tf
    for marker in ["random_string", "random_pet", "timestamp()", "uuid()", "${var.name_prefix}", "${local."]:
        assert marker not in main_tf


def test_network_topology_matches_contract():
    plan = load_plan()
    main_tf = load_main_tf()

    assert len(resources_by_type(plan, "aws_vpc")) == 1
    assert len(resources_by_type(plan, "aws_internet_gateway")) == 1
    assert len(resources_by_type(plan, "aws_subnet")) == 4
    assert len(resources_by_type(plan, "aws_route_table")) == 2
    assert len(resources_by_type(plan, "aws_route")) == 1
    assert len(resources_by_type(plan, "aws_route_table_association")) == 4
    assert len(resources_by_type(plan, "aws_vpc_endpoint")) == 1

    vpc = one_resource(plan, "aws_vpc")["values"]
    assert vpc["cidr_block"] == "10.20.0.0/16"

    subnets = resources_by_type(plan, "aws_subnet")
    assert {subnet["values"]["cidr_block"] for subnet in subnets} == {
        "10.20.0.0/24",
        "10.20.1.0/24",
        "10.20.10.0/24",
        "10.20.11.0/24",
    }

    public_subnets = [subnet["values"] for subnet in subnets if subnet["values"]["map_public_ip_on_launch"]]
    private_subnets = [subnet["values"] for subnet in subnets if not subnet["values"]["map_public_ip_on_launch"]]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert len({subnet["availability_zone"] for subnet in public_subnets}) == 2
    assert len({subnet["availability_zone"] for subnet in private_subnets}) == 2
    assert {subnet["availability_zone"] for subnet in public_subnets} == {subnet["availability_zone"] for subnet in private_subnets}

    route = one_resource(plan, "aws_route")["values"]
    assert route["destination_cidr_block"] == "0.0.0.0/0"
    assert "route_table_id         = aws_route_table.public.id" in main_tf

    private_route_table = resource_values(plan, "aws_route_table", "private")
    default_routes = [
        planned_route["values"]
        for planned_route in resources_by_type(plan, "aws_route")
        if planned_route["values"]["destination_cidr_block"] == "0.0.0.0/0"
    ]
    assert len(default_routes) == 1
    assert "route_table_id         = aws_route_table.private.id" not in main_tf
    assert private_route_table.get("route") in (None, [])

    endpoint = one_resource(plan, "aws_vpc_endpoint")["values"]
    assert endpoint["vpc_endpoint_type"] == "Gateway"
    assert 'route_table_ids   = [aws_route_table.private.id]' in main_tf


def test_security_groups_and_private_wiring_match_contract():
    plan = load_plan()
    main_tf = load_main_tf()

    assert len(resources_by_type(plan, "aws_security_group")) == 3

    api_sg = resource_values(plan, "aws_security_group", "api")
    assert len(api_sg["ingress"]) == 1
    assert api_sg["ingress"][0]["from_port"] == 443
    assert api_sg["ingress"][0]["to_port"] == 443
    assert api_sg["ingress"][0]["cidr_blocks"] == ["0.0.0.0/0"]
    assert len(api_sg["egress"]) == 1
    assert is_allow_all_egress(api_sg["egress"][0])

    worker_sg = resource_values(plan, "aws_security_group", "worker")
    assert worker_sg.get("ingress", []) == []
    assert len(worker_sg["egress"]) == 1
    assert is_allow_all_egress(worker_sg["egress"][0])

    database_sg = resource_values(plan, "aws_security_group", "database")
    assert len(database_sg["ingress"]) == 1
    assert database_sg["ingress"][0]["from_port"] == 5432
    assert database_sg["ingress"][0]["to_port"] == 5432

    assert "security_groups = [aws_security_group.worker.id]" in main_tf
    assert 'security_group_ids = [aws_security_group.worker.id]' in main_tf
    assert 'subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in main_tf
    assert 'vpc_security_group_ids = [aws_security_group.database.id]' in main_tf


def test_compute_resources_match_contract():
    plan = load_plan()
    main_tf = load_main_tf()
    normalized = main_tf.replace(" ", "")

    assert len(resources_by_type(plan, "aws_lambda_function")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_rest_api")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_resource")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_method")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_integration")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_deployment")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_stage")) == 1
    assert len(resources_by_type(plan, "aws_lambda_permission")) == 1
    assert len(resources_by_type(plan, "aws_sqs_queue")) == 1
    assert len(resources_by_type(plan, "aws_lambda_event_source_mapping")) == 1
    assert len(resources_by_type(plan, "aws_sfn_state_machine")) == 1
    pipe_resources = resources_by_type(plan, "aws_pipes_pipe")
    assert len(pipe_resources) in (0, 1)
    assert len(resources_by_type(plan, "aws_cloudwatch_log_group")) == 2

    function = one_resource(plan, "aws_lambda_function")["values"]
    assert function["runtime"] == "python3.12"
    assert function["memory_size"] == 256
    assert function["timeout"] == 10
    assert function["package_type"] == "Zip"

    queue = one_resource(plan, "aws_sqs_queue")["values"]
    assert queue["visibility_timeout_seconds"] == 30
    assert queue["sqs_managed_sse_enabled"] is True

    resource = one_resource(plan, "aws_api_gateway_resource")["values"]
    method = one_resource(plan, "aws_api_gateway_method")["values"]
    integration = one_resource(plan, "aws_api_gateway_integration")["values"]
    mapping = one_resource(plan, "aws_lambda_event_source_mapping")["values"]
    assert resource["path_part"] == "ingest"
    assert method["http_method"] == "POST"
    assert method["authorization"] == "NONE"
    assert integration["type"] == "AWS_PROXY"
    assert integration["integration_http_method"] == "POST"
    assert mapping["batch_size"] == 10

    if pipe_resources:
        assert pipe_resources[0]["values"]["desired_state"] == "RUNNING"
    else:
        assert has_endpoint_gate(main_tf, "aws_pipes_pipe", "ingest")

    assert "source=aws_sqs_queue.ingest.arn" in normalized
    assert "enrichment=aws_lambda_function.ingest.arn" in normalized
    assert "target=aws_sfn_state_machine.ingest.arn" in normalized


def test_iam_configuration_matches_contract():
    plan = load_plan()
    main_tf = load_main_tf()

    assert len(resources_by_type(plan, "aws_iam_role")) == 3
    assert len(resources_by_type(plan, "aws_iam_role_policy")) == 3

    for principal in ['Service = "lambda.amazonaws.com"', 'Service = "states.amazonaws.com"', 'Service = "pipes.amazonaws.com"']:
        assert principal in main_tf

    for action in [
        '"sqs:SendMessage"',
        '"s3:PutObject"',
        '"secretsmanager:GetSecretValue"',
        '"logs:CreateLogStream"',
        '"logs:PutLogEvents"',
        '"lambda:InvokeFunction"',
        '"states:StartExecution"',
        '"sqs:ReceiveMessage"',
        '"sqs:DeleteMessage"',
        '"sqs:GetQueueAttributes"',
        '"sqs:ChangeMessageVisibility"',
    ]:
        assert action in main_tf

    assert 'Resource = aws_sqs_queue.ingest.arn' in main_tf
    assert 'Resource = "${aws_s3_bucket.archive.arn}/*"' in main_tf
    assert 'Resource = aws_secretsmanager_secret.db.arn' in main_tf
    assert 'Resource = aws_lambda_function.ingest.arn' in main_tf
    assert 'Resource = aws_sfn_state_machine.ingest.arn' in main_tf


def test_storage_and_logging_resources_match_contract():
    plan = load_plan()
    main_tf = load_main_tf()

    assert len(resources_by_type(plan, "aws_s3_bucket")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_server_side_encryption_configuration")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_public_access_block")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_ownership_controls")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_policy")) == 1
    assert len(resources_by_type(plan, "aws_secretsmanager_secret")) == 1
    assert len(resources_by_type(plan, "aws_secretsmanager_secret_version")) == 1
    assert len(resources_by_type(plan, "random_password")) == 1
    db_subnet_groups = resources_by_type(plan, "aws_db_subnet_group")
    db_instances = resources_by_type(plan, "aws_db_instance")
    assert len(db_subnet_groups) in (0, 1)
    assert len(db_instances) in (0, 1)

    assert 'subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in main_tf or 'subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in main_tf.replace("  ", " ")

    password = one_resource(plan, "random_password")["values"]
    assert password["length"] == 20
    assert password["min_numeric"] == 1
    assert password["min_special"] == 1

    if db_instances:
        db = db_instances[0]["values"]
        assert db["engine"] == "postgres"
        assert db["engine_version"] == "16.3"
        assert db["instance_class"] == "db.t3.micro"
        assert db["allocated_storage"] == 20
        assert db["storage_type"] == "gp2"
        assert db["publicly_accessible"] is False
        assert db["skip_final_snapshot"] is True
        assert db["storage_encrypted"] is True
    else:
        assert has_endpoint_gate(main_tf, "aws_db_subnet_group", "payments")
        assert has_endpoint_gate(main_tf, "aws_db_instance", "payments")

    assert 'username = "appuser"' in main_tf
    assert "jsondecode(aws_secretsmanager_secret_version.db.secret_string).username" in main_tf
    assert "jsondecode(aws_secretsmanager_secret_version.db.secret_string).password" in main_tf

    step_functions_log_group = resource_values(plan, "aws_cloudwatch_log_group", "step_functions")
    assert step_functions_log_group.get("kms_key_id") in (None, "")


def test_disallowed_protection_settings_are_not_configured():
    main_tf = load_main_tf()
    for forbidden in [
        "deletion_protection",
        "termination_protection",
        "disable_api_termination",
        "prevent_destroy",
        "retain_on_delete",
        "recovery_window_in_days",
        "snapshot_retention_limit",
    ]:
        assert forbidden not in main_tf
