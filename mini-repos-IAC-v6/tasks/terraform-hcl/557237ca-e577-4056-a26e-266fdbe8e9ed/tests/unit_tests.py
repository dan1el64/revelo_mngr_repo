import json
import os
import subprocess
from functools import lru_cache
from pathlib import Path

import pytest


PLAN_PATH = Path("plan.json")
MAIN_TF_PATH = Path("main.tf")


def aws_region():
    return (
        os.environ.get("TF_VAR_aws_region")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    )


@lru_cache(maxsize=1)
def ensure_plan_json():
    if PLAN_PATH.exists():
        return

    if os.environ.get("GENERATE_TERRAFORM_ARTIFACTS") != "1":
        pytest.skip("plan.json no existe; el harness debe generarlo antes de correr pytest")

    env = os.environ.copy()
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_REGION", aws_region())

    subprocess.run(["terraform", "init"], check=True, env=env)
    subprocess.run(["terraform", "plan", "-input=false", "-out=.tfplan"], check=True, env=env)
    with PLAN_PATH.open("w") as plan_file:
        subprocess.run(
            ["terraform", "show", "-json", ".tfplan"],
            check=True,
            env=env,
            stdout=plan_file,
        )


def load_plan():
    ensure_plan_json()
    return json.loads(PLAN_PATH.read_text())


def load_main_tf():
    return MAIN_TF_PATH.read_text()


def collect_resources(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(collect_resources(child))
    return resources


def resources_by_type(plan, resource_type):
    root_module = plan["planned_values"]["root_module"]
    return [
        resource
        for resource in collect_resources(root_module)
        if resource["type"] == resource_type and resource.get("mode", "managed") == "managed"
    ]


def resource_names(plan, resource_type):
    return {resource["name"] for resource in resources_by_type(plan, resource_type)}


def has_full_data_plane(plan):
    return len(resources_by_type(plan, "aws_db_instance")) == 1


def test_declares_only_expected_input_variables():
    plan = load_plan()
    variables = set(plan["configuration"]["root_module"]["variables"].keys())
    assert variables == {"aws_region", "aws_endpoint"}


def test_provider_routes_aws_services():
    main_tf = load_main_tf()

    assert 'region                      = var.aws_region' in main_tf
    assert "endpoints {" in main_tf
    assert "var.aws_endpoint" in main_tf


def test_network_topology_and_security_counts():
    plan = load_plan()

    assert len(resources_by_type(plan, "aws_vpc")) == 1
    assert len(resources_by_type(plan, "aws_internet_gateway")) == 1
    assert len(resources_by_type(plan, "aws_nat_gateway")) == 1
    assert len(resources_by_type(plan, "aws_subnet")) == 4
    assert len(resources_by_type(plan, "aws_security_group")) == 2
    assert len(resources_by_type(plan, "aws_security_group_rule")) == 3

    subnets = resources_by_type(plan, "aws_subnet")
    assert sum(1 for subnet in subnets if subnet["values"]["map_public_ip_on_launch"]) == 2
    assert sum(1 for subnet in subnets if not subnet["values"]["map_public_ip_on_launch"]) == 2


def test_persistence_resources_match_prompt():
    plan = load_plan()

    buckets = resources_by_type(plan, "aws_s3_bucket")
    db_instances = resources_by_type(plan, "aws_db_instance")
    db_subnet_groups = resources_by_type(plan, "aws_db_subnet_group")
    secrets = resources_by_type(plan, "aws_secretsmanager_secret")
    secret_versions = resources_by_type(plan, "aws_secretsmanager_secret_version")

    assert len(buckets) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_server_side_encryption_configuration")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_versioning")) == 1
    assert len(resources_by_type(plan, "aws_s3_bucket_public_access_block")) == 1
    expected_rds_count = 1 if has_full_data_plane(plan) else 0
    assert len(db_instances) == expected_rds_count
    assert len(db_subnet_groups) == expected_rds_count
    assert len(secrets) == 1
    assert len(secret_versions) == 1

    bucket = buckets[0]["values"]
    assert bucket["force_destroy"] is True

    if has_full_data_plane(plan):
        db = db_instances[0]["values"]
        assert db["engine"] == "postgres"
        assert db["engine_version"] == "15.4"
        assert db["instance_class"] == "db.t3.micro"
        assert db["allocated_storage"] == 20
        assert db["storage_encrypted"] is True
        assert db["publicly_accessible"] is False
        assert db["backup_retention_period"] == 0
        assert db["deletion_protection"] is False
        assert db["skip_final_snapshot"] is True


def test_rds_uses_secret_values_without_inline_password():
    main_tf = load_main_tf()

    assert 'resource "aws_secretsmanager_secret" "db"' in main_tf
    assert 'resource "aws_secretsmanager_secret_version" "db"' in main_tf
    assert 'username = local.db_username' in main_tf
    assert 'password = random_password.db.result' in main_tf
    assert 'username                 = jsondecode(aws_secretsmanager_secret_version.db.secret_string).username' in main_tf
    assert 'password                 = jsondecode(aws_secretsmanager_secret_version.db.secret_string).password' in main_tf
    assert 'password                 = "admin"' not in main_tf
    assert 'password                 = "postgres"' not in main_tf


def test_api_gateway_shape_and_stage():
    plan = load_plan()

    assert len(resources_by_type(plan, "aws_api_gateway_rest_api")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_resource")) == 3
    assert len(resources_by_type(plan, "aws_api_gateway_method")) == 3
    assert len(resources_by_type(plan, "aws_api_gateway_integration")) == 3
    assert len(resources_by_type(plan, "aws_api_gateway_deployment")) == 1
    assert len(resources_by_type(plan, "aws_api_gateway_stage")) == 1
    assert len(resources_by_type(plan, "aws_lambda_permission")) == 1

    methods = {resource["values"]["http_method"] for resource in resources_by_type(plan, "aws_api_gateway_method")}
    assert methods == {"POST", "GET"}

    stage = resources_by_type(plan, "aws_api_gateway_stage")[0]["values"]
    assert stage["stage_name"] == "v1"

    integrations = resources_by_type(plan, "aws_api_gateway_integration")
    assert {integration["values"]["type"] for integration in integrations} == {"AWS_PROXY"}

    resource_path_parts = {resource["values"]["path_part"] for resource in resources_by_type(plan, "aws_api_gateway_resource")}
    assert resource_path_parts == {"orders", "{id}", "notify"}


def test_eventing_and_workflow_resources_exist_once():
    plan = load_plan()

    assert len(resources_by_type(plan, "aws_sqs_queue")) == 1
    assert len(resources_by_type(plan, "aws_sns_topic")) == 1
    assert len(resources_by_type(plan, "aws_cloudwatch_event_bus")) == 1
    assert len(resources_by_type(plan, "aws_cloudwatch_event_rule")) == 1
    assert len(resources_by_type(plan, "aws_cloudwatch_event_target")) == 1
    assert len(resources_by_type(plan, "aws_sqs_queue_policy")) == 1
    assert len(resources_by_type(plan, "aws_pipes_pipe")) == (1 if has_full_data_plane(plan) else 0)
    assert len(resources_by_type(plan, "aws_sfn_state_machine")) == 1

    queue = resources_by_type(plan, "aws_sqs_queue")[0]["values"]
    assert queue["visibility_timeout_seconds"] == 60
    assert queue["sqs_managed_sse_enabled"] is True


def test_configuration_declares_pipe_wiring():
    main_tf = load_main_tf()

    assert 'resource "aws_pipes_pipe" "main"' in main_tf
    assert "source     = aws_sqs_queue.orders.arn" in main_tf
    assert "enrichment = aws_lambda_function.enrichment_handler.arn" in main_tf
    assert "target     = aws_sfn_state_machine.main.arn" in main_tf


def test_lambda_source_logic_matches_prompt():
    main_tf = load_main_tf()

    assert "PutObjectCommand" in main_tf
    assert "GetObjectCommand" in main_tf
    assert "PutEventsCommand" in main_tf
    assert 'Source: "orders.api"' in main_tf
    assert 'DetailType: "order.created"' in main_tf
    assert "PublishCommand" in main_tf
    assert "Subject: `Order $${orderId} notification`" in main_tf
    assert "originalBody: event.body ?? event" in main_tf
    assert "enriched: true" in main_tf


def test_lambda_and_iam_footprint_matches_prompt():
    plan = load_plan()

    assert len(resources_by_type(plan, "aws_lambda_function")) == 2
    assert len(resources_by_type(plan, "aws_cloudwatch_log_group")) == 2
    assert len(resources_by_type(plan, "aws_iam_role")) == 4
    assert len(resources_by_type(plan, "aws_iam_role_policy")) == 3

    log_groups = resources_by_type(plan, "aws_cloudwatch_log_group")
    assert {group["values"]["retention_in_days"] for group in log_groups} == {14}

    functions = resources_by_type(plan, "aws_lambda_function")
    for function in functions:
        values = function["values"]
        assert values["runtime"] == "nodejs20.x"
        assert values["handler"] == "index.handler"
        assert values["memory_size"] == 256
        assert values["timeout"] == 15
        assert len(values["vpc_config"]) == 1

    assert resource_names(plan, "aws_iam_role") == {"api_handler", "enrichment_handler", "pipe", "sfn"}


def test_lambda_network_rules_and_no_retain_settings():
    plan = load_plan()
    main_tf = load_main_tf()

    rules = resources_by_type(plan, "aws_security_group_rule")
    assert any(
        rule["values"]["type"] == "egress"
        and rule["values"]["from_port"] == 5432
        and rule["values"]["to_port"] == 5432
        and rule["values"]["protocol"] == "tcp"
        for rule in rules
    )
    assert any(
        rule["values"]["type"] == "egress"
        and rule["values"]["from_port"] == 443
        and rule["values"]["to_port"] == 443
        and rule["values"]["protocol"] == "tcp"
        and rule["values"]["cidr_blocks"] == ["0.0.0.0/0"]
        for rule in rules
    )
    assert "deletion_protection      = true" not in main_tf
    assert "termination_protection" not in main_tf
    assert "prevent_destroy" not in main_tf
    assert "retention_policy" not in main_tf
