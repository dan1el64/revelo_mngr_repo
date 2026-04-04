import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"
PLAN_JSON = ROOT / "plan.json"


def load_state():
    if STATE_JSON.exists():
        return json.loads(STATE_JSON.read_text())
    if PLAN_JSON.exists():
        plan = json.loads(PLAN_JSON.read_text())
        planned_values = plan.get("planned_values")
        assert planned_values, f"plan.json found at {PLAN_JSON} but missing planned_values"
        return {"values": planned_values}
    raise AssertionError(
        f"Neither state.json nor plan.json was found under {ROOT}. "
        "Integration checks require terraform show -json output from either apply or plan."
    )


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def resources_by_type(state, resource_type):
    root_module = state.get("values", {}).get("root_module", {})
    return [r for r in iter_resources(root_module) if r.get("type") == resource_type]


def resource_values(resource):
    return resource.get("values", {})


def test_state_contains_expected_outputs():
    state = load_state()
    outputs = state.get("values", {}).get("outputs", {})

    assert outputs, "Expected Terraform outputs for operational verification"
    assert "sqs_queue_url" in outputs
    assert "http_api_endpoint_url" in outputs
    assert "rds_endpoint_address" in outputs
    assert "secrets_manager_secret_arn" in outputs

    for output_name in outputs:
        if "value" not in outputs[output_name]:
            continue
        value = outputs[output_name]["value"]
        if value is not None:
            assert isinstance(value, str)


def test_state_resource_counts_match_the_prompt():
    state = load_state()

    expected_counts = {
        "aws_vpc": 1,
        "aws_subnet": 2,
        "aws_security_group": 2,
        "random_password": 1,
        "aws_secretsmanager_secret": 1,
        "aws_secretsmanager_secret_version": 1,
        "aws_db_subnet_group": (0, 1),
        "aws_db_instance": (0, 1),
        "aws_apigatewayv2_api": (0, 1),
        "aws_apigatewayv2_stage": (0, 1),
        "aws_apigatewayv2_integration": (0, 2),
        "aws_apigatewayv2_route": (0, 2),
        "aws_sqs_queue": 1,
        "aws_cloudwatch_log_group": 2,
        "aws_lambda_function": 3,
        "aws_lambda_permission": (0, 1),
        "aws_lambda_event_source_mapping": 1,
        "aws_sfn_state_machine": 1,
        "aws_pipes_pipe": (0, 1),
    }

    for resource_type, expected_count in expected_counts.items():
        actual_count = len(resources_by_type(state, resource_type))
        if isinstance(expected_count, tuple):
            assert actual_count in expected_count, (
                f"Expected {resource_type} count in {expected_count}, got {actual_count}"
            )
        else:
            assert actual_count == expected_count, (
                f"Expected {expected_count} resources of type {resource_type}, got {actual_count}"
            )


def test_deployed_queue_database_and_api_values_match_contract():
    state = load_state()

    queue = resource_values(resources_by_type(state, "aws_sqs_queue")[0])
    db_resources = resources_by_type(state, "aws_db_instance")
    stage_resources = resources_by_type(state, "aws_apigatewayv2_stage")
    api_resources = resources_by_type(state, "aws_apigatewayv2_api")
    queue_policy_resources = resources_by_type(state, "aws_sqs_queue_policy")
    db_instance = resource_values(db_resources[0]) if db_resources else {}
    stage = resource_values(stage_resources[0]) if stage_resources else {}
    api = resource_values(api_resources[0]) if api_resources else {}
    queue_policy = resource_values(queue_policy_resources[0]) if queue_policy_resources else {}
    api_roles = [
        resource_values(resource)
        for resource in resources_by_type(state, "aws_iam_role")
        if "apigateway.amazonaws.com" in resource_values(resource).get("assume_role_policy", "")
    ]
    api_role = api_roles[0] if api_roles else {}
    log_groups = [resource_values(resource) for resource in resources_by_type(state, "aws_cloudwatch_log_group")]
    lambdas = [resource_values(resource) for resource in resources_by_type(state, "aws_lambda_function")]

    if queue:
        assert queue["visibility_timeout_seconds"] == 60
        assert queue["message_retention_seconds"] == 1209600
    if db_instance:
        assert db_instance["engine"] == "postgres"
        assert db_instance["engine_version"] == "15.4"
        assert db_instance["instance_class"] == "db.t3.micro"
        assert db_instance["allocated_storage"] == 20
        assert db_instance["storage_type"] == "gp2"
        assert db_instance["publicly_accessible"] is False
        assert db_instance["enabled_cloudwatch_logs_exports"] == ["postgresql"]
        assert db_instance["skip_final_snapshot"] is True

    if api:
        assert api["protocol_type"] == "HTTP"
    if stage:
        assert stage["name"] == "$default"
        assert stage["auto_deploy"] is True
    assert len(log_groups) == 2
    assert all(not group or group["retention_in_days"] == 14 for group in log_groups)
    assert all(not group or not group.get("kms_key_id") for group in log_groups)
    assert all(not function or function["timeout"] == 15 for function in lambdas)
    assert all(not function or function["memory_size"] == 256 for function in lambdas)
    if queue_policy:
        assert "sqs:SendMessage" in queue_policy["policy"]
        if api_role.get("arn"):
            assert api_role["arn"] in queue_policy["policy"]
        if queue.get("arn"):
            assert queue["arn"] in queue_policy["policy"]
        assert "sqs:PurgeQueue" not in queue_policy["policy"]


def test_worker_and_health_lambdas_keep_their_network_and_permission_boundaries():
    state = load_state()

    lambdas = resources_by_type(state, "aws_lambda_function")
    event_source_mapping_resource = resources_by_type(state, "aws_lambda_event_source_mapping")[0]
    event_source_mapping = resource_values(event_source_mapping_resource)
    lambda_permission_resources = resources_by_type(state, "aws_lambda_permission")
    lambda_permission = resource_values(lambda_permission_resources[0]) if lambda_permission_resources else {}

    worker = next(
        resource_values(resource)
        for resource in lambdas
        if resource.get("name") == "worker"
        or resource_values(resource).get("function_name", "").endswith("-worker")
        or resource_values(resource).get("vpc_config")
    )
    health = next(
        resource_values(resource)
        for resource in lambdas
        if resource.get("name") == "health"
        or resource_values(resource).get("function_name", "").endswith("-health")
    )

    if event_source_mapping.get("function_name") and worker.get("function_name"):
        assert event_source_mapping["function_name"].split(":")[-1] == worker["function_name"]
    if lambda_permission.get("function_name") and health.get("function_name"):
        assert lambda_permission["function_name"].split(":")[-1] == health["function_name"]

    assert worker["runtime"] == "python3.12"
    assert worker["handler"] == "app.handler"
    assert worker["timeout"] == 15
    assert worker["memory_size"] == 256
    assert worker["vpc_config"], "Worker Lambda must be attached to the VPC"
    worker_vpc_config = worker["vpc_config"][0]
    if "subnet_ids" in worker_vpc_config:
        assert len(worker_vpc_config["subnet_ids"]) == 2
    if "security_group_ids" in worker_vpc_config:
        assert len(worker_vpc_config["security_group_ids"]) == 1

    assert health["runtime"] == "python3.12"
    assert health["handler"] == "app.handler"
    assert health["timeout"] == 15
    assert health["memory_size"] == 256
    assert health["vpc_config"] == [], "Health Lambda must not be in the VPC"
