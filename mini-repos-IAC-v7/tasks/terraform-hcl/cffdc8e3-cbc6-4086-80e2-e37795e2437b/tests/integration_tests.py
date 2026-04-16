import pytest
import re

from tf_helpers import assignment, contains, main_text, only_resource_block, resource_blocks, run_terraform


def test_terraform_init_and_validate():
    validate_result = run_terraform("validate", "-no-color")
    provider_errors = (
        "Missing required provider",
        "Failed to query available provider packages",
        "failed to read schema for",
        "failed to instantiate provider",
        "Unrecognized remote plugin message",
    )
    if validate_result.returncode != 0 and any(message in validate_result.stderr for message in provider_errors):
        pytest.skip("terraform validate requires provider plugins that are unavailable in this execution environment")
    assert validate_result.returncode == 0, validate_result.stderr or validate_result.stdout


def test_vpc_and_subnet_layout_matches_prompt():
    vpc = only_resource_block("aws_vpc")
    assert assignment(vpc, "cidr_block") == '"10.20.0.0/16"'
    assert assignment(vpc, "enable_dns_support") == "true"
    assert assignment(vpc, "enable_dns_hostnames") == "true"

    subnets = resource_blocks("aws_subnet")
    assert len(subnets) == 4

    subnet_by_cidr = {assignment(subnet, "cidr_block"): subnet for subnet in subnets}
    assert set(subnet_by_cidr) == {
        '"10.20.0.0/24"',
        '"10.20.1.0/24"',
        '"10.20.10.0/24"',
        '"10.20.11.0/24"',
    }

    assert "data.aws_availability_zones.available.names[0]" in subnet_by_cidr['"10.20.0.0/24"']
    assert "data.aws_availability_zones.available.names[0]" in subnet_by_cidr['"10.20.10.0/24"']
    assert "data.aws_availability_zones.available.names[1]" in subnet_by_cidr['"10.20.1.0/24"']
    assert "data.aws_availability_zones.available.names[1]" in subnet_by_cidr['"10.20.11.0/24"']


def test_internet_access_and_route_tables_match_prompt():
    assert len(resource_blocks("aws_internet_gateway")) == 1
    assert len(resource_blocks("aws_nat_gateway")) == 0

    route_tables = resource_blocks("aws_route_table")
    associations = resource_blocks("aws_route_table_association")
    assert len(route_tables) == 2
    assert len(associations) == 4

    public_route_table = next(block for block in route_tables if "0.0.0.0/0" in block)
    private_route_table = next(block for block in route_tables if "0.0.0.0/0" not in block)
    public_label = re.search(r'resource "aws_route_table" "([A-Za-z0-9_]+)"', public_route_table).group(1)
    private_label = re.search(r'resource "aws_route_table" "([A-Za-z0-9_]+)"', private_route_table).group(1)

    assert contains(public_route_table, r'cidr_block\s*=\s*"0\.0\.0\.0/0"')
    assert ".internet_gateway" in public_route_table or ".igw" in public_route_table or "aws_internet_gateway." in public_route_table
    assert "0.0.0.0/0" not in private_route_table

    public_associations = [block for block in associations if assignment(block, "route_table_id") == f"aws_route_table.{public_label}.id"]
    private_associations = [block for block in associations if assignment(block, "route_table_id") == f"aws_route_table.{private_label}.id"]
    assert len(public_associations) == 2
    assert len(private_associations) == 2


def test_vpc_interface_endpoints_and_security_groups_match_prompt():
    security_groups = resource_blocks("aws_security_group")
    assert len(security_groups) == 3

    database_sg = next(block for block in security_groups if contains(block, r"from_port\s*=\s*5432"))
    endpoint_sg = next(block for block in security_groups if contains(block, r"from_port\s*=\s*443"))
    assert contains(database_sg, r"from_port\s*=\s*5432")
    assert contains(database_sg, r"to_port\s*=\s*5432")

    assert contains(endpoint_sg, r"from_port\s*=\s*443")
    assert contains(endpoint_sg, r"to_port\s*=\s*443")

    backend_ref_db = re.search(r"aws_security_group\.([A-Za-z0-9_]+)\.id", database_sg)
    backend_ref_endpoint = re.search(r"aws_security_group\.([A-Za-z0-9_]+)\.id", endpoint_sg)
    assert backend_ref_db and backend_ref_endpoint
    assert backend_ref_db.group(1) == backend_ref_endpoint.group(1)
    endpoint_sg_ref = re.search(r'resource "aws_security_group" "([A-Za-z0-9_]+)"', endpoint_sg).group(1)

    endpoints = resource_blocks("aws_vpc_endpoint")
    assert len(endpoints) == 5

    expected_services = {
        '"com.amazonaws.${var.aws_region}.secretsmanager"',
        '"com.amazonaws.${var.aws_region}.sqs"',
        '"com.amazonaws.${var.aws_region}.states"',
        '"com.amazonaws.${var.aws_region}.logs"',
        '"com.amazonaws.${var.aws_region}.events"',
    }

    assert {assignment(endpoint, "service_name") for endpoint in endpoints} == expected_services

    for endpoint in endpoints:
        assert assignment(endpoint, "vpc_endpoint_type") == '"Interface"'
        assert assignment(endpoint, "private_dns_enabled") == "true"
        assert "aws_subnet." in endpoint
        assert f"aws_security_group.{endpoint_sg_ref}.id" in endpoint


def test_api_gateway_contract_matches_prompt():
    api = only_resource_block("aws_api_gateway_rest_api")
    assert 'types = ["REGIONAL"]' in api

    assert len(resource_blocks("aws_api_gateway_deployment")) == 1
    assert len(resource_blocks("aws_api_gateway_stage")) == 1
    assert len(resource_blocks("aws_api_gateway_resource")) == 2
    assert len(resource_blocks("aws_api_gateway_method")) == 2
    assert len(resource_blocks("aws_api_gateway_integration")) == 2
    assert len(resource_blocks("aws_lambda_permission")) == 2

    stage = only_resource_block("aws_api_gateway_stage")
    stage_name = assignment(stage, "stage_name")
    assert stage_name is not None
    assert "timestamp(" not in stage_name
    assert not re.fullmatch(r'"[A-Za-z0-9_-]+"', stage_name), "stage_name must be Terraform-derived, not a hardcoded label"
    assert "access_log_settings {" in stage
    assert "aws_cloudwatch_log_group.api_gateway_access_logs.arn" in stage

    api_resources = resource_blocks("aws_api_gateway_resource")
    assert {assignment(block, "path_part") for block in api_resources} == {'"health"', '"orders"'}

    methods = resource_blocks("aws_api_gateway_method")
    assert {assignment(block, "http_method") for block in methods} == {'"GET"', '"POST"'}

    integrations = resource_blocks("aws_api_gateway_integration")
    assert len([block for block in integrations if ".invoke_arn" in block]) == 2

    permissions = resource_blocks("aws_lambda_permission")
    permission_text = "\n".join(permissions)
    assert "/GET/health" in permission_text
    assert "/POST/orders" in permission_text


def test_lambda_packaging_runtime_vpc_and_handler_behavior_match_prompt():
    assert len(resource_blocks("archive_file")) == 2
    lambdas = resource_blocks("aws_lambda_function")
    assert len(lambdas) == 2

    runtime_contract = {
        (assignment(block, "runtime"), assignment(block, "handler"), assignment(block, "memory_size"), assignment(block, "timeout"))
        for block in lambdas
    }
    assert runtime_contract == {
        ('"python3.12"', '"app.handler"', "128", "5"),
        ('"python3.12"', '"app.handler"', "256", "15"),
    }

    for lambda_block in lambdas:
        assert "aws_subnet." in lambda_block
        assert "security_group_ids" in lambda_block
        assert 'package_type = "Image"' not in lambda_block

    text = main_text()
    assert text.count('filename = "app.py"') >= 2
    assert '"statusCode": 200' in text
    assert '\\"status\\":\\"ok\\"' in text
    assert "sqs.send_message" in text
    assert "MessageBody=event['body']" in text or 'MessageBody=event["body"]' in text
    assert '"statusCode": 202' in text


def test_sqs_step_functions_and_pipe_match_prompt():
    queue = only_resource_block("aws_sqs_queue")
    assert assignment(queue, "message_retention_seconds") == "1209600"
    assert assignment(queue, "visibility_timeout_seconds") == "30"
    assert "fifo_queue" not in queue or assignment(queue, "fifo_queue") == "false"

    state_machine = only_resource_block("aws_sfn_state_machine")
    assert assignment(state_machine, "type") == '"STANDARD"'
    assert 'Type = "Pass"' in state_machine
    assert "End = true" in state_machine
    assert "logging_configuration {" in state_machine
    assert "aws_cloudwatch_log_group.state_machine_logs.arn" in state_machine

    pipe = only_resource_block("aws_pipes_pipe")
    assert ".arn" in pipe
    assert "enrichment" not in pipe

    pipe_roles = [block for block in resource_blocks("aws_iam_role") if "pipes.amazonaws.com" in block]
    assert len(pipe_roles) == 1

    pipe_policies = [
        block for block in resource_blocks("aws_iam_role_policy")
        if "sqs:ReceiveMessage" in block and "states:StartExecution" in block
    ]
    assert len(pipe_policies) == 1
    pipe_policy = pipe_policies[0]
    assert "sqs:ReceiveMessage" in pipe_policy
    assert "sqs:DeleteMessage" in pipe_policy
    assert "sqs:GetQueueAttributes" in pipe_policy
    assert ".arn" in pipe_policy
    assert "states:StartExecution" in pipe_policy


def test_iam_scoping_matches_prompt():
    roles = resource_blocks("aws_iam_role")
    assert len(roles) >= 3

    lambda_execution_policies = [
        block for block in resource_blocks("aws_iam_role_policy") + resource_blocks("aws_iam_policy")
        if "logs:PutLogEvents" in block or "sqs:SendMessage" in block or "secretsmanager:GetSecretValue" in block
    ]
    assert lambda_execution_policies, "Expected at least one scoped Lambda policy"

    all_policy_text = "\n".join(resource_blocks("aws_iam_role_policy") + resource_blocks("aws_iam_policy"))
    assert 'Resource = "*"' not in all_policy_text
    assert "/aws/lambda/*" not in all_policy_text

    assert "aws_cloudwatch_log_group.health_handler_logs.arn" in all_policy_text
    assert "aws_cloudwatch_log_group.orders_handler_logs.arn" in all_policy_text

    assert "sqs:SendMessage" in all_policy_text
    assert "aws_sqs_queue.main.arn" in all_policy_text
    assert "secretsmanager:GetSecretValue" in all_policy_text
    assert "aws_secretsmanager_secret.db_credentials.arn" in all_policy_text


def test_database_and_secret_configuration_match_prompt():
    password = only_resource_block("random_password")
    assert assignment(password, "special") == "false"

    secret = only_resource_block("aws_secretsmanager_secret")
    secret_version = only_resource_block("aws_secretsmanager_secret_version")
    assert secret
    assert "username" in secret_version
    assert "password" in secret_version
    assert "random_password.db_password.result" in secret_version

    subnet_group = only_resource_block("aws_db_subnet_group")
    assert "aws_subnet." in subnet_group

    database_sg = next(block for block in resource_blocks("aws_security_group") if contains(block, r"from_port\s*=\s*5432"))
    assert contains(database_sg, r"from_port\s*=\s*5432")
    assert "aws_security_group." in database_sg

    db = only_resource_block("aws_db_instance")
    assert assignment(db, "engine") == '"postgres"'
    assert assignment(db, "instance_class") == '"db.t3.micro"'
    assert assignment(db, "allocated_storage") == "20"
    assert assignment(db, "storage_type") in {'"gp2"', '"gp3"'}
    assert assignment(db, "multi_az") == "false"
    assert assignment(db, "publicly_accessible") == "false"
    assert assignment(db, "skip_final_snapshot") == "true"
    assert assignment(db, "backup_retention_period") == "0"
    assert "aws_db_subnet_group." in db
    assert "aws_security_group." in db
    assert "enabled_cloudwatch_logs_exports" in db and "postgresql" in db
    assert "jsondecode(" in db, "DB credentials must be sourced from Secrets Manager"
    assert 'username = "admin"' not in db
    assert "random_password.db_password.result" not in db


def test_observability_alarms_match_prompt():
    alarms = resource_blocks("aws_cloudwatch_metric_alarm")
    assert len(alarms) == 3

    lambda_alarm = next(block for block in alarms if assignment(block, "metric_name") == '"Errors"')
    assert assignment(lambda_alarm, "metric_name") == '"Errors"'
    assert assignment(lambda_alarm, "namespace") == '"AWS/Lambda"'
    assert assignment(lambda_alarm, "comparison_operator") == '"GreaterThanThreshold"'
    assert assignment(lambda_alarm, "evaluation_periods") == '"1"'
    assert assignment(lambda_alarm, "period") == "60"
    assert assignment(lambda_alarm, "threshold") == "0"
    assert "FunctionName" in lambda_alarm and "aws_lambda_function." in lambda_alarm

    api_alarm = next(block for block in alarms if assignment(block, "metric_name") == '"5XXError"')
    assert assignment(api_alarm, "metric_name") == '"5XXError"'
    assert assignment(api_alarm, "namespace") == '"AWS/ApiGateway"'
    assert assignment(api_alarm, "comparison_operator") == '"GreaterThanThreshold"'
    assert assignment(api_alarm, "evaluation_periods") == '"1"'
    assert assignment(api_alarm, "period") == "60"
    assert assignment(api_alarm, "threshold") == "0"
    assert "aws_api_gateway_rest_api." in api_alarm
    assert "aws_api_gateway_stage." in api_alarm

    rds_alarm = next(block for block in alarms if assignment(block, "metric_name") == '"CPUUtilization"')
    assert assignment(rds_alarm, "metric_name") == '"CPUUtilization"'
    assert assignment(rds_alarm, "namespace") == '"AWS/RDS"'
    assert assignment(rds_alarm, "comparison_operator") == '"GreaterThanOrEqualToThreshold"'
    assert assignment(rds_alarm, "evaluation_periods") == '"5"'
    assert assignment(rds_alarm, "period") == "60"
    assert assignment(rds_alarm, "threshold") == "80"
    assert "aws_db_instance." in rds_alarm
