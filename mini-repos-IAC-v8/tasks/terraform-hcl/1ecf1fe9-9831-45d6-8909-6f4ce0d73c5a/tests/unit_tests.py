import re

from tf_helpers import planned_resources, policy_documents, read_main_tf, resources_of_type


ALLOWED_WILDCARD_RESOURCE_ACTIONS = {
    "ec2:AssignPrivateIpAddresses",
    "ec2:CreateNetworkInterface",
    "ec2:DeleteNetworkInterface",
    "ec2:DescribeNetworkInterfaces",
    "ec2:DescribeSecurityGroups",
    "ec2:DescribeSubnets",
    "ec2:DescribeVpcs",
    "ec2:UnassignPrivateIpAddresses",
    "logs:CreateLogDelivery",
    "logs:DeleteLogDelivery",
    "logs:DescribeLogGroups",
    "logs:DescribeResourcePolicies",
    "logs:GetLogDelivery",
    "logs:ListLogDeliveries",
    "logs:PutResourcePolicy",
    "logs:UpdateLogDelivery",
}


def _count(pattern: str, text: str) -> int:
    return len(re.findall(pattern, text, re.DOTALL))


def _resource_values(resource):
    return resource.get("values", {})


def _by_type(resources, type_name):
    return resources_of_type(resources, type_name)


def _only(resources, type_name):
    matches = _by_type(resources, type_name)
    assert len(matches) == 1, f"Expected exactly one {type_name}, found {len(matches)}"
    return matches[0]


def _resource_block(text: str, type_name: str, name: str) -> str:
    pattern = rf'resource "{re.escape(type_name)}" "{re.escape(name)}" \{{'
    match = re.search(pattern, text)
    assert match, f"Could not find resource block {type_name}.{name}"
    start = match.start()
    next_resource = re.search(r'\n(resource|data) "', text[match.end():])
    end = len(text) if not next_resource else match.end() + next_resource.start()
    return text[start:end]


def _assert_resource_block_count(text: str, type_name: str, expected_count: int):
    actual = len(re.findall(rf'resource\s+"{re.escape(type_name)}"\s+"[^"]+"\s+\{{', text))
    assert actual == expected_count, f"Expected {expected_count} declared {type_name} blocks, found {actual}"


def test_main_tf_declares_only_required_inputs_and_provider_configuration():
    main_tf = read_main_tf()
    declared_variables = re.findall(r'variable\s+"([^"]+)"', main_tf)

    assert set(declared_variables) == {
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    }

    provider_block = re.search(r'provider "aws" \{.*?\n\}', main_tf, re.DOTALL)
    assert provider_block, "Expected an aws provider block"

    for expected_ref in (
        "var.aws_region",
        "var.aws_endpoint",
        "var.aws_access_key_id",
        "var.aws_secret_access_key",
    ):
        assert expected_ref in provider_block.group(0)

    assert "resource_prefix" not in main_tf
    assert 'source  = "hashicorp/external"' not in main_tf
    assert "arn:aws:" not in main_tf


def test_main_tf_declares_the_full_prompt_inventory_as_source_contract():
    main_tf = read_main_tf()

    declared_counts = {
        "aws_api_gateway_deployment": 1,
        "aws_api_gateway_integration": 1,
        "aws_api_gateway_method": 1,
        "aws_api_gateway_resource": 1,
        "aws_api_gateway_rest_api": 1,
        "aws_api_gateway_stage": 1,
        "aws_cloudwatch_log_group": 3,
        "aws_db_instance": 1,
        "aws_db_subnet_group": 1,
        "aws_glue_catalog_database": 1,
        "aws_glue_connection": 1,
        "aws_glue_crawler": 1,
        "aws_iam_role": 5,
        "aws_iam_role_policy": 5,
        "aws_internet_gateway": 1,
        "aws_lambda_function": 2,
        "aws_lambda_permission": 1,
        "aws_pipes_pipe": 1,
        "aws_redshift_cluster": 1,
        "aws_redshift_subnet_group": 1,
        "aws_route_table": 2,
        "aws_route_table_association": 4,
        "aws_secretsmanager_secret": 2,
        "aws_secretsmanager_secret_version": 2,
        "aws_sfn_state_machine": 1,
        "aws_security_group": 4,
        "aws_sns_topic": 1,
        "aws_sns_topic_subscription": 1,
        "aws_sqs_queue": 1,
        "aws_sqs_queue_policy": 1,
        "aws_subnet": 4,
        "aws_vpc": 1,
        "aws_vpc_endpoint": 2,
    }

    for type_name, expected_count in declared_counts.items():
        _assert_resource_block_count(main_tf, type_name, expected_count)


def test_plan_contains_the_exact_full_prompt_inventory():
    resources = planned_resources()

    expected_counts = {
        "aws_api_gateway_deployment": 1,
        "aws_api_gateway_integration": 1,
        "aws_api_gateway_method": 1,
        "aws_api_gateway_resource": 1,
        "aws_api_gateway_rest_api": 1,
        "aws_api_gateway_stage": 1,
        "aws_cloudwatch_log_group": 3,
        "aws_db_instance": 1,
        "aws_db_subnet_group": 1,
        "aws_glue_catalog_database": 1,
        "aws_glue_connection": 1,
        "aws_glue_crawler": 1,
        "aws_iam_role": 5,
        "aws_iam_role_policy": 5,
        "aws_internet_gateway": 1,
        "aws_lambda_function": 2,
        "aws_lambda_permission": 1,
        "aws_pipes_pipe": 1,
        "aws_redshift_cluster": 1,
        "aws_redshift_subnet_group": 1,
        "aws_route_table": 2,
        "aws_route_table_association": 4,
        "aws_secretsmanager_secret": 2,
        "aws_secretsmanager_secret_version": 2,
        "aws_sfn_state_machine": 1,
        "aws_security_group": 4,
        "aws_sns_topic": 1,
        "aws_sns_topic_subscription": 1,
        "aws_sqs_queue": 1,
        "aws_sqs_queue_policy": 1,
        "aws_subnet": 4,
        "aws_vpc": 1,
        "aws_vpc_endpoint": 2,
        "random_password": 2,
    }

    for type_name, expected_count in expected_counts.items():
        actual = len(_by_type(resources, type_name))
        assert actual == expected_count, f"Expected {expected_count} {type_name}, found {actual}"


def test_lambda_workflow_and_api_configuration_match_prompt():
    main_tf = read_main_tf()
    resources = planned_resources()
    lambdas = _by_type(resources, "aws_lambda_function")
    lambda_timeouts = sorted(_resource_values(item)["timeout"] for item in lambdas)

    assert len(re.findall(r'data "archive_file" "', main_tf)) == 2
    assert _count(r'source \{\s*content\s*=.*?filename\s*=\s*"index\.py"', main_tf) == 2
    assert '"error": "order_id is required"' in main_tf
    assert len(lambdas) == 2
    assert {_resource_values(item)["runtime"] for item in lambdas} == {"python3.12"}
    assert {_resource_values(item)["handler"] for item in lambdas} == {"index.handler"}
    assert {_resource_values(item)["memory_size"] for item in lambdas} == {256}
    assert lambda_timeouts == [10, 15]

    state_machine_block = _resource_block(main_tf, "aws_sfn_state_machine", "orders")
    state_machine = _only(resources, "aws_sfn_state_machine")
    assert _resource_values(state_machine)["type"] == "STANDARD"
    assert 'StartAt = "LambdaA"' in state_machine_block
    assert "Resource = aws_lambda_function.lambda_a.arn" in state_machine_block
    assert 'Next     = "LambdaB"' in state_machine_block
    assert "Resource = aws_lambda_function.lambda_b.arn" in state_machine_block
    assert "End      = true" in state_machine_block

    pipe_resource_block = _resource_block(main_tf, "aws_pipes_pipe", "orders")
    assert "source   = aws_sqs_queue.order_events.arn" in pipe_resource_block
    assert "enrichment = aws_lambda_function.lambda_a.arn" in pipe_resource_block
    assert "target   = aws_sfn_state_machine.orders.arn" in pipe_resource_block
    assert 'invocation_type = "FIRE_AND_FORGET"' in pipe_resource_block
    assert "count" not in pipe_resource_block
    assert len(_by_type(resources, "aws_pipes_pipe")) == 1

    api_stage = _only(resources, "aws_api_gateway_stage")
    assert _resource_values(api_stage)["stage_name"] == "prod"
    _only(resources, "aws_lambda_permission")
    lambda_permission_block = _resource_block(main_tf, "aws_lambda_permission", "api_gateway_to_lambda_a")
    assert "aws_api_gateway_rest_api.orders.execution_arn" in lambda_permission_block
    assert '/${local.api_stage_name}/POST/orders' in lambda_permission_block


def test_network_topology_routes_and_subnet_groups_are_strict():
    main_tf = read_main_tf()
    resources = planned_resources()
    vpc = _only(resources, "aws_vpc")
    subnets = _by_type(resources, "aws_subnet")
    route_tables = _by_type(resources, "aws_route_table")
    log_groups = _by_type(resources, "aws_cloudwatch_log_group")

    assert _resource_values(vpc)["cidr_block"] == "10.0.0.0/16"

    subnet_by_cidr = {_resource_values(item)["cidr_block"]: item for item in subnets}
    public_a = subnet_by_cidr["10.0.0.0/24"]
    public_b = subnet_by_cidr["10.0.1.0/24"]
    private_a = subnet_by_cidr["10.0.10.0/24"]
    private_b = subnet_by_cidr["10.0.11.0/24"]

    assert _resource_values(public_a)["availability_zone"] != _resource_values(public_b)["availability_zone"]
    assert _resource_values(private_a)["availability_zone"] != _resource_values(private_b)["availability_zone"]

    igw_block = _resource_block(main_tf, "aws_internet_gateway", "main")
    assert "vpc_id = aws_vpc.main.id" in igw_block

    public_rt_block = _resource_block(main_tf, "aws_route_table", "public")
    private_rt_block = _resource_block(main_tf, "aws_route_table", "private")
    assert 'cidr_block = "0.0.0.0/0"' in public_rt_block
    assert "gateway_id = aws_internet_gateway.main.id" in public_rt_block
    assert 'cidr_block = "0.0.0.0/0"' not in private_rt_block

    public_a_assoc = _resource_block(main_tf, "aws_route_table_association", "public_a")
    public_b_assoc = _resource_block(main_tf, "aws_route_table_association", "public_b")
    private_a_assoc = _resource_block(main_tf, "aws_route_table_association", "private_a")
    private_b_assoc = _resource_block(main_tf, "aws_route_table_association", "private_b")
    assert "subnet_id      = aws_subnet.public_a.id" in public_a_assoc
    assert "route_table_id = aws_route_table.public.id" in public_a_assoc
    assert "subnet_id      = aws_subnet.public_b.id" in public_b_assoc
    assert "route_table_id = aws_route_table.public.id" in public_b_assoc
    assert "subnet_id      = aws_subnet.private_a.id" in private_a_assoc
    assert "route_table_id = aws_route_table.private.id" in private_a_assoc
    assert "subnet_id      = aws_subnet.private_b.id" in private_b_assoc
    assert "route_table_id = aws_route_table.private.id" in private_b_assoc

    lambda_sg_block = _resource_block(main_tf, "aws_security_group", "lambda")
    assert 'protocol    = "-1"' in lambda_sg_block
    assert 'cidr_blocks = ["0.0.0.0/0"]' in lambda_sg_block

    db_subnet_group_block = _resource_block(main_tf, "aws_db_subnet_group", "postgres")
    redshift_subnet_group_block = _resource_block(main_tf, "aws_redshift_subnet_group", "analytics")
    assert "aws_subnet.private_a.id" in db_subnet_group_block and "aws_subnet.private_b.id" in db_subnet_group_block
    assert "aws_subnet.private_a.id" in redshift_subnet_group_block and "aws_subnet.private_b.id" in redshift_subnet_group_block
    assert "count" not in db_subnet_group_block
    assert "count" not in redshift_subnet_group_block
    assert len(_by_type(resources, "aws_db_subnet_group")) == 1
    assert len(_by_type(resources, "aws_redshift_subnet_group")) == 1

    for log_group in log_groups:
        assert not _resource_values(log_group).get("kms_key_id")


def test_storage_policies_and_deletion_controls_match_prompt():
    main_tf = read_main_tf()
    resources = planned_resources()
    queue = _only(resources, "aws_sqs_queue")
    topic = _only(resources, "aws_sns_topic")

    queue_values = _resource_values(queue)
    topic_values = _resource_values(topic)
    assert queue_values["visibility_timeout_seconds"] == 30
    assert queue_values["sqs_managed_sse_enabled"] is True
    assert topic_values["kms_master_key_id"] == "alias/aws/sns"

    queue_policy_block = _resource_block(main_tf, "aws_sqs_queue_policy", "order_events")
    assert 'Action   = "sqs:SendMessage"' in queue_policy_block
    assert "Resource = aws_sqs_queue.order_events.arn" in queue_policy_block
    assert '"aws:SourceArn" = aws_sns_topic.order_notifications.arn' in queue_policy_block
    assert 'Service = "sns.amazonaws.com"' in queue_policy_block
    assert len(_by_type(resources, "aws_sqs_queue_policy")) == 1

    forbidden_flags = (
        "prevent_destroy",
        "deletion_protection = true",
        "disable_api_termination = true",
        "termination_protection = true",
        "kms_key_id",
    )
    for flag in forbidden_flags:
        assert flag not in main_tf, f"Unexpected protection or KMS setting found: {flag}"

    db_instance = _only(resources, "aws_db_instance")
    db_values = _resource_values(db_instance)
    assert db_values["identifier"] == "orders-postgres"
    assert db_values["allocated_storage"] == 20
    assert db_values["engine"] == "postgres"
    assert db_values["engine_version"] == "16.3"
    assert db_values["instance_class"] == "db.t3.micro"
    assert db_values["storage_encrypted"] is True
    assert db_values["publicly_accessible"] is False
    assert db_values["skip_final_snapshot"] is True

    redshift_cluster = _only(resources, "aws_redshift_cluster")
    redshift_values = _resource_values(redshift_cluster)
    assert redshift_values["cluster_identifier"] == "orders-analytics"
    assert redshift_values["cluster_type"] == "single-node"
    assert redshift_values["node_type"] == "dc2.large"
    assert redshift_values["database_name"] == "analytics"
    assert redshift_values["encrypted"] is True
    assert redshift_values["publicly_accessible"] is False
    assert redshift_values["port"] == 5439
    assert redshift_values["skip_final_snapshot"] is True

    glue_connection = _only(resources, "aws_glue_connection")
    glue_values = _resource_values(glue_connection)
    glue_connection_block = _resource_block(main_tf, "aws_glue_connection", "redshift")
    assert glue_values["connection_type"] == "JDBC"
    assert 'JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshift_cluster.analytics.dns_name}:5439/analytics"' in glue_connection_block
    assert "SECRET_ID           = aws_secretsmanager_secret.redshift.arn" in glue_connection_block


def test_iam_policies_are_minimal_and_scoped():
    main_tf = read_main_tf()
    resources = planned_resources()

    lambda_a_block = _resource_block(main_tf, "aws_iam_role_policy", "lambda_a")
    lambda_b_block = _resource_block(main_tf, "aws_iam_role_policy", "lambda_b")
    step_functions_block = _resource_block(main_tf, "aws_iam_role_policy", "step_functions")

    assert "aws_secretsmanager_secret.rds.arn" in lambda_a_block
    assert "aws_cloudwatch_log_group.lambda_a.arn" in lambda_a_block
    assert "sqs:ReceiveMessage" not in lambda_a_block
    assert "states:StartExecution" not in lambda_a_block
    assert "aws_secretsmanager_secret.rds.arn" not in lambda_b_block
    assert "aws_sqs_queue.order_events.arn" not in lambda_b_block
    assert "aws_cloudwatch_log_group.lambda_b.arn" in lambda_b_block

    assert "aws_lambda_function.lambda_a.arn" in step_functions_block
    assert "aws_lambda_function.lambda_b.arn" in step_functions_block
    assert "states:StartExecution" not in step_functions_block

    pipe_block = _resource_block(main_tf, "aws_iam_role_policy", "pipe")
    glue_block = _resource_block(main_tf, "aws_iam_role_policy", "glue")
    assert "aws_sqs_queue.order_events.arn" in pipe_block
    assert "aws_lambda_function.lambda_a.arn" in pipe_block
    assert "aws_sfn_state_machine.orders.arn" in pipe_block
    assert "secretsmanager:GetSecretValue" not in pipe_block

    assert "aws_secretsmanager_secret.redshift.arn" in glue_block
    assert "glue:CreateTable" in glue_block
    assert "lambda:InvokeFunction" not in glue_block

    for address, document in policy_documents(resources):
        statements = document.get("Statement", [])
        if isinstance(statements, dict):
            statements = [statements]

        for statement in statements:
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]

            assert actions, f"Policy {address} must include actions"
            assert all(action != "*" for action in actions), f"Policy {address} contains Action='*'"

            resources_list = statement.get("Resource", [])
            if isinstance(resources_list, str):
                resources_list = [resources_list]

            if "*" in resources_list:
                unexpected = set(actions) - ALLOWED_WILDCARD_RESOURCE_ACTIONS
                assert not unexpected, (
                    f"Policy {address} uses Resource='*' for unexpectedly broad actions: {sorted(unexpected)}"
                )
