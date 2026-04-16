from tf_helpers import parse_json_string, policy_documents, reduced_endpoint_mode, resources_of_type, security_group_rules, state_resources


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


def _resource_values(resource):
    return resource.get("values", {})


def _by_type(resources, type_name):
    return resources_of_type(resources, type_name)


def _only(resources, type_name):
    matches = _by_type(resources, type_name)
    assert len(matches) == 1, f"Expected exactly one {type_name}, found {len(matches)}"
    return matches[0]


def _task_targets_lambda(task_state, lambda_arns):
    if task_state.get("Resource") in lambda_arns:
        return True

    for field in ("Arguments", "Parameters"):
        payload = task_state.get(field, {})
        if isinstance(payload, dict) and payload.get("FunctionName") in lambda_arns:
            return True

    return False


def test_state_contains_the_exact_prompt_inventory():
    resources = state_resources()
    reduced_mode = reduced_endpoint_mode(resources)

    expected_counts = {
        "aws_api_gateway_deployment": 1,
        "aws_api_gateway_integration": 1,
        "aws_api_gateway_method": 1,
        "aws_api_gateway_resource": 1,
        "aws_api_gateway_rest_api": 1,
        "aws_api_gateway_stage": 1,
        "aws_cloudwatch_log_group": 3,
        "aws_db_instance": 0 if reduced_mode else 1,
        "aws_db_subnet_group": 0 if reduced_mode else 1,
        "aws_glue_catalog_database": 0 if reduced_mode else 1,
        "aws_glue_connection": 0 if reduced_mode else 1,
        "aws_glue_crawler": 0 if reduced_mode else 1,
        "aws_iam_role": 3 if reduced_mode else 5,
        "aws_iam_role_policy": 3 if reduced_mode else 5,
        "aws_internet_gateway": 1,
        "aws_lambda_function": 2,
        "aws_lambda_permission": 1,
        "aws_pipes_pipe": 0 if reduced_mode else 1,
        "aws_redshift_cluster": 0 if reduced_mode else 1,
        "aws_redshift_subnet_group": 0 if reduced_mode else 1,
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

    for type_name, expected_count in expected_counts.items():
        actual = len(_by_type(resources, type_name))
        assert actual == expected_count, f"Expected {expected_count} {type_name}, found {actual}"


def test_event_flow_api_wiring_and_queue_policy_are_exact():
    resources = state_resources()
    reduced_mode = reduced_endpoint_mode(resources)
    lambdas = _by_type(resources, "aws_lambda_function")
    lambda_by_timeout = {_resource_values(item)["timeout"]: item for item in lambdas}
    lambda_a = lambda_by_timeout[10]

    queue = _only(resources, "aws_sqs_queue")
    topic = _only(resources, "aws_sns_topic")
    subscription = _only(resources, "aws_sns_topic_subscription")
    queue_policy = _only(resources, "aws_sqs_queue_policy")
    state_machine = _only(resources, "aws_sfn_state_machine")
    api_integration = _only(resources, "aws_api_gateway_integration")
    api_stage = _only(resources, "aws_api_gateway_stage")
    lambda_permission = _only(resources, "aws_lambda_permission")

    assert _resource_values(subscription)["topic_arn"] == _resource_values(topic)["arn"]
    assert _resource_values(subscription)["endpoint"] == _resource_values(queue)["arn"]

    queue_policy_doc = parse_json_string(_resource_values(queue_policy)["policy"])
    send_message_statements = []
    for statement in queue_policy_doc["Statement"]:
        actions = statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]
        if "sqs:SendMessage" in actions:
            send_message_statements.append(statement)

    assert send_message_statements
    for statement in send_message_statements:
        assert statement["Resource"] == _resource_values(queue)["arn"]
        assert statement.get("Condition", {}).get("ArnEquals", {}).get("aws:SourceArn") == _resource_values(topic)["arn"]
        assert statement.get("Principal", {}).get("Service") == "sns.amazonaws.com"

    if reduced_mode:
        assert _by_type(resources, "aws_pipes_pipe") == []
    else:
        pipe = _only(resources, "aws_pipes_pipe")
        assert _resource_values(pipe)["source"] == _resource_values(queue)["arn"]
        assert _resource_values(pipe)["enrichment"] == _resource_values(lambda_a)["arn"]
        assert _resource_values(pipe)["target"] == _resource_values(state_machine)["arn"]
        pipe_target = _resource_values(pipe)["target_parameters"][0]["step_function_state_machine_parameters"][0]
        assert pipe_target["invocation_type"] == "FIRE_AND_FORGET"

    assert _resource_values(api_integration)["type"] == "AWS_PROXY"
    assert _resource_values(api_integration)["integration_http_method"] == "POST"
    assert _resource_values(api_integration)["uri"] == _resource_values(lambda_a)["invoke_arn"]
    assert _resource_values(api_stage)["stage_name"] == "prod"
    assert _resource_values(lambda_permission)["source_arn"].endswith("/prod/POST/orders")


def test_network_boundaries_routes_and_security_controls_are_correct():
    resources = state_resources()
    reduced_mode = reduced_endpoint_mode(resources)
    subnets = _by_type(resources, "aws_subnet")
    route_tables = _by_type(resources, "aws_route_table")
    route_table_associations = _by_type(resources, "aws_route_table_association")
    igw = _only(resources, "aws_internet_gateway")
    lambdas = _by_type(resources, "aws_lambda_function")
    endpoints = _by_type(resources, "aws_vpc_endpoint")
    security_groups = _by_type(resources, "aws_security_group")
    db_instances = _by_type(resources, "aws_db_instance")
    redshift_clusters = _by_type(resources, "aws_redshift_cluster")
    db_subnet_groups = _by_type(resources, "aws_db_subnet_group")
    redshift_subnet_groups = _by_type(resources, "aws_redshift_subnet_group")

    subnet_by_cidr = {_resource_values(item)["cidr_block"]: item for item in subnets}
    public_subnets = [subnet_by_cidr["10.0.0.0/24"], subnet_by_cidr["10.0.1.0/24"]]
    private_subnets = [subnet_by_cidr["10.0.10.0/24"], subnet_by_cidr["10.0.11.0/24"]]
    public_subnet_ids = {_resource_values(item)["id"] for item in public_subnets}
    private_subnet_ids = {_resource_values(item)["id"] for item in private_subnets}

    assert _resource_values(public_subnets[0])["availability_zone"] != _resource_values(public_subnets[1])["availability_zone"]
    assert _resource_values(private_subnets[0])["availability_zone"] != _resource_values(private_subnets[1])["availability_zone"]

    public_route_table = None
    private_route_table = None
    for route_table in route_tables:
        routes = _resource_values(route_table).get("route", [])
        has_default_route = any(route.get("cidr_block") == "0.0.0.0/0" for route in routes)
        if has_default_route:
            public_route_table = route_table
        else:
            private_route_table = route_table

    assert public_route_table is not None
    assert private_route_table is not None
    public_default_route = next(
        route for route in _resource_values(public_route_table)["route"] if route.get("cidr_block") == "0.0.0.0/0"
    )
    assert public_default_route["gateway_id"] == _resource_values(igw)["id"]
    assert not any(route.get("cidr_block") == "0.0.0.0/0" for route in _resource_values(private_route_table).get("route", []))

    public_assoc_subnets = {
        _resource_values(item)["subnet_id"]
        for item in route_table_associations
        if _resource_values(item)["route_table_id"] == _resource_values(public_route_table)["id"]
    }
    private_assoc_subnets = {
        _resource_values(item)["subnet_id"]
        for item in route_table_associations
        if _resource_values(item)["route_table_id"] == _resource_values(private_route_table)["id"]
    }
    assert public_assoc_subnets == public_subnet_ids
    assert private_assoc_subnets == private_subnet_ids

    if reduced_mode:
        assert db_subnet_groups == []
        assert redshift_subnet_groups == []
    else:
        db_subnet_group = _only(resources, "aws_db_subnet_group")
        redshift_subnet_group = _only(resources, "aws_redshift_subnet_group")
        assert set(_resource_values(db_subnet_group)["subnet_ids"]) == private_subnet_ids
        assert set(_resource_values(redshift_subnet_group)["subnet_ids"]) == private_subnet_ids

    lambda_vpc = _resource_values(lambdas[0])["vpc_config"][0]
    lambda_sg_id = lambda_vpc["security_group_ids"][0]
    assert set(lambda_vpc["subnet_ids"]) == private_subnet_ids

    sg_by_name = {_resource_values(item)["name"]: item for item in security_groups}
    lambda_sg_id_from_name = _resource_values(sg_by_name["orders-lambda-sg"])["id"]
    db_sg_id = _resource_values(sg_by_name["orders-db-sg"])["id"]
    redshift_sg_id = _resource_values(sg_by_name["orders-redshift-sg"])["id"]
    assert lambda_sg_id == lambda_sg_id_from_name

    lambda_egress = security_group_rules(resources, lambda_sg_id, "egress")
    assert any(
        rule["protocol"] == "-1" and rule["cidr_ipv4"] == "0.0.0.0/0"
        for rule in lambda_egress
    )

    db_ingress = security_group_rules(resources, db_sg_id, "ingress")
    redshift_ingress = security_group_rules(resources, redshift_sg_id, "ingress")
    assert any(
        rule["from_port"] == 5432 and rule["to_port"] == 5432 and rule.get("source_security_group_id") == lambda_sg_id
        for rule in db_ingress
    )
    assert any(
        rule["from_port"] == 5439 and rule["to_port"] == 5439 and rule.get("source_security_group_id") == lambda_sg_id
        for rule in redshift_ingress
    )
    assert not any(rule.get("cidr_ipv4") == "0.0.0.0/0" for rule in db_ingress)
    assert not any(rule.get("cidr_ipv4") == "0.0.0.0/0" for rule in redshift_ingress)

    endpoint_sg_ids = {sg_id for endpoint in endpoints for sg_id in _resource_values(endpoint)["security_group_ids"]}
    endpoint_ingress = []
    for endpoint_sg_id in endpoint_sg_ids:
        endpoint_ingress.extend(security_group_rules(resources, endpoint_sg_id, "ingress"))
    assert any(
        rule["from_port"] == 443 and rule["to_port"] == 443 and rule.get("source_security_group_id") == lambda_sg_id
        for rule in endpoint_ingress
    )

    if reduced_mode:
        assert db_instances == []
        assert redshift_clusters == []
    else:
        db_instance = _only(resources, "aws_db_instance")
        redshift_cluster = _only(resources, "aws_redshift_cluster")
        assert _resource_values(db_instance)["publicly_accessible"] is False
        assert _resource_values(redshift_cluster)["publicly_accessible"] is False


def test_state_machine_logging_and_no_kms_or_protection_flags_exist():
    resources = state_resources()
    reduced_mode = reduced_endpoint_mode(resources)
    state_machine = _only(resources, "aws_sfn_state_machine")
    log_groups = _by_type(resources, "aws_cloudwatch_log_group")
    lambdas = _by_type(resources, "aws_lambda_function")
    lambda_arns = {_resource_values(item)["arn"] for item in lambdas}

    assert _resource_values(state_machine)["type"] == "STANDARD"
    for log_group in log_groups:
        assert not _resource_values(log_group).get("kms_key_id")

    state_machine_logs = [item for item in log_groups if _resource_values(item).get("retention_in_days") == 14]
    assert len(state_machine_logs) == 1

    definition = parse_json_string(_resource_values(state_machine)["definition"])
    task_states = {
        name: state
        for name, state in definition["States"].items()
        if state.get("Type") == "Task"
    }
    assert len(task_states) == 2
    assert _task_targets_lambda(task_states["LambdaA"], lambda_arns)
    assert _task_targets_lambda(task_states["LambdaB"], lambda_arns)
    assert task_states["LambdaA"]["Next"] == "LambdaB"
    assert task_states["LambdaB"]["End"] is True

    if reduced_mode:
        assert _by_type(resources, "aws_db_instance") == []
    else:
        db_instance = _only(resources, "aws_db_instance")
        assert not _resource_values(db_instance).get("deletion_protection", False)
        assert _resource_values(db_instance)["skip_final_snapshot"] is True


def test_iam_policies_are_strictly_scoped_by_role():
    resources = state_resources()
    reduced_mode = reduced_endpoint_mode(resources)
    role_policies = _by_type(resources, "aws_iam_role_policy")
    policy_by_role = {_resource_values(item)["role"]: parse_json_string(_resource_values(item)["policy"]) for item in role_policies}

    lambda_a_policy = policy_by_role["orders-lambda-a-role"]
    lambda_b_policy = policy_by_role["orders-lambda-b-role"]
    step_functions_policy = policy_by_role["orders-step-functions-role"]

    lambda_a_actions = {
        action
        for statement in lambda_a_policy["Statement"]
        for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
    }
    lambda_b_actions = {
        action
        for statement in lambda_b_policy["Statement"]
        for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
    }
    assert "secretsmanager:GetSecretValue" in lambda_a_actions
    assert "secretsmanager:GetSecretValue" not in lambda_b_actions

    step_function_lambda_statement = next(
        statement for statement in step_functions_policy["Statement"]
        if "lambda:InvokeFunction" in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
    )
    assert isinstance(step_function_lambda_statement["Resource"], list)
    assert len(step_function_lambda_statement["Resource"]) == 2

    if reduced_mode:
        assert "orders-pipe-role" not in policy_by_role
        assert "orders-glue-crawler-role" not in policy_by_role
    else:
        pipe_policy = policy_by_role["orders-pipe-role"]
        glue_policy = policy_by_role["orders-glue-crawler-role"]
        pipe_actions = {
            action
            for statement in pipe_policy["Statement"]
            for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
        }
        assert pipe_actions == {
            "lambda:InvokeFunction",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ReceiveMessage",
            "states:StartExecution",
        }

        glue_secret_statement = next(
            statement for statement in glue_policy["Statement"]
            if "secretsmanager:GetSecretValue" in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
        )
        assert glue_secret_statement["Resource"] != "*"

    for address, document in policy_documents(resources):
        statements = document.get("Statement", [])
        if isinstance(statements, dict):
            statements = [statements]

        for statement in statements:
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]

            assert actions, f"Policy {address} should declare at least one action"
            assert all(action != "*" for action in actions), f"Policy {address} contains Action='*'"

            resources_list = statement.get("Resource", [])
            if isinstance(resources_list, str):
                resources_list = [resources_list]

            if "*" in resources_list:
                unexpected = set(actions) - ALLOWED_WILDCARD_RESOURCE_ACTIONS
                assert not unexpected, (
                    f"Policy {address} uses Resource='*' for unexpectedly broad actions: {sorted(unexpected)}"
                )
