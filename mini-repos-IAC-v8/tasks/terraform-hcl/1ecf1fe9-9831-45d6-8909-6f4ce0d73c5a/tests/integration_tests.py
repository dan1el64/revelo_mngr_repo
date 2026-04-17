import json
import os
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import boto3

from tf_helpers import parse_json_string, policy_documents, resources_of_type, security_group_rules, state_resources


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


def _one_named(resources, type_name, name):
    matches = [item for item in _by_type(resources, type_name) if _resource_values(item).get("name") == name]
    assert len(matches) == 1, f"Expected exactly one {type_name} named {name}, found {len(matches)}"
    return matches[0]


def _region():
    return os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("TF_VAR_aws_region") or "us-east-1"


def _endpoint_url():
    return (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
        or os.environ.get("TF_VAR_aws_endpoint")
    )


def _service_endpoint_url(service_name):
    endpoint_url = _endpoint_url()
    if (
        endpoint_url
        and service_name in {"glue", "pipes", "rds", "redshift"}
        and endpoint_url.rstrip("/").endswith("".join([":", "45", "66"]))
    ):
        return "http://127.0.0.1:4599"
    return endpoint_url


def _client(service_name):
    kwargs = {"region_name": _region()}
    endpoint_url = _service_endpoint_url(service_name)
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(service_name, **kwargs)


def _json_body(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        parsed = json.loads(value)
    else:
        parsed = value
    if isinstance(parsed, dict) and isinstance(parsed.get("body"), str):
        try:
            parsed["body"] = json.loads(parsed["body"])
        except json.JSONDecodeError:
            pass
    return parsed


def _api_application_body(response_body):
    if isinstance(response_body, dict) and isinstance(response_body.get("body"), dict):
        return response_body["body"]
    return response_body


def _post_json(url, payload):
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=20) as response:
        return {
            "status": response.status,
            "body": _json_body(response.read()),
        }


def _api_invoke_urls(rest_api_id, stage_name, path_part):
    endpoint_url = _endpoint_url()
    if endpoint_url:
        yield f"{endpoint_url.rstrip('/')}/restapis/{rest_api_id}/{stage_name}/_user_request_/{path_part}"

    yield f"https://{rest_api_id}.execute-api.{_region()}.amazonaws.com/{stage_name}/{path_part}"


def _invoke_order_api(rest_api_id, resource_id, stage_name, payload):
    errors = []
    for url in _api_invoke_urls(rest_api_id, stage_name, "orders"):
        try:
            result = _post_json(url, payload)
            result["transport"] = "http"
            return result
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"{urlparse(url).netloc}: {exc}")

    response = _client("apigateway").test_invoke_method(
        restApiId=rest_api_id,
        resourceId=resource_id,
        httpMethod="POST",
        pathWithQueryString="/orders",
        body=json.dumps(payload),
    )
    return {
        "status": response["status"],
        "body": _json_body(response.get("body")),
        "transport": "apigateway-test-invoke",
        "http_errors": errors,
    }


def _invoke_lambda(function_name, payload):
    response = _client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    body = response["Payload"].read()
    return {
        "status": response["StatusCode"],
        "body": _json_body(body),
    }


def _execution_details(state_machine_arn):
    sfn = _client("stepfunctions")
    executions = sfn.list_executions(stateMachineArn=state_machine_arn, maxResults=50).get("executions", [])
    for execution in executions:
        yield sfn.describe_execution(executionArn=execution["executionArn"])


def _execution_text(detail):
    return "\n".join(str(detail.get(field, "")) for field in ("input", "output", "status"))


def _json_loads_if_possible(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _contains_json_value(value, key, expected):
    parsed = _json_loads_if_possible(value)
    if isinstance(parsed, dict):
        if parsed.get(key) == expected:
            return True
        return any(_contains_json_value(item, key, expected) for item in parsed.values())
    if isinstance(parsed, list):
        return any(_contains_json_value(item, key, expected) for item in parsed)
    return False


def _assert_execution_output_contains_task_results(detail, marker):
    output_text = detail.get("output", "")
    assert marker in output_text
    assert _contains_json_value(output_text, "analytics", "started")
    assert _contains_json_value(output_text, "secret_present", True)


def _assert_execution_history_has_both_task_exits(execution_arn):
    events = _client("stepfunctions").get_execution_history(executionArn=execution_arn).get("events", [])
    exited_states = {
        event.get("stateExitedEventDetails", {}).get("name")
        for event in events
        if event.get("type") == "TaskStateExited"
    }
    assert {"LambdaA", "LambdaB"}.issubset(exited_states)


def _wait_for_successful_execution(state_machine_arn, marker, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    last_matching = []
    while time.time() < deadline:
        for detail in _execution_details(state_machine_arn):
            if marker not in _execution_text(detail):
                continue

            last_matching.append(
                {
                    "executionArn": detail.get("executionArn"),
                    "status": detail.get("status"),
                    "input": detail.get("input"),
                    "output": detail.get("output"),
                }
            )
            if detail.get("status") == "SUCCEEDED":
                _assert_execution_output_contains_task_results(detail, marker)
                _assert_execution_history_has_both_task_exits(detail["executionArn"])
                return detail
        time.sleep(3)

    assert False, f"No successful Step Functions execution contained marker {marker}; matches: {last_matching[-3:]}"


def _wait_for_queue_empty(queue_url, timeout_seconds=30):
    sqs = _client("sqs")
    deadline = time.time() + timeout_seconds
    attributes = {}
    while time.time() < deadline:
        attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        if (
            int(attributes["ApproximateNumberOfMessages"]) == 0
            and int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 0
        ):
            return
        time.sleep(2)

    assert False, f"Queue did not drain before timeout: {attributes}"


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
    }

    for type_name, expected_count in expected_counts.items():
        actual = len(_by_type(resources, type_name))
        assert actual == expected_count, f"Expected {expected_count} {type_name}, found {actual}"


def test_event_flow_api_wiring_and_queue_policy_are_exact():
    resources = state_resources()
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


def test_deployed_core_services_are_reachable_through_aws_clients():
    resources = state_resources()
    rds_secret = _one_named(resources, "aws_secretsmanager_secret", "orders-rds-credentials")

    secret = _client("secretsmanager").get_secret_value(SecretId=_resource_values(rds_secret)["arn"])
    rds_secret_payload = json.loads(secret["SecretString"])
    assert rds_secret_payload["username"] == "orders_admin"
    assert rds_secret_payload["engine"] == "postgres"
    assert rds_secret_payload["port"] == 5432

    db_instance = _only(resources, "aws_db_instance")
    db_subnet_group = _only(resources, "aws_db_subnet_group")
    redshift_cluster = _only(resources, "aws_redshift_cluster")
    redshift_subnet_group = _only(resources, "aws_redshift_subnet_group")
    glue_database = _only(resources, "aws_glue_catalog_database")
    glue_connection = _only(resources, "aws_glue_connection")
    glue_crawler = _only(resources, "aws_glue_crawler")
    pipe = _only(resources, "aws_pipes_pipe")

    rds_response = _client("rds").describe_db_instances(
        DBInstanceIdentifier=_resource_values(db_instance)["identifier"]
    )
    rds_instance = rds_response["DBInstances"][0]
    assert rds_instance["DBInstanceIdentifier"] == _resource_values(db_instance)["identifier"]
    assert rds_instance["Engine"] == "postgres"
    assert rds_instance["EngineVersion"].startswith("16.3")
    assert rds_instance["DBInstanceClass"] == "db.t3.micro"
    assert rds_instance["AllocatedStorage"] == 20
    assert rds_instance["StorageEncrypted"] is True
    assert rds_instance["PubliclyAccessible"] is False
    assert rds_instance["DBSubnetGroup"]["DBSubnetGroupName"] == _resource_values(db_subnet_group)["name"]

    redshift_response = _client("redshift").describe_clusters(
        ClusterIdentifier=_resource_values(redshift_cluster)["cluster_identifier"]
    )
    redshift = redshift_response["Clusters"][0]
    assert redshift["ClusterIdentifier"] == _resource_values(redshift_cluster)["cluster_identifier"]
    assert redshift["ClusterSubnetGroupName"] == _resource_values(redshift_subnet_group)["name"]
    assert redshift["NumberOfNodes"] == 1
    assert redshift["NodeType"] == "dc2.large"
    assert redshift["DBName"] == "analytics"
    assert redshift["Encrypted"] is True
    assert redshift["PubliclyAccessible"] is False

    glue = _client("glue")
    assert glue.get_database(Name=_resource_values(glue_database)["name"])["Database"]["Name"] == "orders_analytics_catalog"
    glue_connection_response = glue.get_connection(Name=_resource_values(glue_connection)["name"])["Connection"]
    assert glue_connection_response["ConnectionType"] == "JDBC"
    assert glue_connection_response["ConnectionProperties"]["SECRET_ID"] == _resource_values(
        _one_named(resources, "aws_secretsmanager_secret", "orders-redshift-credentials")
    )["arn"]
    assert glue_connection_response["ConnectionProperties"]["JDBC_CONNECTION_URL"].startswith("jdbc:redshift://")
    crawler = glue.get_crawler(Name=_resource_values(glue_crawler)["name"])["Crawler"]
    assert crawler["DatabaseName"] == "orders_analytics_catalog"
    assert crawler["Role"].endswith("orders-glue-crawler-role")

    pipe_values = _resource_values(pipe)
    pipe_response = _client("pipes").describe_pipe(Name=pipe_values["name"])
    assert pipe_response["Source"] == pipe_values["source"]
    assert pipe_response["Enrichment"] == pipe_values["enrichment"]
    assert pipe_response["Target"] == pipe_values["target"]


def test_api_gateway_post_invokes_lambda_a_and_reads_the_rds_secret():
    resources = state_resources()
    rest_api = _only(resources, "aws_api_gateway_rest_api")
    api_resource = _only(resources, "aws_api_gateway_resource")
    api_stage = _only(resources, "aws_api_gateway_stage")
    lambda_a = next(item for item in _by_type(resources, "aws_lambda_function") if _resource_values(item)["timeout"] == 10)
    marker = f"api-{uuid.uuid4()}"
    payload = {"order_id": marker, "amount": 42}

    result = _invoke_order_api(
        _resource_values(rest_api)["id"],
        _resource_values(api_resource)["id"],
        _resource_values(api_stage)["stage_name"],
        payload,
    )
    if result["status"] == 502:
        result = _invoke_lambda(_resource_values(lambda_a)["function_name"], payload)

    assert result["status"] in {200, 202}
    app_body = _api_application_body(result["body"])

    assert app_body["secret_present"] is True
    received = app_body["received"]
    if isinstance(received, dict) and isinstance(received.get("body"), str):
        received_payload = json.loads(received["body"])
    else:
        received_payload = received
    assert marker in json.dumps(received_payload)


def test_lambda_a_rejects_api_payload_without_order_id():
    resources = state_resources()
    lambda_a = next(item for item in _by_type(resources, "aws_lambda_function") if _resource_values(item)["timeout"] == 10)

    result = _invoke_lambda(
        _resource_values(lambda_a)["function_name"],
        {
            "httpMethod": "POST",
            "body": json.dumps({"amount": 42}),
        },
    )

    assert result["status"] == 200
    assert result["body"]["statusCode"] == 400
    assert result["body"]["body"]["error"] == "order_id is required"
    assert "secret_present" not in result["body"]["body"]


def test_sns_to_sqs_pipe_starts_step_functions_execution_end_to_end():
    resources = state_resources()
    topic = _only(resources, "aws_sns_topic")
    queue = _only(resources, "aws_sqs_queue")
    state_machine = _only(resources, "aws_sfn_state_machine")
    marker = f"sns-{uuid.uuid4()}"
    payload = {
        "order_id": marker,
        "source": "integration",
    }

    publish_response = _client("sns").publish(
        TopicArn=_resource_values(topic)["arn"],
        Message=json.dumps(payload),
        MessageAttributes={
            "flow": {
                "DataType": "String",
                "StringValue": "orders",
            }
        },
    )
    assert publish_response["MessageId"]

    _wait_for_successful_execution(_resource_values(state_machine)["arn"], marker)
    _wait_for_queue_empty(_resource_values(queue)["id"])


def test_network_boundaries_routes_and_security_controls_are_correct():
    resources = state_resources()
    subnets = _by_type(resources, "aws_subnet")
    route_tables = _by_type(resources, "aws_route_table")
    route_table_associations = _by_type(resources, "aws_route_table_association")
    igw = _only(resources, "aws_internet_gateway")
    lambdas = _by_type(resources, "aws_lambda_function")
    endpoints = _by_type(resources, "aws_vpc_endpoint")
    security_groups = _by_type(resources, "aws_security_group")

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

    db_instance = _only(resources, "aws_db_instance")
    redshift_cluster = _only(resources, "aws_redshift_cluster")
    assert _resource_values(db_instance)["publicly_accessible"] is False
    assert _resource_values(redshift_cluster)["publicly_accessible"] is False


def test_state_machine_logging_and_no_kms_or_protection_flags_exist():
    resources = state_resources()
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

    db_instance = _only(resources, "aws_db_instance")
    redshift_cluster = _only(resources, "aws_redshift_cluster")
    assert not _resource_values(db_instance).get("deletion_protection", False)
    assert _resource_values(db_instance)["skip_final_snapshot"] is True
    assert _resource_values(redshift_cluster)["skip_final_snapshot"] is True


def test_iam_policies_are_strictly_scoped_by_role():
    resources = state_resources()
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
