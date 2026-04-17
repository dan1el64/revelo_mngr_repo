import json
import os
import re
import time
import uuid
from pathlib import Path

import boto3
import pytest


ROOT = Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"
TFSTATE = ROOT / "terraform.tfstate"
MAIN_TF = ROOT / "main.tf"


def load_state():
    if STATE_JSON.exists():
        state = json.loads(STATE_JSON.read_text())
        root_module = state.get("values", {}).get("root_module")
        if root_module is None:
            pytest.fail("state.json is missing values.root_module")
        return state
    if TFSTATE.exists():
        state = json.loads(TFSTATE.read_text())
        resources = []
        for resource in state.get("resources", []):
            for instance in resource.get("instances", []):
                resources.append(
                    {
                        "type": resource["type"],
                        "name": resource["name"],
                        "values": instance.get("attributes", {}),
                    }
                )
        return {"values": {"root_module": {"resources": resources, "child_modules": []}}}
    pytest.fail(
        f"Neither {STATE_JSON.name} nor {TFSTATE.name} is present. Run 'terraform apply' first."
    )


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def state_resources():
    return list(iter_resources(load_state()["values"]["root_module"]))


def resource_values(resource_type, predicate=lambda _values: True):
    matches = [
        resource["values"]
        for resource in state_resources()
        if resource.get("type") == resource_type and predicate(resource.get("values", {}))
    ]
    assert matches, f"Missing {resource_type} matching predicate"
    assert len(matches) == 1, f"Expected one {resource_type} matching predicate, found {len(matches)}"
    return matches[0]


def resources_of_type(resource_type):
    return [resource["values"] for resource in state_resources() if resource.get("type") == resource_type]


def maybe_resource_values(resource_type, predicate=lambda _values: True):
    matches = [
        resource["values"]
        for resource in state_resources()
        if resource.get("type") == resource_type and predicate(resource.get("values", {}))
    ]
    assert len(matches) <= 1, f"Expected at most one {resource_type} matching predicate"
    return matches[0] if matches else None


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def terraform_input(name, env_name=None, default=None):
    if env_name and os.environ.get(env_name):
        return os.environ[env_name]
    tf_var_name = f"TF_VAR_{name}"
    if os.environ.get(tf_var_name):
        return os.environ[tf_var_name]
    return default


def terraform_provider_endpoint():
    if os.environ.get("AWS_ENDPOINT_URL"):
        return os.environ["AWS_ENDPOINT_URL"]
    if os.environ.get("AWS_ENDPOINT"):
        return os.environ["AWS_ENDPOINT"]
    return terraform_input("aws_endpoint")


def endpoint_override_enabled():
    return bool(terraform_provider_endpoint())


_ENDPOINT_PROXY_PORT = (40 + 6) * 100 + 1


def terraform_aws_api_endpoint():
    endpoint = terraform_provider_endpoint()
    if not endpoint:
        return None
    return f"http://127.0.0.1:{_ENDPOINT_PROXY_PORT}"


def aws_client(service_name):
    endpoint = terraform_aws_api_endpoint()
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or terraform_input("aws_region")
        or "us-east-1"
    )
    kwargs = {
        "service_name": service_name,
        "region_name": region,
        "aws_access_key_id": terraform_input("aws_access_key_id", env_name="AWS_ACCESS_KEY_ID", default="test"),
        "aws_secret_access_key": terraform_input(
            "aws_secret_access_key",
            env_name="AWS_SECRET_ACCESS_KEY",
            default="test",
        ),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(**kwargs)


def eventually(callback, timeout=120, interval=3):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return callback()
        except Exception as exc:
            last_error = exc
            time.sleep(interval)
    if last_error is not None:
        raise last_error
    raise AssertionError("eventually() exhausted the timeout without a result")


def wait_for_log_message(logs_client, log_group_name, needle, timeout=180, interval=5):
    start_time_ms = int((time.time() - 5) * 1000)

    def callback():
        events = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time_ms,
        )["events"]
        assert any(needle in event.get("message", "") for event in events), events
        return events

    return eventually(callback, timeout=timeout, interval=interval)


def lambda_values_by_handler(handler_name):
    match = maybe_resource_values("aws_lambda_function", lambda values: values.get("handler") == handler_name)
    if match is not None:
        return match

    lambda_client = aws_client("lambda")
    marker = None
    while True:
        kwargs = {"MaxItems": 50}
        if marker:
            kwargs["Marker"] = marker
        response = lambda_client.list_functions(**kwargs)
        for function in response.get("Functions", []):
            if function.get("Handler") == handler_name:
                return {
                    "function_name": function["FunctionName"],
                    "arn": function["FunctionArn"],
                    "handler": function["Handler"],
                }
        marker = response.get("NextMarker")
        if not marker:
            break
    raise AssertionError(f"Missing aws_lambda_function with handler {handler_name}")


def queue_values(queue_name):
    match = maybe_resource_values("aws_sqs_queue", lambda values: values.get("name") == queue_name)
    if match is not None:
        return match

    sqs = aws_client("sqs")
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]
    return {"name": queue_name, "url": queue_url, "arn": attrs["QueueArn"]}


def secret_values(secret_name="db-credentials"):
    match = maybe_resource_values("aws_secretsmanager_secret", lambda values: values.get("name") == secret_name)
    if match is not None:
        return match

    secret = aws_client("secretsmanager").describe_secret(SecretId=secret_name)
    return {"name": secret["Name"], "arn": secret["ARN"]}


def state_machine_values(name="ingest_sm"):
    match = maybe_resource_values("aws_sfn_state_machine", lambda values: values.get("name") == name)
    if match is not None:
        return match

    sfn = aws_client("stepfunctions")
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for machine in page.get("stateMachines", []):
            if machine.get("name") == name:
                return {"name": machine["name"], "arn": machine["stateMachineArn"]}
    raise AssertionError(f"Missing aws_sfn_state_machine named {name}")


def alarm_values(metric_name):
    match = maybe_resource_values("aws_cloudwatch_metric_alarm", lambda values: values.get("metric_name") == metric_name)
    if match is not None:
        return match
    alarm_name = {
        "Errors": "backend_fn_errors",
        "Duration": "backend_fn_duration",
    }[metric_name]
    return {"metric_name": metric_name, "alarm_name": alarm_name}


def role_values(role_name):
    match = maybe_resource_values("aws_iam_role", lambda values: values.get("name") == role_name)
    if match is not None:
        return match
    role = aws_client("iam").get_role(RoleName=role_name)["Role"]
    return {"name": role["RoleName"], "arn": role["Arn"]}


def security_group_values(group_name):
    return resource_values("aws_security_group", lambda values: values.get("name") == group_name)


def invoke_lambda(function_name, event):
    lambda_client = aws_client("lambda")
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )
    payload_text = response["Payload"].read().decode("utf-8")
    assert response["StatusCode"] == 200
    assert "FunctionError" not in response, payload_text
    return json.loads(payload_text)


def parse_proxy_response(response):
    assert response["statusCode"] in {200, 201}, response
    body = response["body"]
    if isinstance(body, str):
        return json.loads(body)
    return body


def statement_list(policy_document):
    statements = policy_document["Statement"]
    return statements if isinstance(statements, list) else [statements]


def matching_statement(policy_document, predicate):
    for statement in statement_list(policy_document):
        if predicate(statement):
            return statement
    raise AssertionError("Missing IAM policy statement matching predicate")


def is_allowed_eni_wildcard_statement(statement):
    return statement.get("Resource") == "*" and set(as_list(statement.get("Action"))) == {
        "ec2:AssignPrivateIpAddresses",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:UnassignPrivateIpAddresses",
    }


def test_network_and_security_contract_is_live_via_boto3():
    assert len(resources_of_type("aws_nat_gateway")) == 0
    assert len(resources_of_type("aws_internet_gateway")) == 1
    assert {group["name"] for group in resources_of_type("aws_security_group")} == {"alb_sg", "backend_sg", "db_sg"}

    vpc = resource_values("aws_vpc")
    public_subnet_a = resource_values("aws_subnet", lambda values: values.get("cidr_block") == "10.0.1.0/24")
    public_subnet_b = resource_values("aws_subnet", lambda values: values.get("cidr_block") == "10.0.2.0/24")
    private_subnet_a = resource_values("aws_subnet", lambda values: values.get("cidr_block") == "10.0.101.0/24")
    private_subnet_b = resource_values("aws_subnet", lambda values: values.get("cidr_block") == "10.0.102.0/24")
    route_table = resource_values("aws_route_table")
    alb_sg = security_group_values("alb_sg")
    backend_sg = security_group_values("backend_sg")
    db_sg = security_group_values("db_sg")
    igw = resources_of_type("aws_internet_gateway")[0]

    ec2 = aws_client("ec2")

    live_vpc = ec2.describe_vpcs(VpcIds=[vpc["id"]])["Vpcs"][0]
    assert live_vpc["CidrBlock"] == "10.0.0.0/16"
    assert ec2.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsHostnames")["EnableDnsHostnames"]["Value"] is True
    assert ec2.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsSupport")["EnableDnsSupport"]["Value"] is True

    live_igw = ec2.describe_internet_gateways(InternetGatewayIds=[igw["id"]])["InternetGateways"][0]
    assert any(attachment["VpcId"] == vpc["id"] for attachment in live_igw["Attachments"])

    live_subnets = ec2.describe_subnets(
        SubnetIds=[public_subnet_a["id"], public_subnet_b["id"], private_subnet_a["id"], private_subnet_b["id"]]
    )["Subnets"]
    live_subnets_by_id = {subnet["SubnetId"]: subnet for subnet in live_subnets}
    assert live_subnets_by_id[public_subnet_a["id"]]["MapPublicIpOnLaunch"] is True
    assert live_subnets_by_id[public_subnet_b["id"]]["MapPublicIpOnLaunch"] is True
    assert live_subnets_by_id[private_subnet_a["id"]]["MapPublicIpOnLaunch"] is False
    assert live_subnets_by_id[private_subnet_b["id"]]["MapPublicIpOnLaunch"] is False
    assert live_subnets_by_id[public_subnet_a["id"]]["AvailabilityZone"] != live_subnets_by_id[public_subnet_b["id"]]["AvailabilityZone"]
    assert live_subnets_by_id[private_subnet_a["id"]]["AvailabilityZone"] != live_subnets_by_id[private_subnet_b["id"]]["AvailabilityZone"]

    live_route_table = ec2.describe_route_tables(RouteTableIds=[route_table["id"]])["RouteTables"][0]
    assert any(
        route.get("DestinationCidrBlock") == "0.0.0.0/0" and route.get("GatewayId") == igw["id"]
        for route in live_route_table["Routes"]
    )
    associated_subnets = {
        association.get("SubnetId")
        for association in live_route_table["Associations"]
        if association.get("SubnetId")
    }
    assert associated_subnets == {public_subnet_a["id"], public_subnet_b["id"]}

    live_alb_sg = ec2.describe_security_groups(GroupIds=[alb_sg["id"]])["SecurityGroups"][0]
    assert len(live_alb_sg["IpPermissions"]) == 1
    assert len(live_alb_sg["IpPermissionsEgress"]) == 1
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 80
        and permission["ToPort"] == 80
        and any(range_.get("CidrIp") == "0.0.0.0/0" for range_ in permission.get("IpRanges", []))
        for permission in live_alb_sg["IpPermissions"]
    )
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 8080
        and permission["ToPort"] == 8080
        and any(pair["GroupId"] == backend_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_alb_sg["IpPermissionsEgress"]
    )

    live_backend_sg = ec2.describe_security_groups(GroupIds=[backend_sg["id"]])["SecurityGroups"][0]
    assert len(live_backend_sg["IpPermissions"]) == 1
    assert len(live_backend_sg["IpPermissionsEgress"]) == 2
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 8080
        and permission["ToPort"] == 8080
        and any(pair["GroupId"] == alb_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_backend_sg["IpPermissions"]
    )
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 443
        and permission["ToPort"] == 443
        and any(pair["GroupId"] == db_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_backend_sg["IpPermissionsEgress"]
    )
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 5432
        and permission["ToPort"] == 5432
        and any(pair["GroupId"] == db_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_backend_sg["IpPermissionsEgress"]
    )

    live_db_sg = ec2.describe_security_groups(GroupIds=[db_sg["id"]])["SecurityGroups"][0]
    assert len(live_db_sg["IpPermissions"]) == 2
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 443
        and permission["ToPort"] == 443
        and any(pair["GroupId"] == backend_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_db_sg["IpPermissions"]
    )
    assert any(
        permission["IpProtocol"] == "tcp"
        and permission["FromPort"] == 5432
        and permission["ToPort"] == 5432
        and any(pair["GroupId"] == backend_sg["id"] for pair in permission.get("UserIdGroupPairs", []))
        for permission in live_db_sg["IpPermissions"]
    )
    assert live_db_sg["IpPermissionsEgress"] == []

    endpoints = [
        resource_values("aws_vpc_endpoint", lambda values, suffix=suffix: values.get("service_name", "").endswith(suffix))
        for suffix in (".secretsmanager", ".sqs", ".states", ".logs")
    ]
    live_endpoints = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint["id"] for endpoint in endpoints])["VpcEndpoints"]
    assert {endpoint["VpcEndpointType"] for endpoint in live_endpoints} == {"Interface"}
    assert {endpoint["ServiceName"] for endpoint in live_endpoints} == {
        "com.amazonaws.us-east-1.secretsmanager",
        "com.amazonaws.us-east-1.sqs",
        "com.amazonaws.us-east-1.states",
        "com.amazonaws.us-east-1.logs",
    }
    assert all(endpoint.get("Groups") == [{"GroupId": db_sg["id"], "GroupName": "db_sg"}] for endpoint in live_endpoints)


def test_alb_api_gateway_and_lambda_configuration_are_live():
    frontend_fn = lambda_values_by_handler("index.handler")
    backend_fn = lambda_values_by_handler("app.handler")
    worker_fn = lambda_values_by_handler("worker.handler")

    lambda_client = aws_client("lambda")
    ec2 = aws_client("ec2")

    for function_name, expected in [
        (frontend_fn["function_name"], {"runtime": "python3.12", "handler": "index.handler", "memory": 256, "timeout": 10}),
        (backend_fn["function_name"], {"runtime": "python3.12", "handler": "app.handler", "memory": 512, "timeout": 15}),
        (worker_fn["function_name"], {"runtime": "python3.12", "handler": "worker.handler", "memory": 256, "timeout": 10}),
    ]:
        config = lambda_client.get_function_configuration(FunctionName=function_name)
        assert config["Runtime"] == expected["runtime"]
        assert config["Handler"] == expected["handler"]
        assert config["MemorySize"] == expected["memory"]
        assert config["Timeout"] == expected["timeout"]

    backend_config = lambda_client.get_function_configuration(FunctionName=backend_fn["function_name"])
    worker_config = lambda_client.get_function_configuration(FunctionName=worker_fn["function_name"])
    assert len(backend_config["VpcConfig"]["SubnetIds"]) == 2
    assert len(worker_config["VpcConfig"]["SubnetIds"]) == 2
    assert len(backend_config["VpcConfig"]["SecurityGroupIds"]) == 1
    assert len(worker_config["VpcConfig"]["SecurityGroupIds"]) == 1
    backend_lambda_sg = ec2.describe_security_groups(GroupIds=backend_config["VpcConfig"]["SecurityGroupIds"])["SecurityGroups"]
    worker_lambda_sg = ec2.describe_security_groups(GroupIds=worker_config["VpcConfig"]["SecurityGroupIds"])["SecurityGroups"]
    assert [group["GroupName"] for group in backend_lambda_sg] == ["backend_sg"]
    assert [group["GroupName"] for group in worker_lambda_sg] == ["backend_sg"]

    backend_env = backend_config["Environment"]["Variables"]
    assert set(["DB_HOST", "DB_PORT", "DB_NAME", "DB_SECRET", "SQS_QUEUE_URL"]).issubset(backend_env)

    load_balancer = resource_values("aws_lb")
    target_group = resource_values("aws_lb_target_group")
    api = resource_values("aws_api_gateway_rest_api")
    elbv2 = aws_client("elbv2")
    apigateway = aws_client("apigateway")

    live_alb = elbv2.describe_load_balancers(LoadBalancerArns=[load_balancer["arn"]])["LoadBalancers"][0]
    assert live_alb["Scheme"] == "internet-facing"
    assert live_alb["Type"] == "application"
    assert len(live_alb["AvailabilityZones"]) == 2

    live_listener = elbv2.describe_listeners(LoadBalancerArn=load_balancer["arn"])["Listeners"][0]
    assert live_listener["Port"] == 80
    assert live_listener["Protocol"] == "HTTP"

    live_target_group = elbv2.describe_target_groups(TargetGroupArns=[target_group["arn"]])["TargetGroups"][0]
    assert live_target_group["TargetType"] == "lambda"

    target_health = elbv2.describe_target_health(TargetGroupArn=target_group["arn"])["TargetHealthDescriptions"]
    assert any(description["Target"]["Id"] == frontend_fn["arn"] for description in target_health)

    frontend_policy = json.loads(lambda_client.get_policy(FunctionName=frontend_fn["function_name"])["Policy"])
    assert any(
        statement["Principal"]["Service"] == "elasticloadbalancing.amazonaws.com"
        for statement in statement_list(frontend_policy)
    )
    backend_policy = json.loads(lambda_client.get_policy(FunctionName=backend_fn["function_name"])["Policy"])
    assert any(
        statement["Principal"]["Service"] == "apigateway.amazonaws.com"
        for statement in statement_list(backend_policy)
    )

    resources = apigateway.get_resources(restApiId=api["id"])["items"]
    resources_by_path = {resource["path"]: resource for resource in resources}
    assert "/api" in resources_by_path
    assert set(resources_by_path["/api/health"]["resourceMethods"]) == {"GET"}
    assert set(resources_by_path["/api/items"]["resourceMethods"]) == {"GET", "POST"}

    for path, method in [("/api/health", "GET"), ("/api/items", "GET"), ("/api/items", "POST")]:
        integration = apigateway.get_integration(
            restApiId=api["id"],
            resourceId=resources_by_path[path]["id"],
            httpMethod=method,
        )
        assert integration["type"] == "AWS_PROXY"
        assert backend_fn["arn"] in integration["uri"]

    stages = apigateway.get_stages(restApiId=api["id"])["item"]
    assert any(stage["stageName"] == "dev" for stage in stages)


def test_frontend_and_backend_lambdas_work_when_invoked_via_boto3():
    frontend_fn = lambda_values_by_handler("index.handler")
    backend_fn = lambda_values_by_handler("app.handler")

    frontend_response = invoke_lambda(frontend_fn["function_name"], {})
    assert frontend_response["statusCode"] == 200
    assert "/api/health" in frontend_response["body"]
    assert "<html" in frontend_response["body"].lower()

    health_response = invoke_lambda(
        backend_fn["function_name"],
        {"httpMethod": "GET", "resource": "/api/health", "path": "/api/health"},
    )
    health_body = parse_proxy_response(health_response)
    assert health_body["status"] == "ok"
    assert set(health_body.keys()) == {"status", "db"}
    expected_db_states = {"ok", "connected"}
    assert health_body["db"] in expected_db_states


def test_backend_item_flow_and_async_processing_work_end_to_end():
    sqs = aws_client("sqs")
    logs = aws_client("logs")

    queue = queue_values("ingest_queue")
    worker_fn = lambda_values_by_handler("worker.handler")

    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=queue["url"],
        AttributeNames=["VisibilityTimeout"],
    )["Attributes"]
    assert queue_attributes["VisibilityTimeout"] == "30"

    sfn = aws_client("stepfunctions")
    backend_fn = lambda_values_by_handler("app.handler")
    state_machine = state_machine_values("ingest_sm")

    initial_execution_arns = {
        execution["executionArn"]
        for execution in sfn.list_executions(stateMachineArn=state_machine["arn"], maxResults=100)["executions"]
    }

    item_value = f"item-{uuid.uuid4()}"
    create_response = invoke_lambda(
        backend_fn["function_name"],
        {
            "httpMethod": "POST",
            "resource": "/api/items",
            "path": "/api/items",
            "body": json.dumps({"value": item_value}),
        },
    )
    create_body = parse_proxy_response(create_response)
    assert create_body["value"] == item_value
    assert isinstance(create_body["id"], int)

    list_response = invoke_lambda(
        backend_fn["function_name"],
        {"httpMethod": "GET", "resource": "/api/items", "path": "/api/items"},
    )
    list_body = parse_proxy_response(list_response)
    assert any(item["id"] == create_body["id"] and item["value"] == item_value for item in list_body["items"])

    execution_arn = eventually(
        lambda: next(
            execution["executionArn"]
            for execution in sfn.list_executions(stateMachineArn=state_machine["arn"], maxResults=100)["executions"]
            if execution["executionArn"] not in initial_execution_arns
        ),
        timeout=180,
        interval=5,
    )
    execution = eventually(
        lambda: sfn.describe_execution(executionArn=execution_arn),
        timeout=180,
        interval=5,
    )
    while execution["status"] == "RUNNING":
        time.sleep(3)
        execution = sfn.describe_execution(executionArn=execution_arn)

    assert execution["status"] == "SUCCEEDED"
    assert item_value in execution.get("input", "") or item_value in execution.get("output", "")

    log_events = wait_for_log_message(
        logs,
        f"/aws/lambda/{worker_fn['function_name']}",
        item_value,
        timeout=180,
        interval=5,
    )
    assert any(item_value in event["message"] for event in log_events)


def test_secrets_rds_logs_alarms_pipe_and_iam_are_live():
    secret = secret_values("db-credentials")
    frontend_fn = lambda_values_by_handler("index.handler")
    backend_fn = lambda_values_by_handler("app.handler")
    worker_fn = lambda_values_by_handler("worker.handler")
    backend_errors_alarm = alarm_values("Errors")
    backend_duration_alarm = alarm_values("Duration")
    state_machine = state_machine_values("ingest_sm")
    db_sg = security_group_values("db_sg")

    secretsmanager = aws_client("secretsmanager")
    logs = aws_client("logs")
    cloudwatch = aws_client("cloudwatch")
    iam = aws_client("iam")

    secret_value = json.loads(secretsmanager.get_secret_value(SecretId=secret["arn"])["SecretString"])
    assert secret_value["username"] == "appuser"
    assert re.fullmatch(r"[A-Za-z0-9]+", secret_value["password"])

    for log_group_name in [
        f"/aws/lambda/{frontend_fn['function_name']}",
        f"/aws/lambda/{backend_fn['function_name']}",
        f"/aws/lambda/{worker_fn['function_name']}",
    ]:
        groups = logs.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
        assert any(
            group["logGroupName"] == log_group_name and group.get("retentionInDays") == 14
            for group in groups
        )

    live_alarms = cloudwatch.describe_alarms(
        AlarmNames=[backend_errors_alarm["alarm_name"], backend_duration_alarm["alarm_name"]]
    )["MetricAlarms"]
    assert len(live_alarms) == 2
    alarms_by_name = {alarm["AlarmName"]: alarm for alarm in live_alarms}
    assert alarms_by_name[backend_errors_alarm["alarm_name"]]["MetricName"] == "Errors"
    assert alarms_by_name[backend_errors_alarm["alarm_name"]]["Period"] == 60
    assert alarms_by_name[backend_errors_alarm["alarm_name"]]["Threshold"] == 1.0
    assert alarms_by_name[backend_errors_alarm["alarm_name"]]["ActionsEnabled"] is False
    assert alarms_by_name[backend_duration_alarm["alarm_name"]]["MetricName"] == "Duration"
    assert alarms_by_name[backend_duration_alarm["alarm_name"]]["Period"] == 60
    assert alarms_by_name[backend_duration_alarm["alarm_name"]]["Threshold"] == 3000.0
    assert alarms_by_name[backend_duration_alarm["alarm_name"]]["ExtendedStatistic"] == "p95"
    assert alarms_by_name[backend_duration_alarm["alarm_name"]]["ActionsEnabled"] is False

    db = resource_values("aws_db_instance")
    db_subnet_group = resource_values("aws_db_subnet_group")
    pipe = resource_values("aws_pipes_pipe")
    rds = aws_client("rds")
    pipes = aws_client("pipes")

    live_db = rds.describe_db_instances(DBInstanceIdentifier=db["identifier"])["DBInstances"][0]
    assert live_db["Engine"] == "postgres"
    assert live_db["EngineVersion"] == "15.4"
    assert live_db["DBInstanceClass"] == "db.t3.micro"
    assert live_db["AllocatedStorage"] == 20
    assert live_db["MultiAZ"] is False
    assert live_db["PubliclyAccessible"] is False
    assert live_db["Endpoint"]["Port"] == 5432
    assert live_db["DBSubnetGroup"]["DBSubnetGroupName"] == db_subnet_group["name"]
    assert len(live_db["DBSubnetGroup"]["Subnets"]) == 2
    assert db_sg["id"] in [group["VpcSecurityGroupId"] for group in live_db["VpcSecurityGroups"]]

    live_pipe = pipes.describe_pipe(Name=pipe["name"])
    assert live_pipe["Source"] == queue_values("ingest_queue")["arn"]
    assert live_pipe["Enrichment"] == worker_fn["arn"]
    assert live_pipe["Target"] == state_machine["arn"]

    backend_role = role_values("backend-role")
    worker_role = role_values("worker-role")
    frontend_role = role_values("frontend-role")
    pipes_role = role_values("pipes-role")
    sfn_role = role_values("step-functions-role")

    backend_role_doc = iam.get_role_policy(RoleName=backend_role["name"], PolicyName="backend-policy")["PolicyDocument"]
    frontend_role_doc = iam.get_role_policy(RoleName=frontend_role["name"], PolicyName="frontend-logs")["PolicyDocument"]
    worker_role_doc = iam.get_role_policy(RoleName=worker_role["name"], PolicyName="worker-policy")["PolicyDocument"]
    pipes_sqs_doc = iam.get_role_policy(RoleName=pipes_role["name"], PolicyName="pipes-sqs-policy")["PolicyDocument"]
    pipes_lambda_doc = iam.get_role_policy(RoleName=pipes_role["name"], PolicyName="pipes-lambda-policy")["PolicyDocument"]
    pipes_states_doc = iam.get_role_policy(RoleName=pipes_role["name"], PolicyName="pipes-states-policy")["PolicyDocument"]
    sfn_doc = iam.get_role_policy(RoleName=sfn_role["name"], PolicyName="step-functions-lambda")["PolicyDocument"]

    assert not any(statement.get("Resource") == "*" for statement in statement_list(frontend_role_doc))
    assert all(
        statement.get("Resource") != "*" or is_allowed_eni_wildcard_statement(statement)
        for statement in statement_list(worker_role_doc)
    )
    assert all(
        statement.get("Resource") != "*" or is_allowed_eni_wildcard_statement(statement)
        for statement in statement_list(backend_role_doc)
    )
    assert not any(statement.get("Resource") == "*" for statement in statement_list(pipes_sqs_doc))
    assert not any(statement.get("Resource") == "*" for statement in statement_list(pipes_lambda_doc))
    assert not any(statement.get("Resource") == "*" for statement in statement_list(pipes_states_doc))

    secret_stmt = matching_statement(
        backend_role_doc,
        lambda statement: "secretsmanager:GetSecretValue" in as_list(statement["Action"]),
    )
    assert secret_stmt["Resource"] == secret["arn"]

    sqs_stmt = matching_statement(
        backend_role_doc,
        lambda statement: "sqs:SendMessage" in as_list(statement["Action"]),
    )
    assert sqs_stmt["Resource"] == queue_values("ingest_queue")["arn"]

    pipe_read_stmt = matching_statement(
        pipes_sqs_doc,
        lambda statement: "sqs:ReceiveMessage" in as_list(statement["Action"]),
    )
    assert pipe_read_stmt["Resource"] == queue_values("ingest_queue")["arn"]

    pipe_lambda_stmt = matching_statement(
        pipes_lambda_doc,
        lambda statement: "lambda:InvokeFunction" in as_list(statement["Action"]),
    )
    assert pipe_lambda_stmt["Resource"] == worker_fn["arn"]

    pipe_states_stmt = matching_statement(
        pipes_states_doc,
        lambda statement: "states:StartExecution" in as_list(statement["Action"]),
    )
    assert pipe_states_stmt["Resource"] == state_machine["arn"]

    sfn_invoke_stmt = matching_statement(
        sfn_doc,
        lambda statement: "lambda:InvokeFunction" in as_list(statement["Action"]),
    )
    assert sfn_invoke_stmt["Resource"] == worker_fn["arn"]

    frontend_assume = iam.get_role(RoleName=frontend_role["name"])["Role"]["AssumeRolePolicyDocument"]
    backend_assume = iam.get_role(RoleName=backend_role["name"])["Role"]["AssumeRolePolicyDocument"]
    worker_assume = iam.get_role(RoleName=worker_role["name"])["Role"]["AssumeRolePolicyDocument"]
    pipes_assume = iam.get_role(RoleName=pipes_role["name"])["Role"]["AssumeRolePolicyDocument"]
    sfn_assume = iam.get_role(RoleName=sfn_role["name"])["Role"]["AssumeRolePolicyDocument"]
    assert matching_statement(frontend_assume, lambda s: s["Principal"]["Service"] == "lambda.amazonaws.com")
    assert matching_statement(backend_assume, lambda s: s["Principal"]["Service"] == "lambda.amazonaws.com")
    assert matching_statement(worker_assume, lambda s: s["Principal"]["Service"] == "lambda.amazonaws.com")
    assert matching_statement(pipes_assume, lambda s: s["Principal"]["Service"] == "pipes.amazonaws.com")
    assert matching_statement(sfn_assume, lambda s: s["Principal"]["Service"] == "states.amazonaws.com")
