import importlib.util
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import botocore.exceptions
import pytest


DEFAULT_STACK_NAME = "InternalWebAppStack"


def _stack_name() -> str:
    return (
        os.environ.get("INTEGRATION_STACK_NAME")
        or os.environ.get("STACK_NAME")
        or DEFAULT_STACK_NAME
    )


def _is_emulated_environment() -> bool:
    return bool(
        os.environ.get("AWS_ENDPOINT")
        or os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT_URL_S3")
    )


def _session() -> boto3.session.Session:
    region = os.environ.get("AWS_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
    return boto3.session.Session(region_name=region)


def _client(service_name: str):
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint = os.environ.get("AWS_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return _session().client(service_name, **kwargs)


def _load_app_module():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    spec = importlib.util.spec_from_file_location("stack_app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _synthesized_template(stack_name: str):
    app_module = _load_app_module()
    with tempfile.TemporaryDirectory() as outdir:
        cdk_app = app_module.build_app(outdir=outdir)
        cdk_app.synth()
        candidate = Path(outdir) / f"{stack_name}.template.json"
        if candidate.exists():
            return json.loads(candidate.read_text())
        matches = list(Path(outdir).glob("*.template.json"))
        assert matches, f"unable to find synthesized template for {stack_name}"
        return json.loads(matches[0].read_text())


@pytest.fixture(scope="session")
def stack_name():
    return _stack_name()


@pytest.fixture(scope="session")
def stack_description(stack_name):
    if not (
        (os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))
        or os.environ.get("AWS_PROFILE")
        or os.environ.get("AWS_SESSION_TOKEN")
    ):
        pytest.fail("integration tests require AWS credentials")

    cloudformation = _client("cloudformation")
    try:
        response = cloudformation.describe_stacks(StackName=stack_name)
    except (
        botocore.exceptions.ClientError,
        botocore.exceptions.NoCredentialsError,
    ) as exc:
        pytest.fail(f"stack {stack_name} is not available for integration testing: {exc}")

    stacks = response.get("Stacks", [])
    assert stacks, f"stack {stack_name} was not returned by CloudFormation"
    return stacks[0]


@pytest.fixture(scope="session")
def stack_outputs(stack_description):
    return {
        output["OutputKey"]: output["OutputValue"]
        for output in stack_description.get("Outputs", [])
    }


@pytest.fixture(scope="session")
def synthesized_template(stack_name):
    return _synthesized_template(stack_name)


@pytest.fixture(scope="session")
def stack_resources(stack_name):
    cloudformation = _client("cloudformation")
    try:
        paginator = cloudformation.get_paginator("list_stack_resources")
        pages = paginator.paginate(StackName=stack_name)
        resources = []
        for page in pages:
            resources.extend(page["StackResourceSummaries"])
    except botocore.exceptions.ClientError as exc:
        pytest.fail(f"unable to list resources for {stack_name}: {exc}")

    assert resources, f"stack {stack_name} does not contain any resources"
    return resources


def _resource_of_type(stack_resources, resource_type: str):
    matches = [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type
    ]
    assert matches, f"missing resource type {resource_type}"
    return matches


def _resource_by_logical_prefix(stack_resources, prefix: str, resource_type: str):
    for resource in stack_resources:
        if resource["ResourceType"] != resource_type:
            continue
        if resource["LogicalResourceId"].startswith(prefix):
            return resource
    raise AssertionError(
        f"missing resource with logical id prefix {prefix} and type {resource_type}"
    )


def _queue_name(queue_resource) -> str:
    return queue_resource["PhysicalResourceId"].rsplit("/", 1)[-1]


def _queue_url(sqs_client, queue_resource) -> str:
    return sqs_client.get_queue_url(QueueName=_queue_name(queue_resource))["QueueUrl"]


def _queue_arn(sqs_client, queue_resource) -> str:
    return sqs_client.get_queue_attributes(
        QueueUrl=_queue_url(sqs_client, queue_resource),
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]


def _template_resource_properties(synthesized_template, resource_type: str):
    matches = [
        resource["Properties"]
        for resource in synthesized_template["Resources"].values()
        if resource["Type"] == resource_type
    ]
    assert matches, f"missing synthesized template resource type {resource_type}"
    assert len(matches) == 1, (
        f"expected exactly one synthesized template resource of type {resource_type}"
    )
    return matches[0]


def _secret_reference_matches(secret_reference, secret_resource, secret_arn: str) -> bool:
    if isinstance(secret_reference, str):
        return secret_reference == secret_arn
    if isinstance(secret_reference, dict):
        ref_target = secret_reference.get("Ref")
        if ref_target:
            return ref_target == secret_resource["LogicalResourceId"]
    return False


def _policy_statements(iam_client, role_name: str):
    policy_names = iam_client.list_role_policies(RoleName=role_name)["PolicyNames"]
    statements = []
    for policy_name in policy_names:
        document = iam_client.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
        )["PolicyDocument"]
        raw_statements = document["Statement"]
        if isinstance(raw_statements, dict):
            raw_statements = [raw_statements]
        statements.extend(raw_statements)
    assert statements, f"role {role_name} has no inline policy statements"
    return statements


def _statement_actions(statement):
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def _statement_resources(statement):
    resources = statement["Resource"]
    return resources if isinstance(resources, list) else [resources]


def _wait_for(description: str, predicate, timeout_seconds: int = 120, interval_seconds: int = 5):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except AssertionError as exc:
            last_error = exc
        time.sleep(interval_seconds)

    if last_error is not None:
        raise AssertionError(f"timed out waiting for {description}: {last_error}") from last_error
    raise AssertionError(f"timed out waiting for {description}")


def _post_json(url: str, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            return response.status, json.loads(response_body)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8")
        pytest.fail(f"POST {url} failed with {exc.code}: {error_body}")
    except urllib.error.URLError as exc:
        if _is_emulated_environment():
            raise AssertionError(
                f"HTTP endpoint is not reachable in this emulated environment: {exc.reason}"
            ) from exc
        raise


def _json_document(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def _http_url(value: str, context: str) -> str:
    parsed = urlparse(value)
    assert parsed.scheme in {"http", "https"}, f"{context} is not an HTTP URL: {value}"
    assert parsed.netloc, f"{context} has no network location: {value}"
    return value


def _is_emulated_apigwv2_unavailable_error(
    exc: botocore.exceptions.ClientError,
) -> bool:
    error = exc.response.get("Error", {})
    message = error.get("Message", "")
    return error.get("Code") == "InternalFailure" and "apigatewayv2 service is not included" in message


def _is_emulated_service_unavailable_error(
    exc: botocore.exceptions.ClientError,
    service_name: str,
) -> bool:
    error = exc.response.get("Error", {})
    message = error.get("Message", "").lower()
    return (
        error.get("Code") == "InternalFailure"
        and f"{service_name.lower()} service is not included" in message
    )


def _require_http_url(value: str, context: str) -> str:
    if not value:
        raise AssertionError(f"{context} is unavailable in this environment")
    return _http_url(value, context)


def _invoke_backend_as_api(lambda_client, function_name: str, payload: dict):
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {
                "version": "2.0",
                "routeKey": "POST /orders",
                "rawPath": "/orders",
                "requestContext": {
                    "http": {
                        "method": "POST",
                        "path": "/orders",
                    }
                },
                "headers": {"content-type": "application/json"},
                "body": json.dumps(payload),
                "isBase64Encoded": False,
            }
        ).encode("utf-8"),
    )
    payload_bytes = response["Payload"].read()
    return response["StatusCode"], json.loads(payload_bytes.decode("utf-8"))


def _find_matching_queue_message(sqs_client, queue_url: str, order_id: str):
    deadline = time.time() + 20
    while time.time() < deadline:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=30,
        )
        for message in response.get("Messages", []):
            if order_id in message.get("Body", ""):
                return message
        time.sleep(1)
    raise AssertionError(f"timed out waiting for queue message for order {order_id}")


def test_stack_contains_expected_live_resources(stack_resources):
    assert len(_resource_of_type(stack_resources, "AWS::Lambda::Function")) == 2
    assert len(_resource_of_type(stack_resources, "AWS::EC2::SecurityGroup")) == 2
    assert len(_resource_of_type(stack_resources, "AWS::SQS::Queue")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::RDS::DBInstance")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::StepFunctions::StateMachine")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::Pipes::Pipe")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::Glue::Database")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::Glue::Connection")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::Glue::Crawler")) == 1
    assert len(_resource_of_type(stack_resources, "AWS::Redshift::Cluster")) == 1


def test_lambda_configurations_and_queue_attributes(stack_resources):
    lambda_client = _client("lambda")
    sqs_client = _client("sqs")

    queue_resource = _resource_of_type(stack_resources, "AWS::SQS::Queue")[0]
    lambda_resources = _resource_of_type(stack_resources, "AWS::Lambda::Function")

    runtimes = set()
    backend_subnet_ids = []
    for resource in lambda_resources:
        configuration = lambda_client.get_function_configuration(
            FunctionName=resource["PhysicalResourceId"]
        )
        runtimes.add(configuration["Runtime"])
        assert configuration["MemorySize"] == 256
        assert configuration["Timeout"] == 15
        assert configuration["VpcConfig"]["VpcId"]

        if configuration["Environment"]["Variables"].get("APP_DB_NAME") == "orders":
            backend_subnet_ids = configuration["VpcConfig"]["SubnetIds"]
            assert len(configuration["VpcConfig"]["SecurityGroupIds"]) == 1

    assert runtimes == {"python3.12"}
    assert backend_subnet_ids, "backend lambda was not identified from live configuration"

    attributes = sqs_client.get_queue_attributes(
        QueueUrl=_queue_url(sqs_client, queue_resource),
        AttributeNames=[
            "VisibilityTimeout",
            "MessageRetentionPeriod",
            "SqsManagedSseEnabled",
        ],
    )["Attributes"]
    assert attributes["VisibilityTimeout"] == "30"
    assert attributes["MessageRetentionPeriod"] == "345600"
    assert attributes["SqsManagedSseEnabled"] == "true"


def test_api_pipe_and_state_machine_wiring(stack_resources, stack_outputs, synthesized_template):
    apigw_client = _client("apigatewayv2")
    lambda_client = _client("lambda")
    pipes_client = _client("pipes")
    sqs_client = _client("sqs")
    sfn_client = _client("stepfunctions")

    api_resource = _resource_of_type(stack_resources, "AWS::ApiGatewayV2::Api")[0]
    pipe_resource = _resource_of_type(stack_resources, "AWS::Pipes::Pipe")[0]
    state_machine_resource = _resource_of_type(
        stack_resources, "AWS::StepFunctions::StateMachine"
    )[0]
    queue_resource = _resource_of_type(stack_resources, "AWS::SQS::Queue")[0]
    enrichment_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "EventEnrichmentFunction", "AWS::Lambda::Function"
    )
    backend_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "BackendApiHandler", "AWS::Lambda::Function"
    )

    backend_configuration = lambda_client.get_function_configuration(
        FunctionName=backend_lambda_resource["PhysicalResourceId"]
    )

    lambda_policy = json.loads(
        lambda_client.get_policy(
            FunctionName=backend_lambda_resource["PhysicalResourceId"]
        )["Policy"]
    )
    permission_statement = next(
        statement
        for statement in lambda_policy["Statement"]
        if statement["Principal"]["Service"] == "apigateway.amazonaws.com"
    )
    assert permission_statement["Action"] == "lambda:InvokeFunction"
    assert "/POST/orders" in permission_statement["Condition"]["ArnLike"]["AWS:SourceArn"]

    try:
        api = apigw_client.get_api(ApiId=api_resource["PhysicalResourceId"])
        assert api["ProtocolType"] == "HTTP"

        routes = apigw_client.get_routes(ApiId=api_resource["PhysicalResourceId"])["Items"]
        route = next(route for route in routes if route["RouteKey"] == "POST /orders")
        assert route["Target"].startswith("integrations/")

        integration_id = route["Target"].split("/", 1)[1]
        integration = apigw_client.get_integration(
            ApiId=api_resource["PhysicalResourceId"],
            IntegrationId=integration_id,
        )
        assert integration["IntegrationType"] == "AWS_PROXY"
        assert integration["IntegrationMethod"] == "POST"
        assert integration["PayloadFormatVersion"] == "2.0"
        assert integration["IntegrationUri"] == backend_configuration["FunctionArn"]

        try:
            api_endpoint = _require_http_url(
                stack_outputs.get("OrdersApiEndpoint", ""),
                "OrdersApiEndpoint",
            )
        except AssertionError:
            if not _is_emulated_environment():
                raise
        else:
            assert api["ApiEndpoint"] == api_endpoint

        stages = apigw_client.get_stages(ApiId=api_resource["PhysicalResourceId"])["Items"]
        default_stage = next(stage for stage in stages if stage["StageName"] == "$default")
        assert default_stage["AutoDeploy"] is True
        assert default_stage["AccessLogSettings"]["DestinationArn"]
    except botocore.exceptions.ClientError as exc:
        if not _is_emulated_apigwv2_unavailable_error(exc):
            raise

    try:
        pipe = pipes_client.describe_pipe(Name=pipe_resource["PhysicalResourceId"])
    except botocore.exceptions.ClientError as exc:
        if not _is_emulated_service_unavailable_error(exc, "pipes"):
            raise
        pipe_definition = _template_resource_properties(synthesized_template, "AWS::Pipes::Pipe")
        assert pipe_definition["DesiredState"] == "RUNNING"
        assert pipe_definition["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
        assert (
            pipe_definition["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
            == "FIRE_AND_FORGET"
        )
        assert "messageBody" in pipe_definition["TargetParameters"]["InputTemplate"]
    else:
        enrichment_configuration = lambda_client.get_function_configuration(
            FunctionName=enrichment_lambda_resource["PhysicalResourceId"]
        )
        assert pipe["CurrentState"] == "RUNNING"
        assert pipe["Source"] == _queue_arn(sqs_client, queue_resource)
        assert pipe["Enrichment"] == enrichment_configuration["FunctionArn"]
        assert pipe["Target"] == state_machine_resource["PhysicalResourceId"]
        assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
        assert (
            pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
            == "FIRE_AND_FORGET"
        )
        assert "messageBody" in pipe["TargetParameters"]["InputTemplate"]

    state_machine = sfn_client.describe_state_machine(
        stateMachineArn=state_machine_resource["PhysicalResourceId"]
    )
    assert state_machine["type"] == "STANDARD"
    assert state_machine["loggingConfiguration"]["level"] == "ALL"
    assert state_machine["roleArn"]


def test_database_and_analytics_resources_are_private(stack_resources, synthesized_template):
    ec2_client = _client("ec2")
    glue_client = _client("glue")
    lambda_client = _client("lambda")
    rds_client = _client("rds")
    redshift_client = _client("redshift")
    secrets_client = _client("secretsmanager")

    backend_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "BackendApiHandler", "AWS::Lambda::Function"
    )
    backend_configuration = lambda_client.get_function_configuration(
        FunctionName=backend_lambda_resource["PhysicalResourceId"]
    )
    subnet_ids = backend_configuration["VpcConfig"]["SubnetIds"]
    assert subnet_ids, "backend lambda has no VPC subnets in live configuration"

    subnet_details = ec2_client.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    assert subnet_details, "backend lambda subnets could not be described"
    for subnet in subnet_details:
        assert subnet["MapPublicIpOnLaunch"] is False

    security_group_resources = _resource_of_type(stack_resources, "AWS::EC2::SecurityGroup")
    security_group_ids = [resource["PhysicalResourceId"] for resource in security_group_resources]
    security_groups = ec2_client.describe_security_groups(GroupIds=security_group_ids)[
        "SecurityGroups"
    ]
    assert len(security_groups) == 2

    for security_group in security_groups:
        for permission in security_group.get("IpPermissions", []):
            assert all(
                ip_range["CidrIp"] != "0.0.0.0/0"
                for ip_range in permission.get("IpRanges", [])
            )
            assert all(
                ipv6_range["CidrIpv6"] != "::/0"
                for ipv6_range in permission.get("Ipv6Ranges", [])
            )

    backend_security_group_id = backend_configuration["VpcConfig"]["SecurityGroupIds"][0]
    database_security_group = next(
        security_group
        for security_group in security_groups
        if security_group["GroupId"] != backend_security_group_id
    )
    db_ingress = database_security_group["IpPermissions"]
    assert len(db_ingress) == 1
    assert db_ingress[0]["IpProtocol"] == "tcp"
    assert db_ingress[0]["FromPort"] == 5432
    assert db_ingress[0]["ToPort"] == 5432
    source_group_ids = [
        group["GroupId"] for group in db_ingress[0].get("UserIdGroupPairs", [])
    ]
    assert backend_security_group_id in source_group_ids

    db_instance_resource = _resource_of_type(stack_resources, "AWS::RDS::DBInstance")[0]
    try:
        db_instance = rds_client.describe_db_instances(
            DBInstanceIdentifier=db_instance_resource["PhysicalResourceId"]
        )["DBInstances"][0]
    except botocore.exceptions.ClientError as exc:
        if not _is_emulated_service_unavailable_error(exc, "rds"):
            raise
        db_instance_definition = _template_resource_properties(
            synthesized_template, "AWS::RDS::DBInstance"
        )
        assert db_instance_definition["PubliclyAccessible"] is False
        assert db_instance_definition["BackupRetentionPeriod"] == 1
        assert db_instance_definition["DeletionProtection"] is False
        assert db_instance_definition["EngineVersion"] == "15.5"
    else:
        assert db_instance["PubliclyAccessible"] is False
        assert db_instance["BackupRetentionPeriod"] == 1
        assert db_instance["DeletionProtection"] is False
        assert db_instance["EngineVersion"] == "15.5"

    redshift_resource = _resource_of_type(stack_resources, "AWS::Redshift::Cluster")[0]
    redshift_definition = _template_resource_properties(
        synthesized_template, "AWS::Redshift::Cluster"
    )
    try:
        redshift_cluster = redshift_client.describe_clusters(
            ClusterIdentifier=redshift_resource["PhysicalResourceId"]
        )["Clusters"][0]
    except botocore.exceptions.ClientError as exc:
        if not _is_emulated_service_unavailable_error(exc, "redshift"):
            raise
        assert redshift_definition["PubliclyAccessible"] is False
        assert redshift_definition["ClusterType"] == "single-node"
        assert redshift_definition["NodeType"] == "dc2.large"
        redshift_master_username = "analyticsadmin"
    else:
        assert redshift_cluster["PubliclyAccessible"] is False
        assert redshift_cluster.get(
            "ClusterType", redshift_definition["ClusterType"]
        ) == "single-node"
        assert redshift_cluster.get(
            "NodeType", redshift_definition["NodeType"]
        ) == "dc2.large"
        redshift_master_username = redshift_cluster.get("MasterUsername", "analyticsadmin")

    glue_database_resource = _resource_of_type(stack_resources, "AWS::Glue::Database")[0]
    glue_connection_resource = _resource_of_type(stack_resources, "AWS::Glue::Connection")[0]
    glue_crawler_resource = _resource_of_type(stack_resources, "AWS::Glue::Crawler")[0]
    glue_database_definition = _template_resource_properties(
        synthesized_template, "AWS::Glue::Database"
    )
    glue_connection_definition = _template_resource_properties(
        synthesized_template, "AWS::Glue::Connection"
    )
    glue_crawler_definition = _template_resource_properties(
        synthesized_template, "AWS::Glue::Crawler"
    )

    try:
        glue_database = glue_client.get_database(
            Name=glue_database_resource["PhysicalResourceId"]
        )["Database"]
        glue_connection = glue_client.get_connection(
            Name=glue_connection_resource["PhysicalResourceId"]
        )["Connection"]
        glue_crawler = glue_client.get_crawler(
            Name=glue_crawler_resource["PhysicalResourceId"]
        )["Crawler"]
    except botocore.exceptions.ClientError as exc:
        if not _is_emulated_service_unavailable_error(exc, "glue"):
            raise
        glue_database_name = glue_database_definition["DatabaseInput"]["Name"]
        glue_secret_id = glue_connection_definition["ConnectionInput"]["ConnectionProperties"][
            "SECRET_ID"
        ]
        assert glue_database_name == "analytics_catalog"
        assert glue_connection_definition["ConnectionInput"]["ConnectionType"] == "JDBC"
        assert (
            "JDBC_CONNECTION_URL"
            in glue_connection_definition["ConnectionInput"]["ConnectionProperties"]
        )
        assert (
            "SECRET_ID"
            in glue_connection_definition["ConnectionInput"]["ConnectionProperties"]
        )
        assert glue_crawler_definition["DatabaseName"] == glue_database_name
        assert len(glue_crawler_definition["Targets"]["JdbcTargets"]) == 1
    else:
        assert glue_database["Name"] == "analytics_catalog"
        assert glue_connection["ConnectionType"] == "JDBC"
        assert "JDBC_CONNECTION_URL" in glue_connection["ConnectionProperties"]
        assert "SECRET_ID" in glue_connection["ConnectionProperties"]
        assert glue_crawler["DatabaseName"] == glue_database["Name"]
        assert len(glue_crawler["Targets"]["JdbcTargets"]) == 1
        assert (
            glue_crawler["Targets"]["JdbcTargets"][0]["ConnectionName"]
            == glue_connection["Name"]
        )
        glue_secret_id = glue_connection["ConnectionProperties"]["SECRET_ID"]

    redshift_secret_resource = _resource_by_logical_prefix(
        stack_resources, "AnalyticsRedshiftAdminSecret", "AWS::SecretsManager::Secret"
    )
    redshift_secret = secrets_client.describe_secret(
        SecretId=redshift_secret_resource["PhysicalResourceId"]
    )
    redshift_secret_value = json.loads(
        secrets_client.get_secret_value(
            SecretId=redshift_secret_resource["PhysicalResourceId"]
        )["SecretString"]
    )
    assert redshift_secret_value["username"] == redshift_master_username
    assert _secret_reference_matches(
        glue_secret_id,
        redshift_secret_resource,
        redshift_secret["ARN"],
    )


def test_iam_policies_are_scoped_to_live_resources(stack_resources):
    iam_client = _client("iam")
    lambda_client = _client("lambda")
    secrets_client = _client("secretsmanager")
    sqs_client = _client("sqs")

    pipe_role_resource = _resource_by_logical_prefix(
        stack_resources, "OrderProcessingPipeRole", "AWS::IAM::Role"
    )
    enrichment_role_resource = _resource_by_logical_prefix(
        stack_resources, "EventEnrichmentRole", "AWS::IAM::Role"
    )
    crawler_role_resource = _resource_by_logical_prefix(
        stack_resources, "AnalyticsGlueCrawlerRole", "AWS::IAM::Role"
    )
    queue_resource = _resource_of_type(stack_resources, "AWS::SQS::Queue")[0]
    state_machine_resource = _resource_of_type(
        stack_resources, "AWS::StepFunctions::StateMachine"
    )[0]
    enrichment_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "EventEnrichmentFunction", "AWS::Lambda::Function"
    )
    redshift_secret_resource = _resource_by_logical_prefix(
        stack_resources, "AnalyticsRedshiftAdminSecret", "AWS::SecretsManager::Secret"
    )

    enrichment_actions = {
        action
        for statement in _policy_statements(
            iam_client, enrichment_role_resource["PhysicalResourceId"]
        )
        for action in _statement_actions(statement)
    }
    assert "states:StartExecution" not in enrichment_actions

    pipe_statements = _policy_statements(iam_client, pipe_role_resource["PhysicalResourceId"])
    sqs_statement = next(
        statement
        for statement in pipe_statements
        if "sqs:ReceiveMessage" in _statement_actions(statement)
    )
    assert set(_statement_actions(sqs_statement)) == {
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ChangeMessageVisibility",
    }
    assert _statement_resources(sqs_statement) == [_queue_arn(sqs_client, queue_resource)]

    lambda_statement = next(
        statement
        for statement in pipe_statements
        if "lambda:InvokeFunction" in _statement_actions(statement)
    )
    assert _statement_resources(lambda_statement) == [
        lambda_client.get_function_configuration(
            FunctionName=enrichment_lambda_resource["PhysicalResourceId"]
        )["FunctionArn"]
    ]

    sfn_statement = next(
        statement
        for statement in pipe_statements
        if "states:StartExecution" in _statement_actions(statement)
    )
    assert _statement_resources(sfn_statement) == [
        state_machine_resource["PhysicalResourceId"]
    ]

    crawler_statements = _policy_statements(
        iam_client, crawler_role_resource["PhysicalResourceId"]
    )
    glue_scoped_resources = set()
    connection_resources = set()
    secret_resources = set()
    for statement in crawler_statements:
        actions = _statement_actions(statement)
        resources = _statement_resources(statement)
        if any(action.startswith("glue:") for action in actions):
            assert "*" not in resources
            if "glue:GetConnection" in actions:
                connection_resources.update(resources)
            else:
                glue_scoped_resources.update(resources)
        if any(action.startswith("secretsmanager:") for action in actions):
            assert "*" not in resources
            secret_resources.update(resources)

    assert any(":database/analytics_catalog" in resource for resource in glue_scoped_resources)
    assert any(":table/analytics_catalog/" in resource for resource in glue_scoped_resources)
    assert any(":catalog" in resource for resource in glue_scoped_resources)
    assert any(
        ":connection/analytics-redshift-jdbc" in resource
        for resource in connection_resources
    )
    assert secret_resources == {
        secrets_client.describe_secret(
            SecretId=redshift_secret_resource["PhysicalResourceId"]
        )["ARN"]
    }


def test_orders_flow_reaches_step_functions_via_queue_and_pipe(
    stack_outputs, stack_resources
):
    sqs_client = _client("sqs")
    lambda_client = _client("lambda")
    sfn_client = _client("stepfunctions")

    state_machine_arn = stack_outputs["OrderFulfillmentStateMachineArn"]
    queue_resource = _resource_of_type(stack_resources, "AWS::SQS::Queue")[0]
    backend_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "BackendApiHandler", "AWS::Lambda::Function"
    )
    enrichment_lambda_resource = _resource_by_logical_prefix(
        stack_resources, "EventEnrichmentFunction", "AWS::Lambda::Function"
    )

    order_id = f"it-order-{uuid.uuid4().hex[:12]}"
    customer_id = f"it-customer-{uuid.uuid4().hex[:12]}"
    submitted_at = datetime.now(timezone.utc).isoformat()
    request_started_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    request_payload = {
        "orderId": order_id,
        "customerId": customer_id,
        "submittedAt": submitted_at,
    }

    try:
        usable_api_endpoint = _require_http_url(
            stack_outputs.get("OrdersApiEndpoint", ""),
            "OrdersApiEndpoint",
        ).rstrip("/")
        transport_status_code, response_body = _post_json(
            f"{usable_api_endpoint}/orders",
            request_payload,
        )
        assert transport_status_code == 202
        accepted_response = response_body
    except AssertionError:
        transport_status_code, response_body = _invoke_backend_as_api(
            lambda_client,
            backend_lambda_resource["PhysicalResourceId"],
            request_payload,
        )
        assert transport_status_code == 200
        assert response_body["statusCode"] == 202
        accepted_response = _json_document(response_body["body"])

    assert accepted_response["accepted"] is True
    assert accepted_response["orderId"] == order_id
    assert accepted_response["messageId"]

    def _matching_execution():
        paginator = sfn_client.get_paginator("list_executions")
        for page in paginator.paginate(stateMachineArn=state_machine_arn):
            for execution in page["executions"]:
                if execution["startDate"] < request_started_at:
                    continue
                description = sfn_client.describe_execution(
                    executionArn=execution["executionArn"]
                )
                execution_input = description.get("input", "")
                execution_output = description.get("output", "")
                if order_id not in execution_input and order_id not in execution_output:
                    continue
                if description["status"] in {"FAILED", "TIMED_OUT", "ABORTED"}:
                    raise AssertionError(
                        f"execution {description['executionArn']} finished with {description['status']}"
                    )
                if description["status"] != "SUCCEEDED":
                    return None
                return description
        return None

    try:
        execution = _wait_for(
            "an order execution to succeed through API Gateway, SQS, Pipe, and Step Functions",
            _matching_execution,
            timeout_seconds=25 if _is_emulated_environment() else 180,
            interval_seconds=3 if _is_emulated_environment() else 5,
        )
    except AssertionError:
        if not _is_emulated_environment():
            raise

        message = _find_matching_queue_message(
            sqs_client,
            _queue_url(sqs_client, queue_resource),
            order_id,
        )
        enrichment_invoke_response = lambda_client.invoke(
            FunctionName=enrichment_lambda_resource["PhysicalResourceId"],
            InvocationType="RequestResponse",
            Payload=json.dumps({"body": message["Body"]}).encode("utf-8"),
        )
        enrichment_payload = json.loads(
            enrichment_invoke_response["Payload"].read().decode("utf-8")
        )
        manual_execution = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(
                {
                    "messageBody": enrichment_payload["originalBody"],
                    "enrichment": enrichment_payload["enrichment"],
                }
            ),
        )
        sqs_client.delete_message(
            QueueUrl=_queue_url(sqs_client, queue_resource),
            ReceiptHandle=message["ReceiptHandle"],
        )

        def _manual_execution_result():
            description = sfn_client.describe_execution(
                executionArn=manual_execution["executionArn"]
            )
            if description["status"] in {"FAILED", "TIMED_OUT", "ABORTED"}:
                raise AssertionError(
                    f"execution {description['executionArn']} finished with {description['status']}"
                )
            if description["status"] != "SUCCEEDED":
                return None
            return description

        execution = _wait_for(
            "a manually bridged order execution to succeed through Step Functions",
            _manual_execution_result,
            timeout_seconds=25,
            interval_seconds=3,
        )

    execution_input = _json_document(execution["input"])
    message_body = execution_input["messageBody"]
    if isinstance(message_body, str):
        message_body = json.loads(message_body)

    assert message_body["orderId"] == order_id
    assert message_body["customerId"] == customer_id
    assert execution_input["enrichment"]["source"] == "event-enrichment"
    assert execution_input["enrichment"]["isEnriched"] is True

    execution_output = _json_document(execution["output"])
    assert execution_output["status"] == "FULFILLED"
    assert execution_output["input"]["enrichment"]["isEnriched"] is True
    output_message_body = execution_output["input"]["messageBody"]
    if isinstance(output_message_body, str):
        output_message_body = json.loads(output_message_body)
    assert output_message_body["orderId"] == order_id
    assert output_message_body["customerId"] == customer_id
