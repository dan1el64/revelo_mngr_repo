import json
import os
from pathlib import Path

import boto3
import botocore.exceptions
import pytest


STACK_NAME = "InternalWebAppStack"


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


@pytest.fixture(scope="session")
def stack_resources():
    if not (
        (os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))
        or os.environ.get("AWS_PROFILE")
        or os.environ.get("AWS_SESSION_TOKEN")
    ):
        pytest.fail("integration tests require AWS credentials")

    cloudformation = _client("cloudformation")
    try:
        paginator = cloudformation.get_paginator("list_stack_resources")
        pages = paginator.paginate(StackName=STACK_NAME)
        resources = []
        for page in pages:
            resources.extend(page["StackResourceSummaries"])
    except (
        botocore.exceptions.ClientError,
        botocore.exceptions.NoCredentialsError,
    ) as exc:
        pytest.fail(
            f"stack {STACK_NAME} is not available for integration testing: {exc}"
        )
    return resources


@pytest.fixture(scope="session")
def stack_template():
    candidate_paths = [
        Path("template.json"),
        Path("cdk.out") / "InternalWebAppStack.template.json",
    ]
    candidate_paths.extend(Path("cdk.out").glob("*.template.json"))

    for candidate in candidate_paths:
        if candidate.exists():
            return json.loads(candidate.read_text())

    cloudformation = _client("cloudformation")
    try:
        response = cloudformation.get_template(
            StackName=STACK_NAME,
            TemplateStage="Original",
        )
    except botocore.exceptions.ClientError as exc:
        pytest.fail(f"unable to retrieve template for {STACK_NAME}: {exc}")
    template_body = response["TemplateBody"]
    if isinstance(template_body, str):
        template_body = template_body.strip()
        if not template_body:
            pytest.fail(f"template body for {STACK_NAME} is empty")
        try:
            return json.loads(template_body)
        except json.JSONDecodeError:
            pytest.fail(
                f"unable to parse template for {STACK_NAME} from local files or CloudFormation as JSON"
            )
    return template_body


def _resource_of_type(stack_resources, resource_type: str):
    matches = [resource for resource in stack_resources if resource["ResourceType"] == resource_type]
    assert matches, f"missing resource type {resource_type}"
    return matches


def test_stack_contains_expected_live_resources(stack_resources):
    assert len(_resource_of_type(stack_resources, "AWS::Lambda::Function")) == 2
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
    queue_resource = _resource_of_type(stack_resources, "AWS::SQS::Queue")[0]
    lambda_resources = _resource_of_type(stack_resources, "AWS::Lambda::Function")

    runtimes = set()
    for resource in lambda_resources:
        configuration = lambda_client.get_function_configuration(
            FunctionName=resource["PhysicalResourceId"]
        )
        runtimes.add(configuration["Runtime"])
        assert configuration["MemorySize"] == 256
        assert configuration["Timeout"] == 15

    assert runtimes == {"python3.12"}

    sqs_client = _client("sqs")
    queue_url = sqs_client.get_queue_url(QueueName=queue_resource["PhysicalResourceId"].split("/")[-1])["QueueUrl"]
    attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod"],
    )["Attributes"]
    assert attributes["VisibilityTimeout"] == "30"
    assert attributes["MessageRetentionPeriod"] == "345600"


def test_api_pipe_and_state_machine_wiring(stack_resources, stack_template):
    api_resource = _resource_of_type(stack_resources, "AWS::ApiGatewayV2::Api")[0]
    assert api_resource["PhysicalResourceId"]

    resources = stack_template["Resources"]
    route_definition = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::ApiGatewayV2::Route"
    )
    stage_definition = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::ApiGatewayV2::Stage"
    )
    pipe_definition = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Pipes::Pipe"
    )
    state_machine_definition = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::StepFunctions::StateMachine"
    )

    assert route_definition["RouteKey"] == "POST /orders"
    assert stage_definition["StageName"] == "$default"
    assert stage_definition["AutoDeploy"] is True
    assert "AccessLogSettings" in stage_definition
    assert pipe_definition["DesiredState"] == "RUNNING"
    assert pipe_definition["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert (
        pipe_definition["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
        == "FIRE_AND_FORGET"
    )
    assert "messageBody" in pipe_definition["TargetParameters"]["InputTemplate"]
    assert state_machine_definition["StateMachineType"] == "STANDARD"
    assert state_machine_definition["LoggingConfiguration"]["Level"] == "ALL"


def test_database_and_analytics_resources_are_private(stack_template):
    resources = stack_template["Resources"]

    db_instance = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::RDS::DBInstance"
    )
    redshift_cluster = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Redshift::Cluster"
    )
    glue_database = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Glue::Database"
    )
    glue_connection = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Glue::Connection"
    )
    glue_crawler = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Glue::Crawler"
    )

    assert db_instance["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["EngineVersion"] == "15.5"
    assert db_instance["PubliclyAccessible"] is False
    assert db_instance["StorageType"] == "gp2"

    assert redshift_cluster["NodeType"] == "dc2.large"
    assert redshift_cluster["ClusterType"] == "single-node"
    assert redshift_cluster["PubliclyAccessible"] is False
    assert redshift_cluster["DBName"] == "analytics"

    assert glue_database["DatabaseInput"]["Name"] == "analytics_catalog"
    assert glue_connection["ConnectionInput"]["ConnectionType"] == "JDBC"
    assert "JDBC_CONNECTION_URL" in glue_connection["ConnectionInput"]["ConnectionProperties"]
    assert "SECRET_ID" in glue_connection["ConnectionInput"]["ConnectionProperties"]
    assert glue_crawler["DatabaseName"] == "analytics_catalog"
    assert len(glue_crawler["Targets"]["JdbcTargets"]) == 1
