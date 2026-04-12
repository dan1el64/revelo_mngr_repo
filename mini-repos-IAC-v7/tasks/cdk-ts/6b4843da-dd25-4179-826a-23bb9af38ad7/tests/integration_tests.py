import json
import os
import time

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError


pytestmark = pytest.mark.integration

REGION = os.getenv("AWS_REGION", "us-east-1")
STACK_NAME = os.getenv("STACK_NAME", "StandbyRecoveryStack")


def client(service_name):
    return boto3.client(service_name, region_name=REGION)


@pytest.fixture(scope="session")
def stack():
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        pytest.skip("AWS credentials are required for integration tests")

    cloudformation = client("cloudformation")
    try:
        stack = cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
        if stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
            pytest.skip(f"Stack {STACK_NAME} is not ready: {stack['StackStatus']}")
        return stack
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"ValidationError", "ResourceNotFoundException"}:
            pytest.skip(f"Stack {STACK_NAME} is not deployed")
        raise
    except BotoCoreError as exc:
        pytest.skip(f"Unable to reach CloudFormation: {exc}")


@pytest.fixture(scope="session")
def stack_resources(stack):
    cloudformation = client("cloudformation")
    paginator = cloudformation.get_paginator("list_stack_resources")
    resources = []
    for page in paginator.paginate(StackName=stack["StackName"]):
        resources.extend(page["StackResourceSummaries"])
    assert resources
    return resources


def resources_by_type(stack_resources, resource_type):
    return [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type
    ]


def one_resource(stack_resources, resource_type, logical_prefix):
    matches = [
        resource
        for resource in resources_by_type(stack_resources, resource_type)
        if resource["LogicalResourceId"].startswith(logical_prefix)
    ]
    assert len(matches) == 1, f"Expected one {logical_prefix} {resource_type}, found {len(matches)}"
    return matches[0]


def physical_id(stack_resources, resource_type, logical_prefix):
    return one_resource(stack_resources, resource_type, logical_prefix)["PhysicalResourceId"]


def invoke_api_method(stack_resources, path, method, payload=None):
    api_gateway = client("apigateway")
    rest_api_id = physical_id(stack_resources, "AWS::ApiGateway::RestApi", "StandbyApi")
    resources = api_gateway.get_resources(restApiId=rest_api_id)["items"]
    resource = next(resource for resource in resources if resource.get("path") == path)
    request = {
        "restApiId": rest_api_id,
        "resourceId": resource["id"],
        "httpMethod": method,
        "headers": {"content-type": "application/json"},
    }
    if payload is not None:
        request["body"] = json.dumps(payload)
    response = api_gateway.test_invoke_method(**request)
    body = response.get("body")
    return response["status"], json.loads(body) if body else None


def drain_queue(sqs, queue_url):
    deadline = time.time() + 5
    while time.time() < deadline:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        sqs.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
                for index, message in enumerate(messages)
            ],
        )


def collect_messages(sqs, queue_url, predicate, timeout_seconds=20):
    matches = []
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        messages = response.get("Messages", [])
        if messages:
            sqs.delete_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
                    for index, message in enumerate(messages)
                ],
            )
        for message in messages:
            try:
                body = json.loads(message["Body"])
            except json.JSONDecodeError:
                body = message["Body"]
            if predicate(body):
                matches.append(body)
        if matches:
            return matches
    return matches


def collect_queue_bodies(sqs, queue_url, timeout_seconds=20):
    bodies = []
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        messages = response.get("Messages", [])
        if messages:
            sqs.delete_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
                    for index, message in enumerate(messages)
                ],
            )
        for message in messages:
            try:
                bodies.append(json.loads(message["Body"]))
            except json.JSONDecodeError:
                bodies.append(message["Body"])
        if len(bodies) >= 2:
            return bodies
    return bodies


def test_stack_contains_required_resource_families(stack_resources):
    expected_counts = {
        "AWS::ApiGateway::RestApi": 1,
        "AWS::Athena::WorkGroup": 1,
        "AWS::EC2::NatGateway": 1,
        "AWS::EC2::Subnet": 4,
        "AWS::EC2::VPC": 1,
        "AWS::EC2::VPCEndpoint": 2,
        "AWS::Events::EventBus": 1,
        "AWS::Events::Rule": 1,
        "AWS::Glue::Crawler": 1,
        "AWS::Glue::Database": 1,
        "AWS::Lambda::Function": 3,
        "AWS::Pipes::Pipe": 1,
        "AWS::RDS::DBInstance": 1,
        "AWS::S3::Bucket": 1,
        "AWS::SecretsManager::Secret": 1,
        "AWS::SQS::Queue": 3,
        "AWS::StepFunctions::StateMachine": 1,
    }

    for resource_type, expected in expected_counts.items():
        assert len(resources_by_type(stack_resources, resource_type)) == expected


def test_sqs_queues_are_live_and_dlq_is_configured(stack_resources):
    sqs = client("sqs")
    order_queue_url = physical_id(stack_resources, "AWS::SQS::Queue", "OrderQueue")
    dlq_url = physical_id(stack_resources, "AWS::SQS::Queue", "OrderDeadLetterQueue")
    pipe_queue_url = physical_id(stack_resources, "AWS::SQS::Queue", "PipeSourceQueue")

    for queue_url in [order_queue_url, dlq_url, pipe_queue_url]:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn", "RedrivePolicy"],
        )["Attributes"]
        assert attrs["QueueArn"]

    order_attrs = sqs.get_queue_attributes(
        QueueUrl=order_queue_url,
        AttributeNames=["RedrivePolicy"],
    )["Attributes"]
    redrive = json.loads(order_attrs["RedrivePolicy"])
    assert redrive["maxReceiveCount"] == 3


def test_api_gateway_routes_and_lambdas_are_deployed(stack_resources):
    api_gateway = client("apigateway")
    lambda_client = client("lambda")

    rest_api_id = physical_id(stack_resources, "AWS::ApiGateway::RestApi", "StandbyApi")
    resources = api_gateway.get_resources(restApiId=rest_api_id)["items"]
    routes = {
        (resource.get("path"), method)
        for resource in resources
        for method in resource.get("resourceMethods", {})
    }
    assert ("/health", "GET") in routes
    assert ("/orders", "POST") in routes

    stages = api_gateway.get_stages(restApiId=rest_api_id)["item"]
    assert any(stage["stageName"] == "standby" for stage in stages)

    for function in resources_by_type(stack_resources, "AWS::Lambda::Function"):
        response = lambda_client.get_function(FunctionName=function["PhysicalResourceId"])
        config = response["Configuration"]
        assert config["Runtime"] == "nodejs20.x"
        assert config["VpcConfig"]["SubnetIds"]


def test_health_endpoint_returns_json(stack_resources):
    status, body = invoke_api_method(stack_resources, "/health", "GET")
    assert status == 200
    assert body == {"status": "healthy"}


def test_post_orders_reaches_queues_and_event_bus(stack_resources):
    sqs = client("sqs")
    order_queue_url = physical_id(stack_resources, "AWS::SQS::Queue", "OrderQueue")
    pipe_queue_url = physical_id(stack_resources, "AWS::SQS::Queue", "PipeSourceQueue")

    drain_queue(sqs, order_queue_url)
    drain_queue(sqs, pipe_queue_url)

    payload = {"orderId": "qa-flow-001", "amount": 42}
    status, body = invoke_api_method(stack_resources, "/orders", "POST", payload=payload)
    assert status == 202
    assert body == {"accepted": True}

    order_messages = collect_queue_bodies(sqs, order_queue_url)
    assert any(isinstance(body, dict) and body.get("body") == payload for body in order_messages)

    pipe_messages = collect_messages(
        sqs,
        pipe_queue_url,
        lambda body: isinstance(body, dict) and body.get("body") == payload,
    )
    assert pipe_messages

    assert any(isinstance(body, dict) and body.get("source") == "orders.api" for body in order_messages)


def test_events_and_recovery_workflow_are_live(stack_resources):
    events = client("events")
    stepfunctions = client("stepfunctions")

    event_bus_name = physical_id(stack_resources, "AWS::Events::EventBus", "OrderEventBus")
    rule_physical_id = physical_id(stack_resources, "AWS::Events::Rule", "OrderReceivedRule")
    rule_name = rule_physical_id.split("|")[-1].split("/")[-1]
    state_machine_arn = physical_id(
        stack_resources,
        "AWS::StepFunctions::StateMachine",
        "FailureRecoveryStateMachine",
    )

    event_bus = events.describe_event_bus(Name=event_bus_name)
    assert event_bus["Name"]

    targets = events.list_targets_by_rule(Rule=rule_name, EventBusName=event_bus_name)["Targets"]
    assert len(targets) == 1
    assert "sqs" in targets[0]["Arn"]

    execution = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({"source": "integration-test"}),
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        result = stepfunctions.describe_execution(executionArn=execution["executionArn"])
        if result["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}:
            assert result["status"] == "SUCCEEDED"
            return
        time.sleep(1)
    pytest.fail("State machine execution did not finish")


def test_storage_and_snapshot_resources_are_declared(stack_resources):
    s3 = client("s3")
    secrets = client("secretsmanager")

    bucket_name = physical_id(stack_resources, "AWS::S3::Bucket", "StandbyLogsBucket")
    secret_id = physical_id(stack_resources, "AWS::SecretsManager::Secret", "DatabaseSecret")

    secret = secrets.describe_secret(SecretId=secret_id)
    assert secret["ARN"]

    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
    assert versioning["Status"] == "Enabled"

    public_access = s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"]
    assert all(public_access.values())

    assert physical_id(stack_resources, "AWS::RDS::DBInstance", "PostgresDatabase")
    assert physical_id(stack_resources, "AWS::Glue::Database", "StandbyGlueDatabase")
    assert physical_id(stack_resources, "AWS::Glue::Crawler", "StandbyLogsCrawler")
    assert physical_id(stack_resources, "AWS::Athena::WorkGroup", "StandbyAthenaWorkgroup")
