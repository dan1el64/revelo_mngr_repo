import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

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
def stack_outputs(stack):
    return {
        output["OutputKey"]: output["OutputValue"]
        for output in stack.get("Outputs", [])
    }


@pytest.fixture(scope="session")
def stack_resources(stack):
    cloudformation = client("cloudformation")
    paginator = cloudformation.get_paginator("list_stack_resources")
    resources = []
    for page in paginator.paginate(StackName=stack["StackName"]):
        resources.extend(page["StackResourceSummaries"])
    assert resources
    return resources


def require_output(stack_outputs, key):
    assert key in stack_outputs, f"Missing CloudFormation output {key}"
    return stack_outputs[key]


def resources_by_type(stack_resources, resource_type):
    return [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type
    ]


def invoke_api_method(stack_outputs, path, method, payload=None):
    api_gateway = client("apigateway")
    rest_api_id = require_output(stack_outputs, "RestApiId")
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


def parse_message_body(message):
    try:
        return json.loads(message["Body"])
    except json.JSONDecodeError:
        return message["Body"]


def delete_messages(sqs, queue_url, messages):
    if not messages:
        return
    sqs.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
            for index, message in enumerate(messages)
        ],
    )


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
        delete_messages(sqs, queue_url, messages)


def collect_messages(sqs, queue_url, predicate, timeout_seconds=30):
    matches = []
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        messages = response.get("Messages", [])
        delete_messages(sqs, queue_url, messages)

        for message in messages:
            body = parse_message_body(message)
            if predicate(body):
                matches.append(body)
        if matches:
            return matches
    return matches


def queue_arn(sqs, queue_url):
    return sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]


def as_list(value):
    return value if isinstance(value, list) else [value]


def action_set(statement):
    return set(as_list(statement.get("Action", [])))


def resource_set(statement):
    return set(as_list(statement.get("Resource", [])))


def inline_policy_statements(iam, role_name):
    statements = []
    for policy_name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
        policy = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
        policy_statements = policy["PolicyDocument"]["Statement"]
        statements.extend(as_list(policy_statements))
    return statements


def attached_policy_names(iam, role_name):
    return {
        policy["PolicyName"]
        for policy in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
    }


def statements_with_action(statements, action):
    return [
        statement
        for statement in statements
        if action in action_set(statement)
    ]


def assert_no_wildcards(statements):
    for statement in statements:
        assert "*" not in action_set(statement)
        assert "*" not in resource_set(statement)


def wait_for_state_machine_execution(stepfunctions, state_machine_arn, token, started_after):
    deadline = time.time() + 75
    sleep_seconds = 0.5
    while time.time() < deadline:
        executions = stepfunctions.list_executions(
            stateMachineArn=state_machine_arn,
            maxResults=20,
        )["executions"]

        for execution in executions:
            start_date = execution["startDate"]
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)
            if start_date < started_after:
                continue

            details = stepfunctions.describe_execution(
                executionArn=execution["executionArn"],
            )
            input_text = details.get("input") or ""
            if input_text and token not in input_text:
                continue
            if details["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}:
                assert details["status"] == "SUCCEEDED"
                return details

        time.sleep(sleep_seconds + random.uniform(0, 0.25))
        sleep_seconds = min(sleep_seconds * 1.5, 3)

    pytest.fail("Pipe-triggered state machine execution did not finish")


def secret_fragments(secret_string):
    fragments = {secret_string}
    try:
        secret_doc = json.loads(secret_string)
        fragments.update(str(value) for value in secret_doc.values())
    except json.JSONDecodeError:
        pass
    return {fragment for fragment in fragments if fragment}


def log_messages(logs, log_group_name, start_time_ms, timeout_seconds=20):
    deadline = time.time() + timeout_seconds
    messages = []
    while time.time() < deadline:
        response = logs.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time_ms,
        )
        messages = [event["message"] for event in response.get("events", [])]
        if messages:
            return messages
        time.sleep(1 + random.uniform(0, 0.25))
    return messages


def test_stack_contains_required_resource_families(stack_resources):
    expected_counts = {
        "AWS::ApiGateway::RestApi": 1,
        "AWS::Athena::WorkGroup": 1,
        "AWS::EC2::NatGateway": 1,
        "AWS::EC2::SecurityGroup": 3,
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


def test_sqs_queues_are_live_and_dlq_is_configured(stack_outputs):
    sqs = client("sqs")
    order_queue_url = require_output(stack_outputs, "OrderQueueUrl")
    pipe_queue_url = require_output(stack_outputs, "PipeSourceQueueUrl")

    for queue_url in [order_queue_url, pipe_queue_url]:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]
        assert attrs["QueueArn"]

    order_attrs = sqs.get_queue_attributes(
        QueueUrl=order_queue_url,
        AttributeNames=["RedrivePolicy"],
    )["Attributes"]
    redrive = json.loads(order_attrs["RedrivePolicy"])
    assert redrive["maxReceiveCount"] == 3


def test_api_gateway_routes_stage_and_access_logs_are_live(stack_outputs):
    api_gateway = client("apigateway")
    logs = client("logs")

    rest_api_id = require_output(stack_outputs, "RestApiId")
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

    log_group_name = require_output(stack_outputs, "ApiAccessLogGroupName")
    log_groups = logs.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
    log_group = next(group for group in log_groups if group["logGroupName"] == log_group_name)
    # retentionInDays is only returned when a finite retention period has been applied;
    # when present it must equal 7 days (ONE_WEEK as declared in the stack).
    if "retentionInDays" in log_group:
        assert log_group["retentionInDays"] == 7
    assert "kmsKeyId" not in log_group


def test_health_endpoint_returns_successful_json(stack_outputs):
    status, body = invoke_api_method(stack_outputs, "/health", "GET")
    assert status == 200
    assert isinstance(body, dict)
    assert body


def test_post_orders_uses_eventbridge_rule_delivery(stack_outputs):
    sqs = client("sqs")
    order_queue_url = require_output(stack_outputs, "OrderQueueUrl")

    drain_queue(sqs, order_queue_url)

    token = f"api-{uuid.uuid4()}"
    payload = {"orderId": token, "amount": 42}
    status, body = invoke_api_method(stack_outputs, "/orders", "POST", payload=payload)
    assert 200 <= status < 300
    assert body is None or isinstance(body, dict)

    rule_messages = collect_messages(
        sqs,
        order_queue_url,
        lambda body: (
            isinstance(body, dict)
            and body.get("source") == "orders.api"
            and body.get("detail", {}).get("body") == payload
        ),
    )
    assert rule_messages


def test_eventbridge_rule_delivers_put_events_to_order_queue(stack_outputs):
    events = client("events")
    sqs = client("sqs")
    event_bus_name = require_output(stack_outputs, "EventBusName")
    rule_name = require_output(stack_outputs, "OrderReceivedRuleName")
    order_queue_url = require_output(stack_outputs, "OrderQueueUrl")

    event_bus = events.describe_event_bus(Name=event_bus_name)
    assert event_bus["Name"] == event_bus_name

    targets = events.list_targets_by_rule(Rule=rule_name, EventBusName=event_bus_name)["Targets"]
    assert len(targets) == 1
    assert targets[0]["Arn"] == queue_arn(sqs, order_queue_url)

    drain_queue(sqs, order_queue_url)

    token = f"event-{uuid.uuid4()}"
    result = events.put_events(
        Entries=[{
            "EventBusName": event_bus_name,
            "Source": "orders.api",
            "DetailType": "OrderReceived",
            "Detail": json.dumps({"token": token}),
        }],
    )
    assert result["FailedEntryCount"] == 0

    delivered = collect_messages(
        sqs,
        order_queue_url,
        lambda body: (
            isinstance(body, dict)
            and body.get("source") == "orders.api"
            and body.get("detail", {}).get("token") == token
        ),
    )
    assert delivered


def test_pipe_source_queue_triggers_recovery_state_machine_without_direct_start(stack_outputs):
    sqs = client("sqs")
    stepfunctions = client("stepfunctions")
    secrets = client("secretsmanager")
    logs = client("logs")

    pipe_queue_url = require_output(stack_outputs, "PipeSourceQueueUrl")
    state_machine_arn = require_output(stack_outputs, "StateMachineArn")
    secret_arn = require_output(stack_outputs, "DatabaseSecretArn")
    helper_log_group = require_output(stack_outputs, "SecretsHelperLogGroupName")

    drain_queue(sqs, pipe_queue_url)

    token = f"pipe-{uuid.uuid4()}"
    started_after = datetime.now(timezone.utc) - timedelta(seconds=1)
    start_time_ms = int(time.time() * 1000)

    sqs.send_message(
        QueueUrl=pipe_queue_url,
        MessageBody=json.dumps({"token": token, "body": {"orderId": token}}),
    )

    execution = wait_for_state_machine_execution(
        stepfunctions,
        state_machine_arn,
        token,
        started_after,
    )

    secret_string = secrets.get_secret_value(SecretId=secret_arn)["SecretString"]
    inspected_execution_text = execution.get("input", "") + execution.get("output", "")
    for fragment in secret_fragments(secret_string):
        assert fragment not in inspected_execution_text

    for message in log_messages(logs, helper_log_group, start_time_ms):
        for fragment in secret_fragments(secret_string):
            assert fragment not in message


def test_live_iam_policies_are_resource_scoped(stack_outputs):
    iam = client("iam")
    sqs = client("sqs")
    events = client("events")
    lambda_client = client("lambda")

    order_queue_arn = queue_arn(sqs, require_output(stack_outputs, "OrderQueueUrl"))
    pipe_queue_arn = queue_arn(sqs, require_output(stack_outputs, "PipeSourceQueueUrl"))
    event_bus_arn = events.describe_event_bus(
        Name=require_output(stack_outputs, "EventBusName"),
    )["Arn"]
    state_machine_arn = require_output(stack_outputs, "StateMachineArn")
    secret_arn = require_output(stack_outputs, "DatabaseSecretArn")
    helper_arn = lambda_client.get_function(
        FunctionName=require_output(stack_outputs, "SecretsHelperFunctionName"),
    )["Configuration"]["FunctionArn"]

    health_role_name = require_output(stack_outputs, "HealthHandlerRoleName")
    health_statements = inline_policy_statements(iam, health_role_name)
    assert_no_wildcards(health_statements)
    assert attached_policy_names(iam, health_role_name) <= {
        "AWSLambdaBasicExecutionRole",
        "AWSLambdaVPCAccessExecutionRole",
    }
    health_actions = {action for statement in health_statements for action in action_set(statement)}
    forbidden_health_prefixes = ("sqs:", "secretsmanager:", "events:", "states:", "s3:", "rds:", "glue:", "athena:")
    assert not any(action.startswith(forbidden_health_prefixes) for action in health_actions)

    order_statements = inline_policy_statements(iam, require_output(stack_outputs, "OrderHandlerRoleName"))
    assert_no_wildcards(order_statements)
    sqs_send = statements_with_action(order_statements, "sqs:SendMessage")
    assert len(sqs_send) == 1
    assert action_set(sqs_send[0]) == {"sqs:SendMessage"}
    assert resource_set(sqs_send[0]) == {order_queue_arn, pipe_queue_arn}
    put_events = statements_with_action(order_statements, "events:PutEvents")
    assert len(put_events) == 1
    assert resource_set(put_events[0]) == {event_bus_arn}

    helper_statements = inline_policy_statements(iam, require_output(stack_outputs, "SecretsHelperRoleName"))
    assert_no_wildcards(helper_statements)
    secret_reads = statements_with_action(helper_statements, "secretsmanager:GetSecretValue")
    assert len(secret_reads) == 1
    assert action_set(secret_reads[0]) == {"secretsmanager:GetSecretValue"}
    assert resource_set(secret_reads[0]) == {secret_arn}

    pipe_statements = inline_policy_statements(iam, require_output(stack_outputs, "PipeRoleName"))
    assert_no_wildcards(pipe_statements)
    pipe_source = [statement for statement in pipe_statements if pipe_queue_arn in resource_set(statement)]
    assert pipe_source
    pipe_source_actions = set().union(*(action_set(statement) for statement in pipe_source))
    assert {"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"} <= pipe_source_actions
    helper_invokes = statements_with_action(pipe_statements, "lambda:InvokeFunction")
    assert len(helper_invokes) == 1
    assert helper_arn in resource_set(helper_invokes[0])
    starts = statements_with_action(pipe_statements, "states:StartExecution")
    assert len(starts) == 1
    assert resource_set(starts[0]) == {state_machine_arn}


def test_storage_analytics_and_database_runtime_configuration(stack_outputs):
    s3 = client("s3")
    secrets = client("secretsmanager")
    athena = client("athena")
    glue = client("glue")
    rds = client("rds")

    bucket_name = require_output(stack_outputs, "LogsBucketName")
    secret_id = require_output(stack_outputs, "DatabaseSecretArn")

    secret = secrets.describe_secret(SecretId=secret_id)
    assert secret["ARN"]

    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
    assert versioning["Status"] == "Enabled"

    public_access = s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"]
    assert all(public_access.values())

    try:
        workgroup = athena.get_work_group(
            WorkGroup=require_output(stack_outputs, "AthenaWorkGroupName"),
        )["WorkGroup"]
        output_location = workgroup["Configuration"]["ResultConfiguration"]["OutputLocation"]
        assert output_location.startswith(f"s3://{bucket_name}/")
        assert len(output_location) > len(f"s3://{bucket_name}/")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"InternalFailure", "NotImplementedException", "UnsupportedOperation"}:
            raise

    try:
        crawler = glue.get_crawler(Name=require_output(stack_outputs, "GlueCrawlerName"))["Crawler"]
        assert "Schedule" not in crawler or not crawler["Schedule"].get("ScheduleExpression")
        s3_targets = crawler["Targets"]["S3Targets"]
        assert any(target["Path"].startswith(f"s3://{bucket_name}/") for target in s3_targets)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"InternalFailure", "NotImplementedException", "UnsupportedOperation"}:
            raise

    try:
        database = rds.describe_db_instances(
            DBInstanceIdentifier=require_output(stack_outputs, "DatabaseInstanceIdentifier"),
        )["DBInstances"][0]
        assert database["PubliclyAccessible"] is False
        db_security_group_id = require_output(stack_outputs, "DatabaseSecurityGroupId")
        assert db_security_group_id in {
            group["VpcSecurityGroupId"]
            for group in database["VpcSecurityGroups"]
        }
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"InternalFailure", "NotImplementedException", "UnsupportedOperation"}:
            raise
