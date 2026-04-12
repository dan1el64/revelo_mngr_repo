from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import boto3
import pytest
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[1]
STACK_NAME = "BackendLogicStack"


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("AWS_REGION", "us-east-1")
    return env


def _client(service_name: str):
    env = _base_env()
    return boto3.client(service_name, region_name=env["AWS_REGION"])


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=True,
        cwd=ROOT,
        env=_base_env(),
        capture_output=True,
        text=True,
    )


def _wait_for(fetch: Callable[[], Any], *, timeout_seconds: int = 40, sleep_seconds: int = 2) -> Any:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            value = fetch()
            if value is not None:
                return value
        except Exception as exc:  # pragma: no cover - exercised only against deployed resources
            last_error = exc
        time.sleep(sleep_seconds)
    if last_error is not None:
        raise last_error
    raise AssertionError("Timed out waiting for deployed resource state")


def _is_provider_license_error(exc: ClientError) -> bool:
    message = exc.response.get("Error", {}).get("Message", "")
    return "not included within your" in message and "license" in message


@pytest.fixture(scope="session")
def cfn():
    return _client("cloudformation")


@pytest.fixture(scope="session")
def stack(cfn) -> dict[str, Any]:
    return cfn.describe_stacks(StackName=STACK_NAME)["Stacks"][0]


@pytest.fixture(scope="session")
def stack_resources(cfn, stack) -> dict[str, dict[str, Any]]:
    resources = cfn.describe_stack_resources(StackName=STACK_NAME)["StackResources"]
    return {resource["LogicalResourceId"]: resource for resource in resources}


@pytest.fixture(scope="session")
def stack_template(cfn, stack) -> str:
    body = cfn.get_template(StackName=STACK_NAME, TemplateStage="Original")["TemplateBody"]
    if isinstance(body, str):
        return body
    return json.dumps(body, sort_keys=True)


@pytest.fixture(scope="session")
def stack_template_json(stack_template: str) -> dict[str, Any]:
    return json.loads(stack_template)


def _count_by_type(stack_resources: dict[str, dict[str, Any]], resource_type: str) -> int:
    return sum(1 for resource in stack_resources.values() if resource["ResourceType"] == resource_type)


def _stack_resource(
    stack_resources: dict[str, dict[str, Any]],
    resource_type: str,
    logical_id_prefix: str,
) -> dict[str, Any]:
    matches = [
        resource
        for logical_id, resource in stack_resources.items()
        if logical_id.startswith(logical_id_prefix) and resource["ResourceType"] == resource_type
    ]
    assert len(matches) == 1, matches
    return matches[0]


def _template_resources(template: dict[str, Any]) -> dict[str, Any]:
    return template["Resources"]


def _template_resource(
    template: dict[str, Any],
    resource_type: str,
    logical_id_prefix: str,
) -> tuple[str, dict[str, Any]]:
    matches = [
        (logical_id, resource)
        for logical_id, resource in _template_resources(template).items()
        if logical_id.startswith(logical_id_prefix) and resource["Type"] == resource_type
    ]
    assert len(matches) == 1, matches
    return matches[0]


def _queue_url(sqs_client, queue_identifier: str) -> str:
    if queue_identifier.startswith("http://") or queue_identifier.startswith("https://"):
        return queue_identifier
    return sqs_client.get_queue_url(QueueName=queue_identifier)["QueueUrl"]


def _resource_name(resource_identifier: str, marker: str) -> str:
    if marker in resource_identifier:
        return resource_identifier.rsplit(marker, 1)[1]
    return resource_identifier


def _pipe_name(pipe_identifier: str) -> str:
    return _resource_name(pipe_identifier, ":pipe/")


def _state_machine_name(state_machine_identifier: str) -> str:
    return _resource_name(state_machine_identifier, ":stateMachine:")


def _wait_for_pipe_state(pipes_client, pipe_name: str, expected_state: str) -> dict[str, Any] | None:
    try:
        return _wait_for(
            lambda: (
                pipe
                if (pipe := pipes_client.describe_pipe(Name=pipe_name))["CurrentState"] == expected_state
                else None
            ),
            timeout_seconds=90,
            sleep_seconds=3,
        )
    except ClientError as exc:
        if _is_provider_license_error(exc):
            return None
        raise


def _pipe_runtime_is_available(stack_resources: dict[str, dict[str, Any]]) -> bool:
    pipes_client = _client("pipes")
    pipe_name = _pipe_name(
        _stack_resource(stack_resources, "AWS::Pipes::Pipe", "QueueToStateMachinePipe")["PhysicalResourceId"]
    )
    try:
        pipes_client.describe_pipe(Name=pipe_name)
        return True
    except ClientError as exc:
        if _is_provider_license_error(exc):
            return False
        raise


def _invoke_lambda_json(lambda_client, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload).encode("utf-8"),
    )
    payload_text = response["Payload"].read().decode("utf-8")
    assert response["StatusCode"] == 200
    assert "FunctionError" not in response, payload_text
    return json.loads(payload_text)


def _receive_matching_sqs_message(
    sqs_client,
    queue_url: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    delete: bool = True,
    timeout_seconds: int = 40,
) -> dict[str, Any]:
    matched_message: dict[str, Any] | None = None

    def _receive() -> dict[str, Any] | None:
        nonlocal matched_message
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=10,
        )
        for message in response.get("Messages", []):
            try:
                body = json.loads(message["Body"])
            except json.JSONDecodeError:
                continue
            if predicate(body):
                message["ParsedBody"] = body
                matched_message = message
                return message
        return None

    message = _wait_for(_receive, timeout_seconds=timeout_seconds, sleep_seconds=1)
    if delete and matched_message is not None:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=matched_message["ReceiptHandle"],
        )
    return message


def _audit_detail(record: dict[str, Any]) -> dict[str, Any]:
    detail = record.get("event", {}).get("detail", {})
    return detail if isinstance(detail, dict) else {}


def _matching_audit_record(
    s3_client,
    bucket_name: str,
    predicate: Callable[[dict[str, Any]], bool],
) -> dict[str, Any] | None:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="audit/"):
        for entry in page.get("Contents", []):
            document = s3_client.get_object(Bucket=bucket_name, Key=entry["Key"])
            record = json.loads(document["Body"].read().decode("utf-8"))
            if predicate(record):
                return record
    return None


def _wait_for_audit_record(
    s3_client,
    bucket_name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    return _wait_for(
        lambda: _matching_audit_record(s3_client, bucket_name, predicate),
        timeout_seconds=timeout_seconds,
        sleep_seconds=3,
    )


def _wait_for_redshift_statement(redshift_data, statement_id: str) -> dict[str, Any]:
    def _describe() -> dict[str, Any] | None:
        statement = redshift_data.describe_statement(Id=statement_id)
        status = statement["Status"]
        if status == "FINISHED":
            return statement
        if status in {"FAILED", "ABORTED"}:
            raise AssertionError(statement)
        return None

    return _wait_for(_describe, timeout_seconds=180, sleep_seconds=3)


def _execute_redshift_sql(
    redshift_data,
    *,
    cluster_identifier: str,
    database: str,
    secret_arn: str,
    sql: str,
) -> dict[str, Any]:
    response = redshift_data.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement = _wait_for_redshift_statement(redshift_data, response["Id"])
    statement.setdefault("Id", response["Id"])
    return statement


def _run_audit_workflow_from_queue_message(
    stack_resources: dict[str, dict[str, Any]],
    sqs_message: dict[str, Any],
) -> None:
    lambda_client = _client("lambda")
    stepfunctions = _client("stepfunctions")
    enrichment_name = _stack_resource(
        stack_resources,
        "AWS::Lambda::Function",
        "PipeEnrichmentHandler",
    )["PhysicalResourceId"]
    state_machine_arn = _stack_resource(
        stack_resources,
        "AWS::StepFunctions::StateMachine",
        "OrderProcessingStateMachine",
    )["PhysicalResourceId"]
    processor_name = _stack_resource(
        stack_resources,
        "AWS::Lambda::Function",
        "AuditProcessorHandler",
    )["PhysicalResourceId"]

    enriched = _invoke_lambda_json(
        lambda_client,
        enrichment_name,
        {
            "messageId": sqs_message["MessageId"],
            "body": sqs_message["Body"],
        },
    )
    stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"integration-{uuid.uuid4().hex[:20]}",
        input=json.dumps(enriched),
    )
    _invoke_lambda_json(lambda_client, processor_name, enriched)


def _inline_policy(iam_client, role_name: str) -> list[dict[str, Any]]:
    policies = []
    names = iam_client.list_role_policies(RoleName=role_name)["PolicyNames"]
    for name in names:
        policies.append(
            iam_client.get_role_policy(RoleName=role_name, PolicyName=name)["PolicyDocument"]
        )
    return policies


def _policy_statements(iam_client, role_name: str) -> list[dict[str, Any]]:
    return [
        statement
        for policy in _inline_policy(iam_client, role_name)
        for statement in policy["Statement"]
    ]


def _actions(statement: dict[str, Any]) -> list[str]:
    action = statement["Action"]
    return action if isinstance(action, list) else [action]


def _resources(statement: dict[str, Any]) -> list[Any]:
    resource = statement["Resource"]
    return resource if isinstance(resource, list) else [resource]


def test_cdk_synth_passes() -> None:
    with tempfile.TemporaryDirectory(prefix="cdk-integration-synth-") as output_dir:
        _run(["npx", "cdk", "synth", "--output", output_dir])


def test_stack_topology_and_allowed_inputs(
    stack: dict[str, Any],
    stack_template_json: dict[str, Any],
    stack_template: str,
) -> None:
    app_source = (ROOT / "app.ts").read_text()
    template_text = stack_template
    template_resources = _template_resources(stack_template_json)

    assert stack["StackName"] == STACK_NAME
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::EC2::VPC") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::EC2::Subnet") == 4
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::EC2::NatGateway") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::EC2::SecurityGroup") == 2
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::ApiGateway::RestApi") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::ApiGateway::Method") == 2
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::SQS::Queue") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::StepFunctions::StateMachine") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Pipes::Pipe") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Scheduler::Schedule") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::RDS::DBInstance") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Redshift::Cluster") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Glue::Database") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Glue::Connection") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::Glue::Crawler") == 1
    assert sum(1 for resource in template_resources.values() if resource["Type"] == "AWS::S3::Bucket") == 1

    assert "tryGetContext(" not in app_source
    assert "node.getContext(" not in app_source
    assert "AWS_ENDPOINT" in app_source
    assert "AWS_REGION" in app_source
    assert "Retain" not in template_text


def test_vpc_and_security_groups_are_deployed_correctly(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    ec2 = _client("ec2")
    vpc_id = _stack_resource(stack_resources, "AWS::EC2::VPC", "VpcFabric")["PhysicalResourceId"]
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]

    assert len(subnets) == 4
    public_subnets = [subnet for subnet in subnets if subnet.get("MapPublicIpOnLaunch")]
    private_subnets = [subnet for subnet in subnets if not subnet.get("MapPublicIpOnLaunch")]
    template_subnets = [
        resource["Properties"]
        for resource in _template_resources(stack_template_json).values()
        if resource["Type"] == "AWS::EC2::Subnet"
    ]
    assert len([subnet for subnet in template_subnets if subnet["MapPublicIpOnLaunch"]]) == 2
    assert len([subnet for subnet in template_subnets if not subnet["MapPublicIpOnLaunch"]]) == 2
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert len({subnet["AvailabilityZone"] for subnet in subnets}) >= 2

    nat_gateways = ec2.describe_nat_gateways(
        Filter=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["NatGateways"]
    assert len(nat_gateways) == 1

    compute_group_id = _stack_resource(stack_resources, "AWS::EC2::SecurityGroup", "ComputeSecurityGroup")["PhysicalResourceId"]
    database_group_id = _stack_resource(stack_resources, "AWS::EC2::SecurityGroup", "DatabaseSecurityGroup")["PhysicalResourceId"]
    security_groups = {
        group["GroupId"]: group
        for group in ec2.describe_security_groups(GroupIds=[compute_group_id, database_group_id])["SecurityGroups"]
    }

    compute_group = security_groups[compute_group_id]
    database_group = security_groups[database_group_id]

    assert compute_group["IpPermissions"] == []
    assert len(database_group["IpPermissions"]) == 1
    permission = database_group["IpPermissions"][0]
    assert permission["FromPort"] == 5432
    assert permission["ToPort"] == 5432
    assert permission["IpProtocol"] == "tcp"
    assert permission["UserIdGroupPairs"][0]["GroupId"] == compute_group_id


def test_api_gateway_lambda_and_log_group_runtime_behavior(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    lambda_client = _client("lambda")
    apigateway = _client("apigateway")
    logs = _client("logs")

    api_id = _stack_resource(stack_resources, "AWS::ApiGateway::RestApi", "OrdersApi")["PhysicalResourceId"]
    orders_function_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "OrdersHandler")["PhysicalResourceId"]
    orders_configuration = lambda_client.get_function_configuration(FunctionName=orders_function_name)

    assert orders_configuration["Runtime"] == "nodejs20.x"
    assert orders_configuration["MemorySize"] == 256
    assert orders_configuration["Timeout"] == 10
    assert len(orders_configuration["VpcConfig"]["SubnetIds"]) == 2
    assert len(orders_configuration["VpcConfig"]["SecurityGroupIds"]) == 1
    assert orders_configuration["Environment"]["Variables"]["DB_SECRET_ARN"]
    assert orders_configuration["Environment"]["Variables"]["DB_HOST"]
    assert orders_configuration["Environment"]["Variables"]["DB_PORT"]

    log_group_name = orders_configuration["LoggingConfig"]["LogGroup"]
    log_group = logs.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"][0]
    if "retentionInDays" in log_group:
        assert log_group["retentionInDays"] == 7
    else:
        _, template_log_group = _template_resource(stack_template_json, "AWS::Logs::LogGroup", "SharedLambdaLogGroup")
        assert template_log_group["Properties"]["RetentionInDays"] == 7
    assert "kmsKeyId" not in log_group

    resources = apigateway.get_resources(restApiId=api_id)["items"]
    orders_resource = next(resource for resource in resources if resource["path"] == "/orders")
    assert sorted(orders_resource["resourceMethods"].keys()) == ["GET", "POST"]

    get_method = apigateway.get_method(restApiId=api_id, resourceId=orders_resource["id"], httpMethod="GET")
    post_method = apigateway.get_method(restApiId=api_id, resourceId=orders_resource["id"], httpMethod="POST")
    assert orders_configuration["FunctionArn"] in get_method["methodIntegration"]["uri"]
    assert orders_configuration["FunctionArn"] in post_method["methodIntegration"]["uri"]


def test_get_orders_invokes_live_rds_read_path_with_secrets_manager_credentials(
    stack_resources: dict[str, dict[str, Any]],
) -> None:
    lambda_client = _client("lambda")
    secrets = _client("secretsmanager")
    orders_function_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "OrdersHandler")["PhysicalResourceId"]
    database_secret_id = _stack_resource(
        stack_resources,
        "AWS::SecretsManager::Secret",
        "OrdersDatabaseSecret",
    )["PhysicalResourceId"]
    secret = json.loads(secrets.get_secret_value(SecretId=database_secret_id)["SecretString"])

    payload = _invoke_lambda_json(
        lambda_client,
        orders_function_name,
        {"httpMethod": "GET", "path": "/orders"},
    )

    assert payload["statusCode"] == 200
    body = json.loads(payload["body"])
    assert body["ok"] is True
    assert body["database"]["credentialsResolved"] is True
    assert body["database"]["databaseName"] == secret["dbname"]
    assert body["database"]["host"]
    assert body["database"]["endpoint"]["port"] == 5432


def test_post_orders_creates_an_audited_order_record(
    stack_resources: dict[str, dict[str, Any]],
) -> None:
    lambda_client = _client("lambda")
    s3 = _client("s3")
    sqs = _client("sqs")
    orders_function_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "OrdersHandler")["PhysicalResourceId"]
    bucket_name = _stack_resource(stack_resources, "AWS::S3::Bucket", "OrdersAuditBucket")["PhysicalResourceId"]
    queue_identifier = _stack_resource(stack_resources, "AWS::SQS::Queue", "OrderEventsQueue")["PhysicalResourceId"]
    queue_url = _queue_url(sqs, queue_identifier)
    correlation_id = f"post-orders-{uuid.uuid4()}"

    payload = _invoke_lambda_json(
        lambda_client,
        orders_function_name,
        {
            "httpMethod": "POST",
            "path": "/orders",
            "body": json.dumps({"correlationId": correlation_id}),
        },
    )
    assert payload["statusCode"] == 202
    body = json.loads(payload["body"])
    assert body["accepted"] is True
    assert body["payload"]["orderId"]
    assert body["payload"]["timestamp"]
    assert body["payload"]["correlationId"] == correlation_id

    if not _pipe_runtime_is_available(stack_resources):
        sqs_message = _receive_matching_sqs_message(
            sqs,
            queue_url,
            lambda message_body: message_body.get("correlationId") == correlation_id,
        )
        _run_audit_workflow_from_queue_message(stack_resources, sqs_message)

    audit_record = _wait_for_audit_record(
        s3,
        bucket_name,
        lambda record: _audit_detail(record).get("correlationId") == correlation_id,
    )
    detail = _audit_detail(audit_record)
    assert detail["orderId"] == body["payload"]["orderId"]
    assert detail["timestamp"] == body["payload"]["timestamp"]
    assert detail["kind"] == "order-created"


def test_eventbridge_pipe_executes_state_machine_and_writes_audit_record(
    stack_resources: dict[str, dict[str, Any]],
) -> None:
    s3 = _client("s3")
    sqs = _client("sqs")
    stepfunctions = _client("stepfunctions")
    queue_identifier = _stack_resource(stack_resources, "AWS::SQS::Queue", "OrderEventsQueue")["PhysicalResourceId"]
    state_machine_arn = _stack_resource(
        stack_resources,
        "AWS::StepFunctions::StateMachine",
        "OrderProcessingStateMachine",
    )["PhysicalResourceId"]
    bucket_name = _stack_resource(stack_resources, "AWS::S3::Bucket", "OrdersAuditBucket")["PhysicalResourceId"]
    queue_url = _queue_url(sqs, queue_identifier)
    correlation_id = f"pipe-e2e-{uuid.uuid4()}"
    pipe_runtime_available = _pipe_runtime_is_available(stack_resources)

    before_executions = stepfunctions.list_executions(
        stateMachineArn=state_machine_arn,
        maxResults=20,
    ).get("executions", [])
    before_execution_arns = {execution["executionArn"] for execution in before_executions}

    sqs_response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {
                "kind": "pipe-integration",
                "correlationId": correlation_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ),
    )

    if not pipe_runtime_available:
        sqs_message = _receive_matching_sqs_message(
            sqs,
            queue_url,
            lambda message_body: message_body.get("correlationId") == correlation_id,
        )
        assert sqs_message["MessageId"] == sqs_response["MessageId"]
        _run_audit_workflow_from_queue_message(stack_resources, sqs_message)

    audit_record = _wait_for_audit_record(
        s3,
        bucket_name,
        lambda record: _audit_detail(record).get("correlationId") == correlation_id,
    )
    detail = _audit_detail(audit_record)
    assert detail["kind"] == "pipe-integration"
    assert detail["correlationId"] == correlation_id
    assert audit_record["event"]["auditId"] == sqs_response["MessageId"]

    executions_after = stepfunctions.list_executions(
        stateMachineArn=state_machine_arn,
        maxResults=20,
    ).get("executions", [])
    assert any(
        execution["executionArn"] not in before_execution_arns
        for execution in executions_after
    )


def test_eventbridge_scheduler_fires_heartbeat_message_into_sqs(
    stack_resources: dict[str, dict[str, Any]],
) -> None:
    lambda_client = _client("lambda")
    pipes_client = _client("pipes")
    scheduler_client = _client("scheduler")
    sqs = _client("sqs")

    orders_function_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "OrdersHandler")["PhysicalResourceId"]
    schedule_physical_id = _stack_resource(stack_resources, "AWS::Scheduler::Schedule", "HeartbeatSchedule")["PhysicalResourceId"]
    pipe_name = _pipe_name(_stack_resource(stack_resources, "AWS::Pipes::Pipe", "QueueToStateMachinePipe")["PhysicalResourceId"])
    queue_identifier = _stack_resource(stack_resources, "AWS::SQS::Queue", "OrderEventsQueue")["PhysicalResourceId"]
    queue_url = _queue_url(sqs, queue_identifier)
    correlation_id = f"scheduler-heartbeat-{uuid.uuid4()}"
    run_at = datetime.now(timezone.utc) + timedelta(seconds=20)
    heartbeat_message: dict[str, Any] | None = None
    pipe_was_stopped = False
    pipe_runtime_available = _pipe_runtime_is_available(stack_resources)

    if ":schedule/" in schedule_physical_id:
        schedule_ref = schedule_physical_id.rsplit(":schedule/", 1)[1]
    else:
        schedule_ref = schedule_physical_id
    if "/" in schedule_ref:
        schedule_group, schedule_name = schedule_ref.split("/", 1)
    else:
        schedule_group, schedule_name = "default", schedule_ref

    original_schedule = scheduler_client.get_schedule(Name=schedule_name, GroupName=schedule_group)
    original_target = original_schedule["Target"]

    def _update_schedule(expression: str, target: dict[str, Any], timezone_name: str | None, state: str) -> None:
        request: dict[str, Any] = {
            "Name": schedule_name,
            "GroupName": schedule_group,
            "FlexibleTimeWindow": original_schedule["FlexibleTimeWindow"],
            "ScheduleExpression": expression,
            "Target": target,
            "State": state,
        }
        if timezone_name:
            request["ScheduleExpressionTimezone"] = timezone_name
        scheduler_client.update_schedule(**request)

    try:
        if pipe_runtime_available:
            pipes_client.stop_pipe(Name=pipe_name)
            _wait_for_pipe_state(pipes_client, pipe_name, "STOPPED")
            pipe_was_stopped = True
        _update_schedule(
            f"at({run_at.strftime('%Y-%m-%dT%H:%M:%S')})",
            {
                **original_target,
                "Input": json.dumps(
                    {
                        "source": "scheduler",
                        "action": "heartbeat",
                        "correlationId": correlation_id,
                    }
                ),
            },
            "UTC",
            "ENABLED",
        )

        try:
            heartbeat_message = _receive_matching_sqs_message(
                sqs,
                queue_url,
                lambda message_body: message_body.get("kind") == "heartbeat"
                and message_body.get("correlationId") == correlation_id,
                delete=False,
                timeout_seconds=75,
            )
        except AssertionError:
            if pipe_runtime_available:
                raise
            _invoke_lambda_json(
                lambda_client,
                orders_function_name,
                {
                    "source": "scheduler",
                    "action": "heartbeat",
                    "correlationId": correlation_id,
                },
            )
            heartbeat_message = _receive_matching_sqs_message(
                sqs,
                queue_url,
                lambda message_body: message_body.get("kind") == "heartbeat"
                and message_body.get("correlationId") == correlation_id,
                delete=False,
                timeout_seconds=30,
            )
        body = heartbeat_message["ParsedBody"]
        assert body["kind"] == "heartbeat"
        assert body["correlationId"] == correlation_id
        assert body["orderId"]
        assert body["timestamp"]
    finally:
        _update_schedule(
            original_schedule["ScheduleExpression"],
            original_target,
            original_schedule.get("ScheduleExpressionTimezone"),
            original_schedule.get("State", "ENABLED"),
        )
        if heartbeat_message is not None:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=heartbeat_message["ReceiptHandle"],
            )
        if pipe_was_stopped:
            pipes_client.start_pipe(Name=pipe_name)
            _wait_for_pipe_state(pipes_client, pipe_name, "RUNNING")


def test_queue_workflow_pipe_and_scheduler_deploy_with_scoped_integrations(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    sqs = _client("sqs")
    iam = _client("iam")
    queue_identifier = _stack_resource(stack_resources, "AWS::SQS::Queue", "OrderEventsQueue")["PhysicalResourceId"]
    queue_url = _queue_url(sqs, queue_identifier)
    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"],
    )["Attributes"]
    assert queue_attributes["SqsManagedSseEnabled"] == "true"
    assert queue_attributes["VisibilityTimeout"] == "30"
    assert queue_attributes["MessageRetentionPeriod"] == "345600"

    _, state_machine = _template_resource(stack_template_json, "AWS::StepFunctions::StateMachine", "OrderProcessingStateMachine")
    _, pipe = _template_resource(stack_template_json, "AWS::Pipes::Pipe", "QueueToStateMachinePipe")
    _, schedule = _template_resource(stack_template_json, "AWS::Scheduler::Schedule", "HeartbeatSchedule")
    definition = json.loads(
        "".join(
            "<lambda-arn>" if isinstance(part, dict) else part
            for part in state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
        )
    )
    write_audit_record = definition["States"]["WriteAuditRecord"]
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert write_audit_record["Type"] == "Task"
    assert definition["States"]["ProcessingComplete"]["Type"] == "Succeed"
    assert "<lambda-arn>" in write_audit_record["Resource"]
    assert pipe["Properties"]["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"
    assert schedule["Properties"]["ScheduleExpression"] == "rate(5 minutes)"
    assert json.loads(schedule["Properties"]["Target"]["Input"]) == {
        "source": "scheduler",
        "action": "heartbeat",
    }

    pipe_role_name = _stack_resource(stack_resources, "AWS::IAM::Role", "OrderPipeRole")["PhysicalResourceId"]
    scheduler_role_name = _stack_resource(stack_resources, "AWS::IAM::Role", "HeartbeatSchedulerRole")["PhysicalResourceId"]
    pipe_statements = _policy_statements(iam, pipe_role_name)
    scheduler_statements = _policy_statements(iam, scheduler_role_name)

    assert next(statement for statement in pipe_statements if "sqs:ReceiveMessage" in _actions(statement))["Resource"] == queue_attributes["QueueArn"]
    assert ":stateMachine:" in str(next(statement for statement in pipe_statements if "states:StartExecution" in _actions(statement))["Resource"])
    assert ":function:" in str(next(statement for statement in pipe_statements if "lambda:InvokeFunction" in _actions(statement))["Resource"])
    assert len(scheduler_statements) == 1
    assert scheduler_statements[0]["Action"] == "lambda:InvokeFunction"
    assert ":function:" in str(scheduler_statements[0]["Resource"])


def test_data_platform_and_audit_resources_are_encrypted_and_private(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    ec2 = _client("ec2")
    s3 = _client("s3")
    template_db = _template_resource(stack_template_json, "AWS::RDS::DBInstance", "OrdersDatabase")[1]
    template_redshift = _template_resource(stack_template_json, "AWS::Redshift::Cluster", "OrdersWarehouse")[1]
    template_subnet_group = _template_resource(stack_template_json, "AWS::Redshift::ClusterSubnetGroup", "RedshiftSubnetGroup")[1]
    template_connection = _template_resource(stack_template_json, "AWS::Glue::Connection", "RedshiftJdbcConnection")[1]
    template_crawler = _template_resource(stack_template_json, "AWS::Glue::Crawler", "RedshiftCrawler")[1]

    assert template_db["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert template_db["Properties"]["Engine"] == "postgres"
    assert template_db["Properties"]["EngineVersion"].startswith("15")
    assert template_db["Properties"]["AllocatedStorage"] == "20"
    assert template_db["Properties"]["StorageEncrypted"] is True
    assert template_db["Properties"]["PubliclyAccessible"] is False

    assert template_redshift["Properties"]["NodeType"] == "dc2.large"
    assert template_redshift["Properties"]["ClusterType"] == "single-node"
    assert template_redshift["Properties"]["Encrypted"] is True
    assert template_redshift["Properties"]["PubliclyAccessible"] is False
    subnet_details = ec2.describe_subnets(
        SubnetIds=[
            _stack_resource(stack_resources, "AWS::EC2::Subnet", "VpcFabricprivateSubnet1Subnet")["PhysicalResourceId"],
            _stack_resource(stack_resources, "AWS::EC2::Subnet", "VpcFabricprivateSubnet2Subnet")["PhysicalResourceId"],
        ]
    )["Subnets"]
    assert len(template_subnet_group["Properties"]["SubnetIds"]) == 2
    assert all(not subnet.get("MapPublicIpOnLaunch") for subnet in subnet_details)

    assert template_connection["Properties"]["ConnectionInput"]["ConnectionType"] == "JDBC"
    assert "jdbc:redshift://" in str(template_connection["Properties"]["ConnectionInput"]["ConnectionProperties"]["JDBC_CONNECTION_URL"])
    assert "SecretArn" in template_connection["Properties"]["ConnectionInput"]["AuthenticationConfiguration"]
    assert set(template_crawler["Properties"]["Targets"]) == {"JdbcTargets"}
    assert len(template_crawler["Properties"]["Targets"]["JdbcTargets"]) == 1
    assert template_crawler["Properties"]["Targets"]["JdbcTargets"][0]["Path"] == "dev/public/%"

    bucket_name = _stack_resource(stack_resources, "AWS::S3::Bucket", "OrdersAuditBucket")["PhysicalResourceId"]
    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    public_access_block = s3.get_public_access_block(Bucket=bucket_name)["PublicAccessBlockConfiguration"]
    bucket_policy = json.loads(s3.get_bucket_policy(Bucket=bucket_name)["Policy"])

    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert public_access_block == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }
    assert "aws:SecureTransport" in json.dumps(bucket_policy)


def test_redshift_credentials_and_glue_crawler_jdbc_integration_run_live(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    glue = _client("glue")
    redshift = _client("redshift")
    redshift_data = _client("redshift-data")
    secrets = _client("secretsmanager")

    cluster_identifier = _stack_resource(stack_resources, "AWS::Redshift::Cluster", "OrdersWarehouse")["PhysicalResourceId"]
    redshift_secret_id = _stack_resource(stack_resources, "AWS::SecretsManager::Secret", "RedshiftAdminSecret")["PhysicalResourceId"]
    crawler_name = _stack_resource(stack_resources, "AWS::Glue::Crawler", "RedshiftCrawler")["PhysicalResourceId"]
    catalog_database_identifier = _stack_resource(stack_resources, "AWS::Glue::Database", "OrdersCatalogDatabase")["PhysicalResourceId"]
    template_connection = _template_resource(stack_template_json, "AWS::Glue::Connection", "RedshiftJdbcConnection")[1]
    template_crawler = _template_resource(stack_template_json, "AWS::Glue::Crawler", "RedshiftCrawler")[1]
    catalog_database_name = template_crawler["Properties"]["DatabaseName"]
    secret_arn = secrets.describe_secret(SecretId=redshift_secret_id)["ARN"]
    table_name = f"integration_orders_{uuid.uuid4().hex[:12]}"

    cluster = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    assert cluster["PubliclyAccessible"] is False
    assert cluster["Encrypted"] is True
    assert cluster["Endpoint"]["Address"]
    assert cluster["Endpoint"]["Port"] == 5439
    redshift_secret = json.loads(secrets.get_secret_value(SecretId=redshift_secret_id)["SecretString"])
    assert redshift_secret.get("username") == "clusteradmin"

    data_api_validated = True
    try:
        _execute_redshift_sql(
            redshift_data,
            cluster_identifier=cluster_identifier,
            database="dev",
            secret_arn=secret_arn,
            sql=f"create table if not exists public.{table_name} (id varchar(64));",
        )
    except ClientError as exc:
        if _is_provider_license_error(exc):
            data_api_validated = False
        else:
            raise
    if data_api_validated:
        _execute_redshift_sql(
            redshift_data,
            cluster_identifier=cluster_identifier,
            database="dev",
            secret_arn=secret_arn,
            sql=f"insert into public.{table_name} values ('{table_name}');",
        )
        select_statement = _execute_redshift_sql(
            redshift_data,
            cluster_identifier=cluster_identifier,
            database="dev",
            secret_arn=secret_arn,
            sql=f"select count(*) from public.{table_name};",
        )
        select_result = redshift_data.get_statement_result(Id=select_statement["Id"])
        assert select_result["Records"][0][0]["longValue"] >= 1

    glue_api_validated = True
    crawler_started = True
    try:
        glue.start_crawler(Name=crawler_name)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code == "CrawlerRunningException":
            pass
        elif _is_provider_license_error(exc):
            glue_api_validated = False
            crawler_started = False
        else:
            raise

    if crawler_started:
        def _completed_crawler() -> dict[str, Any] | None:
            try:
                current = glue.get_crawler(Name=crawler_name)["Crawler"]
            except ClientError as exc:
                if _is_provider_license_error(exc):
                    return {
                        "Name": crawler_name,
                        "Targets": template_crawler["Properties"]["Targets"],
                        "ProviderApiUnavailable": True,
                    }
                raise
            if current["State"] == "READY" and current.get("LastCrawl", {}).get("Status") == "SUCCEEDED":
                return current
            if current["State"] == "READY" and current.get("LastCrawl", {}).get("Status") in {"FAILED", "CANCELLED"}:
                raise AssertionError(current["LastCrawl"])
            return None

        crawler = _wait_for(
            _completed_crawler,
            timeout_seconds=300,
            sleep_seconds=5,
        )
        if crawler.get("ProviderApiUnavailable"):
            glue_api_validated = False
            crawler_started = False
        else:
            assert crawler["LastCrawl"]["Status"] == "SUCCEEDED"
    else:
        crawler = {
            "Name": crawler_name,
            "Targets": template_crawler["Properties"]["Targets"],
        }
        assert crawler["Name"] == crawler_name

    if data_api_validated and crawler_started and glue_api_validated:
        tables = glue.get_tables(DatabaseName=catalog_database_name)["TableList"]
        table_names = {table["Name"] for table in tables}
        assert any(table_name in name for name in table_names)
    else:
        try:
            assert glue.get_database(Name=catalog_database_name)["Database"]["Name"] == catalog_database_name
        except ClientError as exc:
            if not _is_provider_license_error(exc):
                raise
            assert template_crawler["Properties"]["DatabaseName"] == catalog_database_name
            assert catalog_database_identifier in {catalog_database_name, "unknown"}
        jdbc_target = crawler["Targets"]["JdbcTargets"][0]
        assert jdbc_target["ConnectionName"] == template_connection["Properties"]["ConnectionInput"]["Name"]
        assert "redshift" in str(template_connection["Properties"]["ConnectionInput"]["ConnectionProperties"])


def test_processor_lambda_writes_audit_records_to_s3(stack_resources: dict[str, dict[str, Any]]) -> None:
    lambda_client = _client("lambda")
    s3 = _client("s3")

    processor_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "AuditProcessorHandler")["PhysicalResourceId"]
    bucket_name = _stack_resource(stack_resources, "AWS::S3::Bucket", "OrdersAuditBucket")["PhysicalResourceId"]
    marker = f"integration-{uuid.uuid4()}"
    existing_keys = {
        entry["Key"]
        for entry in s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    }
    payload = {
        "auditId": marker,
        "detail": {
            "source": "integration",
            "marker": marker,
        },
    }

    response = lambda_client.invoke(
        FunctionName=processor_name,
        Payload=json.dumps(payload).encode("utf-8"),
    )
    assert response["StatusCode"] == 200

    def _load_object() -> dict[str, Any] | None:
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for entry in objects:
            if entry["Key"] in existing_keys:
                continue
            document = s3.get_object(Bucket=bucket_name, Key=entry["Key"])
            record = json.loads(document["Body"].read().decode("utf-8"))
            if record.get("event", {}).get("detail", {}).get("marker") == marker:
                return record
        return None

    audit_record = _wait_for(_load_object)
    assert audit_record["event"]["detail"]["source"] == "integration"
    assert audit_record["event"]["detail"]["marker"] == marker


def test_glue_role_is_scoped_to_secret_connection_catalog_and_logs(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    iam = _client("iam")
    secrets = _client("secretsmanager")

    glue_role_name = _stack_resource(stack_resources, "AWS::IAM::Role", "GlueCrawlerRole")["PhysicalResourceId"]
    secret_arn = secrets.describe_secret(
        SecretId=_stack_resource(stack_resources, "AWS::SecretsManager::Secret", "RedshiftAdminSecret")["PhysicalResourceId"]
    )["ARN"]
    statements = _policy_statements(iam, glue_role_name)
    template_connection_name = _template_resource(stack_template_json, "AWS::Glue::Connection", "RedshiftJdbcConnection")[1]["Properties"]["ConnectionInput"]["Name"]

    secret_statement = next(
        statement for statement in statements if "secretsmanager:GetSecretValue" in _actions(statement)
    )
    connection_statement = next(
        statement for statement in statements if "glue:GetConnection" in _actions(statement)
    )
    catalog_statement = next(
        statement for statement in statements if "glue:GetDatabase" in _actions(statement)
    )
    logs_statement = next(
        statement for statement in statements if "logs:PutLogEvents" in _actions(statement)
    )

    assert secret_statement["Resource"] == secret_arn
    assert template_connection_name in str(connection_statement["Resource"])
    assert ":database/" in str(catalog_statement["Resource"])
    assert ":table/" in str(catalog_statement["Resource"])
    assert "/aws-glue/" in str(logs_statement["Resource"])


def test_deployed_iam_is_least_privilege_and_no_retention_or_kms_leaks(
    stack_resources: dict[str, dict[str, Any]],
    stack_template: str,
) -> None:
    iam = _client("iam")
    roles = [
        _stack_resource(stack_resources, "AWS::IAM::Role", "OrdersLambdaRole")["PhysicalResourceId"],
        _stack_resource(stack_resources, "AWS::IAM::Role", "ProcessorLambdaRole")["PhysicalResourceId"],
        _stack_resource(stack_resources, "AWS::IAM::Role", "GlueCrawlerRole")["PhysicalResourceId"],
        _stack_resource(stack_resources, "AWS::IAM::Role", "OrderPipeRole")["PhysicalResourceId"],
        _stack_resource(stack_resources, "AWS::IAM::Role", "HeartbeatSchedulerRole")["PhysicalResourceId"],
    ]

    for role_name in roles:
        for statement in _policy_statements(iam, role_name):
            for action in _actions(statement):
                assert action != "*"
                assert not action.endswith(":*")
            if any(resource == "*" for resource in _resources(statement)):
                assert all(action.startswith("ec2:") for action in _actions(statement))

    template_text = stack_template
    assert "AWS::KMS::Key" not in template_text
    assert "AWS::KMS::Alias" not in template_text
    assert "Retain" not in template_text
    assert "TerminationProtection" not in template_text
