from __future__ import annotations

import contextlib
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from functools import lru_cache

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError


def _endpoint_url() -> str | None:
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT")


def _region() -> str:
    return os.environ.get("AWS_REGION", "us-east-1")


def _client(service_name: str):
    kwargs = {
        "region_name": _region(),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    }
    endpoint = _endpoint_url()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


@lru_cache(maxsize=1)
def _stack() -> dict:
    if not _endpoint_url() and not os.environ.get("AWS_ACCESS_KEY_ID"):
        pytest.skip("Integration environment unavailable: no AWS endpoint or credentials configured")

    try:
        cfn = _client("cloudformation")
        paginator = cfn.get_paginator("list_stacks")
        for page in paginator.paginate(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE"]):
            for summary in page.get("StackSummaries", []):
                stack_name = summary["StackName"]
                if stack_name == "OrdersIngestStack" or "OrdersIngestStack" in stack_name:
                    return cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    except (BotoCoreError, ClientError, EndpointConnectionError) as exc:
        pytest.skip(f"Integration environment unavailable: {exc}")
    pytest.skip("OrdersIngestStack is not deployed")


@lru_cache(maxsize=1)
def _stack_resources() -> list[dict]:
    cfn = _client("cloudformation")
    paginator = cfn.get_paginator("list_stack_resources")
    resources: list[dict] = []
    for page in paginator.paginate(StackName=_stack()["StackName"]):
        resources.extend(page["StackResourceSummaries"])
    return resources


def _physical_ids(resource_type: str) -> list[str]:
    return [
        resource["PhysicalResourceId"]
        for resource in _stack_resources()
        if resource["ResourceType"] == resource_type
    ]


def _physical_ids_with_prefix(resource_type: str, prefix: str) -> list[str]:
    return [
        resource["PhysicalResourceId"]
        for resource in _stack_resources()
        if resource["ResourceType"] == resource_type and resource["LogicalResourceId"].startswith(prefix)
    ]


def _physical_id_with_prefix(resource_type: str, prefix: str) -> str:
    matches = [
        resource["PhysicalResourceId"]
        for resource in _stack_resources()
        if resource["ResourceType"] == resource_type and resource["LogicalResourceId"].startswith(prefix)
    ]
    assert len(matches) == 1, f"Expected one {resource_type} with prefix {prefix}, got {matches}"
    return matches[0]


def _only_physical_id(resource_type: str) -> str:
    matches = _physical_ids(resource_type)
    assert len(matches) == 1, f"Expected one {resource_type}, got {matches}"
    return matches[0]


def _stack_outputs() -> dict[str, str]:
    return {item["OutputKey"]: item["OutputValue"] for item in _stack().get("Outputs", [])}


def _poll(fetch_value, *, description: str, timeout: int = 90, interval: int = 2):
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            value = fetch_value()
            if value is not None:
                return value
        except (AssertionError, BotoCoreError, ClientError, EndpointConnectionError, urllib.error.URLError) as exc:
            last_error = exc
        time.sleep(interval)

    if last_error is None:
        pytest.fail(f"Timed out waiting for {description}")
    pytest.fail(f"Timed out waiting for {description}: {last_error}")


def _json_compact(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _parse_json_payload(value: str | dict) -> str | dict:
    if not isinstance(value, str):
        return value
    with contextlib.suppress(json.JSONDecodeError):
        return json.loads(value)
    return value


def _post_json(url: str, body: str) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _post_json_expect_http_error(url: str, body: str) -> urllib.error.HTTPError:
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(request, timeout=30)
    return exc_info.value


def _wait_for_api_post(payload: dict) -> dict:
    api_url = _stack_outputs()["OrdersApiUrl"]
    raw_body = _json_compact(payload)

    def _attempt() -> dict | None:
        status, response_body = _post_json(f"{api_url}orders", raw_body)
        assert status == 202
        assert response_body["status"] == "accepted"
        return {
            "archive_key": response_body["archiveKey"],
            "request_body": raw_body,
        }

    return _poll(_attempt, description="Orders API POST", timeout=90, interval=3)


def _wait_for_bucket_object(bucket_name: str, key: str, expected_body: str) -> None:
    s3 = _client("s3")

    def _attempt() -> bool | None:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        body = response["Body"].read().decode("utf-8")
        assert body == expected_body
        return True

    _poll(_attempt, description=f"S3 object {key}", timeout=90, interval=3)


def _drain_queue(queue_url: str) -> list[str | dict]:
    sqs = _client("sqs")
    drained_messages: list[str | dict] = []
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return drained_messages
        for message in messages:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            drained_messages.append(_parse_json_payload(message["Body"]))


def _wait_for_queue_messages(queue_url: str, *, minimum_count: int) -> list[str | dict]:
    sqs = _client("sqs")
    collected_messages: list[str | dict] = []

    def _attempt() -> list[str | dict] | None:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        for message in response.get("Messages", []):
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            collected_messages.append(_parse_json_payload(message["Body"]))
        if len(collected_messages) >= minimum_count:
            return list(collected_messages)
        return None

    return _poll(
        _attempt,
        description=f"{minimum_count} messages on orders queue",
        timeout=90,
        interval=2,
    )


@contextlib.contextmanager
def _temporary_topic_subscription():
    sqs = _client("sqs")
    sns = _client("sns")
    topic_arn = _stack_outputs()["OrdersNotificationsTopicArn"]
    queue_name = f"orders-integration-{uuid.uuid4().hex}"

    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowOrdersTopic",
                            "Effect": "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action": "sqs:SendMessage",
                            "Resource": queue_arn,
                            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                        }
                    ],
                }
            )
        },
    )

    subscription_arn = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true"},
    )["SubscriptionArn"]

    try:
        yield queue_url
    finally:
        with contextlib.suppress(BotoCoreError, ClientError, EndpointConnectionError):
            if subscription_arn and subscription_arn != "pending confirmation":
                sns.unsubscribe(SubscriptionArn=subscription_arn)
        with contextlib.suppress(BotoCoreError, ClientError, EndpointConnectionError):
            sqs.delete_queue(QueueUrl=queue_url)


def _wait_for_raw_topic_message(queue_url: str, marker: str) -> dict:
    sqs = _client("sqs")

    def _attempt() -> dict | None:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        for message in response.get("Messages", []):
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            if marker in message["Body"]:
                return json.loads(message["Body"])
        return None

    return _poll(_attempt, description=f"SNS notification containing {marker}", timeout=120, interval=2)


def _pipe_name() -> str:
    outputs = _stack_outputs()
    return outputs.get("OrdersPipeName") or _physical_id_with_prefix("AWS::Pipes::Pipe", "OrdersPipe")


def _event_source_mapping_uuid() -> str:
    return _only_physical_id("AWS::Lambda::EventSourceMapping")


def _wait_for_pipe_state(name: str, state: str) -> None:
    pipes_client = _client("pipes")
    _poll(
        lambda: True
        if pipes_client.describe_pipe(Name=name)["CurrentState"] == state
        else None,
        description=f"pipe state {state}",
        timeout=90,
        interval=3,
    )


@contextlib.contextmanager
def _stopped_pipe():
    pipes_client = _client("pipes")
    name = _pipe_name()
    initial_state = pipes_client.describe_pipe(Name=name)["CurrentState"]

    if initial_state != "STOPPED":
        pipes_client.stop_pipe(Name=name)
        _wait_for_pipe_state(name, "STOPPED")

    try:
        yield
    finally:
        if initial_state != "STOPPED":
            pipes_client.start_pipe(Name=name)
            _wait_for_pipe_state(name, "RUNNING")


@contextlib.contextmanager
def _disabled_worker_event_source_mapping():
    lambda_client = _client("lambda")
    mapping_uuid = _event_source_mapping_uuid()
    initial_state = lambda_client.get_event_source_mapping(UUID=mapping_uuid)["State"]

    if initial_state != "Disabled":
        lambda_client.update_event_source_mapping(UUID=mapping_uuid, Enabled=False)
        _poll(
            lambda: True
            if lambda_client.get_event_source_mapping(UUID=mapping_uuid)["State"] == "Disabled"
            else None,
            description="worker event source mapping disabled",
            timeout=90,
            interval=3,
        )

    try:
        yield
    finally:
        if initial_state != "Disabled":
            lambda_client.update_event_source_mapping(UUID=mapping_uuid, Enabled=True)
            _poll(
                lambda: True
                if lambda_client.get_event_source_mapping(UUID=mapping_uuid)["State"] == "Enabled"
                else None,
                description="worker event source mapping enabled",
                timeout=90,
                interval=3,
            )


def _find_execution_with_marker(
    *,
    state_machine_arn: str,
    baseline_execution_arns: set[str],
    marker: str,
) -> dict | None:
    sfn = _client("stepfunctions")
    executions = sfn.list_executions(stateMachineArn=state_machine_arn, maxResults=25)["executions"]
    for execution in executions:
        execution_arn = execution["executionArn"]
        if execution_arn in baseline_execution_arns:
            continue
        description = sfn.describe_execution(executionArn=execution_arn)
        if marker not in description.get("input", ""):
            continue
        if description["status"] == "RUNNING":
            return None
        return description
    return None


def test_stack_outputs_and_resource_inventory_exist():
    outputs = _stack_outputs()
    assert "OrdersApiUrl" in outputs
    assert "OrdersArchiveBucketName" in outputs
    assert "OrdersQueueUrl" in outputs
    assert "OrdersNotificationsTopicArn" in outputs
    assert "OrdersPipeName" in outputs

    assert len(_physical_ids("AWS::EC2::Subnet")) == 4
    assert len(_physical_ids("AWS::EC2::SecurityGroup")) == 4
    assert len(_physical_ids("AWS::ApiGateway::VpcLink")) == 0
    assert len(_physical_ids("AWS::S3::Bucket")) == 1
    assert len(_physical_ids("AWS::SQS::Queue")) == 1
    assert len(_physical_ids("AWS::SNS::Topic")) == 1
    assert len(_physical_ids("AWS::SecretsManager::Secret")) == 1
    assert len(_physical_ids("AWS::RDS::DBInstance")) == 1
    assert len(_physical_ids("AWS::RDS::DBSubnetGroup")) == 1
    assert len(_physical_ids("AWS::EC2::VPCEndpoint")) == 1
    assert _physical_id_with_prefix("AWS::Lambda::Function", "OrdersApiFunction")
    assert _physical_id_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")
    assert len(_physical_ids("AWS::Lambda::EventSourceMapping")) == 1
    assert len(_physical_ids("AWS::Events::EventBus")) == 1
    assert len(_physical_ids("AWS::Events::Rule")) == 1
    assert len(_physical_ids("AWS::StepFunctions::StateMachine")) == 1
    assert _pipe_name()


def test_deployed_network_layout_and_security_groups_match():
    ec2 = _client("ec2")
    subnet_ids = _physical_ids("AWS::EC2::Subnet")
    subnets = ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    cidrs = {subnet["CidrBlock"] for subnet in subnets}
    assert cidrs == {"10.0.1.0/24", "10.0.2.0/24", "10.0.101.0/24", "10.0.102.0/24"}

    public_map = {subnet["CidrBlock"]: subnet["MapPublicIpOnLaunch"] for subnet in subnets}
    assert public_map["10.0.101.0/24"] is True
    assert public_map["10.0.102.0/24"] is True
    assert public_map["10.0.1.0/24"] is False
    assert public_map["10.0.2.0/24"] is False

    api_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersApiSecurityGroup")
    worker_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersWorkerSecurityGroup")
    endpoint_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersSecretsEndpointSecurityGroup")
    data_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersDataPlaneSecurityGroup")

    security_groups = ec2.describe_security_groups(
        GroupIds=[api_sg_id, worker_sg_id, endpoint_sg_id, data_sg_id]
    )["SecurityGroups"]
    by_id = {group["GroupId"]: group for group in security_groups}

    assert by_id[api_sg_id]["IpPermissions"] == []
    assert by_id[worker_sg_id]["IpPermissions"] == []
    assert by_id[endpoint_sg_id]["IpPermissions"] != []
    assert len(by_id[api_sg_id]["IpPermissionsEgress"]) == 1
    assert len(by_id[worker_sg_id]["IpPermissionsEgress"]) == 3
    assert not any(
        pair.get("GroupId") == endpoint_sg_id
        for permission in by_id[api_sg_id]["IpPermissionsEgress"]
        for pair in permission.get("UserIdGroupPairs", [])
    )
    assert any(
        pair.get("GroupId") == endpoint_sg_id
        for permission in by_id[worker_sg_id]["IpPermissionsEgress"]
        for pair in permission.get("UserIdGroupPairs", [])
    )

    data_ingress = by_id[data_sg_id]["IpPermissions"]
    assert len(data_ingress) == 1
    assert data_ingress[0]["FromPort"] == 5432
    assert data_ingress[0]["ToPort"] == 5432
    assert data_ingress[0]["UserIdGroupPairs"][0]["GroupId"] == worker_sg_id

    endpoint_id = _physical_id_with_prefix("AWS::EC2::VPCEndpoint", "OrdersSecretsManagerEndpoint")
    endpoint = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])["VpcEndpoints"][0]
    assert set(endpoint["SubnetIds"]) == {
        _physical_id_with_prefix("AWS::EC2::Subnet", "OrdersPrivateSubnetA"),
        _physical_id_with_prefix("AWS::EC2::Subnet", "OrdersPrivateSubnetB"),
    }
    assert endpoint["ServiceName"].endswith(".secretsmanager")

    endpoint_sg_id = endpoint["Groups"][0]["GroupId"]
    endpoint_sg = ec2.describe_security_groups(GroupIds=[endpoint_sg_id])["SecurityGroups"][0]
    assert len(endpoint_sg["IpPermissions"]) == 1
    assert endpoint_sg["IpPermissions"][0]["FromPort"] == 443
    assert endpoint_sg["IpPermissions"][0]["ToPort"] == 443
    assert endpoint_sg["IpPermissions"][0]["UserIdGroupPairs"][0]["GroupId"] == worker_sg_id
    assert all(
        pair["GroupId"] != api_sg_id
        for permission in endpoint_sg["IpPermissions"]
        for pair in permission.get("UserIdGroupPairs", [])
    )


def test_deployed_storage_database_and_secret_configuration_match():
    s3 = _client("s3")
    sqs = _client("sqs")
    rds = _client("rds")
    secretsmanager = _client("secretsmanager")

    outputs = _stack_outputs()
    bucket_name = outputs["OrdersArchiveBucketName"]
    queue_url = outputs["OrdersQueueUrl"]

    s3.head_bucket(Bucket=bucket_name)
    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
    assert versioning["Status"] == "Enabled"
    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    assert (
        encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"][
            "SSEAlgorithm"
        ]
        == "AES256"
    )
    public_access = s3.get_public_access_block(Bucket=bucket_name)
    assert public_access["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }

    queue_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "SqsManagedSseEnabled"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "60"
    assert queue_attrs["SqsManagedSseEnabled"].lower() == "true"

    db_instance_id = _physical_id_with_prefix("AWS::RDS::DBInstance", "OrdersDatabase")
    db = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)["DBInstances"][0]
    assert db["Engine"] == "postgres"
    assert db["EngineVersion"].startswith("15")
    assert db["DBInstanceClass"] == "db.t3.micro"
    assert db["AllocatedStorage"] == 20
    assert db["StorageEncrypted"] is True
    assert db["PubliclyAccessible"] is False
    assert db["DBSubnetGroup"]["DBSubnetGroupName"]

    secret_arn = _physical_id_with_prefix("AWS::SecretsManager::Secret", "OrdersDatabaseSecret")
    secret = secretsmanager.describe_secret(SecretId=secret_arn)
    assert secret["ARN"] == secret_arn


def test_deployed_api_lambdas_and_logs_match():
    lambda_client = _client("lambda")
    apigateway = _client("apigateway")
    logs = _client("logs")

    api_fn_name = _physical_id_with_prefix("AWS::Lambda::Function", "OrdersApiFunction")
    worker_fn_name = _physical_id_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")

    api_fn = lambda_client.get_function_configuration(FunctionName=api_fn_name)
    worker_fn = lambda_client.get_function_configuration(FunctionName=worker_fn_name)

    assert api_fn["Runtime"] == "nodejs20.x"
    assert api_fn["MemorySize"] == 256
    assert api_fn["Timeout"] == 10
    assert set(api_fn["Environment"]["Variables"]) == {"BUCKET_NAME", "EVENT_BUS_NAME", "QUEUE_URL"}

    assert worker_fn["Runtime"] == "nodejs20.x"
    assert worker_fn["MemorySize"] == 256
    assert worker_fn["Timeout"] == 20
    assert set(worker_fn["Environment"]["Variables"]) == {
        "DB_HOST",
        "DB_NAME",
        "DB_PORT",
        "DB_SECRET_ARN",
        "TOPIC_ARN",
    }

    rest_api_id = _physical_id_with_prefix("AWS::ApiGateway::RestApi", "OrdersApiGateway")
    resources = apigateway.get_resources(restApiId=rest_api_id)["items"]
    orders_resource = next(item for item in resources if item.get("path") == "/orders")
    assert "POST" in orders_resource["resourceMethods"]

    stages = apigateway.get_stages(restApiId=rest_api_id)["item"]
    assert any(stage["stageName"] == "prod" for stage in stages)

    log_group_names = {f"/aws/lambda/{api_fn_name}", f"/aws/lambda/{worker_fn_name}"}
    target_log_groups: dict[str, dict] = {}
    for log_group_name in log_group_names:
        groups = logs.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
        exact_match = next(group for group in groups if group["logGroupName"] == log_group_name)
        target_log_groups[log_group_name] = exact_match

    assert len(target_log_groups) == 2
    assert all("retentionInDays" in group for group in target_log_groups.values())
    assert all(group["retentionInDays"] == 7 for group in target_log_groups.values())


def test_deployed_eventing_and_workflow_chain_match():
    events_client = _client("events")
    pipes_client = _client("pipes")
    sfn = _client("stepfunctions")
    sns = _client("sns")
    sqs = _client("sqs")

    event_bus_name = _physical_id_with_prefix("AWS::Events::EventBus", "OrdersEventBus")
    rule_physical_id = _physical_id_with_prefix("AWS::Events::Rule", "OrdersEventRule")
    pipe_name = _pipe_name()
    state_machine_arn = _physical_id_with_prefix("AWS::StepFunctions::StateMachine", "OrdersStateMachine")
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=_stack_outputs()["OrdersQueueUrl"],
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    rule_name_candidates = [rule_physical_id]
    if "|" in rule_physical_id:
        _, parsed_rule_name = rule_physical_id.split("|", 1)
        rule_name_candidates.insert(0, parsed_rule_name)

    rules = events_client.list_rules(EventBusName=event_bus_name).get("Rules", [])
    for rule in rules:
        if rule.get("EventPattern") == '{"source":["orders.api"]}':
            rule_name_candidates.insert(0, rule["Name"])
            break

    rule_name = None
    for candidate in rule_name_candidates:
        try:
            rule = events_client.describe_rule(Name=candidate, EventBusName=event_bus_name)
        except ClientError:
            continue
        if rule.get("EventPattern") == '{"source":["orders.api"]}':
            rule_name = candidate
            break

    assert rule_name is not None
    rule = events_client.describe_rule(Name=rule_name, EventBusName=event_bus_name)
    assert rule["EventPattern"] == '{"source":["orders.api"]}'
    targets = events_client.list_targets_by_rule(Rule=rule_name, EventBusName=event_bus_name)["Targets"]
    assert len(targets) == 1
    assert targets[0]["Arn"] == queue_arn

    pipe = pipes_client.describe_pipe(Name=pipe_name)
    assert pipe["Source"] == queue_arn
    assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    state_machine = sfn.describe_state_machine(stateMachineArn=state_machine_arn)
    definition = json.loads(state_machine["definition"])
    assert definition["StartAt"] == "RecordTimestamp"
    assert definition["States"]["RecordTimestamp"]["Type"] == "Pass"
    assert definition["States"]["RecordTimestamp"]["Parameters"]["recordedAt.$"] == "$$.State.EnteredTime"
    assert definition["States"]["OrdersWorkflowSucceeded"]["Type"] == "Succeed"

    topic_arn = _stack_outputs()["OrdersNotificationsTopicArn"]
    topic = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
    assert topic["TopicArn"] == topic_arn


def test_http_post_to_api_archives_payload_and_triggers_processing():
    marker = f"api-{uuid.uuid4().hex}"
    payload = {
        "customerId": "customer-123",
        "items": [{"sku": "sku-1", "quantity": 2}],
        "orderId": marker,
    }

    with _stopped_pipe(), _temporary_topic_subscription() as notifications_queue_url:
        result = _wait_for_api_post(payload)
        _wait_for_bucket_object(
            _stack_outputs()["OrdersArchiveBucketName"],
            result["archive_key"],
            result["request_body"],
        )

        notification = _wait_for_raw_topic_message(notifications_queue_url, marker)
        assert notification["recordCount"] >= 1
        assert marker in json.dumps(notification)


def test_unknown_api_resource_returns_client_error():
    api_url = _stack_outputs()["OrdersApiUrl"]
    error = _post_json_expect_http_error(f"{api_url}missing-resource", json.dumps({"invalid": True}))
    assert error.code in {403, 404}


def test_http_post_to_api_enqueues_direct_and_eventbridge_messages_before_consumers_run():
    queue_url = _stack_outputs()["OrdersQueueUrl"]
    marker = f"queue-flow-{uuid.uuid4().hex}"
    payload = {
        "customerId": "customer-789",
        "items": [{"sku": "sku-2", "quantity": 1}],
        "orderId": marker,
    }

    with _stopped_pipe(), _disabled_worker_event_source_mapping():
        _drain_queue(queue_url)
        result = _wait_for_api_post(payload)
        _wait_for_bucket_object(
            _stack_outputs()["OrdersArchiveBucketName"],
            result["archive_key"],
            result["request_body"],
        )
        messages = _wait_for_queue_messages(queue_url, minimum_count=2)

    assert any(message == payload for message in messages)

    routed_event = next(
        message
        for message in messages
        if isinstance(message, dict) and message.get("source") == "orders.api"
    )
    detail = _parse_json_payload(routed_event["detail"])
    assert (routed_event.get("detail-type") or routed_event.get("detailType")) == "OrderReceived"
    assert detail["archiveKey"] == result["archive_key"]
    assert detail["receivedBody"] == result["request_body"]


def test_worker_lambda_invocation_with_payload_publishes_notification():
    lambda_client = _client("lambda")
    worker_fn_name = _physical_id_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")
    marker = f"invoke-{uuid.uuid4().hex}"
    payload = {"orderId": marker, "source": "direct-invoke"}

    with _temporary_topic_subscription() as notifications_queue_url:
        response = lambda_client.invoke(
            FunctionName=worker_fn_name,
            Payload=json.dumps(payload).encode("utf-8"),
        )
        assert response["StatusCode"] == 200
        assert response.get("FunctionError") is None

        result = json.loads(response["Payload"].read().decode("utf-8"))
        assert result["recordCount"] == 1
        assert result["dbConnection"]["status"] == "stubbed-connection"
        assert result["dbConnection"]["database"] == "orders"

        notification = _wait_for_raw_topic_message(notifications_queue_url, marker)
        assert notification["recordCount"] == 1
        assert notification["records"][0]["payload"]["orderId"] == marker


def test_invoking_unknown_lambda_function_fails():
    lambda_client = _client("lambda")
    with pytest.raises(ClientError) as exc_info:
        lambda_client.invoke(
            FunctionName=f"{_physical_id_with_prefix('AWS::Lambda::Function', 'OrdersWorkerFunction')}-missing",
            Payload=b"{}",
        )
    assert exc_info.value.response["Error"]["Code"] in {"ResourceNotFoundException", "ValidationException"}


def test_sqs_message_flows_through_lambda_to_sns_when_pipe_is_stopped():
    sqs = _client("sqs")
    marker = f"sqs-{uuid.uuid4().hex}"
    payload = {"marker": marker, "path": "sqs-lambda-sns"}

    with _stopped_pipe(), _temporary_topic_subscription() as notifications_queue_url:
        sqs.send_message(
            QueueUrl=_stack_outputs()["OrdersQueueUrl"],
            MessageBody=json.dumps(payload),
        )

        notification = _wait_for_raw_topic_message(notifications_queue_url, marker)
        assert notification["recordCount"] == 1
        assert notification["records"][0]["payload"]["marker"] == marker


def test_eventbridge_event_routes_to_queue_and_reaches_worker():
    events_client = _client("events")
    marker = f"event-{uuid.uuid4().hex}"
    payload = {"marker": marker, "path": "eventbridge-route"}

    with _stopped_pipe(), _temporary_topic_subscription() as notifications_queue_url:
        result = events_client.put_events(
            Entries=[
                {
                    "EventBusName": _physical_id_with_prefix("AWS::Events::EventBus", "OrdersEventBus"),
                    "Source": "orders.api",
                    "DetailType": "IntegrationEvent",
                    "Detail": json.dumps(payload),
                }
            ]
        )
        assert result["FailedEntryCount"] == 0

        notification = _wait_for_raw_topic_message(notifications_queue_url, marker)
        assert notification["recordCount"] >= 1
        assert marker in json.dumps(notification)


def test_pipe_starts_state_machine_execution_when_worker_mapping_is_disabled():
    sqs = _client("sqs")
    sfn = _client("stepfunctions")
    marker = f"pipe-{uuid.uuid4().hex}"
    state_machine_arn = _physical_id_with_prefix("AWS::StepFunctions::StateMachine", "OrdersStateMachine")
    baseline_execution_arns = {
        execution["executionArn"]
        for execution in sfn.list_executions(stateMachineArn=state_machine_arn, maxResults=25)["executions"]
    }

    with _disabled_worker_event_source_mapping():
        sqs.send_message(
            QueueUrl=_stack_outputs()["OrdersQueueUrl"],
            MessageBody=json.dumps({"marker": marker, "path": "pipe-stepfunctions"}),
        )

        execution = _poll(
            lambda: _find_execution_with_marker(
                state_machine_arn=state_machine_arn,
                baseline_execution_arns=baseline_execution_arns,
                marker=marker,
            ),
            description=f"Step Functions execution for {marker}",
            timeout=120,
            interval=3,
        )

    assert execution["status"] == "SUCCEEDED"
    assert marker in execution["input"]
    execution_input = json.loads(execution["input"])
    assert execution_input["recordCount"] == 1
    assert execution_input["records"][0]["payload"]["marker"] == marker
    assert execution_input["dbConnection"]["status"] == "stubbed-connection"
    assert execution_input["dbConnection"]["database"] == "orders"
    output = json.loads(execution["output"])
    assert isinstance(output["ingestMetadata"]["recordedAt"], str)
    assert output["ingestMetadata"]["recordedAt"].endswith("Z")
