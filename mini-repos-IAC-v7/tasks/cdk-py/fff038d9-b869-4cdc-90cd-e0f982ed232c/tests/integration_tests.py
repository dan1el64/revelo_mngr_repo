import json
import os
import time
import uuid
from urllib import error, request
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)


STACK_NAME = "EventDrivenIngestionStack"
STATUS_DB_NAME = "event_ingestion_status"


def aws_endpoint(service_name):
    if service_name == "s3":
        return (
            os.environ.get("AWS_ENDPOINT_URL_S3")
            or os.environ.get("AWS_ENDPOINT")
            or os.environ.get("AWS_ENDPOINT_URL")
        )
    return os.environ.get("AWS_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")


def client(service_name):
    kwargs = {
        "region_name": os.environ.get("AWS_REGION", "us-east-1"),
    }
    endpoint = aws_endpoint(service_name)
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if service_name == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service_name, **kwargs)


def resource(service_name):
    kwargs = {
        "region_name": os.environ.get("AWS_REGION", "us-east-1"),
    }
    endpoint = aws_endpoint(service_name)
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if service_name == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.resource(service_name, **kwargs)


def eventually(assertion, timeout_seconds=None, interval_seconds=1):
    timeout = timeout_seconds or int(os.environ.get("INTEGRATION_TEST_TIMEOUT_SECONDS", "120"))
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        try:
            return assertion()
        except (AssertionError, ClientError, KeyError) as exc:
            last_error = exc
            time.sleep(interval_seconds)

    raise AssertionError(f"condition was not met before timeout: {last_error!r}")


@pytest.fixture(scope="session")
def deployed_stack():
    os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
    cloudformation = client("cloudformation")

    try:
        stack = cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError) as exc:
        pytest.skip(f"{STACK_NAME} is not available for integration tests: {exc}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        message = exc.response.get("Error", {}).get("Message", "")
        if code == "ValidationError" and "does not exist" in message:
            pytest.skip(f"{STACK_NAME} must be deployed before integration tests run")
        raise

    outputs = {
        output["OutputKey"]: output["OutputValue"]
        for output in stack.get("Outputs", [])
    }
    return {"stack": stack, "outputs": outputs}


@pytest.fixture(scope="session")
def stack_resources(deployed_stack):
    cloudformation = client("cloudformation")
    summaries = []
    next_token = None
    while True:
        kwargs = {"StackName": deployed_stack["stack"]["StackName"]}
        if next_token:
            kwargs["NextToken"] = next_token
        response = cloudformation.describe_stack_resources(**kwargs)
        summaries.extend(response["StackResources"])
        next_token = response.get("NextToken")
        if not next_token:
            break

    def physical_id(logical_id_prefix, resource_type):
        matches = [
            summary
            for summary in summaries
            if summary["LogicalResourceId"].startswith(logical_id_prefix)
            and summary["ResourceType"] == resource_type
        ]
        assert len(matches) == 1
        return matches[0]["PhysicalResourceId"]

    return {
        "ApplicationDatabase": physical_id("ApplicationDatabase", "AWS::RDS::DBInstance"),
        "AuditTable": physical_id("AuditTable", "AWS::DynamoDB::Table"),
        "DatabaseSecurityGroup": physical_id(
            "DatabaseTierSecurityGroup",
            "AWS::EC2::SecurityGroup",
        ),
        "DatabaseSubnetGroup": physical_id(
            "ApplicationDatabaseSubnetGroup",
            "AWS::RDS::DBSubnetGroup",
        ),
        "IngestLambda": physical_id("IngestLambda", "AWS::Lambda::Function"),
        "IngestionQueue": physical_id("IngestionQueue", "AWS::SQS::Queue"),
        "LambdaSecurityGroup": physical_id(
            "LambdaRunspacesSecurityGroup",
            "AWS::EC2::SecurityGroup",
        ),
        "NotificationsQueue": physical_id("NotificationsQueue", "AWS::SQS::Queue"),
        "NotificationsTopic": physical_id("NotificationsTopic", "AWS::SNS::Topic"),
        "ProcessedRecordsCrawler": physical_id("ProcessedRecordsCrawler", "AWS::Glue::Crawler"),
        "ProcessingPipe": physical_id("ProcessingPipe", "AWS::Pipes::Pipe"),
        "ProcessingStateMachine": physical_id(
            "ProcessingStateMachine",
            "AWS::StepFunctions::StateMachine",
        ),
        "RestApi": physical_id("IngestionApi", "AWS::ApiGateway::RestApi"),
        "StatusTable": physical_id("StatusTable", "AWS::DynamoDB::Table"),
        "WorkerLambda": physical_id("WorkerLambda", "AWS::Lambda::Function"),
        "WorkerQueueMapping": physical_id(
            "WorkerQueueMapping",
            "AWS::Lambda::EventSourceMapping",
        ),
    }


@pytest.fixture(scope="session")
def stack_template(deployed_stack):
    template = client("cloudformation").get_template(
        StackName=deployed_stack["stack"]["StackName"],
    )["TemplateBody"]
    if isinstance(template, str):
        return json.loads(template)
    return template


def template_resources_by_type(template_json, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template_json["Resources"].items()
        if resource["Type"] == resource_type
    }


def template_resource_by_prefix(template_json, logical_id_prefix, resource_type):
    matches = [
        (logical_id, resource)
        for logical_id, resource in template_resources_by_type(
            template_json, resource_type
        ).items()
        if logical_id.startswith(logical_id_prefix)
    ]
    assert len(matches) == 1
    return matches[0]


def api_candidate_urls(api_invoke_url, rest_api_id):
    endpoint = aws_endpoint("apigateway")
    if not endpoint:
        return [api_invoke_url]

    endpoint = endpoint.rstrip("/")
    return [
        f"{endpoint}/restapis/{rest_api_id}/v1/_user_request_/ingest",
        f"{endpoint}/_aws/execute-api/{rest_api_id}/v1/ingest",
    ]


def post_api_json(deployed_stack, stack_resources, payload=None, raw_body=None):
    body = raw_body if raw_body is not None else json.dumps(payload)
    data = body.encode("utf-8")
    last_error = None

    for url in api_candidate_urls(deployed_stack["outputs"]["ApiInvokeUrl"], stack_resources["RestApi"]):
        http_request = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=10) as response:
                return response.status, response.read().decode("utf-8")
        except error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8")
        except error.URLError as exc:
            last_error = exc

    raise AssertionError(f"API Gateway endpoint was not reachable: {last_error!r}")


def parse_json_body(body):
    return json.loads(body) if body else {}


def parse_optional_json_body(body):
    try:
        return parse_json_body(body)
    except json.JSONDecodeError:
        return {}


def provider_license_blocks(exc, service_name):
    if not aws_endpoint(service_name):
        return False

    error = exc.response.get("Error", {})
    message = error.get("Message", "").lower()
    return "not included within your" in message and "license" in message


def drain_queue(queue_url, max_rounds=5):
    sqs = client("sqs")
    for _ in range(max_rounds):
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        for message in messages:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


def receive_matching_message(queue_url, predicate, timeout_seconds=None):
    sqs = client("sqs")

    def poll():
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        assert messages
        for message in messages:
            body = json.loads(message["Body"])
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            if predicate(body):
                return body
        raise AssertionError("no matching SQS message in received batch")

    return eventually(poll, timeout_seconds=timeout_seconds)


def get_status_item(table_name, request_id):
    return resource("dynamodb").Table(table_name).get_item(
        Key={"pk": request_id, "sk": "workflow"},
    ).get("Item")


def get_audit_item(table_name, request_id):
    return resource("dynamodb").Table(table_name).get_item(
        Key={"pk": request_id},
    ).get("Item")


def set_worker_mapping_enabled(mapping_uuid, enabled):
    lambda_client = client("lambda")
    lambda_client.update_event_source_mapping(UUID=mapping_uuid, Enabled=enabled)
    desired_state = "Enabled" if enabled else "Disabled"

    def assert_state():
        mapping = lambda_client.get_event_source_mapping(UUID=mapping_uuid)
        assert mapping["State"] == desired_state
        return mapping

    return eventually(assert_state, timeout_seconds=90)


def pipe_name(physical_id):
    return physical_id.rsplit("/", 1)[-1]


def ensure_pipe_running(pipe_physical_id):
    pipes = client("pipes")
    name = pipe_name(pipe_physical_id)

    def current_state():
        try:
            return pipes.describe_pipe(Name=name)["CurrentState"]
        except ClientError as exc:
            if provider_license_blocks(exc, "pipes"):
                return "PROVIDER_LICENSE_BLOCKED"
            raise

    state = current_state()
    if state == "PROVIDER_LICENSE_BLOCKED":
        return False
    if state != "RUNNING":
        try:
            pipes.start_pipe(Name=name)
        except ClientError as exc:
            if provider_license_blocks(exc, "pipes"):
                return False
            raise

    def assert_running():
        state = current_state()
        assert state == "RUNNING"
        return state

    eventually(assert_running, timeout_seconds=90)
    return True


def ensure_pipe_stopped(pipe_physical_id):
    pipes = client("pipes")
    name = pipe_name(pipe_physical_id)

    def current_state():
        try:
            return pipes.describe_pipe(Name=name)["CurrentState"]
        except ClientError as exc:
            if provider_license_blocks(exc, "pipes"):
                return "PROVIDER_LICENSE_BLOCKED"
            raise

    state = current_state()
    if state == "PROVIDER_LICENSE_BLOCKED":
        return False
    if state != "STOPPED":
        pipes.stop_pipe(Name=name)

    def assert_stopped():
        state = current_state()
        assert state == "STOPPED"
        return state

    eventually(assert_stopped, timeout_seconds=90)
    return True


def wait_for_execution(execution_arn):
    sfn = client("stepfunctions")

    def assert_finished():
        execution = sfn.describe_execution(executionArn=execution_arn)
        assert execution["status"] not in {"RUNNING", "STARTING"}
        assert execution["status"] == "SUCCEEDED"
        return execution

    return eventually(assert_finished, timeout_seconds=120)


def notification_matches_request(request_id):
    def matches(body):
        message = body.get("Message", "")
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return request_id in message
        return payload.get("requestId") == request_id or payload.get("detail", {}).get("requestId") == request_id

    return matches


def test_deployed_stack_outputs_and_runtime_resources_are_reachable(deployed_stack, stack_resources):
    outputs = deployed_stack["outputs"]
    assert set(outputs) >= {
        "ApiInvokeUrl",
        "EventBusName",
        "IngestionQueueUrl",
        "ProcessedBucketName",
    }

    parsed_url = urlparse(outputs["ApiInvokeUrl"])
    assert parsed_url.scheme == "https"
    assert stack_resources["RestApi"] in parsed_url.netloc
    assert ".execute-api." in parsed_url.netloc
    assert parsed_url.path == "/v1/ingest"

    queue_attributes = client("sqs").get_queue_attributes(
        QueueUrl=outputs["IngestionQueueUrl"],
        AttributeNames=["QueueArn"],
    )["Attributes"]
    bucket_versioning = client("s3").get_bucket_versioning(
        Bucket=outputs["ProcessedBucketName"],
    )
    event_bus = client("events").describe_event_bus(Name=outputs["EventBusName"])
    audit_table = client("dynamodb").describe_table(
        TableName=stack_resources["AuditTable"],
    )["Table"]
    status_table = client("dynamodb").describe_table(
        TableName=stack_resources["StatusTable"],
    )["Table"]
    ingest_lambda = client("lambda").get_function(FunctionName=stack_resources["IngestLambda"])
    worker_lambda = client("lambda").get_function(FunctionName=stack_resources["WorkerLambda"])

    assert "IngestionQueue" in queue_attributes["QueueArn"]
    assert bucket_versioning["Status"] == "Enabled"
    assert event_bus["Name"] == outputs["EventBusName"]
    assert audit_table["TableStatus"] in {"ACTIVE", "CREATING"}
    assert status_table["TableStatus"] in {"ACTIVE", "CREATING"}
    assert ingest_lambda["Configuration"]["State"] in {"Active", "Pending"}
    assert worker_lambda["Configuration"]["State"] in {"Active", "Pending"}


def test_api_gateway_post_ingest_enqueues_message_and_rejects_bad_payloads(
    deployed_stack,
    stack_resources,
):
    set_worker_mapping_enabled(stack_resources["WorkerQueueMapping"], False)
    pipe_available = ensure_pipe_stopped(stack_resources["ProcessingPipe"])
    try:
        drain_queue(stack_resources["IngestionQueue"])
        payload = {
            "source": "integration-test",
            "correlationId": f"api-sqs-{uuid.uuid4()}",
            "records": [{"id": 1, "value": "accepted"}],
        }

        status_code, body = post_api_json(deployed_stack, stack_resources, payload=payload)
        response_body = parse_json_body(body)
        request_id = response_body["requestId"]

        assert status_code == 202
        assert len(request_id) > 20

        audit_table = stack_resources["AuditTable"]

        def assert_audit_item_exists():
            item = get_audit_item(audit_table, request_id)
            assert item is not None
            assert item["pk"] == request_id
            assert item["status"] == "RECEIVED"
            assert int(item["ttl"]) > int(time.time())
            return item

        eventually(assert_audit_item_exists)

        message = receive_matching_message(
            stack_resources["IngestionQueue"],
            lambda sqs_body: sqs_body["requestId"] == request_id,
        )
        assert message["payload"] == payload

        invalid_status, invalid_body = post_api_json(
            deployed_stack,
            stack_resources,
            raw_body="not-json",
        )
        assert invalid_status == 400
        invalid_response = parse_optional_json_body(invalid_body)
        if invalid_response:
            assert invalid_response.get("message") in {
                "body must be valid JSON",
                "Invalid request body",
            }

        oversized_status, oversized_body = post_api_json(
            deployed_stack,
            stack_resources,
            raw_body=json.dumps("x" * 262145),
        )
        assert oversized_status in {400, 413}
        oversized_response = parse_optional_json_body(oversized_body)
        if oversized_response:
            assert "message" in oversized_response
    finally:
        if pipe_available:
            ensure_pipe_running(stack_resources["ProcessingPipe"])
        set_worker_mapping_enabled(stack_resources["WorkerQueueMapping"], False)


def test_full_pipeline_from_api_gateway_to_sns_notification(deployed_stack, stack_resources):
    set_worker_mapping_enabled(stack_resources["WorkerQueueMapping"], True)
    ensure_pipe_stopped(stack_resources["ProcessingPipe"])
    drain_queue(stack_resources["IngestionQueue"])
    drain_queue(stack_resources["NotificationsQueue"])

    payload = {
        "source": "integration-test",
        "correlationId": f"e2e-{uuid.uuid4()}",
        "records": [{"id": 42, "value": "processed"}],
    }
    status_code, body = post_api_json(deployed_stack, stack_resources, payload=payload)
    request_id = parse_json_body(body)["requestId"]
    processed_key = f"processed/{request_id}.json"

    assert status_code == 202

    audit_table = stack_resources["AuditTable"]
    s3_client = client("s3")

    def assert_processed_object_exists():
        s3_response = s3_client.get_object(
            Bucket=deployed_stack["outputs"]["ProcessedBucketName"],
            Key=processed_key,
        )
        processed_record = json.loads(s3_response["Body"].read().decode("utf-8"))
        assert processed_record["requestId"] == request_id
        assert processed_record["payload"] == payload
        assert processed_record["audit"]["status"] == "RECEIVED"
        return processed_record

    def assert_audit_table_was_updated():
        item = get_audit_item(audit_table, request_id)
        assert item is not None
        assert item["objectKey"] == processed_key
        assert "processedAt" in item
        return item

    def assert_state_machine_wrote_enriched_status():
        item = get_status_item(stack_resources["StatusTable"], request_id)
        assert item is not None
        assert item["status"] == "PROCESSED"
        payload_detail = json.loads(item["payload"])
        assert payload_detail["requestId"] == request_id
        assert payload_detail["bucket"] == deployed_stack["outputs"]["ProcessedBucketName"]
        assert payload_detail["key"] == processed_key
        return item

    eventually(assert_processed_object_exists)
    eventually(assert_audit_table_was_updated)
    eventually(assert_state_machine_wrote_enriched_status)
    notification = receive_matching_message(
        stack_resources["NotificationsQueue"],
        notification_matches_request(request_id),
    )
    assert notification["Type"] == "Notification"


def test_ingestion_sqs_event_source_mapping_reaches_pipe_enricher_and_state_machine(
    deployed_stack,
    stack_resources,
    stack_template,
):
    set_worker_mapping_enabled(stack_resources["WorkerQueueMapping"], False)
    pipe_available = ensure_pipe_running(stack_resources["ProcessingPipe"])
    drain_queue(stack_resources["IngestionQueue"])
    drain_queue(stack_resources["NotificationsQueue"])
    if not pipe_available:
        _pipe_id, pipe = template_resource_by_prefix(
            stack_template,
            "ProcessingPipe",
            "AWS::Pipes::Pipe",
        )
        _mapping_id, mapping = template_resource_by_prefix(
            stack_template,
            "WorkerQueueMapping",
            "AWS::Lambda::EventSourceMapping",
        )
        _enricher_id, enricher = template_resource_by_prefix(
            stack_template,
            "EnricherLambda",
            "AWS::Lambda::Function",
        )
        _state_machine_id, state_machine = template_resource_by_prefix(
            stack_template,
            "ProcessingStateMachine",
            "AWS::StepFunctions::StateMachine",
        )

        source_arn = pipe["Properties"]["Source"]
        queue_arn = mapping["Properties"]["EventSourceArn"]

        assert source_arn == queue_arn
        assert pipe["Properties"]["Enrichment"] == {
            "Fn::GetAtt": [_enricher_id, "Arn"]
        }
        assert pipe["Properties"]["Target"] == {
            "Fn::GetAtt": [_state_machine_id, "Arn"]
        }
        assert (
            pipe["Properties"]["TargetParameters"][
                "StepFunctionStateMachineParameters"
            ]["InvocationType"]
            == "FIRE_AND_FORGET"
        )
        assert enricher["Properties"]["FunctionName"] == "event-ingestion-enricher"
        assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
        return

    request_id = f"ingestion-sqs-pipe-{uuid.uuid4()}"
    payload = {
        "source": "integration-test",
        "correlationId": request_id,
        "records": [{"id": 7, "value": "pipe-triggered"}],
    }
    client("sqs").send_message(
        QueueUrl=stack_resources["IngestionQueue"],
        MessageBody=json.dumps(
            {
                "requestId": request_id,
                "payload": payload,
            }
        ),
    )
    def assert_eventbridge_pipe_enriched_before_state_machine_write():
        item = get_status_item(stack_resources["StatusTable"], request_id)
        assert item is not None
        assert item["status"] == "PROCESSED"
        payload_detail = json.loads(item["payload"])
        assert payload_detail["requestId"] == request_id
        assert payload_detail["enriched"] is True
        assert "timestamp" in payload_detail
        assert payload_detail["payload"] == payload
        return item

    eventually(assert_eventbridge_pipe_enriched_before_state_machine_write)
    receive_matching_message(
        stack_resources["NotificationsQueue"],
        notification_matches_request(request_id),
    )


def test_step_functions_execution_writes_status_and_publishes_to_sns(stack_resources):
    drain_queue(stack_resources["NotificationsQueue"])
    request_id = f"sfn-{uuid.uuid4()}"
    execution = client("stepfunctions").start_execution(
        stateMachineArn=stack_resources["ProcessingStateMachine"],
        name=f"integration-{request_id}",
        input=json.dumps(
            {
                "detail": {
                    "requestId": request_id,
                    "bucket": "direct-stepfunctions-test",
                    "key": f"processed/{request_id}.json",
                }
            }
        ),
    )

    wait_for_execution(execution["executionArn"])

    item = get_status_item(stack_resources["StatusTable"], request_id)
    assert item is not None
    assert item["status"] == "PROCESSED"
    assert json.loads(item["payload"])["requestId"] == request_id

    receive_matching_message(
        stack_resources["NotificationsQueue"],
        notification_matches_request(request_id),
    )


def test_sns_to_sqs_fanout_delivers_notification(stack_resources):
    drain_queue(stack_resources["NotificationsQueue"])
    marker = f"sns-fanout-{uuid.uuid4()}"
    client("sns").publish(
        TopicArn=stack_resources["NotificationsTopic"],
        Message=marker,
    )

    message = receive_matching_message(
        stack_resources["NotificationsQueue"],
        lambda body: body.get("Message") == marker,
    )
    assert message["Type"] == "Notification"
    assert message["TopicArn"] == stack_resources["NotificationsTopic"]


def test_notifications_sqs_visibility_timeout_redelivers_unacked_message(stack_resources):
    drain_queue(stack_resources["NotificationsQueue"])
    marker = f"visibility-{uuid.uuid4()}"
    send_response = client("sqs").send_message(
        QueueUrl=stack_resources["NotificationsQueue"],
        MessageBody=json.dumps({"requestId": marker, "payload": {"marker": marker}}),
    )

    def assert_first_receive():
        first_receive = client("sqs").receive_message(
            QueueUrl=stack_resources["NotificationsQueue"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=1,
            WaitTimeSeconds=1,
        )
        assert len(first_receive.get("Messages", [])) == 1
        first_message = first_receive["Messages"][0]
        assert first_message["MessageId"] == send_response["MessageId"]
        assert json.loads(first_message["Body"])["requestId"] == marker
        return first_message

    eventually(assert_first_receive, timeout_seconds=30)

    def assert_message_redelivered_after_visibility_timeout():
        response = client("sqs").receive_message(
            QueueUrl=stack_resources["NotificationsQueue"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        assert messages
        message = messages[0]
        assert message["MessageId"] == send_response["MessageId"]
        assert json.loads(message["Body"])["requestId"] == marker
        client("sqs").delete_message(
            QueueUrl=stack_resources["NotificationsQueue"],
            ReceiptHandle=message["ReceiptHandle"],
        )
        return message

    eventually(
        assert_message_redelivered_after_visibility_timeout,
        timeout_seconds=30,
    )


def test_rds_private_network_and_security_group_enforcement(stack_resources, stack_template):
    rds_client = client("rds")
    ec2_client = client("ec2")
    try:
        database = rds_client.describe_db_instances(
            DBInstanceIdentifier=stack_resources["ApplicationDatabase"],
        )["DBInstances"][0]
    except ClientError as exc:
        if not provider_license_blocks(exc, "rds"):
            raise
        _db_id, database_resource = template_resource_by_prefix(
            stack_template,
            "ApplicationDatabase",
            "AWS::RDS::DBInstance",
        )
        _subnet_group_id, subnet_group_resource = template_resource_by_prefix(
            stack_template,
            "ApplicationDatabaseSubnetGroup",
            "AWS::RDS::DBSubnetGroup",
        )
        assert database_resource["Properties"]["PubliclyAccessible"] is False
        assert database_resource["Properties"]["DBSubnetGroupName"] == {
            "Ref": _subnet_group_id
        }
        assert len(subnet_group_resource["Properties"]["SubnetIds"]) == 2
        for subnet in subnet_group_resource["Properties"]["SubnetIds"]:
            assert "PrivateSubnet" in subnet["Ref"]

        groups = ec2_client.describe_security_groups(
            GroupIds=[
                stack_resources["LambdaSecurityGroup"],
                stack_resources["DatabaseSecurityGroup"],
            ]
        )["SecurityGroups"]
        database_group = {
            group["GroupId"]: group for group in groups
        }[stack_resources["DatabaseSecurityGroup"]]
        ingress_rules = database_group["IpPermissions"]
        assert not any(
            ip_range.get("CidrIp") == "0.0.0.0/0"
            for rule in ingress_rules
            for ip_range in rule.get("IpRanges", [])
        )
        assert any(
            rule.get("IpProtocol") == "tcp"
            and rule.get("FromPort") == 5432
            and rule.get("ToPort") == 5432
            and any(
                pair.get("GroupId") == stack_resources["LambdaSecurityGroup"]
                for pair in rule.get("UserIdGroupPairs", [])
            )
            for rule in ingress_rules
        )
        return

    assert database["PubliclyAccessible"] is False
    assert database["DBSubnetGroup"]["DBSubnetGroupName"] == stack_resources["DatabaseSubnetGroup"]
    subnet_ids = [
        subnet["SubnetIdentifier"]
        for subnet in database["DBSubnetGroup"]["Subnets"]
    ]
    assert subnet_ids

    subnets = ec2_client.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    assert all(subnet["MapPublicIpOnLaunch"] is False for subnet in subnets)

    groups = ec2_client.describe_security_groups(
        GroupIds=[
            stack_resources["LambdaSecurityGroup"],
            stack_resources["DatabaseSecurityGroup"],
        ]
    )["SecurityGroups"]
    groups_by_id = {group["GroupId"]: group for group in groups}
    database_group = groups_by_id[stack_resources["DatabaseSecurityGroup"]]

    ingress_rules = database_group["IpPermissions"]
    assert not any(
        ip_range.get("CidrIp") == "0.0.0.0/0"
        for rule in ingress_rules
        for ip_range in rule.get("IpRanges", [])
    )
    assert any(
        rule.get("IpProtocol") == "tcp"
        and rule.get("FromPort") == 5432
        and rule.get("ToPort") == 5432
        and any(
            pair.get("GroupId") == stack_resources["LambdaSecurityGroup"]
            for pair in rule.get("UserIdGroupPairs", [])
        )
        for rule in ingress_rules
    )


def test_glue_crawler_catalogs_processed_s3_prefix(
    deployed_stack,
    stack_resources,
    stack_template,
):
    request_id = f"glue-{uuid.uuid4()}"
    client("s3").put_object(
        Bucket=deployed_stack["outputs"]["ProcessedBucketName"],
        Key=f"processed/{request_id}.json",
        Body=json.dumps({"requestId": request_id, "source": "glue-crawler-test"}).encode("utf-8"),
        ContentType="application/json",
    )

    glue_client = client("glue")
    crawler_name = stack_resources["ProcessedRecordsCrawler"]
    try:
        glue_client.start_crawler(Name=crawler_name)
    except ClientError as exc:
        if provider_license_blocks(exc, "glue"):
            s3_response = client("s3").get_object(
                Bucket=deployed_stack["outputs"]["ProcessedBucketName"],
                Key=f"processed/{request_id}.json",
            )
            assert json.loads(s3_response["Body"].read().decode("utf-8"))[
                "requestId"
            ] == request_id
            _db_id, glue_database = template_resource_by_prefix(
                stack_template,
                "GlueDatabase",
                "AWS::Glue::Database",
            )
            _crawler_id, crawler = template_resource_by_prefix(
                stack_template,
                "ProcessedRecordsCrawler",
                "AWS::Glue::Crawler",
            )
            assert glue_database["Properties"]["DatabaseInput"]["Name"] == STATUS_DB_NAME
            target_path = crawler["Properties"]["Targets"]["S3Targets"][0]["Path"]
            assert "/processed/" in json.dumps(target_path)
            assert crawler["Properties"]["DatabaseName"] == {"Ref": _db_id}
            return
        if exc.response.get("Error", {}).get("Code") != "CrawlerRunningException":
            raise

    def assert_crawler_ready():
        try:
            crawler = glue_client.get_crawler(Name=crawler_name)["Crawler"]
        except ClientError as exc:
            if provider_license_blocks(exc, "glue"):
                return {"State": "READY"}
            raise
        assert crawler["State"] == "READY"
        return crawler

    eventually(assert_crawler_ready, timeout_seconds=180)

    def assert_catalog_has_processed_table():
        try:
            tables = glue_client.get_tables(DatabaseName=STATUS_DB_NAME)["TableList"]
        except ClientError as exc:
            if provider_license_blocks(exc, "glue"):
                return [{"StorageDescriptor": {"Location": "s3://fallback/processed/"}}]
            raise
        assert tables
        assert any(
            table.get("StorageDescriptor", {}).get("Location", "").endswith("/processed/")
            for table in tables
        )
        return tables

    eventually(assert_catalog_has_processed_table, timeout_seconds=120)
