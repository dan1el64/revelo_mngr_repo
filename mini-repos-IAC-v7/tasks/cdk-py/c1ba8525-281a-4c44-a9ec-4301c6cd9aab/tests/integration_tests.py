import json
import os
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import boto3
import pytest
from botocore.config import Config


STACK_NAME = os.environ.get("INTEGRATION_STACK_NAME") or os.environ.get("CDK_STACK_NAME") or "PocStack"
STAGE_NAME = os.environ.get("API_STAGE_NAME", "prod")

REQUIRED_OUTPUTS = {
    "ApiUrl",
    "RestApiId",
    "QueueUrl",
    "AnalyticsBucketName",
    "RuntimeStoreKey",
    "DatabaseEndpointAddress",
    "DatabaseInstanceIdentifier",
    "BackendLambdaName",
    "EventProcessorLambdaName",
    "BackendLogGroupName",
    "EventProcessorLogGroupName",
}


def _region() -> str:
    return os.environ.get("AWS_REGION") or os.environ.get("aws_region") or "us-east-1"


def _endpoint_url() -> str | None:
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT")


def _client(service_name: str):
    kwargs = {"region_name": _region()}
    endpoint_url = _endpoint_url()
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if service_name == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service_name, **kwargs)


def _uses_postgres(outputs: dict[str, str]) -> bool:
    return (
        outputs["DatabaseEndpointAddress"] != "unknown"
        and outputs["DatabaseInstanceIdentifier"] != "unknown"
    )


def _stack_outputs() -> dict[str, str]:
    response = _client("cloudformation").describe_stacks(StackName=STACK_NAME)
    stacks = response.get("Stacks", [])
    assert len(stacks) == 1, f"Expected exactly one deployed stack named {STACK_NAME}"
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stacks[0].get("Outputs", [])}
    missing = REQUIRED_OUTPUTS - outputs.keys()
    assert not missing, f"Missing deployed stack outputs: {sorted(missing)}"
    outputs["StorageMode"] = "postgresql" if _uses_postgres(outputs) else "s3-compat"
    return outputs


def _api_base_url(outputs: dict[str, str]) -> str:
    endpoint_url = _endpoint_url()
    if endpoint_url:
        return f"{endpoint_url.rstrip('/')}/restapis/{outputs['RestApiId']}/{STAGE_NAME}/_user_request_/"
    return outputs["ApiUrl"].rstrip("/") + "/"


@pytest.fixture(scope="module")
def deployed_stack() -> dict[str, str]:
    outputs = _stack_outputs()
    outputs["ResolvedApiBaseUrl"] = _api_base_url(outputs)
    return outputs


def _http_json(
    method: str,
    base_url: str,
    path: str,
    *,
    payload: dict | None = None,
    raw_body: str | None = None,
) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if raw_body is not None:
        data = raw_body.encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        urljoin(base_url, path.lstrip("/")),
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urlopen(request, timeout=20) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except HTTPError as error:
        status = error.code
        body = error.read().decode("utf-8")

    if not body:
        return status, {}
    try:
        return status, json.loads(body)
    except json.JSONDecodeError:
        return status, body


def _eventually(assertion, *, timeout_seconds: int = 120, interval_seconds: int = 3):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            return assertion()
        except AssertionError as error:
            last_error = error
            time.sleep(interval_seconds)
    if last_error:
        raise last_error
    raise AssertionError("Condition was not satisfied before the timeout")


def _log_messages(log_group_name: str, start_time_ms: int) -> list[str]:
    messages: list[str] = []
    paginator = _client("logs").get_paginator("filter_log_events")
    for page in paginator.paginate(logGroupName=log_group_name, startTime=start_time_ms):
        messages.extend(event["message"] for event in page.get("events", []))
    return messages


def _wait_for_log(log_group_name: str, start_time_ms: int, *needles: str) -> str:
    def assert_log_present() -> str:
        messages = _log_messages(log_group_name, start_time_ms)
        for message in messages:
            if all(needle in message for needle in needles):
                return message
        raise AssertionError(
            f"Did not find log in {log_group_name} containing {needles!r}. "
            f"Recent messages: {messages[-5:]}"
        )

    return _eventually(assert_log_present)


def _queue_message_count(queue_url: str) -> int:
    response = _client("sqs").get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesDelayed",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    attributes = response.get("Attributes", {})
    return sum(int(attributes.get(name, "0")) for name in attributes)


def _wait_for_queue_empty(queue_url: str) -> None:
    def assert_empty() -> None:
        count = _queue_message_count(queue_url)
        assert count == 0, f"Expected queue to be drained, found {count} messages"

    _eventually(assert_empty, timeout_seconds=90, interval_seconds=3)


def _post_item(outputs: dict[str, str], value: str) -> dict:
    def assert_created() -> dict:
        status, body = _http_json(
            "POST",
            outputs["ResolvedApiBaseUrl"],
            "/items",
            payload={"value": value},
        )
        assert status == 201, body
        assert isinstance(body, dict), body
        assert isinstance(body.get("id"), int), body
        return body

    return _eventually(assert_created, timeout_seconds=180, interval_seconds=5)


def _s3_runtime_rows(outputs: dict[str, str]) -> list[dict]:
    response = _client("s3").get_object(
        Bucket=outputs["AnalyticsBucketName"],
        Key=outputs["RuntimeStoreKey"],
    )
    return json.loads(response["Body"].read().decode("utf-8"))


@pytest.fixture(scope="module")
def created_items(deployed_stack) -> dict:
    start_time_ms = int((time.time() - 30) * 1000)
    value_one = f"integration-{uuid.uuid4()}"
    value_two = f"integration-{uuid.uuid4()}"
    item_one = _post_item(deployed_stack, value_one)
    item_two = _post_item(deployed_stack, value_two)
    assert item_one["id"] < item_two["id"]
    return {
        "start_time_ms": start_time_ms,
        "value_one": value_one,
        "value_two": value_two,
        "item_one": item_one,
        "item_two": item_two,
    }


def test_deployed_outputs_resolve_live_resources(deployed_stack):
    lambda_client = _client("lambda")
    sqs = _client("sqs")
    s3 = _client("s3")

    backend = lambda_client.get_function_configuration(
        FunctionName=deployed_stack["BackendLambdaName"]
    )
    processor = lambda_client.get_function_configuration(
        FunctionName=deployed_stack["EventProcessorLambdaName"]
    )
    assert backend["Runtime"] == "python3.12"
    assert processor["Runtime"] == "python3.12"

    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=deployed_stack["QueueUrl"],
        AttributeNames=["QueueArn"],
    )
    assert queue_attributes["Attributes"]["QueueArn"].endswith(
        deployed_stack["QueueUrl"].rsplit("/", 1)[-1]
    )

    s3.head_bucket(Bucket=deployed_stack["AnalyticsBucketName"])


def test_deployed_database_endpoint_is_verified_when_available(deployed_stack):
    if _uses_postgres(deployed_stack):
        database = _client("rds").describe_db_instances(
            DBInstanceIdentifier=deployed_stack["DatabaseInstanceIdentifier"]
        )["DBInstances"][0]
        assert database["Engine"] == "postgres"
        assert database["Endpoint"]["Address"] == deployed_stack["DatabaseEndpointAddress"]
    else:
        assert deployed_stack["StorageMode"] == "s3-compat"


def test_get_health_returns_real_response_body(deployed_stack):
    status, body = _http_json("GET", deployed_stack["ResolvedApiBaseUrl"], "/health")
    assert status == 200
    assert body == {"ok": True}


def test_post_items_persist_and_get_items_returns_ordered_rows(deployed_stack, created_items):
    def assert_rows_persisted() -> list[dict]:
        status, rows = _http_json("GET", deployed_stack["ResolvedApiBaseUrl"], "/items")
        assert status == 200, rows
        assert isinstance(rows, list), rows
        ids = [row["id"] for row in rows]
        assert ids == sorted(ids), rows
        by_value = {row["value"]: row for row in rows}
        assert by_value[created_items["value_one"]]["id"] == created_items["item_one"]["id"]
        assert by_value[created_items["value_two"]]["id"] == created_items["item_two"]["id"]
        return rows

    _eventually(assert_rows_persisted)


def test_runtime_persistence_side_effect_is_present(deployed_stack, created_items):
    if _uses_postgres(deployed_stack):
        assert deployed_stack["StorageMode"] == "postgresql"
        return

    rows = _s3_runtime_rows(deployed_stack)
    by_value = {row["value"]: row for row in rows}
    assert by_value[created_items["value_one"]]["id"] == created_items["item_one"]["id"]
    assert by_value[created_items["value_two"]]["id"] == created_items["item_two"]["id"]


def test_backend_lambda_received_request_and_published_event(deployed_stack, created_items):
    _wait_for_log(
        deployed_stack["BackendLogGroupName"],
        created_items["start_time_ms"],
        '"path": "/items"',
    )
    _wait_for_log(
        deployed_stack["BackendLogGroupName"],
        created_items["start_time_ms"],
        '"created_item_id"',
        created_items["value_one"],
        deployed_stack["StorageMode"],
    )
    _wait_for_log(
        deployed_stack["BackendLogGroupName"],
        created_items["start_time_ms"],
        '"event_published": true',
        f'"item_id": {created_items["item_one"]["id"]}',
    )


def test_eventbridge_rule_delivers_to_sqs_and_processor_consumes(deployed_stack, created_items):
    _wait_for_log(
        deployed_stack["EventProcessorLogGroupName"],
        created_items["start_time_ms"],
        '"processor_status": "processed"',
        f'"item_id": {created_items["item_one"]["id"]}',
    )
    _wait_for_log(
        deployed_stack["EventProcessorLogGroupName"],
        created_items["start_time_ms"],
        '"processor_status": "processed"',
        f'"item_id": {created_items["item_two"]["id"]}',
    )
    _wait_for_queue_empty(deployed_stack["QueueUrl"])


def test_invalid_post_body_returns_expected_error(deployed_stack):
    status, body = _http_json(
        "POST",
        deployed_stack["ResolvedApiBaseUrl"],
        "/items",
        payload={"unexpected": "shape"},
    )
    assert status == 400
    assert body == {"error": "invalid_value"}


def test_malformed_json_post_returns_expected_error(deployed_stack):
    status, body = _http_json(
        "POST",
        deployed_stack["ResolvedApiBaseUrl"],
        "/items",
        raw_body="{not-json",
    )
    assert status == 400
    assert body == {"error": "invalid_json"}


def test_missing_route_returns_runtime_error_status(deployed_stack):
    status, body = _http_json("GET", deployed_stack["ResolvedApiBaseUrl"], "/missing-route")
    assert status in {403, 404}
    assert body


def test_invalid_method_returns_runtime_error_status(deployed_stack):
    status, body = _http_json("DELETE", deployed_stack["ResolvedApiBaseUrl"], "/items")
    assert status in {403, 404}
    assert body


def test_processor_surfaces_malformed_queue_message(deployed_stack):
    start_time_ms = int((time.time() - 30) * 1000)
    trace_id = f"bad-message-{uuid.uuid4()}"
    _client("sqs").send_message(
        QueueUrl=deployed_stack["QueueUrl"],
        MessageBody=json.dumps({"trace_id": trace_id, "not_detail": True}),
    )

    _wait_for_log(
        deployed_stack["EventProcessorLogGroupName"],
        start_time_ms,
        '"processor_status": "failed"',
        trace_id,
    )
    _wait_for_queue_empty(deployed_stack["QueueUrl"])
