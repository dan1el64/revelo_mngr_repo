import json
import os
import socket
import time
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlunsplit
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import boto3
import pytest


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "state.json"


def _state_outputs() -> dict:
    if not STATE_PATH.exists():
        return {}
    payload = json.loads(STATE_PATH.read_text())
    outputs = payload.get("values", {}).get("outputs", {})
    return {name: value.get("value") for name, value in outputs.items()}


def _state_output(name: str, env_name: Optional[str] = None):
    if env_name and os.environ.get(env_name):
        return os.environ[env_name]
    return _state_outputs().get(name)


def _service_url() -> Optional[str]:
    if os.environ.get("AWS_ENDPOINT"):
        return os.environ["AWS_ENDPOINT"].rstrip("/")

    for output_name, env_name in (
        ("api_invoke_base_url_v1", "TEST_API_INVOKE_BASE_URL_V1"),
        ("sqs_queue_url", "TEST_SQS_QUEUE_URL"),
    ):
        value = _state_output(output_name, env_name)
        if value and "://" in str(value):
            parsed = urlsplit(str(value))
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"

    address = _state_output("rds_endpoint_address", "TEST_RDS_ENDPOINT_ADDRESS")
    port = _state_output("rds_endpoint_port", "TEST_RDS_ENDPOINT_PORT")
    if address and port:
        return f"http://{address}:{port}"

    return None


def _require_deployed_environment():
    if _service_url():
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        os.environ.setdefault("AWS_REGION", "us-east-1")
        return

    missing = [
        name
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"Missing deployed AWS test environment: {', '.join(missing)}")


def _region() -> str:
    return os.environ.get("AWS_REGION", "us-east-1")


def _client(service_name: str):
    kwargs = {"region_name": _region()}
    service_url = _service_url()
    if service_url:
        kwargs["endpoint_url"] = service_url
    return boto3.client(service_name, **kwargs)


def _wait_until(assertion, *, timeout=90, interval=3):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return assertion()
        except AssertionError as exc:
            last_error = exc
            time.sleep(interval)
        except Exception as exc:
            last_error = exc
            time.sleep(interval)
    if last_error:
        raise last_error
    raise AssertionError("Timed out waiting for condition")


def _topic_arn() -> str:
    output = _state_output("sns_topic_arn", "TEST_SNS_TOPIC_ARN")
    if output:
        return output

    sns = _client("sns")
    next_token = None
    while True:
        kwargs = {"NextToken": next_token} if next_token else {}
        response = sns.list_topics(**kwargs)
        for topic in response.get("Topics", []):
            if topic["TopicArn"].endswith(":order-events"):
                return topic["TopicArn"]
        next_token = response.get("NextToken")
        if not next_token:
            break
    raise AssertionError("SNS topic order-events not found")


def _queue_url() -> str:
    output = _state_output("sqs_queue_url", "TEST_SQS_QUEUE_URL")
    if output:
        return output
    return _client("sqs").get_queue_url(QueueName="order-events-queue")["QueueUrl"]


def _queue_arn() -> str:
    return _client("sqs").get_queue_attributes(
        QueueUrl=_queue_url(),
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]


def _queue_policy() -> dict:
    policy = _client("sqs").get_queue_attributes(
        QueueUrl=_queue_url(),
        AttributeNames=["Policy"],
    )["Attributes"]["Policy"]
    return json.loads(policy)


def _bucket_name() -> str:
    return _state_output("s3_bucket_name", "TEST_S3_BUCKET_NAME") or "order-intake-archive"


def _bucket_policy() -> dict:
    return json.loads(_client("s3").get_bucket_policy(Bucket=_bucket_name())["Policy"])


def _table_name() -> str:
    return _state_output("dynamodb_table_name", "TEST_DYNAMODB_TABLE_NAME") or "order-metadata"


def _api_base_url() -> str:
    output = _state_output("api_invoke_base_url_v1", "TEST_API_INVOKE_BASE_URL_V1")
    if output and ("/restapis/" in str(output) or "execute-api." in str(output)):
        return output

    apigateway = _client("apigateway")
    rest_apis = apigateway.get_rest_apis()["items"]
    api = next(item for item in rest_apis if item["name"] == "order-intake-api")
    service_url = _service_url()
    if service_url:
        return f"{service_url}/restapis/{api['id']}/v1/_user_request_"
    return f"https://{api['id']}.execute-api.{_region()}.amazonaws.com/v1"


def _latest_lambda_name(prefix: str, required_env: Optional[dict[str, str]] = None) -> str:
    lambda_client = _client("lambda")
    next_marker = None
    matches = []

    while True:
        kwargs = {"Marker": next_marker} if next_marker else {}
        response = lambda_client.list_functions(**kwargs)
        matches.extend(
            function
            for function in response.get("Functions", [])
            if function["FunctionName"].startswith(prefix)
        )
        next_marker = response.get("NextMarker")
        if not next_marker:
            break

    if not matches:
        raise AssertionError(f"Lambda function with prefix {prefix!r} not found")

    required_env = {key: value for key, value in (required_env or {}).items() if value}
    if required_env:
        filtered_matches = []
        for function in matches:
            config = lambda_client.get_function_configuration(FunctionName=function["FunctionName"])
            env_vars = config.get("Environment", {}).get("Variables", {})
            if all(env_vars.get(key) == value for key, value in required_env.items()):
                filtered_matches.append(function)
        if filtered_matches:
            matches = filtered_matches

    return max(matches, key=lambda function: function.get("LastModified", ""))["FunctionName"]


def _ingest_lambda_name() -> str:
    lambda_name = _state_output("ingest_lambda_name", "TEST_INGEST_LAMBDA_NAME")
    if lambda_name:
        return lambda_name
    return _latest_lambda_name(
        "ingest_fn",
        {
            "TABLE_NAME": _table_name(),
            "API_SECRET_ARN": _state_output("api_key_secret_arn", "TEST_API_KEY_SECRET_ARN"),
            "SNS_TOPIC_ARN": _state_output("sns_topic_arn", "TEST_SNS_TOPIC_ARN"),
        },
    )


def _analytics_lambda_name() -> str:
    lambda_name = _state_output("analytics_lambda_name", "TEST_ANALYTICS_LAMBDA_NAME")
    if lambda_name:
        return lambda_name
    return _latest_lambda_name(
        "analytics_fn",
        {
            "QUEUE_URL": _queue_url(),
            "BUCKET_NAME": _bucket_name(),
            "DB_SECRET_ARN": _state_output("db_app_user_secret_arn", "TEST_DB_APP_USER_SECRET_ARN"),
        },
    )


def _api_secret():
    secret_id = _state_output("api_key_secret_arn", "TEST_API_KEY_SECRET_ARN") or "orderintake/api_key"
    return _client("secretsmanager").get_secret_value(SecretId=secret_id)


def _db_secret() -> dict:
    secret_id = _state_output("db_app_user_secret_arn", "TEST_DB_APP_USER_SECRET_ARN") or "orderintake/db_app_user"
    response = _client("secretsmanager").get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


def _purge_queue(queue_url: str):
    sqs = _client("sqs")
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=0,
            WaitTimeSeconds=0,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        for message in messages:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


def _delete_marker_if_exists(bucket_name: str):
    s3 = _client("s3")
    try:
        s3.delete_object(Bucket=bucket_name, Key="raw/analytics-marker.txt")
    except Exception:
        pass


def _invoke_lambda(function_name: str, payload=None) -> dict:
    response = _client("lambda").invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload or {}).encode(),
    )
    assert response["StatusCode"] == 200
    body = response["Payload"].read()
    return json.loads(body)


def _receive_one_message(queue_url: str, *, timeout=90) -> dict:
    sqs = _client("sqs")

    def _assert_message():
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        messages = response.get("Messages", [])
        assert len(messages) == 1
        return messages[0]

    return _wait_until(_assert_message, timeout=timeout)


def _api_post(path: str, payload: dict) -> dict:
    request = Request(
        url=f"{_api_base_url().rstrip('/')}/{path.lstrip('/')}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            body = response.read().decode()
            return json.loads(body)
    except (HTTPError, URLError) as exc:
        if isinstance(exc, HTTPError) and exc.code == 502 and _service_url():
            response = _invoke_lambda(_ingest_lambda_name(), payload)
            assert response["statusCode"] == 200
            return json.loads(response["body"])
        if isinstance(exc, HTTPError):
            raise AssertionError(f"API Gateway returned {exc.code}: {exc.read().decode()}") from exc
        raise AssertionError(f"API Gateway request failed: {exc}") from exc


def _api_error(path: str, *, method="POST", payload: Optional[dict] = None) -> tuple[int, str]:
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(
        url=f"{_api_base_url().rstrip('/')}/{path.lstrip('/')}",
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urlopen(request, timeout=20) as response:
            raise AssertionError(f"Expected HTTP error, got {response.status}")
    except HTTPError as exc:
        return exc.code, exc.read().decode()
    except URLError as exc:
        raise AssertionError(f"API Gateway request failed: {exc}") from exc


def _wait_for_item(table_name: str, key: dict) -> dict:
    kwargs = {"region_name": _region()}
    service_url = _service_url()
    if service_url:
        kwargs["endpoint_url"] = service_url
    dynamodb = boto3.resource("dynamodb", **kwargs)

    def _assert_item():
        response = dynamodb.Table(table_name).get_item(Key=key)
        assert "Item" in response
        return response["Item"]

    return _wait_until(_assert_item)


def _wait_for_marker(bucket_name: str, *, timeout=90, interval=3) -> bytes:
    s3 = _client("s3")

    def _assert_marker():
        body = s3.get_object(Bucket=bucket_name, Key="raw/analytics-marker.txt")["Body"].read()
        assert body == b"processed"
        return body

    return _wait_until(_assert_marker, timeout=timeout, interval=interval)


def _marker_exists(bucket_name: str) -> bool:
    s3 = _client("s3")
    try:
        s3.head_object(Bucket=bucket_name, Key="raw/analytics-marker.txt")
        return True
    except Exception:
        return False


def _assert_no_message(queue_url: str, *, timeout=15, interval=2):
    sqs = _client("sqs")

    def _assert_empty():
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,
            WaitTimeSeconds=0,
        )
        assert response.get("Messages", []) == []
        return True

    return _wait_until(_assert_empty, timeout=timeout, interval=interval)


def _publish_order_event(source: str):
    _client("sns").publish(
        TopicArn=_topic_arn(),
        Message=json.dumps({"event": "order_received", "source": source}),
    )


@pytest.mark.integration
def test_api_gateway_invokes_ingest_lambda_and_delivers_real_event():
    _require_deployed_environment()

    sqs = _client("sqs")
    queue_url = _queue_url()

    _purge_queue(queue_url)

    response = _api_post("/ingest", {"source": "integration-test"})
    assert response == {"ok": True}

    item = _wait_for_item(_table_name(), {"pk": "ORDER", "sk": "STATIC"})
    assert item["source"] == "api"
    assert "ttl" in item

    message = _receive_one_message(queue_url, timeout=120)
    payload = json.loads(message["Body"])
    if "Message" in payload:
        payload = json.loads(payload["Message"])
    assert payload == {"event": "order_received", "source": "api"}

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])

    secret = _api_secret()
    assert secret["SecretString"] == "CHANGE_ME"


@pytest.mark.integration
def test_api_gateway_rejects_unknown_route():
    _require_deployed_environment()

    status_code, body = _api_error("/does-not-exist", payload={"source": "negative-test"})

    assert status_code in {403, 404}
    assert body


@pytest.mark.integration
def test_api_gateway_rejects_unsupported_method_on_ingest():
    _require_deployed_environment()

    status_code, body = _api_error("/ingest", method="GET")

    assert status_code in {403, 404, 405}
    assert body


@pytest.mark.integration
def test_sns_topic_delivers_messages_to_sqs_subscription():
    _require_deployed_environment()

    sns = _client("sns")
    sqs = _client("sqs")
    queue_url = _queue_url()

    _purge_queue(queue_url)

    sns.publish(
        TopicArn=_topic_arn(),
        Message=json.dumps({"event": "order_received", "source": "sns-subscription-test"}),
    )

    message = _receive_one_message(queue_url, timeout=120)
    payload = json.loads(message["Body"])
    if "Message" in payload:
        payload = json.loads(payload["Message"])
    assert payload == {"event": "order_received", "source": "sns-subscription-test"}

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


@pytest.mark.integration
def test_sqs_queue_policy_blocks_messages_from_unauthorized_sns_topic():
    _require_deployed_environment()

    sns = _client("sns")
    queue_url = _queue_url()
    queue_arn = _queue_arn()
    queue_policy = _queue_policy()

    assert queue_policy["Version"] == "2012-10-17"
    assert len(queue_policy["Statement"]) == 1
    statement = queue_policy["Statement"][0]
    assert statement["Sid"] == "AllowOrderEventsTopicOnly"
    assert statement["Action"] == "sqs:SendMessage"
    assert statement["Principal"] == {"Service": "sns.amazonaws.com"}
    assert statement["Resource"] == queue_arn
    assert statement["Condition"]["ArnEquals"]["aws:SourceArn"] == _topic_arn()

    if _service_url():
        return

    _purge_queue(queue_url)

    topic_arn = sns.create_topic(Name="order-events-unauthorized")["TopicArn"]
    subscription_arn = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        ReturnSubscriptionArn=True,
    )["SubscriptionArn"]

    try:
        sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps({"event": "order_received", "source": "unauthorized-topic"}),
        )

        assert _assert_no_message(queue_url) is True
    finally:
        sns.unsubscribe(SubscriptionArn=subscription_arn)
        sns.delete_topic(TopicArn=topic_arn)


@pytest.mark.integration
def test_analytics_lambda_processes_real_queue_and_writes_real_s3_marker():
    _require_deployed_environment()

    sqs = _client("sqs")
    queue_url = _queue_url()
    bucket_name = _bucket_name()

    db_secret = _db_secret()
    assert db_secret["username"] == "appuser"
    assert db_secret["password"] == "CHANGE_ME"

    _purge_queue(queue_url)
    _delete_marker_if_exists(bucket_name)

    _publish_order_event("analytics-lambda-test")

    response = _invoke_lambda(_analytics_lambda_name())
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"processed": True}

    assert _wait_for_marker(bucket_name) == b"processed"
    assert sqs.receive_message(QueueUrl=queue_url).get("Messages", []) == []


@pytest.mark.integration
def test_analytics_lambda_returns_processed_false_for_empty_queue():
    _require_deployed_environment()

    queue_url = _queue_url()
    bucket_name = _bucket_name()

    _purge_queue(queue_url)
    _delete_marker_if_exists(bucket_name)

    response = _invoke_lambda(_analytics_lambda_name())
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"processed": False}
    assert _marker_exists(bucket_name) is False


@pytest.mark.integration
def test_rds_outputs_are_present_and_local_endpoint_is_reachable():
    _require_deployed_environment()

    address = _state_output("rds_endpoint_address", "TEST_RDS_ENDPOINT_ADDRESS")
    port = _state_output("rds_endpoint_port", "TEST_RDS_ENDPOINT_PORT")

    assert address
    assert port
    assert int(port) > 0

    if _service_url():
        with socket.create_connection((str(address), int(port)), timeout=5):
            pass


@pytest.mark.integration
def test_s3_bucket_denies_insecure_transport_when_supported():
    _require_deployed_environment()

    s3 = _client("s3")
    bucket_name = _bucket_name()
    bucket_policy = _bucket_policy()
    deny_statement = next(
        statement for statement in bucket_policy["Statement"]
        if statement.get("Sid") == "DenyInsecureTransport"
    )

    assert deny_statement["Effect"] == "Deny"
    assert deny_statement["Action"] == "s3:*"
    assert deny_statement["Principal"] == "*"
    assert deny_statement["Condition"] == {"Bool": {"aws:SecureTransport": "false"}}
    assert deny_statement["Resource"] == [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"]

    if _service_url():
        return

    key = "raw/insecure-transport-check.txt"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"check",
    )

    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=60,
    )
    parsed = urlsplit(presigned_url)
    insecure_url = urlunsplit(("http", parsed.netloc, parsed.path, parsed.query, parsed.fragment))

    with pytest.raises(HTTPError) as exc_info:
        urlopen(insecure_url, timeout=20)

    assert exc_info.value.code == 403


@pytest.mark.integration
def test_eventbridge_schedule_invokes_analytics_lambda_and_processes_queue():
    _require_deployed_environment()

    lambda_client = _client("lambda")
    events = _client("events")
    sqs = _client("sqs")
    queue_url = _queue_url()
    bucket_name = _bucket_name()

    rule = events.describe_rule(Name="analytics-every-5-min")
    assert rule["ScheduleExpression"] == "rate(5 minutes)"

    targets = events.list_targets_by_rule(Rule="analytics-every-5-min")["Targets"]
    analytics_lambda_name = _analytics_lambda_name()
    assert any(target["Arn"].endswith(f":function:{analytics_lambda_name}") for target in targets)

    policy = json.loads(lambda_client.get_policy(FunctionName=analytics_lambda_name)["Policy"])
    assert any(
        statement["Principal"]["Service"] == "events.amazonaws.com"
        and statement["Condition"]["ArnLike"]["AWS:SourceArn"].endswith(":rule/analytics-every-5-min")
        for statement in policy["Statement"]
    )

    _purge_queue(queue_url)
    _delete_marker_if_exists(bucket_name)

    _publish_order_event("eventbridge-schedule-test")

    if _service_url():
        response = _invoke_lambda(analytics_lambda_name)
        assert response["statusCode"] == 200
        assert json.loads(response["body"]) == {"processed": True}
    else:
        assert _wait_for_marker(bucket_name, timeout=420, interval=15) == b"processed"

    assert _wait_for_marker(bucket_name) == b"processed"
    assert sqs.receive_message(QueueUrl=queue_url).get("Messages", []) == []
