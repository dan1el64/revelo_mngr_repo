import json
import os
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError, NoCredentialsError


STACK_NAME = "EventDrivenIngestionStack"
INGEST_FUNCTION_NAME = "event-ingestion-ingest"
WORKER_FUNCTION_NAME = "event-ingestion-worker"
ENRICHER_FUNCTION_NAME = "event-ingestion-enricher"


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


def eventually(assertion, timeout_seconds=45, interval_seconds=1):
    deadline = time.time() + timeout_seconds
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
    summaries = cloudformation.describe_stack_resources(
        StackName=deployed_stack["stack"]["StackName"],
    )["StackResources"]

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
        "AuditTable": physical_id("AuditTable", "AWS::DynamoDB::Table"),
        "StatusTable": physical_id("StatusTable", "AWS::DynamoDB::Table"),
        "ProcessingStateMachine": physical_id(
            "ProcessingStateMachine",
            "AWS::StepFunctions::StateMachine",
        ),
        "NotificationsQueue": physical_id("NotificationsQueue", "AWS::SQS::Queue"),
    }


def test_deployed_stack_outputs_are_reachable_through_aws_apis(deployed_stack, stack_resources):
    outputs = deployed_stack["outputs"]

    assert set(outputs) >= {
        "ApiInvokeUrl",
        "EventBusName",
        "IngestionQueueUrl",
        "ProcessedBucketName",
    }

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
    ingest_lambda = client("lambda").get_function(FunctionName=INGEST_FUNCTION_NAME)
    worker_lambda = client("lambda").get_function(FunctionName=WORKER_FUNCTION_NAME)
    enricher_lambda = client("lambda").get_function(FunctionName=ENRICHER_FUNCTION_NAME)

    assert "IngestionQueue" in queue_attributes["QueueArn"]
    assert bucket_versioning["Status"] == "Enabled"
    assert event_bus["Name"] == outputs["EventBusName"]
    assert audit_table["TableStatus"] in {"ACTIVE", "CREATING"}
    assert status_table["TableStatus"] in {"ACTIVE", "CREATING"}
    assert ingest_lambda["Configuration"]["State"] in {"Active", "Pending"}
    assert worker_lambda["Configuration"]["State"] in {"Active", "Pending"}
    assert enricher_lambda["Configuration"]["State"] in {"Active", "Pending"}


def test_ingest_lambda_writes_an_audit_record_through_sqs_and_dynamodb(stack_resources):
    payload = {
        "body": json.dumps(
            {
                "source": "integration-test",
                "correlationId": f"ingest-{uuid.uuid4()}",
                "records": [{"id": 1, "value": "accepted"}],
            }
        )
    }

    response = client("lambda").invoke(
        FunctionName=INGEST_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    lambda_payload = json.loads(response["Payload"].read().decode("utf-8"))
    body = json.loads(lambda_payload["body"])
    request_id = body["requestId"]
    audit_table = resource("dynamodb").Table(stack_resources["AuditTable"])

    assert response["StatusCode"] == 200
    assert "FunctionError" not in response
    assert lambda_payload["statusCode"] == 202

    def assert_audit_item_exists():
        item = audit_table.get_item(Key={"pk": request_id}).get("Item")
        assert item is not None
        assert item["pk"] == request_id
        assert item["status"] == "RECEIVED"
        assert int(item["ttl"]) > int(time.time())
        return item

    eventually(assert_audit_item_exists)


def test_worker_lambda_persists_processed_payload_and_updates_audit_table(
    deployed_stack,
    stack_resources,
):
    request_id = f"worker-{uuid.uuid4()}"
    payload = {
        "source": "integration-test",
        "requestId": request_id,
        "records": [{"id": 42, "value": "processed"}],
    }
    audit_table = resource("dynamodb").Table(stack_resources["AuditTable"])
    audit_table.put_item(
        Item={
            "pk": request_id,
            "ttl": int(time.time()) + 600,
            "status": "SEEDED",
        }
    )

    response = client("lambda").invoke(
        FunctionName=WORKER_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {
                "Records": [
                    {
                        "body": json.dumps(
                            {
                                "requestId": request_id,
                                "payload": payload,
                            }
                        )
                    }
                ]
            }
        ).encode("utf-8"),
    )
    worker_result = json.loads(response["Payload"].read().decode("utf-8"))

    assert response["StatusCode"] == 200
    assert "FunctionError" not in response
    assert worker_result == {"batchItemFailures": []}

    processed_key = f"processed/{request_id}.json"
    s3_client = client("s3")

    def assert_processed_object_exists():
        s3_response = s3_client.get_object(
            Bucket=deployed_stack["outputs"]["ProcessedBucketName"],
            Key=processed_key,
        )
        processed_record = json.loads(s3_response["Body"].read().decode("utf-8"))
        assert processed_record["requestId"] == request_id
        assert processed_record["payload"] == payload
        assert processed_record["audit"]["status"] == "SEEDED"
        assert processed_record["audit"]["pk"] == request_id
        return processed_record

    def assert_audit_table_was_updated():
        item = audit_table.get_item(Key={"pk": request_id}).get("Item")
        assert item is not None
        assert item["objectKey"] == processed_key
        assert "processedAt" in item
        return item

    eventually(assert_processed_object_exists)
    eventually(assert_audit_table_was_updated)
