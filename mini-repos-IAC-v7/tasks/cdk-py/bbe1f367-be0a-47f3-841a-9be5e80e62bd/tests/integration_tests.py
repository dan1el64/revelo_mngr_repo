"""Integration tests for the SecurityBaseline CDK stack.

These tests use boto3 to cross real AWS service boundaries and verify
terminal pipeline outcomes against a deployed stack.

Pipeline under test:
    POST /ingest  ->  IngestHandler Lambda  ->  SQS IngestQueue
    SQS  ->  EventBridge Pipe (enrichment: IngestHandler)  ->  SFN WorkflowStateMachine
    SFN  ->  WorkflowWorker Lambda  ->  SecretsManager
"""

import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import boto3
import pytest


# ---------------------------------------------------------------------------
# Load app module for coverage (both branches of build_lambda_environment)
# ---------------------------------------------------------------------------


def _load_app_module():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    spec = importlib.util.spec_from_file_location("app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules["app"] = module
    spec.loader.exec_module(module)
    return module


_app = _load_app_module()


# ---------------------------------------------------------------------------
# CDK synthesis smoke tests — cover app.py code paths, not template shape
# ---------------------------------------------------------------------------


def test_stack_synthesizes_with_aws_endpoint_env_var_set(monkeypatch):
    """Stack must instantiate without errors when AWS_ENDPOINT is configured."""
    import aws_cdk as cdk

    monkeypatch.setenv("AWS_ENDPOINT", "https://test.internal")
    cdk_app = cdk.App()
    stack = _app.SecurityBaselineStack(cdk_app, "SecurityBaselineStack")
    assert stack is not None


def test_stack_synthesizes_with_aws_endpoint_env_var_unset(monkeypatch):
    """Stack must instantiate without errors when AWS_ENDPOINT is absent."""
    import aws_cdk as cdk

    monkeypatch.delenv("AWS_ENDPOINT", raising=False)
    cdk_app = cdk.App()
    stack = _app.SecurityBaselineStack(cdk_app, "SecurityBaselineStack2")
    assert stack is not None


# ---------------------------------------------------------------------------
# Boto3 helpers
# ---------------------------------------------------------------------------

AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
STACK_NAME = "SecurityBaselineStack"


def _client(service: str):
    if not AWS_ENDPOINT:
        pytest.skip("AWS_ENDPOINT is not set; skipping live service test")
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


# ---------------------------------------------------------------------------
# Stack resource discovery
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def stack_resources() -> dict[str, dict]:
    """Return {LogicalId: {physical_id, type}} for every resource in the stack."""
    cfn = _client("cloudformation")
    resources: dict[str, dict] = {}
    paginator = cfn.get_paginator("list_stack_resources")
    for page in paginator.paginate(StackName=STACK_NAME):
        for r in page["StackResourceSummaries"]:
            resources[r["LogicalResourceId"]] = {
                "physical_id": r["PhysicalResourceId"],
                "type": r["ResourceType"],
            }
    return resources


def _find_physical_id(
    resources: dict[str, dict], resource_type: str, logical_prefix: str
) -> str:
    """Find a physical resource ID by CloudFormation type and logical ID prefix."""
    for lid, r in resources.items():
        if r["type"] == resource_type and lid.startswith(logical_prefix):
            return r["physical_id"]
    raise AssertionError(
        f"no {resource_type} with logical ID prefix {logical_prefix!r} found in stack"
    )


@pytest.fixture(scope="session")
def ingest_function_name(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(stack_resources, "AWS::Lambda::Function", "IngestHandler")


@pytest.fixture(scope="session")
def workflow_function_name(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(stack_resources, "AWS::Lambda::Function", "WorkflowWorker")


@pytest.fixture(scope="session")
def queue_url(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(stack_resources, "AWS::SQS::Queue", "IngestQueue")


@pytest.fixture(scope="session")
def state_machine_arn(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(
        stack_resources, "AWS::StepFunctions::StateMachine", "WorkflowStateMachine"
    )


@pytest.fixture(scope="session")
def secret_arn(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(
        stack_resources, "AWS::SecretsManager::Secret", "DatabaseSecret"
    )


@pytest.fixture(scope="session")
def crawler_bucket_name(stack_resources: dict[str, dict]) -> str:
    return _find_physical_id(stack_resources, "AWS::S3::Bucket", "CrawlerBucket")


# ---------------------------------------------------------------------------
# Ingest Lambda — API gateway path
# ---------------------------------------------------------------------------


def test_ingest_handler_api_path_returns_202_and_enqueues_payload(
    ingest_function_name: str, queue_url: str
) -> None:
    """POST /ingest flow: Lambda must return HTTP 202 and put the payload on SQS."""
    lm = _client("lambda")
    payload = {"source": "integration-test", "id": 42}
    event = {
        "requestContext": {"requestId": "int-test-001"},
        "body": json.dumps(payload),
    }

    resp = lm.invoke(
        FunctionName=ingest_function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode(),
    )
    result = json.loads(resp["Payload"].read())

    assert result["statusCode"] == 202
    assert json.loads(result["body"])["status"] == "accepted"

    sqs = _client("sqs")
    msgs = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=3,
    ).get("Messages", [])
    assert len(msgs) >= 1, "expected at least one message in IngestQueue"
    assert json.loads(msgs[0]["Body"]) == payload
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])


# ---------------------------------------------------------------------------
# Ingest Lambda — SQS enrichment path
# ---------------------------------------------------------------------------


def test_ingest_handler_enrichment_path_returns_pipe_enrichment_structure(
    ingest_function_name: str,
) -> None:
    """SQS enrichment path: Lambda must return {source: pipe-enrichment, payload: ...}."""
    lm = _client("lambda")
    inner = {"enriched": True, "data": "from-pipe"}
    event = {"Records": [{"body": json.dumps(inner)}]}

    resp = lm.invoke(
        FunctionName=ingest_function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode(),
    )
    result = json.loads(resp["Payload"].read())

    assert result["source"] == "pipe-enrichment"
    assert result["payload"] == inner


# ---------------------------------------------------------------------------
# State machine — end-to-end execution
# ---------------------------------------------------------------------------


def test_state_machine_execution_reaches_succeeded_terminal_state(
    state_machine_arn: str,
) -> None:
    """SFN pipeline: starting an execution must reach SUCCEEDED within 40 seconds."""
    sfn = _client("stepfunctions")
    exec_resp = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({"test": "integration-pipeline-run"}),
    )
    exec_arn = exec_resp["executionArn"]

    status = "RUNNING"
    desc: dict = {}
    for _ in range(20):
        desc = sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]
        if status in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}:
            break
        time.sleep(2)

    assert status == "SUCCEEDED", f"state machine ended with: {status}"
    assert json.loads(desc.get("output", "{}")).get("status") == "processed"


# ---------------------------------------------------------------------------
# WorkflowWorker Lambda — direct invocation
# ---------------------------------------------------------------------------


def test_workflow_worker_invocation_returns_processed_status(
    workflow_function_name: str,
) -> None:
    """WorkflowWorker must invoke successfully, read the secret, and return status=processed."""
    lm = _client("lambda")
    resp = lm.invoke(
        FunctionName=workflow_function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps({"task": "direct-test"}).encode(),
    )
    assert "FunctionError" not in resp
    result = json.loads(resp["Payload"].read())
    assert result.get("status") == "processed"
    assert "username" in result


# ---------------------------------------------------------------------------
# SQS queue — real attribute verification
# ---------------------------------------------------------------------------


def test_ingest_queue_has_sqs_managed_encryption_and_30s_visibility_timeout(
    queue_url: str,
) -> None:
    """SQS queue attributes: SSE enabled, visibility timeout = 30 s."""
    sqs = _client("sqs")
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "SqsManagedSseEnabled"],
    )["Attributes"]
    assert attrs.get("VisibilityTimeout") == "30"
    assert attrs.get("SqsManagedSseEnabled", "").lower() == "true"


# ---------------------------------------------------------------------------
# S3 crawler bucket — real bucket verification
# ---------------------------------------------------------------------------


def test_crawler_bucket_is_private_and_versioning_enabled(
    crawler_bucket_name: str,
) -> None:
    """Crawler S3 bucket: block-all-public-access and versioning must be active."""
    s3 = _client("s3")

    pab = s3.get_public_access_block(Bucket=crawler_bucket_name)[
        "PublicAccessBlockConfiguration"
    ]
    assert pab["BlockPublicAcls"] is True
    assert pab["BlockPublicPolicy"] is True
    assert pab["IgnorePublicAcls"] is True
    assert pab["RestrictPublicBuckets"] is True

    versioning = s3.get_bucket_versioning(Bucket=crawler_bucket_name)
    assert versioning.get("Status") == "Enabled"


# ---------------------------------------------------------------------------
# SecretsManager — secret exists with expected structure
# ---------------------------------------------------------------------------


def test_database_secret_contains_username_and_password_keys(
    secret_arn: str,
) -> None:
    """DatabaseSecret must be retrievable and contain username + password keys."""
    sm = _client("secretsmanager")
    value = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
    assert "username" in value
    assert "password" in value
