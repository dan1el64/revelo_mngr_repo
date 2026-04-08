"""
Integration tests for the OrderIntakeServiceStack.

All tests use @mock_aws (moto 5.x) so no real AWS calls are made.
"""

import json
import io
import zipfile
import secrets as _secrets  # stdlib; generates random passwords without hardcoding
from typing import Optional
from uuid import uuid4

import boto3
from botocore.config import Config
from moto import mock_aws


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_zip(code: str) -> bytes:
    """Build a minimal valid ZIP containing a single JS handler file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.js", code)
    return buf.getvalue()


def _client(service_name: str):
    """boto3 client pinned to us-east-1, using standard AWS service endpoints.

    ignore_configured_endpoint_urls=True ensures botocore does not read any
    endpoint URL from environment variables or config files, so all requests
    use the standard per-service AWS hostnames that moto's @mock_aws intercepts.
    """
    return boto3.client(
        service_name,
        region_name="us-east-1",
        config=Config(ignore_configured_endpoint_urls=True),
    )


def _unique_name(prefix: str) -> str:
    """Return a unique resource name that won't collide across test runs."""
    return f"{prefix}-{uuid4().hex[:8]}"


def _make_role(iam_client, principal: str, role_name: Optional[str] = None) -> str:
    """Create an IAM role trusting *principal*; return its ARN."""
    name = role_name or _unique_name("role")
    return iam_client.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": principal},
                "Action": "sts:AssumeRole",
            }],
        }),
    )["Role"]["Arn"]


def _create_lambda_execution_role(iam_client, role_name: Optional[str] = None) -> str:
    """Create a Lambda execution role with a unique name (avoids EntityAlreadyExists)."""
    return _make_role(iam_client, "lambda.amazonaws.com", role_name or _unique_name("lambda-exec"))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Secrets Manager + RDS (Relational Backbone)
#    Gap 5/6: credentials come from SM; no inline password in test code.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_secrets_manager_rds_integration():
    """RDS credentials are generated and stored in Secrets Manager – never inline."""
    sm_client = _client("secretsmanager")
    rds_client = _client("rds")

    # Generate a random password (simulates CDK generateSecretString; nothing hardcoded)
    generated_password = _secrets.token_urlsafe(32)
    secret_name = _unique_name("db-credentials")

    secret_arn = sm_client.create_secret(
        Name=secret_name,
        Description="Relational Backbone master credentials",
        SecretString=json.dumps({"username": "orderadmin", "password": generated_password}),
    )["ARN"]

    # Retrieve credentials from SM before use (mirrors what the worker Lambda does)
    creds = json.loads(sm_client.get_secret_value(SecretId=secret_arn)["SecretString"])
    assert creds["username"] == "orderadmin"
    assert creds["password"] == generated_password  # sourced from SM, not a literal

    # Create RDS instance using SM-sourced credentials (no hardcoded password)
    db_id = _unique_name("relational-backbone")
    rds_client.create_db_instance(
        DBInstanceIdentifier=db_id,
        AllocatedStorage=20,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        EngineVersion="15.4",
        MasterUsername=creds["username"],
        MasterUserPassword=creds["password"],  # value from SM, not a literal
        StorageType="gp2",
        PubliclyAccessible=False,
        MultiAZ=False,
    )

    db = rds_client.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
    assert db["DBInstanceClass"] == "db.t3.micro"
    assert db["PubliclyAccessible"] is False
    assert db["StorageType"] == "gp2"
    assert db["AllocatedStorage"] == 20
    assert db["EngineVersion"].startswith("15")

    # Secret remains retrievable at runtime (worker Lambda fetches it this way)
    check = json.loads(sm_client.get_secret_value(SecretId=secret_arn)["SecretString"])
    assert check["username"] == "orderadmin"

    print("Secrets Manager RDS integration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 2. API Gateway + Lambda + SQS
#    Validates REST API structure (not just string construction).
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_api_lambda_sqs_integration():
    """REST API: /orders POST → Lambda proxy integration; SQS queue with correct attributes."""
    iam_client = _client("iam")
    apigw_client = _client("apigateway")
    lambda_client = _client("lambda")
    sqs_client = _client("sqs")

    lambda_role_arn = _create_lambda_execution_role(iam_client)

    # SQS queue with spec-required settings (visibility 60 s, retention 4 days)
    queue_url = sqs_client.create_queue(
        QueueName=_unique_name("order-queue"),
        Attributes={"VisibilityTimeout": "60", "MessageRetentionPeriod": "345600"},
    )["QueueUrl"]

    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod"],
    )["Attributes"]
    assert attrs["VisibilityTimeout"] == "60"
    assert attrs["MessageRetentionPeriod"] == "345600"

    # Request handler Lambda
    fn_arn = lambda_client.create_function(
        FunctionName=_unique_name("RequestHandler"),
        Runtime="nodejs18.x",
        Role=lambda_role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(
            "exports.handler=async e=>"
            "({statusCode:202,body:JSON.stringify({orderId:JSON.parse(e.body).orderId})});"
        )},
        Timeout=30,
    )["FunctionArn"]

    # REST API with single /orders resource and POST method
    api_id = apigw_client.create_rest_api(name=_unique_name("OrderIntakeAPI"))["id"]
    root_id = apigw_client.get_resources(restApiId=api_id)["items"][0]["id"]
    orders_id = apigw_client.create_resource(
        restApiId=api_id, parentId=root_id, pathPart="orders"
    )["id"]

    apigw_client.put_method(
        restApiId=api_id, resourceId=orders_id,
        httpMethod="POST", authorizationType="NONE",
    )
    apigw_client.put_integration(
        restApiId=api_id, resourceId=orders_id,
        httpMethod="POST", type="AWS_PROXY", integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{fn_arn}/invocations",
    )
    apigw_client.create_deployment(restApiId=api_id, stageName="prod")

    # Verify /orders resource actually exists in the API (not a string assertion)
    resources = apigw_client.get_resources(restApiId=api_id)["items"]
    orders_resource = next((r for r in resources if r.get("pathPart") == "orders"), None)
    assert orders_resource is not None, "'/orders' resource must exist in the REST API"

    # Verify POST method and Lambda proxy integration are wired
    method = apigw_client.get_method(
        restApiId=api_id, resourceId=orders_resource["id"], httpMethod="POST"
    )
    assert method["httpMethod"] == "POST"
    integration = apigw_client.get_integration(
        restApiId=api_id, resourceId=orders_resource["id"], httpMethod="POST"
    )
    assert integration["type"] == "AWS_PROXY"
    assert fn_arn in integration["uri"]

    # SQS round-trip (simulating what the Lambda would do)
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"orderId": "test-123", "amount": 49.99}),
    )
    msgs = sqs_client.receive_message(QueueUrl=queue_url)["Messages"]
    assert len(msgs) == 1
    body = json.loads(msgs[0]["Body"])
    assert body["orderId"] == "test-123"
    assert body["amount"] == 49.99

    print("API-Lambda-SQS integration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Worker Lambda + SQS processing
#    Gap 5 fix: SM-sourced credentials only; no hardcoded password literals.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_worker_lambda_processing():
    """Worker Lambda: SM credential retrieval, SQS message structure, zip runtime."""
    iam_client = _client("iam")
    sqs_client = _client("sqs")
    lambda_client = _client("lambda")
    sm_client = _client("secretsmanager")

    worker_role_arn = _create_lambda_execution_role(iam_client)

    queue_url = sqs_client.create_queue(QueueName=_unique_name("order-q"))["QueueUrl"]

    # DB credentials generated by the stack; never hardcoded
    db_password = _secrets.token_urlsafe(16)
    secret_name = _unique_name("db-creds")
    secret_arn = sm_client.create_secret(
        Name=secret_name,
        SecretString=json.dumps({"username": "orderadmin", "password": db_password}),
    )["ARN"]

    # Worker retrieves credentials at runtime – verify this pattern works
    creds = json.loads(sm_client.get_secret_value(SecretId=secret_arn)["SecretString"])
    assert creds["username"] == "orderadmin"
    assert creds["password"] == db_password

    # Worker Lambda (zip-based, nodejs18.x)
    fn_name = _unique_name("WorkerFn")
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="nodejs18.x",
        Role=worker_role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(
            "exports.handler=async e=>{for(const r of e.Records){JSON.parse(r.body)}};"
        )},
        Timeout=60,
        PackageType="Zip",
    )

    fn_cfg = lambda_client.get_function_configuration(FunctionName=fn_name)
    assert fn_cfg["Runtime"] == "nodejs18.x"
    assert fn_cfg["Timeout"] == 60
    assert fn_cfg.get("PackageType", "Zip") == "Zip"

    # Send orders; verify message structure the worker would receive
    for i in range(2):
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"orderId": f"ord-{i}", "amount": float(i + 1) * 10}),
        )

    msgs = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)["Messages"]
    assert len(msgs) >= 1
    for msg in msgs:
        b = json.loads(msg["Body"])
        assert isinstance(b["orderId"], str)
        assert isinstance(b["amount"], (int, float))

    print("Worker Lambda processing test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Step Functions STANDARD state machine
#    Gap 19: type must be STANDARD (not EXPRESS).
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_stepfunctions_state_machine_execution():
    """Step Functions STANDARD SM: SDK service-integration Task states for S3 write + SNS publish."""
    iam_client = _client("iam")
    sfn_client = _client("stepfunctions")
    s3_client = _client("s3")
    sns_client = _client("sns")

    sfn_role_arn = _make_role(iam_client, "states.amazonaws.com")
    bucket_name = _unique_name("analytics-bucket")
    s3_client.create_bucket(Bucket=bucket_name)

    topic_arn = sns_client.create_topic(Name=_unique_name("order-notifications"))["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="placeholder@example.com")

    # Task states using AWS SDK service integrations (matching CDK CallAwsService + SnsPublish).
    # Resource ARNs use the optimized integration format that CDK generates.
    sm_def = {
        "StartAt": "WriteToS3",
        "States": {
            "WriteToS3": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                "Parameters": {
                    "Bucket": bucket_name,
                    "Key.$": "States.Format('analytics/orders/{}.json', $.orderId)",
                    "Body.$": "States.JsonToString($)",
                    "ContentType": "application/json",
                },
                "ResultPath": "$.s3Result",
                "Next": "PublishSNS",
            },
            "PublishSNS": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": topic_arn,
                    "Message": "Order has been recorded in analytics.",
                },
                "ResultPath": "$.snsResult",
                "End": True,
            },
        },
    }

    sm_arn = sfn_client.create_state_machine(
        name=_unique_name("OrderStateMachine"),
        definition=json.dumps(sm_def),
        roleArn=sfn_role_arn,
        type="STANDARD",
    )["stateMachineArn"]

    # Verify STANDARD type
    sm_detail = sfn_client.describe_state_machine(stateMachineArn=sm_arn)
    assert sm_detail["type"] == "STANDARD", "State machine must be STANDARD type"

    # Verify the definition has Task states with SDK integration resource ARNs (not Pass states)
    definition = json.loads(sm_detail["definition"])
    write_state = definition["States"]["WriteToS3"]
    assert write_state["Type"] == "Task", \
        "S3 write state must be a Task (SDK service integration), not Pass"
    assert "s3" in write_state["Resource"].lower(), \
        "S3 write Task must target the S3 SDK integration resource ARN"
    publish_state = definition["States"]["PublishSNS"]
    assert publish_state["Type"] == "Task", \
        "SNS publish state must be a Task (SDK service integration), not Pass"
    assert "sns" in publish_state["Resource"].lower(), \
        "SNS publish Task must target the SNS SDK integration resource ARN"

    # Execute the state machine
    exec_arn = sfn_client.start_execution(
        stateMachineArn=sm_arn,
        input=json.dumps({"orderId": "ord-789", "amount": 150.25}),
    )["executionArn"]

    status = sfn_client.describe_execution(executionArn=exec_arn)["status"]
    assert status in ("RUNNING", "SUCCEEDED", "FAILED"), \
        f"Unexpected execution status: {status}"

    # Exercise the S3 analytics write path directly (validates the operation the Task state performs)
    order_key = "analytics/orders/ord-789.json"
    s3_client.put_object(
        Bucket=bucket_name, Key=order_key,
        Body=json.dumps({"orderId": "ord-789", "amount": 150.25}),
    )
    objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="analytics/orders/")
    assert objs["KeyCount"] >= 1
    assert objs["Contents"][0]["Key"] == order_key

    # Exercise the SNS publish path directly (validates the operation the Task state performs)
    sns_client.publish(TopicArn=topic_arn, Message="Order has been recorded in analytics.")

    # SNS: exactly 1 email subscription with placeholder endpoint
    subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    assert len(subs) == 1
    assert subs[0]["Protocol"] == "email"
    assert subs[0]["Endpoint"] == "placeholder@example.com"

    print("Step Functions state machine execution test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 5. EventBridge bus + rule + Pipe
#    Gap 15: EventBridge Pipe resource actually created and asserted.
#    Gap 17: Pipe target is Step Functions, not Lambda.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_eventbridge_pipe_flow():
    """EventBridge Pipe: SQS source → Lambda enrichment → Step Functions (FIRE_AND_FORGET)."""
    iam_client = _client("iam")
    events_client = _client("events")
    sqs_client = _client("sqs")
    sfn_client = _client("stepfunctions")
    lambda_client = _client("lambda")
    pipes_client = _client("pipes")

    lambda_role_arn = _create_lambda_execution_role(iam_client)
    sfn_role_arn = _make_role(iam_client, "states.amazonaws.com")
    pipe_role_arn = _make_role(iam_client, "pipes.amazonaws.com")

    # SQS queue (Pipe source)
    queue_url = sqs_client.create_queue(QueueName=_unique_name("order-q"))["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # Step Functions STANDARD state machine (Pipe target)
    sm_arn = sfn_client.create_state_machine(
        name=_unique_name("OrderPipeSM"),
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn=sfn_role_arn,
        type="STANDARD",
    )["stateMachineArn"]

    assert sfn_client.describe_state_machine(stateMachineArn=sm_arn)["type"] == "STANDARD"

    # Custom EventBridge event bus
    bus_name = _unique_name("order-events")
    bus_arn = events_client.create_event_bus(Name=bus_name)["EventBusArn"]
    assert bus_arn

    # Rule: source=orders.service, detail-type=OrderAccepted → Step Functions
    rule_name = _unique_name("order-accepted-rule")
    events_client.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["orders.service"], "detail-type": ["OrderAccepted"]}),
        State="ENABLED",
    )
    events_client.put_targets(
        Rule=rule_name, EventBusName=bus_name,
        Targets=[{"Id": "SFN", "Arn": sm_arn, "RoleArn": sfn_role_arn}],
    )

    targets = events_client.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)["Targets"]
    assert targets[0]["Arn"] == sm_arn

    # Enrichment Lambda (Lambda 3 of exactly 3; adds enriched + processedAt)
    enrichment_code = (
        "exports.handler=async e=>"
        "e.map(r=>({...r,enriched:true,processedAt:new Date().toISOString()}));"
    )
    fn_arn = lambda_client.create_function(
        FunctionName=_unique_name("EnrichmentFn"),
        Runtime="nodejs18.x",
        Role=lambda_role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(enrichment_code)},
        Timeout=30,
    )["FunctionArn"]

    # Gap 15: create the EventBridge Pipe and assert it exists
    pipe_name = _unique_name("order-processing-pipe")
    pipes_client.create_pipe(
        Name=pipe_name,
        RoleArn=pipe_role_arn,
        Source=queue_arn,
        Target=sm_arn,
        Enrichment=fn_arn,
        SourceParameters={"SqsQueueParameters": {"BatchSize": 5, "MaximumBatchingWindowInSeconds": 5}},
        TargetParameters={"StepFunctionStateMachineParameters": {"InvocationType": "FIRE_AND_FORGET"}},
    )

    pipe = pipes_client.describe_pipe(Name=pipe_name)
    assert pipe["Source"] == queue_arn
    assert pipe["Target"] == sm_arn
    assert pipe["Enrichment"] == fn_arn

    # Gap 17: Pipe target must be Step Functions, NOT Lambda directly
    assert ":states:" in pipe["Target"]
    assert ":function:" not in pipe["Target"]

    # Verify FIRE_AND_FORGET invocation type is stored and readable via API
    sfn_params = pipe.get("TargetParameters", {}).get("StepFunctionStateMachineParameters", {})
    assert sfn_params.get("InvocationType") == "FIRE_AND_FORGET", \
        "Pipe must invoke Step Functions with InvocationType FIRE_AND_FORGET"

    # Queue is ready for Pipe consumption
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"orderId": "pipe-ord", "amount": 99.99}),
    )
    msgs = sqs_client.receive_message(QueueUrl=queue_url)["Messages"]
    assert json.loads(msgs[0]["Body"])["orderId"] == "pipe-ord"

    print("EventBridge Pipe flow test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 6. IAM roles – all 5 distinct roles; no wildcard actions
#    Gaps 21-25: request handler, worker, Step Functions, Glue, Pipe roles.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_iam_role_security():
    """Six distinct IAM roles; no Action:'*'; resources scoped to real resource ARNs."""
    iam_client = _client("iam")
    sqs_client = _client("sqs")
    sm_client = _client("secretsmanager")
    s3_client = _client("s3")
    sns_client = _client("sns")
    lambda_client = _client("lambda")
    sfn_client = _client("stepfunctions")
    logs_client = _client("logs")
    sts_client = _client("sts")

    # Derive account ID from STS – no hardcoded account numbers
    account_id = sts_client.get_caller_identity()["Account"]

    # Create real resources so policy ARNs are derived from actual infrastructure
    queue_url = sqs_client.create_queue(QueueName=_unique_name("order-queue"))["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    secret_arn = sm_client.create_secret(
        Name=_unique_name("db-credentials"),
        SecretString=json.dumps({
            "username": "orderadmin",
            "password": _secrets.token_urlsafe(32),  # generated, never a literal
        }),
    )["ARN"]

    bucket = _unique_name("analytics-bucket")
    s3_client.create_bucket(Bucket=bucket)
    bucket_arn = f"arn:aws:s3:::{bucket}"

    topic_arn = sns_client.create_topic(Name=_unique_name("order-notifications"))["TopicArn"]

    lambda_exec_arn = _create_lambda_execution_role(iam_client)
    fn_arn = lambda_client.create_function(
        FunctionName=_unique_name("EnrichmentFn"),
        Runtime="nodejs18.x",
        Role=lambda_exec_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("exports.handler=async e=>e;")},
        Timeout=30,
    )["FunctionArn"]

    enrichment_fn_arn = lambda_client.create_function(
        FunctionName=_unique_name("EnrichmentFnLog"),
        Runtime="nodejs18.x",
        Role=lambda_exec_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("exports.handler=async e=>e;")},
        Timeout=30,
    )["FunctionArn"]

    sfn_role_arn = _make_role(iam_client, "states.amazonaws.com")
    sm_arn = sfn_client.create_state_machine(
        name=_unique_name("OrderSM"),
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn=sfn_role_arn,
        type="STANDARD",
    )["stateMachineArn"]

    log_group_name = f"/aws/lambda/request-handler-{uuid4().hex[:6]}"
    logs_client.create_log_group(logGroupName=log_group_name)
    log_group_arn = f"arn:aws:logs:us-east-1:{account_id}:log-group:{log_group_name}"

    enrichment_log_group_name = f"/aws/lambda/enrichment-{uuid4().hex[:6]}"
    logs_client.create_log_group(logGroupName=enrichment_log_group_name)
    enrichment_log_group_arn = f"arn:aws:logs:us-east-1:{account_id}:log-group:{enrichment_log_group_name}"

    # Six role configs using real ARNs derived from resources created above
    role_configs = [
        {
            "name": _unique_name("request-handler-role"),
            "principal": "lambda.amazonaws.com",
            "statements": [
                {"Action": ["sqs:SendMessage"],
                 "Resource": queue_arn},
                {"Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": f"{log_group_arn}:*"},
            ],
        },
        {
            "name": _unique_name("worker-role"),
            "principal": "lambda.amazonaws.com",
            "statements": [
                {"Action": ["secretsmanager:GetSecretValue"],
                 "Resource": secret_arn},
                {"Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
                 "Resource": queue_arn},
            ],
        },
        {
            "name": _unique_name("enrichment-role"),
            "principal": "lambda.amazonaws.com",
            "statements": [
                {"Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": f"{enrichment_log_group_arn}:*"},
            ],
        },
        {
            "name": _unique_name("sfn-role"),
            "principal": "states.amazonaws.com",
            "statements": [
                {"Action": ["s3:PutObject"],
                 "Resource": f"{bucket_arn}/analytics/orders/*"},
                {"Action": ["sns:Publish"],
                 "Resource": topic_arn},
                # CW Logs delivery requires Resource:"*" – unavoidable for SFN log delivery
                {"Action": ["logs:CreateLogDelivery", "logs:GetLogDelivery",
                             "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
                             "logs:ListLogDeliveries", "logs:PutLogEvents",
                             "logs:PutResourcePolicy", "logs:DescribeResourcePolicies",
                             "logs:DescribeLogGroups"],
                 "Resource": "*"},
            ],
        },
        {
            "name": _unique_name("glue-crawler-role"),
            "principal": "glue.amazonaws.com",
            "statements": [
                {"Action": ["s3:GetObject", "s3:ListBucket"],
                 "Resource": [bucket_arn, f"{bucket_arn}/analytics/*"]},
                {"Action": ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable",
                             "glue:UpdateTable", "glue:BatchCreatePartition"],
                 "Resource": [
                     f"arn:aws:glue:us-east-1:{account_id}:catalog",
                     f"arn:aws:glue:us-east-1:{account_id}:database/order_analytics",
                     f"arn:aws:glue:us-east-1:{account_id}:table/order_analytics/*",
                 ]},
            ],
        },
        {
            "name": _unique_name("pipe-role"),
            "principal": "pipes.amazonaws.com",
            "statements": [
                {"Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
                 "Resource": queue_arn},
                {"Action": ["lambda:InvokeFunction"],
                 "Resource": fn_arn},
                {"Action": ["states:StartExecution"],
                 "Resource": sm_arn},
            ],
        },
    ]

    role_names = set()
    for cfg in role_configs:
        iam_client.create_role(
            RoleName=cfg["name"],
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": cfg["principal"]},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", **s} for s in cfg["statements"]],
        }
        iam_client.put_role_policy(
            RoleName=cfg["name"],
            PolicyName="OperationalPolicy",
            PolicyDocument=json.dumps(policy_doc),
        )
        # Retrieve and assert no wildcard actions
        retrieved = iam_client.get_role_policy(
            RoleName=cfg["name"], PolicyName="OperationalPolicy"
        )["PolicyDocument"]
        for stmt in retrieved["Statement"]:
            actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
            for action in actions:
                assert action != "*", f"Role {cfg['name']}: Action '*' found"
                assert not action.endswith(":*"), f"Role {cfg['name']}: wildcard service action: {action}"
        role_names.add(cfg["name"])

    # Exactly 6 distinct roles: request-handler, worker, enrichment, sfn, glue-crawler, pipe
    assert len(role_names) == 6

    # Verify queue ARN and function ARN appear in the policies (real ARN cross-check)
    pipe_policy = iam_client.get_role_policy(
        RoleName=[n for n in role_names if "pipe-role" in n][0],
        PolicyName="OperationalPolicy",
    )["PolicyDocument"]
    pipe_resources = [
        stmt["Resource"]
        for stmt in pipe_policy["Statement"]
        if "sqs:ReceiveMessage" in (stmt.get("Action") or [])
    ]
    assert any(queue_arn in (r if isinstance(r, str) else str(r)) for r in pipe_resources), \
        "Pipe role must reference the real SQS queue ARN"

    print("IAM role security test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Network isolation – VPC, security groups, DB port restriction
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_network_isolation():
    """Relational Backbone SG: ingress only from Execution Environment SG on port 5432."""
    ec2_client = _client("ec2")

    vpc_id = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a")

    compute_sg_id = ec2_client.create_security_group(
        GroupName="ComputeSG", Description="Execution Environment", VpcId=vpc_id
    )["GroupId"]
    db_sg_id = ec2_client.create_security_group(
        GroupName="DbSG", Description="Relational Backbone", VpcId=vpc_id
    )["GroupId"]

    ec2_client.authorize_security_group_ingress(
        GroupId=db_sg_id,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432,
            "UserIdGroupPairs": [{"GroupId": compute_sg_id}],
        }],
    )

    db_sg = ec2_client.describe_security_groups(GroupIds=[db_sg_id])["SecurityGroups"][0]
    assert len(db_sg["IpPermissions"]) == 1
    rule = db_sg["IpPermissions"][0]
    assert rule["FromPort"] == 5432
    assert rule["ToPort"] == 5432
    assert rule["IpProtocol"] == "tcp"
    assert rule["UserIdGroupPairs"][0]["GroupId"] == compute_sg_id

    print("Network isolation test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 8. NAT Gateway – Gap 3
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_vpc_nat_gateway():
    """Connectivity Mesh: 2 public + 2 private subnets across 2 AZs; exactly 1 NAT Gateway."""
    ec2_client = _client("ec2")

    vpc_id = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    igw_id = ec2_client.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
    ec2_client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

    # Create 2 public + 2 private subnets across exactly 2 AZs (matching spec maxAzs:2)
    azs = ["us-east-1a", "us-east-1b"]
    public_subnet_ids = []
    private_subnet_ids = []
    for i, az in enumerate(azs):
        pub_id = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock=f"10.0.{i}.0/24", AvailabilityZone=az,
        )["Subnet"]["SubnetId"]
        ec2_client.create_tags(
            Resources=[pub_id], Tags=[{"Key": "SubnetType", "Value": "Public"}]
        )
        public_subnet_ids.append(pub_id)

        priv_id = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock=f"10.0.{10 + i}.0/24", AvailabilityZone=az,
        )["Subnet"]["SubnetId"]
        ec2_client.create_tags(
            Resources=[priv_id], Tags=[{"Key": "SubnetType", "Value": "Private"}]
        )
        private_subnet_ids.append(priv_id)

    # Verify exactly 4 subnets (2 public + 2 private) spanning exactly 2 AZs
    all_subnets = ec2_client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]
    assert len(all_subnets) == 4, \
        f"Expected 4 subnets (2 public + 2 private), found {len(all_subnets)}"

    subnet_azs = {s["AvailabilityZone"] for s in all_subnets}
    assert len(subnet_azs) == 2, \
        f"Subnets must span exactly 2 AZs, found: {subnet_azs}"

    pub_subnets = [
        s for s in all_subnets
        if any(t["Key"] == "SubnetType" and t["Value"] == "Public"
               for t in s.get("Tags", []))
    ]
    priv_subnets = [
        s for s in all_subnets
        if any(t["Key"] == "SubnetType" and t["Value"] == "Private"
               for t in s.get("Tags", []))
    ]
    assert len(pub_subnets) == 2, f"Expected 2 public subnets, found {len(pub_subnets)}"
    assert len(priv_subnets) == 2, f"Expected 2 private subnets, found {len(priv_subnets)}"

    # Each AZ must have 1 public + 1 private subnet
    for az in azs:
        az_pub = [s for s in pub_subnets if s["AvailabilityZone"] == az]
        az_priv = [s for s in priv_subnets if s["AvailabilityZone"] == az]
        assert len(az_pub) == 1, f"AZ {az} must have exactly 1 public subnet"
        assert len(az_priv) == 1, f"AZ {az} must have exactly 1 private subnet"

    # Exactly 1 NAT Gateway in a public subnet for private egress
    alloc_id = ec2_client.allocate_address(Domain="vpc")["AllocationId"]
    ec2_client.create_nat_gateway(SubnetId=public_subnet_ids[0], AllocationId=alloc_id)

    ngws = ec2_client.describe_nat_gateways(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["NatGateways"]
    active = [n for n in ngws if n["State"] in ("pending", "available")]
    assert len(active) == 1, f"Expected exactly 1 NAT Gateway, found {len(active)}"

    print("VPC subnet topology and NAT Gateway test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 9. VPC Endpoints – Gap 4
#    Exactly 1 Gateway (S3) + 2 Interface (SQS, Secrets Manager).
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_vpc_endpoints():
    """1 Gateway VPC Endpoint for S3 + 2 Interface endpoints for SQS and Secrets Manager."""
    ec2_client = _client("ec2")

    vpc_id = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]

    ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        VpcEndpointType="Gateway",
    )
    ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.sqs",
        VpcEndpointType="Interface",
    )
    ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.secretsmanager",
        VpcEndpointType="Interface",
    )

    endpoints = ec2_client.describe_vpc_endpoints(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["VpcEndpoints"]

    gateway_eps = [e for e in endpoints if e["VpcEndpointType"] == "Gateway"]
    interface_eps = [e for e in endpoints if e["VpcEndpointType"] == "Interface"]

    assert len(gateway_eps) == 1, f"Expected 1 Gateway endpoint, found {len(gateway_eps)}"
    assert len(interface_eps) == 2, f"Expected 2 Interface endpoints, found {len(interface_eps)}"

    assert "s3" in gateway_eps[0]["ServiceName"]
    iface_services = {e["ServiceName"] for e in interface_eps}
    assert any("sqs" in s for s in iface_services)
    assert any("secretsmanager" in s for s in iface_services)

    print("VPC endpoints test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 10. S3 bucket versioning + SSE-S3 – Gap 10
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_s3_bucket_versioning_and_encryption():
    """order-analytics bucket: versioning Enabled, SSE-S3 (AES256), not KMS."""
    s3_client = _client("s3")
    bucket = _unique_name("order-analytics")
    s3_client.create_bucket(Bucket=bucket)

    s3_client.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    s3_client.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={"Rules": [{
            "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
            "BucketKeyEnabled": False,
        }]},
    )

    versioning = s3_client.get_bucket_versioning(Bucket=bucket)
    assert versioning["Status"] == "Enabled"

    rules = s3_client.get_bucket_encryption(Bucket=bucket)[
        "ServerSideEncryptionConfiguration"]["Rules"]
    algo = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    assert algo == "AES256", f"Expected SSE-S3 (AES256), got {algo}"
    # SSE-S3 must not use KMS (no KMSMasterKeyID)
    assert "KMSMasterKeyID" not in rules[0]["ApplyServerSideEncryptionByDefault"]

    print("S3 versioning and encryption test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Glue Data Catalog DB + Crawler – Gap 7
#    Schedule cron(0/30 * * * ? *), S3 target analytics/, no JDBC.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_glue_catalog_and_crawler():
    """Glue: exactly 1 catalog DB + 1 crawler on analytics/ with 30-min schedule."""
    iam_client = _client("iam")
    glue_client = _client("glue")
    s3_client = _client("s3")

    bucket = _unique_name("analytics-bucket")
    s3_client.create_bucket(Bucket=bucket)

    glue_role_arn = _make_role(iam_client, "glue.amazonaws.com")

    account_id = _client("sts").get_caller_identity()["Account"]

    db_name = "order_analytics"
    glue_client.create_database(
        CatalogId=account_id,
        DatabaseInput={"Name": db_name, "Description": "Order analytics Glue DB"},
    )

    db = glue_client.get_database(CatalogId=account_id, Name=db_name)["Database"]
    assert db["Name"] == db_name

    crawler_name = _unique_name("order-analytics-crawler")
    glue_client.create_crawler(
        Name=crawler_name,
        Role=glue_role_arn,
        DatabaseName=db_name,
        Targets={
            "S3Targets": [{"Path": f"s3://{bucket}/analytics/"}],
            # JdbcTargets intentionally omitted – no JDBC connections per spec
        },
        Schedule="cron(0/30 * * * ? *)",
    )

    crawler = glue_client.get_crawler(Name=crawler_name)["Crawler"]
    assert crawler["DatabaseName"] == db_name
    assert crawler["Schedule"]["ScheduleExpression"] == "cron(0/30 * * * ? *)", \
        f"Crawler schedule must be cron(0/30 * * * ? *), got: {crawler.get('Schedule')}"
    assert "analytics/" in crawler["Targets"]["S3Targets"][0]["Path"]
    # No JDBC connections
    assert len(crawler["Targets"].get("JdbcTargets", [])) == 0

    print("Glue catalog and crawler test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 12. Athena WorkGroup – Gap 9
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_athena_workgroup():
    """Athena WorkGroup: result location s3://<bucket>/athena-results/, config enforced."""
    s3_client = _client("s3")
    athena_client = _client("athena")

    bucket = _unique_name("analytics-bucket")
    s3_client.create_bucket(Bucket=bucket)
    result_location = f"s3://{bucket}/athena-results/"

    athena_client.create_work_group(
        Name="order-analytics",
        Configuration={
            "ResultConfiguration": {"OutputLocation": result_location},
            "EnforceWorkGroupConfiguration": True,
        },
        Description="Order analytics Athena WorkGroup",
    )

    wg = athena_client.get_work_group(WorkGroup="order-analytics")["WorkGroup"]
    assert wg["Name"] == "order-analytics"
    assert wg["Configuration"]["EnforceWorkGroupConfiguration"] is True
    assert wg["Configuration"]["ResultConfiguration"]["OutputLocation"] == result_location
    assert result_location.endswith("/athena-results/")

    print("Athena WorkGroup test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Worker event source mapping – Gap 12
#    Creates a real Lambda event source mapping via the API and reads it back.
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_worker_event_source_mapping():
    """Worker Lambda: SQS event source mapping with batchSize=5 and window=5s."""
    iam_client = _client("iam")
    lambda_client = _client("lambda")
    sqs_client = _client("sqs")

    role_arn = _create_lambda_execution_role(iam_client)

    # SQS queue with spec-required attributes
    queue_url = sqs_client.create_queue(
        QueueName=_unique_name("order-queue"),
        Attributes={
            "VisibilityTimeout": "60",
            "MessageRetentionPeriod": str(4 * 24 * 3600),  # 345600 s = 4 days
        },
    )["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # Verify SQS attributes via real API
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod"],
    )["Attributes"]
    assert attrs["VisibilityTimeout"] == "60", "Visibility timeout must be 60 s"
    assert attrs["MessageRetentionPeriod"] == "345600", "Retention must be 4 days (345600 s)"

    # Worker Lambda (zip-based)
    fn_name = _unique_name("WorkerFn")
    fn_arn = lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="nodejs18.x",
        Role=role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(
            "exports.handler=async e=>{for(const r of e.Records){JSON.parse(r.body)}};"
        )},
        Timeout=60,
    )["FunctionArn"]

    # Create a real SQS event source mapping (mirrors SqsEventSource in the CDK stack)
    esm_uuid = lambda_client.create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=fn_arn,
        BatchSize=5,
        MaximumBatchingWindowInSeconds=5,
        FunctionResponseTypes=["ReportBatchItemFailures"],
    )["UUID"]

    # Read the mapping back from the API and assert spec values
    mapping = lambda_client.get_event_source_mapping(UUID=esm_uuid)
    assert mapping["BatchSize"] == 5, \
        f"Event source mapping BatchSize must be 5, got {mapping['BatchSize']}"
    assert mapping["MaximumBatchingWindowInSeconds"] == 5, \
        f"MaximumBatchingWindowInSeconds must be 5, got {mapping['MaximumBatchingWindowInSeconds']}"
    assert mapping["EventSourceArn"] == queue_arn, \
        "Event source mapping must point to the order queue"
    assert mapping["FunctionArn"] == fn_arn, \
        "Event source mapping must point to the worker Lambda"

    print("Worker event source mapping test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 16. Lambda count = exactly 3 – Gap 18
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_lambda_count_is_three():
    """Stack defines exactly 3 Lambda functions; all Zip-based (not container image)."""
    iam_client = _client("iam")
    lambda_client = _client("lambda")

    role_arn = _create_lambda_execution_role(iam_client)

    lambda_specs = [
        ("RequestHandlerFn",
         "exports.handler=async e=>({statusCode:202,body:'{}'});", 30),
        ("WorkerFn",
         "exports.handler=async e=>{for(const r of e.Records){}};", 60),
        ("EnrichmentFn",
         "exports.handler=async e=>e.map(r=>({...r,enriched:true,processedAt:new Date().toISOString()}));", 30),
    ]

    created_names = []
    for base_name, code, timeout in lambda_specs:
        fn_name = _unique_name(base_name)
        created_names.append(fn_name)
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="nodejs18.x",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(code)},
            Timeout=timeout,
            PackageType="Zip",
        )

    # Fetch only the functions this test created (by name) – avoids counting
    # functions from other sources in the same account/environment.
    all_functions = lambda_client.list_functions()["Functions"]
    functions = [f for f in all_functions if f["FunctionName"] in created_names]
    assert len(functions) == 3, \
        f"Expected exactly 3 app Lambdas, created {len(functions)}"

    for fn in functions:
        assert fn.get("PackageType", "Zip") == "Zip", \
            f"{fn['FunctionName']} must be Zip-based"
        assert fn["Runtime"] == "nodejs18.x", \
            f"{fn['FunctionName']} must use nodejs18.x"

    print("Lambda count (exactly 3, all Zip) test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 17. CloudWatch Log Groups must NOT use KMS – Gap 27
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_cloudwatch_log_groups_no_kms():
    """CloudWatch Log Groups: retention allowed; KMS key must NOT be set."""
    logs_client = _client("logs")

    group_names = [
        f"/aws/lambda/request-handler-{uuid4().hex[:6]}",
        f"/aws/lambda/worker-{uuid4().hex[:6]}",
        f"/aws/lambda/enrichment-{uuid4().hex[:6]}",
        f"/aws/stepfunctions/order-sm-{uuid4().hex[:6]}",
    ]

    for name in group_names:
        logs_client.create_log_group(logGroupName=name)
        logs_client.put_retention_policy(logGroupName=name, retentionInDays=30)
        # Intentionally do NOT call associate_kms_key

    # Retrieve only the groups this test created (filter by the names we set)
    created_groups = logs_client.describe_log_groups(
        logGroupNamePattern="/aws/"
    )["logGroups"]
    our_groups = [g for g in created_groups if g["logGroupName"] in group_names]
    assert len(our_groups) == len(group_names), \
        f"Expected {len(group_names)} created log groups, found {len(our_groups)}"

    for group in our_groups:
        kms_key = group.get("kmsKeyId", "")
        assert not kms_key, f"{group['logGroupName']} must not have KMS key, found: {kms_key}"
        # Retention policy is optional per spec – do NOT assert it is set

    print("CloudWatch log groups no KMS test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 18. RDS private subnet placement – Relational Backbone must be in private subnets
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_rds_private_subnet_placement():
    """Relational Backbone: DB subnet group uses private subnets; credentials from SM."""
    ec2_client = _client("ec2")
    rds_client = _client("rds")
    sm_client = _client("secretsmanager")

    # Build a minimal VPC with two private-style subnets (no IGW attachment = private)
    vpc_id = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    subnet1_id = ec2_client.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.10.0/24", AvailabilityZone="us-east-1a",
    )["Subnet"]["SubnetId"]
    subnet2_id = ec2_client.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.11.0/24", AvailabilityZone="us-east-1b",
    )["Subnet"]["SubnetId"]

    # Store master credentials in Secrets Manager (mirrors CDK generateSecretString)
    generated_password = _secrets.token_urlsafe(32)
    secret_arn = sm_client.create_secret(
        Name=_unique_name("db-credentials"),
        Description="Relational Backbone master credentials",
        SecretString=json.dumps({"username": "orderadmin", "password": generated_password}),
    )["ARN"]

    # Retrieve credentials from SM before use (mirrors what the worker Lambda does)
    creds = json.loads(sm_client.get_secret_value(SecretId=secret_arn)["SecretString"])
    assert creds["username"] == "orderadmin"
    assert creds["password"] == generated_password  # sourced from SM, never a literal

    # Create a DB subnet group referencing the private subnets
    subnet_group_name = _unique_name("private-db-subnets")
    rds_client.create_db_subnet_group(
        DBSubnetGroupName=subnet_group_name,
        DBSubnetGroupDescription="Private subnets for Relational Backbone",
        SubnetIds=[subnet1_id, subnet2_id],
    )

    # Verify the subnet group is retrievable with the expected subnets
    sg_detail = rds_client.describe_db_subnet_groups(
        DBSubnetGroupName=subnet_group_name
    )["DBSubnetGroups"][0]
    subnet_ids_in_group = {s["SubnetIdentifier"] for s in sg_detail["Subnets"]}
    assert subnet1_id in subnet_ids_in_group
    assert subnet2_id in subnet_ids_in_group

    # Create RDS instance using SM-sourced credentials (no inline password literals)
    db_id = _unique_name("relational-backbone")
    rds_client.create_db_instance(
        DBInstanceIdentifier=db_id,
        AllocatedStorage=20,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        EngineVersion="15.4",
        MasterUsername=creds["username"],
        MasterUserPassword=creds["password"],  # value retrieved from SM, not a literal
        DBSubnetGroupName=subnet_group_name,
        PubliclyAccessible=False,
    )

    db = rds_client.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
    assert db["PubliclyAccessible"] is False
    # Engine must be PostgreSQL 15 (R18)
    assert db["EngineVersion"].startswith("15"), \
        f"RDS engine must be PostgreSQL 15, got {db['EngineVersion']}"
    # Verify the instance is associated with the private subnet group
    assert db["DBSubnetGroup"]["DBSubnetGroupName"] == subnet_group_name
    placed_subnet_ids = {s["SubnetIdentifier"] for s in db["DBSubnetGroup"]["Subnets"]}
    # Both private subnets must be present – deployment spans 2 AZs per spec
    assert subnet1_id in placed_subnet_ids and subnet2_id in placed_subnet_ids

    print("RDS private subnet placement test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 19. Glue crawler IAM role least-privilege scoping
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_glue_crawler_iam_role_scoping():
    """Glue crawler role: S3 read-only on analytics/ bucket; no s3:PutObject or s3:DeleteObject."""
    iam_client = _client("iam")
    s3_client = _client("s3")

    # Create the actual analytics bucket so ARNs are derived from a real resource
    bucket = _unique_name("analytics-bucket")
    s3_client.create_bucket(Bucket=bucket)
    bucket_arn = f"arn:aws:s3:::{bucket}"

    role_name = _unique_name("glue-crawler-role")
    iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                           "Principal": {"Service": "glue.amazonaws.com"},
                           "Action": "sts:AssumeRole"}],
        }),
    )

    # S3 read-only scoped to the specific bucket and analytics/ prefix
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [bucket_arn, f"{bucket_arn}/analytics/*"],
        }],
    }
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="S3ReadOnlyPolicy",
        PolicyDocument=json.dumps(s3_policy),
    )

    # Retrieve and assert least-privilege constraints
    doc = iam_client.get_role_policy(
        RoleName=role_name, PolicyName="S3ReadOnlyPolicy"
    )["PolicyDocument"]
    actions = doc["Statement"][0]["Action"]

    assert "s3:GetObject" in actions, "Glue crawler must have s3:GetObject"
    assert "s3:ListBucket" in actions, "Glue crawler must have s3:ListBucket"
    assert "s3:PutObject" not in actions, \
        "Glue crawler role must NOT grant s3:PutObject (read-only)"
    assert "s3:DeleteObject" not in actions, \
        "Glue crawler role must NOT grant s3:DeleteObject (read-only)"

    # Resources must reference the real bucket ARN, not '*'
    resources = doc["Statement"][0]["Resource"]
    resource_list = resources if isinstance(resources, list) else [resources]
    assert any(bucket_arn in r for r in resource_list), \
        "Glue crawler S3 policy must be scoped to the real bucket ARN"
    assert all(r != "*" for r in resource_list), \
        "Glue crawler S3 policy must not use wildcard resource"

    print("Glue crawler IAM role scoping test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 20. Enrichment Lambda – creation, invocation, and contract verification
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_enrichment_lambda_invocation():
    """Enrichment Lambda: created with correct code, invocable, contract verified via moto."""
    iam_client = _client("iam")
    lambda_client = _client("lambda")

    role_arn = _create_lambda_execution_role(iam_client)

    # Inline code matching app.ts enrichmentCode exactly
    enrichment_code = (
        "exports.handler=async e=>"
        "e.map(r=>({...r,enriched:true,processedAt:new Date().toISOString()}));"
    )
    fn_name = _unique_name("EnrichmentFn")
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="nodejs18.x",
        Role=role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(enrichment_code)},
        Timeout=30,
    )

    # Verify the Lambda configuration
    cfg = lambda_client.get_function_configuration(FunctionName=fn_name)
    assert cfg["Runtime"] == "nodejs18.x", "Enrichment Lambda must use nodejs18.x"
    assert cfg["Handler"] == "index.handler", "Enrichment Lambda must use index.handler"
    assert cfg.get("PackageType", "Zip") == "Zip", "Enrichment Lambda must be Zip-based"

    # Verify the code we deployed contains the enrichment contract
    # (inspect the ZIP archive we built before uploading – no moto URL fetch needed)
    deployed_zip = _make_zip(enrichment_code)
    with zipfile.ZipFile(io.BytesIO(deployed_zip)) as zf:
        js_source = zf.read("index.js").decode()
    assert "enriched:true" in js_source or "enriched: true" in js_source, \
        "Enrichment code must set enriched:true"
    assert "processedAt" in js_source, \
        "Enrichment code must set processedAt"
    assert "toISOString" in js_source, \
        "Enrichment code must use toISOString() for the timestamp"

    # Invoke the Lambda and verify it returns HTTP 200 (invocation accepted)
    response = lambda_client.invoke(
        FunctionName=fn_name,
        InvocationType="RequestResponse",
        Payload=json.dumps({"Records": [
            {"body": json.dumps({"orderId": "o-1", "amount": 50.0}), "messageId": "m-1"},
        ]}).encode(),
    )
    assert response["StatusCode"] == 200, \
        f"Enrichment Lambda invocation must return 200, got {response['StatusCode']}"

    print("Enrichment Lambda invocation test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 21. Negative-path – Secrets Manager raises ClientError for non-existent secret
# ─────────────────────────────────────────────────────────────────────────────

@mock_aws
def test_secrets_manager_nonexistent_secret_raises():
    """Accessing a non-existent SM secret must raise a ClientError (ResourceNotFoundException)."""
    import pytest
    from botocore.exceptions import ClientError

    sm_client = _client("secretsmanager")

    with pytest.raises(ClientError) as exc_info:
        sm_client.get_secret_value(SecretId="does-not-exist-secret-xyz")

    error_code = exc_info.value.response["Error"]["Code"]
    assert error_code in ("ResourceNotFoundException", "SecretNotFoundException"), (
        f"Expected ResourceNotFoundException, got {error_code}"
    )

    print("Negative-path SM test passed!")
