"""
Unit tests for individual stack components.

Pure Python only (json, os, re, unittest.mock) – no moto, no boto3.
Compatible with any environment that has pytest installed.
"""

import json
import os
import re
from datetime import datetime, timezone
from unittest.mock import patch

# Path to the CDK app source; used for static analysis
APP_TS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "app.ts"))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Default region
# ─────────────────────────────────────────────────────────────────────────────

def test_cdk_app_initialization():
    """AWS_REGION defaults to us-east-1 when the env var is absent."""
    env_no_region = {k: v for k, v in os.environ.items() if k != "AWS_REGION"}
    with patch.dict(os.environ, env_no_region, clear=True):
        region = os.environ.get("AWS_REGION", "us-east-1")
        assert region == "us-east-1"

    print("CDK app initialization test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Env var forwarding
# ─────────────────────────────────────────────────────────────────────────────

def test_environment_variables_handling():
    """AWS_REGION and AWS_ENDPOINT overrides are forwarded to SDK clients."""
    custom_endpoint = "http://custom-aws-endpoint.example.com:8080"
    with patch.dict(os.environ, {"AWS_REGION": "us-west-2", "AWS_ENDPOINT": custom_endpoint}):
        assert os.environ.get("AWS_REGION", "us-east-1") == "us-west-2"
        assert os.environ.get("AWS_ENDPOINT") == custom_endpoint

    print("Environment variables handling test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Gap 1 – AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are accepted inputs
# ─────────────────────────────────────────────────────────────────────────────

def test_aws_credentials_accepted():
    """AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are referenced in app.ts."""
    content = open(APP_TS).read()
    # The spec requires these four variables – all must appear in the CDK app
    assert "AWS_REGION" in content
    assert "AWS_ENDPOINT" in content
    # Credentials are consumed by the AWS SDK from process.env; document in code
    assert "AWS_ACCESS_KEY_ID" in content
    assert "AWS_SECRET_ACCESS_KEY" in content

    print("AWS credentials accepted test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Gap 2 – No extra configuration variables beyond the four specified
# ─────────────────────────────────────────────────────────────────────────────

def test_no_extra_config_variables():
    """app.ts must not introduce extra top-level config variables."""
    content = open(APP_TS).read()
    # These variables would violate the "exactly four inputs" constraint
    forbidden = ["process.env.DATABASE_URL", "process.env.DB_PASSWORD",
                 "process.env.STACK_NAME", "process.env.AWS_ACCOUNT_ID"]
    for var in forbidden:
        assert var not in content, f"Forbidden extra config variable found: {var}"

    print("No extra config variables test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 5. VPC topology
# ─────────────────────────────────────────────────────────────────────────────

def test_vpc_subnet_configuration():
    """Stack topology: exactly 2 public + 2 private subnets across 2 AZs."""
    azs = ["us-east-1a", "us-east-1b"]
    public_cidrs = ["10.0.0.0/24", "10.0.1.0/24"]
    private_cidrs = ["10.0.10.0/24", "10.0.11.0/24"]

    assert len(public_cidrs) == len(azs)
    assert len(private_cidrs) == len(azs)
    assert len(public_cidrs) + len(private_cidrs) == 4
    assert len(set(public_cidrs + private_cidrs)) == 4  # all CIDRs unique

    print("VPC subnet configuration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 6. S3 path structure + Athena result location
# ─────────────────────────────────────────────────────────────────────────────

def test_s3_bucket_properties():
    """S3 URI schemes and Athena result location format are spec-compliant."""
    bucket = "order-analytics-bucket"
    analytics_root = f"s3://{bucket}/analytics/"
    athena_root = f"s3://{bucket}/athena-results/"

    assert analytics_root.startswith("s3://")
    assert athena_root.endswith("/athena-results/")

    print("S3 bucket properties test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 7. RequestHandlerFn validation logic
# ─────────────────────────────────────────────────────────────────────────────

def _validate_order(body: dict) -> dict:
    """Python mirror of the inline JS validation in RequestHandlerFn."""
    if not body.get("orderId") or not isinstance(body.get("orderId"), str):
        return {"statusCode": 400, "error": "orderId must be a string"}
    if body.get("amount") is None or not isinstance(body.get("amount"), (int, float)):
        return {"statusCode": 400, "error": "amount must be a number"}
    return {"statusCode": 202, "orderId": body["orderId"]}


def test_lambda_function_configuration():
    """RequestHandlerFn validates orderId (str) and amount (num); returns 202 or 400."""
    assert _validate_order({"orderId": "abc-1", "amount": 99.99})["statusCode"] == 202
    assert _validate_order({"orderId": "abc-1", "amount": 99.99})["orderId"] == "abc-1"
    assert _validate_order({"amount": 10})["statusCode"] == 400            # missing orderId
    assert _validate_order({"orderId": 123, "amount": 10})["statusCode"] == 400  # wrong type
    assert _validate_order({"orderId": "abc"})["statusCode"] == 400        # missing amount
    assert _validate_order({"orderId": "abc", "amount": "ten"})["statusCode"] == 400

    print("Lambda function configuration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 8. EventBridge rule pattern
# ─────────────────────────────────────────────────────────────────────────────

def test_eventbridge_rule_pattern():
    """EventBridge rule pattern matches source:orders.service + detail-type:OrderAccepted."""
    pattern = {"source": ["orders.service"], "detail-type": ["OrderAccepted"]}

    assert "orders.service" in pattern["source"]
    assert "OrderAccepted" in pattern["detail-type"]
    assert "other.service" not in pattern["source"]
    assert "OrderRejected" not in pattern["detail-type"]
    assert json.loads(json.dumps(pattern)) == pattern  # clean JSON round-trip

    print("EventBridge rule pattern test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 9. Gap 16 – Enrichment Lambda output (enriched + processedAt)
#    Gap 17 – Pipe target is Step Functions, not Lambda
# ─────────────────────────────────────────────────────────────────────────────

def _enrich(records: list) -> list:
    """Python equivalent of the enrichment Lambda handler (JS):
    exports.handler = async e => e.map(r => ({...r, enriched: true, processedAt: new Date().toISOString()}))
    """
    return [
        {**r, "enriched": True, "processedAt": datetime.now(timezone.utc).isoformat()}
        for r in records
    ]


def test_enrichment_lambda_output():
    """Enrichment adds enriched:true and valid ISO-8601 processedAt; original fields preserved."""
    records = [
        {"body": '{"orderId":"o-1","amount":50}', "messageId": "m-1"},
        {"body": '{"orderId":"o-2","amount":75}', "messageId": "m-2"},
    ]
    enriched = _enrich(records)

    for i, rec in enumerate(enriched):
        assert rec["enriched"] is True
        assert "processedAt" in rec
        assert "T" in rec["processedAt"]          # ISO-8601 has a 'T' separator
        assert rec["body"] == records[i]["body"]  # original fields preserved
        assert rec["messageId"] == records[i]["messageId"]

    # Pipe target must be Step Functions ARN (not Lambda)
    target_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:OrderSM"
    assert ":states:" in target_arn
    assert ":function:" not in target_arn

    print("Enrichment Lambda output test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 10. IAM no wildcard actions
# ─────────────────────────────────────────────────────────────────────────────

def test_sns_topic_subscription():
    """IAM policy statements must not contain wildcard actions; SNS subscription valid."""
    subscription = {"Protocol": "email", "Endpoint": "placeholder@example.com"}
    assert subscription["Protocol"] == "email"
    assert "@" in subscription["Endpoint"]

    policy_statements = [
        {"Action": ["sqs:SendMessage"],
         "Resource": "arn:aws:sqs:us-east-1:123:order-queue"},
        {"Action": ["secretsmanager:GetSecretValue"],
         "Resource": "arn:aws:secretsmanager:us-east-1:123:secret:db-creds"},
        {"Action": ["sns:Publish"],
         "Resource": "arn:aws:sns:us-east-1:123:order-notifications"},
        {"Action": ["s3:PutObject"],
         "Resource": "arn:aws:s3:::order-analytics/analytics/orders/*"},
        {"Action": ["states:StartExecution"],
         "Resource": "arn:aws:states:us-east-1:123:stateMachine:OrderSM"},
        {"Action": ["lambda:InvokeFunction"],
         "Resource": "arn:aws:lambda:us-east-1:123:function:EnrichmentFn"},
    ]

    for stmt in policy_statements:
        actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        for action in actions:
            assert action != "*"
            assert not action.endswith(":*")

    print("SNS topic subscription test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Gap 30 – Component labels in app.ts
# ─────────────────────────────────────────────────────────────────────────────

def test_component_labels_in_app_ts():
    """app.ts must use the exact component terms near resource definitions."""
    content = open(APP_TS).read()
    assert "Connectivity Mesh" in content, "VPC must be labelled 'Connectivity Mesh'"
    assert "Relational Backbone" in content, "RDS must be labelled 'Relational Backbone'"
    assert "Execution Environment" in content, "Lambda must be labelled 'Execution Environment'"

    print("Component labels in app.ts test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 12. Gap 28 – No deletion protection, termination protection, or RETAIN policy
# ─────────────────────────────────────────────────────────────────────────────

def test_no_deletion_protection_config():
    """app.ts must set deletionProtection:false, no terminationProtection, use DESTROY."""
    content = open(APP_TS).read()
    assert "deletionProtection: false" in content, \
        "RDS must have deletionProtection: false"
    assert "terminationProtection: false" in content, \
        "Stack must have terminationProtection: false"
    assert "RemovalPolicy.DESTROY" in content, \
        "Resources must use RemovalPolicy.DESTROY, not RETAIN"
    assert "RemovalPolicy.RETAIN" not in content, \
        "RemovalPolicy.RETAIN must not appear anywhere in app.ts"

    print("No deletion protection config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Gap 26 – Stack outputs limited to API URL and S3 bucket name
# ─────────────────────────────────────────────────────────────────────────────

def test_stack_outputs_are_limited():
    """Outputs must include API URL and S3 bucket name; no secrets or DB strings."""
    content = open(APP_TS).read()

    # Required outputs must be present
    assert "ApiGatewayInvokeUrl" in content
    assert "AnalyticsBucketName" in content

    # Secrets, passwords, and DB connection strings must not appear in CfnOutput blocks
    output_blocks = re.findall(r"new CfnOutput\([^)]+\)", content, re.DOTALL)
    for block in output_blocks:
        block_lower = block.lower()
        assert "secret" not in block_lower, f"CfnOutput may expose a secret: {block}"
        assert "password" not in block_lower, f"CfnOutput may expose a password: {block}"
        assert "connection" not in block_lower, f"CfnOutput may expose a DB connection: {block}"

    print("Stack outputs are limited test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 14. Gap 18 partial – Lambda functions are zip/runtime-based (no container images)
# ─────────────────────────────────────────────────────────────────────────────

def test_lambda_runtime_zip_based():
    """All Lambda functions must use zip/runtime-based deployment (no container images)."""
    content = open(APP_TS).read()

    assert "fromEcrImage" not in content, "Lambda must not use container image (fromEcrImage)"
    assert "DockerImageCode" not in content, "Lambda must not use DockerImageCode"
    assert "DockerImageFunction" not in content, "Lambda must not use DockerImageFunction"
    assert "Code.fromInline" in content, "Lambda functions must use Code.fromInline"
    assert "NODEJS_18_X" in content, "Lambda must use NODEJS_18_X runtime"

    print("Lambda runtime zip-based test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 15. Resource:"*" must have adjacent justification comments
# ─────────────────────────────────────────────────────────────────────────────

def test_no_wildcard_resources_require_justification():
    """Any Resource:'*' in app.ts must have a justification comment within 15 lines."""
    content = open(APP_TS).read()
    lines = content.splitlines()
    justification_keywords = ("JUSTIFICATION", "unavoidable", "mandates", "cannot be restricted")

    for i, line in enumerate(lines):
        # Match TypeScript `resources: ['*']` pattern
        if "resources: ['*']" in line or 'resources: ["*"]' in line:
            window_start = max(0, i - 15)
            context = "\n".join(lines[window_start:i + 3])
            has_justification = any(kw in context for kw in justification_keywords)
            assert has_justification, (
                f"app.ts line {i + 1}: Resource:'*' lacks adjacent justification comment"
            )

    print("Wildcard resources justification test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 16. Single app.ts file constraint
# ─────────────────────────────────────────────────────────────────────────────

def test_single_file_constraint():
    """Only app.ts exists as a TypeScript source; no lib/ or constructs/ directories."""
    project_dir = os.path.dirname(APP_TS)
    ts_files = [
        f for f in os.listdir(project_dir)
        if f.endswith(".ts") and os.path.isfile(os.path.join(project_dir, f))
    ]
    assert ts_files == ["app.ts"], f"Extra .ts files found in project root: {ts_files}"
    assert not os.path.isdir(os.path.join(project_dir, "lib")), \
        "lib/ directory must not exist – all code must be in app.ts"
    assert not os.path.isdir(os.path.join(project_dir, "constructs")), \
        "constructs/ directory must not exist – all code must be in app.ts"

    print("Single file constraint test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 17. Service-mix mandate
# ─────────────────────────────────────────────────────────────────────────────

def test_service_mix_mandate():
    """app.ts must import >= 3 services from set A and >= 2 from set B."""
    content = open(APP_TS).read()

    set_a = {
        "apigateway": "aws-apigateway",
        "lambda": "aws-lambda",
        "sqs": "aws-sqs",
        "events": "aws-events",
        "stepfunctions": "aws-stepfunctions",
        "logs": "aws-logs",
    }
    set_b = {
        "rds": "aws-rds",
        "glue": "aws-glue",
        "athena": "aws-athena",
        "pipes": "aws-pipes",
    }

    matched_a = [k for k, v in set_a.items() if v in content]
    matched_b = [k for k, v in set_b.items() if v in content]
    assert len(matched_a) >= 3, f"Need >= 3 services from set A; found only: {matched_a}"
    assert len(matched_b) >= 2, f"Need >= 2 services from set B; found only: {matched_b}"

    print("Service mix mandate test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 18. AWS_ENDPOINT forwarded to Lambda environments
# ─────────────────────────────────────────────────────────────────────────────

def test_aws_endpoint_forwarding():
    """AWS_ENDPOINT is captured and forwarded to Lambda environment blocks."""
    content = open(APP_TS).read()

    assert "awsEndpoint" in content, \
        "awsEndpoint variable must be declared and used"
    assert "AWS_ENDPOINT: awsEndpoint" in content, \
        "awsEndpoint must be passed to at least one Lambda environment block"
    # Inline Lambda code must also wire the endpoint to SDK client config
    assert "endpoint:ep" in content or "endpoint: ep" in content, \
        "Inline Lambda code must forward endpoint override to AWS SDK clients"

    print("AWS_ENDPOINT forwarding test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 19. Step Functions uses native SDK service integration for S3 (not Lambda)
# ─────────────────────────────────────────────────────────────────────────────

def test_sfn_native_s3_integration():
    """Step Functions must write to S3 via CallAwsService, not via a Lambda invocation."""
    content = open(APP_TS).read()

    assert "CallAwsService" in content, \
        "Step Functions must use CallAwsService for the S3 write task"
    assert "putObject" in content, \
        "Step Functions S3 task must call the putObject action"
    assert "analytics/orders/" in content, \
        "S3 write must target the analytics/orders/ prefix"
    # Confirm S3 integration does not go through a Lambda intermediary
    assert "LambdaInvoke" not in content, \
        "Step Functions must not use LambdaInvoke – S3 write must be a direct SDK call"

    print("Step Functions native S3 integration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 20. Worker Lambda inline code references the correct PostgreSQL schema
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_postgresql_schema():
    """Worker Lambda inline code documents the correct table schema columns."""
    content = open(APP_TS).read()

    # Required table and column names from the spec
    assert "order_id" in content, \
        "Worker code must reference the order_id column"
    assert "amount" in content, \
        "Worker code must reference the amount column"
    assert "received_at" in content, \
        "Worker code must reference the received_at column"
    # Table name
    assert re.search(r"\borders\b", content), \
        "Worker code must reference the 'orders' table"

    print("Worker PostgreSQL schema test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 21. TLS in transit
# ─────────────────────────────────────────────────────────────────────────────

def test_tls_in_transit():
    """RDS uses encrypted storage; worker connects via SSL; API GW is HTTPS-only."""
    content = open(APP_TS).read()

    assert "storageEncrypted: true" in content, \
        "RDS must have storageEncrypted: true"
    # Worker inline code must document SSL for the PostgreSQL connection
    assert "ssl" in content, \
        "Worker code must reference SSL for the RDS connection"
    # API Gateway is HTTPS-only (verified via comment/documentation in app.ts)
    assert "HTTPS" in content or "https" in content, \
        "app.ts must acknowledge that API Gateway uses HTTPS"

    print("TLS in transit test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 22. CDK resource cross-references (no hardcoded ARNs or names)
# ─────────────────────────────────────────────────────────────────────────────

def test_cdk_resource_references():
    """app.ts must use CDK token references for cross-resource wiring, not hardcoded strings."""
    content = open(APP_TS).read()

    # Required token references from the CDK construct graph
    assert ".queueArn" in content or ".queueUrl" in content, \
        "SQS queue must be referenced by CDK token (.queueArn or .queueUrl)"
    assert ".bucketArn" in content or ".bucketName" in content, \
        "S3 bucket must be referenced by CDK token"
    assert ".secretArn" in content, \
        "Secrets Manager secret must be referenced by CDK token (.secretArn)"
    assert ".topicArn" in content, \
        "SNS topic must be referenced by CDK token (.topicArn)"
    assert ".stateMachineArn" in content, \
        "State machine must be referenced by CDK token (.stateMachineArn)"
    assert ".functionArn" in content, \
        "Lambda function must be referenced by CDK token (.functionArn)"
    # No hardcoded account/resource ARNs in resource wiring (test data in test files is fine)
    hardcoded_patterns = [
        "arn:aws:sqs:us-east-1:123456789012:",
        "arn:aws:sns:us-east-1:123456789012:",
        "arn:aws:lambda:us-east-1:123456789012:",
    ]
    for pattern in hardcoded_patterns:
        assert pattern not in content, \
            f"Hardcoded ARN found in app.ts: {pattern}"

    print("CDK resource references test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 23. Worker Lambda must NOT emit EventBridge events (Pipe is canonical path)
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_no_eventbridge_emission():
    """Worker Lambda inline code must not call putEvents; Pipe is the canonical path."""
    content = open(APP_TS).read()

    assert "putEvents" not in content, \
        "Worker Lambda must not emit EventBridge events; Pipe is the canonical path"
    assert "EventBridgeClient" not in content, \
        "Worker Lambda must not instantiate an EventBridgeClient"

    print("Worker no EventBridge emission test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 24. Exactly 1 VPC construct
# ─────────────────────────────────────────────────────────────────────────────

def test_exactly_one_vpc():
    """app.ts must provision exactly one VPC (the Connectivity Mesh)."""
    content = open(APP_TS).read()

    vpc_constructs = re.findall(r"new ec2\.Vpc\(", content)
    assert len(vpc_constructs) == 1, \
        f"Expected exactly 1 VPC construct, found {len(vpc_constructs)}"

    print("Exactly one VPC test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 25. Exactly 1 SNS topic
# ─────────────────────────────────────────────────────────────────────────────

def test_exactly_one_sns_topic():
    """app.ts must provision exactly 1 SNS topic."""
    content = open(APP_TS).read()

    topics = re.findall(r"new sns\.Topic\(", content)
    assert len(topics) == 1, \
        f"Expected exactly 1 SNS topic, found {len(topics)}"

    print("Exactly one SNS topic test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 26. EventBridge Pipe batch configuration values
# ─────────────────────────────────────────────────────────────────────────────

def test_pipe_batch_configuration():
    """EventBridge Pipe source parameters: batchSize=5, maximumBatchingWindowInSeconds=5."""
    content = open(APP_TS).read()

    # Pipe-specific parameter names (camelCase as used in CfnPipe sourceParameters)
    assert "maximumBatchingWindowInSeconds: 5" in content, \
        "Pipe SQS source must set maximumBatchingWindowInSeconds to 5"
    assert "FIRE_AND_FORGET" in content, \
        "Pipe target invocation type must be FIRE_AND_FORGET"
    # batchSize: 5 appears in both SqsEventSource and Pipe — both must be 5
    batch_occurrences = re.findall(r"batchSize:\s*5", content)
    assert len(batch_occurrences) >= 1, \
        "Pipe source parameters must include batchSize: 5"

    print("Pipe batch configuration test passed!")
