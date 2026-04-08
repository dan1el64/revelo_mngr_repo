"""
Unit tests for individual stack components.

Pure Python only (json, os, re, unittest.mock) – no moto, no boto3.
Compatible with any environment that has pytest installed.
"""

import os
import re
import pytest
from unittest.mock import patch

# Path to the CDK app source; used for static analysis
APP_TS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "app.ts"))


def _read_app_ts() -> str:
    """Read app.ts and return its content; fail the test cleanly if the file is absent."""
    if not os.path.isfile(APP_TS):
        pytest.fail(f"app.ts not found at expected path: {APP_TS}")
    return open(APP_TS).read()


def _code_lines(content: str) -> str:
    """Return content with every // comment-only line stripped.

    Prevents assertions from matching token names or patterns that appear only
    inside inline comments, avoiding false positives and false negatives.
    """
    return "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("//")
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Default region – env var + app.ts confirmation
# ─────────────────────────────────────────────────────────────────────────────

def test_cdk_app_initialization():
    """AWS_REGION defaults to us-east-1 when the env var is absent; app.ts reads it."""
    env_no_region = {k: v for k, v in os.environ.items() if k != "AWS_REGION"}
    with patch.dict(os.environ, env_no_region, clear=True):
        region = os.environ.get("AWS_REGION", "us-east-1")
        assert region == "us-east-1"

    # app.ts must also express this default (static analysis confirms the pattern)
    code = _code_lines(_read_app_ts())
    assert "AWS_REGION" in code, \
        "app.ts must read the AWS_REGION env var for the deployment region"
    assert "us-east-1" in code, \
        "app.ts must fall back to 'us-east-1' when AWS_REGION is absent"

    print("CDK app initialization test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Env var forwarding – os.environ + app.ts confirmation
# ─────────────────────────────────────────────────────────────────────────────

def test_environment_variables_handling():
    """AWS_REGION and AWS_ENDPOINT are read from env and forwarded in app.ts."""
    custom_endpoint = "http://custom-aws-endpoint.example.com:8080"
    with patch.dict(os.environ, {"AWS_REGION": "us-west-2", "AWS_ENDPOINT": custom_endpoint}):
        assert os.environ.get("AWS_REGION", "us-east-1") == "us-west-2"
        assert os.environ.get("AWS_ENDPOINT") == custom_endpoint

    # app.ts must capture and forward both variables
    code = _code_lines(_read_app_ts())
    assert "AWS_ENDPOINT" in code, \
        "app.ts must read AWS_ENDPOINT for SDK endpoint override"
    assert "AWS_REGION" in code, \
        "app.ts must read AWS_REGION for the deployment region"

    print("Environment variables handling test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 3. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY referenced in app.ts
# ─────────────────────────────────────────────────────────────────────────────

def test_aws_credentials_accepted():
    """AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are referenced in app.ts."""
    content = _read_app_ts()
    assert "AWS_REGION" in content
    assert "AWS_ENDPOINT" in content
    assert "AWS_ACCESS_KEY_ID" in content
    assert "AWS_SECRET_ACCESS_KEY" in content

    print("AWS credentials accepted test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 4. No extra configuration variables beyond the four specified
# ─────────────────────────────────────────────────────────────────────────────

def test_no_extra_config_variables():
    """app.ts must not introduce extra top-level config variables."""
    code = _code_lines(_read_app_ts())
    forbidden = ["process.env.DATABASE_URL", "process.env.DB_PASSWORD",
                 "process.env.STACK_NAME", "process.env.AWS_ACCOUNT_ID"]
    for var in forbidden:
        assert var not in code, f"Forbidden extra config variable found: {var}"

    print("No extra config variables test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 5. VPC topology – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_vpc_configuration():
    """app.ts must configure the VPC with maxAzs:2, natGateways:1, public + private subnets."""
    code = _code_lines(_read_app_ts())

    assert "maxAzs: 2" in code, "VPC must set maxAzs: 2"
    assert "natGateways: 1" in code, "VPC must set natGateways: 1"
    assert "SubnetType.PUBLIC" in code, "VPC must configure public subnets"
    assert "PRIVATE_WITH_EGRESS" in code, "VPC must configure PRIVATE_WITH_EGRESS subnets"

    print("VPC configuration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 6. RDS configuration – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_rds_configuration():
    """app.ts must configure RDS with VER_15, GP2 storage, 20 GiB, not publicly accessible."""
    code = _code_lines(_read_app_ts())

    assert "VER_15" in code, "RDS engine must be PostgreSQL version 15 (VER_15)"
    assert "GP2" in code, "RDS must use gp2 storage type"
    assert "allocatedStorage: 20" in code, "RDS must allocate exactly 20 GiB storage"
    assert "publiclyAccessible: false" in code, "RDS must not be publicly accessible"

    print("RDS configuration test passed!")


# ─���───────────────────────────────────────────────────────────────────────────
# 7. RDS private subnet placement – vpcSubnets token in RDS construct
# ─────────────────────────────────────────────────────────────────────────────

def test_rds_private_subnet_placement():
    """RDS DatabaseInstance must specify vpcSubnets with PRIVATE_WITH_EGRESS subnet type."""
    content = _read_app_ts()

    # Extract the DatabaseInstance construct block
    db_match = re.search(
        r"new rds\.DatabaseInstance\(.*?\)\s*;",
        content,
        re.DOTALL,
    )
    assert db_match, "new rds.DatabaseInstance(...) construct not found in app.ts"
    db_block = db_match.group(0)
    db_code = _code_lines(db_block)

    assert "vpcSubnets" in db_code, \
        "RDS DatabaseInstance must specify vpcSubnets"
    assert "PRIVATE_WITH_EGRESS" in db_code, \
        "RDS must be placed in PRIVATE_WITH_EGRESS subnets"

    print("RDS private subnet placement test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 8. S3 configuration – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_s3_configuration():
    """app.ts must enable versioning and use SSE-S3 (S3_MANAGED) on the analytics bucket."""
    code = _code_lines(_read_app_ts())

    assert "versioned: true" in code, "S3 bucket must have versioned: true"
    assert "S3_MANAGED" in code, "S3 bucket must use BucketEncryption.S3_MANAGED (SSE-S3)"

    print("S3 configuration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 9. S3 bucket must not configure object lock or bucket retention
# ─────────────────────────────────────────────────────────────────────────────

def test_s3_no_object_lock_or_retention():
    """app.ts S3 bucket must not configure object lock or retention (RemovalPolicy.DESTROY)."""
    code = _code_lines(_read_app_ts())

    assert "objectLockEnabled" not in code, \
        "S3 bucket must not enable Object Lock"
    assert "objectLockDefaultRetention" not in code, \
        "S3 bucket must not configure default Object Lock retention"
    assert "RemovalPolicy.RETAIN" not in code, \
        "No resource may use RemovalPolicy.RETAIN – bucket must be destroyable"

    print("S3 no object lock/retention test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 10. SQS queue configuration – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_sqs_queue_configuration():
    """app.ts must set SQS visibility to Duration.seconds(60) and retention to Duration.days(4)."""
    code = _code_lines(_read_app_ts())

    assert "Duration.seconds(60)" in code, \
        "SQS visibility timeout must be Duration.seconds(60)"
    assert "Duration.days(4)" in code, \
        "SQS message retention must be Duration.days(4)"

    print("SQS queue configuration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Athena WorkGroup configuration – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_athena_workgroup_config():
    """app.ts must set enforceWorkGroupConfiguration:true and use a CDK token for output location."""
    code = _code_lines(_read_app_ts())

    assert "enforceWorkGroupConfiguration: true" in code, \
        "Athena WorkGroup must set enforceWorkGroupConfiguration: true"
    # Result location must use a CDK token (bucketName) not a hardcoded string
    assert "athena-results/" in code, \
        "Athena WorkGroup result location must include 'athena-results/' prefix"
    assert re.search(r"analyticsBucket\.bucketName.*athena-results|athena-results.*analyticsBucket\.bucketName", code), \
        "Athena result location must reference the analytics bucket via CDK token (.bucketName)"

    print("Athena WorkGroup config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 12. Glue crawler schedule – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_glue_crawler_schedule_config():
    """app.ts must configure the Glue crawler with schedule cron(0/30 * * * ? *)."""
    code = _code_lines(_read_app_ts())

    assert "cron(0/30 * * * ? *)" in code, \
        "Glue crawler must use scheduleExpression 'cron(0/30 * * * ? *)'"

    print("Glue crawler schedule config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Glue crawler IAM role scoping – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_glue_crawler_iam_role_scoping():
    """Glue crawler role must grant S3 read-only on the specific bucket via CDK token."""
    code = _code_lines(_read_app_ts())

    # Glue role must grant S3 read actions
    assert "s3:GetObject" in code, \
        "Glue crawler role must grant s3:GetObject"
    assert "s3:ListBucket" in code, \
        "Glue crawler role must grant s3:ListBucket"
    # Resources must reference the CDK bucket token, not a hardcoded name
    assert "analyticsBucket.bucketArn" in code, \
        "Glue crawler role S3 resource must use analyticsBucket.bucketArn CDK token"
    # Glue catalog write access must be scoped to the specific database
    assert "order_analytics" in code, \
        "Glue role catalog resources must be scoped to the 'order_analytics' database"
    # Must NOT grant s3:PutObject to the Glue role
    glue_role_match = re.search(
        r"const glueRole\s*=.*?(?=const glueCrawler\s*=)",
        code, re.DOTALL,
    )
    if glue_role_match:
        glue_role_block = glue_role_match.group(0)
        assert "s3:PutObject" not in glue_role_block, \
            "Glue crawler role must not grant s3:PutObject (read-only on analytics/)"
        assert "glue:DeleteTable" not in glue_role_block, \
            "Glue crawler role must not grant glue:DeleteTable (least-privilege)"
        assert "glue:DeletePartition" not in glue_role_block, \
            "Glue crawler role must not grant glue:DeletePartition (least-privilege)"

    print("Glue crawler IAM role scoping test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 14. Worker SQS event source configuration – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_sqs_event_source_config():
    """app.ts must wire SqsEventSource to WorkerFn with batchSize:5 and maxBatchingWindow:5s."""
    code = _code_lines(_read_app_ts())

    assert "SqsEventSource" in code, "Worker Lambda must use SqsEventSource"
    assert "batchSize: 5" in code, "SqsEventSource must set batchSize: 5"
    assert "maxBatchingWindow: Duration.seconds(5)" in code, \
        "SqsEventSource must set maxBatchingWindow: Duration.seconds(5)"

    print("Worker SQS event source config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 15. Worker Lambda IAM role scoping – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_iam_role_scoping():
    """Worker Lambda role must use CDK tokens for secret and queue ARNs (not hardcoded)."""
    code = _code_lines(_read_app_ts())

    # Worker role for SM must reference dbSecret.secretArn (CDK token)
    assert "dbSecret.secretArn" in code, \
        "Worker role SM policy must scope resource to dbSecret.secretArn CDK token"
    # Worker role for SQS must reference orderQueue.queueArn (CDK token)
    assert "orderQueue.queueArn" in code, \
        "Worker role SQS policy must scope resource to orderQueue.queueArn CDK token"

    print("Worker IAM role scoping test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 16. Request handler validation – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_request_handler_validation():
    """app.ts request handler inline code must validate types and return 202 or 400."""
    code = _code_lines(_read_app_ts())

    assert re.search(r"statusCode\s*:\s*202", code), \
        "Request handler must return statusCode 202 on valid input"
    assert re.search(r"statusCode\s*:\s*400", code), \
        "Request handler must return statusCode 400 on invalid input"
    assert "typeof" in code, \
        "Request handler must use typeof for runtime type validation"

    print("Request handler validation test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 17. EventBridge Pipe construct present in app.ts
# ─────────────────────────────────────────────────────────────────────────────

def test_eventbridge_pipe_construct_present():
    """app.ts must instantiate a CfnPipe construct (EventBridge Pipe)."""
    code = _code_lines(_read_app_ts())

    pipe_constructs = re.findall(r"new pipes\.CfnPipe\(", code)
    assert len(pipe_constructs) >= 1, \
        "app.ts must contain at least one new pipes.CfnPipe(...) construct"

    print("EventBridge Pipe construct present test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 18. EventBridge rule wired to Step Functions target
# ─────────────────────────────────────────────────────────────────────────────

def test_eventbridge_rule_targets_sfn():
    """app.ts EventBridge rule must target the Step Functions state machine."""
    code = _code_lines(_read_app_ts())

    assert "SfnStateMachine" in code, \
        "EventBridge rule must use SfnStateMachine as target"
    assert "stateMachine" in code, \
        "EventBridge rule target must reference the stateMachine CDK construct"

    print("EventBridge rule targets SFN test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 19. EventBridge rule pattern – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_eventbridge_rule_in_app():
    """app.ts must define an EventBridge rule matching source:orders.service + OrderAccepted."""
    code = _code_lines(_read_app_ts())

    assert "orders.service" in code, \
        "EventBridge rule must match source 'orders.service'"
    assert "OrderAccepted" in code, \
        "EventBridge rule must match detail-type 'OrderAccepted'"

    print("EventBridge rule in app.ts test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 20. Step Functions state machine has both S3 write AND SNS publish tasks
# ─────────────────────────────────────────────────────────────────────────────

def test_sfn_definition_has_s3_and_sns_tasks():
    """app.ts state machine definition must chain an S3 write Task and an SNS publish Task."""
    code = _code_lines(_read_app_ts())

    # S3 write via CallAwsService
    assert "CallAwsService" in code, \
        "SFN must use CallAwsService for the S3 write task"
    assert "putObject" in code, \
        "SFN S3 task must invoke the putObject action"
    # SNS publish via SnsPublish construct
    assert "SnsPublish" in code, \
        "SFN must use SnsPublish task for the notification step"
    # Both tasks must be chained (writeToS3Task.next or equivalent)
    assert ".next(" in code, \
        "SFN tasks must be chained with .next() (S3 write then SNS publish)"

    print("SFN definition S3+SNS tasks test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 21. Enrichment Lambda inline code – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_enrichment_lambda_config():
    """app.ts enrichment Lambda inline code must add enriched:true and processedAt."""
    code = _code_lines(_read_app_ts())

    assert re.search(r"enriched\s*:\s*true", code), \
        "Enrichment Lambda must set enriched: true on each record"
    assert "processedAt" in code, \
        "Enrichment Lambda must add a processedAt field"
    assert "toISOString()" in code, \
        "Enrichment Lambda must use toISOString() for the timestamp"

    print("Enrichment Lambda config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 22. SNS email subscription – static analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_sns_email_subscription():
    """app.ts must configure an SNS EmailSubscription to placeholder@example.com."""
    code = _code_lines(_read_app_ts())

    assert "EmailSubscription" in code, \
        "SNS topic must have an EmailSubscription"
    assert "placeholder@example.com" in code, \
        "SNS EmailSubscription endpoint must be placeholder@example.com"

    print("SNS email subscription test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 23. Wildcard detector negative-path: synthetic + real app.ts
# ─────────────────────────────────────────────────────────────────────────────

def test_wildcard_detector_catches_unjustified():
    """Wildcard detector must flag unjustified wildcards; real app.ts wildcards must be justified."""
    justification_keywords = ("JUSTIFICATION", "unavoidable", "mandates", "cannot be restricted")

    def _check(lines_text: str) -> list:
        """Return list of line numbers where Resource:'*' lacks a justification."""
        lines = lines_text.splitlines()
        violations = []
        for i, line in enumerate(lines):
            if re.search(r"resources:\s*\[['\"]\*['\"]\]", line):
                window_start = max(0, i - 15)
                context = "\n".join(lines[window_start:i + 3])
                if not any(kw in context for kw in justification_keywords):
                    violations.append(i + 1)
        return violations

    # Negative path: synthetic unjustified wildcard must be caught
    unjustified = "\n".join([
        "const policy = new iam.PolicyStatement({",
        "  actions: ['logs:PutLogEvents'],",
        "  resources: ['*'],",
        "});",
    ])
    assert _check(unjustified), \
        "Wildcard detector must flag Resource:'*' without a justification"

    # Positive path: real app.ts must have no unjustified wildcards
    content = _read_app_ts()
    violations = _check(content)
    assert not violations, \
        f"app.ts has unjustified Resource:'*' at lines: {violations}"

    print("Wildcard detector test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 24. Component labels in app.ts
# ─────────────────────────────────────────────────────────────────────────────

def test_component_labels_in_app_ts():
    """app.ts must use the exact component terms near resource definitions."""
    content = _read_app_ts()
    assert "Connectivity Mesh" in content, "VPC must be labelled 'Connectivity Mesh'"
    assert "Relational Backbone" in content, "RDS must be labelled 'Relational Backbone'"
    assert "Execution Environment" in content, "Lambda must be labelled 'Execution Environment'"

    print("Component labels in app.ts test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 25. No deletion protection, no terminationProtection:true, use DESTROY
# ─────────────────────────────────────────────────────────────────────────────

def test_no_deletion_protection_config():
    """app.ts must not set terminationProtection:true; RDS must have deletionProtection:false."""
    content = _read_app_ts()
    code = _code_lines(content)

    assert "deletionProtection: false" in code, \
        "RDS must have deletionProtection: false"
    assert "terminationProtection: true" not in code, \
        "Stack must not have terminationProtection: true"
    assert "RemovalPolicy.DESTROY" in code, \
        "Resources must use RemovalPolicy.DESTROY"
    assert "RemovalPolicy.RETAIN" not in code, \
        "RemovalPolicy.RETAIN must not appear anywhere in app.ts"

    print("No deletion protection config test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 26. Stack outputs limited to API URL and S3 bucket name
# ─────────────────────────────────────────────────────────────────────────────

def test_stack_outputs_are_limited():
    """Outputs must include API URL and S3 bucket name; no secrets or DB strings."""
    content = _read_app_ts()

    assert "ApiGatewayInvokeUrl" in content
    assert "AnalyticsBucketName" in content

    output_blocks = re.findall(r"new CfnOutput\([^)]+\)", content, re.DOTALL)
    for block in output_blocks:
        block_lower = block.lower()
        assert "secret" not in block_lower, f"CfnOutput may expose a secret: {block}"
        assert "password" not in block_lower, f"CfnOutput may expose a password: {block}"
        assert "connection" not in block_lower, f"CfnOutput may expose a DB connection: {block}"

    print("Stack outputs are limited test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 27. Lambda functions are zip/runtime-based (no container images)
# ─────────────────────────────────────────────────────────────────────────────

def test_lambda_runtime_zip_based():
    """All Lambda functions must use zip/runtime-based deployment (no container images)."""
    code = _code_lines(_read_app_ts())

    assert "fromEcrImage" not in code, "Lambda must not use fromEcrImage"
    assert "DockerImageCode" not in code, "Lambda must not use DockerImageCode"
    assert "DockerImageFunction" not in code, "Lambda must not use DockerImageFunction"
    assert "Code.fromInline" in code, "Lambda functions must use Code.fromInline"
    assert "NODEJS_18_X" in code, "Lambda must use NODEJS_18_X runtime"

    print("Lambda runtime zip-based test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 28. Resource:"*" must have adjacent justification comments
# ─────────────────────────────────────────────────────────────────────────────

def test_no_wildcard_resources_require_justification():
    """Any resources:['*'] in app.ts must have a justification comment within 15 lines."""
    content = _read_app_ts()
    lines = content.splitlines()
    justification_keywords = ("JUSTIFICATION", "unavoidable", "mandates", "cannot be restricted")

    for i, line in enumerate(lines):
        # Match both single and double-quoted wildcard in a resources array
        if re.search(r"resources:\s*\[['\"]\*['\"]\]", line):
            window_start = max(0, i - 15)
            context = "\n".join(lines[window_start:i + 3])
            has_justification = any(kw in context for kw in justification_keywords)
            assert has_justification, (
                f"app.ts line {i + 1}: resources:['*'] lacks adjacent justification comment"
            )

    print("Wildcard resources justification test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 29. Single app.ts file constraint
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
        "lib/ directory must not exist"
    assert not os.path.isdir(os.path.join(project_dir, "constructs")), \
        "constructs/ directory must not exist"

    print("Single file constraint test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 30. Service-mix mandate
# ─────────────────────────────────────────────────────────────────────────────

def test_service_mix_mandate():
    """app.ts must import >= 3 services from set A and >= 2 from set B."""
    content = _read_app_ts()

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
    assert len(matched_a) >= 3, f"Need >= 3 from set A; found: {matched_a}"
    assert len(matched_b) >= 2, f"Need >= 2 from set B; found: {matched_b}"

    print("Service mix mandate test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 31. AWS_ENDPOINT forwarded to Lambda environments
# ─────────────────────────────────────────────────────────────────────────────

def test_aws_endpoint_forwarding():
    """AWS_ENDPOINT is captured and forwarded to Lambda environment blocks."""
    code = _code_lines(_read_app_ts())

    assert "awsEndpoint" in code, "awsEndpoint variable must be declared and used"
    # normalizedEndpoint is derived from awsEndpoint and passed to Lambda env blocks
    assert "normalizedEndpoint" in code or "AWS_ENDPOINT: awsEndpoint" in code, \
        "awsEndpoint (or its normalized form) must be passed to at least one Lambda environment block"
    assert "endpoint:ep" in code or "endpoint: ep" in code, \
        "Inline Lambda code must forward endpoint override to AWS SDK clients"

    print("AWS_ENDPOINT forwarding test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 32. Step Functions uses native SDK service integration for S3 (not Lambda)
# ─────────────────────────────────────────────────────────────────────────────

def test_sfn_native_s3_integration():
    """Step Functions must write to S3 via CallAwsService, not via a Lambda invocation."""
    code = _code_lines(_read_app_ts())

    assert "CallAwsService" in code, \
        "Step Functions must use CallAwsService for the S3 write task"
    assert "putObject" in code, \
        "Step Functions S3 task must call the putObject action"
    assert "analytics/orders/" in code, \
        "S3 write must target the analytics/orders/ prefix"
    assert "LambdaInvoke" not in code, \
        "Step Functions must not use LambdaInvoke for the S3 write"

    print("Step Functions native S3 integration test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 33. Worker Lambda inline code references the correct PostgreSQL schema
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_postgresql_schema():
    """Worker Lambda inline code documents the correct table schema columns."""
    content = _read_app_ts()

    assert "order_id" in content, "Worker code must reference the order_id column"
    assert "amount" in content, "Worker code must reference the amount column"
    assert "received_at" in content, "Worker code must reference the received_at column"
    assert re.search(r"\borders\b", content), "Worker code must reference the 'orders' table"

    print("Worker PostgreSQL schema test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 34. TLS in transit
# ─────────────────────────────────────────────────────────────────────────────

def test_tls_in_transit():
    """RDS uses encrypted storage; worker connects via SSL; API GW is HTTPS-only."""
    code = _code_lines(_read_app_ts())

    assert "storageEncrypted: true" in code, "RDS must have storageEncrypted: true"
    assert "ssl" in code, "Worker code must reference SSL for the RDS connection"
    assert "HTTPS" in code or "https" in code, \
        "app.ts must acknowledge that API Gateway uses HTTPS"

    print("TLS in transit test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 35. CDK resource cross-references (no hardcoded ARNs)
# ─────────────────────────────────────────────────────────────────────────────

def test_cdk_resource_references():
    """app.ts must use CDK token references for cross-resource wiring, not hardcoded ARNs."""
    code = _code_lines(_read_app_ts())

    assert ".queueArn" in code or ".queueUrl" in code, \
        "SQS queue must be referenced by CDK token"
    assert ".bucketArn" in code or ".bucketName" in code, \
        "S3 bucket must be referenced by CDK token"
    assert ".secretArn" in code, \
        "Secrets Manager secret must be referenced by CDK token (.secretArn)"
    assert ".topicArn" in code, \
        "SNS topic must be referenced by CDK token (.topicArn)"
    assert ".stateMachineArn" in code, \
        "State machine must be referenced by CDK token (.stateMachineArn)"
    assert ".functionArn" in code, \
        "Lambda function must be referenced by CDK token (.functionArn)"

    # Detect hardcoded ARNs via regex (catches any 12-digit account ID, not just one)
    hardcoded_arn_re = re.compile(
        r"arn:aws:(sqs|sns|lambda|states|secretsmanager):[a-z0-9-]+:\d{12}:"
    )
    matches = hardcoded_arn_re.findall(code)
    assert not matches, \
        f"Hardcoded ARNs with account IDs found in app.ts code: {matches}"

    print("CDK resource references test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 36. Worker Lambda must NOT emit EventBridge events (Pipe is canonical path)
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_no_eventbridge_emission():
    """Worker Lambda inline code must not emit EventBridge events; Pipe is the canonical path."""
    content = _read_app_ts()

    # Try to extract just the workerCode block; fall back to full-file check
    worker_match = re.search(
        r"const workerCode\s*=.*?(?=const workerFn\s*=)",
        content,
        re.DOTALL,
    )
    check_block = worker_match.group(0) if worker_match else content
    code = _code_lines(check_block)

    assert "putEvents" not in code, \
        "Worker Lambda must not call putEvents (Pipe is the canonical path to SFN)"
    assert "EventBridgeClient" not in code, \
        "Worker Lambda must not import EventBridgeClient"
    assert "client-eventbridge" not in code, \
        "Worker Lambda must not import @aws-sdk/client-eventbridge"

    print("Worker no EventBridge emission test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 37. Exactly 1 VPC construct
# ─────────────────────────────────────────────────────────────────────────────

def test_exactly_one_vpc():
    """app.ts must provision exactly one VPC."""
    code = _code_lines(_read_app_ts())

    vpc_constructs = re.findall(r"new ec2\.Vpc\(", code)
    assert len(vpc_constructs) == 1, \
        f"Expected exactly 1 VPC construct, found {len(vpc_constructs)}"

    print("Exactly one VPC test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 38. Exactly 1 SNS topic
# ─────────────────────────────────────────────────────────────────────────────

def test_exactly_one_sns_topic():
    """app.ts must provision exactly 1 SNS topic."""
    code = _code_lines(_read_app_ts())

    topics = re.findall(r"new sns\.Topic\(", code)
    assert len(topics) == 1, \
        f"Expected exactly 1 SNS topic, found {len(topics)}"

    print("Exactly one SNS topic test passed!")


# ─────────────────────────────────────────────────────────────────────────────
# 39. EventBridge Pipe batch configuration values
# ─────────────────────────────────────────────────────────────────────────────

def test_pipe_batch_configuration():
    """EventBridge Pipe: batchSize=5, maximumBatchingWindowInSeconds=5, FIRE_AND_FORGET."""
    code = _code_lines(_read_app_ts())

    assert "maximumBatchingWindowInSeconds: 5" in code, \
        "Pipe SQS source must set maximumBatchingWindowInSeconds to 5"
    assert "FIRE_AND_FORGET" in code, \
        "Pipe target invocation type must be FIRE_AND_FORGET"
    assert re.search(r"batchSize:\s*5", code), \
        "Pipe source parameters must include batchSize: 5"

    print("Pipe batch configuration test passed!")
