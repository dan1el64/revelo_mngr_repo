import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError


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
    assert "unsafeUnwrap(" not in app_source
    assert "AWS_ENDPOINT" in app_source
    assert "AWS_REGION" in app_source
    assert "Retain" not in template_text
    assert "OrdersApiUrl" in stack_template_json.get("Outputs", {})


def test_vpc_and_security_groups_are_deployed_correctly(
    stack_resources: dict[str, dict[str, Any]],
    stack_template_json: dict[str, Any],
) -> None:
    ec2 = _client("ec2")
    vpc_id = _stack_resource(stack_resources, "AWS::EC2::VPC", "VpcFabric")["PhysicalResourceId"]
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]

    assert len(subnets) >= 3
    public_subnets = [subnet for subnet in subnets if subnet.get("MapPublicIpOnLaunch")]
    private_subnets = [subnet for subnet in subnets if not subnet.get("MapPublicIpOnLaunch")]
    template_subnets = [
        resource["Properties"]
        for resource in _template_resources(stack_template_json).values()
        if resource["Type"] == "AWS::EC2::Subnet"
    ]
    assert len([subnet for subnet in template_subnets if subnet["MapPublicIpOnLaunch"]]) == 2
    assert len([subnet for subnet in template_subnets if not subnet["MapPublicIpOnLaunch"]]) == 2
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
    assert len(compute_group["IpPermissionsEgress"]) >= 1
    assert any(
        permission["IpProtocol"] == "-1"
        and any(range_item.get("CidrIp") == "0.0.0.0/0" for range_item in permission.get("IpRanges", []))
        for permission in compute_group["IpPermissionsEgress"]
    )
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


def test_post_orders_creates_an_audited_order_record(
    stack_resources: dict[str, dict[str, Any]],
) -> None:
    lambda_client = _client("lambda")
    s3 = _client("s3")
    sqs = _client("sqs")
    orders_function_name = _stack_resource(stack_resources, "AWS::Lambda::Function", "OrdersHandler")["PhysicalResourceId"]
    queue_identifier = _stack_resource(stack_resources, "AWS::SQS::Queue", "OrderEventsQueue")["PhysicalResourceId"]
    queue_url = _queue_url(sqs, queue_identifier)

    sqs.purge_queue(QueueUrl=queue_url)
    time.sleep(1)

    response = lambda_client.invoke(
        FunctionName=orders_function_name,
        Payload=json.dumps({"httpMethod": "POST", "path": "/orders"}).encode("utf-8"),
    )
    payload = json.loads(response["Payload"].read().decode("utf-8"))
    assert response["StatusCode"] == 200
    assert payload["statusCode"] == 202
    body = json.loads(payload["body"])
    assert body["accepted"] is True
    assert body["payload"]["orderId"]
    assert body["payload"]["timestamp"]

    message = _wait_for(
        lambda: (
            sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
            ).get("Messages") or [None]
        )[0],
        timeout_seconds=20,
        sleep_seconds=1,
    )
    message_body = json.loads(message["Body"])
    assert message_body["orderId"] == body["payload"]["orderId"]
    assert message_body["timestamp"] == body["payload"]["timestamp"]
    assert message_body["kind"] == "order-created"


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

    assert len(statements) == 5
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
    ec2_statement = next(
        statement for statement in statements if "ec2:CreateNetworkInterface" in _actions(statement)
    )

    assert secret_statement["Resource"] == secret_arn
    assert template_connection_name in str(connection_statement["Resource"])
    assert ":database/" in str(catalog_statement["Resource"])
    assert ":table/" in str(catalog_statement["Resource"])
    assert "/aws-glue/" in str(logs_statement["Resource"])
    assert ec2_statement["Resource"] == "*"


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
