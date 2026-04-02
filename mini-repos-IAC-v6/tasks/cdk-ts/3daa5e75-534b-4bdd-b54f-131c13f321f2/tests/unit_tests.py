from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
APP_SOURCE = (REPO_DIR / "app.ts").read_text()


def _base_app_env() -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "PATH",
            "HOME",
            "TMPDIR",
            "TMP",
            "TEMP",
            "SHELL",
            "TERM",
            "NPM_CONFIG_CACHE",
            "npm_config_cache",
        }
    }
    env["AWS_REGION"] = os.environ.get("AWS_REGION", "us-east-1")
    return env


def _run_synth(extra_env: dict[str, str] | None = None, *, outdir: str) -> subprocess.CompletedProcess[str]:
    env = _base_app_env()
    if extra_env:
        env.update(extra_env)
    env["CDK_OUTDIR"] = outdir
    return subprocess.run(
        ["npx", "ts-node", "app.ts"],
        cwd=REPO_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _synthesize_template(extra_env: dict[str, str] | None = None) -> tuple[dict, dict]:
    with tempfile.TemporaryDirectory() as tmpdir:
        result = _run_synth(extra_env, outdir=tmpdir)
        assert result.returncode == 0, result.stderr or result.stdout
        template_path = Path(tmpdir) / "OrdersIngestStack.template.json"
        assert template_path.exists(), "Synthesized full-mode template not found"
        manifest_path = Path(tmpdir) / "manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        return json.loads(template_path.read_text()), manifest


def _synthesize_failure(extra_env: dict[str, str]) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        result = _run_synth(extra_env, outdir=tmpdir)
        assert result.returncode != 0
        return result.stderr or result.stdout


def _load_template() -> dict:
    template, _ = _synthesize_template()
    return template


def _resources_by_type_in(template: dict, resource_type: str) -> dict[str, dict]:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def _resources_with_prefix_in(template: dict, prefix: str) -> dict[str, dict]:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if logical_id.startswith(prefix)
    }


TEMPLATE = _load_template()
RESOURCES = TEMPLATE["Resources"]
OUTPUTS = TEMPLATE.get("Outputs", {})


def _resources_by_type(resource_type: str) -> dict[str, dict]:
    return {
        logical_id: resource
        for logical_id, resource in RESOURCES.items()
        if resource["Type"] == resource_type
    }


def _resource_with_prefix(resource_type: str, prefix: str) -> tuple[str, dict]:
    matches = [
        (logical_id, resource)
        for logical_id, resource in _resources_by_type(resource_type).items()
        if logical_id.startswith(prefix)
    ]
    assert len(matches) == 1, f"Expected one {resource_type} with prefix {prefix}, got {matches}"
    return matches[0]


def _lambda_inline_code(prefix: str) -> str:
    _, resource = _resource_with_prefix("AWS::Lambda::Function", prefix)
    return resource["Properties"]["Code"]["ZipFile"]


def _policy_document(prefix: str) -> dict:
    _, resource = _resource_with_prefix("AWS::IAM::Policy", prefix)
    return resource["Properties"]["PolicyDocument"]


def _all_policy_statements() -> list[dict]:
    statements: list[dict] = []
    for resource in _resources_by_type("AWS::IAM::Policy").values():
        policy_statements = resource["Properties"]["PolicyDocument"]["Statement"]
        if isinstance(policy_statements, dict):
            statements.append(policy_statements)
        else:
            statements.extend(policy_statements)
    return statements


def _wildcard_resource_statements(template: dict) -> list[dict]:
    statements: list[dict] = []
    for resource in _resources_by_type_in(template, "AWS::IAM::Policy").values():
        policy_statements = resource["Properties"]["PolicyDocument"]["Statement"]
        if isinstance(policy_statements, dict):
            policy_statements = [policy_statements]
        for statement in policy_statements:
            resources = statement["Resource"]
            if not isinstance(resources, list):
                resources = [resources]
            if "*" in resources:
                statements.append(statement)
    return statements


def _invoke_inline_lambda(prefix: str, *, event: dict, env: dict[str, str]) -> dict:
    node_script = """
const vm = require('node:vm');
const Module = require('node:module');

const code = process.env.INLINE_LAMBDA_CODE;
const event = JSON.parse(process.env.INLINE_LAMBDA_EVENT || '{}');
const calls = [];

class FixedDate extends Date {
  constructor(...args) {
    if (args.length > 0) {
      super(...args);
      return;
    }
    super('2024-01-02T03:04:05.000Z');
  }

  static now() {
    return 1704164645000;
  }
}

class PutObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

class SendMessageCommand {
  constructor(input) {
    this.input = input;
  }
}

class PutEventsCommand {
  constructor(input) {
    this.input = input;
  }
}

class GetSecretValueCommand {
  constructor(input) {
    this.input = input;
  }
}

class PublishCommand {
  constructor(input) {
    this.input = input;
  }
}

function clientFactory(serviceName) {
  return class MockClient {
    async send(command) {
      calls.push({
        service: serviceName,
        command: command.constructor.name,
        input: command.input,
      });
      if (serviceName === 'secretsmanager') {
        return {
          SecretString: JSON.stringify({
            username: 'orders_app',
            dbname: 'orders',
          }),
        };
      }
      if (serviceName === 'eventbridge') {
        return { FailedEntryCount: 0, Entries: [] };
      }
      if (serviceName === 'sns') {
        return { MessageId: 'sns-message-id' };
      }
      if (serviceName === 'sqs') {
        return { MessageId: 'sqs-message-id' };
      }
      return {};
    }
  };
}

const moduleMocks = {
  '@aws-sdk/client-s3': {
    S3Client: clientFactory('s3'),
    PutObjectCommand,
  },
  '@aws-sdk/client-sqs': {
    SQSClient: clientFactory('sqs'),
    SendMessageCommand,
  },
  '@aws-sdk/client-eventbridge': {
    EventBridgeClient: clientFactory('eventbridge'),
    PutEventsCommand,
  },
  '@aws-sdk/client-secrets-manager': {
    SecretsManagerClient: clientFactory('secretsmanager'),
    GetSecretValueCommand,
  },
  '@aws-sdk/client-sns': {
    SNSClient: clientFactory('sns'),
    PublishCommand,
  },
  'node:crypto': {
    randomUUID: () => '123e4567-e89b-12d3-a456-426614174000',
  },
};

const originalLoad = Module._load;
Module._load = function patchedLoad(request, parent, isMain) {
  if (request in moduleMocks) {
    return moduleMocks[request];
  }
  return originalLoad.call(this, request, parent, isMain);
};

(async () => {
  try {
    const moduleObject = { exports: {} };
    const sandbox = {
      module: moduleObject,
      exports: moduleObject.exports,
      require,
      process,
      console,
      Buffer,
      Date: FixedDate,
      setTimeout,
      clearTimeout,
      setInterval,
      clearInterval,
    };
    vm.runInNewContext(code, sandbox, { filename: 'inline-handler.js' });
    const handler = moduleObject.exports.handler || sandbox.exports.handler;
    const result = await handler(event);
    process.stdout.write(JSON.stringify({ result, calls }));
  } catch (error) {
    console.error(error);
    process.exit(1);
  } finally {
    Module._load = originalLoad;
  }
})();
"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as script_file:
        script_file.write(node_script)
        script_path = Path(script_file.name)
    try:
        result = subprocess.run(
            ["node", str(script_path)],
            cwd=REPO_DIR,
            env={
                **_base_app_env(),
                **env,
                "INLINE_LAMBDA_CODE": _lambda_inline_code(prefix),
                "INLINE_LAMBDA_EVENT": json.dumps(event),
            },
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        script_path.unlink(missing_ok=True)
    assert result.returncode == 0, result.stderr or result.stdout
    return json.loads(result.stdout)


def _ensure_no_retain_or_snapshot() -> None:
    for logical_id, resource in RESOURCES.items():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}, logical_id
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}, logical_id


def test_delivery_is_single_source_file_and_synthesizes_from_app_ts():
    ts_sources = sorted(
        path.relative_to(REPO_DIR).as_posix()
        for path in REPO_DIR.rglob("*.ts")
        if "node_modules" not in path.parts and "cdk.out" not in path.parts
    )
    assert ts_sources == ["app.ts"]
    assert "new OrdersIngestStack(app, 'OrdersIngestStack'" in APP_SOURCE
    assert "app.synth()" in APP_SOURCE


def test_template_has_expected_resource_counts():
    assert len(_resources_by_type("AWS::EC2::VPC")) == 1
    assert len(_resources_by_type("AWS::EC2::Subnet")) == 4
    assert len(_resources_by_type("AWS::ApiGateway::VpcLink")) == 0
    assert len(_resources_by_type("AWS::S3::Bucket")) == 1
    assert len(_resources_by_type("AWS::SQS::Queue")) == 1
    assert len(_resources_by_type("AWS::SNS::Topic")) == 1
    assert len(_resources_by_type("AWS::SecretsManager::Secret")) == 1
    assert len(_resources_by_type("AWS::RDS::DBInstance")) == 1
    assert len(_resources_by_type("AWS::RDS::DBSubnetGroup")) == 1
    assert len(_resources_by_type("AWS::EC2::VPCEndpoint")) == 1
    assert len(_resources_by_type("AWS::Lambda::Function")) >= 2
    assert len(
        [
            logical_id
            for logical_id in _resources_by_type("AWS::Lambda::Function")
            if logical_id.startswith("Orders") and "Custom" not in logical_id
        ]
    ) == 2
    assert len(_resources_by_type("AWS::Events::EventBus")) == 1
    assert len(_resources_by_type("AWS::Events::Rule")) == 1
    assert len(_resources_by_type("AWS::Pipes::Pipe")) == 1
    assert len(_resources_by_type("AWS::StepFunctions::StateMachine")) == 1
    assert len(_resources_by_type("AWS::Logs::LogGroup")) == 2
    assert len(_resources_by_type("AWS::EC2::SecurityGroup")) == 4
    _ensure_no_retain_or_snapshot()


def test_network_topology_matches_the_prompt():
    _, vpc = next(iter(_resources_by_type("AWS::EC2::VPC").items()))
    assert vpc["Properties"]["CidrBlock"] == "10.0.0.0/16"

    subnets = _resources_by_type("AWS::EC2::Subnet")
    cidr_to_subnet = {
        resource["Properties"]["CidrBlock"]: resource["Properties"]
        for resource in subnets.values()
    }
    assert set(cidr_to_subnet) == {
        "10.0.1.0/24",
        "10.0.2.0/24",
        "10.0.101.0/24",
        "10.0.102.0/24",
    }
    assert cidr_to_subnet["10.0.1.0/24"]["MapPublicIpOnLaunch"] is False
    assert cidr_to_subnet["10.0.2.0/24"]["MapPublicIpOnLaunch"] is False
    assert cidr_to_subnet["10.0.101.0/24"]["MapPublicIpOnLaunch"] is True
    assert cidr_to_subnet["10.0.102.0/24"]["MapPublicIpOnLaunch"] is True

    private_a_az = cidr_to_subnet["10.0.1.0/24"]["AvailabilityZone"]
    private_b_az = cidr_to_subnet["10.0.2.0/24"]["AvailabilityZone"]
    public_a_az = cidr_to_subnet["10.0.101.0/24"]["AvailabilityZone"]
    public_b_az = cidr_to_subnet["10.0.102.0/24"]["AvailabilityZone"]
    assert private_a_az == public_a_az
    assert private_b_az == public_b_az
    assert private_a_az != private_b_az

    _, endpoint = next(iter(_resources_by_type("AWS::EC2::VPCEndpoint").items()))
    endpoint_props = endpoint["Properties"]
    assert endpoint_props["VpcEndpointType"] == "Interface"
    assert endpoint_props["PrivateDnsEnabled"] is True
    assert len(endpoint_props["SubnetIds"]) == 2
    assert endpoint_props["ServiceName"] == "com.amazonaws.us-east-1.secretsmanager"
    endpoint_sg_id, _ = _resource_with_prefix("AWS::EC2::SecurityGroup", "OrdersSecretsEndpointSecurityGroup")
    assert endpoint_props["SecurityGroupIds"][0]["Fn::GetAtt"][0] == endpoint_sg_id


def test_app_maps_aws_endpoint_to_sdk_endpoint_url_and_preserves_region():
    assert "process.env.AWS_ENDPOINT?.trim() || process.env.AWS_ENDPOINT_URL?.trim()" in APP_SOURCE
    assert "process.env.AWS_ENDPOINT_URL = parsedEndpoint.toString();" in APP_SOURCE
    assert "process.env.AWS_REGION = awsRegion;" in APP_SOURCE
    assert "process.env.CDK_DEFAULT_REGION = process.env.CDK_DEFAULT_REGION ?? awsRegion;" in APP_SOURCE
    assert "new URL(awsEndpoint)" in APP_SOURCE

    template, manifest = _synthesize_template(
        {
            "AWS_REGION": "us-west-2",
            "AWS_ENDPOINT": "https://aws-endpoint.internal",
        }
    )
    assert len(_resources_by_type_in(template, "AWS::EC2::VPCEndpoint")) == 1
    assert len(_resources_by_type_in(template, "AWS::RDS::DBInstance")) == 1
    assert len(_resources_by_type_in(template, "AWS::Lambda::EventSourceMapping")) == 1
    assert len(_resources_by_type_in(template, "AWS::EC2::SecurityGroup")) == 4
    assert len(
        [
            logical_id
            for logical_id, resource in _resources_with_prefix_in(template, "OrdersPipe").items()
            if resource["Type"] == "AWS::CloudFormation::CustomResource"
        ]
    ) == 1
    assert "OrdersPipeName" in template.get("Outputs", {})

    artifacts = manifest.get("artifacts", {})
    stack_artifact = next(value for key, value in artifacts.items() if key == "OrdersIngestStack")
    assert stack_artifact["environment"].endswith("/us-west-2")


def test_name_prefix_scopes_the_deployed_stack_name_without_changing_the_stack_artifact_id():
    template, manifest = _synthesize_template({"NAME_PREFIX": "testrun"})
    assert template["Outputs"]["OrdersStackName"]["Value"] == "testrun-OrdersIngestStack"

    artifacts = manifest.get("artifacts", {})
    assert "OrdersIngestStack" in artifacts


def test_invalid_name_prefix_fails_fast():
    failure_output = _synthesize_failure({"NAME_PREFIX": "9invalid-prefix"})
    assert 'NAME_PREFIX must start with a letter and contain only letters, numbers, and hyphens' in failure_output


def test_invalid_aws_region_fails_fast():
    failure_output = _synthesize_failure({"AWS_REGION": "definitely-not-a-region"})
    assert 'AWS_REGION must be a valid AWS region' in failure_output


def test_blank_aws_region_defaults_to_us_east_1():
    template, manifest = _synthesize_template({"AWS_REGION": "   "})
    assert len(_resources_by_type_in(template, "AWS::Lambda::Function")) >= 2
    artifacts = manifest.get("artifacts", {})
    stack_artifact = next(value for key, value in artifacts.items() if key == "OrdersIngestStack")
    assert stack_artifact["environment"].endswith("/us-east-1")


def test_malformed_aws_endpoint_fails_fast():
    failure_output = _synthesize_failure(
        {
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": "not-a-valid-url",
        }
    )
    assert 'AWS_ENDPOINT must be a valid http(s) URL' in failure_output


def test_inline_api_lambda_executes_the_required_cross_service_flow():
    payload = {"orderId": "unit-api-1", "items": [{"sku": "sku-1", "quantity": 2}]}
    response = _invoke_inline_lambda(
        "OrdersApiFunction",
        event={"body": json.dumps(payload)},
        env={
            "AWS_REGION": "us-east-1",
            "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
            "BUCKET_NAME": "orders-archive-bucket",
            "EVENT_BUS_NAME": "orders-bus",
        },
    )
    calls_by_service = {call["service"]: call for call in response["calls"]}

    assert response["result"]["statusCode"] == 202
    body = json.loads(response["result"]["body"])
    assert body["status"] == "accepted"
    assert body["archiveKey"] == "orders/1704164645000-123e4567-e89b-12d3-a456-426614174000.json"

    assert calls_by_service["sqs"]["command"] == "SendMessageCommand"
    assert calls_by_service["sqs"]["input"] == {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
        "MessageBody": json.dumps(payload),
    }

    assert calls_by_service["s3"]["command"] == "PutObjectCommand"
    assert calls_by_service["s3"]["input"] == {
        "Bucket": "orders-archive-bucket",
        "Key": body["archiveKey"],
        "Body": json.dumps(payload),
        "ContentType": "application/json",
    }

    assert calls_by_service["eventbridge"]["command"] == "PutEventsCommand"
    entry = calls_by_service["eventbridge"]["input"]["Entries"][0]
    assert entry["EventBusName"] == "orders-bus"
    assert entry["Source"] == "orders.api"
    assert entry["DetailType"] == "OrderReceived"
    assert json.loads(entry["Detail"]) == {
        "archiveKey": body["archiveKey"],
        "receivedBody": json.dumps(payload),
    }


def test_inline_worker_lambda_fetches_secret_parses_records_and_publishes_notification():
    response = _invoke_inline_lambda(
        "OrdersWorkerFunction",
        event={
            "Records": [
                {"messageId": "record-1", "body": json.dumps({"orderId": "unit-worker-1"})},
                {"messageId": "record-2", "body": "plain-text-body"},
            ]
        },
        env={
            "AWS_REGION": "us-east-1",
            "DB_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:orders-db",
            "DB_HOST": "orders.cluster.local",
            "DB_PORT": "5432",
            "DB_NAME": "orders",
            "TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:orders-topic",
        },
    )
    calls_by_service = {call["service"]: call for call in response["calls"]}

    assert response["result"]["recordCount"] == 2
    assert response["result"]["dbConnection"]["host"] == "orders.cluster.local"
    assert response["result"]["dbConnection"]["port"] == 5432
    assert response["result"]["dbConnection"]["username"] == "orders_app"
    assert response["result"]["dbConnection"]["database"] == "orders"
    assert response["result"]["dbConnection"]["status"] == "stubbed-connection"

    assert calls_by_service["secretsmanager"]["command"] == "GetSecretValueCommand"
    assert calls_by_service["secretsmanager"]["input"] == {
        "SecretId": "arn:aws:secretsmanager:us-east-1:123456789012:secret:orders-db"
    }

    assert calls_by_service["sns"]["command"] == "PublishCommand"
    publish_input = calls_by_service["sns"]["input"]
    assert publish_input["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:orders-topic"
    assert publish_input["Subject"] == "orders-worker-processed"
    published_message = json.loads(publish_input["Message"])
    assert published_message["records"] == [
        {"messageId": "record-1", "payload": {"orderId": "unit-worker-1"}},
        {"messageId": "record-2", "payload": "plain-text-body"},
    ]
    assert published_message["dbConnection"]["username"] == "orders_app"
    assert published_message["dbConnection"]["status"] == "stubbed-connection"


def test_inline_pipe_manager_handler_source_covers_create_update_and_delete():
    assert 'client.create_pipe(**_pipe_parameters(props))' in APP_SOURCE
    assert 'client.update_pipe(Name=pipe_name' in APP_SOURCE
    assert 'client.delete_pipe(Name=pipe_name)' in APP_SOURCE


def test_storage_database_and_queue_configuration_are_exact():
    _, bucket = next(iter(_resources_by_type("AWS::S3::Bucket").items()))
    bucket_props = bucket["Properties"]
    assert bucket_props["BucketEncryption"]["ServerSideEncryptionConfiguration"][0][
        "ServerSideEncryptionByDefault"
    ]["SSEAlgorithm"] == "AES256"
    assert bucket_props["VersioningConfiguration"]["Status"] == "Enabled"
    assert bucket_props["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }

    _, queue = next(iter(_resources_by_type("AWS::SQS::Queue").items()))
    queue_props = queue["Properties"]
    assert queue_props["SqsManagedSseEnabled"] is True
    assert queue_props["VisibilityTimeout"] == 60

    _, secret = next(iter(_resources_by_type("AWS::SecretsManager::Secret").items()))
    generate_secret = secret["Properties"]["GenerateSecretString"]
    assert generate_secret["GenerateStringKey"] == "password"
    assert '"username":"orders_app"' in generate_secret["SecretStringTemplate"]

    _, subnet_group = next(iter(_resources_by_type("AWS::RDS::DBSubnetGroup").items()))
    subnet_ids = subnet_group["Properties"]["SubnetIds"]
    assert len(subnet_ids) == 2

    _, db = next(iter(_resources_by_type("AWS::RDS::DBInstance").items()))
    db_props = db["Properties"]
    assert db_props["Engine"] == "postgres"
    assert str(db_props["EngineVersion"]).startswith("15")
    assert db_props["DBInstanceClass"] == "db.t3.micro"
    assert db_props["AllocatedStorage"] == "20"
    assert db_props["StorageEncrypted"] is True
    assert db_props["PubliclyAccessible"] is False
    assert db_props["DeletionProtection"] is False
    assert len(db_props["VPCSecurityGroups"]) == 1


def test_api_lambda_and_logs_match_the_contract():
    api_logical_id, api_lambda = _resource_with_prefix("AWS::Lambda::Function", "OrdersApiFunction")
    worker_logical_id, worker_lambda = _resource_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")
    queue_logical_id, _ = _resource_with_prefix("AWS::SQS::Queue", "OrdersQueue")

    api_props = api_lambda["Properties"]
    worker_props = worker_lambda["Properties"]

    assert api_props["Runtime"] == "nodejs20.x"
    assert api_props["MemorySize"] == 256
    assert api_props["Timeout"] == 10
    assert sorted(api_props["Environment"]["Variables"]) == ["BUCKET_NAME", "EVENT_BUS_NAME", "QUEUE_URL"]
    assert len(api_props["VpcConfig"]["SubnetIds"]) == 2
    assert len(api_props["VpcConfig"]["SecurityGroupIds"]) == 1

    assert worker_props["Runtime"] == "nodejs20.x"
    assert worker_props["MemorySize"] == 256
    assert worker_props["Timeout"] == 20
    assert sorted(worker_props["Environment"]["Variables"]) == [
        "DB_HOST",
        "DB_NAME",
        "DB_PORT",
        "DB_SECRET_ARN",
        "TOPIC_ARN",
    ]
    assert len(worker_props["VpcConfig"]["SubnetIds"]) == 2
    assert len(worker_props["VpcConfig"]["SecurityGroupIds"]) == 1

    event_source_mappings = _resources_by_type("AWS::Lambda::EventSourceMapping")
    assert len(event_source_mappings) == 1
    _, mapping = next(iter(event_source_mappings.items()))
    mapping_props = mapping["Properties"]
    assert mapping_props["BatchSize"] == 1
    assert mapping_props["FunctionName"]["Ref"] == worker_logical_id
    assert mapping_props["EventSourceArn"]["Fn::GetAtt"][0] == queue_logical_id

    _, rest_api = next(iter(_resources_by_type("AWS::ApiGateway::RestApi").items()))
    assert rest_api["Properties"]["EndpointConfiguration"]["Types"] == ["REGIONAL"]

    _, stage = _resource_with_prefix("AWS::ApiGateway::Stage", "OrdersApiGatewayDeploymentStageprod")
    assert stage["Properties"]["StageName"] == "prod"
    assert stage["Properties"]["MethodSettings"] == [
        {
            "DataTraceEnabled": False,
            "HttpMethod": "*",
            "ResourcePath": "/*",
            "ThrottlingBurstLimit": 1,
            "ThrottlingRateLimit": 1,
        }
    ]

    _, resource = _resource_with_prefix("AWS::ApiGateway::Resource", "OrdersApiGatewayorders")
    assert resource["Properties"]["PathPart"] == "orders"

    _, method = _resource_with_prefix("AWS::ApiGateway::Method", "OrdersApiGatewayordersPOST")
    integration = method["Properties"]["Integration"]
    assert method["Properties"]["HttpMethod"] == "POST"
    assert integration["Type"] == "AWS_PROXY"
    assert api_logical_id in json.dumps(integration["Uri"])

    log_groups = _resources_by_type("AWS::Logs::LogGroup")
    for resource in log_groups.values():
        assert resource["Properties"]["RetentionInDays"] == 7
        assert resource["DeletionPolicy"] == "Delete"

    assert "OrdersApiUrl" in OUTPUTS
    assert "OrdersArchiveBucketName" in OUTPUTS
    assert "OrdersQueueUrl" in OUTPUTS
    assert "OrdersNotificationsTopicArn" in OUTPUTS
    assert "OrdersStackName" in OUTPUTS


def test_log_policies_do_not_reintroduce_lambda_to_log_group_dependency_cycles():
    for policy_prefix in ("OrdersApiLambdaRoleDefaultPolicy", "OrdersWorkerLambdaRoleDefaultPolicy"):
        policy_document = _policy_document(policy_prefix)
        policy_json = json.dumps(policy_document)
        assert "OrdersApiLogGroup" not in policy_json
        assert "OrdersWorkerLogGroup" not in policy_json
        assert "log-group:/aws/lambda/*" in policy_json
        assert "log-group:/aws/lambda/*:*" in policy_json

        statements = policy_document["Statement"]
        assert any(
            statement["Action"] == "logs:CreateLogGroup"
            and "log-group:/aws/lambda/*" in json.dumps(statement["Resource"])
            for statement in statements
        )
        assert any(
            statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"]
            and "log-group:/aws/lambda/*:*" in json.dumps(statement["Resource"])
            for statement in statements
        )


def test_eventbridge_pipe_stepfunctions_and_security_boundaries_are_scoped():
    queue_logical_id, _ = _resource_with_prefix("AWS::SQS::Queue", "OrdersQueue")
    event_bus_logical_id, _ = _resource_with_prefix("AWS::Events::EventBus", "OrdersEventBus")
    secret_logical_id, _ = _resource_with_prefix("AWS::SecretsManager::Secret", "OrdersDatabaseSecret")
    topic_logical_id, _ = _resource_with_prefix("AWS::SNS::Topic", "OrdersNotificationsTopic")

    _, bus = next(iter(_resources_by_type("AWS::Events::EventBus").items()))
    assert bus["Properties"]["Name"]

    _, rule = next(iter(_resources_by_type("AWS::Events::Rule").items()))
    rule_props = rule["Properties"]
    assert rule_props["EventPattern"] == {"source": ["orders.api"]}
    assert len(rule_props["Targets"]) == 1

    _, pipe = next(iter(_resources_by_type("AWS::Pipes::Pipe").items()))
    pipe_props = pipe["Properties"]
    assert pipe_props["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe_props["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    _, state_machine = next(iter(_resources_by_type("AWS::StepFunctions::StateMachine").items()))
    definition = json.loads(state_machine["Properties"]["DefinitionString"])
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert definition["StartAt"] == "RecordTimestamp"
    assert definition["States"]["RecordTimestamp"]["Type"] == "Pass"
    assert definition["States"]["RecordTimestamp"]["Parameters"]["recordedAt.$"] == "$$.State.EnteredTime"
    assert definition["States"]["OrdersWorkflowSucceeded"]["Type"] == "Succeed"

    api_sg_id, api_sg = _resource_with_prefix("AWS::EC2::SecurityGroup", "OrdersApiSecurityGroup")
    worker_sg_id, worker_sg = _resource_with_prefix("AWS::EC2::SecurityGroup", "OrdersWorkerSecurityGroup")
    endpoint_sg_id, endpoint_sg = _resource_with_prefix("AWS::EC2::SecurityGroup", "OrdersSecretsEndpointSecurityGroup")
    data_sg_id, data_sg = _resource_with_prefix("AWS::EC2::SecurityGroup", "OrdersDataPlaneSecurityGroup")

    assert "SecurityGroupIngress" not in api_sg["Properties"]
    assert "SecurityGroupIngress" not in worker_sg["Properties"]
    assert "SecurityGroupIngress" not in endpoint_sg["Properties"]
    assert len(api_sg["Properties"]["SecurityGroupEgress"]) == 1
    assert len(worker_sg["Properties"]["SecurityGroupEgress"]) == 3
    assert not any(endpoint_sg_id in json.dumps(rule) for rule in api_sg["Properties"]["SecurityGroupEgress"])
    assert any(endpoint_sg_id in json.dumps(rule) for rule in worker_sg["Properties"]["SecurityGroupEgress"])

    data_ingress = data_sg["Properties"]["SecurityGroupIngress"]
    assert len(data_ingress) == 1
    assert data_ingress[0]["FromPort"] == 5432
    assert data_ingress[0]["ToPort"] == 5432
    assert data_ingress[0]["SourceSecurityGroupId"]["Fn::GetAtt"][0] == worker_sg_id

    _, endpoint_ingress = _resource_with_prefix(
        "AWS::EC2::SecurityGroupIngress", "OrdersSecretsEndpointSecurityGroupIngressFromWorker"
    )
    endpoint_ingress_props = endpoint_ingress["Properties"]
    assert endpoint_ingress_props["FromPort"] == 443
    assert endpoint_ingress_props["ToPort"] == 443
    assert endpoint_ingress_props["GroupId"]["Fn::GetAtt"][0] == endpoint_sg_id
    assert endpoint_ingress_props["SourceSecurityGroupId"]["Fn::GetAtt"][0] == worker_sg_id

    api_policy = _policy_document("OrdersApiLambdaRoleDefaultPolicy")
    api_statements = api_policy["Statement"]
    assert any(
        statement["Action"] == "sqs:SendMessage" and statement["Resource"]["Fn::GetAtt"][0] == queue_logical_id
        for statement in api_statements
    )
    assert any(
        statement["Action"] == "s3:PutObject" and statement["Resource"]["Fn::Join"][1][-1] == "/orders/*"
        for statement in api_statements
    )
    assert any(
        statement["Action"] == "events:PutEvents" and statement["Resource"]["Fn::GetAtt"][0] == event_bus_logical_id
        for statement in api_statements
    )
    assert not any(statement["Action"] == "sqs:ReceiveMessage" for statement in api_statements)
    assert not any(statement["Action"] == "secretsmanager:GetSecretValue" for statement in api_statements)
    assert not any(
        action.startswith("rds:")
        for statement in api_statements
        for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
    )
    assert sum(1 for statement in api_statements if statement["Resource"] == "*") == 1
    assert any(
        statement["Action"] == "logs:CreateLogGroup"
        and "log-group:/aws/lambda/*" in json.dumps(statement["Resource"])
        for statement in api_statements
    )
    assert any(
        statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"]
        and "log-group:/aws/lambda/*:*" in json.dumps(statement["Resource"])
        for statement in api_statements
    )

    worker_policy = _policy_document("OrdersWorkerLambdaRoleDefaultPolicy")
    worker_statements = worker_policy["Statement"]
    assert any(
        statement["Action"] == "secretsmanager:GetSecretValue" and statement["Resource"]["Ref"] == secret_logical_id
        for statement in worker_statements
    )
    assert any(
        statement["Action"] == "sns:Publish" and statement["Resource"]["Ref"] == topic_logical_id
        for statement in worker_statements
    )
    assert not any(statement["Action"] == "s3:PutObject" for statement in worker_statements)
    assert not any(statement["Action"] == "events:PutEvents" for statement in worker_statements)
    assert sum(1 for statement in worker_statements if statement["Resource"] == "*") == 1
    assert any(
        statement["Action"] == "logs:CreateLogGroup"
        and "log-group:/aws/lambda/*" in json.dumps(statement["Resource"])
        for statement in worker_statements
    )
    assert any(
        statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"]
        and "log-group:/aws/lambda/*:*" in json.dumps(statement["Resource"])
        for statement in worker_statements
    )

    pipe_policy = _policy_document("OrdersPipeRoleDefaultPolicy")
    pipe_actions = {
        tuple(statement["Action"]) if isinstance(statement["Action"], list) else statement["Action"]
        for statement in pipe_policy["Statement"]
    }
    assert ("sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes") in pipe_actions
    assert "lambda:InvokeFunction" in pipe_actions
    assert "states:StartExecution" in pipe_actions
    assert not any(logical_id.startswith("OrdersStateMachineRoleDefaultPolicy") for logical_id in _resources_by_type("AWS::IAM::Policy"))


def test_global_iam_guardrails_and_generated_names_are_enforced():
    for role in _resources_by_type("AWS::IAM::Role").values():
        managed_policy_arns = role["Properties"].get("ManagedPolicyArns", [])
        assert all("AdministratorAccess" not in json.dumps(arn) for arn in managed_policy_arns)

    wildcard_action_violations: list[tuple[object, object]] = []
    iam_namespace_violations: list[str] = []
    for statement in _all_policy_statements():
        actions = statement["Action"]
        if not isinstance(actions, list):
            actions = [actions]
        for action in actions:
            if action == "*" or action.endswith(":*"):
                wildcard_action_violations.append((action, statement["Resource"]))
            if action.lower().startswith("iam:"):
                iam_namespace_violations.append(action)

    assert wildcard_action_violations == []
    assert iam_namespace_violations == []
    default_wildcard_statements = _wildcard_resource_statements(TEMPLATE)
    assert len(default_wildcard_statements) == 2
    assert all(
        set(statement["Action"])
        == {
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
            "ec2:AssignPrivateIpAddresses",
            "ec2:UnassignPrivateIpAddresses",
            "ec2:DescribeSubnets",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeVpcs",
        }
        for statement in default_wildcard_statements
    )

    local_template, _ = _synthesize_template(
        {
            "AWS_REGION": "us-west-2",
            "AWS_ENDPOINT_URL": "https://aws-endpoint.internal",
        }
    )
    local_wildcard_statements = _wildcard_resource_statements(local_template)
    assert len(local_wildcard_statements) == 3
    assert sum(
        1
        for statement in local_wildcard_statements
        if set(statement["Action"])
        == {
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
            "ec2:AssignPrivateIpAddresses",
            "ec2:UnassignPrivateIpAddresses",
            "ec2:DescribeSubnets",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeVpcs",
        }
    ) == 2
    assert any(
        statement["Action"]
        == ["pipes:CreatePipe", "pipes:UpdatePipe", "pipes:DeletePipe", "pipes:DescribePipe"]
        and statement["Resource"] == "*"
        for statement in local_wildcard_statements
    )
    assert "wildcard statement isolated to the minimum" in APP_SOURCE

    physical_name_fields = {
        ("AWS::S3::Bucket", "BucketName"),
        ("AWS::SQS::Queue", "QueueName"),
        ("AWS::SNS::Topic", "TopicName"),
        ("AWS::Lambda::Function", "FunctionName"),
        ("AWS::SecretsManager::Secret", "Name"),
        ("AWS::StepFunctions::StateMachine", "StateMachineName"),
        ("AWS::RDS::DBInstance", "DBInstanceIdentifier"),
        ("AWS::RDS::DBSubnetGroup", "DBSubnetGroupName"),
        ("AWS::EC2::SecurityGroup", "GroupName"),
    }
    for resource_type, property_name in physical_name_fields:
        for resource in _resources_by_type(resource_type).values():
            assert property_name not in resource.get("Properties", {})

    bus_name = next(iter(_resources_by_type("AWS::Events::EventBus").values()))["Properties"]["Name"]
    assert bus_name != "orders-event-bus"
    assert bus_name.startswith("OrdersIngestStackOrdersEventBus")
    topic_name = next(iter(_resources_by_type("AWS::SNS::Topic").values())).get("Properties", {}).get("TopicName")
    assert topic_name is None

    hardcoded_name_props = re.findall(
        r"\b(bucketName|queueName|topicName|functionName|stateMachineName|secretName|roleName|dbSubnetGroupName)\s*:",
        APP_SOURCE,
    )
    assert hardcoded_name_props == []


def test_local_mode_pipe_manager_resources_and_policy_are_validated():
    template, _ = _synthesize_template(
        {
            "AWS_REGION": "us-west-2",
            "AWS_ENDPOINT_URL": "https://aws-endpoint.internal",
        }
    )

    _, manager_lambda = next(
        (
            logical_id,
            resource,
        )
        for logical_id, resource in _resources_by_type_in(template, "AWS::Lambda::Function").items()
        if logical_id.startswith("OrdersPipeManagerFunction")
    )
    manager_props = manager_lambda["Properties"]
    assert manager_props["Runtime"] == "python3.11"
    assert manager_props["Timeout"] == 60
    assert manager_props["Environment"]["Variables"]["AWS_ENDPOINT_URL"] == "https://aws-endpoint.internal/"
    assert len(_resources_by_type_in(template, "AWS::Pipes::Pipe")) == 0
    assert len(
        [
            logical_id
            for logical_id, resource in _resources_with_prefix_in(template, "OrdersPipe").items()
            if resource["Type"] == "AWS::CloudFormation::CustomResource"
        ]
    ) == 1

    policies = _resources_by_type_in(template, "AWS::IAM::Policy")
    manager_policy = next(
        resource["Properties"]["PolicyDocument"]
        for resource in policies.values()
        if "pipes:CreatePipe" in json.dumps(resource["Properties"]["PolicyDocument"])
    )
    manager_statements = manager_policy["Statement"]
    assert any(
        statement["Action"] == [
            "pipes:CreatePipe",
            "pipes:UpdatePipe",
            "pipes:DeletePipe",
            "pipes:DescribePipe",
        ]
        and statement["Resource"] == "*"
        for statement in manager_statements
    )


def test_traffic_flow_wiring_matches_the_prompt_overview():
    queue_logical_id, queue_resource = _resource_with_prefix("AWS::SQS::Queue", "OrdersQueue")
    bucket_logical_id, _ = _resource_with_prefix("AWS::S3::Bucket", "OrdersArchiveBucket")
    bus_logical_id, _ = _resource_with_prefix("AWS::Events::EventBus", "OrdersEventBus")
    topic_logical_id, _ = _resource_with_prefix("AWS::SNS::Topic", "OrdersNotificationsTopic")
    api_logical_id, _ = _resource_with_prefix("AWS::Lambda::Function", "OrdersApiFunction")
    worker_logical_id, _ = _resource_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")
    state_machine_logical_id, _ = _resource_with_prefix("AWS::StepFunctions::StateMachine", "OrdersStateMachine")

    _, api_method = _resource_with_prefix("AWS::ApiGateway::Method", "OrdersApiGatewayordersPOST")
    assert api_method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
    assert api_logical_id in json.dumps(api_method["Properties"]["Integration"]["Uri"])

    lambda_permissions = _resources_by_type("AWS::Lambda::Permission")
    assert any(
        permission["Properties"]["Principal"] == "apigateway.amazonaws.com"
        and permission["Properties"]["FunctionName"]["Fn::GetAtt"][0] == api_logical_id
        for permission in lambda_permissions.values()
    )

    api_policy = _policy_document("OrdersApiLambdaRoleDefaultPolicy")["Statement"]
    assert any(
        statement["Action"] == "sqs:SendMessage" and statement["Resource"]["Fn::GetAtt"][0] == queue_logical_id
        for statement in api_policy
    )
    assert any(
        statement["Action"] == "s3:PutObject" and bucket_logical_id in json.dumps(statement["Resource"])
        for statement in api_policy
    )
    assert any(
        statement["Action"] == "events:PutEvents" and statement["Resource"]["Fn::GetAtt"][0] == bus_logical_id
        for statement in api_policy
    )

    _, rule = _resource_with_prefix("AWS::Events::Rule", "OrdersEventRule")
    assert rule["Properties"]["Targets"][0]["Arn"]["Fn::GetAtt"][0] == queue_logical_id

    _, mapping = next(iter(_resources_by_type("AWS::Lambda::EventSourceMapping").items()))
    assert mapping["Properties"]["EventSourceArn"]["Fn::GetAtt"][0] == queue_logical_id
    assert mapping["Properties"]["FunctionName"]["Ref"] == worker_logical_id

    _, pipe = _resource_with_prefix("AWS::Pipes::Pipe", "OrdersPipe")
    assert pipe["Properties"]["Source"]["Fn::GetAtt"][0] == queue_logical_id
    assert pipe["Properties"]["Enrichment"]["Fn::GetAtt"][0] == worker_logical_id
    assert pipe["Properties"]["Target"]["Ref"] == state_machine_logical_id

    worker_policy = _policy_document("OrdersWorkerLambdaRoleDefaultPolicy")["Statement"]
    assert any(
        statement["Action"] == "sns:Publish" and statement["Resource"]["Ref"] == topic_logical_id
        for statement in worker_policy
    )
