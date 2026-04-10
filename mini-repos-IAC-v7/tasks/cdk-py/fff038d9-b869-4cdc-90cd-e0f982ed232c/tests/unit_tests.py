import importlib.util
import json
import os
from pathlib import Path
import sys
import types

import aws_cdk as cdk
from aws_cdk.assertions import Template

APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def load_app_module(monkeypatch, **env_overrides):
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")

    for key in [
        "AWS_ENDPOINT",
        "AWS_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL",
    ]:
        monkeypatch.delenv(key, raising=False)

    for key, value in env_overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    sys.modules.pop("app", None)
    spec = importlib.util.spec_from_file_location("app", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["app"] = module
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def synthesize(monkeypatch, **env_overrides):
    app_module = load_app_module(monkeypatch, **env_overrides)
    app = app_module.main()
    stack = app.node.find_child("EventDrivenIngestionStack")
    template = Template.from_stack(stack)
    return app_module, app, stack, template.to_json()


def resources_by_type(template_json, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template_json["Resources"].items()
        if resource["Type"] == resource_type
    }


def find_single_resource(template_json, resource_type, predicate=lambda _id, _res: True):
    matches = [
        (logical_id, resource)
        for logical_id, resource in resources_by_type(template_json, resource_type).items()
        if predicate(logical_id, resource)
    ]
    assert len(matches) == 1
    return matches[0]


def role_with_policy(template_json, policy_name):
    for logical_id, resource in resources_by_type(template_json, "AWS::IAM::Role").items():
        for policy in resource["Properties"].get("Policies", []):
            if policy["PolicyName"] == policy_name:
                return logical_id, resource
    raise AssertionError(f"Role with policy {policy_name} not found")


def policy_statements(role_resource, policy_name):
    for policy in role_resource["Properties"].get("Policies", []):
        if policy["PolicyName"] == policy_name:
            return policy["PolicyDocument"]["Statement"]
    raise AssertionError(f"Policy {policy_name} not found")


def flatten_actions(role_resource):
    actions = set()
    for policy in role_resource["Properties"].get("Policies", []):
        for statement in policy["PolicyDocument"]["Statement"]:
            value = statement["Action"]
            if isinstance(value, list):
                actions.update(value)
            else:
                actions.add(value)
    return actions


def load_inline_handler(code, fake_boto3):
    fake_module = types.ModuleType("boto3")
    fake_module.client = fake_boto3.client
    fake_module.resource = fake_boto3.resource
    previous = sys.modules.get("boto3")
    sys.modules["boto3"] = fake_module
    namespace = {}
    try:
        exec(code, namespace)
    finally:
        if previous is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = previous
    return namespace["handler"]


def find_statement_with_action(statements, action):
    for statement in statements:
        statement_action = statement["Action"]
        if statement_action == action:
            return statement
        if isinstance(statement_action, list) and action in statement_action:
            return statement
    raise AssertionError(f"Statement with action {action} not found")


def test_app_file_and_allowed_inputs_contract(monkeypatch):
    monkeypatch.setenv("HOME", "/var/empty")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")
    for key in [
        "AWS_ENDPOINT",
        "AWS_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL",
    ]:
        monkeypatch.delenv(key, raising=False)
    sys.modules.pop("app", None)
    spec = importlib.util.spec_from_file_location("app", APP_PATH)
    app_module = importlib.util.module_from_spec(spec)
    sys.modules["app"] = app_module
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(app_module)

    assert APP_PATH.name == "app.py"
    assert APP_PATH.is_file()
    assert app_module.__file__ == str(APP_PATH)
    assert os.environ["HOME"] == "/tmp"
    assert app_module.load_config() == {
        "endpoint": None,
        "region": "us-east-1",
        "access_key_id": None,
        "secret_access_key": None,
    }


def test_main_defaults_region_and_uses_no_template_parameters(monkeypatch):
    app_module, app, stack, template_json = synthesize(monkeypatch)

    assert isinstance(app, cdk.App)
    assert stack.region == "us-east-1"
    assert app_module.load_config()["region"] == "us-east-1"
    assert set(template_json.get("Parameters", {})) <= {"BootstrapVersion"}


def test_configure_environment_uses_only_allowed_values(monkeypatch):
    app_module = load_app_module(
        monkeypatch,
        AWS_ENDPOINT="https://example.invalid",
        AWS_REGION="us-west-2",
        AWS_ACCESS_KEY_ID="key",
        AWS_SECRET_ACCESS_KEY="secret",
    )

    config = app_module.load_config()
    app_module.configure_environment(config)

    assert set(config) == {
        "endpoint",
        "region",
        "access_key_id",
        "secret_access_key",
    }
    assert os.environ["AWS_REGION"] == "us-west-2"
    assert os.environ["AWS_ENDPOINT_URL"] == "https://example.invalid"
    assert os.environ["AWS_ACCESS_KEY_ID"] == "key"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "secret"


def test_network_topology_and_database_security_contract(monkeypatch):
    _app_module, _app, _stack, template_json = synthesize(monkeypatch)
    subnet_resources = resources_by_type(template_json, "AWS::EC2::Subnet")
    route_resources = resources_by_type(template_json, "AWS::EC2::Route")
    security_ingress = resources_by_type(template_json, "AWS::EC2::SecurityGroupIngress")

    assert len(resources_by_type(template_json, "AWS::EC2::VPC")) == 1
    assert len(resources_by_type(template_json, "AWS::EC2::InternetGateway")) == 1
    assert len(resources_by_type(template_json, "AWS::EC2::VPCGatewayAttachment")) == 1
    assert len(resources_by_type(template_json, "AWS::EC2::NatGateway")) == 1
    assert len(resources_by_type(template_json, "AWS::EC2::SecurityGroup")) == 2

    public_subnets = {
        logical_id: resource
        for logical_id, resource in subnet_resources.items()
        if resource["Properties"]["MapPublicIpOnLaunch"] is True
    }
    private_subnets = {
        logical_id: resource
        for logical_id, resource in subnet_resources.items()
        if resource["Properties"]["MapPublicIpOnLaunch"] is False
    }
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    public_azs = {
        json.dumps(resource["Properties"]["AvailabilityZone"], sort_keys=True)
        for resource in public_subnets.values()
    }
    private_azs = {
        json.dumps(resource["Properties"]["AvailabilityZone"], sort_keys=True)
        for resource in private_subnets.values()
    }
    assert len(public_azs) == 2
    assert len(private_azs) == 2
    assert public_azs == private_azs

    nat_gateway_id, nat_gateway = find_single_resource(template_json, "AWS::EC2::NatGateway")
    assert nat_gateway["Properties"]["SubnetId"]["Ref"] in public_subnets

    nat_routes = [
        resource
        for resource in route_resources.values()
        if "NatGatewayId" in resource["Properties"]
        and resource["Properties"]["DestinationCidrBlock"] == "0.0.0.0/0"
    ]
    assert len(nat_routes) == 2
    for route in nat_routes:
        assert route["Properties"]["NatGatewayId"]["Ref"] == nat_gateway_id

    assert len(security_ingress) == 1
    _, ingress_rule = next(iter(security_ingress.items()))
    assert ingress_rule["Properties"]["FromPort"] == 5432
    assert ingress_rule["Properties"]["ToPort"] == 5432
    assert ingress_rule["Properties"]["IpProtocol"] == "tcp"
    assert "CidrIp" not in ingress_rule["Properties"]
    assert "CidrIpv6" not in ingress_rule["Properties"]


def test_api_gateway_ingress_contract(monkeypatch):
    _app_module, _app, _stack, template_json = synthesize(monkeypatch)
    _, resource = find_single_resource(template_json, "AWS::ApiGateway::Resource")
    _, method = find_single_resource(template_json, "AWS::ApiGateway::Method")
    _, stage = find_single_resource(template_json, "AWS::ApiGateway::Stage")
    _, validator = find_single_resource(template_json, "AWS::ApiGateway::RequestValidator")
    _, model = find_single_resource(template_json, "AWS::ApiGateway::Model")

    assert len(resources_by_type(template_json, "AWS::ApiGateway::RestApi")) == 1
    assert len(resources_by_type(template_json, "AWS::ApiGateway::Resource")) == 1
    assert len(resources_by_type(template_json, "AWS::ApiGateway::Method")) == 1

    assert resource["Properties"]["PathPart"] == "ingest"
    assert method["Properties"]["HttpMethod"] == "POST"
    assert method["Properties"]["AuthorizationType"] == "NONE"
    assert method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
    assert "IngestLambda" in json.dumps(method["Properties"]["Integration"]["Uri"])
    assert method["Properties"]["RequestValidatorId"] == {"Ref": next(iter(resources_by_type(template_json, "AWS::ApiGateway::RequestValidator")))}
    assert method["Properties"]["RequestModels"] == {
        "application/json": {"Ref": next(iter(resources_by_type(template_json, "AWS::ApiGateway::Model")))}
    }

    assert validator["Properties"]["ValidateRequestBody"] is True
    assert validator["Properties"]["RestApiId"] == method["Properties"]["RestApiId"]
    assert model["Properties"]["ContentType"] == "application/json"
    assert model["Properties"]["Schema"]["maxLength"] == 262144
    assert stage["Properties"]["StageName"] == "v1"


def test_lambda_resource_contract(monkeypatch):
    app_module, _app, _stack, template_json = synthesize(monkeypatch)
    lambda_resources = resources_by_type(template_json, "AWS::Lambda::Function")
    lambda_permissions = resources_by_type(template_json, "AWS::Lambda::Permission")
    ingest_lambda_id, _ = find_single_resource(
        template_json,
        "AWS::Lambda::Function",
        lambda _id, res: res["Properties"]["FunctionName"] == "event-ingestion-ingest",
    )
    worker_lambda_id, _ = find_single_resource(
        template_json,
        "AWS::Lambda::Function",
        lambda _id, res: res["Properties"]["FunctionName"] == "event-ingestion-worker",
    )
    enricher_lambda_id, _ = find_single_resource(
        template_json,
        "AWS::Lambda::Function",
        lambda _id, res: res["Properties"]["FunctionName"] == "event-ingestion-enricher",
    )

    assert len(lambda_resources) == 3
    for resource in lambda_resources.values():
        props = resource["Properties"]
        assert props["Runtime"] == "python3.11"
        assert props["Architectures"] == ["x86_64"]
        assert props["Timeout"] == 30
        assert props["MemorySize"] == 256
        assert props["Code"].get("ZipFile")
        assert "ImageUri" not in props["Code"]
        assert props.get("PackageType", "Zip") == "Zip"

    assert len(resources_by_type(template_json, "AWS::Logs::LogGroup")) == 3
    for resource in resources_by_type(template_json, "AWS::Logs::LogGroup").values():
        props = resource["Properties"]
        assert props["RetentionInDays"] == 14
        assert "KmsKeyId" not in props

    _, event_source_mapping = find_single_resource(template_json, "AWS::Lambda::EventSourceMapping")
    assert event_source_mapping["Properties"]["BatchSize"] == 10
    assert event_source_mapping["Properties"]["MaximumBatchingWindowInSeconds"] == 5
    assert event_source_mapping["Properties"]["FunctionName"] == {"Ref": worker_lambda_id}

    assert lambda_permissions
    for permission in lambda_permissions.values():
        props = permission["Properties"]
        assert props["Principal"] == "apigateway.amazonaws.com"
        assert props["FunctionName"] == {"Fn::GetAtt": [ingest_lambda_id, "Arn"]}
        assert "/POST/ingest" in json.dumps(props["SourceArn"])

    permission_payload = json.dumps(lambda_permissions)
    assert worker_lambda_id not in permission_payload
    assert enricher_lambda_id not in permission_payload

    for code in [
        app_module.INGEST_LAMBDA_CODE,
        app_module.WORKER_LAMBDA_CODE,
        app_module.ENRICHER_LAMBDA_CODE,
    ]:
        lowered = code.lower()
        assert "psycopg" not in lowered
        assert "postgres" not in lowered
        assert 'boto3.client("rds")' not in lowered
        assert "secretsmanager" not in lowered


def test_ingest_lambda_logic(monkeypatch):
    app_module = load_app_module(monkeypatch)

    class FakeSqsClient:
        def __init__(self):
            self.sent_messages = []

        def send_message(self, **kwargs):
            self.sent_messages.append(kwargs)
            return {"MessageId": "1"}

    class FakeTable:
        def __init__(self):
            self.items = []

        def put_item(self, **kwargs):
            self.items.append(kwargs["Item"])
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    class FakeDynamoResource:
        def __init__(self, table):
            self.table = table

        def Table(self, _name):
            return self.table

    class FakeBoto3:
        def __init__(self):
            self.sqs = FakeSqsClient()
            self.table = FakeTable()
            self.client_calls = []
            self.resource_calls = []

        def client(self, service_name, **kwargs):
            self.client_calls.append((service_name, kwargs))
            assert service_name == "sqs"
            return self.sqs

        def resource(self, service_name, **kwargs):
            self.resource_calls.append((service_name, kwargs))
            assert service_name == "dynamodb"
            return FakeDynamoResource(self.table)

    fake_boto3 = FakeBoto3()
    handler = load_inline_handler(app_module.INGEST_LAMBDA_CODE, fake_boto3)

    monkeypatch.setenv("AWS_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("INGESTION_QUEUE_URL", "https://queue-url")
    monkeypatch.setenv("AUDIT_TABLE_NAME", "audit-table")

    invalid = handler({"body": "not-json"}, None)
    assert invalid["statusCode"] == 400

    response = handler({"body": json.dumps({"hello": "world"})}, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 202
    assert fake_boto3.client_calls == [("sqs", {"endpoint_url": "https://example.invalid"})]
    assert fake_boto3.resource_calls == [("dynamodb", {"endpoint_url": "https://example.invalid"})]
    assert len(fake_boto3.sqs.sent_messages) == 1
    sent_message = fake_boto3.sqs.sent_messages[0]
    assert sent_message["QueueUrl"] == "https://queue-url"
    sent_body = json.loads(sent_message["MessageBody"])
    assert sent_body["payload"] == {"hello": "world"}
    assert sent_body["requestId"] == body["requestId"]

    assert len(fake_boto3.table.items) == 1
    audit_item = fake_boto3.table.items[0]
    assert audit_item["pk"] == body["requestId"]
    assert audit_item["status"] == "RECEIVED"
    assert audit_item["ttl"] > 0


def test_worker_and_enricher_lambda_logic(monkeypatch):
    app_module = load_app_module(monkeypatch)

    class FakeAuditTable:
        def __init__(self):
            self.get_requests = []
            self.updates = []

        def get_item(self, **kwargs):
            self.get_requests.append(kwargs)
            return {"Item": {"pk": kwargs["Key"]["pk"], "status": "RECEIVED"}}

        def update_item(self, **kwargs):
            self.updates.append(kwargs)
            return {"Attributes": {}}

    class FakeDynamoResource:
        def __init__(self, table):
            self.table = table

        def Table(self, _name):
            return self.table

    class FakeS3Client:
        def __init__(self):
            self.objects = []

        def put_object(self, **kwargs):
            self.objects.append(kwargs)
            return {"ETag": "abc"}

    class FakeEventsClient:
        def __init__(self):
            self.entries = []

        def put_events(self, **kwargs):
            self.entries.append(kwargs)
            return {"FailedEntryCount": 0}

    class FakeBoto3:
        def __init__(self):
            self.table = FakeAuditTable()
            self.s3 = FakeS3Client()
            self.events = FakeEventsClient()
            self.client_calls = []
            self.resource_calls = []

        def client(self, service_name, **kwargs):
            self.client_calls.append((service_name, kwargs))
            if service_name == "s3":
                return self.s3
            if service_name == "events":
                return self.events
            raise AssertionError(service_name)

        def resource(self, service_name, **kwargs):
            self.resource_calls.append((service_name, kwargs))
            assert service_name == "dynamodb"
            return FakeDynamoResource(self.table)

    fake_boto3 = FakeBoto3()
    worker_handler = load_inline_handler(app_module.WORKER_LAMBDA_CODE, fake_boto3)

    monkeypatch.setenv("AWS_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("AUDIT_TABLE_NAME", "audit-table")
    monkeypatch.setenv("PROCESSED_BUCKET_NAME", "processed-bucket")
    monkeypatch.setenv("EVENT_BUS_NAME", "event-ingestion-bus")
    monkeypatch.setenv("EVENT_SOURCE", "app.ingestion")
    monkeypatch.setenv("EVENT_DETAIL_TYPE", "ProcessingComplete")

    event = {
        "Records": [
            {"body": json.dumps({"requestId": "req-123", "payload": {"a": 1}})}
        ]
    }
    response = worker_handler(event, None)

    assert response == {"batchItemFailures": []}
    assert ("dynamodb", {"endpoint_url": "https://example.invalid"}) in fake_boto3.resource_calls
    assert ("s3", {"endpoint_url": "https://example.invalid"}) in fake_boto3.client_calls
    assert ("events", {"endpoint_url": "https://example.invalid"}) in fake_boto3.client_calls
    assert fake_boto3.table.get_requests == [{"Key": {"pk": "req-123"}}]
    assert len(fake_boto3.table.updates) == 1
    assert len(fake_boto3.s3.objects) == 1
    stored_object = fake_boto3.s3.objects[0]
    assert stored_object["Bucket"] == "processed-bucket"
    assert stored_object["Key"] == "processed/req-123.json"
    assert stored_object["ContentType"] == "application/json"
    payload = json.loads(stored_object["Body"].decode("utf-8"))
    assert payload["requestId"] == "req-123"
    assert payload["audit"]["status"] == "RECEIVED"
    assert payload["payload"] == {"a": 1}

    assert len(fake_boto3.events.entries) == 1
    event_entry = fake_boto3.events.entries[0]["Entries"][0]
    assert event_entry["EventBusName"] == "event-ingestion-bus"
    assert event_entry["Source"] == "app.ingestion"
    assert event_entry["DetailType"] == "ProcessingComplete"
    assert json.loads(event_entry["Detail"]) == {
        "requestId": "req-123",
        "bucket": "processed-bucket",
        "key": "processed/req-123.json",
    }

    class NoopBoto3:
        def client(self, *_args, **_kwargs):
            raise AssertionError("Enricher must not create clients")

        def resource(self, *_args, **_kwargs):
            raise AssertionError("Enricher must not create resources")

    enricher_handler = load_inline_handler(app_module.ENRICHER_LAMBDA_CODE, NoopBoto3())
    enriched = enricher_handler({"body": json.dumps({"requestId": "req-123"})}, None)
    assert enriched["requestId"] == "req-123"
    assert enriched["enriched"] is True
    assert "timestamp" in enriched


def test_eventing_and_state_machine_contract(monkeypatch):
    _app_module, _app, _stack, template_json = synthesize(monkeypatch)
    queue_resources = resources_by_type(template_json, "AWS::SQS::Queue")
    queue_id, _ = find_single_resource(
        template_json,
        "AWS::SQS::Queue",
        lambda _id, res: res.get("Properties", {}).get("VisibilityTimeout") == 60,
    )
    enricher_lambda_id, _ = find_single_resource(
        template_json,
        "AWS::Lambda::Function",
        lambda _id, res: res["Properties"]["FunctionName"] == "event-ingestion-enricher",
    )
    state_machine_id, state_machine = find_single_resource(
        template_json, "AWS::StepFunctions::StateMachine"
    )
    _, bus = find_single_resource(template_json, "AWS::Events::EventBus")
    _, rule = find_single_resource(template_json, "AWS::Events::Rule")
    _, pipe = find_single_resource(template_json, "AWS::Pipes::Pipe")
    _, subscription = find_single_resource(template_json, "AWS::SNS::Subscription")
    notifications_topic_id, _ = find_single_resource(template_json, "AWS::SNS::Topic")

    assert len(queue_resources) == 2
    notifications_queue_ids = set(queue_resources) - {queue_id}
    assert len(notifications_queue_ids) == 1

    assert bus["Properties"]["Name"] != "default"
    assert rule["Properties"]["EventPattern"] == {
        "source": ["app.ingestion"],
        "detail-type": ["ProcessingComplete"],
    }
    assert len(rule["Properties"]["Targets"]) == 1
    assert rule["Properties"]["Targets"][0]["Arn"] == {"Fn::GetAtt": [state_machine_id, "Arn"]}

    definition = json.loads(state_machine["Properties"]["DefinitionString"])
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert definition["StartAt"] == "NormalizeInput"
    assert definition["States"]["NormalizeInput"]["Type"] == "Pass"
    assert definition["States"]["WriteStatusRow"]["Type"] == "Task"
    assert definition["States"]["WriteStatusRow"]["Resource"] == "arn:aws:states:::dynamodb:putItem"
    assert definition["States"]["PublishNotification"]["Type"] == "Task"
    assert definition["States"]["PublishNotification"]["Resource"] == "arn:aws:states:::sns:publish"
    assert definition["States"]["PublishNotification"]["End"] is True
    assert state_machine["Properties"]["DefinitionSubstitutions"]["NotificationsTopicArn"] == {
        "Ref": notifications_topic_id
    }

    assert pipe["Properties"]["Source"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert pipe["Properties"]["Enrichment"] == {"Fn::GetAtt": [enricher_lambda_id, "Arn"]}
    assert pipe["Properties"]["Target"] == {"Fn::GetAtt": [state_machine_id, "Arn"]}
    assert pipe["Properties"]["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    assert subscription["Properties"]["Protocol"] == "sqs"
    assert len(resources_by_type(template_json, "AWS::SNS::Subscription")) == 1
    assert subscription["Properties"]["Endpoint"] == {
        "Fn::GetAtt": [next(iter(notifications_queue_ids)), "Arn"]
    }


def test_persistence_analytics_outputs_and_destroyability(monkeypatch):
    _app_module, _app, stack, template_json = synthesize(monkeypatch)
    resources = template_json["Resources"]
    outputs = template_json["Outputs"]
    queue_resources = resources_by_type(template_json, "AWS::SQS::Queue")

    _, audit_table = find_single_resource(
        template_json,
        "AWS::DynamoDB::Table",
        lambda _id, res: "TimeToLiveSpecification" in res["Properties"],
    )
    _, status_table = find_single_resource(
        template_json,
        "AWS::DynamoDB::Table",
        lambda _id, res: "TimeToLiveSpecification" not in res["Properties"],
    )
    bucket_id, bucket = find_single_resource(template_json, "AWS::S3::Bucket")
    database_sg_id, _ = find_single_resource(
        template_json,
        "AWS::EC2::SecurityGroup",
        lambda _id, res: "database tier" in res["Properties"]["GroupDescription"].lower(),
    )
    _, database = find_single_resource(template_json, "AWS::RDS::DBInstance")
    subnet_group_id, subnet_group = find_single_resource(template_json, "AWS::RDS::DBSubnetGroup")
    _, secret = find_single_resource(template_json, "AWS::SecretsManager::Secret")
    _, crawler = find_single_resource(template_json, "AWS::Glue::Crawler")

    assert stack.termination_protection is False
    assert len(queue_resources) == 2
    ingestion_queue_id, ingestion_queue = find_single_resource(
        template_json,
        "AWS::SQS::Queue",
        lambda _id, res: res.get("Properties", {}).get("VisibilityTimeout") == 60,
    )
    assert ingestion_queue["Properties"]["MessageRetentionPeriod"] == 345600
    assert "FifoQueue" not in ingestion_queue["Properties"]

    assert len(resources_by_type(template_json, "AWS::DynamoDB::Table")) == 2
    assert audit_table["Properties"]["BillingMode"] == "PAY_PER_REQUEST"
    assert audit_table["Properties"]["TimeToLiveSpecification"] == {
        "AttributeName": "ttl",
        "Enabled": True,
    }
    assert audit_table["Properties"]["StreamSpecification"]["StreamViewType"] == "NEW_AND_OLD_IMAGES"
    assert status_table["Properties"]["BillingMode"] == "PAY_PER_REQUEST"
    assert status_table["Properties"]["KeySchema"] == [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]

    assert bucket["Properties"]["VersioningConfiguration"]["Status"] == "Enabled"
    lifecycle_rule = bucket["Properties"]["LifecycleConfiguration"]["Rules"][0]
    assert lifecycle_rule["Prefix"] == "processed/"
    assert lifecycle_rule["ExpirationInDays"] == 30

    assert database["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert database["Properties"]["Engine"] == "postgres"
    assert database["Properties"]["EngineVersion"].startswith("15.")
    assert database["Properties"]["AllocatedStorage"] == "20"
    assert database["Properties"]["StorageType"] == "gp2"
    assert database["Properties"]["PubliclyAccessible"] is False
    assert database["Properties"]["DeletionProtection"] is False
    assert database["Properties"]["DBSubnetGroupName"] == {"Ref": subnet_group_id}
    assert database["Properties"]["VPCSecurityGroups"] == [
        {"Fn::GetAtt": [database_sg_id, "GroupId"]}
    ]
    assert len(subnet_group["Properties"]["SubnetIds"]) == 2
    for subnet in subnet_group["Properties"]["SubnetIds"]:
        assert "PrivateSubnet" in subnet["Ref"]
    assert database["Properties"]["MasterUsername"]["Fn::Join"][0] == ""
    assert database["Properties"]["MasterUserPassword"]["Fn::Join"][0] == ""
    assert secret["DeletionPolicy"] == "Delete"

    assert len(resources_by_type(template_json, "AWS::Glue::Database")) == 1
    assert crawler["Properties"]["Targets"]["S3Targets"] == [
        {"Path": {"Fn::Join": ["", ["s3://", {"Ref": bucket_id}, "/processed/"]]}}
    ]

    assert sorted(outputs) == [
        "ApiInvokeUrl",
        "EventBusName",
        "IngestionQueueUrl",
        "ProcessedBucketName",
    ]
    assert outputs["IngestionQueueUrl"]["Value"] == {"Ref": ingestion_queue_id}

    allowed_resource_types = {
        "AWS::ApiGateway::Deployment",
        "AWS::ApiGateway::Method",
        "AWS::ApiGateway::Model",
        "AWS::ApiGateway::RequestValidator",
        "AWS::ApiGateway::Resource",
        "AWS::ApiGateway::RestApi",
        "AWS::ApiGateway::Stage",
        "AWS::DynamoDB::Table",
        "AWS::EC2::EIP",
        "AWS::EC2::InternetGateway",
        "AWS::EC2::NatGateway",
        "AWS::EC2::Route",
        "AWS::EC2::RouteTable",
        "AWS::EC2::SecurityGroup",
        "AWS::EC2::SecurityGroupIngress",
        "AWS::EC2::Subnet",
        "AWS::EC2::SubnetRouteTableAssociation",
        "AWS::EC2::VPC",
        "AWS::EC2::VPCGatewayAttachment",
        "AWS::Events::EventBus",
        "AWS::Events::Rule",
        "AWS::Glue::Crawler",
        "AWS::Glue::Database",
        "AWS::IAM::Role",
        "AWS::Lambda::EventSourceMapping",
        "AWS::Lambda::Function",
        "AWS::Lambda::Permission",
        "AWS::Logs::LogGroup",
        "AWS::Pipes::Pipe",
        "AWS::RDS::DBInstance",
        "AWS::RDS::DBSubnetGroup",
        "AWS::S3::Bucket",
        "AWS::SNS::Subscription",
        "AWS::SNS::Topic",
        "AWS::SQS::Queue",
        "AWS::SQS::QueuePolicy",
        "AWS::SecretsManager::Secret",
        "AWS::StepFunctions::StateMachine",
    }
    assert {resource["Type"] for resource in resources.values()} == allowed_resource_types

    for resource in resources.values():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}


def test_iam_scoping_contract(monkeypatch):
    _app_module, _app, _stack, template_json = synthesize(monkeypatch)
    notifications_topic_id, _ = find_single_resource(template_json, "AWS::SNS::Topic")
    ingestion_queue_id, _ = find_single_resource(
        template_json,
        "AWS::SQS::Queue",
        lambda _id, res: res.get("Properties", {}).get("VisibilityTimeout") == 60,
    )
    notifications_queue_id = next(
        queue_id
        for queue_id in resources_by_type(template_json, "AWS::SQS::Queue")
        if queue_id != ingestion_queue_id
    )
    audit_table_id, _ = find_single_resource(
        template_json,
        "AWS::DynamoDB::Table",
        lambda _id, res: "TimeToLiveSpecification" in res["Properties"],
    )
    status_table_id, _ = find_single_resource(
        template_json,
        "AWS::DynamoDB::Table",
        lambda _id, res: "TimeToLiveSpecification" not in res["Properties"],
    )
    bucket_id, _ = find_single_resource(template_json, "AWS::S3::Bucket")
    bus_id, _ = find_single_resource(template_json, "AWS::Events::EventBus")
    state_machine_id, _ = find_single_resource(template_json, "AWS::StepFunctions::StateMachine")
    enricher_lambda_id, _ = find_single_resource(
        template_json,
        "AWS::Lambda::Function",
        lambda _id, res: res["Properties"]["FunctionName"] == "event-ingestion-enricher",
    )
    _, queue_policy = find_single_resource(template_json, "AWS::SQS::QueuePolicy")
    ingest_role_id, ingest_role = role_with_policy(template_json, "IngestPermissions")
    worker_role_id, worker_role = role_with_policy(template_json, "WorkerPermissions")
    state_machine_role_id, state_machine_role = role_with_policy(template_json, "StateMachinePermissions")
    pipe_role_id, pipe_role = role_with_policy(template_json, "PipePermissions")
    glue_role_id, glue_role = role_with_policy(template_json, "GlueCrawlerPermissions")

    for _logical_id, role in [
        (ingest_role_id, ingest_role),
        (worker_role_id, worker_role),
        (state_machine_role_id, state_machine_role),
        (pipe_role_id, pipe_role),
        (glue_role_id, glue_role),
    ]:
        for action in flatten_actions(role):
            assert action != "*"

    ingest_actions = flatten_actions(ingest_role)
    assert "sqs:SendMessage" in ingest_actions
    assert "dynamodb:PutItem" in ingest_actions
    ingest_statements = policy_statements(ingest_role, "IngestPermissions")
    assert find_statement_with_action(ingest_statements, "sqs:SendMessage")["Resource"] == {
        "Fn::GetAtt": [ingestion_queue_id, "Arn"]
    }
    assert find_statement_with_action(ingest_statements, "dynamodb:PutItem")["Resource"] == {
        "Fn::GetAtt": [audit_table_id, "Arn"]
    }

    worker_actions = flatten_actions(worker_role)
    assert {"sqs:ReceiveMessage", "sqs:DeleteMessage"} <= worker_actions
    assert {"dynamodb:GetItem", "dynamodb:UpdateItem"} <= worker_actions
    assert "s3:PutObject" in worker_actions
    assert "events:PutEvents" in worker_actions
    worker_statements = policy_statements(worker_role, "WorkerPermissions")
    assert find_statement_with_action(worker_statements, "sqs:ReceiveMessage")["Resource"] == {
        "Fn::GetAtt": [ingestion_queue_id, "Arn"]
    }
    assert find_statement_with_action(worker_statements, "dynamodb:GetItem")["Resource"] == {
        "Fn::GetAtt": [audit_table_id, "Arn"]
    }
    assert find_statement_with_action(worker_statements, "s3:PutObject")["Resource"] == {
        "Fn::Join": ["", [{"Fn::GetAtt": [bucket_id, "Arn"]}, "/processed/*"]]
    }
    assert find_statement_with_action(worker_statements, "events:PutEvents")["Resource"] == {
        "Fn::GetAtt": [bus_id, "Arn"]
    }

    pipe_actions = flatten_actions(pipe_role)
    assert {"sqs:ReceiveMessage", "lambda:InvokeFunction", "states:StartExecution"} <= pipe_actions
    pipe_statements = policy_statements(pipe_role, "PipePermissions")
    assert find_statement_with_action(pipe_statements, "sqs:ReceiveMessage")["Resource"] == {
        "Fn::GetAtt": [ingestion_queue_id, "Arn"]
    }
    assert find_statement_with_action(pipe_statements, "lambda:InvokeFunction")["Resource"] == {
        "Fn::GetAtt": [enricher_lambda_id, "Arn"]
    }
    assert find_statement_with_action(pipe_statements, "states:StartExecution")["Resource"] == {
        "Fn::GetAtt": [state_machine_id, "Arn"]
    }

    state_machine_actions = flatten_actions(state_machine_role)
    assert {"dynamodb:PutItem", "dynamodb:UpdateItem", "sns:Publish"} <= state_machine_actions
    state_machine_statements = policy_statements(
        state_machine_role, "StateMachinePermissions"
    )
    assert find_statement_with_action(state_machine_statements, "dynamodb:PutItem")[
        "Resource"
    ] == {"Fn::GetAtt": [status_table_id, "Arn"]}
    assert find_statement_with_action(state_machine_statements, "sns:Publish")["Resource"] == {
        "Ref": notifications_topic_id
    }

    glue_actions = flatten_actions(glue_role)
    assert {"s3:GetObject", "s3:ListBucket", "glue:GetDatabase", "glue:CreateTable"} <= glue_actions

    statement = queue_policy["Properties"]["PolicyDocument"]["Statement"][0]
    assert statement["Principal"] == {"Service": "sns.amazonaws.com"}
    assert statement["Action"] == "sqs:SendMessage"
    assert statement["Condition"]["ArnEquals"]["aws:SourceArn"] == {"Ref": notifications_topic_id}
    assert statement["Resource"] == {"Fn::GetAtt": [notifications_queue_id, "Arn"]}
