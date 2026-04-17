import importlib.util
import json
import os
import sys
import tempfile
import types
from pathlib import Path

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template


def _load_app_module():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    spec = importlib.util.spec_from_file_location("stack_app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def synth_template() -> Template:
    os.environ.setdefault("AWS_REGION", "us-east-1")
    app_module = _load_app_module()
    cdk_app = app_module.build_app()
    stack = cdk_app.node.find_child("InternalWebAppStack")
    return Template.from_stack(stack)


def synth_template_dict():
    os.environ.setdefault("AWS_REGION", "us-east-1")
    app_module = _load_app_module()
    with tempfile.TemporaryDirectory() as outdir:
        cdk_app = app_module.build_app(outdir=outdir)
        cdk_app.synth()
        return json.loads((Path(outdir) / "InternalWebAppStack.template.json").read_text())


def _policy_by_name(template_dict, prefix):
    for logical_id, resource in template_dict["Resources"].items():
        if resource["Type"] == "AWS::IAM::Policy" and logical_id.startswith(prefix):
            return resource["Properties"]["PolicyDocument"]["Statement"]
    raise AssertionError(f"missing policy with prefix {prefix}")


def _find_resource(template_dict, resource_type, predicate=lambda _logical_id, _resource: True):
    for logical_id, resource in template_dict["Resources"].items():
        if resource["Type"] == resource_type and predicate(logical_id, resource):
            return logical_id, resource
    raise AssertionError(f"missing resource of type {resource_type}")


def _actions_for_statement(statement):
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def _security_group_logical_id(reference):
    if isinstance(reference, dict):
        if "Fn::GetAtt" in reference:
            return reference["Fn::GetAtt"][0]
        if "Ref" in reference:
            return reference["Ref"]
    raise AssertionError(f"unsupported security group reference: {reference}")


def _lambda_code_module(source, monkeypatch, *, with_pg8000):
    requested_services = []
    client_kwargs = {}
    sqs_messages = []
    secret_requests = []
    sql_calls = []
    connection_kwargs = {}

    class FakeCursor:
        def execute(self, sql, params=None):
            sql_calls.append((sql, params))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def close(self):
            return None

    class FakeSecretsClient:
        def get_secret_value(self, SecretId):
            secret_requests.append(SecretId)
            return {"SecretString": json.dumps({"username": "dbuser", "password": "dbpass"})}

    class FakeSqsClient:
        def send_message(self, QueueUrl, MessageBody):
            sqs_messages.append((QueueUrl, MessageBody))
            return {"MessageId": "msg-123"}

    class FakeSession:
        def client(self, service_name, **kwargs):
            requested_services.append(service_name)
            client_kwargs[service_name] = kwargs
            if service_name == "sqs":
                return FakeSqsClient()
            if service_name == "secretsmanager":
                return FakeSecretsClient()
            raise AssertionError(f"unexpected boto3 client request: {service_name}")

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.session = types.SimpleNamespace(Session=lambda **kwargs: FakeSession())

    fake_pg8000 = types.ModuleType("pg8000")
    fake_pg8000.connect = lambda **kwargs: connection_kwargs.update(kwargs) or FakeConnection()

    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    if with_pg8000:
        monkeypatch.setitem(sys.modules, "pg8000", fake_pg8000)
    else:
        monkeypatch.delitem(sys.modules, "pg8000", raising=False)

    module = types.ModuleType("embedded_lambda")
    exec(source, module.__dict__)
    return (
        module,
        requested_services,
        client_kwargs,
        sqs_messages,
        secret_requests,
        sql_calls,
        connection_kwargs,
    )


def test_exact_resource_inventory():
    template = synth_template()

    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::EC2::SecurityGroup", 2)
    template.resource_count_is("AWS::SQS::Queue", 1)
    template.resource_count_is("AWS::RDS::DBInstance", 1)
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.resource_count_is("AWS::Pipes::Pipe", 1)
    template.resource_count_is("AWS::Glue::Database", 1)
    template.resource_count_is("AWS::Glue::Crawler", 1)
    template.resource_count_is("AWS::Glue::Connection", 1)
    template.resource_count_is("AWS::Redshift::Cluster", 1)


def test_region_defaults_to_us_east_1_when_unset(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    app_module = _load_app_module()
    cdk_app = app_module.build_app()
    stack = cdk_app.node.find_child("InternalWebAppStack")
    assert stack.region == "us-east-1"


def test_vpc_topology_private_placement_and_az_distribution():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]

    subnets = {
        logical_id: resource["Properties"]
        for logical_id, resource in resources.items()
        if resource["Type"] == "AWS::EC2::Subnet"
    }
    assert len(subnets) == 4

    public_subnets = {
        logical_id: subnet
        for logical_id, subnet in subnets.items()
        if subnet["MapPublicIpOnLaunch"] is True
    }
    private_subnets = {
        logical_id: subnet
        for logical_id, subnet in subnets.items()
        if subnet["MapPublicIpOnLaunch"] is False
    }
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2

    azs = {
        json.dumps(subnet["AvailabilityZone"], sort_keys=True)
        for subnet in subnets.values()
    }
    assert len(azs) == 2

    private_refs = {json.dumps({"Ref": logical_id}, sort_keys=True) for logical_id in private_subnets}

    _, backend_lambda = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: resource["Properties"]["Environment"]["Variables"].get("APP_DB_NAME") == "orders",
    )
    assert {
        json.dumps(subnet_ref, sort_keys=True)
        for subnet_ref in backend_lambda["Properties"]["VpcConfig"]["SubnetIds"]
    } == private_refs

    _, db_subnet_group = _find_resource(template_dict, "AWS::RDS::DBSubnetGroup")
    assert {
        json.dumps(subnet_ref, sort_keys=True)
        for subnet_ref in db_subnet_group["Properties"]["SubnetIds"]
    } == private_refs

    _, redshift_subnet_group = _find_resource(template_dict, "AWS::Redshift::ClusterSubnetGroup")
    assert {
        json.dumps(subnet_ref, sort_keys=True)
        for subnet_ref in redshift_subnet_group["Properties"]["SubnetIds"]
    } == private_refs

    template = Template.from_json(template_dict)
    template.resource_count_is("AWS::EC2::NatGateway", 1)
    template.has_resource_properties(
        "AWS::EC2::VPC",
        {
            "EnableDnsHostnames": True,
            "EnableDnsSupport": True,
        },
    )


def test_security_groups_do_not_allow_public_ingress_and_db_uses_dedicated_group():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]
    database_sg_id, _ = _find_resource(
        template_dict,
        "AWS::EC2::SecurityGroup",
        lambda _lid, resource: "PostgreSQL database tier" in resource["Properties"]["GroupDescription"],
    )

    ingress_resources = [
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::EC2::SecurityGroupIngress"
    ]
    assert len(ingress_resources) == 1

    ingress = ingress_resources[0]
    assert ingress["IpProtocol"] == "tcp"
    assert ingress["FromPort"] == 5432
    assert ingress["ToPort"] == 5432
    assert "SourceSecurityGroupId" in ingress
    assert "CidrIp" not in ingress

    for resource in resources.values():
        if resource["Type"] == "AWS::EC2::SecurityGroup":
            assert "SecurityGroupIngress" not in resource["Properties"]

    db_instance = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::RDS::DBInstance"
    )
    assert db_instance["VPCSecurityGroups"] == [{"Fn::GetAtt": [database_sg_id, "GroupId"]}]

    redshift = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Redshift::Cluster"
    )
    assert redshift["VpcSecurityGroupIds"] == [{"Fn::GetAtt": [database_sg_id, "GroupId"]}]

    glue_connection = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Glue::Connection"
    )
    glue_requirements = glue_connection["ConnectionInput"]["PhysicalConnectionRequirements"]
    assert glue_requirements["SecurityGroupIdList"] == [{"Fn::GetAtt": [database_sg_id, "GroupId"]}]


def test_enrichment_lambda_uses_only_declared_security_groups():
    template_dict = synth_template_dict()
    declared_security_group_ids = {
        logical_id
        for logical_id, resource in template_dict["Resources"].items()
        if resource["Type"] == "AWS::EC2::SecurityGroup"
    }
    backend_sg_id, _ = _find_resource(
        template_dict,
        "AWS::EC2::SecurityGroup",
        lambda _lid, resource: "backend API handler" in resource["Properties"]["GroupDescription"],
    )
    _, enrichment_lambda = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: "APP_DB_NAME" not in resource["Properties"]["Environment"]["Variables"],
    )

    enrichment_security_group_ids = [
        _security_group_logical_id(reference)
        for reference in enrichment_lambda["Properties"]["VpcConfig"]["SecurityGroupIds"]
    ]
    assert set(enrichment_security_group_ids).issubset(declared_security_group_ids)
    assert enrichment_security_group_ids == [backend_sg_id]


def test_lambdas_use_required_shape_logs_and_zip_package():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]

    lambda_functions = [
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Lambda::Function"
    ]
    assert len(lambda_functions) == 2

    for function in lambda_functions:
        assert function["Runtime"] == "python3.12"
        assert function["MemorySize"] == 256
        assert function["Timeout"] == 15
        assert function.get("PackageType", "Zip") == "Zip"

    log_groups = [
        resource
        for resource in resources.values()
        if resource["Type"] == "AWS::Logs::LogGroup"
    ]
    assert len(log_groups) >= 4
    for resource in log_groups:
        assert resource["Properties"]["RetentionInDays"] == 14
        assert "KmsKeyId" not in resource["Properties"]


def test_api_stage_route_and_access_logs_match_prompt():
    template = synth_template()
    template_dict = synth_template_dict()

    template.has_resource_properties(
        "AWS::ApiGatewayV2::Api",
        {
            "ProtocolType": "HTTP",
        },
    )
    template.has_resource_properties(
        "AWS::ApiGatewayV2::Route",
        {
            "RouteKey": "POST /orders",
        },
    )
    template.has_resource_properties(
        "AWS::ApiGatewayV2::Stage",
        {
            "StageName": "$default",
            "AutoDeploy": True,
            "AccessLogSettings": {
                "DestinationArn": Match.any_value(),
                "Format": Match.string_like_regexp("requestId"),
            },
        },
    )

    _, stage = _find_resource(template_dict, "AWS::ApiGatewayV2::Stage")
    destination_arn = stage["Properties"]["AccessLogSettings"]["DestinationArn"]
    assert "Fn::GetAtt" in destination_arn
    access_log_group_id = destination_arn["Fn::GetAtt"][0]
    access_log_group = template_dict["Resources"][access_log_group_id]
    assert access_log_group["Type"] == "AWS::Logs::LogGroup"
    assert access_log_group["Properties"]["RetentionInDays"] == 14
    assert "KmsKeyId" not in access_log_group["Properties"]


def test_rds_and_redshift_use_generated_secrets_without_inline_passwords():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]

    secrets = [
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::SecretsManager::Secret"
    ]
    assert len(secrets) == 2
    assert all("GenerateSecretString" in secret for secret in secrets)

    db_instance = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::RDS::DBInstance"
    )
    assert "resolve:secretsmanager" in json.dumps(db_instance["MasterUsername"])
    assert "resolve:secretsmanager" in json.dumps(db_instance["MasterUserPassword"])
    assert db_instance["PubliclyAccessible"] is False
    assert db_instance["StorageType"] == "gp2"
    assert db_instance["BackupRetentionPeriod"] == 1
    assert db_instance["DeletionProtection"] is False
    assert db_instance["AllocatedStorage"] == "20"
    assert db_instance["DBInstanceClass"] == "db.t3.micro"

    redshift = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::Redshift::Cluster"
    )
    assert "resolve:secretsmanager" in json.dumps(redshift["MasterUsername"])
    assert "resolve:secretsmanager" in json.dumps(redshift["MasterUserPassword"])
    assert redshift["PubliclyAccessible"] is False
    assert redshift["ClusterType"] == "single-node"
    assert redshift["NodeType"] == "dc2.large"
    assert redshift["DBName"] == "analytics"
    assert "AWS::SecretsManager::SecretTargetAttachment" not in {
        resource["Type"] for resource in resources.values()
    }


def test_state_machine_definition_is_single_task_to_backend_lambda_with_logging():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]
    backend_lambda_id, _ = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: resource["Properties"]["Environment"]["Variables"].get("APP_DB_NAME") == "orders",
    )

    state_machine = next(
        resource["Properties"]
        for resource in resources.values()
        if resource["Type"] == "AWS::StepFunctions::StateMachine"
    )
    definition_parts = state_machine["DefinitionString"]["Fn::Join"][1]
    rendered_definition = "".join(
        part if isinstance(part, str) else "__BACKEND_LAMBDA_ARN__"
        for part in definition_parts
    )

    assert rendered_definition.count('"Type":"Task"') == 1
    assert '"StartAt":"InvokeBackendApiHandler"' in rendered_definition
    assert "__BACKEND_LAMBDA_ARN__" in rendered_definition
    assert state_machine["StateMachineType"] == "STANDARD"
    assert state_machine["LoggingConfiguration"]["Level"] == "ALL"

    task_references = [
        part["Fn::GetAtt"]
        for part in definition_parts
        if isinstance(part, dict) and "Fn::GetAtt" in part
    ]
    assert [backend_lambda_id, "Arn"] in task_references


def test_pipe_configuration_and_glue_redshift_wiring_match_prompt():
    template_dict = synth_template_dict()
    pipe_id, pipe_resource = _find_resource(template_dict, "AWS::Pipes::Pipe")
    state_machine_id, _ = _find_resource(template_dict, "AWS::StepFunctions::StateMachine")
    enrichment_lambda_id, _ = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: "APP_DB_NAME" not in resource["Properties"]["Environment"]["Variables"],
    )

    pipe = pipe_resource["Properties"]
    assert pipe["DesiredState"] == "RUNNING"
    assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe["TargetParameters"]["InputTemplate"] == '{"messageBody": <$.originalBody>, "enrichment": <$.enrichment>}'
    assert (
        pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
        == "FIRE_AND_FORGET"
    )
    assert pipe["Enrichment"] == {"Fn::GetAtt": [enrichment_lambda_id, "Arn"]}
    assert pipe["Target"] == {"Ref": state_machine_id}
    assert set(pipe.keys()) == {
        "DesiredState",
        "Enrichment",
        "RoleArn",
        "Source",
        "SourceParameters",
        "Target",
        "TargetParameters",
    }

    connection = next(
        resource["Properties"]
        for resource in template_dict["Resources"].values()
        if resource["Type"] == "AWS::Glue::Connection"
    )
    jdbc_url = json.dumps(
        connection["ConnectionInput"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
    )
    assert "jdbc:redshift://" in jdbc_url

    queue = next(
        resource["Properties"]
        for resource in template_dict["Resources"].values()
        if resource["Type"] == "AWS::SQS::Queue"
    )
    assert queue["SqsManagedSseEnabled"] is True

    crawler_id, crawler_resource = _find_resource(template_dict, "AWS::Glue::Crawler")
    assert pipe_id
    assert crawler_id

    crawler = next(
        resource["Properties"]
        for resource in template_dict["Resources"].values()
        if resource["Type"] == "AWS::Glue::Crawler"
    )
    assert crawler["DatabaseName"] == "analytics_catalog"
    assert set(crawler["Targets"].keys()) == {"JdbcTargets"}
    assert len(crawler["Targets"]["JdbcTargets"]) == 1
    assert "S3Targets" not in crawler["Targets"]
    assert "MongoDBTargets" not in crawler["Targets"]
    assert "DynamoDBTargets" not in crawler["Targets"]
    assert crawler_resource["Properties"]["Targets"]["JdbcTargets"][0]["Path"] == "analytics/public/%"


def test_iam_policies_are_least_privilege_for_primary_roles():
    template_dict = synth_template_dict()
    resources = template_dict["Resources"]

    backend_lambda_id, _ = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: resource["Properties"]["Environment"]["Variables"].get("APP_DB_NAME") == "orders",
    )
    _, enrichment_lambda = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: "APP_DB_NAME" not in resource["Properties"]["Environment"]["Variables"],
    )
    enrichment_lambda_id = next(
        logical_id
        for logical_id, resource in resources.items()
        if resource is enrichment_lambda
    )
    queue_id, _ = _find_resource(template_dict, "AWS::SQS::Queue")
    db_secret_id, _ = _find_resource(
        template_dict,
        "AWS::SecretsManager::Secret",
        lambda _lid, resource: resource["Properties"]["GenerateSecretString"]["SecretStringTemplate"] == '{"username":"appadmin"}',
    )
    state_machine_id, _ = _find_resource(template_dict, "AWS::StepFunctions::StateMachine")
    redshift_secret_id, _ = _find_resource(
        template_dict,
        "AWS::SecretsManager::Secret",
        lambda _lid, resource: resource["Properties"]["GenerateSecretString"]["SecretStringTemplate"] == '{"username":"analyticsadmin"}',
    )

    backend_policy = _policy_by_name(template_dict, "BackendApiHandlerRoleDefaultPolicy")
    assert any(
        statement["Action"] == "sqs:SendMessage"
        and statement["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
        for statement in backend_policy
    )
    assert any(
        statement["Action"] == ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        and statement["Resource"] == {"Ref": db_secret_id}
        for statement in backend_policy
    )
    assert not any("*" in _actions_for_statement(statement) for statement in backend_policy)
    assert not any("states:StartExecution" in json.dumps(statement) for statement in backend_policy)

    enrichment_policy = _policy_by_name(template_dict, "EventEnrichmentRoleDefaultPolicy")
    assert any("logs:PutLogEvents" in json.dumps(statement) for statement in enrichment_policy)
    assert not any("states:StartExecution" in json.dumps(statement) for statement in enrichment_policy)
    assert not any("lambda:InvokeFunction" in json.dumps(statement) for statement in enrichment_policy)
    assert all(
        action.startswith(("logs:", "ec2:"))
        for statement in enrichment_policy
        for action in _actions_for_statement(statement)
    )

    pipe_policy = _policy_by_name(template_dict, "OrderProcessingPipeRoleDefaultPolicy")
    assert any(
        statement["Action"] == ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
        and statement["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
        for statement in pipe_policy
    )
    assert any(
        statement["Action"] == "lambda:InvokeFunction"
        and statement["Resource"] == {"Fn::GetAtt": [enrichment_lambda_id, "Arn"]}
        for statement in pipe_policy
    )
    assert any(
        statement["Action"] == "states:StartExecution"
        and statement["Resource"] == {"Ref": state_machine_id}
        for statement in pipe_policy
    )
    assert not any(statement["Resource"] == "*" for statement in pipe_policy)

    state_machine_policy = _policy_by_name(template_dict, "OrderFulfillmentStateMachineRoleDefaultPolicy")
    invoke_statements = [
        statement
        for statement in state_machine_policy
        if statement["Action"] == "lambda:InvokeFunction"
    ]
    assert invoke_statements
    assert all(
        json.dumps(statement["Resource"]).count(backend_lambda_id) > 0
        for statement in invoke_statements
    )
    assert any("logs:CreateLogDelivery" in json.dumps(statement) for statement in state_machine_policy)

    glue_policy = _policy_by_name(template_dict, "AnalyticsGlueCrawlerRoleDefaultPolicy")
    assert any(":database/analytics_catalog" in json.dumps(statement) for statement in glue_policy)
    assert any(":connection/analytics-redshift-jdbc" in json.dumps(statement) for statement in glue_policy)
    assert any(redshift_secret_id in json.dumps(statement) for statement in glue_policy)


def test_enrichment_lambda_has_no_direct_triggers_or_public_endpoints():
    template = synth_template()
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 0)
    template.resource_count_is("AWS::Lambda::Url", 0)

    template_dict = synth_template_dict()
    backend_lambda_id, _ = _find_resource(
        template_dict,
        "AWS::Lambda::Function",
        lambda _lid, resource: resource["Properties"]["Environment"]["Variables"].get("APP_DB_NAME") == "orders",
    )
    permissions = [
        resource["Properties"]
        for resource in template_dict["Resources"].values()
        if resource["Type"] == "AWS::Lambda::Permission"
    ]
    assert len(permissions) == 1
    assert permissions[0]["Principal"] == "apigateway.amazonaws.com"
    assert permissions[0]["FunctionName"] == {"Ref": backend_lambda_id}


def test_all_resources_are_destructible_without_retain_policies():
    template_dict = synth_template_dict()

    for resource in template_dict["Resources"].values():
        assert resource.get("DeletionPolicy") != "Retain"
        assert resource.get("UpdateReplacePolicy") != "Retain"


def test_backend_lambda_runtime_validates_enqueues_once_and_writes_single_row(monkeypatch):
    app_module = _load_app_module()
    (
        module,
        requested_services,
        client_kwargs,
        sqs_messages,
        secret_requests,
        sql_calls,
        connection_kwargs,
    ) = _lambda_code_module(app_module.BACKEND_LAMBDA_CODE, monkeypatch, with_pg8000=True)

    monkeypatch.setenv("WORK_QUEUE_URL", "https://sqs.example/orders")
    monkeypatch.setenv("APP_DB_SECRET_ARN", "arn:aws:secretsmanager:::secret:db")
    monkeypatch.setenv("APP_DB_HOST", "db.example")
    monkeypatch.setenv("APP_DB_PORT", "5432")
    monkeypatch.setenv("APP_DB_NAME", "orders")
    monkeypatch.setenv("AWS_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    response = module.handler(
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"orderId": "ord-1", "customerId": "cust-1"}),
        },
        None,
    )

    assert response["statusCode"] == 202
    assert len(sqs_messages) == 1
    assert sqs_messages[0][0] == "https://sqs.example/orders"
    assert len(secret_requests) == 1
    assert secret_requests[0] == "arn:aws:secretsmanager:::secret:db"
    insert_calls = [call for call in sql_calls if call[0].lower().startswith("insert into orders")]
    assert len(insert_calls) == 1
    assert insert_calls[0][1][0:2] == ("ord-1", "cust-1")
    assert len(insert_calls[0][1]) == 3
    assert connection_kwargs["host"] == "db.example"
    assert client_kwargs["sqs"]["endpoint_url"] == "http://endpoint"
    assert client_kwargs["secretsmanager"]["endpoint_url"] == "http://endpoint"
    assert client_kwargs["sqs"]["region_name"] == "us-east-1"
    assert all(service in {"sqs", "secretsmanager"} for service in requested_services)
    assert "stepfunctions" not in requested_services


def test_backend_lambda_rejects_invalid_payload_without_side_effects(monkeypatch):
    app_module = _load_app_module()
    (
        module,
        requested_services,
        _client_kwargs,
        sqs_messages,
        _secret_requests,
        _sql_calls,
        _connection_kwargs,
    ) = _lambda_code_module(app_module.BACKEND_LAMBDA_CODE, monkeypatch, with_pg8000=False)

    monkeypatch.setenv("WORK_QUEUE_URL", "https://sqs.example/orders")

    invalid_events = [
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"customerId": "cust-1"}),
        },
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"orderId": "ord-1"}),
        },
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": '{"orderId":',
        },
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": "",
        },
        {
            "requestContext": {"http": {"method": "POST"}},
            "body": None,
        },
    ]

    for event in invalid_events:
        response = module.handler(event, None)
        assert response["statusCode"] == 400

    assert sqs_messages == []
    assert requested_services == []


def test_backend_lambda_handles_step_functions_invocation_without_api_side_effects(monkeypatch):
    app_module = _load_app_module()
    (
        module,
        requested_services,
        _client_kwargs,
        sqs_messages,
        _secret_requests,
        _sql_calls,
        _connection_kwargs,
    ) = _lambda_code_module(app_module.BACKEND_LAMBDA_CODE, monkeypatch, with_pg8000=False)

    workflow_event = {
        "messageBody": json.dumps({"orderId": "ord-1", "customerId": "cust-1"}),
        "enrichment": {"isEnriched": True},
    }
    response = module.handler(workflow_event, None)

    assert response["status"] == "FULFILLED"
    assert response["workflowId"]
    assert response["input"] == workflow_event
    assert sqs_messages == []
    assert requested_services == []


def test_enrichment_lambda_runtime_returns_original_body_and_enrichment(monkeypatch):
    app_module = _load_app_module()
    module, *_ = _lambda_code_module(app_module.ENRICHMENT_LAMBDA_CODE, monkeypatch, with_pg8000=False)

    original_body = json.dumps({"orderId": "ord-1", "customerId": "cust-1"})
    response = module.handler({"body": original_body}, None)

    assert response["originalBody"] == original_body
    assert response["enrichment"]["source"] == "event-enrichment"
    assert response["enrichment"]["isEnriched"] is True
