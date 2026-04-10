import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

import pytest


ROOT = Path(__file__).resolve().parents[1]
APP_TS = ROOT / "app.ts"
DIST_APP = ROOT / "dist" / "app.js"
ALLOWED_EXTERNAL_INPUTS = {
    "AWS_ENDPOINT",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
}
ALLOWED_ENV_REFERENCES = ALLOWED_EXTERNAL_INPUTS | {
    "AWS_ENDPOINT_URL",
    "CDK_DEFAULT_REGION",
    "ORDER_QUEUE_URL",
    "DB_SECRET_ARN",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "AUDIT_BUCKET_NAME",
}


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("AWS_REGION", "us-east-1")
    return env


def _run(cmd: list[str], *, env: Optional[dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=True,
        cwd=ROOT,
        env=env or _base_env(),
        capture_output=True,
        text=True,
    )


def _run_node_json(script: str, *, env: Optional[dict[str, str]] = None) -> Any:
    completed = subprocess.run(
        ["node", "-e", script],
        check=True,
        cwd=ROOT,
        env=env or _base_env(),
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


@pytest.fixture(scope="session")
def built_app() -> Path:
    _run(["npm", "run", "build"])
    assert DIST_APP.exists()
    return DIST_APP


@pytest.fixture(scope="session")
def template(built_app: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cdk-unit-synth-") as output_dir:
        _run(["npx", "cdk", "synth", "--output", output_dir])
        return json.loads((Path(output_dir) / "BackendLogicStack.template.json").read_text())


@pytest.fixture(scope="session")
def resources(template: dict[str, Any]) -> dict[str, Any]:
    return template["Resources"]


def _entries_of_type(resources: dict[str, Any], resource_type: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (logical_id, resource)
        for logical_id, resource in resources.items()
        if resource["Type"] == resource_type
    ]


def _only_resource(
    resources: dict[str, Any],
    resource_type: str,
    predicate: Optional[Any] = None,
) -> tuple[str, dict[str, Any]]:
    matches = [
        entry
        for entry in _entries_of_type(resources, resource_type)
        if predicate is None or predicate(entry)
    ]
    assert len(matches) == 1, matches
    return matches[0]


def _attached_policies(resources: dict[str, Any], role_logical_id: str) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    for _, policy in _entries_of_type(resources, "AWS::IAM::Policy"):
        if any(role_ref["Ref"] == role_logical_id for role_ref in policy["Properties"].get("Roles", [])):
            policies.append(policy)
    return policies


def _policy_statements(resources: dict[str, Any], role_logical_id: str) -> list[dict[str, Any]]:
    return [
        statement
        for policy in _attached_policies(resources, role_logical_id)
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
    ]


def _flatten_actions(action: Any) -> list[str]:
    return action if isinstance(action, list) else [action]


def _flatten_resources(resource: Any) -> list[Any]:
    return resource if isinstance(resource, list) else [resource]


def _flatten_cfn(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_flatten_cfn(part) for part in value)
    if isinstance(value, dict):
        if "Fn::Join" in value:
            return "".join(_flatten_cfn(part) for part in value["Fn::Join"][1])
        if "Ref" in value:
            return f"<Ref:{value['Ref']}>"
        if "Fn::GetAtt" in value:
            logical_id, attribute = value["Fn::GetAtt"]
            return f"<GetAtt:{logical_id}.{attribute}>"
    return str(value)


def _env_vars(resource: dict[str, Any]) -> dict[str, Any]:
    return resource["Properties"].get("Environment", {}).get("Variables", {})


def _orders_lambda(resources: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return _only_resource(
        resources,
        "AWS::Lambda::Function",
        lambda entry: "ORDER_QUEUE_URL" in _env_vars(entry[1]),
    )


def _processor_lambda(resources: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return _only_resource(
        resources,
        "AWS::Lambda::Function",
        lambda entry: "AUDIT_BUCKET_NAME" in _env_vars(entry[1]),
    )


def _enrichment_lambda(resources: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    orders_lambda_id, _ = _orders_lambda(resources)
    processor_lambda_id, _ = _processor_lambda(resources)
    return _only_resource(
        resources,
        "AWS::Lambda::Function",
        lambda entry: entry[0] not in {orders_lambda_id, processor_lambda_id},
    )


def _compute_and_database_security_groups(resources: dict[str, Any]) -> tuple[tuple[str, dict[str, Any]], tuple[str, dict[str, Any]]]:
    _, orders_lambda = _orders_lambda(resources)
    _, db_instance = _only_resource(resources, "AWS::RDS::DBInstance")

    compute_sg_id = orders_lambda["Properties"]["VpcConfig"]["SecurityGroupIds"][0]["Fn::GetAtt"][0]
    database_sg_id = db_instance["Properties"]["VPCSecurityGroups"][0]["Fn::GetAtt"][0]

    return (
        _only_resource(resources, "AWS::EC2::SecurityGroup", lambda entry: entry[0] == compute_sg_id),
        _only_resource(resources, "AWS::EC2::SecurityGroup", lambda entry: entry[0] == database_sg_id),
    )


def test_source_contract_and_allowed_inputs(built_app: Path) -> None:
    root_ts_files = sorted(path.name for path in ROOT.glob("*.ts"))
    app_source = APP_TS.read_text()
    referenced_envs = set(re.findall(r"process\\.env\\.([A-Z0-9_]+)", app_source))

    assert root_ts_files == ["app.ts"]
    assert "tryGetContext(" not in app_source
    assert "node.getContext(" not in app_source
    assert "unsafeUnwrap(" not in app_source
    assert not re.search(r"secretValue(?:FromJson)?\([^)]*\)\.(?:toString|unsafeUnwrap)\(", app_source)
    for env_name in ALLOWED_EXTERNAL_INPUTS:
        assert env_name in app_source
    assert referenced_envs <= ALLOWED_ENV_REFERENCES


def test_synthesize_app_defaults_region_and_applies_endpoint_overrides(built_app: Path) -> None:
    env = os.environ.copy()
    env.pop("AWS_REGION", None)
    env.pop("AWS_ENDPOINT", None)
    env.pop("AWS_ENDPOINT_URL", None)
    env.pop("CDK_DEFAULT_REGION", None)
    env["AWS_ACCESS_KEY_ID"] = "fixture-access-key"
    env["AWS_SECRET_ACCESS_KEY"] = "fixture-secret-key"
    env["AWS_ENDPOINT"] = "  https://custom-endpoint.internal.example/base  "

    result = _run_node_json(
        """
        const mod = require('./dist/app.js');
        const app = mod.synthesizeApp();
        const stack = app.node.findChild('BackendLogicStack');
        console.log(JSON.stringify({
          stackRegion: stack.region,
          processEnv: {
            AWS_REGION: process.env.AWS_REGION,
            CDK_DEFAULT_REGION: process.env.CDK_DEFAULT_REGION,
            AWS_ENDPOINT: process.env.AWS_ENDPOINT,
            AWS_ENDPOINT_URL: process.env.AWS_ENDPOINT_URL,
          },
        }));
        """,
        env=env,
    )

    assert result["stackRegion"] == "us-east-1"
    assert result["processEnv"] == {
        "AWS_REGION": "us-east-1",
        "CDK_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT": "https://custom-endpoint.internal.example/base",
        "AWS_ENDPOINT_URL": "https://custom-endpoint.internal.example/base",
    }


def test_network_fabric_topology(resources: dict[str, Any]) -> None:
    assert len(_entries_of_type(resources, "AWS::EC2::VPC")) == 1
    assert len(_entries_of_type(resources, "AWS::EC2::Subnet")) == 4
    assert len(_entries_of_type(resources, "AWS::EC2::NatGateway")) == 1
    assert len(_entries_of_type(resources, "AWS::EC2::SecurityGroup")) == 2

    subnets = [resource["Properties"] for _, resource in _entries_of_type(resources, "AWS::EC2::Subnet")]
    public_subnets = [subnet for subnet in subnets if subnet["MapPublicIpOnLaunch"]]
    private_subnets = [subnet for subnet in subnets if not subnet["MapPublicIpOnLaunch"]]

    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert sorted(subnet["AvailabilityZone"]["Fn::Select"][0] for subnet in subnets) == [0, 0, 1, 1]


def test_security_groups_are_strict(resources: dict[str, Any]) -> None:
    (compute_sg_id, compute_sg), (database_sg_id, database_sg) = _compute_and_database_security_groups(resources)
    ingress_rules = _entries_of_type(resources, "AWS::EC2::SecurityGroupIngress")

    assert compute_sg["Properties"].get("SecurityGroupIngress") is None
    assert any(
        rule.get("IpProtocol") == "-1" and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in compute_sg["Properties"]["SecurityGroupEgress"]
    )
    assert database_sg["Properties"].get("SecurityGroupIngress") is None
    assert len(ingress_rules) == 1
    _, ingress_rule = ingress_rules[0]
    assert ingress_rule["Properties"] == {
        "Description": "Allow PostgreSQL access only from the compute tier",
        "FromPort": 5432,
        "GroupId": {"Fn::GetAtt": [database_sg_id, "GroupId"]},
        "IpProtocol": "tcp",
        "SourceSecurityGroupId": {"Fn::GetAtt": [compute_sg_id, "GroupId"]},
        "ToPort": 5432,
    }


def test_api_gateway_and_lambda_shape(resources: dict[str, Any]) -> None:
    assert len(_entries_of_type(resources, "AWS::ApiGateway::RestApi")) == 1
    assert len(_entries_of_type(resources, "AWS::ApiGateway::Resource")) == 1
    assert len(_entries_of_type(resources, "AWS::ApiGateway::Method")) == 2

    _, orders_resource = _only_resource(resources, "AWS::ApiGateway::Resource")
    assert orders_resource["Properties"]["PathPart"] == "orders"

    methods = [resource["Properties"] for _, resource in _entries_of_type(resources, "AWS::ApiGateway::Method")]
    assert sorted(method["HttpMethod"] for method in methods) == ["GET", "POST"]
    assert len({str(method["Integration"]["Uri"]) for method in methods}) == 1

    (compute_sg_id, _), _ = _compute_and_database_security_groups(resources)
    _, orders_lambda = _orders_lambda(resources)
    lambda_props = orders_lambda["Properties"]

    assert lambda_props["Runtime"] == "nodejs20.x"
    assert lambda_props["MemorySize"] == 256
    assert lambda_props["Timeout"] == 10
    assert lambda_props["Handler"].endswith(".handler")
    assert "ZipFile" in lambda_props["Code"]
    assert lambda_props["VpcConfig"]["SecurityGroupIds"] == [
        {"Fn::GetAtt": [compute_sg_id, "GroupId"]}
    ]
    assert len(lambda_props["VpcConfig"]["SubnetIds"]) == 2


def test_lambda_log_group_and_inline_handler_contract(resources: dict[str, Any]) -> None:
    _, log_group = _only_resource(resources, "AWS::Logs::LogGroup")
    _, orders_lambda = _orders_lambda(resources)
    handler_code = orders_lambda["Properties"]["Code"]["ZipFile"]

    assert log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in log_group["Properties"]
    assert 'if (method === "POST" && path === "/orders")' in handler_code
    assert 'if (method === "GET" && path === "/orders")' in handler_code
    assert 'event.source === "scheduler" && event.action === "heartbeat"' in handler_code
    assert 'new SendMessageCommand' in handler_code
    assert "orderId" in handler_code
    assert "timestamp" in handler_code
    assert 'MessageBody: JSON.stringify(payload)' in handler_code
    assert 'new GetSecretValueCommand' in handler_code
    assert 'await loadDbSecret();' in handler_code
    assert 'await checkDatabaseEndpoint(' in handler_code
    assert 'statusCode: 202' in handler_code
    assert 'statusCode: 200' in handler_code


def test_sqs_and_api_lambda_permissions(resources: dict[str, Any]) -> None:
    queue_id, queue = _only_resource(resources, "AWS::SQS::Queue")
    _, orders_lambda = _orders_lambda(resources)
    orders_role_id = orders_lambda["Properties"]["Role"]["Fn::GetAtt"][0]
    statements = _policy_statements(resources, orders_role_id)

    assert queue["Properties"] == {
        "MessageRetentionPeriod": 345600,
        "SqsManagedSseEnabled": True,
        "VisibilityTimeout": 30,
    }

    sqs_statement = next(
        statement
        for statement in statements
        if "sqs:SendMessage" in _flatten_actions(statement["Action"])
    )
    secret_statement = next(
        statement
        for statement in statements
        if "secretsmanager:GetSecretValue" in _flatten_actions(statement["Action"])
    )

    assert sqs_statement["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert sorted(_flatten_actions(secret_statement["Action"])) == [
        "secretsmanager:DescribeSecret",
        "secretsmanager:GetSecretValue",
    ]
    assert secret_statement["Resource"] == _env_vars(orders_lambda)["DB_SECRET_ARN"]


def test_state_machine_pipe_and_scheduler(resources: dict[str, Any]) -> None:
    queue_id, _ = _only_resource(resources, "AWS::SQS::Queue")
    state_machine_id, state_machine = _only_resource(resources, "AWS::StepFunctions::StateMachine")
    orders_lambda_id, _ = _orders_lambda(resources)
    enrichment_lambda_id, _ = _enrichment_lambda(resources)
    processor_lambda_id, _ = _processor_lambda(resources)
    _, pipe = _only_resource(resources, "AWS::Pipes::Pipe")
    _, schedule = _only_resource(resources, "AWS::Scheduler::Schedule")

    definition = json.loads(_flatten_cfn(state_machine["Properties"]["DefinitionString"]))
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert len(definition["States"]) >= 2
    assert any(state["Type"] == "Task" for state in definition["States"].values())
    assert any(state["Type"] == "Succeed" for state in definition["States"].values())
    task_state = next(state for state in definition["States"].values() if state["Type"] == "Task")
    assert task_state["Resource"] == f"<GetAtt:{processor_lambda_id}.Arn>"

    assert pipe["Properties"]["Source"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert pipe["Properties"]["Target"] == {"Ref": state_machine_id}
    assert pipe["Properties"]["Enrichment"] == {"Fn::GetAtt": [enrichment_lambda_id, "Arn"]}
    assert pipe["Properties"]["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    assert schedule["Properties"]["ScheduleExpression"] == "rate(5 minutes)"
    assert schedule["Properties"]["Target"]["Arn"] == {"Fn::GetAtt": [orders_lambda_id, "Arn"]}
    assert schedule["Properties"]["Target"]["Input"] == '{"source":"scheduler","action":"heartbeat"}'

    pipe_role_id, _ = _only_resource(
        resources,
        "AWS::IAM::Role",
        lambda entry: "pipes.amazonaws.com" in str(entry[1]["Properties"]["AssumeRolePolicyDocument"]),
    )
    scheduler_role_id, _ = _only_resource(
        resources,
        "AWS::IAM::Role",
        lambda entry: "scheduler.amazonaws.com" in str(entry[1]["Properties"]["AssumeRolePolicyDocument"]),
    )
    pipe_statements = _policy_statements(resources, pipe_role_id)
    scheduler_statements = _policy_statements(resources, scheduler_role_id)

    assert next(
        statement for statement in pipe_statements if "sqs:ReceiveMessage" in _flatten_actions(statement["Action"])
    )["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert next(
        statement for statement in pipe_statements if "states:StartExecution" in _flatten_actions(statement["Action"])
    )["Resource"] == {"Ref": state_machine_id}
    assert next(
        statement for statement in pipe_statements if "lambda:InvokeFunction" in _flatten_actions(statement["Action"])
    )["Resource"] == {"Fn::GetAtt": [enrichment_lambda_id, "Arn"]}

    assert len(scheduler_statements) == 1
    assert _flatten_actions(scheduler_statements[0]["Action"]) == ["lambda:InvokeFunction"]
    assert scheduler_statements[0]["Resource"] == {"Fn::GetAtt": [orders_lambda_id, "Arn"]}


def test_rds_redshift_glue_and_audit_storage(resources: dict[str, Any], template: dict[str, Any]) -> None:
    assert len(_entries_of_type(resources, "AWS::Glue::Database")) == 1

    _, (db_security_group_id, _) = _compute_and_database_security_groups(resources)
    private_subnet_ids = {
        logical_id
        for logical_id, resource in _entries_of_type(resources, "AWS::EC2::Subnet")
        if not resource["Properties"]["MapPublicIpOnLaunch"]
    }
    _, db_instance = _only_resource(resources, "AWS::RDS::DBInstance")
    _, redshift_subnet_group = _only_resource(resources, "AWS::Redshift::ClusterSubnetGroup")
    _, redshift_cluster = _only_resource(resources, "AWS::Redshift::Cluster")
    redshift_secret_id, _ = _only_resource(
        resources,
        "AWS::SecretsManager::Secret",
        lambda entry: "clusteradmin" in str(entry[1]["Properties"].get("GenerateSecretString", {})),
    )
    _, glue_connection = _only_resource(resources, "AWS::Glue::Connection")
    _, crawler = _only_resource(resources, "AWS::Glue::Crawler")
    bucket_id, bucket = _only_resource(resources, "AWS::S3::Bucket")
    _, bucket_policy = _only_resource(resources, "AWS::S3::BucketPolicy")
    _, processor_lambda = _processor_lambda(resources)
    processor_role_id = processor_lambda["Properties"]["Role"]["Fn::GetAtt"][0]
    processor_statements = _policy_statements(resources, processor_role_id)
    processor_code = processor_lambda["Properties"]["Code"]["ZipFile"]

    assert db_instance["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["Properties"]["Engine"] == "postgres"
    assert str(db_instance["Properties"]["EngineVersion"]).startswith("15")
    assert db_instance["Properties"]["AllocatedStorage"] == "20"
    assert db_instance["Properties"]["StorageEncrypted"] is True
    assert db_instance["Properties"]["PubliclyAccessible"] is False
    assert db_instance["Properties"]["DeletionProtection"] is False
    assert db_instance["Properties"]["VPCSecurityGroups"] == [{"Fn::GetAtt": [db_security_group_id, "GroupId"]}]
    assert "{{resolve:secretsmanager:" in _flatten_cfn(db_instance["Properties"]["MasterUserPassword"])

    assert redshift_subnet_group["Properties"]["SubnetIds"] == [{"Ref": subnet_id} for subnet_id in sorted(private_subnet_ids)]
    assert redshift_cluster["Properties"]["ClusterType"] == "single-node"
    assert redshift_cluster["Properties"]["NodeType"] == "dc2.large"
    assert redshift_cluster["Properties"]["NumberOfNodes"] == 1
    assert redshift_cluster["Properties"]["Encrypted"] is True
    assert redshift_cluster["Properties"]["PubliclyAccessible"] is False
    assert redshift_cluster["Properties"]["ClusterSubnetGroupName"] == {"Ref": _only_resource(resources, "AWS::Redshift::ClusterSubnetGroup")[0]}
    assert "{{resolve:secretsmanager:" in _flatten_cfn(redshift_cluster["Properties"]["MasterUserPassword"])

    assert glue_connection["Properties"]["ConnectionInput"]["ConnectionType"] == "JDBC"
    assert "jdbc:redshift://" in _flatten_cfn(
        glue_connection["Properties"]["ConnectionInput"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
    )
    assert glue_connection["Properties"]["ConnectionInput"]["AuthenticationConfiguration"]["SecretArn"] == {
        "Ref": redshift_secret_id
    }
    assert set(crawler["Properties"]["Targets"]) == {"JdbcTargets"}
    assert len(crawler["Properties"]["Targets"]["JdbcTargets"]) == 1
    assert crawler["Properties"]["Targets"]["JdbcTargets"][0]["ConnectionName"] == (
        glue_connection["Properties"]["ConnectionInput"]["Name"]
    )
    assert "redshift" in _flatten_cfn(
        glue_connection["Properties"]["ConnectionInput"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
    )

    assert bucket["Properties"]["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"] == {
        "SSEAlgorithm": "AES256"
    }
    assert bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert "aws:SecureTransport" in str(bucket_policy["Properties"]["PolicyDocument"])
    assert "new PutObjectCommand" in processor_code
    assert "Body: JSON.stringify(record)" in processor_code
    assert 'Key: "audit/" + auditId + ".json"' in processor_code
    put_object_statement = next(
        statement
        for statement in processor_statements
        if "s3:PutObject" in _flatten_actions(statement["Action"])
    )
    assert put_object_statement["Resource"] == {"Fn::Join": ["", [{"Fn::GetAtt": [bucket_id, "Arn"]}, "/*"]]}
    assert "s3:ListAllMyBuckets" not in str(template)


def test_glue_role_permissions_are_minimal(resources: dict[str, Any]) -> None:
    redshift_secret_id, _ = _only_resource(
        resources,
        "AWS::SecretsManager::Secret",
        lambda entry: "clusteradmin" in str(entry[1]["Properties"].get("GenerateSecretString", {})),
    )
    _, glue_connection = _only_resource(resources, "AWS::Glue::Connection")
    _, glue_database = _only_resource(resources, "AWS::Glue::Database")
    glue_role_id, _ = _only_resource(
        resources,
        "AWS::IAM::Role",
        lambda entry: "glue.amazonaws.com" in str(entry[1]["Properties"]["AssumeRolePolicyDocument"]),
    )
    statements = _policy_statements(resources, glue_role_id)

    assert len(statements) == 5

    secret_statement = next(
        statement
        for statement in statements
        if "secretsmanager:GetSecretValue" in _flatten_actions(statement["Action"])
    )
    connection_statement = next(
        statement
        for statement in statements
        if "glue:GetConnection" in _flatten_actions(statement["Action"])
    )
    catalog_statement = next(
        statement
        for statement in statements
        if "glue:GetDatabase" in _flatten_actions(statement["Action"])
    )
    logs_statement = next(
        statement
        for statement in statements
        if "logs:PutLogEvents" in _flatten_actions(statement["Action"])
    )
    ec2_statement = next(
        statement
        for statement in statements
        if "ec2:CreateNetworkInterface" in _flatten_actions(statement["Action"])
    )

    assert secret_statement["Resource"] == {"Ref": redshift_secret_id}
    assert connection_statement["Resource"] == _flatten_resources(connection_statement["Resource"])[0]
    assert "connection/" in _flatten_cfn(connection_statement["Resource"])
    catalog_resources = [_flatten_cfn(resource) for resource in _flatten_resources(catalog_statement["Resource"])]
    assert any(resource.endswith(":catalog") for resource in catalog_resources)
    assert any(":database/" in resource for resource in catalog_resources)
    assert any(":table/" in resource and resource.endswith("/*") for resource in catalog_resources)
    assert glue_database["Properties"]["DatabaseInput"]["Name"] in "".join(catalog_resources)
    assert all(resource.startswith("arn:") for resource in [_flatten_cfn(resource) for resource in _flatten_resources(logs_statement["Resource"])])
    assert ec2_statement["Resource"] == "*"


def test_cross_resource_wiring_uses_references_and_stack_outputs(resources: dict[str, Any], template: dict[str, Any]) -> None:
    app_source = APP_TS.read_text()
    queue_id, _ = _only_resource(resources, "AWS::SQS::Queue")
    state_machine_id, _ = _only_resource(resources, "AWS::StepFunctions::StateMachine")
    orders_lambda_id, orders_lambda = _orders_lambda(resources)
    enrichment_lambda_id, _ = _enrichment_lambda(resources)
    _, processor_lambda = _processor_lambda(resources)
    _, glue_connection = _only_resource(resources, "AWS::Glue::Connection")
    _, crawler = _only_resource(resources, "AWS::Glue::Crawler")
    outputs = template["Outputs"]

    assert "OrdersApiUrl" in outputs
    assert outputs["OrdersApiUrl"]["Value"]
    assert "arn:aws:" not in app_source
    assert not re.search(r"\b\d{12}\b", app_source)

    methods = [resource["Properties"] for _, resource in _entries_of_type(resources, "AWS::ApiGateway::Method")]
    assert all(isinstance(method["Integration"]["Uri"], dict) for method in methods)
    assert all(
        _env_vars(orders_lambda)[name]
        for name in ["ORDER_QUEUE_URL", "DB_SECRET_ARN", "DB_HOST", "DB_PORT", "DB_NAME"]
    )
    assert _env_vars(processor_lambda)["AUDIT_BUCKET_NAME"]

    _, pipe = _only_resource(resources, "AWS::Pipes::Pipe")
    assert pipe["Properties"]["Source"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert pipe["Properties"]["Target"] == {"Ref": state_machine_id}
    assert pipe["Properties"]["Enrichment"] == {"Fn::GetAtt": [enrichment_lambda_id, "Arn"]}
    assert glue_connection["Properties"]["ConnectionInput"]["AuthenticationConfiguration"]["SecretArn"] == {
        "Ref": _only_resource(
            resources,
            "AWS::SecretsManager::Secret",
            lambda entry: "clusteradmin" in str(entry[1]["Properties"].get("GenerateSecretString", {})),
        )[0]
    }
    assert crawler["Properties"]["Targets"]["JdbcTargets"][0]["ConnectionName"] == (
        glue_connection["Properties"]["ConnectionInput"]["Name"]
    )


def test_least_privilege_and_cleanup_settings(resources: dict[str, Any], template: dict[str, Any]) -> None:
    assert len(_entries_of_type(resources, "AWS::KMS::Key")) == 0
    assert len(_entries_of_type(resources, "AWS::KMS::Alias")) == 0

    for _, policy in _entries_of_type(resources, "AWS::IAM::Policy"):
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            for action in _flatten_actions(statement["Action"]):
                assert action != "*"
                assert not action.endswith(":*")
            if any(resource == "*" for resource in _flatten_resources(statement["Resource"])):
                assert all(action.startswith("ec2:") for action in _flatten_actions(statement["Action"]))

    for logical_id, resource in resources.items():
        assert resource.get("DeletionPolicy") != "Retain", logical_id
        assert resource.get("UpdateReplacePolicy") != "Retain", logical_id
        assert resource.get("UpdateReplacePolicy") != "Snapshot", logical_id
        assert resource.get("Properties", {}).get("DeletionProtection") is not True
        assert resource.get("Properties", {}).get("EnableTerminationProtection") is not True

    template_text = json.dumps(template, sort_keys=True)
    assert "AWS::KMS::Key" not in template_text
    assert "AWS::KMS::Alias" not in template_text
    assert "Action: '*'" not in template_text
    assert "Retain" not in template_text
