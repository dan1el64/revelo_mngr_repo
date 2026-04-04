import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_NAME = "ThreeTierPocStack"
TEMPLATE_PATH = REPO_ROOT / "cdk.out" / f"{STACK_NAME}.template.json"
MANIFEST_PATH = REPO_ROOT / "cdk.out" / "manifest.json"


def _run_app_synth() -> None:
    shutil.rmtree(REPO_ROOT / "cdk.out", ignore_errors=True)
    env = os.environ.copy()
    env.setdefault("AWS_REGION", "us-east-1")
    subprocess.run(
        ["npx", "ts-node", "app.ts"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def _resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def _only_resource(template: dict, resource_type: str) -> tuple[str, dict]:
    resources = _resources_by_type(template, resource_type)
    assert len(resources) == 1, f"Expected exactly one {resource_type}, found {len(resources)}"
    return next(iter(resources.items()))


def _policy_document(template: dict, role_logical_id: str) -> dict:
    for resource in template["Resources"].values():
        if resource["Type"] != "AWS::IAM::Policy":
            continue
        role_refs = resource["Properties"].get("Roles", [])
        if {"Ref": role_logical_id} in role_refs:
            return resource["Properties"]["PolicyDocument"]
    raise AssertionError(f"No inline policy found for role {role_logical_id}")


def _statements_for_role(template: dict, role_logical_id: str) -> list[dict]:
    return _policy_document(template, role_logical_id)["Statement"]


def _route_keys(template: dict) -> set[str]:
    return {
        resource["Properties"]["RouteKey"]
        for resource in _resources_by_type(template, "AWS::ApiGatewayV2::Route").values()
    }


def _log_groups(template: dict) -> dict:
    return _resources_by_type(template, "AWS::Logs::LogGroup")


def _logical_id_with_prefix(template: dict, resource_type: str, prefix: str) -> str:
    matches = [
        logical_id
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type and logical_id.startswith(prefix)
    ]
    assert len(matches) == 1, f"Expected exactly one {resource_type} starting with {prefix}, found {matches}"
    return matches[0]


@pytest.fixture(scope="session")
def synthesized() -> dict:
    _run_app_synth()
    template = json.loads(TEMPLATE_PATH.read_text())
    manifest = json.loads(MANIFEST_PATH.read_text())
    source = (REPO_ROOT / "app.ts").read_text()
    return {
        "template": template,
        "manifest": manifest,
        "source": source,
    }


def test_prompt_surface_is_present_in_source(synthesized: dict) -> None:
    source = synthesized["source"]

    assert "process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'us-east-1'" in source
    assert "tryGetContext" not in source
    assert "valueFromLookup" not in source
    assert "CREATE TABLE IF NOT EXISTS items" in source
    assert "INSERT INTO items (id, created_at, s3_key, status, payload_sha256)" in source
    assert "id TEXT PRIMARY KEY" in source
    assert "created_at TIMESTAMP NOT NULL" in source
    assert "s3_key TEXT NOT NULL" in source
    assert "status TEXT NOT NULL" in source
    assert "payload_sha256 TEXT NOT NULL" in source
    assert "rawPath === '/api/health'" in source
    assert "rawPath === '/api/items'" in source
    assert "path: '/api/{proxy+}'" in source
    assert "methods: [apigwv2.HttpMethod.ANY]" in source
    assert "BUCKET_OWNER_ENFORCED" in source
    assert "FIRE_AND_FORGET" in source
    assert "Database endpoint is not available" in source
    assert "rapid-prototype-poc" in source
    assert "target.textContent=JSON.stringify(data,null,2)" in source
    assert "new SendMessageCommand" in source
    assert "new HeadObjectCommand" in source
    assert "sqlLiteral('created')" in source
    assert "sqlLiteral('processing')" in source
    assert "sqlLiteral('validated')" in source
    assert "unsafeUnwrap" not in source


def test_stack_level_destructibility_and_forbidden_resource_types(synthesized: dict) -> None:
    template = synthesized["template"]
    manifest = synthesized["manifest"]
    resources = template["Resources"]

    assert manifest["artifacts"][STACK_NAME]["properties"]["terminationProtection"] is False

    for resource in resources.values():
        deletion_policy = resource.get("DeletionPolicy")
        update_replace_policy = resource.get("UpdateReplacePolicy")
        assert deletion_policy in (None, "Delete")
        assert update_replace_policy in (None, "Delete")

    assert '"DeletionPolicy": "Retain"' not in json.dumps(template)
    assert '"UpdateReplacePolicy": "Retain"' not in json.dumps(template)
    assert '"DeletionPolicy": "Snapshot"' not in json.dumps(template)
    assert '"UpdateReplacePolicy": "Snapshot"' not in json.dumps(template)
    assert "AWS::ElasticLoadBalancing::LoadBalancer" not in json.dumps(template)
    assert "AWS::ElasticLoadBalancingV2::LoadBalancer" not in json.dumps(template)


def test_vpc_topology_is_exact(synthesized: dict) -> None:
    template = synthesized["template"]
    _, vpc = _only_resource(template, "AWS::EC2::VPC")
    subnets = list(_resources_by_type(template, "AWS::EC2::Subnet").values())
    nat_gateways = _resources_by_type(template, "AWS::EC2::NatGateway")
    az_indexes = {
        subnet["Properties"]["AvailabilityZone"]["Fn::Select"][0]
        for subnet in subnets
        if isinstance(subnet["Properties"].get("AvailabilityZone"), dict)
        and "Fn::Select" in subnet["Properties"]["AvailabilityZone"]
    }

    assert vpc["Properties"]["EnableDnsHostnames"] is True
    assert vpc["Properties"]["EnableDnsSupport"] is True
    assert len(subnets) == 4
    assert sum(1 for subnet in subnets if subnet["Properties"]["MapPublicIpOnLaunch"]) == 2
    assert sum(1 for subnet in subnets if not subnet["Properties"]["MapPublicIpOnLaunch"]) == 2
    assert az_indexes == {0, 1}
    assert len(nat_gateways) == 1


def test_network_security_and_vpc_endpoint_contract(synthesized: dict) -> None:
    template = synthesized["template"]
    security_groups = _resources_by_type(template, "AWS::EC2::SecurityGroup")
    ingress_rules = _resources_by_type(template, "AWS::EC2::SecurityGroupIngress")
    egress_rules = _resources_by_type(template, "AWS::EC2::SecurityGroupEgress")
    _, vpc_endpoint = _only_resource(template, "AWS::EC2::VPCEndpoint")

    assert len(security_groups) == 2
    backend_sg_id = next(logical_id for logical_id in security_groups if logical_id.startswith("BackendSecurityGroup"))
    database_sg_id = next(logical_id for logical_id in security_groups if logical_id.startswith("DatabaseSecurityGroup"))
    backend_sg = security_groups[backend_sg_id]["Properties"]

    assert backend_sg.get("SecurityGroupIngress", []) == []
    assert any(
        rule["FromPort"] == 443 and rule["ToPort"] == 443 and rule["IpProtocol"] == "tcp"
        for rule in backend_sg["SecurityGroupEgress"]
    )
    assert not any(rule.get("IpProtocol") == "-1" for rule in backend_sg["SecurityGroupEgress"])
    assert not any(
        rule.get("CidrIp") == "0.0.0.0/0" and (rule.get("FromPort"), rule.get("ToPort")) != (443, 443)
        for rule in backend_sg["SecurityGroupEgress"]
    )

    database_ingress = [
        resource["Properties"]
        for resource in ingress_rules.values()
        if resource["Properties"].get("GroupId", {}).get("Fn::GetAtt", [""])[0] == database_sg_id
    ]
    assert len(database_ingress) == 1
    assert database_ingress[0]["FromPort"] == 5432
    assert database_ingress[0]["ToPort"] == 5432
    assert database_ingress[0]["SourceSecurityGroupId"]["Fn::GetAtt"][0] == backend_sg_id

    backend_to_database = [
        resource["Properties"]
        for resource in egress_rules.values()
        if resource["Properties"].get("GroupId", {}).get("Fn::GetAtt", [""])[0] == backend_sg_id
    ]
    assert len(backend_to_database) == 1
    assert backend_to_database[0]["FromPort"] == 5432
    assert backend_to_database[0]["ToPort"] == 5432
    assert backend_to_database[0]["DestinationSecurityGroupId"]["Fn::GetAtt"][0] == database_sg_id

    assert vpc_endpoint["Properties"]["VpcEndpointType"] == "Interface"
    assert vpc_endpoint["Properties"]["PrivateDnsEnabled"] is True
    assert vpc_endpoint["Properties"]["ServiceName"] == "com.amazonaws.us-east-1.secretsmanager"
    assert vpc_endpoint["Properties"]["SubnetIds"]


def test_http_api_routes_and_lambda_wiring_are_exact(synthesized: dict) -> None:
    template = synthesized["template"]
    apis = _resources_by_type(template, "AWS::ApiGatewayV2::Api")
    integrations = _resources_by_type(template, "AWS::ApiGatewayV2::Integration")
    routes = _resources_by_type(template, "AWS::ApiGatewayV2::Route")
    frontend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "FrontendLambda")
    backend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "BackendLambda")

    assert len(apis) == 1
    assert len(integrations) == 2
    assert len(routes) == 2
    assert _route_keys(template) == {"GET /", "ANY /api/{proxy+}"}

    integration_targets = {
        logical_id: resource["Properties"]["IntegrationUri"]["Fn::GetAtt"][0]
        for logical_id, resource in integrations.items()
    }
    assert set(integration_targets.values()) == {frontend_lambda_id, backend_lambda_id}

    route_targets = {resource["Properties"]["RouteKey"]: resource["Properties"]["Target"] for resource in routes.values()}
    assert route_targets["GET /"]["Fn::Join"][1][-1]["Ref"] in integration_targets
    assert route_targets["ANY /api/{proxy+}"]["Fn::Join"][1][-1]["Ref"] in integration_targets


def test_s3_bucket_contract_matches_prompt(synthesized: dict) -> None:
    template = synthesized["template"]
    _, bucket = _only_resource(template, "AWS::S3::Bucket")
    properties = bucket["Properties"]
    lifecycle_rules = properties["LifecycleConfiguration"]["Rules"]

    assert properties["VersioningConfiguration"]["Status"] == "Enabled"
    assert properties["OwnershipControls"]["Rules"] == [{"ObjectOwnership": "BucketOwnerEnforced"}]
    assert properties["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"][
        "SSEAlgorithm"
    ] == "AES256"
    assert lifecycle_rules
    assert any(rule.get("Prefix") == "items/" and rule.get("ExpirationInDays") == 90 for rule in lifecycle_rules)
    assert any(
        rule.get("NoncurrentVersionExpiration", {}).get("NoncurrentDays") == 30
        for rule in lifecycle_rules
    )


def test_database_secret_subnet_group_and_instance_are_correct(synthesized: dict) -> None:
    template = synthesized["template"]
    secrets = _resources_by_type(template, "AWS::SecretsManager::Secret")
    subnet_groups = _resources_by_type(template, "AWS::RDS::DBSubnetGroup")
    subnets = _resources_by_type(template, "AWS::EC2::Subnet")
    _, db_instance = _only_resource(template, "AWS::RDS::DBInstance")
    _, subnet_group = _only_resource(template, "AWS::RDS::DBSubnetGroup")
    private_subnet_ids = {
        logical_id for logical_id, subnet in subnets.items() if subnet["Properties"]["MapPublicIpOnLaunch"] is False
    }
    subnet_group_refs = {
        subnet_ref["Ref"] for subnet_ref in subnet_group["Properties"]["SubnetIds"] if isinstance(subnet_ref, dict) and "Ref" in subnet_ref
    }

    assert len(secrets) == 1
    assert len(subnet_groups) == 1
    assert subnet_group_refs == private_subnet_ids

    db_props = db_instance["Properties"]
    assert db_props["Engine"] == "postgres"
    assert db_props["EngineVersion"].startswith("16")
    assert db_props["DBInstanceClass"] == "db.t3.micro"
    assert db_props["AllocatedStorage"] == "20"
    assert db_props["StorageType"] in {"gp2", "gp3"}
    assert db_props["MultiAZ"] is False
    assert db_props["PubliclyAccessible"] is False
    assert db_props["BackupRetentionPeriod"] == 7
    assert db_props["StorageEncrypted"] is True
    assert db_props["DeletionProtection"] is False
    assert db_props["Port"] == "5432"
    assert len(db_props["VPCSecurityGroups"]) == 1
    assert db_props["DBSubnetGroupName"]["Ref"] == next(iter(subnet_groups))


def test_lambda_functions_logs_and_custom_resource_shape_are_correct(synthesized: dict) -> None:
    template = synthesized["template"]
    functions = _resources_by_type(template, "AWS::Lambda::Function")
    log_groups = _log_groups(template)
    frontend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "FrontendLambda")
    backend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "BackendLambda")
    schema_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "SchemaLambda")
    custom_resources = {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"].startswith("Custom::") or resource["Type"] == "AWS::CloudFormation::CustomResource"
    }

    assert len(functions) == 3
    assert len(log_groups) == 3
    assert len(custom_resources) == 1

    frontend = functions[frontend_lambda_id]["Properties"]
    backend = functions[backend_lambda_id]["Properties"]
    schema = functions[schema_lambda_id]["Properties"]

    assert frontend["Runtime"] == "nodejs20.x"
    assert frontend["MemorySize"] == 256
    assert frontend["Timeout"] == 5
    assert "VpcConfig" not in frontend
    assert frontend.get("PackageType", "Zip") == "Zip"

    assert backend["Runtime"] == "nodejs20.x"
    assert backend["MemorySize"] == 512
    assert backend["Timeout"] == 15
    assert backend["VpcConfig"]["SecurityGroupIds"][0]["Fn::GetAtt"][0] == _logical_id_with_prefix(
        template, "AWS::EC2::SecurityGroup", "BackendSecurityGroup"
    )
    assert len(backend["VpcConfig"]["SubnetIds"]) == 2
    assert backend.get("PackageType", "Zip") == "Zip"

    assert schema["Runtime"] == "nodejs20.x"
    assert schema["MemorySize"] == 512
    assert schema["Timeout"] == 60
    assert schema["VpcConfig"]["SecurityGroupIds"][0]["Fn::GetAtt"][0] == _logical_id_with_prefix(
        template, "AWS::EC2::SecurityGroup", "BackendSecurityGroup"
    )
    assert len(schema["VpcConfig"]["SubnetIds"]) == 2
    assert schema.get("PackageType", "Zip") == "Zip"

    for log_group in log_groups.values():
        assert log_group["Properties"]["RetentionInDays"] == 14
        assert "KmsKeyId" not in log_group["Properties"]

    custom_resource = next(iter(custom_resources.values()))
    assert custom_resource["Properties"]["ServiceToken"]["Fn::GetAtt"][0] == schema_lambda_id


def test_async_components_are_present_and_wired_correctly(synthesized: dict) -> None:
    template = synthesized["template"]
    _, queue = _only_resource(template, "AWS::SQS::Queue")
    _, event_source_mapping = _only_resource(template, "AWS::Lambda::EventSourceMapping")
    _, state_machine = _only_resource(template, "AWS::StepFunctions::StateMachine")
    _, pipe = _only_resource(template, "AWS::Pipes::Pipe")
    queue_id = _logical_id_with_prefix(template, "AWS::SQS::Queue", "IngestionQueue")
    backend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "BackendLambda")
    state_machine_id = _logical_id_with_prefix(template, "AWS::StepFunctions::StateMachine", "ValidationStateMachine")

    assert event_source_mapping["Properties"]["BatchSize"] == 5
    assert event_source_mapping["Properties"]["FunctionName"]["Ref"] == backend_lambda_id
    assert event_source_mapping["Properties"]["EventSourceArn"]["Fn::GetAtt"][0] == queue_id

    definition = "".join(
        part if isinstance(part, str) else json.dumps(part)
        for part in state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
    )
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert definition.count('"Type":"Task"') == 1
    assert '"StartAt":"ValidateArchivedObject"' in definition
    assert '"End":true' in definition
    assert '"mode":"validate"' in definition
    assert '"s3Key.$":"$.s3Key"' in definition
    assert backend_lambda_id in json.dumps(state_machine)

    pipe_props = pipe["Properties"]
    assert pipe_props["Source"]["Fn::GetAtt"][0] == queue_id
    assert pipe_props["Enrichment"]["Fn::GetAtt"][0] == backend_lambda_id
    assert pipe_props["Target"]["Ref"] == state_machine_id
    assert pipe_props["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe_props["SourceParameters"]["SqsQueueParameters"]["MaximumBatchingWindowInSeconds"] == 5
    assert pipe_props["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"


def test_iam_scoping_matches_the_prompt_and_runtime_requirements(synthesized: dict) -> None:
    template = synthesized["template"]
    frontend_role_id = _logical_id_with_prefix(template, "AWS::IAM::Role", "FrontendLambdaRole")
    backend_role_id = _logical_id_with_prefix(template, "AWS::IAM::Role", "BackendLambdaRole")
    schema_role_id = _logical_id_with_prefix(template, "AWS::IAM::Role", "SchemaLambdaRole")
    state_machine_role_id = _logical_id_with_prefix(template, "AWS::IAM::Role", "StateMachineRole")
    pipe_role_id = _logical_id_with_prefix(template, "AWS::IAM::Role", "PipeRole")
    frontend_statements = _statements_for_role(template, frontend_role_id)
    backend_statements = _statements_for_role(template, backend_role_id)
    schema_statements = _statements_for_role(template, schema_role_id)
    state_machine_statements = _statements_for_role(template, state_machine_role_id)
    pipe_statements = _statements_for_role(template, pipe_role_id)
    backend_lambda_id = _logical_id_with_prefix(template, "AWS::Lambda::Function", "BackendLambda")
    database_secret_id = _logical_id_with_prefix(template, "AWS::SecretsManager::Secret", "DatabaseSecret")
    queue_id = _logical_id_with_prefix(template, "AWS::SQS::Queue", "IngestionQueue")

    assert frontend_statements
    assert any(statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"] for statement in frontend_statements)
    assert all(statement["Resource"] != "*" for statement in frontend_statements)
    assert all("FrontendLogGroup" in json.dumps(statement["Resource"]) for statement in frontend_statements)
    assert not any(
        statement["Action"] != ["logs:CreateLogStream", "logs:PutLogEvents"] for statement in frontend_statements
    )

    backend_actions = {tuple(statement["Action"]) if isinstance(statement["Action"], list) else (statement["Action"],) for statement in backend_statements}
    assert ("logs:CreateLogStream", "logs:PutLogEvents") in backend_actions
    assert ("secretsmanager:GetSecretValue",) in backend_actions
    assert any(
        "s3:PutObject" in statement["Action"] and "items/*" in json.dumps(statement["Resource"])
        for statement in backend_statements
        if isinstance(statement["Action"], list)
    )
    assert any(
        "sqs:SendMessage" in statement["Action"] and queue_id in json.dumps(statement["Resource"])
        for statement in backend_statements
        if isinstance(statement["Action"], list)
    )
    assert any(
        statement["Action"] in ("secretsmanager:GetSecretValue", ["secretsmanager:GetSecretValue"])
        and database_secret_id in json.dumps(statement["Resource"])
        for statement in backend_statements
    )
    assert any(
        statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"] and statement["Resource"] != "*"
        for statement in backend_statements
    )
    assert not any("rds-db:connect" in json.dumps(statement) for statement in backend_statements)

    schema_actions = {tuple(statement["Action"]) if isinstance(statement["Action"], list) else (statement["Action"],) for statement in schema_statements}
    assert ("logs:CreateLogStream", "logs:PutLogEvents") in schema_actions
    assert ("secretsmanager:GetSecretValue",) in schema_actions
    assert any(
        statement["Action"] in ("secretsmanager:GetSecretValue", ["secretsmanager:GetSecretValue"])
        and database_secret_id in json.dumps(statement["Resource"])
        for statement in schema_statements
    )
    assert any(
        statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"] and statement["Resource"] != "*"
        for statement in schema_statements
    )
    assert not any("s3:" in json.dumps(statement) for statement in schema_statements)
    assert not any("sqs:" in json.dumps(statement) for statement in schema_statements)
    assert not any("states:" in json.dumps(statement) for statement in schema_statements)
    assert not any("lambda:InvokeFunction" in json.dumps(statement) for statement in schema_statements)
    assert not any("rds-db:connect" in json.dumps(statement) for statement in schema_statements)

    assert all(statement["Action"] == "lambda:InvokeFunction" for statement in state_machine_statements)
    assert all(backend_lambda_id in json.dumps(statement["Resource"]) for statement in state_machine_statements)

    pipe_actions = {tuple(statement["Action"]) if isinstance(statement["Action"], list) else (statement["Action"],) for statement in pipe_statements}
    assert ("lambda:InvokeFunction",) in pipe_actions
    assert ("states:StartExecution",) in pipe_actions
    assert ("sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ReceiveMessage") in pipe_actions
    assert not any(
        statement.get("Resource") == "*"
        and (
            statement.get("Action") == ["logs:CreateLogStream", "logs:PutLogEvents"]
            or statement.get("Action") == ["secretsmanager:GetSecretValue"]
        )
        for statement in backend_statements + schema_statements + frontend_statements
    )


def test_no_additional_data_stores_are_defined(synthesized: dict) -> None:
    template_json = json.dumps(synthesized["template"])

    forbidden_resource_types = [
        "AWS::DynamoDB::Table",
        "AWS::ElastiCache::CacheCluster",
        "AWS::ElastiCache::ReplicationGroup",
        "AWS::MemoryDB::Cluster",
        "AWS::OpenSearchService::Domain",
        "AWS::Neptune::DBCluster",
        "AWS::DocDB::DBCluster",
        "AWS::Redshift::Cluster",
    ]

    for resource_type in forbidden_resource_types:
        assert resource_type not in template_json


def test_resource_references_are_dynamic_and_no_physical_names_are_forced(synthesized: dict) -> None:
    template = synthesized["template"]

    forbidden_name_properties = {
        "AWS::S3::Bucket": ["BucketName"],
        "AWS::SQS::Queue": ["QueueName"],
        "AWS::SecretsManager::Secret": ["Name"],
        "AWS::Lambda::Function": ["FunctionName"],
        "AWS::IAM::Role": ["RoleName"],
        "AWS::StepFunctions::StateMachine": ["StateMachineName"],
        "AWS::RDS::DBInstance": ["DBInstanceIdentifier"],
    }

    for resource_type, property_names in forbidden_name_properties.items():
        for resource in _resources_by_type(template, resource_type).values():
            for property_name in property_names:
                assert property_name not in resource["Properties"]
