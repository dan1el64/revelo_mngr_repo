import json
import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATHS = [
    REPO_ROOT / "template.json",
    *sorted((REPO_ROOT / "cdk.out").glob("*.template.json")),
]


def _template():
    for path in TEMPLATE_PATHS:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    pytest.fail("No synthesized CloudFormation template found")


def _resources(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template.get("Resources", {}).items()
        if resource.get("Type") == resource_type
    }


def _single(resources, predicate=lambda *_: True, message="Expected exactly one match"):
    matches = [(logical_id, resource) for logical_id, resource in resources.items() if predicate(logical_id, resource)]
    assert len(matches) == 1, message
    return matches[0]


def _zipfile_text(function_properties):
    zip_file = function_properties.get("Code", {}).get("ZipFile", "")
    if isinstance(zip_file, str):
        return zip_file
    if isinstance(zip_file, dict) and "Fn::Join" in zip_file:
        return "".join(part for part in zip_file["Fn::Join"][1] if isinstance(part, str))
    return ""


def _statements_for_role(template, role_logical_id):
    policies = []
    for resource in template.get("Resources", {}).values():
        if resource.get("Type") != "AWS::IAM::Policy":
            continue
        properties = resource.get("Properties", {})
        attached_roles = properties.get("Roles", [])
        refs = {entry.get("Ref") for entry in attached_roles if isinstance(entry, dict) and "Ref" in entry}
        if role_logical_id in refs:
            statements = properties.get("PolicyDocument", {}).get("Statement", [])
            if isinstance(statements, dict):
                statements = [statements]
            policies.extend(statements)
    return policies


def _find_statement(statements, action):
    for statement in statements:
        actions = statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if action in actions:
            return statement
    raise AssertionError(f"Could not find policy statement for action {action}")


def _actions(statement):
    actions = statement.get("Action", [])
    if isinstance(actions, str):
        return [actions]
    return actions


def _resources_list(statement):
    resources = statement.get("Resource", [])
    if isinstance(resources, list):
        return resources
    return [resources]


def _all_statements(template):
    statements = []
    for resource in template.get("Resources", {}).values():
        if resource.get("Type") != "AWS::IAM::Policy":
            continue
        current = resource.get("Properties", {}).get("PolicyDocument", {}).get("Statement", [])
        if isinstance(current, dict):
            current = [current]
        statements.extend(current)
    return statements


def test_expected_resource_counts():
    template = _template()
    assert len(_resources(template, "AWS::EC2::VPC")) == 1
    assert len(_resources(template, "AWS::EC2::Subnet")) == 4
    assert len(_resources(template, "AWS::EC2::NatGateway")) == 1
    assert len(_resources(template, "AWS::EC2::InternetGateway")) == 1
    assert len(_resources(template, "AWS::EC2::SecurityGroup")) == 2
    assert len(_resources(template, "AWS::RDS::DBSubnetGroup")) == 1
    assert len(_resources(template, "AWS::RDS::DBInstance")) == 1
    assert len(_resources(template, "AWS::SecretsManager::Secret")) == 1
    assert len(_resources(template, "AWS::S3::Bucket")) == 1
    assert len(_resources(template, "AWS::Logs::LogGroup")) == 2
    assert len(_resources(template, "AWS::Lambda::Function")) == 2
    assert len(_resources(template, "AWS::IAM::Role")) == 2
    assert len(_resources(template, "AWS::ApiGateway::RestApi")) == 1
    assert len(_resources(template, "AWS::ApiGateway::Resource")) == 2
    assert len(_resources(template, "AWS::ApiGateway::Method")) == 2
    assert len(_resources(template, "AWS::Scheduler::Schedule")) == 1


def test_vpc_subnets_and_routes_match_contract():
    template = _template()
    vpc_id, vpc = _single(_resources(template, "AWS::EC2::VPC"))
    assert vpc["Properties"]["CidrBlock"] == "10.0.0.0/16"

    subnets = _resources(template, "AWS::EC2::Subnet")
    public_subnets = [resource for resource in subnets.values() if resource["Properties"].get("MapPublicIpOnLaunch") is True]
    private_subnets = [resource for resource in subnets.values() if resource["Properties"].get("MapPublicIpOnLaunch") is not True]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2

    availability_zones = {
        subnet["Properties"]["AvailabilityZone"]["Fn::Select"][0]
        for subnet in subnets.values()
    }
    assert availability_zones == {0, 1}

    routes = _resources(template, "AWS::EC2::Route")
    assert any("GatewayId" in route["Properties"] for route in routes.values()), "Missing public route to the Internet Gateway"
    assert any("NatGatewayId" in route["Properties"] for route in routes.values()), "Missing private route to the NAT Gateway"

    nat_id, nat_gateway = _single(_resources(template, "AWS::EC2::NatGateway"))
    assert nat_gateway["Properties"]["SubnetId"]["Ref"] in {
        logical_id for logical_id, subnet in subnets.items() if subnet["Properties"].get("MapPublicIpOnLaunch") is True
    }
    assert nat_id
    assert vpc_id


def test_database_configuration_and_secret_generation():
    template = _template()
    subnets = _resources(template, "AWS::EC2::Subnet")
    private_subnet_ids = {
        logical_id
        for logical_id, subnet in subnets.items()
        if subnet["Properties"].get("MapPublicIpOnLaunch") is not True
    }

    _, subnet_group = _single(_resources(template, "AWS::RDS::DBSubnetGroup"))
    subnet_ids = subnet_group["Properties"]["SubnetIds"]
    assert len(subnet_ids) == 2
    assert {
        subnet_id["Ref"]
        for subnet_id in subnet_ids
        if isinstance(subnet_id, dict) and "Ref" in subnet_id
    } == private_subnet_ids

    _, db_instance = _single(_resources(template, "AWS::RDS::DBInstance"))
    properties = db_instance["Properties"]
    assert properties["Engine"] == "postgres"
    assert str(properties["EngineVersion"]).startswith("15")
    assert properties["DBInstanceClass"] == "db.t3.micro"
    assert str(properties["AllocatedStorage"]) == "20"
    assert properties["StorageType"] == "gp3"
    assert properties["MultiAZ"] is False
    assert properties["BackupRetentionPeriod"] == 7
    assert properties["DeletionProtection"] is False
    assert properties["PubliclyAccessible"] is False
    assert properties["DBSubnetGroupName"]["Ref"]

    _, secret = _single(_resources(template, "AWS::SecretsManager::Secret"))
    generator = secret["Properties"]["GenerateSecretString"]
    assert generator["ExcludePunctuation"] is True
    assert generator["SecretStringTemplate"] == '{"username":"infraanalyst"}'
    assert generator["GenerateStringKey"] == "password"

    master_username = properties["MasterUsername"]
    master_password = properties["MasterUserPassword"]
    assert master_username == "infraanalyst"
    serialized = json.dumps({"password": master_password})
    assert "resolve:secretsmanager" in serialized


def test_s3_bucket_retention_and_security_settings():
    template = _template()
    _, bucket = _single(_resources(template, "AWS::S3::Bucket"))
    properties = bucket["Properties"]
    assert properties["VersioningConfiguration"]["Status"] == "Enabled"
    assert properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    public_access = properties["PublicAccessBlockConfiguration"]
    assert public_access == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }

    rules = properties["LifecycleConfiguration"]["Rules"]
    assert len(rules) == 2
    noncurrent_rule = next(rule for rule in rules if "NoncurrentVersionTransitions" in rule)
    assert noncurrent_rule["NoncurrentVersionTransitions"][0]["StorageClass"] == "STANDARD_IA"
    assert noncurrent_rule["NoncurrentVersionTransitions"][0]["TransitionInDays"] == 30
    assert noncurrent_rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 365

    reports_rule = next(rule for rule in rules if rule.get("Prefix") == "reports/")
    assert reports_rule["ExpirationInDays"] == 30

    assert bucket["DeletionPolicy"] == "Delete"
    assert bucket["UpdateReplacePolicy"] == "Delete"


def test_log_groups_are_explicit_and_destroyable():
    template = _template()
    log_groups = _resources(template, "AWS::Logs::LogGroup")
    names = set()
    for resource in log_groups.values():
        properties = resource["Properties"]
        names.add(properties["LogGroupName"])
        assert properties["RetentionInDays"] == 14
        assert "KmsKeyId" not in properties
        assert resource["DeletionPolicy"] == "Delete"
        assert resource["UpdateReplacePolicy"] == "Delete"
    assert names == {
        "/aws/lambda/infrastructure-analyst-collector",
        "/aws/lambda/infrastructure-analyst-summary",
    }


def test_lambda_contracts_vpc_wiring_and_inline_code():
    template = _template()
    lambdas = _resources(template, "AWS::Lambda::Function")
    _, collector = _single(
        lambdas,
        lambda _, resource: resource["Properties"]["MemorySize"] == 256,
        "Expected exactly one collector Lambda",
    )
    _, summary = _single(
        lambdas,
        lambda _, resource: resource["Properties"]["MemorySize"] == 128,
        "Expected exactly one summary Lambda",
    )

    collector_props = collector["Properties"]
    summary_props = summary["Properties"]

    assert collector_props["Runtime"] == "python3.12"
    assert collector_props["Timeout"] == 60
    assert collector_props["FunctionName"] == "infrastructure-analyst-collector"

    assert summary_props["Runtime"] == "python3.12"
    assert summary_props["Timeout"] == 30
    assert summary_props["FunctionName"] == "infrastructure-analyst-summary"

    expected_env_keys = {"REPORT_BUCKET", "DB_SECRET_ARN", "DB_HOST", "DB_PORT", "DB_NAME", "AWS_ENDPOINT"}
    assert set(collector_props["Environment"]["Variables"].keys()) == expected_env_keys
    assert set(summary_props["Environment"]["Variables"].keys()) == expected_env_keys

    for props in (collector_props, summary_props):
        vpc_config = props["VpcConfig"]
        assert len(vpc_config["SubnetIds"]) == 2
        assert len(vpc_config["SecurityGroupIds"]) == 1

    collector_code = _zipfile_text(collector_props)
    summary_code = _zipfile_text(summary_props)
    for code in (collector_code, summary_code):
        assert "_load_secret" in code
        assert "PgConnection" in code
        assert "_client(\"secretsmanager\")" in code
        assert "AWS_ENDPOINT" in code
        assert "reports/" in code

    assert "_list_s3_buckets" in collector_code
    assert "_list_lambda_functions" in collector_code
    assert "_list_sqs_queues" in collector_code
    assert "_list_eventbridge_rules" in collector_code
    assert "_list_rds_instances" in collector_code
    assert "_list_redshift_clusters" in collector_code
    assert "put_object" in collector_code
    assert "INSERT INTO inventory_runs" in collector_code
    assert "subprocess.run" in collector_code
    assert "sys.executable" in collector_code
    assert "\"-c\"" in collector_code or "'-c'" in collector_code

    forbidden_mutators = [
        "create_bucket(",
        "delete_bucket(",
        "put_bucket",
        "create_function(",
        "delete_function(",
        "update_function",
        "create_queue(",
        "delete_queue(",
        "create_db_instance(",
        "delete_db_instance(",
        "modify_db_instance(",
        "create_cluster(",
        "delete_cluster(",
        "run_instances(",
    ]
    lowered_collector_code = collector_code.lower()
    assert all(mutator not in lowered_collector_code for mutator in forbidden_mutators)

    assert "_latest_report" in summary_code
    assert "SELECT run_id, created_at::text, counts_by_service::text, s3_report_key" in summary_code
    assert "counts_by_service" in summary_code
    assert "\"latest_run_id\"" in summary_code or "'latest_run_id'" in summary_code
    assert "\"latest_created_at\"" in summary_code or "'latest_created_at'" in summary_code
    assert "\"counts_by_service\"" in summary_code or "'counts_by_service'" in summary_code
    assert "\"s3_report_key\"" in summary_code or "'s3_report_key'" in summary_code

    env_json = json.dumps(collector_props["Environment"]["Variables"])
    assert "password" not in env_json.lower()
    assert "secretstring" not in env_json.lower()


def test_iam_roles_are_scoped_to_expected_actions():
    template = _template()
    roles = _resources(template, "AWS::IAM::Role")
    collector_role_id, _ = _single(
        roles,
        lambda _, resource: resource["Properties"]["Description"] == "Execution role for the collector Lambda",
        "Expected collector role",
    )
    summary_role_id, _ = _single(
        roles,
        lambda _, resource: resource["Properties"]["Description"] == "Execution role for the summary Lambda",
        "Expected summary role",
    )

    collector_statements = _statements_for_role(template, collector_role_id)
    summary_statements = _statements_for_role(template, summary_role_id)
    all_statements = _all_statements(template)

    list_bucket_stmt = _find_statement(collector_statements, "s3:ListBucket")
    assert list_bucket_stmt["Resource"]["Fn::GetAtt"][1] == "Arn"

    put_object_stmt = _find_statement(collector_statements, "s3:PutObject")
    assert put_object_stmt["Resource"]["Fn::Join"][1][-1] == "/*"
    abort_stmt = _find_statement(collector_statements, "s3:AbortMultipartUpload")
    assert abort_stmt["Resource"]["Fn::Join"][1][-1] == "/*"

    collector_secret_get = _find_statement(collector_statements, "secretsmanager:GetSecretValue")
    collector_secret_describe = _find_statement(collector_statements, "secretsmanager:DescribeSecret")
    assert collector_secret_get["Resource"]["Ref"]
    assert collector_secret_describe["Resource"]["Ref"] == collector_secret_get["Resource"]["Ref"]

    inventory_stmt = _find_statement(collector_statements, "lambda:ListFunctions")
    assert set(inventory_stmt["Action"]) == {
        "lambda:ListFunctions",
        "sqs:ListQueues",
        "events:ListRules",
        "rds:DescribeDBInstances",
        "redshift:DescribeClusters",
    }
    assert _find_statement(collector_statements, "s3:ListAllMyBuckets")["Resource"] == "*"

    collector_ec2_stmt = _find_statement(collector_statements, "ec2:CreateNetworkInterface")
    assert set(_actions(collector_ec2_stmt)) == {
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
    }
    assert collector_ec2_stmt["Resource"] == "*"

    assert _find_statement(summary_statements, "s3:GetObject")["Resource"]["Fn::Join"][1][-1] == "/*"
    summary_list_stmt = _find_statement(summary_statements, "s3:ListBucket")
    assert summary_list_stmt["Resource"]["Fn::GetAtt"][1] == "Arn"

    summary_secret_get = _find_statement(summary_statements, "secretsmanager:GetSecretValue")
    summary_secret_describe = _find_statement(summary_statements, "secretsmanager:DescribeSecret")
    assert summary_secret_get["Resource"]["Ref"]
    assert summary_secret_describe["Resource"]["Ref"] == summary_secret_get["Resource"]["Ref"]

    summary_ec2_stmt = _find_statement(summary_statements, "ec2:CreateNetworkInterface")
    assert set(_actions(summary_ec2_stmt)) == {
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
    }
    assert summary_ec2_stmt["Resource"] == "*"

    for statement in all_statements:
        assert "*" not in _actions(statement), f"Unexpected wildcard action in {statement}"
        actions = _actions(statement)
        if any(action.startswith("s3:") or action.startswith("secretsmanager:") for action in actions):
            allowed_global_s3_list = set(actions) <= {"s3:ListAllMyBuckets", "s3:ListBuckets"}
            if not allowed_global_s3_list:
                assert all(resource != "*" for resource in _resources_list(statement)), (
                    f"S3 and Secrets Manager statements must not use wildcard resources: {statement}"
                )
        assert "rds-db:connect" not in actions


def test_security_groups_allow_only_expected_traffic():
    template = _template()
    security_groups = _resources(template, "AWS::EC2::SecurityGroup")
    _, lambda_group = _single(
        security_groups,
        lambda _, resource: "shared by the analyst Lambda functions" in resource["Properties"]["GroupDescription"],
        "Expected Lambda security group",
    )
    _, database_group = _single(
        security_groups,
        lambda _, resource: "PostgreSQL instance" in resource["Properties"]["GroupDescription"],
        "Expected database security group",
    )

    assert "SecurityGroupIngress" not in lambda_group["Properties"]
    lambda_egress = lambda_group["Properties"].get("SecurityGroupEgress", [])
    assert any(
        rule.get("IpProtocol") == "-1" and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in lambda_egress
    )
    assert "SecurityGroupIngress" not in database_group["Properties"]
    _, ingress_rule = _single(_resources(template, "AWS::EC2::SecurityGroupIngress"))
    rule = ingress_rule["Properties"]
    assert rule["IpProtocol"] == "tcp"
    assert rule["FromPort"] == 5432
    assert rule["ToPort"] == 5432
    assert rule["SourceSecurityGroupId"]["Fn::GetAtt"][1] == "GroupId"


def test_api_gateway_methods_logging_and_schedule_wiring():
    template = _template()
    _, api = _single(_resources(template, "AWS::ApiGateway::RestApi"))
    assert api["Properties"]["Name"]

    resources = _resources(template, "AWS::ApiGateway::Resource")
    resource_paths = {resource["Properties"]["PathPart"] for resource in resources.values()}
    assert resource_paths == {"summary", "run"}

    methods = _resources(template, "AWS::ApiGateway::Method")
    get_method = [method for method in methods.values() if method["Properties"]["HttpMethod"] == "GET"]
    post_method = [method for method in methods.values() if method["Properties"]["HttpMethod"] == "POST"]
    assert len(get_method) == 1
    assert len(post_method) == 1
    assert get_method[0]["Properties"]["Integration"]["Type"] == "AWS_PROXY"
    assert post_method[0]["Properties"]["Integration"]["Type"] == "AWS_PROXY"

    _, stage = _single(_resources(template, "AWS::ApiGateway::Stage"))
    assert any(
        setting.get("LoggingLevel") == "INFO"
        and setting.get("DataTraceEnabled") is True
        for setting in stage["Properties"]["MethodSettings"]
    )

    _, schedule = _single(_resources(template, "AWS::Scheduler::Schedule"))
    schedule_props = schedule["Properties"]
    assert schedule_props["ScheduleExpression"] == "rate(1 hour)"
    assert schedule_props["State"] == "ENABLED"
    assert schedule_props["FlexibleTimeWindow"]["Mode"] == "OFF"
    assert "Arn" in schedule_props["Target"]

    permissions = _resources(template, "AWS::Lambda::Permission")
    assert any(permission["Properties"]["Principal"] == "scheduler.amazonaws.com" for permission in permissions.values())
    api_permissions = [
        permission
        for permission in permissions.values()
        if permission["Properties"]["Principal"] == "apigateway.amazonaws.com"
    ]
    assert any("/GET/summary" in json.dumps(permission["Properties"]["SourceArn"]) for permission in api_permissions)
    assert any("/POST/run" in json.dumps(permission["Properties"]["SourceArn"]) for permission in api_permissions)


def test_deliverable_input_contract_and_destructibility():
    template = _template()
    app_content = (REPO_ROOT / "app.ts").read_text(encoding="utf-8")
    template_parameters = template.get("Parameters", {})

    assert (REPO_ROOT / "app.ts").exists()
    assert {path.name for path in REPO_ROOT.glob("*.ts")} == {"app.ts"}
    assert set(template_parameters.keys()).issubset({"BootstrapVersion"})
    assert len(_resources(template, "AWS::ApiGateway::DomainName")) == 0

    env_names = set(re.findall(r"process\.env\.([A-Z0-9_]+)", app_content))
    assert env_names == {"AWS_REGION", "AWS_ENDPOINT"}
    assert "process.env.AWS_REGION ?? 'us-east-1'" in app_content
    assert 'os.getenv("AWS_REGION", "us-east-1")' in app_content

    disallowed_tokens = [
        "AWS_ENDPOINT" + "_URL",
        "NAME_PREFIX",
        "CDK_DEFAULT_ACCOUNT",
        "CfnParameter",
    ]
    assert all(token not in app_content for token in disallowed_tokens)

    lowered = app_content.lower()
    assert "aws_access_key_id" not in lowered
    assert "aws_secret_access_key" not in lowered

    for logical_id, resource in template.get("Resources", {}).items():
        assert resource.get("DeletionPolicy") != "Retain", f"{logical_id} must not retain on delete"
        assert resource.get("UpdateReplacePolicy") != "Retain", f"{logical_id} must not retain on replace"
        assert resource.get("UpdateReplacePolicy") != "Snapshot", f"{logical_id} must not snapshot on replace"
        assert resource.get("DeletionPolicy") != "Snapshot", f"{logical_id} must not snapshot on delete"


def test_outputs_exist_for_runtime_discovery():
    template = _template()
    outputs = template.get("Outputs", {})
    assert {
        "ApiUrl",
        "FindingsBucketName",
        "CollectorFunctionName",
        "SummaryFunctionName",
        "DatabaseSecretArn",
    }.issubset(set(outputs.keys()))
