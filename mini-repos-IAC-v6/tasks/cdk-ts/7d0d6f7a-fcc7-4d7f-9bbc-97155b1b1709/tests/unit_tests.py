import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
STACK_NAME = "SecurityPostureStack"
APP_TS = ROOT / "app.ts"
APP_SOURCE = APP_TS.read_text()


def load_template():
    template_path = ROOT / "template.json"
    if template_path.exists():
        return json.loads(template_path.read_text())
    return synth_template()


def synth_template():
    with tempfile.TemporaryDirectory() as outdir:
        env = os.environ.copy()
        env.setdefault("AWS_REGION", "us-east-1")
        env["CDK_OUTDIR"] = outdir
        subprocess.run(
            ["npx", "ts-node", "app.ts"],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        template_path = Path(outdir) / f"{STACK_NAME}.template.json"
        return json.loads(template_path.read_text())


def synth_failure(source):
    with tempfile.NamedTemporaryFile("w", suffix=".ts", dir=ROOT, delete=False) as temp_app:
        temp_path = Path(temp_app.name)
        temp_app.write(source)

    try:
        with tempfile.TemporaryDirectory() as outdir:
            env = os.environ.copy()
            env.setdefault("AWS_REGION", "us-east-1")
            env["CDK_OUTDIR"] = outdir
            return subprocess.run(
                ["npx", "ts-node", temp_path.name],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
    finally:
        temp_path.unlink()


TEMPLATE = load_template()
RESOURCES = TEMPLATE["Resources"]


def resources_by_type(resource_type):
    return {
        logical_id: resource
        for logical_id, resource in RESOURCES.items()
        if resource["Type"] == resource_type
    }


def single_resource(resource_type):
    matches = resources_by_type(resource_type)
    assert len(matches) == 1, f"expected exactly one {resource_type}, found {len(matches)}"
    return next(iter(matches.values()))


def find_resource(prefix, resource_type):
    matches = [
        resource
        for logical_id, resource in RESOURCES.items()
        if resource["Type"] == resource_type and logical_id.startswith(prefix)
    ]
    assert len(matches) == 1, f"expected one {resource_type} starting with {prefix}, found {len(matches)}"
    return matches[0]


def find_logical_id(prefix, resource_type):
    matches = [
        logical_id
        for logical_id, resource in RESOURCES.items()
        if resource["Type"] == resource_type and logical_id.startswith(prefix)
    ]
    assert len(matches) == 1, f"expected one {resource_type} starting with {prefix}, found {len(matches)}"
    return matches[0]


def render_joined_string(value):
    if isinstance(value, str):
        return value
    if "Fn::Join" in value:
        parts = []
        for item in value["Fn::Join"][1]:
            parts.append(item if isinstance(item, str) else "<token>")
        return "".join(parts)
    raise AssertionError(f"unsupported render type: {value}")


def replace_once(source, old, new):
    replaced = source.replace(old, new, 1)
    assert replaced != source, f"expected to replace {old!r}"
    return replaced


def parse_state_machine_definition(state_machine):
    return json.loads(render_joined_string(state_machine["Properties"]["DefinitionString"]))


def statement_actions(statement):
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def statement_resources(statement):
    resources = statement["Resource"]
    return resources if isinstance(resources, list) else [resources]


def find_statement(statements, required_actions):
    required = set(required_actions if isinstance(required_actions, list) else [required_actions])
    matches = [statement for statement in statements if set(statement_actions(statement)) == required]
    assert len(matches) == 1, f"expected one statement for {sorted(required)}, found {len(matches)}"
    return matches[0]


def role_policy_statements(role_prefix):
    role_logical_id = find_logical_id(role_prefix, "AWS::IAM::Role")
    statements = []
    for policy in resources_by_type("AWS::IAM::Policy").values():
        attached_roles = policy["Properties"].get("Roles", [])
        if {"Ref": role_logical_id} in attached_roles:
            policy_statements = policy["Properties"]["PolicyDocument"]["Statement"]
            if isinstance(policy_statements, dict):
                statements.append(policy_statements)
            else:
                statements.extend(policy_statements)
    return role_logical_id, statements


def assert_iam_template_valid(resources):
    role_ids = {
        logical_id
        for logical_id, resource in resources.items()
        if resource["Type"] == "AWS::IAM::Role"
    }
    policy_ids = {
        logical_id
        for logical_id, resource in resources.items()
        if resource["Type"] == "AWS::IAM::Policy"
    }

    for logical_id, policy in resources.items():
        if policy["Type"] != "AWS::IAM::Policy":
            continue

        rendered_document = json.dumps(policy["Properties"]["PolicyDocument"], separators=(",", ":"))
        assert len(rendered_document) <= 6144, f"{logical_id} exceeds the IAM inline policy size limit"

        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            for action in statement_actions(statement):
                assert re.fullmatch(r"[a-z0-9-]+:[A-Za-z*]+", action), (
                    f"{logical_id} contains an invalid IAM action format: {action}"
                )

        attached_roles = policy["Properties"].get("Roles", [])
        assert attached_roles, f"{logical_id} should attach to at least one IAM role"
        for role_ref in attached_roles:
            assert set(role_ref) == {"Ref"}
            assert role_ref["Ref"] in role_ids, f"{logical_id} attaches to an unknown IAM role"

    for logical_id, role in resources.items():
        if role["Type"] != "AWS::IAM::Role":
            continue
        depends_on = role.get("DependsOn", [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        assert not (set(depends_on) & policy_ids), f"{logical_id} should not depend on IAM policies"


def pre_stack_source():
    return APP_SOURCE.split("class SecurityPostureStack", 1)[0]


def inline_handler_code(function_prefix):
    function = find_resource(function_prefix, "AWS::Lambda::Function")
    return function["Properties"]["Code"]["ZipFile"]


def invoke_inline_handler(function_prefix, env, event=None):
    handler_code = inline_handler_code(function_prefix)
    event = {} if event is None else event
    with tempfile.TemporaryDirectory() as tempdir:
        temp_path = Path(tempdir)
        module_sources = {
            "@aws-sdk/client-sqs": """
class SQSClient {
  async send() {
    return {};
  }
}
class SendMessageCommand {
  constructor(input) {
    this.input = input;
  }
}
module.exports = { SQSClient, SendMessageCommand };
""",
            "@aws-sdk/client-secrets-manager": """
class SecretsManagerClient {
  async send() {
    return {};
  }
}
class GetSecretValueCommand {
  constructor(input) {
    this.input = input;
  }
}
module.exports = { SecretsManagerClient, GetSecretValueCommand };
""",
            "@aws-sdk/client-cloudwatch": """
class CloudWatchClient {
  async send() {
    return {};
  }
}
class PutMetricDataCommand {
  constructor(input) {
    this.input = input;
  }
}
module.exports = { CloudWatchClient, PutMetricDataCommand };
""",
        }
        for module_name, source in module_sources.items():
            module_dir = temp_path / "node_modules" / Path(module_name)
            module_dir.mkdir(parents=True, exist_ok=True)
            (module_dir / "index.js").write_text(source)

        (temp_path / "handler.cjs").write_text(handler_code)
        (temp_path / "runner.cjs").write_text(
            """
const { handler } = require("./handler.cjs");

(async () => {
  try {
    await handler(JSON.parse(process.env.TEST_EVENT_JSON || "{}"));
  } catch (error) {
    process.stderr.write(String(error && error.message ? error.message : error));
    process.exit(1);
  }
})();
"""
        )

        run_env = os.environ.copy()
        run_env.update(env)
        run_env["TEST_EVENT_JSON"] = json.dumps(event)
        return subprocess.run(
            ["node", "runner.cjs"],
            cwd=temp_path,
            env=run_env,
            capture_output=True,
            text=True,
        )


def test_resource_inventory_matches_exact_counts():
    expected = {
        "AWS::ApiGateway::Method": 1,
        "AWS::ApiGateway::Resource": 1,
        "AWS::ApiGateway::RestApi": 1,
        "AWS::ApiGateway::Stage": 1,
        "AWS::CloudWatch::Alarm": 2,
        "AWS::EC2::NatGateway": 1,
        "AWS::EC2::SecurityGroup": 2,
        "AWS::EC2::Subnet": 4,
        "AWS::EC2::VPC": 1,
        "AWS::IAM::Role": 4,
        "AWS::Lambda::Function": 2,
        "AWS::Logs::LogGroup": 4,
        "AWS::Logs::MetricFilter": 1,
        "AWS::Pipes::Pipe": 1,
        "AWS::RDS::DBInstance": 1,
        "AWS::RDS::DBSubnetGroup": 1,
        "AWS::SecretsManager::Secret": 1,
        "AWS::SecretsManager::SecretTargetAttachment": 1,
        "AWS::SQS::Queue": 1,
        "AWS::StepFunctions::StateMachine": 1,
    }

    actual = {}
    for resource in RESOURCES.values():
        actual[resource["Type"]] = actual.get(resource["Type"], 0) + 1

    for resource_type, count in expected.items():
        assert actual.get(resource_type) == count, f"{resource_type} count mismatch"


def test_solution_is_implemented_in_single_root_app_ts_file():
    assert APP_TS.exists()
    root_ts_files = sorted(path.name for path in ROOT.glob("*.ts") if path.name != "app.d.ts")
    assert root_ts_files == ["app.ts"]


def test_stack_input_contract_is_limited_to_allowed_configuration_variables():
    header = pre_stack_source()
    input_reads = set(
        re.findall(r"const\s+\w+\s*=\s*process\.env\.([A-Z0-9_]+)", header)
    )

    assert input_reads == {"AWS_REGION", "AWS_ENDPOINT"}
    assert "const defaultRegion = process.env.AWS_REGION ?? 'us-east-1';" in header
    assert "const endpointOverride = process.env.AWS_ENDPOINT;" in header
    assert "process.env.AWS_ENDPOINT_URL = endpointOverride;" in header
    assert "process.env.NAME_PREFIX" not in APP_SOURCE
    assert "process.env.CDK_DEFAULT_ACCOUNT" not in APP_SOURCE
    assert "process.env.AWS_ACCESS_KEY_ID" not in header
    assert "process.env.AWS_SECRET_ACCESS_KEY" not in header


def test_stack_synthesis_fails_for_invalid_network_configuration():
    result = synth_failure(
        replace_once(
            APP_SOURCE,
            "ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),",
            "ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/33'),",
        )
    )

    assert result.returncode != 0
    assert "is not a valid VPC CIDR range" in (result.stderr + result.stdout)


def test_stack_synthesis_fails_when_required_pipe_properties_are_missing():
    result = synth_failure(
        replace_once(
            APP_SOURCE,
            "roleArn: pipesExecutionRole.roleArn,",
            "roleArn: undefined as any,",
        )
    )

    assert result.returncode != 0
    assert "should be a string" in (result.stderr + result.stdout)


def test_stack_synthesis_fails_for_conflicting_construct_ids():
    result = synth_failure(
        replace_once(
            APP_SOURCE,
            "new lambda.Function(this, 'EnrichWorker'",
            "new lambda.Function(this, 'IngestWorker'",
        )
    )

    assert result.returncode != 0
    assert "There is already a Construct with name 'IngestWorker'" in (result.stderr + result.stdout)


def test_aws_endpoint_is_forwarded_to_sdk_endpoint_resolution():
    header = pre_stack_source()
    assert "if (endpointOverride) {" in header
    assert "process.env.AWS_ENDPOINT_URL = endpointOverride;" in header


def test_network_boundaries_are_deterministic_and_restrictive():
    vpc = single_resource("AWS::EC2::VPC")
    assert vpc["Properties"]["CidrBlock"] == "10.0.0.0/16"
    assert vpc["Properties"]["EnableDnsHostnames"] is True
    assert vpc["Properties"]["EnableDnsSupport"] is True

    subnets = list(resources_by_type("AWS::EC2::Subnet").values())
    public_subnets = [subnet for subnet in subnets if subnet["Properties"]["MapPublicIpOnLaunch"] is True]
    private_subnets = [subnet for subnet in subnets if subnet["Properties"]["MapPublicIpOnLaunch"] is False]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2

    compute_sg = find_resource("SGCompute", "AWS::EC2::SecurityGroup")
    database_sg = find_resource("SGDatabase", "AWS::EC2::SecurityGroup")

    assert "SecurityGroupIngress" not in compute_sg["Properties"]
    assert compute_sg["Properties"]["SecurityGroupEgress"] == [
        {
            "CidrIp": "0.0.0.0/0",
            "Description": "HTTPS egress only",
            "FromPort": 443,
            "IpProtocol": "tcp",
            "ToPort": 443,
        }
    ]
    assert database_sg["Properties"]["SecurityGroupEgress"] == [
        {
            "CidrIp": "0.0.0.0/0",
            "Description": "HTTPS egress only",
            "FromPort": 443,
            "IpProtocol": "tcp",
            "ToPort": 443,
        }
    ]

    ingress_rules = list(resources_by_type("AWS::EC2::SecurityGroupIngress").values())
    assert len(ingress_rules) == 1
    ingress = ingress_rules[0]["Properties"]
    assert ingress["FromPort"] == 5432
    assert ingress["ToPort"] == 5432
    assert ingress["IpProtocol"] == "tcp"
    assert ingress["SourceSecurityGroupId"] == {
        "Fn::GetAtt": [next(k for k, v in RESOURCES.items() if v == compute_sg), "GroupId"]
    }
    assert ingress["GroupId"] == {
        "Fn::GetAtt": [next(k for k, v in RESOURCES.items() if v == database_sg), "GroupId"]
    }


def test_compute_and_api_entrypoint_are_configured_as_requested():
    ingest = find_resource("IngestWorker", "AWS::Lambda::Function")
    enrich = find_resource("EnrichWorker", "AWS::Lambda::Function")
    compute_sg_id = {"Fn::GetAtt": [find_logical_id("SGCompute", "AWS::EC2::SecurityGroup"), "GroupId"]}
    ingest_log_group_id = find_logical_id("IngestWorkerLogGroup", "AWS::Logs::LogGroup")
    enrich_log_group_id = find_logical_id("EnrichWorkerLogGroup", "AWS::Logs::LogGroup")
    ingest_role_id = find_logical_id("IngestWorkerRole", "AWS::IAM::Role")
    enrich_role_id = find_logical_id("EnrichWorkerRole", "AWS::IAM::Role")
    private_subnet_refs = [
        {"Ref": logical_id}
        for logical_id, resource in RESOURCES.items()
        if resource["Type"] == "AWS::EC2::Subnet" and resource["Properties"]["MapPublicIpOnLaunch"] is False
    ]

    for function in (ingest, enrich):
        assert function["Properties"]["Runtime"] == "nodejs20.x"
        assert function["Properties"]["MemorySize"] == 256
        assert function["Properties"]["Timeout"] == 10
        assert function["Properties"]["ReservedConcurrentExecutions"] == 2
        assert function["Properties"]["Code"].get("ZipFile")
        assert function["Properties"]["VpcConfig"]["SecurityGroupIds"] == [compute_sg_id]
        assert sorted(function["Properties"]["VpcConfig"]["SubnetIds"], key=str) == sorted(private_subnet_refs, key=str)
        assert "PackageType" not in function["Properties"]

    assert ingest["Properties"]["Role"] == {"Fn::GetAtt": [ingest_role_id, "Arn"]}
    assert enrich["Properties"]["Role"] == {"Fn::GetAtt": [enrich_role_id, "Arn"]}
    assert ingest["Properties"]["Role"] != enrich["Properties"]["Role"]
    assert ingest["Properties"]["Environment"]["Variables"]["QUEUE_URL"]["Ref"].startswith("IngestQueue")
    assert ingest["Properties"]["Environment"]["Variables"]["DB_SECRET_ARN"]["Ref"].startswith("DatabaseCredentialsSecret")
    assert enrich["Properties"]["Environment"]["Variables"]["DB_SECRET_ARN"]["Ref"].startswith("DatabaseCredentialsSecret")
    assert "QUEUE_URL" not in enrich["Properties"]["Environment"]["Variables"]
    assert ingest["Properties"]["LoggingConfig"]["LogGroup"] == {"Ref": ingest_log_group_id}
    assert enrich["Properties"]["LoggingConfig"]["LogGroup"] == {"Ref": enrich_log_group_id}

    api = single_resource("AWS::ApiGateway::RestApi")
    stage = single_resource("AWS::ApiGateway::Stage")
    method = single_resource("AWS::ApiGateway::Method")
    resource = single_resource("AWS::ApiGateway::Resource")

    assert api["Properties"]["Name"] == "IngestApi"
    assert resource["Properties"]["PathPart"] == "ingest"
    assert method["Properties"]["HttpMethod"] == "POST"
    assert method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
    assert method["Properties"]["Integration"]["IntegrationHttpMethod"] == "POST"
    assert stage["Properties"]["StageName"] == "prod"
    assert stage["Properties"]["MethodSettings"] == [
        {
            "DataTraceEnabled": False,
            "HttpMethod": "*",
            "LoggingLevel": "INFO",
            "ResourcePath": "/*",
        }
    ]


def test_lambda_code_is_zip_compatible_and_not_image_based():
    for function in resources_by_type("AWS::Lambda::Function").values():
        code = function["Properties"]["Code"]
        assert "ImageUri" not in code
        assert "ZipFile" in code or {"S3Bucket", "S3Key"}.issubset(code.keys())


def test_lambda_environment_values_use_resource_references_where_needed():
    ingest = find_resource("IngestWorker", "AWS::Lambda::Function")
    enrich = find_resource("EnrichWorker", "AWS::Lambda::Function")

    assert ingest["Properties"]["Environment"]["Variables"] == {
        "QUEUE_URL": {"Ref": find_logical_id("IngestQueue", "AWS::SQS::Queue")},
        "DB_SECRET_ARN": {"Ref": find_logical_id("DatabaseCredentialsSecret", "AWS::SecretsManager::Secret")},
    }
    assert enrich["Properties"]["Environment"]["Variables"] == {
        "DB_SECRET_ARN": {"Ref": find_logical_id("DatabaseCredentialsSecret", "AWS::SecretsManager::Secret")},
    }


def test_physical_name_properties_are_not_hardcoded_strings():
    physical_name_properties = {
        "AWS::IAM::Role": ["RoleName"],
        "AWS::Lambda::Function": ["FunctionName"],
        "AWS::Logs::LogGroup": ["LogGroupName"],
        "AWS::RDS::DBInstance": ["DBInstanceIdentifier"],
        "AWS::SecretsManager::Secret": ["SecretName"],
        "AWS::SQS::Queue": ["QueueName"],
        "AWS::StepFunctions::StateMachine": ["StateMachineName"],
    }

    for resource_type, property_names in physical_name_properties.items():
        for resource in resources_by_type(resource_type).values():
            properties = resource.get("Properties", {})
            for property_name in property_names:
                if property_name in properties:
                    assert not isinstance(properties[property_name], str), (
                        f"{resource_type} should not hardcode {property_name}"
                    )


def test_state_machine_pipe_database_and_queue_wiring_are_correct():
    state_machine = single_resource("AWS::StepFunctions::StateMachine")
    pipe = single_resource("AWS::Pipes::Pipe")
    database = single_resource("AWS::RDS::DBInstance")
    subnet_group = single_resource("AWS::RDS::DBSubnetGroup")
    queue = single_resource("AWS::SQS::Queue")
    enrich_lambda_id = find_logical_id("EnrichWorker", "AWS::Lambda::Function")

    definition = parse_state_machine_definition(state_machine)
    invoke_state = definition["States"]["InvokeEnrichWorker"]

    assert definition["StartAt"] == "InvokeEnrichWorker"
    assert set(definition["States"]) == {"InvokeEnrichWorker", "Success"}
    assert invoke_state["Type"] == "Task"
    assert invoke_state["Next"] == "Success"
    assert invoke_state["Resource"] == "<token>"
    assert invoke_state["Retry"] == [
        {
            "BackoffRate": 2,
            "ErrorEquals": [
                "Lambda.ClientExecutionTimeoutException",
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
            ],
            "IntervalSeconds": 2,
            "MaxAttempts": 6,
        }
    ]
    assert definition["States"]["Success"] == {"Type": "Succeed"}
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert state_machine["Properties"]["LoggingConfiguration"]["Level"] == "ALL"
    assert state_machine["Properties"]["LoggingConfiguration"]["IncludeExecutionData"] is True

    assert pipe["Properties"]["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe["Properties"]["Source"] == {"Fn::GetAtt": [next(iter(resources_by_type("AWS::SQS::Queue"))), "Arn"]}
    assert pipe["Properties"]["Enrichment"] == {"Fn::GetAtt": [enrich_lambda_id, "Arn"]}
    assert pipe["Properties"]["Target"] == {"Ref": next(iter(resources_by_type("AWS::StepFunctions::StateMachine")))}
    assert pipe["Properties"]["TargetParameters"]["StepFunctionStateMachineParameters"] == {
        "InvocationType": "FIRE_AND_FORGET"
    }

    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert queue["Properties"]["MessageRetentionPeriod"] == 345600
    assert "FifoQueue" not in queue["Properties"]

    assert database["Properties"]["Engine"] == "postgres"
    assert database["Properties"]["EngineVersion"] == "15.5"
    assert database["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert database["Properties"]["AllocatedStorage"] == "20"
    assert database["Properties"]["StorageType"] == "gp2"
    assert database["Properties"]["PubliclyAccessible"] is False
    assert database["Properties"]["Port"] == "5432"
    assert database["Properties"]["BackupRetentionPeriod"] == 1
    assert database["Properties"]["DeletionProtection"] is False
    assert len(subnet_group["Properties"]["SubnetIds"]) == 2
    assert database["Properties"]["MasterUsername"]["Fn::Join"][1][0] == "{{resolve:secretsmanager:"
    assert database["Properties"]["MasterUserPassword"]["Fn::Join"][1][0] == "{{resolve:secretsmanager:"


def test_lambda_roles_separate_permissions_by_responsibility():
    queue_id = find_logical_id("IngestQueue", "AWS::SQS::Queue")
    secret_id = find_logical_id("DatabaseCredentialsSecret", "AWS::SecretsManager::Secret")

    _, ingest_statements = role_policy_statements("IngestWorkerRole")
    _, enrich_statements = role_policy_statements("EnrichWorkerRole")

    ingest_send = find_statement(ingest_statements, "sqs:SendMessage")
    assert ingest_send["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}

    ingest_secret = find_statement(ingest_statements, "secretsmanager:GetSecretValue")
    assert ingest_secret["Resource"] == {"Ref": secret_id}

    enrich_secret = find_statement(enrich_statements, "secretsmanager:GetSecretValue")
    assert enrich_secret["Resource"] == {"Ref": secret_id}

    enrich_metric = find_statement(enrich_statements, "cloudwatch:PutMetricData")
    assert enrich_metric["Resource"] == "*"
    assert enrich_metric["Condition"] == {
        "StringEquals": {"cloudwatch:namespace": "Custom/EnrichWorker"}
    }

    assert all("cloudwatch:PutMetricData" not in statement_actions(statement) for statement in ingest_statements)
    assert all("sqs:SendMessage" not in statement_actions(statement) for statement in enrich_statements)


def test_iam_is_minimally_scoped_and_only_uses_unavoidable_resource_wildcards():
    roles = resources_by_type("AWS::IAM::Role")
    assert len(roles) == 4

    lambda_role_prefixes = ("IngestWorkerRole", "EnrichWorkerRole")
    for prefix in lambda_role_prefixes:
        role = find_resource(prefix, "AWS::IAM::Role")
        assert role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
            "Service": "lambda.amazonaws.com"
        }
        assert role["Properties"]["ManagedPolicyArns"] == [
            {
                "Fn::Join": [
                    "",
                    [
                        "arn:",
                        {"Ref": "AWS::Partition"},
                        ":iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                    ],
                ]
            },
            {
                "Fn::Join": [
                    "",
                    [
                        "arn:",
                        {"Ref": "AWS::Partition"},
                        ":iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
                    ],
                ]
            },
        ]

    states_role = find_resource("StepFunctionsExecutionRole", "AWS::IAM::Role")
    pipes_role = find_resource("PipesExecutionRole", "AWS::IAM::Role")
    assert states_role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "states.amazonaws.com"
    }
    assert pipes_role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "pipes.amazonaws.com"
    }
    assert "ManagedPolicyArns" not in states_role["Properties"]
    assert "ManagedPolicyArns" not in pipes_role["Properties"]

    wildcard_resources = []
    for policy in resources_by_type("AWS::IAM::Policy").values():
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = statement_actions(statement)
            assert all("*" not in action for action in actions)
            if "*" in statement_resources(statement):
                wildcard_resources.append(statement)

    assert len(wildcard_resources) == 2
    assert any(
        statement_actions(statement) == ["cloudwatch:PutMetricData"]
        and statement.get("Condition") == {"StringEquals": {"cloudwatch:namespace": "Custom/EnrichWorker"}}
        for statement in wildcard_resources
    )
    assert any(
        set(statement_actions(statement))
        == {
            "logs:CreateLogDelivery",
            "logs:DeleteLogDelivery",
            "logs:DescribeLogGroups",
            "logs:DescribeResourcePolicies",
            "logs:GetLogDelivery",
            "logs:ListLogDeliveries",
            "logs:PutResourcePolicy",
            "logs:UpdateLogDelivery",
        }
        for statement in wildcard_resources
    )


def test_iam_policies_remain_valid_for_size_action_format_and_dependencies():
    assert_iam_template_valid(RESOURCES)


def test_iam_policy_validation_fails_when_inline_policy_exceeds_size_limit():
    invalid_resources = json.loads(json.dumps(RESOURCES))
    policy_id = next(iter(resources_by_type("AWS::IAM::Policy")))
    invalid_resources[policy_id]["Properties"]["PolicyDocument"]["Statement"][0]["Sid"] = "A" * 7000

    with pytest.raises(AssertionError, match="exceeds the IAM inline policy size limit"):
        assert_iam_template_valid(invalid_resources)


def test_iam_policy_validation_fails_for_invalid_action_names():
    invalid_resources = json.loads(json.dumps(RESOURCES))
    policy_id = next(iter(resources_by_type("AWS::IAM::Policy")))
    invalid_resources[policy_id]["Properties"]["PolicyDocument"]["Statement"][0]["Action"] = "lambdaInvokeFunction"

    with pytest.raises(AssertionError, match="contains an invalid IAM action format"):
        assert_iam_template_valid(invalid_resources)


def test_iam_policy_validation_fails_for_role_policy_circular_dependencies():
    invalid_resources = json.loads(json.dumps(RESOURCES))
    role_id = next(iter(resources_by_type("AWS::IAM::Role")))
    policy_id = next(iter(resources_by_type("AWS::IAM::Policy")))
    invalid_resources[role_id]["DependsOn"] = [policy_id]

    with pytest.raises(AssertionError, match="should not depend on IAM policies"):
        assert_iam_template_valid(invalid_resources)


def test_iam_policy_statements_are_scoped_to_expected_resources():
    queue_id = find_logical_id("IngestQueue", "AWS::SQS::Queue")
    enrich_lambda_id = find_logical_id("EnrichWorker", "AWS::Lambda::Function")
    state_machine_id = find_logical_id("EnrichmentStateMachine", "AWS::StepFunctions::StateMachine")
    sfn_log_group_id = find_logical_id("StepFunctionsLogGroup", "AWS::Logs::LogGroup")

    _, step_functions_statements = role_policy_statements("StepFunctionsExecutionRole")
    invoke_statement = find_statement(step_functions_statements, "lambda:InvokeFunction")
    assert invoke_statement["Resource"] == [
        {"Fn::GetAtt": [enrich_lambda_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [enrich_lambda_id, "Arn"]}, ":*"]]},
    ]
    logs_statement = find_statement(
        step_functions_statements,
        ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"],
    )
    assert logs_statement["Resource"] == [
        {"Fn::GetAtt": [sfn_log_group_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [sfn_log_group_id, "Arn"]}, ":*"]]},
    ]

    _, pipes_statements = role_policy_statements("PipesExecutionRole")
    receive_statement = find_statement(
        pipes_statements,
        ["sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ReceiveMessage"],
    )
    assert receive_statement["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    pipe_invoke_statement = find_statement(pipes_statements, "lambda:InvokeFunction")
    assert pipe_invoke_statement["Resource"] == [
        {"Fn::GetAtt": [enrich_lambda_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [enrich_lambda_id, "Arn"]}, ":*"]]},
    ]
    start_execution_statement = find_statement(pipes_statements, "states:StartExecution")
    assert start_execution_statement["Resource"] == {"Ref": state_machine_id}


def test_observability_resources_have_expected_retention_filters_and_alarms():
    log_groups = list(resources_by_type("AWS::Logs::LogGroup").values())
    assert len(log_groups) == 4
    for log_group in log_groups:
        assert log_group["Properties"]["RetentionInDays"] == 14
        assert "KmsKeyId" not in log_group["Properties"]

    metric_filter = single_resource("AWS::Logs::MetricFilter")
    assert metric_filter["Properties"]["FilterPattern"] == "{ $.status = 5* }"
    assert metric_filter["Properties"]["MetricTransformations"] == [
        {
            "MetricName": "ServerErrors5xx",
            "MetricNamespace": "Custom/ApiGateway",
            "MetricValue": "1",
        }
    ]

    alarms = list(resources_by_type("AWS::CloudWatch::Alarm").values())
    metric_targets = sorted(alarm["Properties"]["Dimensions"][0]["Value"]["Ref"] for alarm in alarms)
    assert metric_targets == sorted(
        [
            next(
                logical_id
                for logical_id in RESOURCES
                if logical_id.startswith("IngestWorker") and RESOURCES[logical_id]["Type"] == "AWS::Lambda::Function"
            ),
            next(
                logical_id
                for logical_id in RESOURCES
                if logical_id.startswith("EnrichWorker") and RESOURCES[logical_id]["Type"] == "AWS::Lambda::Function"
            ),
        ]
    )
    for alarm in alarms:
        assert alarm["Properties"]["Namespace"] == "AWS/Lambda"
        assert alarm["Properties"]["MetricName"] == "Errors"
        assert alarm["Properties"]["Statistic"] == "Sum"
        assert alarm["Properties"]["Period"] == 60
        assert alarm["Properties"]["EvaluationPeriods"] == 1
        assert alarm["Properties"]["Threshold"] == 1
        assert alarm["Properties"]["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
        assert alarm["Properties"]["TreatMissingData"] == "notBreaching"


def test_api_logging_and_metric_filter_use_dedicated_stage_log_group():
    stage = single_resource("AWS::ApiGateway::Stage")
    metric_filter = single_resource("AWS::Logs::MetricFilter")
    api_log_group_id = find_logical_id("ApiStageLogGroup", "AWS::Logs::LogGroup")

    assert stage["Properties"]["AccessLogSetting"]["DestinationArn"] == {
        "Fn::GetAtt": [api_log_group_id, "Arn"]
    }
    assert '"status":"$context.status"' in stage["Properties"]["AccessLogSetting"]["Format"]
    assert metric_filter["Properties"]["LogGroupName"] == {"Ref": api_log_group_id}


def test_secret_credentials_and_database_attachment_are_generated_and_referenced():
    secret = single_resource("AWS::SecretsManager::Secret")
    attachment = single_resource("AWS::SecretsManager::SecretTargetAttachment")
    db_id = find_logical_id("Database", "AWS::RDS::DBInstance")
    secret_id = find_logical_id("DatabaseCredentialsSecret", "AWS::SecretsManager::Secret")

    generate = secret["Properties"]["GenerateSecretString"]
    secret_template = json.loads(generate["SecretStringTemplate"])
    assert "username" in secret_template
    assert secret_template["username"]
    assert generate["GenerateStringKey"] == "password"
    assert generate["ExcludePunctuation"] is True
    assert attachment["Properties"]["SecretId"] == {"Ref": secret_id}
    assert attachment["Properties"]["TargetId"] == {"Ref": db_id}
    assert attachment["Properties"]["TargetType"] == "AWS::RDS::DBInstance"


def test_template_is_destructible_without_retention_policies():
    for resource in RESOURCES.values():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}


def test_ingest_worker_handler_requires_queue_url_env_var():
    result = invoke_inline_handler(
        "IngestWorker",
        env={
            "AWS_REGION": "us-east-1",
            "DB_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        },
    )
    assert result.returncode != 0
    assert "QUEUE_URL environment variable is required" in result.stderr


def test_ingest_worker_handler_requires_secret_arn_env_var():
    result = invoke_inline_handler(
        "IngestWorker",
        env={
            "AWS_REGION": "us-east-1",
            "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test",
        },
    )
    assert result.returncode != 0
    assert "DB_SECRET_ARN environment variable is required" in result.stderr


def test_ingest_worker_handler_rejects_malformed_queue_url_env_var():
    result = invoke_inline_handler(
        "IngestWorker",
        env={
            "AWS_REGION": "us-east-1",
            "QUEUE_URL": "not-a-queue-url",
            "DB_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        },
    )
    assert result.returncode != 0
    assert "QUEUE_URL environment variable must be a valid SQS queue URL" in result.stderr


def test_ingest_worker_handler_rejects_blank_queue_url_env_var():
    result = invoke_inline_handler(
        "IngestWorker",
        env={
            "AWS_REGION": "us-east-1",
            "QUEUE_URL": "   ",
            "DB_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        },
    )
    assert result.returncode != 0
    assert "QUEUE_URL environment variable is required" in result.stderr


def test_ingest_worker_handler_rejects_malformed_secret_arn_env_var():
    result = invoke_inline_handler(
        "IngestWorker",
        env={
            "AWS_REGION": "us-east-1",
            "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test",
            "DB_SECRET_ARN": "arn:aws:sqs:us-east-1:123456789012:test",
        },
    )
    assert result.returncode != 0
    assert "DB_SECRET_ARN environment variable must be a Secrets Manager secret ARN" in result.stderr


def test_enrich_worker_handler_requires_secret_arn_env_var():
    result = invoke_inline_handler(
        "EnrichWorker",
        env={"AWS_REGION": "us-east-1"},
    )
    assert result.returncode != 0
    assert "DB_SECRET_ARN environment variable is required" in result.stderr


def test_enrich_worker_handler_rejects_blank_secret_arn_env_var():
    result = invoke_inline_handler(
        "EnrichWorker",
        env={
            "AWS_REGION": "us-east-1",
            "DB_SECRET_ARN": "   ",
        },
    )
    assert result.returncode != 0
    assert "DB_SECRET_ARN environment variable is required" in result.stderr


def test_enrich_worker_handler_rejects_malformed_secret_arn_env_var():
    result = invoke_inline_handler(
        "EnrichWorker",
        env={
            "AWS_REGION": "us-east-1",
            "DB_SECRET_ARN": "malformed-secret-arn",
        },
    )
    assert result.returncode != 0
    assert "DB_SECRET_ARN environment variable must be a Secrets Manager secret ARN" in result.stderr
