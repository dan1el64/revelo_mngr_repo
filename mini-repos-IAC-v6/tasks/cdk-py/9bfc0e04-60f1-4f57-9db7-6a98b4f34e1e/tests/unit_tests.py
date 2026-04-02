import ast
import builtins
import copy
import json
import os
import re
import subprocess
import sys
import tempfile
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STACK_TEMPLATE = "InfrastructureAnalysisStack.template.json"
REQUIRED_OUTPUTS = {
    "HttpApiUrl",
    "AlbDnsName",
    "SqsQueueUrl",
    "SqsQueueArn",
    "EventBusName",
    "EventBusArn",
    "EventRuleArn",
    "EventPipeArn",
    "StateMachineArn",
    "DynamoDBTableName",
    "RdsEndpointAddress",
    "RdsEndpointPort",
    "DbSecretArn",
}


def synth_template(extra_env=None, clear_region=False):
    env = {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "HOME", "TMPDIR", "TERM", "LANG", "SHELL", "PYTHONPATH", "PYTHONHOME"}
    }
    if clear_region:
        env.pop("AWS_REGION", None)
    else:
        env["AWS_REGION"] = env.get("AWS_REGION", "us-east-1")
    if extra_env:
        env.update(extra_env)

    with tempfile.TemporaryDirectory() as outdir:
        env["CDK_OUTDIR"] = outdir
        subprocess.run(
            [sys.executable, "app.py"],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        template_path = Path(outdir, STACK_TEMPLATE)
        manifest_path = Path(outdir, "manifest.json")
        return (
            json.loads(template_path.read_text()),
            template_path.read_text(),
            json.loads(manifest_path.read_text()),
        )


def resources_by_type(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def single_resource(template, resource_type):
    matches = resources_by_type(template, resource_type)
    assert len(matches) == 1, f"expected one {resource_type}, found {len(matches)}"
    return next(iter(matches.items()))


def policy_for_role(template, role_ref):
    for _, resource in resources_by_type(template, "AWS::IAM::Policy").items():
        if role_ref in resource["Properties"]["Roles"]:
            return resource["Properties"]["PolicyDocument"]["Statement"]
    raise AssertionError(f"policy for role ref {role_ref} not found")


def normalize_actions(statement):
    actions = statement["Action"]
    return set(actions if isinstance(actions, list) else [actions])


def normalize_resources(statement):
    resources = statement["Resource"]
    return resources if isinstance(resources, list) else [resources]


def load_inline_lambda_module(zipfile_source, env_overrides, boto3_impl):
    previous_env = os.environ.copy()
    previous_boto3 = sys.modules.get("boto3")
    try:
        os.environ.update(env_overrides)
        sys.modules["boto3"] = boto3_impl
        namespace = {}
        exec(zipfile_source, namespace)
        return namespace
    finally:
        os.environ.clear()
        os.environ.update(previous_env)
        if previous_boto3 is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = previous_boto3


def render_cfn_string(node):
    if isinstance(node, str):
        return node
    if isinstance(node, dict) and "Fn::Join" in node:
        return "".join(render_cfn_string(part) for part in node["Fn::Join"][1])
    if isinstance(node, dict):
        return "__TOKEN__"
    if isinstance(node, list):
        return "".join(render_cfn_string(part) for part in node)
    return str(node)


def load_configure_sdk_context():
    module_ast = ast.parse(Path(ROOT, "app.py").read_text())
    function_node = next(
        node for node in module_ast.body
        if isinstance(node, ast.FunctionDef) and node.name == "configure_sdk_context"
    )
    function_node = copy.deepcopy(function_node)
    function_node.returns = None
    for arg in function_node.args.args:
        arg.annotation = None
    compiled = compile(
        ast.fix_missing_locations(
            ast.Module(
                body=[
                    ast.Import(names=[ast.alias(name="os")]),
                    function_node,
                ],
                type_ignores=[],
            )
        ),
        filename=str(Path(ROOT, "app.py")),
        mode="exec",
    )
    namespace = {}
    exec(compiled, namespace)
    return namespace["configure_sdk_context"]


def find_stack_constructor_call():
    module_ast = ast.parse(Path(ROOT, "app.py").read_text())
    return next(
        node for node in ast.walk(module_ast)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "InfrastructureAnalysisStack"
    )


def test_topology_outputs_and_named_resources_match_prompt():
    template, template_text, manifest = synth_template()
    allowed_resource_types = {
        "AWS::ApiGatewayV2::Api",
        "AWS::ApiGatewayV2::Integration",
        "AWS::ApiGatewayV2::Route",
        "AWS::ApiGatewayV2::Stage",
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
        "AWS::ECS::Cluster",
        "AWS::ECS::Service",
        "AWS::ECS::TaskDefinition",
        "AWS::ElasticLoadBalancingV2::Listener",
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        "AWS::ElasticLoadBalancingV2::TargetGroup",
        "AWS::Events::EventBus",
        "AWS::Events::Rule",
        "AWS::IAM::Policy",
        "AWS::IAM::Role",
        "AWS::Lambda::EventSourceMapping",
        "AWS::Lambda::Function",
        "AWS::Lambda::Permission",
        "AWS::Logs::LogGroup",
        "AWS::Pipes::Pipe",
        "AWS::RDS::DBInstance",
        "AWS::RDS::DBSubnetGroup",
        "AWS::SQS::Queue",
        "AWS::SQS::QueuePolicy",
        "AWS::SecretsManager::Secret",
        "AWS::StepFunctions::StateMachine",
    }
    assert {resource["Type"] for resource in template["Resources"].values()} <= allowed_resource_types

    assert len(resources_by_type(template, "AWS::EC2::VPC")) == 1
    assert len(resources_by_type(template, "AWS::EC2::Subnet")) == 4
    assert len(resources_by_type(template, "AWS::EC2::NatGateway")) == 1
    assert len(resources_by_type(template, "AWS::EC2::SecurityGroup")) == 3
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")) == 1
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::Listener")) == 1
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::TargetGroup")) == 1
    assert len(resources_by_type(template, "AWS::ECS::Cluster")) == 1
    assert len(resources_by_type(template, "AWS::ECS::TaskDefinition")) == 1
    assert len(resources_by_type(template, "AWS::ECS::Service")) == 1
    assert len(resources_by_type(template, "AWS::ApiGatewayV2::Api")) == 1
    assert len(resources_by_type(template, "AWS::ApiGatewayV2::Integration")) == 1
    assert len(resources_by_type(template, "AWS::ApiGatewayV2::Route")) == 1
    assert len(resources_by_type(template, "AWS::ApiGatewayV2::Stage")) == 1
    assert len(resources_by_type(template, "AWS::Lambda::Function")) == 2
    assert len(resources_by_type(template, "AWS::Lambda::Permission")) == 1
    assert len(resources_by_type(template, "AWS::SQS::Queue")) == 1
    assert len(resources_by_type(template, "AWS::DynamoDB::Table")) == 1
    assert len(resources_by_type(template, "AWS::Events::EventBus")) == 1
    assert len(resources_by_type(template, "AWS::Events::Rule")) == 1
    assert len(resources_by_type(template, "AWS::Pipes::Pipe")) == 1
    assert len(resources_by_type(template, "AWS::StepFunctions::StateMachine")) == 1
    assert len(resources_by_type(template, "AWS::SecretsManager::Secret")) == 1
    assert len(resources_by_type(template, "AWS::RDS::DBInstance")) == 1

    outputs = template["Outputs"]
    assert REQUIRED_OUTPUTS.issubset(outputs.keys())

    _, vpc = single_resource(template, "AWS::EC2::VPC")
    assert vpc["Properties"]["EnableDnsHostnames"] is True
    assert vpc["Properties"]["EnableDnsSupport"] is True

    subnets = resources_by_type(template, "AWS::EC2::Subnet")
    public_subnets = [
        resource for resource in subnets.values()
        if any(tag.get("Value") == "Public" for tag in resource["Properties"]["Tags"])
    ]
    private_subnets = [
        resource for resource in subnets.values()
        if any(tag.get("Value") == "Private" for tag in resource["Properties"]["Tags"])
    ]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert len({json.dumps(subnet["Properties"]["AvailabilityZone"]) for subnet in public_subnets}) == 2
    assert len({json.dumps(subnet["Properties"]["AvailabilityZone"]) for subnet in private_subnets}) == 2

    assert "123456789012" not in template_text

    app_text = Path(ROOT, "app.py").read_text()
    assert "Internet -> ALB:80 -> backend Processing Units:80 -> Storage Layer (RDS PostgreSQL:5432)" in app_text
    stack_artifacts = [
        artifact for artifact in manifest["artifacts"].values()
        if artifact.get("type") == "aws:cloudformation:stack"
    ]
    assert len(stack_artifacts) == 1

    explicit_role_names = {
        role["Properties"]["RoleName"]
        for role in resources_by_type(template, "AWS::IAM::Role").values()
        if "RoleName" in role["Properties"]
    }
    assert explicit_role_names == {
        "infra-analysis-ingest-role",
        "infra-analysis-analyzer-role",
        "infra-analysis-pipe-role",
        "infra-analysis-sfn-role",
    }
    _, state_machine = single_resource(template, "AWS::StepFunctions::StateMachine")
    assert state_machine["Properties"]["StateMachineName"] == "infra-analysis-state-machine"
    _, db_subnet_group = single_resource(template, "AWS::RDS::DBSubnetGroup")
    assert db_subnet_group["Properties"]["DBSubnetGroupName"] == "infra-analysis-db-subnet-group"
    _, secret = single_resource(template, "AWS::SecretsManager::Secret")
    assert secret["Properties"]["Name"] == "infra-analysis-db-secret"


def test_network_compute_storage_and_eventing_properties_are_explicit():
    template, _, _ = synth_template()

    security_groups = resources_by_type(template, "AWS::EC2::SecurityGroup")
    alb_sg = next(
        resource for resource in security_groups.values()
        if resource["Properties"]["GroupName"] == "infra-analysis-alb-sg"
    )
    backend_sg_logical_id = next(
        logical_id for logical_id, resource in security_groups.items()
        if resource["Properties"]["GroupName"] == "infra-analysis-backend-sg"
    )
    alb_sg_logical_id = next(
        logical_id for logical_id, resource in security_groups.items()
        if resource["Properties"]["GroupName"] == "infra-analysis-alb-sg"
    )
    db_sg_logical_id = next(
        logical_id for logical_id, resource in security_groups.items()
        if resource["Properties"]["GroupName"] == "infra-analysis-db-sg"
    )
    assert alb_sg["Properties"]["SecurityGroupIngress"] == [
        {
            "CidrIp": "0.0.0.0/0",
            "Description": "Allow HTTP from the internet",
            "FromPort": 80,
            "IpProtocol": "tcp",
            "ToPort": 80,
        }
    ]
    assert alb_sg["Properties"]["SecurityGroupEgress"][0]["IpProtocol"] == "-1"
    backend_sg = security_groups[backend_sg_logical_id]
    db_sg = security_groups[db_sg_logical_id]
    assert backend_sg["Properties"]["SecurityGroupEgress"][0]["IpProtocol"] == "-1"
    assert db_sg["Properties"]["SecurityGroupEgress"][0]["IpProtocol"] == "-1"

    ingress_rules = resources_by_type(template, "AWS::EC2::SecurityGroupIngress")
    _, task_definition = single_resource(template, "AWS::ECS::TaskDefinition")
    task_props = task_definition["Properties"]
    container = task_props["ContainerDefinitions"][0]
    backend_port = container["PortMappings"][0]["ContainerPort"]
    assert any(
        rule["Properties"]["GroupId"]["Fn::GetAtt"][0] == backend_sg_logical_id
        and rule["Properties"]["SourceSecurityGroupId"]["Fn::GetAtt"][0] == alb_sg_logical_id
        and rule["Properties"]["FromPort"] == backend_port
        and rule["Properties"]["ToPort"] == backend_port
        for rule in ingress_rules.values()
    )
    assert any(
        rule["Properties"]["GroupId"]["Fn::GetAtt"][0] == db_sg_logical_id
        and rule["Properties"]["SourceSecurityGroupId"]["Fn::GetAtt"][0] == backend_sg_logical_id
        and rule["Properties"]["FromPort"] == 5432
        and rule["Properties"]["ToPort"] == 5432
        for rule in ingress_rules.values()
    )

    _, alb = single_resource(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    assert alb["Properties"]["Name"] == "infra-analysis-alb"
    assert alb["Properties"]["Scheme"] == "internet-facing"
    assert alb["Properties"]["Type"] == "application"

    _, listener = single_resource(template, "AWS::ElasticLoadBalancingV2::Listener")
    assert listener["Properties"]["Port"] == 80
    assert listener["Properties"]["Protocol"] == "HTTP"

    _, target_group = single_resource(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
    assert target_group["Properties"]["Name"] == "infra-analysis-tg"
    assert target_group["Properties"]["Port"] == backend_port
    assert target_group["Properties"]["TargetType"] == "ip"

    _, cluster = single_resource(template, "AWS::ECS::Cluster")
    assert cluster["Properties"]["ClusterName"] == "infra-analysis-cluster"

    assert task_props["Family"] == "infra-analysis-task"
    assert task_props["RequiresCompatibilities"] == ["FARGATE"]
    assert task_props["NetworkMode"] == "awsvpc"
    assert container["Image"].startswith("public.ecr.aws/")
    assert container["PortMappings"] == [{"ContainerPort": backend_port, "Protocol": "tcp"}]
    assert container["LogConfiguration"]["LogDriver"] == "awslogs"
    ecs_log_group_logical_id = next(
        logical_id for logical_id, resource in resources_by_type(template, "AWS::Logs::LogGroup").items()
        if resource["Properties"]["LogGroupName"] == "/ecs/infra-analysis-backend"
    )
    assert container["LogConfiguration"]["Options"]["awslogs-group"] == {"Ref": ecs_log_group_logical_id}
    assert len(container["Secrets"]) >= 2
    secret_logical_id = next(iter(resources_by_type(template, "AWS::SecretsManager::Secret")))
    assert all("ValueFrom" in secret for secret in container["Secrets"])
    assert sum(
        1
        for secret in container["Secrets"]
        if secret_logical_id in json.dumps(secret["ValueFrom"])
    ) >= 2
    queue_logical_id = next(iter(resources_by_type(template, "AWS::SQS::Queue")))
    event_bus_logical_id = next(iter(resources_by_type(template, "AWS::Events::EventBus")))
    container_env = {entry["Name"]: entry["Value"] for entry in container["Environment"]}
    assert container_env["TELEMETRY_QUEUE_URL"] == {"Ref": queue_logical_id}
    assert container_env["TELEMETRY_EVENT_BUS_NAME"] == {"Ref": event_bus_logical_id}

    _, service = single_resource(template, "AWS::ECS::Service")
    service_props = service["Properties"]
    assert service_props["DesiredCount"] == 1
    assert service_props["LaunchType"] == "FARGATE"
    assert service_props["ServiceName"] == "infra-analysis-backend"
    assert service_props["NetworkConfiguration"]["AwsvpcConfiguration"]["AssignPublicIp"] == "DISABLED"
    assert len(service_props["NetworkConfiguration"]["AwsvpcConfiguration"]["Subnets"]) == 2
    assert service_props["NetworkConfiguration"]["AwsvpcConfiguration"]["SecurityGroups"] == [
        {"Fn::GetAtt": [backend_sg_logical_id, "GroupId"]}
    ]
    assert service_props["LoadBalancers"][0]["ContainerPort"] == backend_port

    lambda_functions = resources_by_type(template, "AWS::Lambda::Function")
    ingest = next(resource for resource in lambda_functions.values() if resource["Properties"]["FunctionName"] == "infra-analysis-ingest")
    analyzer = next(resource for resource in lambda_functions.values() if resource["Properties"]["FunctionName"] == "infra-analysis-analyzer")
    assert ingest["Properties"]["Runtime"] == "python3.12"
    assert ingest["Properties"]["MemorySize"] == 256
    assert ingest["Properties"]["Timeout"] == 10
    assert ingest["Properties"].get("PackageType", "Zip") == "Zip"
    assert "ImageUri" not in ingest["Properties"]
    assert ingest["Properties"]["Environment"]["Variables"]["INGEST_SOURCE"] == "infrastructure-analysis.ingest"
    assert ingest["Properties"]["Environment"]["Variables"]["INGEST_DETAIL_TYPE"] == "IngestAccepted"
    assert analyzer["Properties"]["Runtime"] == "python3.12"
    assert analyzer["Properties"]["MemorySize"] == 256
    assert analyzer["Properties"]["Timeout"] == 15
    assert analyzer["Properties"].get("PackageType", "Zip") == "Zip"
    assert "ImageUri" not in analyzer["Properties"]

    _, integration = single_resource(template, "AWS::ApiGatewayV2::Integration")
    assert integration["Properties"]["IntegrationType"] == "AWS_PROXY"

    api_logical_id, api = single_resource(template, "AWS::ApiGatewayV2::Api")
    assert api["Properties"]["Name"] == "infra-analysis-http-api"
    _, route = single_resource(template, "AWS::ApiGatewayV2::Route")
    assert route["Properties"]["RouteKey"] == "POST /ingest"

    _, stage = single_resource(template, "AWS::ApiGatewayV2::Stage")
    assert stage["Properties"]["ApiId"] == {"Ref": api_logical_id}
    assert stage["Properties"]["StageName"] == "$default"
    assert stage["Properties"]["AutoDeploy"] is True

    ingest_logical_id = next(
        logical_id for logical_id, resource in lambda_functions.items()
        if resource["Properties"]["FunctionName"] == "infra-analysis-ingest"
    )
    _, invoke_permission = single_resource(template, "AWS::Lambda::Permission")
    assert invoke_permission["Properties"]["Action"] == "lambda:InvokeFunction"
    assert invoke_permission["Properties"]["Principal"] == "apigateway.amazonaws.com"
    assert ingest_logical_id in json.dumps(invoke_permission["Properties"]["FunctionName"])
    permission_source_arn = render_cfn_string(invoke_permission["Properties"]["SourceArn"])
    assert "execute-api" in permission_source_arn
    assert api_logical_id in json.dumps(invoke_permission["Properties"]["SourceArn"])

    _, queue = single_resource(template, "AWS::SQS::Queue")
    assert queue["Properties"]["QueueName"] == "infra-analysis-queue"
    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert "FifoQueue" not in queue["Properties"]

    _, table = single_resource(template, "AWS::DynamoDB::Table")
    assert table["Properties"]["TableName"] == "infra-analysis-ledger"
    assert table["Properties"]["BillingMode"] == "PAY_PER_REQUEST"
    assert table["Properties"]["AttributeDefinitions"] == [
        {"AttributeName": "pk", "AttributeType": "S"}
    ]
    assert table["Properties"]["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]

    event_bus_logical_id, event_bus = single_resource(template, "AWS::Events::EventBus")
    _, rule = single_resource(template, "AWS::Events::Rule")
    assert rule["Properties"]["Name"] == "infra-analysis-ingest-rule"
    assert rule["Properties"]["EventPattern"] == {
        "source": ["infrastructure-analysis.ingest"],
        "detail-type": ["IngestAccepted"],
    }
    assert rule["Properties"]["EventBusName"] == {"Ref": event_bus_logical_id}
    assert rule["Properties"]["Targets"][0]["Arn"]["Fn::GetAtt"][0] == next(iter(resources_by_type(template, "AWS::SQS::Queue")))
    assert event_bus["Properties"]["Name"] == "infra-analysis-bus"

    _, event_source_mapping = single_resource(template, "AWS::Lambda::EventSourceMapping")
    assert event_source_mapping["Properties"]["EventSourceArn"]["Fn::GetAtt"][0] == next(iter(resources_by_type(template, "AWS::SQS::Queue")))

    _, pipe = single_resource(template, "AWS::Pipes::Pipe")
    assert pipe["Properties"]["Name"] == "infra-analysis-pipe"
    analyzer_logical_id = next(
        logical_id for logical_id, resource in lambda_functions.items()
        if resource["Properties"]["FunctionName"] == "infra-analysis-analyzer"
    )
    state_machine_logical_id, _ = single_resource(template, "AWS::StepFunctions::StateMachine")
    assert pipe["Properties"]["Source"]["Fn::GetAtt"][0] == next(iter(resources_by_type(template, "AWS::SQS::Queue")))
    assert pipe["Properties"]["Enrichment"]["Fn::GetAtt"][0] == analyzer_logical_id
    assert pipe["Properties"]["Target"] == {"Ref": state_machine_logical_id}

    _, state_machine = single_resource(template, "AWS::StepFunctions::StateMachine")
    definition = json.loads(render_cfn_string(state_machine["Properties"]["DefinitionString"]))
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert len(definition["States"]) == 2
    pass_state_name = next(name for name, state in definition["States"].items() if state["Type"] == "Pass")
    task_state_name = next(name for name, state in definition["States"].items() if state["Type"] == "Task")
    pass_state = definition["States"][pass_state_name]
    assert definition["StartAt"] == pass_state_name
    assert pass_state["Next"] == task_state_name
    assert "Parameters" in pass_state
    assert pass_state.get("ResultPath", "$") == "$"
    assert any(
        value == "$$.State.EnteredTime"
        for key, value in pass_state["Parameters"].items()
        if key.endswith(".$")
    )
    assert analyzer_logical_id in json.dumps(state_machine["Properties"]["DefinitionString"])

    log_groups = resources_by_type(template, "AWS::Logs::LogGroup")
    log_group_names = {resource["Properties"]["LogGroupName"] for resource in log_groups.values()}
    assert log_group_names == {
        "/aws/lambda/infra-analysis-ingest",
        "/aws/lambda/infra-analysis-analyzer",
        "/ecs/infra-analysis-backend",
    }

    _, db_instance = single_resource(template, "AWS::RDS::DBInstance")
    assert db_instance["Properties"]["DBInstanceIdentifier"] == "infra-analysis-db"
    assert db_instance["Properties"]["Engine"] == "postgres"
    assert db_instance["Properties"]["EngineVersion"]
    assert db_instance["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["Properties"]["AllocatedStorage"] == "20"
    assert db_instance["Properties"]["PubliclyAccessible"] is False
    assert db_instance["Properties"]["MultiAZ"] is False
    assert db_instance["Properties"]["DeletionProtection"] is False
    assert "{{resolve:secretsmanager:" in render_cfn_string(db_instance["Properties"]["MasterUsername"])
    assert "{{resolve:secretsmanager:" in render_cfn_string(db_instance["Properties"]["MasterUserPassword"])
    assert db_instance["Properties"]["VPCSecurityGroups"] == [{"Fn::GetAtt": [db_sg_logical_id, "GroupId"]}]
    assert "SourceDBInstanceIdentifier" not in db_instance["Properties"]

    _, db_subnet_group = single_resource(template, "AWS::RDS::DBSubnetGroup")
    private_subnet_ids = {
        logical_id
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if any(tag.get("Value") == "Private" for tag in resource["Properties"]["Tags"])
    }
    assert {
        subnet_id["Ref"] for subnet_id in db_subnet_group["Properties"]["SubnetIds"]
    } == private_subnet_ids


def test_iam_logs_endpoint_input_and_destroy_semantics_follow_prompt():
    template, template_text, _ = synth_template(
        {
            "AWS_REGION": "eu-west-1",
            "AWS_ENDPOINT": "https://example.invalid",
        }
    )

    _, queue_policy = single_resource(template, "AWS::SQS::QueuePolicy")
    queue_policy_statements = queue_policy["Properties"]["PolicyDocument"]["Statement"]
    rule_logical_id = next(iter(resources_by_type(template, "AWS::Events::Rule")))
    assert any(
        statement["Action"] == "sqs:SendMessage"
        and statement.get("Condition") == {
            "ArnEquals": {
                "aws:SourceArn": {"Fn::GetAtt": [rule_logical_id, "Arn"]}
            }
        }
        for statement in queue_policy_statements
    )

    log_groups = resources_by_type(template, "AWS::Logs::LogGroup")
    assert len(log_groups) == 3
    for resource in log_groups.values():
        assert resource["Properties"]["RetentionInDays"] == 7
        assert "KmsKeyId" not in resource["Properties"]

    for resource in template["Resources"].values():
        for key in ("DeletionPolicy", "UpdateReplacePolicy"):
            if key in resource:
                assert resource[key] == "Delete"
        if resource["Type"] == "AWS::IAM::Policy":
            for statement in resource["Properties"]["PolicyDocument"]["Statement"]:
                assert statement["Action"] != "*"
                assert statement["Resource"] != "*"
    for resource_type in (
        "AWS::RDS::DBInstance",
        "AWS::RDS::DBSubnetGroup",
        "AWS::DynamoDB::Table",
        "AWS::SQS::Queue",
        "AWS::Logs::LogGroup",
        "AWS::SecretsManager::Secret",
    ):
        for resource in resources_by_type(template, resource_type).values():
            for key in ("DeletionPolicy", "UpdateReplacePolicy"):
                if key in resource:
                    assert resource[key] == "Delete"

    roles = resources_by_type(template, "AWS::IAM::Role")
    ingest_role_ref = next(logical_id for logical_id, role in roles.items() if role["Properties"]["RoleName"] == "infra-analysis-ingest-role")
    analyzer_role_ref = next(logical_id for logical_id, role in roles.items() if role["Properties"]["RoleName"] == "infra-analysis-analyzer-role")
    pipe_role_ref = next(logical_id for logical_id, role in roles.items() if role["Properties"]["RoleName"] == "infra-analysis-pipe-role")
    sfn_role_ref = next(logical_id for logical_id, role in roles.items() if role["Properties"]["RoleName"] == "infra-analysis-sfn-role")
    task_role_ref = next(
        logical_id
        for logical_id, role in roles.items()
        if role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {"Service": "ecs-tasks.amazonaws.com"}
    )
    task_definition = next(iter(resources_by_type(template, "AWS::ECS::TaskDefinition").values()))
    execution_role_ref = task_definition["Properties"]["ExecutionRoleArn"]["Fn::GetAtt"][0]
    queue_logical_id = next(iter(resources_by_type(template, "AWS::SQS::Queue")))
    event_bus_logical_id = next(iter(resources_by_type(template, "AWS::Events::EventBus")))
    table_logical_id = next(iter(resources_by_type(template, "AWS::DynamoDB::Table")))
    ingest_log_group_logical_id = next(
        logical_id for logical_id, resource in resources_by_type(template, "AWS::Logs::LogGroup").items()
        if resource["Properties"]["LogGroupName"] == "/aws/lambda/infra-analysis-ingest"
    )
    analyzer_log_group_logical_id = next(
        logical_id for logical_id, resource in resources_by_type(template, "AWS::Logs::LogGroup").items()
        if resource["Properties"]["LogGroupName"] == "/aws/lambda/infra-analysis-analyzer"
    )
    analyzer_function_logical_id = next(
        logical_id for logical_id, resource in resources_by_type(template, "AWS::Lambda::Function").items()
        if resource["Properties"]["FunctionName"] == "infra-analysis-analyzer"
    )
    state_machine_logical_id = next(iter(resources_by_type(template, "AWS::StepFunctions::StateMachine")))

    ingest_statements = policy_for_role(template, {"Ref": ingest_role_ref})
    assert any(
        normalize_actions(statement) == {"sqs:SendMessage"}
        and normalize_resources(statement) == [{"Fn::GetAtt": [queue_logical_id, "Arn"]}]
        for statement in ingest_statements
    )
    assert any(
        normalize_actions(statement) == {"events:PutEvents"}
        and normalize_resources(statement) == [{"Fn::GetAtt": [event_bus_logical_id, "Arn"]}]
        for statement in ingest_statements
    )
    assert any(
        normalize_actions(statement) == {"logs:CreateLogStream", "logs:PutLogEvents"}
        and normalize_resources(statement) == [
            {"Fn::GetAtt": [ingest_log_group_logical_id, "Arn"]},
            {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": [ingest_log_group_logical_id, "Arn"]}, ":*"],
                ]
            },
        ]
        for statement in ingest_statements
    )

    analyzer_statements = policy_for_role(template, {"Ref": analyzer_role_ref})
    assert any(
        normalize_actions(statement) == {"dynamodb:PutItem"}
        and normalize_resources(statement) == [{"Fn::GetAtt": [table_logical_id, "Arn"]}]
        for statement in analyzer_statements
    )
    assert any(
        normalize_actions(statement) == {"logs:CreateLogStream", "logs:PutLogEvents"}
        and normalize_resources(statement) == [
            {"Fn::GetAtt": [analyzer_log_group_logical_id, "Arn"]},
            {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": [analyzer_log_group_logical_id, "Arn"]}, ":*"],
                ]
            },
        ]
        for statement in analyzer_statements
    )

    pipe_statements = policy_for_role(template, {"Ref": pipe_role_ref})
    assert any(
        {"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"}.issubset(normalize_actions(statement))
        and normalize_resources(statement) == [{"Fn::GetAtt": [queue_logical_id, "Arn"]}]
        for statement in pipe_statements
    )
    assert any(
        normalize_actions(statement) == {"lambda:InvokeFunction"}
        and normalize_resources(statement) == [{"Fn::GetAtt": [analyzer_function_logical_id, "Arn"]}]
        for statement in pipe_statements
    )
    assert any(
        normalize_actions(statement) == {"states:StartExecution"}
        and normalize_resources(statement) == [{"Ref": state_machine_logical_id}]
        for statement in pipe_statements
    )

    sfn_statements = policy_for_role(template, {"Ref": sfn_role_ref})
    assert any(
        "lambda:InvokeFunction" in normalize_actions(statement)
        and {"Fn::GetAtt": [analyzer_function_logical_id, "Arn"]} in normalize_resources(statement)
        for statement in sfn_statements
    )

    task_role_statements = policy_for_role(template, {"Ref": task_role_ref})
    assert any(
        "sqs:SendMessage" in normalize_actions(statement)
        and {"Fn::GetAtt": [queue_logical_id, "Arn"]} in normalize_resources(statement)
        for statement in task_role_statements
    )
    assert any(
        "events:PutEvents" in normalize_actions(statement)
        and {"Fn::GetAtt": [event_bus_logical_id, "Arn"]} in normalize_resources(statement)
        for statement in task_role_statements
    )

    execution_role = roles[execution_role_ref]
    assert execution_role_ref != task_role_ref
    execution_role_statements = policy_for_role(template, {"Ref": execution_role_ref})
    assert any(
        "secretsmanager:GetSecretValue" in normalize_actions(statement)
        for statement in execution_role_statements
    )

    lambda_functions = resources_by_type(template, "AWS::Lambda::Function")
    for resource in lambda_functions.values():
        assert resource["Properties"]["Environment"]["Variables"]["APP_REGION"] == "eu-west-1"
        assert resource["Properties"]["Environment"]["Variables"]["AWS_ENDPOINT"] == "https://example.invalid"
        assert "AKIA" not in resource["Properties"]["Code"]["ZipFile"]
        assert "SECRET_ACCESS_KEY" not in resource["Properties"]["Code"]["ZipFile"]

    captured_calls = []
    fake_boto3 = types.ModuleType("boto3")

    class FakeSqsClient:
        def send_message(self, QueueUrl, MessageBody):
            captured_calls.append(("send_message", QueueUrl, MessageBody))
            return {"MessageId": "msg-123"}

    class FakeEventsClient:
        def put_events(self, Entries):
            captured_calls.append(("put_events", Entries))
            return {"Entries": [{"EventId": "evt-123"}]}

    def fake_client(service_name, **kwargs):
        captured_calls.append(("client", service_name, kwargs))
        if service_name == "sqs":
            return FakeSqsClient()
        if service_name == "events":
            return FakeEventsClient()
        return object()

    class FakeTable:
        def put_item(self, Item):
            captured_calls.append(("put_item", Item))

    class FakeDynamoResource:
        def Table(self, table_name):
            captured_calls.append(("table", table_name))
            return FakeTable()

    def fake_resource(service_name, **kwargs):
        captured_calls.append(("resource", service_name, kwargs))
        if service_name == "dynamodb":
            return FakeDynamoResource()
        return object()

    fake_boto3.client = fake_client
    fake_boto3.resource = fake_resource

    ingest_code = next(
        resource["Properties"]["Code"]["ZipFile"]
        for resource in lambda_functions.values()
        if resource["Properties"]["FunctionName"] == "infra-analysis-ingest"
    )
    ingest_module = load_inline_lambda_module(
        ingest_code,
        {
            "APP_REGION": "eu-west-1",
            "AWS_ENDPOINT": "https://example.invalid",
            "QUEUE_URL": "https://queue.example.invalid/123/test",
            "EVENT_BUS_NAME": "infra-analysis-bus",
            "INGEST_SOURCE": "infrastructure-analysis.ingest",
            "INGEST_DETAIL_TYPE": "IngestAccepted",
        },
        fake_boto3,
    )
    response = ingest_module["lambda_handler"](
        {"body": json.dumps({"hello": "world"})},
        types.SimpleNamespace(aws_request_id="req-123"),
    )
    assert response["statusCode"] == 200
    assert any(
        call[0] == "put_events"
        for call in captured_calls
    )
    assert any(
        call[0] == "send_message"
        for call in captured_calls
    )

    analyzer_code = next(
        resource["Properties"]["Code"]["ZipFile"]
        for resource in lambda_functions.values()
        if resource["Properties"]["FunctionName"] == "infra-analysis-analyzer"
    )
    analyzer_module = load_inline_lambda_module(
        analyzer_code,
        {
            "APP_REGION": "eu-west-1",
            "AWS_ENDPOINT": "https://example.invalid",
            "TABLE_NAME": "infra-analysis-ledger",
        },
        fake_boto3,
    )
    logged_messages = []
    previous_print = builtins.print
    try:
        builtins.print = lambda *args, **kwargs: logged_messages.append(" ".join(str(arg) for arg in args))
        analyzer_module["lambda_handler"](
            {
                "Records": [
                    {
                        "body": json.dumps({"event_bridge_event_id": "evt-123", "payload": {"ok": True}}),
                        "messageId": "msg-123",
                    }
                ]
            },
            None,
        )
    finally:
        builtins.print = previous_print
    assert any(
        call[0] == "put_item"
        for call in captured_calls
    )
    written_item = next(call[1] for call in captured_calls if call[0] == "put_item")
    analyzer_logs = [json.loads(message) for message in logged_messages if message.startswith("{")]
    assert any(
        log["sqs_message_id"] == "msg-123"
        and log["event_bridge_event_id"] == "evt-123"
        and log["dynamodb_item_key"] == written_item["pk"]
        for log in analyzer_logs
    )

    backend_container = task_definition["Properties"]["ContainerDefinitions"][0]
    assert any(
        env_var["Name"] == "AWS_ENDPOINT" and env_var["Value"] == "https://example.invalid"
        for env_var in backend_container["Environment"]
    )
    assert any(
        env_var["Name"] == "TELEMETRY_QUEUE_URL"
        for env_var in backend_container["Environment"]
    )
    assert any(
        env_var["Name"] == "TELEMETRY_EVENT_BUS_NAME"
        for env_var in backend_container["Environment"]
    )
    assert not any(env_var["Name"] in {"DB_USERNAME", "DB_PASSWORD"} for env_var in backend_container["Environment"])
    assert "AWS_SECRET_ACCESS_KEY" not in template_text
    assert "AWS_ACCESS_KEY_ID" not in template_text


def test_default_region_is_us_east_1_when_not_provided():
    template, _, _ = synth_template(clear_region=True)
    lambda_functions = resources_by_type(template, "AWS::Lambda::Function")
    for resource in lambda_functions.values():
        assert resource["Properties"]["Environment"]["Variables"]["APP_REGION"] == "us-east-1"
    task_definition = next(iter(resources_by_type(template, "AWS::ECS::TaskDefinition").values()))
    assert any(
        env_var["Name"] == "APP_REGION" and env_var["Value"] == "us-east-1"
        for env_var in task_definition["Properties"]["ContainerDefinitions"][0]["Environment"]
    )


def test_sdk_context_contract_reads_credentials_and_stack_region_from_inputs():
    configure_sdk_context = load_configure_sdk_context()

    class FakeNode:
        def __init__(self, values):
            self.values = values

        def try_get_context(self, key):
            return self.values.get(key)

    class FakeApp:
        def __init__(self, values):
            self.node = FakeNode(values)

    previous_env = os.environ.copy()
    try:
        for key in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            os.environ.pop(key, None)
        sdk_context = configure_sdk_context(
            FakeApp(
                {
                    "AWS_REGION": "eu-west-1",
                    "AWS_ENDPOINT": "https://example.invalid",
                    "AWS_ACCESS_KEY_ID": "INPUT_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY": "INPUT_SECRET_ACCESS_KEY",
                }
            )
        )
    finally:
        os.environ.clear()
        os.environ.update(previous_env)

    assert sdk_context == {
        "AWS_REGION": "eu-west-1",
        "AWS_ENDPOINT": "https://example.invalid",
        "AWS_ACCESS_KEY_ID": "INPUT_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY": "INPUT_SECRET_ACCESS_KEY",
    }

    previous_env = os.environ.copy()
    try:
        for key in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            os.environ.pop(key, None)
        configure_sdk_context(
            FakeApp(
                {
                    "AWS_REGION": "eu-west-1",
                    "AWS_ACCESS_KEY_ID": "INPUT_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY": "INPUT_SECRET_ACCESS_KEY",
                }
            )
        )
        assert os.environ["AWS_REGION"] == "eu-west-1"
        assert os.environ["AWS_DEFAULT_REGION"] == "eu-west-1"
        assert os.environ["AWS_ACCESS_KEY_ID"] == "INPUT_ACCESS_KEY_ID"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "INPUT_SECRET_ACCESS_KEY"
    finally:
        os.environ.clear()
        os.environ.update(previous_env)

    stack_call = find_stack_constructor_call()
    env_keyword = next(keyword for keyword in stack_call.keywords if keyword.arg == "env")
    assert isinstance(env_keyword.value, ast.Call)
    assert isinstance(env_keyword.value.func, ast.Name)
    assert env_keyword.value.func.id == "Environment"
    region_keyword = next(keyword for keyword in env_keyword.value.keywords if keyword.arg == "region")
    assert isinstance(region_keyword.value, ast.Subscript)
    assert isinstance(region_keyword.value.value, ast.Name)
    assert region_keyword.value.value.id == "sdk_context"
    region_slice = region_keyword.value.slice
    assert isinstance(region_slice, ast.Constant)
    assert region_slice.value == "AWS_REGION"
