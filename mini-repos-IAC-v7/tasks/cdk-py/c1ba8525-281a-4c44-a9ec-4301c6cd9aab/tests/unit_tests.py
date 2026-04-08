import ast
import inspect
import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"
APP_SOURCE = APP_PATH.read_text()
APP_SPEC = spec_from_file_location("app", APP_PATH)
APP_MODULE = module_from_spec(APP_SPEC)
sys.modules["app"] = APP_MODULE
assert APP_SPEC and APP_SPEC.loader
APP_SPEC.loader.exec_module(APP_MODULE)
PocStack = APP_MODULE.PocStack


# ── Synthesis helpers ─────────────────────────────────────────────────────────


def synth_template(**stack_kwargs) -> dict:
    app = cdk.App()
    stack = PocStack(app, "TestPocStack", **stack_kwargs)
    return Template.from_stack(stack).to_json()


def resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def single_resource(template: dict, resource_type: str) -> tuple:
    resources = resources_by_type(template, resource_type)
    assert len(resources) == 1, f"Expected exactly 1 {resource_type}, found {len(resources)}"
    return next(iter(resources.items()))


def sg_refs_by_description(template: dict) -> dict:
    refs = {}
    for logical_id, resource in resources_by_type(template, "AWS::EC2::SecurityGroup").items():
        description = resource["Properties"]["GroupDescription"]
        refs[description] = {"Fn::GetAtt": [logical_id, "GroupId"]}
    return refs


def collect_sg_rules(template: dict, direction: str) -> list:
    property_name = "SecurityGroupIngress" if direction == "ingress" else "SecurityGroupEgress"
    resource_type = (
        "AWS::EC2::SecurityGroupIngress"
        if direction == "ingress"
        else "AWS::EC2::SecurityGroupEgress"
    )

    rules = []
    for logical_id, resource in resources_by_type(template, "AWS::EC2::SecurityGroup").items():
        for rule in resource["Properties"].get(property_name, []):
            hydrated_rule = dict(rule)
            hydrated_rule["GroupId"] = {"Fn::GetAtt": [logical_id, "GroupId"]}
            rules.append(hydrated_rule)

    for resource in resources_by_type(template, resource_type).values():
        rules.append(resource["Properties"])

    return rules


def collect_env_input_names_from_ast() -> set:
    parsed = ast.parse(APP_SOURCE)
    names = set()

    for node in ast.walk(parsed):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "_read_input":
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    names.add(node.args[0].value)
            elif isinstance(node.func, ast.Attribute):
                if (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "os"
                    and node.func.attr == "getenv"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    names.add(node.args[0].value)
                elif (
                    isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == "os"
                    and node.func.value.attr == "environ"
                    and node.func.attr == "get"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    names.add(node.args[0].value)

        if isinstance(node, ast.Subscript):
            if (
                isinstance(node.value, ast.Attribute)
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "os"
                and node.value.attr == "environ"
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
            ):
                names.add(node.slice.value)

    return names


def private_subnet_refs(template: dict) -> list:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if not resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def public_subnet_refs(template: dict) -> list:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def policy_documents(template: dict) -> list:
    return [
        resource["Properties"]["PolicyDocument"]
        for resource in resources_by_type(template, "AWS::IAM::Policy").values()
    ]


def lambda_resources_by_shape(template: dict) -> dict:
    lambdas = list(resources_by_type(template, "AWS::Lambda::Function").values())
    backend = next(
        resource
        for resource in lambdas
        if resource["Properties"].get("ReservedConcurrentExecutions") == 20
    )
    processor = next(
        resource
        for resource in lambdas
        if resource["Properties"]["MemorySize"] == 512
    )
    return {"backend": backend, "processor": processor}


def attached_policy_for_role(template: dict, role_logical_id: str) -> dict:
    for resource in resources_by_type(template, "AWS::IAM::Policy").values():
        if resource["Properties"]["Roles"] == [{"Ref": role_logical_id}]:
            return resource["Properties"]["PolicyDocument"]
    raise AssertionError(f"No IAM policy attached to role {role_logical_id}")


def log_statement_from_policy(policy_document: dict) -> dict:
    for statement in policy_document["Statement"]:
        actions = statement["Action"]
        if actions == ["logs:CreateLogStream", "logs:PutLogEvents"]:
            return statement
    raise AssertionError("No logs statement found in policy")


def statement_by_actions(policy_document: dict, actions: list) -> dict:
    for statement in policy_document["Statement"]:
        if statement["Action"] == actions:
            return statement
    raise AssertionError(f"No statement found for actions {actions}")


def log_group_logical_id_by_name(template: dict, log_group_name: str) -> str:
    for logical_id, resource in resources_by_type(template, "AWS::Logs::LogGroup").items():
        if resource["Properties"]["LogGroupName"] == log_group_name:
            return logical_id
    raise AssertionError(f"Log group {log_group_name} not found")


def log_group_logical_id_for_lambda(template: dict, lambda_resource: dict) -> str:
    function_name = lambda_resource["Properties"]["FunctionName"]
    return log_group_logical_id_by_name(template, f"/aws/lambda/{function_name}")


# ── Input contract ────────────────────────────────────────────────────────────


def test_input_contract_and_sdk_configuration(monkeypatch):
    signature = inspect.signature(PocStack.__init__)
    custom_params = {
        name
        for name, parameter in signature.parameters.items()
        if name not in {"self", "scope", "construct_id", "kwargs"}
    }

    assert custom_params <= {
        "aws_endpoint",
        "aws_region",
        "aws_access_key_id",
        "aws_secret_access_key",
    }
    assert "aws_region" in custom_params
    assert signature.parameters["aws_region"].default == "us-east-1"

    # Verify the real stack synthesises correctly with a non-default region
    real_stack = PocStack(cdk.App(), "InputContractStack", aws_region="us-west-2")
    assert real_stack.termination_protection is False
    real_template = Template.from_stack(real_stack).to_json()
    assert real_template.get("Resources"), "Synthesis must produce CloudFormation resources"

    # Verify main() reads aws_region from environment and wires it through to PocStack
    captured: dict = {}

    class DummyApp:
        def synth(self) -> None:
            captured["synth_called"] = True

    def fake_stack(scope, construct_id, **kwargs):
        captured["construct_id"] = construct_id
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.delenv("aws_region", raising=False)
    monkeypatch.setattr(APP_MODULE.cdk, "App", DummyApp)
    monkeypatch.setattr(APP_MODULE, "PocStack", fake_stack)
    APP_MODULE.main()

    assert captured["construct_id"] == "PocStack"
    assert captured["kwargs"]["aws_region"] == "us-east-1"
    assert captured["kwargs"]["env"].region == "us-east-1"
    assert captured["synth_called"] is True


def test_only_declared_inputs_are_read_by_the_app():
    parsed = ast.parse(APP_SOURCE)
    read_input_names = collect_env_input_names_from_ast()
    assert read_input_names <= {
        "aws_endpoint",
        "aws_region",
        "aws_access_key_id",
        "aws_secret_access_key",
    }
    assert "aws_region" in read_input_names
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"get_context", "try_get_context"}
        for node in ast.walk(parsed)
    )


# ── Negative path tests ───────────────────────────────────────────────────────


def test_stack_synthesises_without_optional_env_vars(monkeypatch):
    """Synthesis must succeed when only the mandatory aws_region default is used."""
    monkeypatch.delenv("aws_region", raising=False)
    monkeypatch.delenv("aws_endpoint", raising=False)
    template = synth_template()  # aws_region defaults to "us-east-1"
    assert resources_by_type(template, "AWS::EC2::VPC"), "VPC must exist even with default region"


def test_no_kms_keys_anywhere_in_template():
    """No KMS key resource should appear — the spec forbids KMS on log groups."""
    template = synth_template(aws_region="us-east-1")
    kms_keys = resources_by_type(template, "AWS::KMS::Key")
    assert len(kms_keys) == 0, f"Unexpected KMS keys: {list(kms_keys.keys())}"


def test_rds_credentials_use_secrets_manager_dynamic_reference():
    """MasterUsername and MasterUserPassword must use {{resolve:secretsmanager:…}}, not plaintext."""
    template = synth_template(aws_region="us-east-1")
    _, db_instance = single_resource(template, "AWS::RDS::DBInstance")
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    master_username = str(db_instance["Properties"]["MasterUsername"])
    master_password = str(db_instance["Properties"]["MasterUserPassword"])
    assert "{{resolve:secretsmanager:" in master_username, (
        "MasterUsername must use a CloudFormation dynamic reference, not plaintext"
    )
    assert "{{resolve:secretsmanager:" in master_password, (
        "MasterUserPassword must use a CloudFormation dynamic reference, not plaintext"
    )
    assert secret_logical_id in master_username
    assert secret_logical_id in master_password


def test_no_publicly_accessible_rds_and_no_deletion_protection():
    """RDS must not be publicly accessible and must not have deletion protection enabled."""
    template = synth_template(aws_region="us-east-1")
    _, db_instance = single_resource(template, "AWS::RDS::DBInstance")
    assert db_instance["Properties"]["PubliclyAccessible"] is False
    assert db_instance["Properties"]["DeletionProtection"] is False


# ── Network topology ──────────────────────────────────────────────────────────


def test_network_topology_az_spread_and_endpoint_wiring():
    template = synth_template(aws_region="us-east-1")

    vpcs = resources_by_type(template, "AWS::EC2::VPC")
    subnets = resources_by_type(template, "AWS::EC2::Subnet")
    nat_gateways = resources_by_type(template, "AWS::EC2::NatGateway")
    routes = resources_by_type(template, "AWS::EC2::Route")
    associations = resources_by_type(template, "AWS::EC2::SubnetRouteTableAssociation")
    endpoints = resources_by_type(template, "AWS::EC2::VPCEndpoint")

    assert len(vpcs) == 1
    assert len(subnets) == 4
    assert len(nat_gateways) == 1
    assert len(endpoints) == 1
    assert next(iter(vpcs.values()))["Properties"]["CidrBlock"] == "10.20.0.0/16"

    subnet_items = list(subnets.items())
    public_subnets = [
        (logical_id, subnet)
        for logical_id, subnet in subnet_items
        if subnet["Properties"]["MapPublicIpOnLaunch"]
    ]
    private_subnets = [
        (logical_id, subnet)
        for logical_id, subnet in subnet_items
        if not subnet["Properties"]["MapPublicIpOnLaunch"]
    ]

    assert len(public_subnets) == 2
    assert len(private_subnets) == 2

    public_azs = {
        str(subnet["Properties"]["AvailabilityZone"]) for _, subnet in public_subnets
    }
    private_azs = {
        str(subnet["Properties"]["AvailabilityZone"]) for _, subnet in private_subnets
    }
    assert len(public_azs) == 2
    assert len(private_azs) == 2
    public_subnet_ids = {logical_id for logical_id, _ in public_subnets}

    private_route_tables = {
        str(association["Properties"]["RouteTableId"])
        for association in associations.values()
        if association["Properties"]["SubnetId"]["Ref"]
        in {logical_id for logical_id, _ in private_subnets}
    }
    nat_gateway_ref = {"Ref": next(iter(nat_gateways.keys()))}

    endpoint = next(iter(endpoints.values()))
    assert endpoint["Properties"]["VpcEndpointType"] == "Gateway"
    # Verify this is the S3 gateway endpoint, not some other gateway
    assert "s3" in str(endpoint["Properties"]["ServiceName"]).lower()
    assert {
        str(route_table_id) for route_table_id in endpoint["Properties"]["RouteTableIds"]
    } == private_route_tables
    private_default_routes = [
        route["Properties"]
        for route in routes.values()
        if route["Properties"].get("DestinationCidrBlock") == "0.0.0.0/0"
        and route["Properties"].get("NatGatewayId") == nat_gateway_ref
    ]
    assert len(private_default_routes) == 2
    assert {
        str(route["RouteTableId"]) for route in private_default_routes
    } == private_route_tables
    assert all(
        nat_gateway["Properties"]["SubnetId"]["Ref"] in public_subnet_ids
        for nat_gateway in nat_gateways.values()
    )


def test_security_group_rules_cover_required_flows():
    template = synth_template(aws_region="us-east-1")
    sg_refs = sg_refs_by_description(template)
    ingress_rules = collect_sg_rules(template, "ingress")
    egress_rules = collect_sg_rules(template, "egress")

    assert len(sg_refs) == 3
    frontend_sg = sg_refs["Frontend ALB security group"]
    backend_sg = sg_refs["Backend service security group"]
    db_sg = sg_refs["Database security group"]

    assert any(
        rule["GroupId"] == frontend_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 80
        and rule["ToPort"] == 80
        and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in ingress_rules
    )
    assert any(
        rule["GroupId"] == frontend_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 3000
        and rule["ToPort"] == 3000
        and rule.get("DestinationSecurityGroupId") == backend_sg
        for rule in egress_rules
    )
    assert any(
        rule["GroupId"] == backend_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 3000
        and rule["ToPort"] == 3000
        and rule.get("SourceSecurityGroupId") == frontend_sg
        for rule in ingress_rules
    )
    backend_ingress_rules = [rule for rule in ingress_rules if rule["GroupId"] == backend_sg]
    assert len(backend_ingress_rules) == 1
    assert any(
        rule["GroupId"] == backend_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 5432
        and rule["ToPort"] == 5432
        and rule.get("DestinationSecurityGroupId") == db_sg
        for rule in egress_rules
    )
    assert any(
        rule["GroupId"] == backend_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 443
        and rule["ToPort"] == 443
        and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in egress_rules
    )
    backend_egress_rules = [rule for rule in egress_rules if rule["GroupId"] == backend_sg]
    assert len(backend_egress_rules) == 2
    assert any(
        rule["GroupId"] == db_sg
        and rule["IpProtocol"] == "tcp"
        and rule["FromPort"] == 5432
        and rule["ToPort"] == 5432
        and rule.get("SourceSecurityGroupId") == backend_sg
        for rule in ingress_rules
    )
    db_ingress_rules = [rule for rule in ingress_rules if rule["GroupId"] == db_sg]
    assert len(db_ingress_rules) == 1
    # Database SG must allow all outbound traffic (spec requirement)
    assert any(
        rule["GroupId"] == db_sg
        and rule["IpProtocol"] == "-1"
        and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in egress_rules
    )


def test_no_retention_policies_and_database_teardown_settings():
    template = synth_template(aws_region="us-east-1")

    for resource in template["Resources"].values():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}

    db_instance = next(iter(resources_by_type(template, "AWS::RDS::DBInstance").values()))
    analytics_bucket = next(iter(resources_by_type(template, "AWS::S3::Bucket").values()))
    assert db_instance["Properties"]["BackupRetentionPeriod"] == 0
    assert db_instance["Properties"]["DeletionProtection"] is False
    assert db_instance["Properties"]["DeleteAutomatedBackups"] is True
    assert analytics_bucket["DeletionPolicy"] == "Delete"
    assert analytics_bucket["UpdateReplacePolicy"] == "Delete"


def test_log_groups_have_retention_without_kms_keys():
    template = synth_template(aws_region="us-east-1")
    log_groups = list(resources_by_type(template, "AWS::Logs::LogGroup").values())

    # Exactly 4: backend Lambda, event processor Lambda, API Gateway access, ECS frontend
    assert len(log_groups) == 4
    assert all(log_group["Properties"]["RetentionInDays"] == 7 for log_group in log_groups)
    assert all("KmsKeyId" not in log_group["Properties"] for log_group in log_groups)


def test_cluster_and_output_contract():
    template = synth_template(aws_region="us-east-1")

    assert len(resources_by_type(template, "AWS::ECS::Cluster")) == 1
    outputs = template["Outputs"]
    assert "FrontendAlbDns" in outputs
    assert outputs["FrontendAlbDns"]["Export"]["Name"] == "FrontendAlbDns"
    assert outputs["FrontendAlbDns"]["Value"]["Fn::GetAtt"][1] == "DNSName"


def test_single_app_file_and_inline_delivery_shape():
    root_python_files = sorted(path.name for path in APP_PATH.parent.glob("*.py"))
    assert root_python_files == ["app.py"]


def test_no_manual_prerequisites_are_modeled_in_template():
    template = synth_template(aws_region="us-east-1")

    parameter_names = set(template.get("Parameters", {}).keys())
    assert parameter_names <= {"BootstrapVersion"}
    assert not resources_by_type(template, "AWS::SSM::Parameter")
    assert "Fn::ImportValue" not in str(template)


# ── Frontend / API Gateway contract ──────────────────────────────────────────


def test_frontend_and_api_gateway_contract():
    template = synth_template(aws_region="us-east-1")
    sg_refs = sg_refs_by_description(template)

    cluster_logical_id, _ = single_resource(template, "AWS::ECS::Cluster")
    _, load_balancer = single_resource(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    _, listener = single_resource(template, "AWS::ElasticLoadBalancingV2::Listener")
    _, target_group = single_resource(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
    _, service = single_resource(template, "AWS::ECS::Service")
    _, task_definition = single_resource(template, "AWS::ECS::TaskDefinition")
    rest_api_logical_id, rest_api = single_resource(template, "AWS::ApiGateway::RestApi")
    stage_logical_id, stage = single_resource(template, "AWS::ApiGateway::Stage")
    target_group_logical_id, _ = single_resource(template, "AWS::ElasticLoadBalancingV2::TargetGroup")

    assert load_balancer["Properties"]["Scheme"] == "internet-facing"
    assert load_balancer["Properties"]["Type"] == "application"
    assert sorted(str(subnet_id) for subnet_id in load_balancer["Properties"]["Subnets"]) == sorted(
        str(subnet_id) for subnet_id in public_subnet_refs(template)
    )
    assert listener["Properties"]["Port"] == 80
    assert listener["Properties"]["Protocol"] == "HTTP"
    assert target_group["Properties"]["Port"] == 80
    assert target_group["Properties"]["TargetType"] == "ip"
    assert target_group["Properties"]["HealthCheckPath"] == "/"
    assert target_group["Properties"]["Matcher"]["HttpCode"] == "200-399"

    awsvpc = service["Properties"]["NetworkConfiguration"]["AwsvpcConfiguration"]
    assert service["Properties"]["DesiredCount"] == 2
    assert service["Properties"]["LaunchType"] == "FARGATE"
    assert service["Properties"]["Cluster"] == {"Ref": cluster_logical_id}
    assert awsvpc["AssignPublicIp"] == "DISABLED"
    assert sg_refs["Frontend ALB security group"] in awsvpc["SecurityGroups"]
    assert sorted(str(subnet_id) for subnet_id in awsvpc["Subnets"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )

    container = task_definition["Properties"]["ContainerDefinitions"][0]
    assert task_definition["Properties"]["Cpu"] == "512"
    assert task_definition["Properties"]["Memory"] == "1024"
    assert isinstance(container["Image"], str)
    assert container["Image"]
    assert (
        container["Image"].startswith("public.ecr.aws/")
        or re.match(r"^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9._-]+)?$", container["Image"])
    )
    assert not re.match(r"^\d+\.dkr\.ecr\.[^.]+\.amazonaws\.com/", container["Image"])
    assert container["PortMappings"] == [{"ContainerPort": 80, "Protocol": "tcp"}]
    frontend_log_group_logical_id = container["LogConfiguration"]["Options"]["awslogs-group"]["Ref"]
    frontend_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[frontend_log_group_logical_id]
    assert frontend_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in frontend_log_group["Properties"]

    env_vars = {item["Name"]: item["Value"] for item in container["Environment"]}
    assert env_vars["BACKEND_API_BASE_URL"] == {
        "Fn::Join": [
            "",
            [
                "https://",
                {"Ref": rest_api_logical_id},
                ".execute-api.",
                {"Ref": "AWS::Region"},
                ".",
                {"Ref": "AWS::URLSuffix"},
                "/",
                {"Ref": stage_logical_id},
                "/",
            ],
        ]
    }

    assert rest_api["Properties"]["EndpointConfiguration"]["Types"] == ["REGIONAL"]
    assert any(
        setting["HttpMethod"] == "*"
        and setting["ResourcePath"] == "/*"
        and setting["LoggingLevel"] == "INFO"
        for setting in stage["Properties"]["MethodSettings"]
    )
    assert stage["Properties"]["AccessLogSetting"]["DestinationArn"]

    api_resources = resources_by_type(template, "AWS::ApiGateway::Resource")
    path_parts = {resource["Properties"]["PathPart"] for resource in api_resources.values()}
    assert path_parts == {"health", "items"}

    health_resource_id = next(
        logical_id for logical_id, resource in api_resources.items()
        if resource["Properties"]["PathPart"] == "health"
    )
    items_resource_id = next(
        logical_id for logical_id, resource in api_resources.items()
        if resource["Properties"]["PathPart"] == "items"
    )
    methods = list(resources_by_type(template, "AWS::ApiGateway::Method").values())
    assert any(
        method["Properties"]["HttpMethod"] == "GET"
        and method["Properties"]["ResourceId"] == {"Ref": health_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert any(
        method["Properties"]["HttpMethod"] == "GET"
        and method["Properties"]["ResourceId"] == {"Ref": items_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert any(
        method["Properties"]["HttpMethod"] == "POST"
        and method["Properties"]["ResourceId"] == {"Ref": items_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert listener["Properties"]["DefaultActions"] == [
        {"TargetGroupArn": {"Ref": target_group_logical_id}, "Type": "forward"}
    ]
    assert any(
        lb["ContainerPort"] == 80 and lb["TargetGroupArn"] == {"Ref": target_group_logical_id}
        for lb in service["Properties"]["LoadBalancers"]
    )

    access_log_destination = stage["Properties"]["AccessLogSetting"]["DestinationArn"]
    access_log_group_logical_id = access_log_destination["Fn::GetAtt"][0]
    access_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[access_log_group_logical_id]
    assert access_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in access_log_group["Properties"]


# ── Backend Lambda ────────────────────────────────────────────────────────────


def test_backend_lambda_code_and_vpc_configuration():
    template = synth_template(aws_region="us-east-1")
    lambdas = lambda_resources_by_shape(template)
    sg_refs = sg_refs_by_description(template)
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")

    backend = lambdas["backend"]
    code = str(backend["Properties"]["Code"]["ZipFile"])
    backend_log_group_logical_id = log_group_logical_id_for_lambda(template, backend)
    backend_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[backend_log_group_logical_id]

    assert backend["Properties"]["Runtime"] == "python3.12"
    assert backend["Properties"]["Architectures"] == ["arm64"]
    assert backend["Properties"]["MemorySize"] == 1024
    assert backend["Properties"]["Timeout"] == 10
    assert backend["Properties"]["ReservedConcurrentExecutions"] == 20
    assert backend["Properties"]["VpcConfig"]["SecurityGroupIds"] == [
        sg_refs["Backend service security group"]
    ]
    assert sorted(str(subnet_id) for subnet_id in backend["Properties"]["VpcConfig"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    assert backend_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in backend_log_group["Properties"]

    env_vars = backend["Properties"]["Environment"]["Variables"]
    assert env_vars["DB_NAME"] == "appdb"
    assert env_vars["EVENTS_SOURCE"] == "app.backend"
    assert env_vars["EVENTS_DETAIL_TYPE"] == "item.created"
    assert env_vars["DB_SECRET_ARN"] == {"Ref": secret_logical_id}
    assert env_vars["QUEUE_URL"] == {"Ref": queue_logical_id}

    assert 'path.endswith("/health")' in code
    assert '{"ok": True}' in code
    assert 'path.endswith("/items")' in code
    assert 'boto3.client("secretsmanager")' in code
    assert 'get_secret_value(SecretId=secret_arn)' in code
    assert 'os.environ["DB_SECRET_ARN"]' in code
    assert 'credentials["username"]' in code
    assert 'credentials["password"]' in code
    assert "LIMIT 20" in code
    assert "INSERT INTO" in code
    assert "RETURNING id" in code
    assert 'boto3.client("events").put_events' in code
    assert '"Source": os.environ["EVENTS_SOURCE"]' in code
    assert '"DetailType": os.environ["EVENTS_DETAIL_TYPE"]' in code
    assert re.search(r'"Detail"\s*:\s*json\.dumps\(\{\s*"id"\s*:', code)


# ── Persistence / eventing / Glue ─────────────────────────────────────────────


def test_persistence_eventing_and_data_catalog_contract():
    template = synth_template(aws_region="us-east-1")
    sg_refs = sg_refs_by_description(template)
    lambdas = lambda_resources_by_shape(template)

    queue_logical_id, queue = single_resource(template, "AWS::SQS::Queue")
    _, rule = single_resource(template, "AWS::Events::Rule")
    _, event_source_mapping = single_resource(template, "AWS::Lambda::EventSourceMapping")
    _, bucket = single_resource(template, "AWS::S3::Bucket")
    _, glue_database = single_resource(template, "AWS::Glue::Database")
    _, crawler = single_resource(template, "AWS::Glue::Crawler")
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    _, db_subnet_group = single_resource(template, "AWS::RDS::DBSubnetGroup")
    _, db_instance = single_resource(template, "AWS::RDS::DBInstance")

    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert queue["Properties"]["MessageRetentionPeriod"] == 345600
    assert rule["Properties"]["EventPattern"] == {
        "detail-type": ["item.created"],
        "source": ["app.backend"],
    }
    assert len(rule["Properties"]["Targets"]) == 1
    assert rule["Properties"]["Targets"][0]["Arn"] == {"Fn::GetAtt": [queue_logical_id, "Arn"]}
    assert event_source_mapping["Properties"]["BatchSize"] == 10
    assert event_source_mapping["Properties"]["EventSourceArn"] == {
        "Fn::GetAtt": [queue_logical_id, "Arn"]
    }

    assert bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert "VersioningConfiguration" not in bucket["Properties"]

    assert "Schedule" not in crawler["Properties"]
    assert len(crawler["Properties"]["Targets"]["S3Targets"]) == 1
    glue_database_name = glue_database["Properties"]["DatabaseInput"]["Name"]
    crawler_name = crawler["Properties"]["Name"]
    assert crawler["Properties"]["DatabaseName"] == glue_database_name
    assert "/data/" in str(crawler["Properties"]["Targets"]["S3Targets"][0]["Path"])

    assert db_instance["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["Properties"]["AllocatedStorage"] == "20"
    assert db_instance["Properties"]["StorageType"] == "gp2"
    assert db_instance["Properties"]["Engine"] == "postgres"
    assert str(db_instance["Properties"]["EngineVersion"]).startswith("16")
    assert db_instance["Properties"]["DBName"] == "appdb"
    assert db_instance["Properties"]["PubliclyAccessible"] is False
    assert db_instance["Properties"]["BackupRetentionPeriod"] == 0
    assert db_instance["Properties"]["DeleteAutomatedBackups"] is True
    assert "DBSnapshotIdentifier" not in db_instance["Properties"]
    assert db_instance["Properties"]["VPCSecurityGroups"] == [sg_refs["Database security group"]]
    # RDS subnet group must use private subnets
    assert sorted(str(subnet_id) for subnet_id in db_subnet_group["Properties"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    # Credentials must be dynamic references, not plaintext
    assert "{{resolve:secretsmanager:" in str(db_instance["Properties"]["MasterUsername"])
    assert "{{resolve:secretsmanager:" in str(db_instance["Properties"]["MasterUserPassword"])
    assert secret_logical_id in str(db_instance["Properties"]["MasterUsername"])
    assert secret_logical_id in str(db_instance["Properties"]["MasterUserPassword"])

    processor = lambdas["processor"]
    processor_code = str(processor["Properties"]["Code"]["ZipFile"])
    processor_log_group_logical_id = log_group_logical_id_for_lambda(template, processor)
    processor_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[processor_log_group_logical_id]
    assert processor["Properties"]["Runtime"] == "python3.12"
    assert processor["Properties"]["Architectures"] == ["arm64"]
    assert processor["Properties"]["MemorySize"] == 512
    assert processor["Properties"]["Timeout"] == 10
    assert processor["Properties"]["VpcConfig"]["SecurityGroupIds"] == [
        sg_refs["Backend service security group"]
    ]
    assert sorted(str(subnet_id) for subnet_id in processor["Properties"]["VpcConfig"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    assert "print(" in processor_code or "logging." in processor_code
    assert processor_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in processor_log_group["Properties"]


# ── IAM policies ──────────────────────────────────────────────────────────────


def test_iam_policies_are_scoped_to_stack_resources():
    template = synth_template(aws_region="us-east-1")
    documents = policy_documents(template)
    statements = [
        statement for document in documents for statement in document["Statement"]
    ]
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")
    bucket_logical_id, _ = single_resource(template, "AWS::S3::Bucket")
    _, glue_database = single_resource(template, "AWS::Glue::Database")
    _, crawler = single_resource(template, "AWS::Glue::Crawler")
    glue_database_name = glue_database["Properties"]["DatabaseInput"]["Name"]
    crawler_name = crawler["Properties"]["Name"]
    lambdas = lambda_resources_by_shape(template)
    backend_role_logical_id = lambdas["backend"]["Properties"]["Role"]["Fn::GetAtt"][0]
    processor_role_logical_id = lambdas["processor"]["Properties"]["Role"]["Fn::GetAtt"][0]
    backend_policy = attached_policy_for_role(template, backend_role_logical_id)
    processor_policy = attached_policy_for_role(template, processor_role_logical_id)
    backend_logs_statement = log_statement_from_policy(backend_policy)
    processor_logs_statement = log_statement_from_policy(processor_policy)
    backend_ec2_statement = statement_by_actions(
        backend_policy,
        [
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
        ],
    )
    backend_log_group_logical_id = log_group_logical_id_for_lambda(template, lambdas["backend"])
    processor_log_group_logical_id = log_group_logical_id_for_lambda(template, lambdas["processor"])

    assert any(
        statement["Action"] == "secretsmanager:GetSecretValue"
        and statement["Resource"] == {"Ref": secret_logical_id}
        for statement in statements
    )
    assert any(
        statement["Action"] == "events:PutEvents"
        and statement["Resource"] == {
            "Fn::Join": [
                "",
                [
                    "arn:",
                    {"Ref": "AWS::Partition"},
                    ":events:",
                    {"Ref": "AWS::Region"},
                    ":",
                    {"Ref": "AWS::AccountId"},
                    ":event-bus/default",
                ],
            ]
        }
        for statement in statements
    )
    assert not any("kms:Decrypt" in str(statement["Action"]) for statement in backend_policy["Statement"])
    assert not any("kms:GenerateDataKey" in str(statement["Action"]) for statement in backend_policy["Statement"])
    assert not any("rds-db:connect" in str(statement["Action"]) for statement in statements)
    assert backend_ec2_statement["Resource"] == "*"
    assert any(
        statement["Action"] == [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
        ]
        and statement["Resource"] == {"Fn::GetAtt": [queue_logical_id, "Arn"]}
        for statement in statements
    )
    assert backend_logs_statement["Resource"] == [
        {"Fn::GetAtt": [backend_log_group_logical_id, "Arn"]},
        {
            "Fn::Join": [
                "",
                [{"Fn::GetAtt": [backend_log_group_logical_id, "Arn"]}, ":*"],
            ]
        },
    ]
    assert processor_logs_statement["Resource"] == [
        {"Fn::GetAtt": [processor_log_group_logical_id, "Arn"]},
        {
            "Fn::Join": [
                "",
                [{"Fn::GetAtt": [processor_log_group_logical_id, "Arn"]}, ":*"],
            ]
        },
    ]
    assert any(
        statement["Action"] == ["s3:GetObject", "s3:ListBucket"]
        and statement["Resource"] == [
            {"Fn::GetAtt": [bucket_logical_id, "Arn"]},
            {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": [bucket_logical_id, "Arn"]}, "/*"],
                ]
            },
        ]
        for statement in statements
    )
    glue_statement = next(
        statement
        for statement in statements
        if all(action.startswith("glue:") for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]))
    )
    glue_actions = glue_statement["Action"]
    if isinstance(glue_actions, str):
        glue_actions = [glue_actions]
    assert "glue:*" not in glue_actions
    assert all(action.startswith("glue:") for action in glue_actions)
    assert any(
        action in glue_actions
        for action in [
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:DeleteTable",
            "glue:BatchCreatePartition",
            "glue:BatchDeletePartition",
            "glue:BatchUpdatePartition",
        ]
    )
    glue_resources = glue_statement["Resource"]
    assert "*" not in glue_resources
    assert all(str(resource) != "*" for resource in glue_resources)
    assert any(glue_database_name in str(resource) for resource in glue_resources)
    assert any(crawler_name in str(resource) for resource in glue_resources)
