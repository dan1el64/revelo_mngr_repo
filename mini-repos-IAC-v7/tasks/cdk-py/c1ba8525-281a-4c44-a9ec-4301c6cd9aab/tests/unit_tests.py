import ast
import inspect
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


def collect_env_input_names_from_ast() -> set[str]:
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

    stack = PocStack(cdk.App(), "InputContractStack", aws_region="us-west-2")
    assert stack.termination_protection is False

    captured: dict[str, object] = {}

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

    assert len(log_groups) >= 4
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
