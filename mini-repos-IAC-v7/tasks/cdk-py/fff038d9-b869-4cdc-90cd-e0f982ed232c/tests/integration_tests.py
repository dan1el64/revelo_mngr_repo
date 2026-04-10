import importlib.util
import os
from pathlib import Path
import runpy
import sys

from aws_cdk.assertions import Template

APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def load_app_module():
    sys.modules.pop("app", None)
    spec = importlib.util.spec_from_file_location("app", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["app"] = module
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_app_script_entrypoint_synthesizes(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    namespace = runpy.run_path(str(APP_PATH), run_name="__main__")

    assert "main" in namespace


def test_main_builds_single_stack_with_required_outputs(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    app_module = load_app_module()
    app = app_module.main()

    assert [child.node.id for child in app.node.children] == ["EventDrivenIngestionStack"]

    stack = app.node.find_child("EventDrivenIngestionStack")
    template = Template.from_stack(stack).to_json()
    assert sorted(template["Outputs"]) == [
        "ApiInvokeUrl",
        "EventBusName",
        "IngestionQueueUrl",
        "ProcessedBucketName",
    ]


def test_main_stack_has_no_termination_protection_and_exact_core_counts(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    app_module = load_app_module()
    app = app_module.main()
    stack = app.node.find_child("EventDrivenIngestionStack")
    template = Template.from_stack(stack).to_json()
    resources = template["Resources"]

    assert stack.termination_protection is False
    assert len([r for r in resources.values() if r["Type"] == "AWS::Lambda::Function"]) == 3
    assert len([r for r in resources.values() if r["Type"] == "AWS::SQS::Queue"]) == 2
    assert len([r for r in resources.values() if r["Type"] == "AWS::DynamoDB::Table"]) == 2
    assert len([r for r in resources.values() if r["Type"] == "AWS::StepFunctions::StateMachine"]) == 1
    assert len([r for r in resources.values() if r["Type"] == "AWS::Pipes::Pipe"]) == 1


def test_main_template_uses_only_expected_resource_types(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp")
    monkeypatch.setenv("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/jsii-cache")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    app_module = load_app_module()
    template = Template.from_stack(app_module.main().node.find_child("EventDrivenIngestionStack")).to_json()
    resource_types = {resource["Type"] for resource in template["Resources"].values()}

    assert resource_types == {
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
