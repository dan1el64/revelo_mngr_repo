from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import json
import sys

import aws_cdk as cdk
from aws_cdk.assertions import Template


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"
APP_SPEC = spec_from_file_location("app", APP_PATH)
APP_MODULE = module_from_spec(APP_SPEC)
sys.modules["app"] = APP_MODULE
assert APP_SPEC and APP_SPEC.loader
APP_SPEC.loader.exec_module(APP_MODULE)
PocStack = APP_MODULE.PocStack


def synth_template() -> dict:
    app = cdk.App()
    stack = PocStack(app, "IntegrationPocStack", aws_region="us-east-1")
    return Template.from_stack(stack).to_json()


def resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def single_resource(template: dict, resource_type: str) -> tuple[str, dict]:
    resources = resources_by_type(template, resource_type)
    assert len(resources) == 1, f"Expected exactly 1 {resource_type}, found {len(resources)}"
    return next(iter(resources.items()))


def private_subnet_refs(template: dict) -> list[dict]:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if not resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def public_subnet_refs(template: dict) -> list[dict]:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def sg_ref_by_description(template: dict, description: str) -> dict:
    for logical_id, resource in resources_by_type(template, "AWS::EC2::SecurityGroup").items():
        if resource["Properties"]["GroupDescription"] == description:
            return {"Fn::GetAtt": [logical_id, "GroupId"]}
    raise AssertionError(f"Security group not found: {description}")


def lambda_by_memory(template: dict, memory_size: int) -> dict:
    for resource in resources_by_type(template, "AWS::Lambda::Function").values():
        if resource["Properties"]["MemorySize"] == memory_size:
            return resource
    raise AssertionError(f"Lambda with MemorySize={memory_size} not found")


def test_alb_is_internet_facing_application():
    template = synth_template()
    _, alb = single_resource(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")

    assert alb["Properties"]["Scheme"] == "internet-facing"
    assert alb["Properties"]["Type"] == "application"
    assert sorted(str(subnet) for subnet in alb["Properties"]["Subnets"]) == sorted(
        str(subnet) for subnet in public_subnet_refs(template)
    )


def test_alb_dns_name_exported():
    template = synth_template()
    alb_logical_id, _ = single_resource(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    output = template["Outputs"]["FrontendAlbDns"]

    assert output["Export"]["Name"] == "FrontendAlbDns"
    assert output["Value"] == {"Fn::GetAtt": [alb_logical_id, "DNSName"]}


def test_alb_listener_on_http_80():
    template = synth_template()
    _, listener = single_resource(template, "AWS::ElasticLoadBalancingV2::Listener")

    assert listener["Properties"]["Port"] == 80
    assert listener["Properties"]["Protocol"] == "HTTP"
    assert listener["Properties"]["DefaultActions"][0]["Type"] == "forward"


def test_ecs_fargate_service_desired_count_2():
    template = synth_template()
    cluster_logical_id, _ = single_resource(template, "AWS::ECS::Cluster")
    _, service = single_resource(template, "AWS::ECS::Service")
    task_definition_logical_id, task_definition = single_resource(
        template, "AWS::ECS::TaskDefinition"
    )
    frontend_sg = sg_ref_by_description(template, "Frontend ALB security group")

    network = service["Properties"]["NetworkConfiguration"]["AwsvpcConfiguration"]
    assert service["Properties"]["DesiredCount"] == 2
    assert service["Properties"]["LaunchType"] == "FARGATE"
    assert service["Properties"]["Cluster"] == {"Ref": cluster_logical_id}
    assert service["Properties"]["TaskDefinition"] == {"Ref": task_definition_logical_id}
    assert task_definition["Properties"]["Cpu"] == "512"
    assert task_definition["Properties"]["Memory"] == "1024"
    assert network["AssignPublicIp"] == "DISABLED"
    assert network["SecurityGroups"] == [frontend_sg]
    assert sorted(str(subnet) for subnet in network["Subnets"]) == sorted(
        str(subnet) for subnet in private_subnet_refs(template)
    )


def test_api_gateway_has_health_and_items_resources():
    template = synth_template()
    resources = resources_by_type(template, "AWS::ApiGateway::Resource")
    paths = {resource["Properties"]["PathPart"] for resource in resources.values()}

    assert paths == {"health", "items"}


def test_api_gateway_prod_stage_exists():
    template = synth_template()
    _, stage = single_resource(template, "AWS::ApiGateway::Stage")

    assert stage["Properties"]["StageName"] == "prod"
    assert any(
        setting["HttpMethod"] == "*"
        and setting["ResourcePath"] == "/*"
        and setting["LoggingLevel"] == "INFO"
        for setting in stage["Properties"]["MethodSettings"]
    )


def test_backend_lambda_runtime_arch_and_concurrency():
    template = synth_template()
    backend_sg = sg_ref_by_description(template, "Backend service security group")
    backend = lambda_by_memory(template, 1024)

    assert backend["Properties"]["Runtime"] == "python3.12"
    assert backend["Properties"]["Architectures"] == ["arm64"]
    assert backend["Properties"]["ReservedConcurrentExecutions"] == 20
    assert backend["Properties"]["Timeout"] == 10
    assert backend["Properties"]["VpcConfig"]["SecurityGroupIds"] == [backend_sg]
    assert sorted(str(subnet) for subnet in backend["Properties"]["VpcConfig"]["SubnetIds"]) == sorted(
        str(subnet) for subnet in private_subnet_refs(template)
    )


def test_event_processor_lambda_runtime_and_config():
    template = synth_template()
    backend_sg = sg_ref_by_description(template, "Backend service security group")
    processor = lambda_by_memory(template, 512)

    assert processor["Properties"]["Runtime"] == "python3.12"
    assert processor["Properties"]["Architectures"] == ["arm64"]
    assert processor["Properties"]["Timeout"] == 10
    assert processor["Properties"]["VpcConfig"]["SecurityGroupIds"] == [backend_sg]


def test_rds_instance_engine_class_and_settings():
    template = synth_template()
    db_sg = sg_ref_by_description(template, "Database security group")
    _, db = single_resource(template, "AWS::RDS::DBInstance")
    _, db_subnet_group = single_resource(template, "AWS::RDS::DBSubnetGroup")

    assert db["Properties"]["Engine"] == "postgres"
    assert str(db["Properties"]["EngineVersion"]).startswith("16")
    assert db["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert db["Properties"]["StorageType"] == "gp2"
    assert db["Properties"]["DBName"] == "appdb"
    assert db["Properties"]["PubliclyAccessible"] is False
    assert db["Properties"]["BackupRetentionPeriod"] == 0
    assert db["Properties"]["DeletionProtection"] is False
    assert db["Properties"]["VPCSecurityGroups"] == [db_sg]
    assert sorted(str(subnet) for subnet in db_subnet_group["Properties"]["SubnetIds"]) == sorted(
        str(subnet) for subnet in private_subnet_refs(template)
    )


def test_rds_secret_contains_username_and_password():
    template = synth_template()
    secret_logical_id, secret = single_resource(template, "AWS::SecretsManager::Secret")
    _, db = single_resource(template, "AWS::RDS::DBInstance")

    assert json.loads(secret["Properties"]["GenerateSecretString"]["SecretStringTemplate"]) == {
        "username": "appuser"
    }
    assert secret["Properties"]["GenerateSecretString"]["GenerateStringKey"] == "password"
    assert secret_logical_id in str(db["Properties"]["MasterUsername"])
    assert secret_logical_id in str(db["Properties"]["MasterUserPassword"])
    assert "{{resolve:secretsmanager:" in str(db["Properties"]["MasterUsername"])
    assert "{{resolve:secretsmanager:" in str(db["Properties"]["MasterUserPassword"])


def test_sqs_queue_visibility_timeout_and_retention():
    template = synth_template()
    _, queue = single_resource(template, "AWS::SQS::Queue")

    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert queue["Properties"]["MessageRetentionPeriod"] == 345600


def test_eventbridge_rule_pattern_and_sqs_target():
    template = synth_template()
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")
    _, rule = single_resource(template, "AWS::Events::Rule")

    assert rule["Properties"]["EventPattern"] == {
        "detail-type": ["item.created"],
        "source": ["app.backend"],
    }
    assert rule["Properties"]["Targets"][0]["Arn"] == {
        "Fn::GetAtt": [queue_logical_id, "Arn"]
    }


def test_s3_bucket_blocks_all_public_access():
    template = synth_template()
    _, bucket = single_resource(template, "AWS::S3::Bucket")

    assert bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }


def test_glue_crawler_targets_s3_data_prefix():
    template = synth_template()
    _, glue_database = single_resource(template, "AWS::Glue::Database")
    _, crawler = single_resource(template, "AWS::Glue::Crawler")

    assert crawler["Properties"]["DatabaseName"] == glue_database["Properties"]["DatabaseInput"]["Name"]
    assert len(crawler["Properties"]["Targets"]["S3Targets"]) == 1
    assert "/data/" in str(crawler["Properties"]["Targets"]["S3Targets"][0]["Path"])


def test_put_event_reaches_sqs_queue():
    template = synth_template()
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")
    _, mapping = single_resource(template, "AWS::Lambda::EventSourceMapping")
    _, rule = single_resource(template, "AWS::Events::Rule")

    assert mapping["Properties"]["EventSourceArn"] == {
        "Fn::GetAtt": [queue_logical_id, "Arn"]
    }
    assert mapping["Properties"]["BatchSize"] == 10
    assert rule["Properties"]["Targets"][0]["Arn"] == {
        "Fn::GetAtt": [queue_logical_id, "Arn"]
    }
