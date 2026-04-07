"""Integration tests that verify deployed resources through boto3."""

import json
import os
from pathlib import Path

import boto3
import pytest


ROOT = Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"


def load_state():
    if not STATE_JSON.exists():
        pytest.skip(
            f"state.json not found at {STATE_JSON}. Run 'terraform apply' and then "
            "'terraform show -json > state.json' before integration tests."
        )

    state = json.loads(STATE_JSON.read_text())
    root_module = state.get("values", {}).get("root_module")
    if root_module is None:
        pytest.fail("state.json is missing values.root_module")
    return state


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def state_resources():
    state = load_state()
    root_module = state["values"]["root_module"]
    return list(iter_resources(root_module))


def matching_resources(resource_type, name=None):
    matches = [resource for resource in state_resources() if resource.get("type") == resource_type]
    if name is not None:
        matches = [resource for resource in matches if resource.get("name") == name]
    return matches


def single_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert matches, f"Missing {resource_type}.{name} in state.json"
    assert len(matches) == 1, f"Expected a single {resource_type}.{name} in state.json"
    return matches[0]["values"]


def optional_resource_values(resource_type, name):
    matches = matching_resources(resource_type, name)
    assert len(matches) <= 1, f"Expected at most one {resource_type}.{name} in state.json"
    return matches[0]["values"] if matches else None


def aws_client(service_name):
    endpoint = (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
        or os.environ.get("TF_VAR_aws_endpoint")
    )
    region = os.environ.get("AWS_REGION") or os.environ.get("TF_VAR_aws_region") or "us-east-1"
    return boto3.client(
        service_name,
        region_name=region,
        endpoint_url=endpoint or None,
        aws_access_key_id=(
            os.environ.get("AWS_ACCESS_KEY_ID")
            or os.environ.get("TF_VAR_aws_access_key_id")
            or "test"
        ),
        aws_secret_access_key=(
            os.environ.get("AWS_SECRET_ACCESS_KEY")
            or os.environ.get("TF_VAR_aws_secret_access_key")
            or "test"
        ),
    )


def test_queue_and_secret_are_live():
    queue = single_resource_values("aws_sqs_queue", "intake")
    secret = single_resource_values("aws_secretsmanager_secret", "database_credentials")

    sqs = aws_client("sqs")
    queue_url = queue.get("url") or queue.get("id")
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod", "Policy"],
    )["Attributes"]

    assert attributes["VisibilityTimeout"] == "60"
    assert attributes["MessageRetentionPeriod"] == "1209600"
    if "Policy" in attributes:
        assert "sqs:SendMessage" in attributes["Policy"]
        assert queue["arn"] in attributes["Policy"]

    secretsmanager = aws_client("secretsmanager")
    secret_value = secretsmanager.get_secret_value(SecretId=secret["arn"])
    payload = json.loads(secret_value["SecretString"])
    assert payload["username"] == "payments_app"
    assert payload["password"]


def test_lambda_configurations_and_log_groups_are_live():
    worker = single_resource_values("aws_lambda_function", "worker")
    health = single_resource_values("aws_lambda_function", "health")
    enrichment = single_resource_values("aws_lambda_function", "enrichment")
    application_logs = single_resource_values("aws_cloudwatch_log_group", "application")
    api_access_logs = single_resource_values("aws_cloudwatch_log_group", "api_access")

    lambda_client = aws_client("lambda")

    worker_cfg = lambda_client.get_function_configuration(FunctionName=worker["function_name"])
    health_cfg = lambda_client.get_function_configuration(FunctionName=health["function_name"])
    enrichment_cfg = lambda_client.get_function_configuration(FunctionName=enrichment["function_name"])

    for cfg in [worker_cfg, health_cfg, enrichment_cfg]:
        assert cfg["Runtime"] == "python3.12"
        assert cfg["Handler"] == "app.handler"
        assert cfg["MemorySize"] == 256
        assert cfg["Timeout"] == 15

    worker_vpc = worker_cfg.get("VpcConfig", {})
    assert sorted(worker_vpc.get("SubnetIds", [])) == sorted(worker["vpc_config"][0]["subnet_ids"])
    assert sorted(worker_vpc.get("SecurityGroupIds", [])) == sorted(worker["vpc_config"][0]["security_group_ids"])

    assert health_cfg.get("VpcConfig", {}).get("SubnetIds", []) == []
    assert health_cfg.get("VpcConfig", {}).get("SecurityGroupIds", []) == []
    assert enrichment_cfg.get("VpcConfig", {}).get("SubnetIds", []) == []
    assert enrichment_cfg.get("VpcConfig", {}).get("SecurityGroupIds", []) == []

    logs = aws_client("logs")
    for log_group in [application_logs["name"], api_access_logs["name"]]:
        groups = logs.describe_log_groups(logGroupNamePrefix=log_group)["logGroups"]
        match = next((group for group in groups if group["logGroupName"] == log_group), None)
        assert match is not None, f"Missing live log group {log_group}"
        assert match["retentionInDays"] == 14
        assert "kmsKeyId" not in match


def test_state_machine_and_optional_live_resources_are_queryable():
    state_machine = single_resource_values("aws_sfn_state_machine", "processing")

    sfn = aws_client("stepfunctions")
    description = sfn.describe_state_machine(stateMachineArn=state_machine["arn"])
    assert description["type"] == "STANDARD"
    assert "Prepare" in description["definition"]
    assert "Complete" in description["definition"]

    api = optional_resource_values("aws_apigatewayv2_api", "front_door")
    stage = optional_resource_values("aws_apigatewayv2_stage", "default")
    if api and stage:
        apigateway = aws_client("apigatewayv2")
        live_api = apigateway.get_api(ApiId=api["api_id"])
        live_stage = apigateway.get_stage(ApiId=api["api_id"], StageName=stage["name"])
        assert live_api["ProtocolType"] == "HTTP"
        assert live_stage["AutoDeploy"] is True

    db_instance = optional_resource_values("aws_db_instance", "storage_layer")
    if db_instance:
        rds = aws_client("rds")
        live_db = rds.describe_db_instances(DBInstanceIdentifier=db_instance["identifier"])["DBInstances"][0]
        assert live_db["Engine"] == "postgres"
        assert live_db["DBInstanceClass"] == "db.t3.micro"
        assert live_db["PubliclyAccessible"] is False

    pipe = optional_resource_values("aws_pipes_pipe", "processing")
    if pipe:
        pipes = aws_client("pipes")
        live_pipe = pipes.describe_pipe(Name=pipe["name"])
        assert live_pipe["CurrentState"] == "RUNNING"
        assert live_pipe["Source"] == pipe["source"]
        assert live_pipe["Target"] == pipe["target"]


def test_vpc_and_subnet_network_foundation():
    vpc = single_resource_values("aws_vpc", "cloud_boundaries")
    subnets = matching_resources("aws_subnet")

    ec2 = aws_client("ec2")

    vpcs = ec2.describe_vpcs(VpcIds=[vpc["id"]])["Vpcs"]
    assert vpcs, f"VPC {vpc['id']} not found"
    live_vpc = vpcs[0]
    assert live_vpc["CidrBlock"] == "10.20.0.0/16"

    dns_hostnames = ec2.describe_vpc_attribute(
        VpcId=vpc["id"], Attribute="enableDnsHostnames"
    )["EnableDnsHostnames"]["Value"]
    dns_support = ec2.describe_vpc_attribute(
        VpcId=vpc["id"], Attribute="enableDnsSupport"
    )["EnableDnsSupport"]["Value"]
    assert dns_hostnames is True, "VPC must have DNS hostnames enabled"
    assert dns_support is True, "VPC must have DNS support enabled"

    subnet_ids = [s["values"]["id"] for s in subnets]
    live_subnets = ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    assert len(live_subnets) == 2, f"Expected 2 subnets, got {len(live_subnets)}"

    cidrs = {s["CidrBlock"] for s in live_subnets}
    assert cidrs == {"10.20.1.0/24", "10.20.2.0/24"}, f"Unexpected subnet CIDRs: {cidrs}"

    azs = {s["AvailabilityZone"] for s in live_subnets}
    assert len(azs) == 2, f"Subnets must be in distinct AZs, got: {azs}"

    for subnet in live_subnets:
        assert subnet["MapPublicIpOnLaunch"] is False, (
            f"Subnet {subnet['SubnetId']} must not auto-assign public IPs"
        )


def test_security_group_rules_block_unauthorized_access():
    processing_sg = single_resource_values("aws_security_group", "processing_units")
    storage_sg = single_resource_values("aws_security_group", "storage_layer")

    ec2 = aws_client("ec2")

    all_rules = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [storage_sg["id"]]}]
    )["SecurityGroupRules"]

    ingress_rules = [r for r in all_rules if not r.get("IsEgress", False)]

    # Negative path: storage SG must not expose 0.0.0.0/0 on any ingress rule
    for rule in ingress_rules:
        assert rule.get("CidrIpv4") != "0.0.0.0/0", (
            "Storage security group must not allow 0.0.0.0/0 ingress"
        )
        assert rule.get("CidrIpv6") not in ("::/0",), (
            "Storage security group must not allow ::/0 ingress"
        )

    # Must allow port 5432 exclusively from the processing_units security group
    postgres_rules = [
        r for r in ingress_rules
        if r.get("FromPort") == 5432
        and r.get("ToPort") == 5432
        and r.get("ReferencedGroupInfo", {}).get("GroupId") == processing_sg["id"]
    ]
    assert postgres_rules, (
        "Storage SG must allow port 5432 ingress from processing_units security group only"
    )


def test_rds_instance_configuration_matches_spec():
    db_instance = optional_resource_values("aws_db_instance", "storage_layer")
    if db_instance:
        rds = aws_client("rds")
        live_db = rds.describe_db_instances(
            DBInstanceIdentifier=db_instance["identifier"]
        )["DBInstances"][0]

        assert live_db["Engine"] == "postgres"
        assert live_db["EngineVersion"].startswith("15.4")
        assert live_db["DBInstanceClass"] == "db.t3.micro"
        assert live_db["AllocatedStorage"] == 20
        assert live_db["StorageType"] == "gp2"
        assert live_db["PubliclyAccessible"] is False
        assert live_db.get("DeletionProtection") is False


def test_api_gateway_routes_and_protocol():
    api = optional_resource_values("aws_apigatewayv2_api", "front_door")
    if api:
        apigateway = aws_client("apigatewayv2")

        live_api = apigateway.get_api(ApiId=api["api_id"])
        assert live_api["ProtocolType"] == "HTTP"

        routes = apigateway.get_routes(ApiId=api["api_id"])
        route_keys = {r["RouteKey"] for r in routes.get("Items", [])}
        assert "POST /submit" in route_keys, f"Missing POST /submit route, got: {route_keys}"
        assert "GET /health" in route_keys, f"Missing GET /health route, got: {route_keys}"
