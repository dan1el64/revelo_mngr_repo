import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config


REPO_DIR = Path(__file__).resolve().parent.parent
STATE_PATH = REPO_DIR / "state.json"


def aws_region():
    return (
        os.environ.get("TF_VAR_aws_region")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    )


def discover_endpoint():
    endpoint = (
        os.environ.get("TF_VAR_aws_endpoint")
        or os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
    )
    if endpoint:
        return endpoint

    if not STATE_PATH.exists():
        return None

    state = json.loads(STATE_PATH.read_text())
    for resource in state_resources(state["values"]["root_module"]):
        values = resource.get("values", {})
        for key in ("id", "url", "invoke_url", "queue_url"):
            candidate = values.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                parsed = urlparse(candidate)
                hostname = parsed.hostname or ""
                labels = hostname.split(".")
                port = f":{parsed.port}" if parsed.port else ""
                region = aws_region()
                if len(labels) > 2 and labels[1] == region:
                    generic_host = ".".join(labels[2:])
                    if generic_host:
                        return f"{parsed.scheme}://{generic_host}{port}"
                return f"{parsed.scheme}://{parsed.netloc}"
    return None


def aws_client(service_name):
    kwargs = {
        "region_name": aws_region(),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    }
    endpoint = discover_endpoint()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if service_name == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service_name, **kwargs)


def load_state():
    if not STATE_PATH.exists():
        pytest.fail("state.json must exist before running integration tests")
    return json.loads(STATE_PATH.read_text())


def state_resources(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(state_resources(child))
    return resources


def resources_by_type(state, resource_type):
    root_module = state["values"]["root_module"]
    return [
        resource
        for resource in state_resources(root_module)
        if resource["type"] == resource_type and resource.get("mode", "managed") == "managed"
    ]


def one_resource(state, resource_type):
    resources = resources_by_type(state, resource_type)
    assert len(resources) == 1, f"expected exactly one {resource_type}, found {len(resources)}"
    return resources[0]


def resource_values(state, resource_type, resource_name):
    for resource in resources_by_type(state, resource_type):
        if resource["name"] == resource_name:
            return resource["values"]
    raise AssertionError(f"{resource_type}.{resource_name} not found")


def inline_policy(role_name, policy_name):
    iam = aws_client("iam")
    return iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)["PolicyDocument"]


def permission_has_cidr(permission, cidr):
    return any(ip_range.get("CidrIp") == cidr for ip_range in permission.get("IpRanges", []))


def test_network_topology_matches_contract():
    state = load_state()
    ec2 = aws_client("ec2")

    vpc = one_resource(state, "aws_vpc")["values"]
    assert vpc["cidr_block"] == "10.20.0.0/16"

    subnets = resources_by_type(state, "aws_subnet")
    assert len(subnets) == 4
    subnet_ids = [subnet["values"]["id"] for subnet in subnets]
    subnet_details = ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    assert {subnet["CidrBlock"] for subnet in subnet_details} == {
        "10.20.0.0/24",
        "10.20.1.0/24",
        "10.20.10.0/24",
        "10.20.11.0/24",
    }
    public_subnets = [subnet for subnet in subnet_details if subnet["MapPublicIpOnLaunch"]]
    private_subnets = [subnet for subnet in subnet_details if not subnet["MapPublicIpOnLaunch"]]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert len({subnet["AvailabilityZone"] for subnet in public_subnets}) == 2
    assert len({subnet["AvailabilityZone"] for subnet in private_subnets}) == 2
    assert {subnet["AvailabilityZone"] for subnet in public_subnets} == {subnet["AvailabilityZone"] for subnet in private_subnets}

    igw = one_resource(state, "aws_internet_gateway")["values"]
    described_igw = ec2.describe_internet_gateways(InternetGatewayIds=[igw["id"]])["InternetGateways"][0]
    assert described_igw["Attachments"][0]["VpcId"] == vpc["id"]

    route_tables = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc["id"]]}])["RouteTables"]
    managed_route_table_ids = {
        resource_values(state, "aws_route_table", "public")["id"],
        resource_values(state, "aws_route_table", "private")["id"],
    }
    route_tables = [route_table for route_table in route_tables if route_table["RouteTableId"] in managed_route_table_ids]
    assert len(route_tables) == 2

    endpoint = one_resource(state, "aws_vpc_endpoint")["values"]
    described_endpoint = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint["id"]])["VpcEndpoints"][0]
    assert described_endpoint["VpcEndpointType"] == "Gateway"


def test_security_groups_and_private_wiring_match_contract():
    state = load_state()
    ec2 = aws_client("ec2")
    lambda_client = aws_client("lambda")
    rds = aws_client("rds")

    api_sg = resource_values(state, "aws_security_group", "api")
    worker_sg = resource_values(state, "aws_security_group", "worker")
    db_sg = resource_values(state, "aws_security_group", "database")

    groups = ec2.describe_security_groups(GroupIds=[api_sg["id"], worker_sg["id"], db_sg["id"]])["SecurityGroups"]
    groups_by_id = {group["GroupId"]: group for group in groups}

    api_group = groups_by_id[api_sg["id"]]
    assert any(
        permission["FromPort"] == 443
        and permission["ToPort"] == 443
        and permission["IpProtocol"] == "tcp"
        and permission_has_cidr(permission, "0.0.0.0/0")
        for permission in api_group["IpPermissions"]
    )

    worker_group = groups_by_id[worker_sg["id"]]
    assert worker_group["IpPermissions"] == []

    database_group = groups_by_id[db_sg["id"]]
    assert len(database_group["IpPermissions"]) == 1
    permission = database_group["IpPermissions"][0]
    assert permission["FromPort"] == 5432
    assert permission["ToPort"] == 5432
    assert permission["UserIdGroupPairs"][0]["GroupId"] == worker_sg["id"]

    function = lambda_client.get_function(FunctionName="ingest-function")["Configuration"]
    assert function["VpcConfig"]["SecurityGroupIds"] == [worker_sg["id"]]

    db_instance = rds.describe_db_instances(DBInstanceIdentifier="payments-db")["DBInstances"][0]
    assert db_instance["VpcSecurityGroups"][0]["VpcSecurityGroupId"] == db_sg["id"]


def test_compute_resources_match_contract():
    state = load_state()
    apigateway = aws_client("apigateway")
    lambda_client = aws_client("lambda")
    sqs = aws_client("sqs")

    function = lambda_client.get_function(FunctionName="ingest-function")["Configuration"]
    assert function["Runtime"] == "python3.12"
    assert function["MemorySize"] == 256
    assert function["Timeout"] == 10
    assert function["PackageType"] == "Zip"

    queue_url = sqs.get_queue_url(QueueName="ingest-queue")["QueueUrl"]
    queue_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "SqsManagedSseEnabled"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "30"
    assert queue_attrs["SqsManagedSseEnabled"] == "true"

    mappings = lambda_client.list_event_source_mappings(FunctionName="ingest-function")["EventSourceMappings"]
    assert len(mappings) == 1
    assert mappings[0]["BatchSize"] == 10
    assert mappings[0]["EventSourceArn"] == resource_values(state, "aws_sqs_queue", "ingest")["arn"]

    api = one_resource(state, "aws_api_gateway_rest_api")["values"]
    resources = apigateway.get_resources(restApiId=api["id"])["items"]
    ingest_resource = next(resource for resource in resources if resource["path"] == "/ingest")
    method = apigateway.get_method(restApiId=api["id"], resourceId=ingest_resource["id"], httpMethod="POST")
    integration = apigateway.get_integration(restApiId=api["id"], resourceId=ingest_resource["id"], httpMethod="POST")
    assert method["httpMethod"] == "POST"
    assert method["authorizationType"] == "NONE"
    assert integration["type"] == "AWS_PROXY"


def test_workflow_resources_match_contract():
    state = load_state()
    logs = aws_client("logs")
    stepfunctions = aws_client("stepfunctions")
    pipes = aws_client("pipes")

    log_groups = {
        group["logGroupName"]: group
        for group in logs.describe_log_groups(logGroupNamePrefix="/aws/")["logGroups"]
    }
    assert log_groups["/aws/lambda/ingest-function"]["retentionInDays"] == 14
    assert log_groups["/aws/vendedlogs/states/ingest-state-machine"]["retentionInDays"] == 7

    machine = stepfunctions.describe_state_machine(
        stateMachineArn=resource_values(state, "aws_sfn_state_machine", "ingest")["arn"]
    )
    definition = json.loads(machine["definition"])
    assert machine["type"] == "STANDARD"
    assert definition["StartAt"] == "InvokeIngestFunction"
    assert definition["States"]["InvokeIngestFunction"]["Type"] == "Task"
    assert definition["States"]["InvokeIngestFunction"]["End"] is True

    pipe = pipes.describe_pipe(Name="ingest-pipe")
    assert pipe["Source"] == resource_values(state, "aws_sqs_queue", "ingest")["arn"]
    assert pipe["Enrichment"] == resource_values(state, "aws_lambda_function", "ingest")["arn"]
    assert pipe["Target"] == resource_values(state, "aws_sfn_state_machine", "ingest")["arn"]
    assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 10


def test_iam_configuration_matches_contract():
    state = load_state()
    iam = aws_client("iam")

    lambda_role = iam.get_role(RoleName="lambda-execution-role")["Role"]
    sfn_role = iam.get_role(RoleName="step-functions-role")["Role"]
    pipes_role = iam.get_role(RoleName="eventbridge-pipes-role")["Role"]

    assert lambda_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "lambda.amazonaws.com"
    assert sfn_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "states.amazonaws.com"
    assert pipes_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "pipes.amazonaws.com"

    lambda_policy = inline_policy("lambda-execution-role", "lambda-inline-policy")
    sfn_policy = inline_policy("step-functions-role", "step-functions-inline-policy")
    pipe_policy = inline_policy("eventbridge-pipes-role", "eventbridge-pipes-inline-policy")

    lambda_statements = {statement["Sid"]: statement for statement in lambda_policy["Statement"]}
    assert lambda_statements["SendToQueue"]["Resource"] == resource_values(state, "aws_sqs_queue", "ingest")["arn"]
    assert lambda_statements["ArchivePayloads"]["Resource"] == "arn:aws:s3:::payments-ingest-bucket/*"
    assert lambda_statements["ReadDatabaseSecret"]["Resource"] == resource_values(state, "aws_secretsmanager_secret", "db")["arn"]

    sfn_statements = {statement["Sid"]: statement for statement in sfn_policy["Statement"]}
    assert sfn_statements["InvokeIngestLambda"]["Resource"] == resource_values(state, "aws_lambda_function", "ingest")["arn"]

    pipe_statements = {statement["Sid"]: statement for statement in pipe_policy["Statement"]}
    assert pipe_statements["ConsumeIngestQueue"]["Resource"] == resource_values(state, "aws_sqs_queue", "ingest")["arn"]
    assert pipe_statements["InvokeEnrichmentLambda"]["Resource"] == resource_values(state, "aws_lambda_function", "ingest")["arn"]
    assert pipe_statements["StartStateMachineExecution"]["Resource"] == resource_values(state, "aws_sfn_state_machine", "ingest")["arn"]


def test_storage_resources_match_contract():
    state = load_state()
    s3 = aws_client("s3")
    secretsmanager = aws_client("secretsmanager")
    rds = aws_client("rds")

    encryption = s3.get_bucket_encryption(Bucket="payments-ingest-bucket")
    public_access = s3.get_public_access_block(Bucket="payments-ingest-bucket")["PublicAccessBlockConfiguration"]
    ownership = s3.get_bucket_ownership_controls(Bucket="payments-ingest-bucket")["OwnershipControls"]["Rules"][0]
    bucket_policy = json.loads(s3.get_bucket_policy(Bucket="payments-ingest-bucket")["Policy"])

    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert public_access["BlockPublicAcls"] is True
    assert public_access["BlockPublicPolicy"] is True
    assert public_access["IgnorePublicAcls"] is True
    assert public_access["RestrictPublicBuckets"] is True
    assert ownership["ObjectOwnership"] == "BucketOwnerEnforced"

    deny_statement = next(statement for statement in bucket_policy["Statement"] if statement["Sid"] == "DenyInsecureTransport")
    assert deny_statement["Condition"]["Bool"]["aws:SecureTransport"] == "false"

    credentials = json.loads(secretsmanager.get_secret_value(SecretId="db-credentials")["SecretString"])
    assert credentials["username"] == "appuser"
    assert len(credentials["password"]) == 20
    assert re.search(r"\d", credentials["password"])
    assert re.search(r"[^A-Za-z0-9]", credentials["password"])

    db_instance = rds.describe_db_instances(DBInstanceIdentifier="payments-db")["DBInstances"][0]
    assert db_instance["Engine"] == "postgres"
    assert db_instance["EngineVersion"] == "16.3"
    assert db_instance["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["AllocatedStorage"] == 20
    assert db_instance["StorageType"] == "gp2"
    assert db_instance["PubliclyAccessible"] is False
    assert db_instance["StorageEncrypted"] is True

    subnet_group = rds.describe_db_subnet_groups(DBSubnetGroupName="payments-db-subnet-group")["DBSubnetGroups"][0]
    assert {subnet["SubnetIdentifier"] for subnet in subnet_group["Subnets"]} == {
        resource_values(state, "aws_subnet", "private_a")["id"],
        resource_values(state, "aws_subnet", "private_b")["id"],
    }
