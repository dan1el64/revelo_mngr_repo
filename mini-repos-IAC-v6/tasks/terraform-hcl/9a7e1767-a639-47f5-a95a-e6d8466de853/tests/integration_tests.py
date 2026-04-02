import json
import os
import re

import boto3
from botocore.config import Config


VPC_NAME = "payments-ingestion-vpc"
PUBLIC_ROUTE_TABLE_NAME = "payments-public-rt"
PRIVATE_ROUTE_TABLE_NAME = "payments-private-rt"
PUBLIC_SUBNET_A_NAME = "payments-public-a"
PUBLIC_SUBNET_B_NAME = "payments-public-b"
PRIVATE_SUBNET_A_NAME = "payments-private-a"
PRIVATE_SUBNET_B_NAME = "payments-private-b"
API_SECURITY_GROUP_NAME = "api-security-group"
WORKER_SECURITY_GROUP_NAME = "worker-security-group"
DATABASE_SECURITY_GROUP_NAME = "database-security-group"
LAMBDA_FUNCTION_NAME = "ingest-function"
QUEUE_NAME = "ingest-queue"
REST_API_NAME = "ingest-api"
STATE_MACHINE_NAME = "ingest-state-machine"
PIPE_NAME = "ingest-pipe"
S3_BUCKET_NAME = "payments-ingest-bucket"
SECRET_NAME = "db-credentials"
DB_INSTANCE_IDENTIFIER = "payments-db"
DB_SUBNET_GROUP_NAME = "payments-db-subnet-group"
LAMBDA_ROLE_NAME = "lambda-execution-role"
STEP_FUNCTIONS_ROLE_NAME = "step-functions-role"
PIPES_ROLE_NAME = "eventbridge-pipes-role"


def aws_region():
    return (
        os.environ.get("TF_VAR_aws_region")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    )


def discover_endpoint():
    return (
        os.environ.get("TF_VAR_aws_endpoint")
        or os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
    )


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


def describe_one(items, description):
    assert len(items) == 1, f"expected exactly one {description}, found {len(items)}"
    return items[0]


def get_vpc(ec2):
    return describe_one(
        ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [VPC_NAME]}])["Vpcs"],
        f"VPC tagged {VPC_NAME}",
    )


def get_subnet_by_name(ec2, subnet_name):
    return describe_one(
        ec2.describe_subnets(Filters=[{"Name": "tag:Name", "Values": [subnet_name]}])["Subnets"],
        f"subnet tagged {subnet_name}",
    )


def get_route_table_by_name(ec2, route_table_name):
    return describe_one(
        ec2.describe_route_tables(Filters=[{"Name": "tag:Name", "Values": [route_table_name]}])["RouteTables"],
        f"route table tagged {route_table_name}",
    )


def get_security_group_by_name(ec2, group_name):
    return describe_one(
        ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [group_name]}])["SecurityGroups"],
        f"security group named {group_name}",
    )


def get_rest_api(apigateway):
    apis = [api for api in apigateway.get_rest_apis().get("items", []) if api.get("name") == REST_API_NAME]
    return describe_one(apis, f"REST API named {REST_API_NAME}")


def get_state_machine(stepfunctions):
    machines = [
        machine for machine in stepfunctions.list_state_machines().get("stateMachines", []) if machine["name"] == STATE_MACHINE_NAME
    ]
    return describe_one(machines, f"state machine named {STATE_MACHINE_NAME}")


def get_secret(secretsmanager):
    return secretsmanager.describe_secret(SecretId=SECRET_NAME)


def inline_policy(role_name, policy_name):
    iam = aws_client("iam")
    return iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)["PolicyDocument"]


def permission_has_cidr(permission, cidr):
    return any(ip_range.get("CidrIp") == cidr for ip_range in permission.get("IpRanges", []))


def permission_is_allow_all_egress(permission):
    ip_protocol = permission.get("IpProtocol")
    return ip_protocol == "-1" and permission_has_cidr(permission, "0.0.0.0/0")


def uses_aws_endpoint():
    endpoint = discover_endpoint()
    return endpoint is None or "amazonaws.com" in endpoint


def test_network_topology_matches_contract():
    ec2 = aws_client("ec2")

    vpc = get_vpc(ec2)
    assert vpc["CidrBlock"] == "10.20.0.0/16"

    subnet_details = [
        get_subnet_by_name(ec2, PUBLIC_SUBNET_A_NAME),
        get_subnet_by_name(ec2, PUBLIC_SUBNET_B_NAME),
        get_subnet_by_name(ec2, PRIVATE_SUBNET_A_NAME),
        get_subnet_by_name(ec2, PRIVATE_SUBNET_B_NAME),
    ]
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

    described_igw = describe_one(
        ec2.describe_internet_gateways(Filters=[{"Name": "attachment.vpc-id", "Values": [vpc["VpcId"]]}])["InternetGateways"],
        f"internet gateway attached to {VPC_NAME}",
    )
    assert described_igw["Attachments"][0]["VpcId"] == vpc["VpcId"]

    public_route_table = get_route_table_by_name(ec2, PUBLIC_ROUTE_TABLE_NAME)
    private_route_table = get_route_table_by_name(ec2, PRIVATE_ROUTE_TABLE_NAME)

    public_default_routes = [
        route
        for route in public_route_table["Routes"]
        if route.get("DestinationCidrBlock") == "0.0.0.0/0"
        and route.get("GatewayId") == described_igw["InternetGatewayId"]
        and route.get("State") == "active"
    ]
    assert len(public_default_routes) == 1

    private_default_routes = [
        route
        for route in private_route_table["Routes"]
        if route.get("DestinationCidrBlock") == "0.0.0.0/0"
    ]
    assert private_default_routes == []

    described_endpoint = describe_one(
        ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc["VpcId"]]},
                {"Name": "service-name", "Values": [f"com.amazonaws.{aws_region()}.s3"]},
            ]
        )["VpcEndpoints"],
        "S3 gateway VPC endpoint",
    )
    assert described_endpoint["VpcEndpointType"] == "Gateway"
    assert private_route_table["RouteTableId"] in described_endpoint["RouteTableIds"]


def test_security_groups_and_private_wiring_match_contract():
    ec2 = aws_client("ec2")
    lambda_client = aws_client("lambda")
    rds = aws_client("rds")

    api_group = get_security_group_by_name(ec2, API_SECURITY_GROUP_NAME)
    assert any(
        permission["FromPort"] == 443
        and permission["ToPort"] == 443
        and permission["IpProtocol"] == "tcp"
        and permission_has_cidr(permission, "0.0.0.0/0")
        for permission in api_group["IpPermissions"]
    )
    assert len(api_group["IpPermissionsEgress"]) == 1
    assert permission_is_allow_all_egress(api_group["IpPermissionsEgress"][0])

    worker_group = get_security_group_by_name(ec2, WORKER_SECURITY_GROUP_NAME)
    assert worker_group["IpPermissions"] == []
    assert len(worker_group["IpPermissionsEgress"]) == 1
    assert permission_is_allow_all_egress(worker_group["IpPermissionsEgress"][0])

    database_group = get_security_group_by_name(ec2, DATABASE_SECURITY_GROUP_NAME)
    assert len(database_group["IpPermissions"]) == 1
    permission = database_group["IpPermissions"][0]
    assert permission["FromPort"] == 5432
    assert permission["ToPort"] == 5432
    assert permission["UserIdGroupPairs"][0]["GroupId"] == worker_group["GroupId"]

    function = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)["Configuration"]
    assert function["VpcConfig"]["SecurityGroupIds"] == [worker_group["GroupId"]]

    if uses_aws_endpoint():
        db_instance = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)["DBInstances"][0]
        assert db_instance["VpcSecurityGroups"][0]["VpcSecurityGroupId"] == database_group["GroupId"]


def test_compute_resources_match_contract():
    apigateway = aws_client("apigateway")
    lambda_client = aws_client("lambda")
    sqs = aws_client("sqs")

    function = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)["Configuration"]
    assert function["Runtime"] == "python3.12"
    assert function["MemorySize"] == 256
    assert function["Timeout"] == 10
    assert function["PackageType"] == "Zip"

    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    queue_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "SqsManagedSseEnabled"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "30"
    assert queue_attrs["SqsManagedSseEnabled"] == "true"

    mappings = lambda_client.list_event_source_mappings(FunctionName=LAMBDA_FUNCTION_NAME)["EventSourceMappings"]
    assert len(mappings) == 1
    assert mappings[0]["BatchSize"] == 10
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    assert mappings[0]["EventSourceArn"] == queue_arn

    api = get_rest_api(apigateway)
    resources = apigateway.get_resources(restApiId=api["id"])["items"]
    ingest_resource = next(resource for resource in resources if resource["path"] == "/ingest")
    method = apigateway.get_method(restApiId=api["id"], resourceId=ingest_resource["id"], httpMethod="POST")
    integration = apigateway.get_integration(restApiId=api["id"], resourceId=ingest_resource["id"], httpMethod="POST")
    assert method["httpMethod"] == "POST"
    assert method["authorizationType"] == "NONE"
    assert integration["type"] == "AWS_PROXY"


def test_workflow_resources_match_contract():
    logs = aws_client("logs")
    stepfunctions = aws_client("stepfunctions")
    pipes = aws_client("pipes")

    log_groups = {
        group["logGroupName"]: group
        for group in logs.describe_log_groups(logGroupNamePrefix="/aws/")["logGroups"]
    }
    assert log_groups["/aws/lambda/ingest-function"]["retentionInDays"] == 14
    assert log_groups["/aws/vendedlogs/states/ingest-state-machine"]["retentionInDays"] == 7
    assert not log_groups["/aws/vendedlogs/states/ingest-state-machine"].get("kmsKeyId")

    machine_summary = get_state_machine(stepfunctions)
    machine = stepfunctions.describe_state_machine(stateMachineArn=machine_summary["stateMachineArn"])
    definition = json.loads(machine["definition"])
    assert machine["type"] == "STANDARD"
    assert definition["StartAt"] == "InvokeIngestFunction"
    assert definition["States"]["InvokeIngestFunction"]["Type"] == "Task"
    assert definition["States"]["InvokeIngestFunction"]["End"] is True

    queue_url = aws_client("sqs").get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    queue_arn = aws_client("sqs").get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    function_arn = aws_client("lambda").get_function(FunctionName=LAMBDA_FUNCTION_NAME)["Configuration"]["FunctionArn"]
    if uses_aws_endpoint():
        pipe = pipes.describe_pipe(Name=PIPE_NAME)
        assert pipe["Source"] == queue_arn
        assert pipe["Enrichment"] == function_arn
        assert pipe["Target"] == machine_summary["stateMachineArn"]
        assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 10


def test_iam_configuration_matches_contract():
    iam = aws_client("iam")
    lambda_client = aws_client("lambda")
    secretsmanager = aws_client("secretsmanager")
    sqs = aws_client("sqs")
    stepfunctions = aws_client("stepfunctions")

    lambda_role = iam.get_role(RoleName=LAMBDA_ROLE_NAME)["Role"]
    sfn_role = iam.get_role(RoleName=STEP_FUNCTIONS_ROLE_NAME)["Role"]
    pipes_role = iam.get_role(RoleName=PIPES_ROLE_NAME)["Role"]

    assert lambda_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "lambda.amazonaws.com"
    assert sfn_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "states.amazonaws.com"
    assert pipes_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"] == "pipes.amazonaws.com"

    lambda_policy = inline_policy(LAMBDA_ROLE_NAME, "lambda-inline-policy")
    sfn_policy = inline_policy(STEP_FUNCTIONS_ROLE_NAME, "step-functions-inline-policy")
    pipe_policy = inline_policy(PIPES_ROLE_NAME, "eventbridge-pipes-inline-policy")

    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    secret_arn = get_secret(secretsmanager)["ARN"]
    function_arn = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)["Configuration"]["FunctionArn"]
    state_machine_arn = get_state_machine(stepfunctions)["stateMachineArn"]

    lambda_statements = {statement["Sid"]: statement for statement in lambda_policy["Statement"]}
    assert lambda_statements["SendToQueue"]["Resource"] == queue_arn
    assert lambda_statements["ArchivePayloads"]["Resource"] == "arn:aws:s3:::payments-ingest-bucket/*"
    assert lambda_statements["ReadDatabaseSecret"]["Resource"] == secret_arn

    sfn_statements = {statement["Sid"]: statement for statement in sfn_policy["Statement"]}
    assert sfn_statements["InvokeIngestLambda"]["Resource"] == function_arn

    pipe_statements = {statement["Sid"]: statement for statement in pipe_policy["Statement"]}
    assert pipe_statements["ConsumeIngestQueue"]["Resource"] == queue_arn
    assert pipe_statements["InvokeEnrichmentLambda"]["Resource"] == function_arn
    assert pipe_statements["StartStateMachineExecution"]["Resource"] == state_machine_arn


def test_storage_resources_match_contract():
    s3 = aws_client("s3")
    secretsmanager = aws_client("secretsmanager")
    rds = aws_client("rds")

    encryption = s3.get_bucket_encryption(Bucket=S3_BUCKET_NAME)
    public_access = s3.get_public_access_block(Bucket=S3_BUCKET_NAME)["PublicAccessBlockConfiguration"]
    ownership = s3.get_bucket_ownership_controls(Bucket=S3_BUCKET_NAME)["OwnershipControls"]["Rules"][0]
    bucket_policy = json.loads(s3.get_bucket_policy(Bucket=S3_BUCKET_NAME)["Policy"])

    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert public_access["BlockPublicAcls"] is True
    assert public_access["BlockPublicPolicy"] is True
    assert public_access["IgnorePublicAcls"] is True
    assert public_access["RestrictPublicBuckets"] is True
    assert ownership["ObjectOwnership"] == "BucketOwnerEnforced"

    deny_statement = next(statement for statement in bucket_policy["Statement"] if statement["Sid"] == "DenyInsecureTransport")
    assert deny_statement["Condition"]["Bool"]["aws:SecureTransport"] == "false"

    credentials = json.loads(secretsmanager.get_secret_value(SecretId=SECRET_NAME)["SecretString"])
    assert credentials["username"] == "appuser"
    assert len(credentials["password"]) == 20
    assert re.search(r"\d", credentials["password"])
    assert re.search(r"[^A-Za-z0-9]", credentials["password"])

    if uses_aws_endpoint():
        db_instance = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)["DBInstances"][0]
        assert db_instance["Engine"] == "postgres"
        assert db_instance["EngineVersion"] == "16.3"
        assert db_instance["DBInstanceClass"] == "db.t3.micro"
        assert db_instance["AllocatedStorage"] == 20
        assert db_instance["StorageType"] == "gp2"
        assert db_instance["PubliclyAccessible"] is False
        assert db_instance["StorageEncrypted"] is True

        ec2 = aws_client("ec2")
        subnet_group = rds.describe_db_subnet_groups(DBSubnetGroupName=DB_SUBNET_GROUP_NAME)["DBSubnetGroups"][0]
        assert {subnet["SubnetIdentifier"] for subnet in subnet_group["Subnets"]} == {
            get_subnet_by_name(ec2, PRIVATE_SUBNET_A_NAME)["SubnetId"],
            get_subnet_by_name(ec2, PRIVATE_SUBNET_B_NAME)["SubnetId"],
        }
