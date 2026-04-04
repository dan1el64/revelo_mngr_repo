import json
import os
import time
from datetime import datetime

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


STACK_NAME = "ThreeTierPocStack"
SERVICE_NAME = "rapid-prototype-poc"


def _region() -> str:
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def _client(service_name: str):
    kwargs = {
        "region_name": _region(),
    }
    access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key_id is not None and secret_access_key is not None:
        kwargs["aws_access_key_id"] = access_key_id
        kwargs["aws_secret_access_key"] = secret_access_key
    elif access_key_id is not None or secret_access_key is not None:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be provided together")
    return boto3.client(service_name, **kwargs)


def _stack_resources() -> list[dict]:
    client = _client("cloudformation")
    resources = []
    paginator = client.get_paginator("list_stack_resources")
    for page in paginator.paginate(StackName=STACK_NAME):
        resources.extend(page["StackResourceSummaries"])
    return resources


def _single_resource(resources: list[dict], resource_type: str) -> dict:
    matches = [resource for resource in resources if resource["ResourceType"] == resource_type]
    assert len(matches) == 1, f"Expected exactly one {resource_type}, found {len(matches)}"
    return matches[0]


def _template_resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def _invoke_lambda(function_name: str, payload: dict) -> dict:
    lambda_client = _client("lambda")
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    body = response["Payload"].read().decode("utf-8")
    if response.get("FunctionError"):
        raise AssertionError(f"Lambda {function_name} failed: {body}")
    return json.loads(body) if body else {}


def _queue_depth(queue_url: str) -> tuple[int, int]:
    attributes = _client("sqs").get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return (
        int(attributes.get("ApproximateNumberOfMessages", "0")),
        int(attributes.get("ApproximateNumberOfMessagesNotVisible", "0")),
    )


def _resolve_queue_identifiers(resource: dict) -> tuple[str, str]:
    sqs_client = _client("sqs")
    physical_id = resource["PhysicalResourceId"]

    if physical_id.startswith("https://"):
        queue_url = physical_id
    elif physical_id.startswith("arn:"):
        queue_name = physical_id.rsplit(":", 1)[-1]
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    else:
        queue_url = None
        candidates = [physical_id]
        if "/" in physical_id:
            candidates.append(physical_id.rsplit("/", 1)[-1])
        if ":" in physical_id:
            candidates.append(physical_id.rsplit(":", 1)[-1])

        for candidate in candidates:
            try:
                queue_url = sqs_client.get_queue_url(QueueName=candidate)["QueueUrl"]
                break
            except ClientError:
                continue

        if queue_url is None:
            listed = sqs_client.list_queues().get("QueueUrls", [])
            for candidate in candidates:
                suffix = "/" + candidate
                match = next((url for url in listed if url.endswith(suffix)), None)
                if match:
                    queue_url = match
                    break

        if queue_url is None:
            raise AssertionError(f"Unable to resolve queue url from physical id {physical_id}")

    queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return queue_url, queue_arn


@pytest.fixture(scope="session")
def deployment() -> dict:
    try:
        cloudformation = _client("cloudformation")
        stack = cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
        resources = _stack_resources()
    except (ClientError, BotoCoreError, NoCredentialsError) as error:
        pytest.fail(f"Deployed stack {STACK_NAME} is not available: {error}")

    if stack["StackStatus"] != "CREATE_COMPLETE":
        pytest.fail(f"Stack {STACK_NAME} is not ready for integration tests: {stack['StackStatus']}")

    try:
        queue_url, queue_arn = _resolve_queue_identifiers(_single_resource(resources, "AWS::SQS::Queue"))
    except AssertionError as error:
        pytest.fail(f"Stack {STACK_NAME} does not expose the expected deployed resources: {error}")

    outputs = {output["OutputKey"]: output["OutputValue"] for output in stack.get("Outputs", [])}
    assert outputs.get("HttpApiEndpoint"), "Expected HttpApiEndpoint stack output"

    template_body = cloudformation.get_template(StackName=STACK_NAME)["TemplateBody"]
    template = json.loads(template_body) if isinstance(template_body, str) else template_body

    return {
        "stack": stack,
        "resources": resources,
        "template": template,
        "api_endpoint": outputs["HttpApiEndpoint"].rstrip("/"),
        "bucket_name": _single_resource(resources, "AWS::S3::Bucket")["PhysicalResourceId"],
        "queue_url": queue_url,
        "queue_arn": queue_arn,
        "vpc_id": _single_resource(resources, "AWS::EC2::VPC")["PhysicalResourceId"],
        "vpce_id": _single_resource(resources, "AWS::EC2::VPCEndpoint")["PhysicalResourceId"],
        "backend_lambda_name": next(
            resource["PhysicalResourceId"]
            for resource in resources
            if resource["ResourceType"] == "AWS::Lambda::Function" and resource["LogicalResourceId"].startswith("BackendLambda")
        ),
        "frontend_lambda_name": next(
            resource["PhysicalResourceId"]
            for resource in resources
            if resource["ResourceType"] == "AWS::Lambda::Function" and resource["LogicalResourceId"].startswith("FrontendLambda")
        ),
        "schema_lambda_name": next(
            resource["PhysicalResourceId"]
            for resource in resources
            if resource["ResourceType"] == "AWS::Lambda::Function" and resource["LogicalResourceId"].startswith("SchemaLambda")
        ),
        "backend_sg_id": next(
            resource["PhysicalResourceId"]
            for resource in resources
            if resource["ResourceType"] == "AWS::EC2::SecurityGroup" and resource["LogicalResourceId"].startswith("BackendSecurityGroup")
        ),
        "database_sg_id": next(
            resource["PhysicalResourceId"]
            for resource in resources
            if resource["ResourceType"] == "AWS::EC2::SecurityGroup" and resource["LogicalResourceId"].startswith("DatabaseSecurityGroup")
        ),
    }


def test_deployed_http_api_contract_and_runtime(deployment: dict) -> None:
    lambda_client = _client("lambda")
    routes = _template_resources_by_type(deployment["template"], "AWS::ApiGatewayV2::Route")
    route_keys = {route["Properties"]["RouteKey"] for route in routes.values()}

    assert route_keys == {"GET /", "ANY /api/{proxy+}"}
    assert len(_template_resources_by_type(deployment["template"], "AWS::ApiGatewayV2::Integration")) == 2
    assert deployment["api_endpoint"]

    frontend_cfg = lambda_client.get_function_configuration(FunctionName=deployment["frontend_lambda_name"])
    backend_cfg = lambda_client.get_function_configuration(FunctionName=deployment["backend_lambda_name"])
    schema_cfg = lambda_client.get_function_configuration(FunctionName=deployment["schema_lambda_name"])

    assert frontend_cfg["Runtime"] == "nodejs20.x"
    assert frontend_cfg["MemorySize"] == 256
    assert frontend_cfg["Timeout"] == 5
    assert "VpcConfig" not in frontend_cfg or not frontend_cfg["VpcConfig"].get("SubnetIds")

    assert backend_cfg["Runtime"] == "nodejs20.x"
    assert backend_cfg["MemorySize"] == 512
    assert backend_cfg["Timeout"] == 15
    assert len(backend_cfg["VpcConfig"]["SubnetIds"]) == 2
    assert deployment["backend_sg_id"] in backend_cfg["VpcConfig"]["SecurityGroupIds"]

    assert schema_cfg["Runtime"] == "nodejs20.x"
    assert schema_cfg["MemorySize"] == 512
    assert schema_cfg["Timeout"] == 60
    assert len(schema_cfg["VpcConfig"]["SubnetIds"]) == 2
    assert deployment["backend_sg_id"] in schema_cfg["VpcConfig"]["SecurityGroupIds"]

    frontend_payload = _invoke_lambda(deployment["frontend_lambda_name"], {})
    assert frontend_payload["statusCode"] == 200
    assert "text/html" in frontend_payload["headers"]["content-type"]
    assert 'fetch("/api/health")' in frontend_payload["body"]
    assert 'target.textContent=JSON.stringify(data,null,2)' in frontend_payload["body"]

    health_payload = _invoke_lambda(
        deployment["backend_lambda_name"],
        {
            "version": "2.0",
            "rawPath": "/api/health",
            "body": "",
            "isBase64Encoded": False,
            "requestContext": {"http": {"method": "GET", "path": "/api/health"}},
        },
    )
    assert health_payload["statusCode"] == 200
    body = json.loads(health_payload["body"])
    assert body["service"] == SERVICE_NAME
    datetime.fromisoformat(body["timestamp"].replace("Z", "+00:00"))


def test_post_items_archives_raw_payload_in_s3(deployment: dict) -> None:
    s3_client = _client("s3")
    sqs_client = _client("sqs")
    raw_payload = json.dumps({"kind": "integration", "value": 7}, separators=(",", ":"))
    baseline_visible, baseline_not_visible = _queue_depth(deployment["queue_url"])

    payload = _invoke_lambda(
        deployment["backend_lambda_name"],
        {
            "version": "2.0",
            "rawPath": "/api/items",
            "body": raw_payload,
            "isBase64Encoded": False,
            "requestContext": {"http": {"method": "POST", "path": "/api/items"}},
        },
    )
    assert payload["statusCode"] == 201

    response = json.loads(payload["body"])
    assert response["status"] == "created"
    assert response["id"]
    assert response["s3Key"].startswith("items/")

    for _ in range(20):
        try:
            object_response = s3_client.get_object(Bucket=deployment["bucket_name"], Key=response["s3Key"])
            archived_body = object_response["Body"].read().decode("utf-8")
            break
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise
            time.sleep(2)
    else:
        raise AssertionError("The archived S3 object was not found after POST /api/items")

    assert archived_body == raw_payload

    queue_enqueued = False
    for _ in range(15):
        visible, not_visible = _queue_depth(deployment["queue_url"])
        if visible + not_visible > baseline_visible + baseline_not_visible:
            queue_enqueued = True
            break

        message_batch = sqs_client.receive_message(
            QueueUrl=deployment["queue_url"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,
            WaitTimeSeconds=1,
        )
        messages = message_batch.get("Messages", [])
        if messages:
            body = json.loads(messages[0]["Body"])
            if body.get("id") == response["id"] and body.get("s3Key") == response["s3Key"]:
                queue_enqueued = True
                break
        time.sleep(1)

    assert queue_enqueued, "POST /api/items did not enqueue the expected SQS message"


def test_deployed_async_data_store_and_network_contract(deployment: dict) -> None:
    lambda_client = _client("lambda")
    s3_client = _client("s3")
    ec2_client = _client("ec2")

    template = deployment["template"]
    resource_types = [resource["ResourceType"] for resource in deployment["resources"]]
    assert resource_types.count("AWS::RDS::DBInstance") == 1
    assert resource_types.count("AWS::SecretsManager::Secret") == 1
    assert resource_types.count("AWS::StepFunctions::StateMachine") == 1
    assert resource_types.count("AWS::Pipes::Pipe") == 1

    mappings = lambda_client.list_event_source_mappings(FunctionName=deployment["backend_lambda_name"])["EventSourceMappings"]
    assert len(mappings) == 1
    assert mappings[0]["BatchSize"] == 5
    assert mappings[0]["EventSourceArn"] == deployment["queue_arn"]

    sqs_payload = _invoke_lambda(
        deployment["backend_lambda_name"],
        {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps({"id": "integration-item"}),
                }
            ]
        },
    )
    assert isinstance(sqs_payload, dict)
    if "batchItemFailures" in sqs_payload:
        assert sqs_payload["batchItemFailures"] == []

    versioning = s3_client.get_bucket_versioning(Bucket=deployment["bucket_name"])
    assert versioning["Status"] == "Enabled"

    vpc = ec2_client.describe_vpcs(VpcIds=[deployment["vpc_id"]])["Vpcs"][0]
    subnets = ec2_client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [deployment["vpc_id"]]}])["Subnets"]
    nat_gateways = ec2_client.describe_nat_gateways(Filter=[{"Name": "vpc-id", "Values": [deployment["vpc_id"]]}])["NatGateways"]
    vpc_endpoint = ec2_client.describe_vpc_endpoints(VpcEndpointIds=[deployment["vpce_id"]])["VpcEndpoints"][0]
    security_groups = ec2_client.describe_security_groups(
        GroupIds=[deployment["backend_sg_id"], deployment["database_sg_id"]]
    )["SecurityGroups"]
    security_groups_by_id = {group["GroupId"]: group for group in security_groups}
    vpc_resource = next(iter(_template_resources_by_type(template, "AWS::EC2::VPC").values()))
    template_security_group_egress = _template_resources_by_type(template, "AWS::EC2::SecurityGroupEgress")
    template_security_group_ingress = _template_resources_by_type(template, "AWS::EC2::SecurityGroupIngress")

    assert vpc_resource["Properties"]["EnableDnsSupport"] is True
    assert vpc_resource["Properties"]["EnableDnsHostnames"] is True
    if "EnableDnsSupport" in vpc:
        assert vpc["EnableDnsSupport"] is True
    if "EnableDnsHostnames" in vpc:
        assert vpc["EnableDnsHostnames"] is True
    assert len(subnets) == 4
    assert len({subnet["AvailabilityZone"] for subnet in subnets}) == 2
    assert sum(1 for subnet in subnets if subnet.get("MapPublicIpOnLaunch") is True) == 2
    assert sum(1 for subnet in subnets if subnet.get("MapPublicIpOnLaunch") is False) == 2
    assert len(nat_gateways) == 1

    backend_sg = security_groups_by_id[deployment["backend_sg_id"]]
    database_sg = security_groups_by_id[deployment["database_sg_id"]]
    backend_to_database = [
        resource["Properties"]
        for resource in template_security_group_egress.values()
        if resource["Properties"].get("FromPort") == 5432
    ]
    database_from_backend = [
        resource["Properties"]
        for resource in template_security_group_ingress.values()
        if resource["Properties"].get("FromPort") == 5432
    ]

    assert backend_sg["IpPermissions"] == []
    assert len(backend_to_database) == 1
    assert len(database_from_backend) == 1
    if database_sg.get("IpPermissions"):
        assert len(database_sg["IpPermissions"]) == 1
        assert database_sg["IpPermissions"][0]["FromPort"] == 5432
        assert database_sg["IpPermissions"][0]["ToPort"] == 5432

    assert vpc_endpoint["ServiceName"] == f"com.amazonaws.{_region()}.secretsmanager"
    assert vpc_endpoint["VpcEndpointType"] == "Interface"
    assert vpc_endpoint["PrivateDnsEnabled"] is True
