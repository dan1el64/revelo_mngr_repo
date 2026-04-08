import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.loaders import create_loader
from botocore.exceptions import NoCredentialsError


STACK_NAME = "ThreeTierInternalWebAppStack"
REGION = os.getenv("AWS_REGION", "us-east-1")


def aws_client(service_name: str):
    return boto3.client(service_name, region_name=REGION)


def expected_cloudfront_alias_hosted_zone_id() -> str:
    partition = aws_client("sts").get_caller_identity()["Arn"].split(":")[1]
    if partition == "aws":
        return "Z2FDTNDATAQYW2"

    endpoints = create_loader().load_data("endpoints")
    partition_data = next(candidate for candidate in endpoints["partitions"] if candidate["partition"] == partition)
    dns_suffix = partition_data["dnsSuffix"]
    if dns_suffix == "amazonaws.com.cn":
        return "Z3RFFRIM2A3IF5"
    if dns_suffix == "amazonaws.com":
        return "Z2FDTNDATAQYW2"
    raise AssertionError(f"unsupported partition for CloudFront alias hosted zone id: {partition}")


def integration_environment_ready() -> bool:
    return bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))


def require_integration_environment() -> None:
    if not integration_environment_ready():
        raise AssertionError("AWS credentials are required for integration tests")


def get_stack_resources() -> list[dict]:
    cloudformation = aws_client("cloudformation")
    try:
        stack = cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
    except NoCredentialsError as exc:
        raise AssertionError(f"stack {STACK_NAME} is not deployed or credentials are unavailable: {exc}") from exc

    status = stack["StackStatus"]
    if not status.endswith("_COMPLETE") or status.startswith("ROLLBACK"):
        raise AssertionError(f"stack {STACK_NAME} is not in a usable deployed state: {status}")

    return cloudformation.describe_stack_resources(StackName=STACK_NAME)["StackResources"]


def resources_of_type(resources: list[dict], resource_type: str) -> list[dict]:
    return [resource for resource in resources if resource["ResourceType"] == resource_type]


def physical_id(resources: list[dict], resource_type: str, logical_id_fragment: str) -> str:
    for resource in resources_of_type(resources, resource_type):
        if logical_id_fragment in resource["LogicalResourceId"]:
            return resource["PhysicalResourceId"]
    raise AssertionError(f"resource {resource_type} with logical id containing {logical_id_fragment} not found")


def load_balancer_description(resources: list[dict]):
    elbv2_client = aws_client("elbv2")
    load_balancer_arn = resources_of_type(resources, "AWS::ElasticLoadBalancingV2::LoadBalancer")[0]["PhysicalResourceId"]
    return elbv2_client.describe_load_balancers(LoadBalancerArns=[load_balancer_arn])["LoadBalancers"][0]


def alb_base_url(resources: list[dict]) -> str:
    dns_name = load_balancer_description(resources)["DNSName"]
    if dns_name.startswith("http://") or dns_name.startswith("https://"):
        return dns_name.rstrip("/")
    # When a custom endpoint is configured, the ALB is served through that port, not port 80.
    # Extract the port from AWS_ENDPOINT so the URL resolves correctly.
    endpoint = os.getenv("AWS_ENDPOINT")
    if endpoint:
        try:
            parsed = urllib.parse.urlparse(endpoint)
            if parsed.port and parsed.port not in (80, 443):
                return f"http://{dns_name}:{parsed.port}".rstrip("/")
        except Exception:
            pass
    return f"http://{dns_name}".rstrip("/")


def http_json_request(url: str, method: str = "GET", body: str | None = None, attempts: int = 10) -> tuple[int, dict]:
    data = body.encode("utf-8") if body is not None else None
    headers = {"content-type": "application/json"} if body is not None else {}

    last_error = None
    for _ in range(attempts):
        request = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read().decode("utf-8")
                return response.status, json.loads(payload)
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8")
            return exc.code, json.loads(payload)
        except urllib.error.URLError as exc:
            last_error = exc
            time.sleep(1)

    raise AssertionError(f"failed to reach ALB endpoint {url}: {last_error}")


def invoke_json_lambda(lambda_client, function_name: str, payload: dict) -> dict:
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    assert response["StatusCode"] == 200
    execution_payload = json.loads(response["Payload"].read().decode("utf-8"))
    if "FunctionError" in response:
        raise AssertionError(f"lambda {function_name} returned an error payload: {execution_payload}")
    return execution_payload


def invoke_backend_route(lambda_client, function_name: str, method: str, path: str, body: str | None = None) -> tuple[int, dict]:
    payload = {"httpMethod": method, "path": path}
    if body is not None:
        payload["body"] = body
    execution_payload = invoke_json_lambda(lambda_client, function_name, payload)
    return execution_payload["statusCode"], json.loads(execution_payload["body"])


def queue_observed_enqueue(sqs_client, queue_url: str, baseline: dict, attempts: int = 10) -> bool:
    attribute_names = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]
    for _ in range(attempts):
        attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=attribute_names,
        )["Attributes"]
        if any(
            int(attributes.get(name, "0")) > int(baseline.get(name, "0"))
            for name in attribute_names
        ):
            return True

        import time
        time.sleep(0.5)

    return False


def test_deployed_stack_has_expected_resource_inventory():
    require_integration_environment()

    resources = get_stack_resources()
    assert resources
    ec2_client = aws_client("ec2")
    vpc_id = physical_id(resources, "AWS::EC2::VPC", "ApplicationVpc")
    load_balancer_arn = physical_id(resources, "AWS::ElasticLoadBalancingV2::LoadBalancer", "TrafficEntryLoadBalancer")
    target_group_arn = physical_id(resources, "AWS::ElasticLoadBalancingV2::TargetGroup", "BackendTargetGroup")
    assert len(resources_of_type(resources, "AWS::ElasticLoadBalancingV2::LoadBalancer")) == 1
    assert load_balancer_arn
    assert target_group_arn

    nat_gateways = resources_of_type(resources, "AWS::EC2::NatGateway")
    assert len(nat_gateways) == 1
    nat_gateway_id = nat_gateways[0]["PhysicalResourceId"]
    nat_gateway = ec2_client.describe_nat_gateways(NatGatewayIds=[nat_gateway_id])["NatGateways"][0]
    nat_subnet_id = nat_gateway["SubnetId"]
    nat_subnet = ec2_client.describe_subnets(SubnetIds=[nat_subnet_id])["Subnets"][0]
    assert nat_subnet["MapPublicIpOnLaunch"] is True
    assert vpc_id


def test_deployed_lambda_queue_and_database_configuration():
    require_integration_environment()

    resources = get_stack_resources()

    lambda_client = aws_client("lambda")
    sqs_client = aws_client("sqs")
    ec2_client = aws_client("ec2")
    logs_client = aws_client("logs")
    secretsmanager_client = aws_client("secretsmanager")
    base_url = None
    try:
        base_url = alb_base_url(resources)
    except Exception:
        base_url = None

    backend_function_name = physical_id(resources, "AWS::Lambda::Function", "BackendFunction")
    enrichment_function_name = physical_id(resources, "AWS::Lambda::Function", "EnrichmentFunction")
    queue_url = physical_id(resources, "AWS::SQS::Queue", "OrderQueue")
    database_security_group_id = physical_id(resources, "AWS::EC2::SecurityGroup", "DatabaseSecurityGroup")

    backend_config = lambda_client.get_function_configuration(FunctionName=backend_function_name)
    enrichment_config = lambda_client.get_function_configuration(FunctionName=enrichment_function_name)

    for config in (backend_config, enrichment_config):
        assert config["Runtime"] == "nodejs20.x"
        assert config["MemorySize"] == 512
        assert config["Timeout"] == 10
        assert config["VpcConfig"]["SubnetIds"]
        assert config["VpcConfig"]["SecurityGroupIds"]
        log_groups = logs_client.describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{config['FunctionName']}")["logGroups"]
        matching_log_group = next(
            (
                group
                for group in log_groups
                if group["logGroupName"] == f"/aws/lambda/{config['FunctionName']}"
            ),
            None,
        )
        if matching_log_group is None:
            log_group_fragment = "BackendLogGroup" if config["FunctionName"] == backend_function_name else "EnrichmentLogGroup"
            log_group_name = physical_id(resources, "AWS::Logs::LogGroup", log_group_fragment)
            matching_log_group = next(
                (
                    group
                    for group in logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
                    if group["logGroupName"] == log_group_name
                ),
                None,
            )
        assert matching_log_group is not None
        if "retentionInDays" in matching_log_group:
            assert matching_log_group["retentionInDays"] == 14

    backend_env = backend_config["Environment"]["Variables"]
    assert backend_env["ORDER_QUEUE_URL"] == queue_url
    assert "DB_SECRET_ARN" in backend_env
    assert "DB_HOST" in backend_env
    assert backend_env["DB_PORT"] == "5432"
    secret_value = secretsmanager_client.get_secret_value(SecretId=backend_env["DB_SECRET_ARN"])
    secret_payload = json.loads(secret_value["SecretString"])
    assert secret_payload["username"] == "appuser"
    assert backend_env["DB_HOST"]

    backend_security_group_id = backend_config["VpcConfig"]["SecurityGroupIds"][0]
    enrichment_security_group_id = enrichment_config["VpcConfig"]["SecurityGroupIds"][0]
    assert enrichment_security_group_id == backend_security_group_id
    db_security_group = ec2_client.describe_security_groups(GroupIds=[database_security_group_id])["SecurityGroups"][0]
    backend_security_group = ec2_client.describe_security_groups(GroupIds=[backend_security_group_id])["SecurityGroups"][0]
    assert any(
        permission.get("FromPort") == 5432
        and permission.get("ToPort") == 5432
        and any(
            pair.get("GroupId") == backend_security_group_id
            for pair in permission.get("UserIdGroupPairs", [])
        )
        for permission in db_security_group.get("IpPermissions", [])
    )
    assert any(
        permission.get("IpProtocol")
        for permission in backend_security_group.get("IpPermissionsEgress", [])
    )
    assert backend_security_group.get("IpPermissions", []) == []

    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "MessageRetentionPeriod",
            "VisibilityTimeout",
            "SqsManagedSseEnabled",
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    assert queue_attributes["MessageRetentionPeriod"] == "345600"
    assert queue_attributes["VisibilityTimeout"] == "30"
    if "SqsManagedSseEnabled" in queue_attributes:
        assert queue_attributes["SqsManagedSseEnabled"].lower() == "true"
    else:
        raise AssertionError("SQS-managed SSE attribute was not reported for the queue")

    health_status, health_payload = invoke_backend_route(
        lambda_client,
        backend_function_name,
        "GET",
        "/health",
    )
    root_status, root_payload = invoke_backend_route(
        lambda_client,
        backend_function_name,
        "GET",
        "/",
    )
    assert health_status == 200
    assert health_payload == {"status": "ok"}
    assert root_status == 200
    assert root_payload == {"region": REGION}
    if base_url is not None:
        alb_health_status, alb_health_payload = http_json_request(f"{base_url}/health")
        assert alb_health_status == 200
        assert alb_health_payload == {"status": "ok"}

    order_body = json.dumps({"orderId": str(uuid.uuid4())})
    order_status, order_payload = invoke_backend_route(
        lambda_client,
        backend_function_name,
        "POST",
        "/orders",
        body=order_body,
    )
    assert order_status == 202
    assert order_payload == {"status": "accepted"}
    assert queue_observed_enqueue(sqs_client, queue_url, queue_attributes)
    received_message = None
    for _ in range(10):
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            VisibilityTimeout=0,
        )
        messages = response.get("Messages", [])
        if messages:
            received_message = messages[0]
            break
        time.sleep(1)
    assert received_message is not None
    assert json.loads(received_message["Body"]) == json.loads(order_body)


def test_deployed_frontend_and_analytics_configuration():
    require_integration_environment()

    resources = get_stack_resources()

    s3_client = aws_client("s3")
    route53_client = aws_client("route53")
    frontend_bucket = physical_id(resources, "AWS::S3::Bucket", "FrontendBucket")
    distribution_id = physical_id(resources, "AWS::CloudFront::Distribution", "FrontendDistribution")
    analytics_bucket = physical_id(resources, "AWS::S3::Bucket", "AnalyticsInputBucket")
    results_bucket = physical_id(resources, "AWS::S3::Bucket", "AthenaResultsBucket")
    hosted_zone_id = physical_id(resources, "AWS::Route53::HostedZone", "ApplicationHostedZone")
    workgroup_name = physical_id(resources, "AWS::Athena::WorkGroup", "AnalyticsWorkGroup")
    crawler_name = physical_id(resources, "AWS::Glue::Crawler", "AnalyticsCrawler")
    database_name = physical_id(resources, "AWS::Glue::Database", "AnalyticsDatabase")
    website_config = s3_client.get_bucket_website(Bucket=frontend_bucket)
    bucket_policy = json.loads(s3_client.get_bucket_policy(Bucket=frontend_bucket)["Policy"])
    public_access_block = s3_client.get_public_access_block(Bucket=frontend_bucket)["PublicAccessBlockConfiguration"]
    analytics_public_access_block = s3_client.get_public_access_block(Bucket=analytics_bucket)["PublicAccessBlockConfiguration"]
    results_public_access_block = s3_client.get_public_access_block(Bucket=results_bucket)["PublicAccessBlockConfiguration"]
    assert website_config["IndexDocument"]["Suffix"] == "index.html"
    assert website_config["ErrorDocument"]["Key"] == "index.html"
    statement = bucket_policy["Statement"][0]
    assert statement["Principal"] == {"Service": "cloudfront.amazonaws.com"}
    assert statement["Action"] == "s3:GetObject"
    assert public_access_block == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }
    assert analytics_public_access_block == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }
    assert results_public_access_block == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }

    hosted_zone = route53_client.get_hosted_zone(Id=hosted_zone_id)["HostedZone"]
    assert hosted_zone["Config"]["PrivateZone"] is False
    record_sets = route53_client.list_resource_record_sets(HostedZoneId=hosted_zone_id)["ResourceRecordSets"]
    alias_records = [record for record in record_sets if record["Type"] == "A" and "AliasTarget" in record]
    assert len(alias_records) == 1
    hosted_zone_name = hosted_zone["Name"].rstrip(".")
    alias_record_name = alias_records[0]["Name"].rstrip(".")
    assert alias_record_name == hosted_zone_name or alias_record_name.endswith(f".{hosted_zone_name}")
    assert alias_records[0]["AliasTarget"]["HostedZoneId"] == expected_cloudfront_alias_hosted_zone_id()
    assert alias_records[0]["AliasTarget"]["DNSName"]


def test_deployed_async_pipeline_configuration():
    require_integration_environment()

    resources = get_stack_resources()
    pipe_resources = resources_of_type(resources, "AWS::Pipes::Pipe")
    state_machine_resources = resources_of_type(resources, "AWS::StepFunctions::StateMachine")
    assert len(pipe_resources) == 1
    assert len(state_machine_resources) == 1

    lambda_client = aws_client("lambda")
    stepfunctions_client = aws_client("stepfunctions")
    queue_arn = physical_id(resources, "AWS::SQS::Queue", "OrderQueue")
    state_machine_arn = physical_id(resources, "AWS::StepFunctions::StateMachine", "ProcessingStateMachine")
    pipe_name = physical_id(resources, "AWS::Pipes::Pipe", "OrdersPipe")
    enrichment_function_name = physical_id(resources, "AWS::Lambda::Function", "EnrichmentFunction")
    enrichment_function_arn = lambda_client.get_function_configuration(FunctionName=enrichment_function_name)["FunctionArn"]
    state_machine = stepfunctions_client.describe_state_machine(stateMachineArn=state_machine_arn)
    definition = json.loads(state_machine["definition"])
    assert state_machine["type"] == "STANDARD"
    assert definition["TimeoutSeconds"] == 30
    assert len(definition["States"]) == 1
    only_state = definition["States"][definition["StartAt"]]
    assert only_state["Type"] == "Task"
    assert only_state["End"] is True
    assert only_state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert only_state["Parameters"]["FunctionName"] == enrichment_function_arn
    assert only_state["Parameters"]["Payload.$"] == "$"


def test_backend_lambda_returns_404_for_unknown_route():
    """Negative-path: backend Lambda must return 404 for an unrecognized path."""
    require_integration_environment()

    resources = get_stack_resources()
    lambda_client = aws_client("lambda")
    backend_function_name = physical_id(resources, "AWS::Lambda::Function", "BackendFunction")

    status, payload = invoke_backend_route(lambda_client, backend_function_name, "GET", "/not-a-real-path")
    assert status == 404
    assert "error" in payload

    status2, payload2 = invoke_backend_route(lambda_client, backend_function_name, "DELETE", "/orders")
    assert status2 == 404
    assert "error" in payload2
