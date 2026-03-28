import json
import os
import subprocess
import time
import uuid
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlparse

import boto3
from botocore.session import get_session
import pytest


STATE_PATH = Path("state.json")


def collect_resources(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(collect_resources(child))
    return resources


def resources_by_type(state, resource_type):
    root_module = state["values"]["root_module"]
    return [
        resource
        for resource in collect_resources(root_module)
        if resource["type"] == resource_type and resource.get("mode", "managed") == "managed"
    ]


def get_single_resource(state, resource_type, name=None):
    resources = resources_by_type(state, resource_type)
    if name is not None:
        resources = [resource for resource in resources if resource["name"] == name]
    assert len(resources) == 1
    return resources[0]


def aws_region():
    return (
        os.environ.get("TF_VAR_aws_region")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    )


def inferred_endpoint(state, service_name):
    queue = get_single_resource(state, "aws_sqs_queue")["values"]
    parsed = urlparse(queue["url"])
    if not parsed.scheme or not parsed.netloc:
        return None

    host = parsed.hostname or ""
    labels = host.split(".")
    if len(labels) <= 1:
        return f"{parsed.scheme}://{parsed.netloc}"

    service_prefix = get_session().get_service_model(service_name).endpoint_prefix
    suffix = ".".join(labels[1:])
    if parsed.port is not None:
        return f"{parsed.scheme}://{service_prefix}.{suffix}:{parsed.port}"
    return f"{parsed.scheme}://{service_prefix}.{suffix}"


def client(state, service_name):
    kwargs = {
        "region_name": aws_region(),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    }
    if os.environ.get("AWS_SESSION_TOKEN"):
        kwargs["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]

    endpoint = inferred_endpoint(state, service_name)
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    return boto3.client(service_name, **kwargs)


def parse_policy_document(document):
    if isinstance(document, dict):
        return document
    return json.loads(unquote(document))


def flatten_actions(statement):
    actions = statement["Action"]
    return set(actions if isinstance(actions, list) else [actions])


def has_full_data_plane(state):
    return len(resources_by_type(state, "aws_db_instance")) == 1


def wait_for_queue_message(sqs_client, queue_url, expected_fragment, attempts=12, delay=2):
    for _ in range(attempts):
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if messages:
            message = messages[0]
            body = message["Body"]
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            if expected_fragment in body:
                return body
        time.sleep(delay)
    raise AssertionError(f"No se encontro mensaje con fragmento {expected_fragment!r}")


@lru_cache(maxsize=1)
def ensure_state_json():
    if STATE_PATH.exists():
        return

    if os.environ.get("GENERATE_TERRAFORM_ARTIFACTS") != "1":
        pytest.skip("state.json no existe; el harness debe generarlo después del apply")

    env = os.environ.copy()
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_REGION", aws_region())

    subprocess.run(["terraform", "init"], check=True, env=env)
    subprocess.run(
        ["terraform", "apply", "-input=false", "-auto-approve"],
        check=True,
        env=env,
    )
    with STATE_PATH.open("w") as state_file:
        subprocess.run(
            ["terraform", "show", "-json"],
            check=True,
            env=env,
            stdout=state_file,
        )


def load_state():
    ensure_state_json()
    return json.loads(STATE_PATH.read_text())


def test_persistence_and_network_posture():
    state = load_state()

    s3_client = client(state, "s3")
    rds_client = client(state, "rds")
    ec2_client = client(state, "ec2")
    secrets_client = client(state, "secretsmanager")

    bucket = get_single_resource(state, "aws_s3_bucket")["values"]["bucket"]
    encryption = s3_client.get_bucket_encryption(Bucket=bucket)
    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"

    versioning = s3_client.get_bucket_versioning(Bucket=bucket)
    assert versioning["Status"] == "Enabled"

    public_access = s3_client.get_public_access_block(Bucket=bucket)
    config = public_access["PublicAccessBlockConfiguration"]
    assert config["BlockPublicAcls"] is True
    assert config["BlockPublicPolicy"] is True
    assert config["IgnorePublicAcls"] is True
    assert config["RestrictPublicBuckets"] is True

    if has_full_data_plane(state):
        db = get_single_resource(state, "aws_db_instance")["values"]
        described_db = rds_client.describe_db_instances(DBInstanceIdentifier=db["identifier"])["DBInstances"][0]
        assert described_db["Engine"] == "postgres"
        assert described_db["EngineVersion"] == "15.4"
        assert described_db["DBInstanceClass"] == "db.t3.micro"
        assert described_db["AllocatedStorage"] == 20
        assert described_db["StorageEncrypted"] is True
        assert described_db["PubliclyAccessible"] is False
        assert described_db["BackupRetentionPeriod"] == 0
        assert described_db["DeletionProtection"] is False

    secret = get_single_resource(state, "aws_secretsmanager_secret")["values"]
    secret_value = secrets_client.get_secret_value(SecretId=secret["arn"])
    secret_payload = json.loads(secret_value["SecretString"])
    assert set(secret_payload.keys()) == {"username", "password"}
    assert secret_payload["username"] == "orders_admin"
    assert secret_payload["password"]

    vpc = get_single_resource(state, "aws_vpc")["values"]
    described_vpc = ec2_client.describe_vpcs(VpcIds=[vpc["id"]])["Vpcs"][0]
    assert described_vpc["CidrBlock"] == "10.0.0.0/16"
    assert ec2_client.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsSupport")["EnableDnsSupport"]["Value"] is True
    assert ec2_client.describe_vpc_attribute(VpcId=vpc["id"], Attribute="enableDnsHostnames")["EnableDnsHostnames"]["Value"] is True

    subnets = ec2_client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc["id"]]}]
    )["Subnets"]
    assert len(subnets) == 4
    assert sum(1 for subnet in subnets if subnet["MapPublicIpOnLaunch"]) == 2
    assert sum(1 for subnet in subnets if not subnet["MapPublicIpOnLaunch"]) == 2

    security_groups = ec2_client.describe_security_groups(
        GroupIds=[
            get_single_resource(state, "aws_security_group", "lambda")["values"]["id"],
            get_single_resource(state, "aws_security_group", "rds")["values"]["id"],
        ]
    )["SecurityGroups"]
    groups_by_id = {group["GroupId"]: group for group in security_groups}
    lambda_sg = groups_by_id[get_single_resource(state, "aws_security_group", "lambda")["values"]["id"]]
    rds_sg = groups_by_id[get_single_resource(state, "aws_security_group", "rds")["values"]["id"]]

    assert lambda_sg["IpPermissions"] == []
    assert any(
        perm.get("FromPort") == 5432
        and perm.get("ToPort") == 5432
        and any(pair["GroupId"] == lambda_sg["GroupId"] for pair in perm.get("UserIdGroupPairs", []))
        for perm in rds_sg["IpPermissions"]
    )
    assert all("0.0.0.0/0" not in [rng["CidrIp"] for rng in perm.get("IpRanges", [])] for perm in rds_sg["IpPermissions"])


def test_api_gateway_lambda_and_logs_are_wired_correctly():
    state = load_state()

    api_client = client(state, "apigateway")
    lambda_client = client(state, "lambda")
    logs_client = client(state, "logs")

    rest_api = get_single_resource(state, "aws_api_gateway_rest_api")["values"]
    resources = api_client.get_resources(restApiId=rest_api["id"])["items"]
    resources_by_path = {resource["path"]: resource for resource in resources}
    assert set(resources_by_path.keys()) >= {"/orders", "/orders/{id}", "/orders/{id}/notify"}

    expected_methods = {
        "/orders": "POST",
        "/orders/{id}": "GET",
        "/orders/{id}/notify": "POST",
    }
    for path, method in expected_methods.items():
        integration = api_client.get_integration(
            restApiId=rest_api["id"],
            resourceId=resources_by_path[path]["id"],
            httpMethod=method,
        )
        assert integration["type"] == "AWS_PROXY"
        assert ":lambda:path/2015-03-31/functions/" in integration["uri"]

    stages = api_client.get_stages(restApiId=rest_api["id"])["item"]
    assert len(stages) == 1
    assert stages[0]["stageName"] == "v1"

    api_lambda = get_single_resource(state, "aws_lambda_function", "api_handler")["values"]
    enrichment_lambda = get_single_resource(state, "aws_lambda_function", "enrichment_handler")["values"]

    for function in (api_lambda, enrichment_lambda):
        configuration = lambda_client.get_function_configuration(FunctionName=function["function_name"])
        assert configuration["Runtime"] == "nodejs20.x"
        assert configuration["Handler"] == "index.handler"
        assert configuration["MemorySize"] == 256
        assert configuration["Timeout"] == 15
        assert len(configuration["VpcConfig"]["SubnetIds"]) == 2
        assert len(configuration["VpcConfig"]["SecurityGroupIds"]) == 1

    for log_group_name in (
        f"/aws/lambda/{api_lambda['function_name']}",
        f"/aws/lambda/{enrichment_lambda['function_name']}",
    ):
        groups = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
        matching = [group for group in groups if group["logGroupName"] == log_group_name]
        assert len(matching) == 1
        assert matching[0]["retentionInDays"] == 14

    policy = json.loads(lambda_client.get_policy(FunctionName=api_lambda["function_name"])["Policy"])
    statements = policy["Statement"]
    assert any(
        statement["Principal"]["Service"] == "apigateway.amazonaws.com"
        and "/v1/" in statement["Condition"]["ArnLike"]["AWS:SourceArn"]
        for statement in statements
    )


def test_lambda_business_logic_with_boto3():
    state = load_state()

    lambda_client = client(state, "lambda")
    s3_client = client(state, "s3")
    sqs_client = client(state, "sqs")
    sns_client = client(state, "sns")

    api_lambda = get_single_resource(state, "aws_lambda_function", "api_handler")["values"]
    enrichment_lambda = get_single_resource(state, "aws_lambda_function", "enrichment_handler")["values"]
    bucket = get_single_resource(state, "aws_s3_bucket")["values"]["bucket"]
    queue_url = get_single_resource(state, "aws_sqs_queue")["values"]["url"]
    topic_arn = get_single_resource(state, "aws_sns_topic")["values"]["arn"]

    order_id = f"order-{uuid.uuid4().hex[:8]}"
    payload = {"id": order_id, "item": "attachment"}
    post_event = {
        "resource": "/orders",
        "httpMethod": "POST",
        "body": json.dumps(payload),
        "pathParameters": None,
        "requestContext": {"requestId": f"req-{order_id}"},
    }
    post_response = lambda_client.invoke(
        FunctionName=api_lambda["function_name"],
        InvocationType="RequestResponse",
        Payload=json.dumps(post_event).encode(),
    )
    post_payload = json.loads(post_response["Payload"].read())
    assert post_payload["statusCode"] == 201
    assert json.loads(post_payload["body"])["orderId"] == order_id

    object_response = s3_client.get_object(Bucket=bucket, Key=f"orders/{order_id}.json")
    stored_payload = json.loads(object_response["Body"].read().decode())
    assert stored_payload == payload

    queue_body = wait_for_queue_message(sqs_client, queue_url, "order.created")
    assert "orders.api" in queue_body

    get_event = {
        "resource": "/orders/{id}",
        "httpMethod": "GET",
        "body": None,
        "pathParameters": {"id": order_id},
        "requestContext": {"requestId": f"req-get-{order_id}"},
    }
    get_response = lambda_client.invoke(
        FunctionName=api_lambda["function_name"],
        InvocationType="RequestResponse",
        Payload=json.dumps(get_event).encode(),
    )
    get_payload = json.loads(get_response["Payload"].read())
    assert get_payload["statusCode"] == 200
    assert json.loads(get_payload["body"]) == payload

    temp_queue_name = f"notify-{uuid.uuid4().hex[:8]}"
    temp_queue_url = sqs_client.create_queue(QueueName=temp_queue_name)["QueueUrl"]
    temp_queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=temp_queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    subscription_arn = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=temp_queue_arn,
        ReturnSubscriptionArn=True,
    )["SubscriptionArn"]
    sqs_client.set_queue_attributes(
        QueueUrl=temp_queue_url,
        Attributes={
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action": "sqs:SendMessage",
                            "Resource": temp_queue_arn,
                            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                        }
                    ],
                }
            )
        },
    )

    notify_event = {
        "resource": "/orders/{id}/notify",
        "httpMethod": "POST",
        "body": json.dumps({"channel": "email"}),
        "pathParameters": {"id": order_id},
        "requestContext": {"requestId": f"req-notify-{order_id}"},
    }
    notify_response = lambda_client.invoke(
        FunctionName=api_lambda["function_name"],
        InvocationType="RequestResponse",
        Payload=json.dumps(notify_event).encode(),
    )
    notify_payload = json.loads(notify_response["Payload"].read())
    assert notify_payload["statusCode"] == 202

    notification_body = wait_for_queue_message(sqs_client, temp_queue_url, order_id)
    assert f"Order {order_id} notification" in notification_body

    sns_client.unsubscribe(SubscriptionArn=subscription_arn)
    sqs_client.delete_queue(QueueUrl=temp_queue_url)

    enrichment_event = {"body": json.dumps({"orderId": order_id})}
    enrichment_response = lambda_client.invoke(
        FunctionName=enrichment_lambda["function_name"],
        InvocationType="RequestResponse",
        Payload=json.dumps(enrichment_event).encode(),
    )
    enrichment_payload = json.loads(enrichment_response["Payload"].read())
    assert enrichment_payload["enriched"] is True
    assert enrichment_payload["originalBody"] == enrichment_event["body"]


def test_eventing_pipe_and_state_machine_wiring():
    state = load_state()

    sqs_client = client(state, "sqs")
    sns_client = client(state, "sns")
    events_client = client(state, "events")
    pipes_client = client(state, "pipes")
    sfn_client = client(state, "stepfunctions")

    queue = get_single_resource(state, "aws_sqs_queue")["values"]
    queue_url = queue["url"]
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "60"
    assert queue_attrs["SqsManagedSseEnabled"] == "true"

    topic = get_single_resource(state, "aws_sns_topic")["values"]
    topic_attrs = sns_client.get_topic_attributes(TopicArn=topic["arn"])["Attributes"]
    assert topic_attrs["KmsMasterKeyId"] == "alias/aws/sns"

    event_bus = get_single_resource(state, "aws_cloudwatch_event_bus")["values"]
    described_bus = events_client.describe_event_bus(Name=event_bus["name"])
    assert described_bus["Name"] == event_bus["name"]

    rule = get_single_resource(state, "aws_cloudwatch_event_rule")["values"]
    described_rule = events_client.describe_rule(Name=rule["name"], EventBusName=event_bus["name"])
    pattern = json.loads(described_rule["EventPattern"])
    assert pattern["source"] == ["orders.api"]
    assert pattern["detail-type"] == ["order.created"]

    targets = events_client.list_targets_by_rule(
        Rule=rule["name"],
        EventBusName=event_bus["name"],
    )["Targets"]
    assert len(targets) == 1
    assert targets[0]["Arn"] == queue["arn"]

    queue_policy = json.loads(queue_attrs["Policy"])
    statement = queue_policy["Statement"][0]
    assert statement["Principal"]["Service"] == "events.amazonaws.com"
    assert statement["Condition"]["ArnEquals"]["aws:SourceArn"] == described_rule["Arn"]

    if has_full_data_plane(state):
        pipe = get_single_resource(state, "aws_pipes_pipe")["values"]
        described_pipe = pipes_client.describe_pipe(Name=pipe["name"])
        assert described_pipe["Source"] == queue["arn"]
        assert described_pipe["Enrichment"] == get_single_resource(state, "aws_lambda_function", "enrichment_handler")["values"]["arn"]
        assert described_pipe["Target"] == get_single_resource(state, "aws_sfn_state_machine")["values"]["arn"]

    state_machine = get_single_resource(state, "aws_sfn_state_machine")["values"]
    described_state_machine = sfn_client.describe_state_machine(stateMachineArn=state_machine["arn"])
    assert described_state_machine["type"] == "STANDARD"
    definition = json.loads(described_state_machine["definition"])
    assert list(definition["States"].values()) == [{"Type": "Pass", "End": True}]


def test_iam_policies_are_tightly_scoped():
    state = load_state()

    iam_client = client(state, "iam")

    api_role = get_single_resource(state, "aws_iam_role", "api_handler")["values"]["name"]
    enrichment_role = get_single_resource(state, "aws_iam_role", "enrichment_handler")["values"]["name"]
    pipe_role = get_single_resource(state, "aws_iam_role", "pipe")["values"]["name"]
    sfn_role = get_single_resource(state, "aws_iam_role", "sfn")["values"]["name"]

    api_policy_name = get_single_resource(state, "aws_iam_role_policy", "api_handler")["values"]["name"]
    enrichment_policy_name = get_single_resource(state, "aws_iam_role_policy", "enrichment_handler")["values"]["name"]
    pipe_policy_name = get_single_resource(state, "aws_iam_role_policy", "pipe")["values"]["name"]

    api_policy = parse_policy_document(
        iam_client.get_role_policy(RoleName=api_role, PolicyName=api_policy_name)["PolicyDocument"]
    )
    api_statements = api_policy["Statement"]
    api_actions = set().union(*(flatten_actions(statement) for statement in api_statements))
    assert {"s3:PutObject", "s3:GetObject", "events:PutEvents", "sns:Publish", "secretsmanager:GetSecretValue"} <= api_actions

    s3_statement = next(statement for statement in api_statements if "s3:PutObject" in flatten_actions(statement))
    assert set(s3_statement["Resource"]) == {
        get_single_resource(state, "aws_s3_bucket")["values"]["arn"],
        f"{get_single_resource(state, 'aws_s3_bucket')['values']['arn']}/*",
    }

    events_statement = next(statement for statement in api_statements if "events:PutEvents" in flatten_actions(statement))
    assert events_statement["Resource"] == get_single_resource(state, "aws_cloudwatch_event_bus")["values"]["arn"]

    sns_statement = next(statement for statement in api_statements if "sns:Publish" in flatten_actions(statement))
    assert sns_statement["Resource"] == get_single_resource(state, "aws_sns_topic")["values"]["arn"]

    secret_statement = next(statement for statement in api_statements if "secretsmanager:GetSecretValue" in flatten_actions(statement))
    assert secret_statement["Resource"] == get_single_resource(state, "aws_secretsmanager_secret")["values"]["arn"]

    enrichment_policy = parse_policy_document(
        iam_client.get_role_policy(RoleName=enrichment_role, PolicyName=enrichment_policy_name)["PolicyDocument"]
    )
    enrichment_actions = set().union(*(flatten_actions(statement) for statement in enrichment_policy["Statement"]))
    assert enrichment_actions <= {
        "logs:CreateLogStream",
        "logs:PutLogEvents",
    }

    pipe_policy = parse_policy_document(
        iam_client.get_role_policy(RoleName=pipe_role, PolicyName=pipe_policy_name)["PolicyDocument"]
    )
    pipe_statements = pipe_policy["Statement"]
    sqs_statement = next(statement for statement in pipe_statements if "sqs:ReceiveMessage" in flatten_actions(statement))
    lambda_statement = next(statement for statement in pipe_statements if "lambda:InvokeFunction" in flatten_actions(statement))
    sfn_statement = next(statement for statement in pipe_statements if "states:StartExecution" in flatten_actions(statement))
    assert sqs_statement["Resource"] == get_single_resource(state, "aws_sqs_queue")["values"]["arn"]
    assert lambda_statement["Resource"] == get_single_resource(state, "aws_lambda_function", "enrichment_handler")["values"]["arn"]
    assert sfn_statement["Resource"] == get_single_resource(state, "aws_sfn_state_machine")["values"]["arn"]

    assert iam_client.list_role_policies(RoleName=sfn_role)["PolicyNames"] == []
