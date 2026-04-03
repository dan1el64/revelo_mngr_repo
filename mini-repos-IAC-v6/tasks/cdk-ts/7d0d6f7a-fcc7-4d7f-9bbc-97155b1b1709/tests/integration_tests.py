import json
import os
import time
import urllib.error
import urllib.request

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError


STACK_NAME = "SecurityPostureStack"
REGION = os.getenv("AWS_REGION", "us-east-1")


def endpoint_override():
    for key in sorted(os.environ):
        if key.startswith("AWS_") and "ENDPOINT" in key:
            value = os.environ.get(key)
            if value:
                return value
    return None


def client(service_name):
    kwargs = {
        "region_name": REGION,
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    }
    override = endpoint_override()
    if override:
        kwargs["endpoint_url"] = override
    return boto3.client(service_name, **kwargs)


@pytest.fixture(scope="session")
def stack_resources():
    cloudformation = client("cloudformation")
    try:
        paginator = cloudformation.get_paginator("list_stack_resources")
        summaries = []
        for page in paginator.paginate(StackName=STACK_NAME):
            summaries.extend(page["StackResourceSummaries"])
    except (ClientError, BotoCoreError, EndpointConnectionError) as error:
        pytest.fail(f"integration environment not available: {error}")

    if not summaries:
        pytest.fail(f"stack {STACK_NAME} is not deployed")

    return summaries


@pytest.fixture(scope="session")
def deployed_template():
    cloudformation = client("cloudformation")
    response = cloudformation.get_template(StackName=STACK_NAME)
    body = response["TemplateBody"]
    return body if isinstance(body, dict) else json.loads(body)


def find_resource(stack_resources, logical_prefix, resource_type):
    matches = [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type and resource["LogicalResourceId"].startswith(logical_prefix)
    ]
    assert len(matches) == 1, f"expected one {resource_type} for {logical_prefix}, found {len(matches)}"
    return matches[0]


def maybe_find_resource(stack_resources, logical_prefix, resource_type):
    matches = [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type and resource["LogicalResourceId"].startswith(logical_prefix)
    ]
    assert len(matches) <= 1, f"expected at most one {resource_type} for {logical_prefix}, found {len(matches)}"
    return matches[0] if matches else None


def template_resources_by_type(deployed_template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in deployed_template["Resources"].items()
        if resource["Type"] == resource_type
    }


def template_find_logical_id(deployed_template, logical_prefix, resource_type):
    matches = [
        logical_id
        for logical_id, resource in deployed_template["Resources"].items()
        if resource["Type"] == resource_type and logical_id.startswith(logical_prefix)
    ]
    assert len(matches) == 1, f"expected one {resource_type} for {logical_prefix}, found {len(matches)}"
    return matches[0]


def template_statement_actions(statement):
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def template_statement_resources(statement):
    resources = statement["Resource"]
    return resources if isinstance(resources, list) else [resources]


def template_find_statement(statements, required_actions):
    required = set(required_actions if isinstance(required_actions, list) else [required_actions])
    matches = [statement for statement in statements if set(template_statement_actions(statement)) == required]
    assert len(matches) == 1, f"expected one statement for {sorted(required)}, found {len(matches)}"
    return matches[0]


def template_role_policy_statements(deployed_template, role_prefix):
    role_logical_id = template_find_logical_id(deployed_template, role_prefix, "AWS::IAM::Role")
    statements = []
    for policy in template_resources_by_type(deployed_template, "AWS::IAM::Policy").values():
        if {"Ref": role_logical_id} in policy["Properties"].get("Roles", []):
            policy_statements = policy["Properties"]["PolicyDocument"]["Statement"]
            if isinstance(policy_statements, dict):
                statements.append(policy_statements)
            else:
                statements.extend(policy_statements)
    return role_logical_id, statements


def queue_url_from_resource(stack_resources):
    queue_resource = find_resource(stack_resources, "IngestQueue", "AWS::SQS::Queue")
    physical_id = queue_resource["PhysicalResourceId"]
    if physical_id.startswith("http://") or physical_id.startswith("https://"):
        return physical_id
    return client("sqs").get_queue_url(QueueName=physical_id)["QueueUrl"]


def api_url(stack_resources, path):
    api_id = find_resource(stack_resources, "IngestApi", "AWS::ApiGateway::RestApi")["PhysicalResourceId"]
    clean_path = path if path.startswith("/") else f"/{path}"
    override = endpoint_override()
    if override:
        return f"{override.rstrip('/')}/restapis/{api_id}/prod/_user_request_{clean_path}"
    return f"https://{api_id}.execute-api.{REGION}.amazonaws.com/prod{clean_path}"


def http_call(method, url, body=None):
    headers = {"Content-Type": "application/json"}
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def drain_queue(queue_url):
    sqs_client = client("sqs")
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            return
        for message in messages:
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


def wait_for_queue_message(queue_url, predicate, timeout_seconds=60):
    sqs_client = client("sqs")

    def receive_matching_message():
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        for message in response.get("Messages", []):
            if predicate(message):
                return message
        return None

    return wait_until(receive_matching_message, timeout_seconds=timeout_seconds)


def wait_until(predicate, timeout_seconds=60, interval_seconds=2):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval_seconds)
    raise AssertionError("timed out waiting for expected condition")


def log_group_name(stack_resources, logical_prefix):
    return find_resource(stack_resources, logical_prefix, "AWS::Logs::LogGroup")["PhysicalResourceId"]


def assert_https_only_like_rule(rule):
    assert rule["IpRanges"] == [{"CidrIp": "0.0.0.0/0"}]
    assert rule["Ipv6Ranges"] == []
    assert rule["PrefixListIds"] == []
    assert rule["UserIdGroupPairs"] == []
    if rule["IpProtocol"] == "tcp":
        assert rule["FromPort"] == 443
        assert rule["ToPort"] == 443
    else:
        assert rule["IpProtocol"] == "-1"


def wait_for_execution(state_machine_arn, previous_execution_arns, timeout_seconds=90):
    sfn_client = client("stepfunctions")
    execution = wait_until(
        lambda: next(
            (
                item
                for item in sfn_client.list_executions(stateMachineArn=state_machine_arn, maxResults=100)["executions"]
                if item["executionArn"] not in previous_execution_arns
            ),
            None,
        ),
        timeout_seconds=timeout_seconds,
    )
    return wait_until(
        lambda: (
            result
            if (result := sfn_client.describe_execution(executionArn=execution["executionArn"]))["status"]
            in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}
            else None
        ),
        timeout_seconds=timeout_seconds,
    )


def test_deployed_template_preserves_exact_resource_contract(deployed_template):
    expected = {
        "AWS::ApiGateway::Method": 1,
        "AWS::ApiGateway::Resource": 1,
        "AWS::ApiGateway::RestApi": 1,
        "AWS::ApiGateway::Stage": 1,
        "AWS::CloudWatch::Alarm": 2,
        "AWS::EC2::NatGateway": 1,
        "AWS::EC2::SecurityGroup": 2,
        "AWS::EC2::Subnet": 4,
        "AWS::EC2::VPC": 1,
        "AWS::IAM::Role": 4,
        "AWS::Lambda::Function": 2,
        "AWS::Logs::LogGroup": 4,
        "AWS::Logs::MetricFilter": 1,
        "AWS::Pipes::Pipe": 1,
        "AWS::RDS::DBInstance": 1,
        "AWS::RDS::DBSubnetGroup": 1,
        "AWS::SecretsManager::Secret": 1,
        "AWS::SecretsManager::SecretTargetAttachment": 1,
        "AWS::SQS::Queue": 1,
        "AWS::StepFunctions::StateMachine": 1,
    }

    actual = {}
    for resource in deployed_template["Resources"].values():
        actual[resource["Type"]] = actual.get(resource["Type"], 0) + 1

    for resource_type, count in expected.items():
        assert actual.get(resource_type) == count, f"{resource_type} count mismatch"

    for resource in deployed_template["Resources"].values():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}


def test_deployed_template_observability_and_database_contract(deployed_template):
    resources = deployed_template["Resources"]

    db_instance = next(resource for resource in resources.values() if resource["Type"] == "AWS::RDS::DBInstance")
    assert db_instance["Properties"]["Port"] == "5432"
    assert db_instance["Properties"]["BackupRetentionPeriod"] == 1
    assert db_instance["Properties"]["DeletionProtection"] is False
    assert db_instance["Properties"]["PubliclyAccessible"] is False

    log_groups = [
        resource for resource in resources.values()
        if resource["Type"] == "AWS::Logs::LogGroup"
    ]
    assert len(log_groups) == 4
    for log_group in log_groups:
        assert log_group["Properties"]["RetentionInDays"] == 14
        assert "KmsKeyId" not in log_group["Properties"]

    metric_filter = next(resource for resource in resources.values() if resource["Type"] == "AWS::Logs::MetricFilter")
    assert metric_filter["Properties"]["FilterPattern"] == "{ $.status = 5* }"
    assert metric_filter["Properties"]["MetricTransformations"] == [
        {
            "MetricName": "ServerErrors5xx",
            "MetricNamespace": "Custom/ApiGateway",
            "MetricValue": "1",
        }
    ]

    stage = next(resource for resource in resources.values() if resource["Type"] == "AWS::ApiGateway::Stage")
    assert stage["Properties"]["StageName"] == "prod"
    assert stage["Properties"]["MethodSettings"] == [
        {
            "DataTraceEnabled": False,
            "HttpMethod": "*",
            "LoggingLevel": "INFO",
            "ResourcePath": "/*",
        }
    ]
    assert '"status":"$context.status"' in stage["Properties"]["AccessLogSetting"]["Format"]

    state_machine = next(resource for resource in resources.values() if resource["Type"] == "AWS::StepFunctions::StateMachine")
    assert state_machine["Properties"]["LoggingConfiguration"]["Level"] == "ALL"
    assert state_machine["Properties"]["LoggingConfiguration"]["IncludeExecutionData"] is True

    functions = [
        resource for resource in resources.values()
        if resource["Type"] == "AWS::Lambda::Function"
    ]
    assert len(functions) == 2
    for function in functions:
        assert function["Properties"]["ReservedConcurrentExecutions"] == 2


def test_deployed_runtime_network_and_data_boundaries(stack_resources):
    lambda_client = client("lambda")
    ec2_client = client("ec2")
    rds_client = client("rds")
    logs_client = client("logs")
    sqs_client = client("sqs")

    ingest_name = find_resource(stack_resources, "IngestWorker", "AWS::Lambda::Function")["PhysicalResourceId"]
    enrich_name = find_resource(stack_resources, "EnrichWorker", "AWS::Lambda::Function")["PhysicalResourceId"]
    compute_sg_id = find_resource(stack_resources, "SGCompute", "AWS::EC2::SecurityGroup")["PhysicalResourceId"]
    database_sg_id = find_resource(stack_resources, "SGDatabase", "AWS::EC2::SecurityGroup")["PhysicalResourceId"]
    queue_url = queue_url_from_resource(stack_resources)
    database_resource = maybe_find_resource(stack_resources, "Database", "AWS::RDS::DBInstance")

    for function_name in (ingest_name, enrich_name):
        config = lambda_client.get_function_configuration(FunctionName=function_name)
        assert config["Runtime"] == "nodejs20.x"
        assert config["MemorySize"] == 256
        assert config["Timeout"] == 10
        assert config["VpcConfig"]["SecurityGroupIds"] == [compute_sg_id]
        assert len(config["VpcConfig"]["SubnetIds"]) == 2

    security_groups = ec2_client.describe_security_groups(GroupIds=[compute_sg_id, database_sg_id])["SecurityGroups"]
    compute_group = next(group for group in security_groups if group["GroupId"] == compute_sg_id)
    database_group = next(group for group in security_groups if group["GroupId"] == database_sg_id)

    assert compute_group["IpPermissions"] == []
    assert len(compute_group["IpPermissionsEgress"]) == 1
    assert_https_only_like_rule(compute_group["IpPermissionsEgress"][0])
    assert len(database_group["IpPermissionsEgress"]) == 1
    assert_https_only_like_rule(database_group["IpPermissionsEgress"][0])
    assert database_group["IpPermissions"][0]["FromPort"] == 5432
    assert database_group["IpPermissions"][0]["ToPort"] == 5432
    assert database_group["IpPermissions"][0]["UserIdGroupPairs"][0]["GroupId"] == compute_sg_id

    if database_resource is not None:
        database = rds_client.describe_db_instances(
            DBInstanceIdentifier=database_resource["PhysicalResourceId"]
        )["DBInstances"][0]
        assert database["Engine"] == "postgres"
        assert database["EngineVersion"] == "15.5"
        assert database["DBInstanceClass"] == "db.t3.micro"
        assert database["AllocatedStorage"] == 20
        assert database["StorageType"] == "gp2"
        assert database["PubliclyAccessible"] is False
        assert database["BackupRetentionPeriod"] == 1
        assert database["DeletionProtection"] is False
        assert database["VpcSecurityGroups"][0]["VpcSecurityGroupId"] == database_sg_id

    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "MessageRetentionPeriod"],
    )["Attributes"]
    assert queue_attributes["VisibilityTimeout"] == "30"
    assert queue_attributes["MessageRetentionPeriod"] == "345600"

    ingest_log_group = logs_client.describe_log_groups(
        logGroupNamePrefix=log_group_name(stack_resources, "IngestWorkerLogGroup"),
    )["logGroups"]
    enrich_log_group = logs_client.describe_log_groups(
        logGroupNamePrefix=log_group_name(stack_resources, "EnrichWorkerLogGroup"),
    )["logGroups"]
    assert len(ingest_log_group) == 1
    assert len(enrich_log_group) == 1


def test_api_gateway_deployed_route_targets_lambda_proxy(stack_resources):
    apigateway_client = client("apigateway")
    api_id = find_resource(stack_resources, "IngestApi", "AWS::ApiGateway::RestApi")["PhysicalResourceId"]
    resources = apigateway_client.get_resources(restApiId=api_id, embed=["methods"])["items"]
    ingest_resource = next(item for item in resources if item.get("path") == "/ingest")
    method = apigateway_client.get_method(
        restApiId=api_id,
        resourceId=ingest_resource["id"],
        httpMethod="POST",
    )

    assert method["httpMethod"] == "POST"
    assert method["methodIntegration"]["type"] == "AWS_PROXY"
    assert method["methodIntegration"]["httpMethod"] == "POST"


def test_direct_queue_message_is_accepted_by_deployed_queue(stack_resources):
    sqs_client = client("sqs")

    queue_url = queue_url_from_resource(stack_resources)
    drain_queue(queue_url)
    expected_body = json.dumps({"source": "direct-sqs", "recordId": "queue-path"})

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=expected_body,
    )

    message = wait_for_queue_message(
        queue_url,
        lambda item: item["Body"] == expected_body,
    )
    assert message["Body"] == expected_body
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


def test_api_ingest_happy_path_enqueues_message(stack_resources):
    sqs_client = client("sqs")
    queue_url = queue_url_from_resource(stack_resources)
    drain_queue(queue_url)
    payload = {"recordId": "abc-123", "value": "payload"}

    status_code, response_body = http_call(
        "POST",
        api_url(stack_resources, "/ingest"),
        payload,
    )
    assert status_code == 202, response_body

    message = wait_for_queue_message(
        queue_url,
        lambda item: json.loads(item["Body"]) == payload,
    )
    assert json.loads(message["Body"]) == payload
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


def test_api_rejects_wrong_method_and_unknown_route(stack_resources):
    get_status, get_body = http_call("GET", api_url(stack_resources, "/ingest"))
    assert get_status in {403, 404, 405}, get_body

    post_status, post_body = http_call("POST", api_url(stack_resources, "/not-found"), {"unexpected": True})
    assert post_status in {403, 404, 405}, post_body


def test_observability_and_runtime_resources_exist_deployed(stack_resources):
    cloudwatch_client = client("cloudwatch")
    logs_client = client("logs")
    apigateway_client = client("apigateway")

    api_id = find_resource(stack_resources, "IngestApi", "AWS::ApiGateway::RestApi")["PhysicalResourceId"]
    metric_filter_resource = find_resource(
        stack_resources,
        "ApiGateway5xxMetricFilter",
        "AWS::Logs::MetricFilter",
    )
    ingest_alarm_name = find_resource(
        stack_resources,
        "IngestWorkerErrorsAlarm",
        "AWS::CloudWatch::Alarm",
    )["PhysicalResourceId"]
    enrich_alarm_name = find_resource(
        stack_resources,
        "EnrichWorkerErrorsAlarm",
        "AWS::CloudWatch::Alarm",
    )["PhysicalResourceId"]
    api_log_group_name = log_group_name(stack_resources, "ApiStageLogGroup")
    api_log_group = logs_client.describe_log_groups(logGroupNamePrefix=api_log_group_name)["logGroups"]
    sfn_log_group = logs_client.describe_log_groups(
        logGroupNamePrefix=log_group_name(stack_resources, "StepFunctionsLogGroup"),
    )["logGroups"]

    stage = apigateway_client.get_stage(restApiId=api_id, stageName="prod")
    method_settings = stage.get("methodSettings", {})
    if "*/*" in method_settings:
        assert method_settings["*/*"]["loggingLevel"] == "INFO"
    else:
        assert "accessLogSettings" in stage, stage
        assert '"status":"$context.status"' in stage["accessLogSettings"]["format"]
    assert len(api_log_group) == 1
    assert len(sfn_log_group) == 1
    assert "kmsKeyId" not in api_log_group[0]
    assert "kmsKeyId" not in sfn_log_group[0]

    alarms = cloudwatch_client.describe_alarms(AlarmNames=[ingest_alarm_name, enrich_alarm_name])["MetricAlarms"]
    assert len(alarms) == 2
    for alarm in alarms:
        assert alarm["Namespace"] == "AWS/Lambda"
        assert alarm["MetricName"] == "Errors"
        assert alarm["Period"] == 60
        assert alarm["EvaluationPeriods"] == 1
        assert alarm["Threshold"] == 1.0
        assert alarm["TreatMissingData"] == "notBreaching"

    metric_filters = logs_client.describe_metric_filters(logGroupName=api_log_group_name)["metricFilters"]
    matching_filter = next(
        (
            filter_item
            for filter_item in metric_filters
            if filter_item["metricTransformations"][0]["metricName"] == "ServerErrors5xx"
        ),
        None,
    )
    assert metric_filter_resource["PhysicalResourceId"]
    if matching_filter is not None:
        assert matching_filter["filterPattern"] == "{ $.status = 5* }"


def test_generated_secret_contains_username_and_password(stack_resources):
    secrets_client = client("secretsmanager")
    secret_id = find_resource(
        stack_resources,
        "DatabaseCredentialsSecret",
        "AWS::SecretsManager::Secret",
    )["PhysicalResourceId"]

    secret_value = secrets_client.get_secret_value(SecretId=secret_id)
    secret_payload = json.loads(secret_value["SecretString"])
    assert isinstance(secret_payload["username"], str)
    assert secret_payload["username"]
    assert isinstance(secret_payload["password"], str)
    assert secret_payload["password"]


def test_deployed_template_iam_scoping_matches_prompt(deployed_template):
    queue_id = template_find_logical_id(deployed_template, "IngestQueue", "AWS::SQS::Queue")
    secret_id = template_find_logical_id(
        deployed_template,
        "DatabaseCredentialsSecret",
        "AWS::SecretsManager::Secret",
    )

    ingest_role = next(
        resource
        for logical_id, resource in deployed_template["Resources"].items()
        if logical_id.startswith("IngestWorkerRole") and resource["Type"] == "AWS::IAM::Role"
    )
    enrich_role = next(
        resource
        for logical_id, resource in deployed_template["Resources"].items()
        if logical_id.startswith("EnrichWorkerRole") and resource["Type"] == "AWS::IAM::Role"
    )
    assert ingest_role["Properties"]["ManagedPolicyArns"] == enrich_role["Properties"]["ManagedPolicyArns"]

    _, ingest_statements = template_role_policy_statements(deployed_template, "IngestWorkerRole")
    _, enrich_statements = template_role_policy_statements(deployed_template, "EnrichWorkerRole")

    ingest_send = template_find_statement(ingest_statements, "sqs:SendMessage")
    assert ingest_send["Resource"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    ingest_secret = template_find_statement(ingest_statements, "secretsmanager:GetSecretValue")
    assert ingest_secret["Resource"] == {"Ref": secret_id}

    enrich_secret = template_find_statement(enrich_statements, "secretsmanager:GetSecretValue")
    assert enrich_secret["Resource"] == {"Ref": secret_id}
    enrich_metric = template_find_statement(enrich_statements, "cloudwatch:PutMetricData")
    assert enrich_metric["Resource"] == "*"
    assert enrich_metric["Condition"] == {
        "StringEquals": {"cloudwatch:namespace": "Custom/EnrichWorker"}
    }

    assert all("cloudwatch:PutMetricData" not in template_statement_actions(statement) for statement in ingest_statements)
    assert all("sqs:SendMessage" not in template_statement_actions(statement) for statement in enrich_statements)

    wildcard_actions = []
    wildcard_resources = []
    for policy in template_resources_by_type(deployed_template, "AWS::IAM::Policy").values():
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = template_statement_actions(statement)
            resources = template_statement_resources(statement)
            if any("*" in action for action in actions):
                wildcard_actions.append(statement)
            if "*" in resources:
                wildcard_resources.append(statement)

    assert wildcard_actions == []
    assert len(wildcard_resources) == 2
    assert any(
        template_statement_actions(statement) == ["cloudwatch:PutMetricData"]
        and statement.get("Condition") == {"StringEquals": {"cloudwatch:namespace": "Custom/EnrichWorker"}}
        for statement in wildcard_resources
    )
    assert any(
        set(template_statement_actions(statement))
        == {
            "logs:CreateLogDelivery",
            "logs:DeleteLogDelivery",
            "logs:DescribeLogGroups",
            "logs:DescribeResourcePolicies",
            "logs:GetLogDelivery",
            "logs:ListLogDeliveries",
            "logs:PutResourcePolicy",
            "logs:UpdateLogDelivery",
        }
        for statement in wildcard_resources
    )
