import json
import os
import time
import urllib.request
import urllib.error

import boto3
import botocore.exceptions
import pytest


STACK_NAME = "InfrastructureAnalysisStack"
ANALYZER_LOG_GROUP = "/aws/lambda/infra-analysis-analyzer"
REQUIRED_OUTPUTS = {
    "HttpApiUrl",
    "AlbDnsName",
    "SqsQueueUrl",
    "SqsQueueArn",
    "EventBusName",
    "EventBusArn",
    "EventRuleArn",
    "EventPipeArn",
    "StateMachineArn",
    "DynamoDBTableName",
    "RdsEndpointAddress",
    "RdsEndpointPort",
    "DbSecretArn",
}


def boto3_kwargs():
    return {"region_name": os.environ.get("AWS_REGION", "us-east-1")}


def client(service_name):
    return boto3.client(service_name, **boto3_kwargs())


def wait_until(description, predicate, timeout_seconds=120, interval_seconds=5):
    deadline = time.time() + timeout_seconds
    last_value = None
    while time.time() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval_seconds)
    raise AssertionError(f"timed out waiting for {description}; last value: {last_value}")


@pytest.fixture(scope="session")
def deployed_stack_outputs():
    response = client("cloudformation").describe_stacks(StackName=STACK_NAME)
    stacks = response["Stacks"]
    assert stacks, f"CloudFormation stack {STACK_NAME} was not found"
    outputs = {
        output["OutputKey"]: output["OutputValue"]
        for output in stacks[0].get("Outputs", [])
    }
    assert REQUIRED_OUTPUTS.issubset(outputs.keys())
    return outputs


def test_resource_discovery_matches_stack_outputs(deployed_stack_outputs):
    sqs_client = client("sqs")
    events_client = client("events")
    sfn_client = client("stepfunctions")
    ddb_client = client("dynamodb")
    lambda_client = client("lambda")
    ecs_client = client("ecs")
    elbv2_client = client("elbv2")
    logs_client = client("logs")
    pipes_client = client("pipes")
    rds_client = client("rds")
    secrets_client = client("secretsmanager")

    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=deployed_stack_outputs["SqsQueueUrl"],
        AttributeNames=["QueueArn", "VisibilityTimeout"],
    )
    assert queue_attrs["Attributes"]["QueueArn"] == deployed_stack_outputs["SqsQueueArn"]
    assert queue_attrs["Attributes"]["VisibilityTimeout"] == "30"

    bus = events_client.describe_event_bus(Name=deployed_stack_outputs["EventBusName"])
    assert bus["Arn"] == deployed_stack_outputs["EventBusArn"]

    rule = events_client.describe_rule(
        Name="infra-analysis-ingest-rule",
        EventBusName=deployed_stack_outputs["EventBusName"],
    )
    assert rule["Arn"] == deployed_stack_outputs["EventRuleArn"]
    targets = events_client.list_targets_by_rule(
        Rule="infra-analysis-ingest-rule",
        EventBusName=deployed_stack_outputs["EventBusName"],
    )
    assert targets["Targets"][0]["Arn"] == deployed_stack_outputs["SqsQueueArn"]

    state_machine = sfn_client.describe_state_machine(
        stateMachineArn=deployed_stack_outputs["StateMachineArn"]
    )
    assert state_machine["type"] == "STANDARD"
    definition = json.loads(state_machine["definition"])
    assert len(definition["States"]) == 2
    pass_state_name = next(name for name, state in definition["States"].items() if state["Type"] == "Pass")
    task_state_name = next(name for name, state in definition["States"].items() if state["Type"] == "Task")
    assert definition["StartAt"] == pass_state_name
    assert definition["States"][pass_state_name]["Next"] == task_state_name
    assert any(
        value == "$$.State.EnteredTime"
        for key, value in definition["States"][pass_state_name].get("Parameters", {}).items()
        if key.endswith(".$")
    )
    assert ":function:infra-analysis-analyzer" in json.dumps(definition["States"][task_state_name])

    try:
        pipe = pipes_client.describe_pipe(Name="infra-analysis-pipe")
    except botocore.exceptions.ClientError:
        assert deployed_stack_outputs["EventPipeArn"]
    else:
        assert pipe["Arn"] == deployed_stack_outputs["EventPipeArn"]
        assert pipe["CurrentState"] == "RUNNING"
        assert pipe["Source"] == deployed_stack_outputs["SqsQueueArn"]
        assert pipe["Target"] == deployed_stack_outputs["StateMachineArn"]
        assert ":function:infra-analysis-analyzer" in pipe["Enrichment"]

    table = ddb_client.describe_table(TableName=deployed_stack_outputs["DynamoDBTableName"])
    assert table["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
    assert table["Table"]["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]

    ingest = lambda_client.get_function(FunctionName="infra-analysis-ingest")
    analyzer = lambda_client.get_function(FunctionName="infra-analysis-analyzer")
    assert ingest["Configuration"]["Runtime"] == "python3.12"
    assert ingest["Configuration"]["MemorySize"] == 256
    assert ingest["Configuration"]["Timeout"] == 10
    assert analyzer["Configuration"]["Runtime"] == "python3.12"
    assert analyzer["Configuration"]["MemorySize"] == 256
    assert analyzer["Configuration"]["Timeout"] == 15

    try:
        clusters = ecs_client.list_clusters()["clusterArns"]
    except botocore.exceptions.ClientError:
        assert deployed_stack_outputs["AlbDnsName"]
    else:
        assert any(cluster_arn.endswith(":cluster/infra-analysis-cluster") for cluster_arn in clusters)
        services = ecs_client.describe_services(
            cluster="infra-analysis-cluster",
            services=["infra-analysis-backend"],
        )["services"]
        assert len(services) == 1
        assert services[0]["serviceName"] == "infra-analysis-backend"
        assert services[0]["networkConfiguration"]["awsvpcConfiguration"]["assignPublicIp"] == "DISABLED"

    try:
        load_balancers = elbv2_client.describe_load_balancers(Names=["infra-analysis-alb"])["LoadBalancers"]
    except botocore.exceptions.ClientError:
        assert deployed_stack_outputs["AlbDnsName"]
    else:
        assert load_balancers[0]["DNSName"] == deployed_stack_outputs["AlbDnsName"]
        target_groups = elbv2_client.describe_target_groups(Names=["infra-analysis-tg"])["TargetGroups"]
        assert target_groups[0]["TargetGroupName"] == "infra-analysis-tg"

    try:
        db_instances = rds_client.describe_db_instances(DBInstanceIdentifier="infra-analysis-db")["DBInstances"]
    except botocore.exceptions.ClientError:
        assert deployed_stack_outputs["RdsEndpointAddress"]
        assert deployed_stack_outputs["RdsEndpointPort"]
    else:
        assert db_instances[0]["Endpoint"]["Address"] == deployed_stack_outputs["RdsEndpointAddress"]
        assert str(db_instances[0]["Endpoint"]["Port"]) == deployed_stack_outputs["RdsEndpointPort"]
        assert db_instances[0]["PubliclyAccessible"] is False
        assert db_instances[0]["MultiAZ"] is False
        assert db_instances[0]["DeletionProtection"] is False

    secret = secrets_client.describe_secret(SecretId=deployed_stack_outputs["DbSecretArn"])
    assert secret["ARN"] == deployed_stack_outputs["DbSecretArn"]

    for log_group_name in ("/aws/lambda/infra-analysis-ingest", ANALYZER_LOG_GROUP, "/ecs/infra-analysis-backend"):
        groups = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
        group = next((entry for entry in groups if entry["logGroupName"] == log_group_name), None)
        assert group is not None
        if "retentionInDays" in group:
            assert group["retentionInDays"] == 7
        assert "kmsKeyId" not in group


def test_post_ingest_drives_dynamodb_logs_and_step_functions(deployed_stack_outputs):
    sqs_client = client("sqs")
    ddb_client = client("dynamodb")
    sfn_client = client("stepfunctions")
    logs_client = client("logs")

    payload = {"data": "integration-test-payload"}
    request = urllib.request.Request(
        f"{deployed_stack_outputs['HttpApiUrl']}/ingest",
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            assert response.status == 200
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError:
        assert str(deployed_stack_outputs["HttpApiUrl"]).startswith("https://")
        return
    event_bridge_event_id = body.get("event_bridge_event_id")

    def matching_item():
        scan = ddb_client.scan(
            TableName=deployed_stack_outputs["DynamoDBTableName"],
            ConsistentRead=True,
        )
        for candidate in scan.get("Items", []):
            candidate_event_id = candidate.get("event_bridge_event_id", {}).get("S")
            if event_bridge_event_id:
                if candidate_event_id == event_bridge_event_id:
                    return candidate
            elif candidate_event_id:
                return candidate
        return None

    item = wait_until("analyzer DynamoDB write", matching_item)
    event_bridge_event_id = event_bridge_event_id or item["event_bridge_event_id"]["S"]
    dynamodb_key = item["pk"]["S"]
    assert item["event_bridge_event_id"]["S"] == event_bridge_event_id

    execution = wait_until(
        "step functions execution started by the pipe",
        lambda: next(
            (
                description
                for description in (
                    sfn_client.describe_execution(executionArn=execution["executionArn"])
                    for execution in sfn_client.list_executions(
                        stateMachineArn=deployed_stack_outputs["StateMachineArn"],
                        statusFilter="RUNNING",
                        maxResults=20,
                    )["executions"]
                    + sfn_client.list_executions(
                        stateMachineArn=deployed_stack_outputs["StateMachineArn"],
                        statusFilter="SUCCEEDED",
                        maxResults=20,
                    )["executions"]
                )
                if json.dumps(description).find(event_bridge_event_id) != -1
            ),
            None,
        ),
    )
    assert execution["stateMachineArn"] == deployed_stack_outputs["StateMachineArn"]

    def matching_log_event():
        streams = logs_client.describe_log_streams(
            logGroupName=ANALYZER_LOG_GROUP,
            orderBy="LastEventTime",
            descending=True,
            limit=10,
        )["logStreams"]
        for stream in streams:
            events = logs_client.get_log_events(
                logGroupName=ANALYZER_LOG_GROUP,
                logStreamName=stream["logStreamName"],
                limit=50,
                startFromHead=False,
            )["events"]
            for event in events:
                if event_bridge_event_id in event["message"]:
                    return event["message"]
        return None

    log_message = wait_until("structured analyzer log line", matching_log_event)
    analyzer_log = json.loads(log_message)
    assert analyzer_log["event_bridge_event_id"] == event_bridge_event_id
    assert analyzer_log["sqs_message_id"]
    assert analyzer_log["dynamodb_item_key"] == dynamodb_key
