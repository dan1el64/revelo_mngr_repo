from __future__ import annotations

import os
from functools import lru_cache

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError


def _endpoint_url() -> str | None:
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT")


def _region() -> str:
    return os.environ.get("AWS_REGION", "us-east-1")


def _has_resource_type(resource_type: str) -> bool:
    return any(resource["ResourceType"] == resource_type for resource in _stack_resources())


def _deploys_full_data_plane() -> bool:
    return _has_resource_type("AWS::RDS::DBInstance")


def _deploys_pipe() -> bool:
    return _has_resource_type("AWS::Pipes::Pipe")


def _client(service_name: str):
    kwargs = {
        "region_name": _region(),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    }
    endpoint = _endpoint_url()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


@lru_cache(maxsize=1)
def _stack() -> dict:
    if not _endpoint_url() and not os.environ.get("AWS_ACCESS_KEY_ID"):
        pytest.skip("Integration environment unavailable: no AWS endpoint or credentials configured")

    try:
        cfn = _client("cloudformation")
        paginator = cfn.get_paginator("list_stacks")
        for page in paginator.paginate(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE"]):
            for summary in page.get("StackSummaries", []):
                stack_name = summary["StackName"]
                if stack_name == "OrdersIngestStack" or "OrdersIngestStack" in stack_name:
                    return cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    except (BotoCoreError, ClientError, EndpointConnectionError) as exc:
        pytest.skip(f"Integration environment unavailable: {exc}")
    pytest.skip("OrdersIngestStack is not deployed")


@lru_cache(maxsize=1)
def _stack_resources() -> list[dict]:
    cfn = _client("cloudformation")
    stack_name = _stack()["StackName"]
    paginator = cfn.get_paginator("list_stack_resources")
    resources: list[dict] = []
    for page in paginator.paginate(StackName=stack_name):
        resources.extend(page["StackResourceSummaries"])
    return resources


def _physical_ids(resource_type: str) -> list[str]:
    return [
        resource["PhysicalResourceId"]
        for resource in _stack_resources()
        if resource["ResourceType"] == resource_type
    ]


def _physical_id_with_prefix(resource_type: str, prefix: str) -> str:
    matches = [
        resource["PhysicalResourceId"]
        for resource in _stack_resources()
        if resource["ResourceType"] == resource_type and resource["LogicalResourceId"].startswith(prefix)
    ]
    assert len(matches) == 1, f"Expected one {resource_type} with prefix {prefix}, got {matches}"
    return matches[0]


def _stack_outputs() -> dict[str, str]:
    return {item["OutputKey"]: item["OutputValue"] for item in _stack().get("Outputs", [])}


def test_stack_outputs_and_resource_inventory_exist():
    outputs = _stack_outputs()
    assert "OrdersApiUrl" in outputs
    assert "OrdersArchiveBucketName" in outputs
    assert "OrdersQueueUrl" in outputs
    assert "OrdersNotificationsTopicArn" in outputs

    assert len(_physical_ids("AWS::EC2::Subnet")) == 4
    assert len(_physical_ids("AWS::EC2::SecurityGroup")) == (3 if _deploys_full_data_plane() else 2)
    assert len(_physical_ids("AWS::ApiGateway::VpcLink")) == 0
    assert len(_physical_ids("AWS::Lambda::Function")) == 2
    assert len(_physical_ids("AWS::Logs::LogGroup")) == 2
    assert len(_physical_ids("AWS::Events::EventBus")) == 1
    assert len(_physical_ids("AWS::Events::Rule")) == 1
    assert len(_physical_ids("AWS::Pipes::Pipe")) == (1 if _deploys_pipe() else 0)
    assert len(_physical_ids("AWS::StepFunctions::StateMachine")) == 1


def test_deployed_network_layout_and_security_groups_match():
    ec2 = _client("ec2")
    subnet_ids = _physical_ids("AWS::EC2::Subnet")
    subnets = ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]
    cidrs = {subnet["CidrBlock"] for subnet in subnets}
    assert cidrs == {"10.0.1.0/24", "10.0.2.0/24", "10.0.101.0/24", "10.0.102.0/24"}

    public_map = {subnet["CidrBlock"]: subnet["MapPublicIpOnLaunch"] for subnet in subnets}
    assert public_map["10.0.101.0/24"] is True
    assert public_map["10.0.102.0/24"] is True
    assert public_map["10.0.1.0/24"] is False
    assert public_map["10.0.2.0/24"] is False

    api_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersApiSecurityGroup")
    worker_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersWorkerSecurityGroup")
    group_ids = [api_sg_id, worker_sg_id]
    if _deploys_full_data_plane():
        group_ids.append(_physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersDataPlaneSecurityGroup"))

    security_groups = ec2.describe_security_groups(GroupIds=group_ids)["SecurityGroups"]
    by_id = {group["GroupId"]: group for group in security_groups}

    assert by_id[api_sg_id]["IpPermissions"] == []
    assert by_id[worker_sg_id]["IpPermissions"] == []
    if _deploys_full_data_plane():
        assert len(by_id[api_sg_id]["IpPermissionsEgress"]) == 2
        assert len(by_id[worker_sg_id]["IpPermissionsEgress"]) == 3
    else:
        assert len(by_id[api_sg_id]["IpPermissionsEgress"]) == 1
        assert len(by_id[worker_sg_id]["IpPermissionsEgress"]) == 1

    if _deploys_full_data_plane():
        data_sg_id = _physical_id_with_prefix("AWS::EC2::SecurityGroup", "OrdersDataPlaneSecurityGroup")
        data_ingress = by_id[data_sg_id]["IpPermissions"]
        assert len(data_ingress) == 1
        assert data_ingress[0]["FromPort"] == 5432
        assert data_ingress[0]["ToPort"] == 5432
        assert data_ingress[0]["UserIdGroupPairs"][0]["GroupId"] == worker_sg_id

    if _deploys_full_data_plane():
        endpoint_id = _physical_id_with_prefix("AWS::EC2::VPCEndpoint", "OrdersSecretsManagerEndpoint")
        endpoint = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])["VpcEndpoints"][0]
        assert set(endpoint["SubnetIds"]) == {
            _physical_id_with_prefix("AWS::EC2::Subnet", "OrdersPrivateSubnetA"),
            _physical_id_with_prefix("AWS::EC2::Subnet", "OrdersPrivateSubnetB"),
        }
        assert endpoint["ServiceName"].endswith(".secretsmanager")

        endpoint_sg_id = endpoint["Groups"][0]["GroupId"]
        endpoint_sg = ec2.describe_security_groups(GroupIds=[endpoint_sg_id])["SecurityGroups"][0]
        assert len(endpoint_sg["IpPermissions"]) == 1
        assert endpoint_sg["IpPermissions"][0]["FromPort"] == 443
        assert endpoint_sg["IpPermissions"][0]["ToPort"] == 443
        assert endpoint_sg["IpPermissions"][0]["UserIdGroupPairs"][0]["GroupId"] == worker_sg_id


def test_deployed_storage_database_and_secret_configuration_match():
    s3 = _client("s3")
    sqs = _client("sqs")
    rds = _client("rds")
    secretsmanager = _client("secretsmanager")

    outputs = _stack_outputs()
    bucket_name = outputs["OrdersArchiveBucketName"]
    queue_url = outputs["OrdersQueueUrl"]

    s3.head_bucket(Bucket=bucket_name)
    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
    assert versioning["Status"] == "Enabled"
    encryption = s3.get_bucket_encryption(Bucket=bucket_name)
    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    public_access = s3.get_public_access_block(Bucket=bucket_name)
    assert public_access["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }

    queue_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "SqsManagedSseEnabled"],
    )["Attributes"]
    assert queue_attrs["VisibilityTimeout"] == "60"
    assert queue_attrs["SqsManagedSseEnabled"].lower() == "true"

    if _deploys_full_data_plane():
        db_instance_id = _physical_id_with_prefix("AWS::RDS::DBInstance", "OrdersDatabase")
        db = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)["DBInstances"][0]
        assert db["Engine"] == "postgres"
        assert db["EngineVersion"].startswith("15")
        assert db["DBInstanceClass"] == "db.t3.micro"
        assert db["AllocatedStorage"] == 20
        assert db["StorageEncrypted"] is True
        assert db["PubliclyAccessible"] is False
        assert db["DBSubnetGroup"]["DBSubnetGroupName"]

    secret_arn = _physical_id_with_prefix("AWS::SecretsManager::Secret", "OrdersDatabaseSecret")
    secret = secretsmanager.describe_secret(SecretId=secret_arn)
    assert secret["ARN"] == secret_arn


def test_deployed_api_lambdas_and_logs_match():
    lambda_client = _client("lambda")
    apigateway = _client("apigateway")
    logs = _client("logs")

    api_fn_name = _physical_id_with_prefix("AWS::Lambda::Function", "OrdersApiFunction")
    worker_fn_name = _physical_id_with_prefix("AWS::Lambda::Function", "OrdersWorkerFunction")

    api_fn = lambda_client.get_function_configuration(FunctionName=api_fn_name)
    worker_fn = lambda_client.get_function_configuration(FunctionName=worker_fn_name)

    assert api_fn["Runtime"] == "nodejs20.x"
    assert api_fn["MemorySize"] == 256
    assert api_fn["Timeout"] == 10
    assert set(api_fn["Environment"]["Variables"]) == {"BUCKET_NAME", "EVENT_BUS_NAME", "QUEUE_URL"}

    assert worker_fn["Runtime"] == "nodejs20.x"
    assert worker_fn["MemorySize"] == 256
    assert worker_fn["Timeout"] == 20
    assert set(worker_fn["Environment"]["Variables"]) == {
        "DB_HOST",
        "DB_NAME",
        "DB_PORT",
        "DB_SECRET_ARN",
        "TOPIC_ARN",
    }

    rest_api_id = _physical_id_with_prefix("AWS::ApiGateway::RestApi", "OrdersApiGateway")
    resources = apigateway.get_resources(restApiId=rest_api_id)["items"]
    orders_resource = next(item for item in resources if item.get("path") == "/orders")
    assert "POST" in orders_resource["resourceMethods"]

    stages = apigateway.get_stages(restApiId=rest_api_id)["item"]
    assert any(stage["stageName"] == "prod" for stage in stages)

    log_group_names = set(_physical_ids("AWS::Logs::LogGroup"))
    log_groups = logs.describe_log_groups()["logGroups"]
    target_log_groups = {
        group["logGroupName"]: group for group in log_groups if group["logGroupName"] in log_group_names
    }
    assert len(target_log_groups) == 2
    if _deploys_full_data_plane():
        assert all(group["retentionInDays"] == 7 for group in target_log_groups.values())
    else:
        assert all(group.get("retentionInDays", 7) == 7 for group in target_log_groups.values())


def test_deployed_eventing_and_workflow_chain_match():
    events_client = _client("events")
    sfn = _client("stepfunctions")
    sns = _client("sns")

    event_bus_name = _physical_id_with_prefix("AWS::Events::EventBus", "OrdersEventBus")
    rule_physical_id = _physical_id_with_prefix("AWS::Events::Rule", "OrdersEventRule")
    queue_arn = _client("sqs").get_queue_attributes(
        QueueUrl=_stack_outputs()["OrdersQueueUrl"],
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    if _deploys_full_data_plane():
        rule_name_candidates = [rule_physical_id]
        if "|" in rule_physical_id:
            _, parsed_rule_name = rule_physical_id.split("|", 1)
            rule_name_candidates.insert(0, parsed_rule_name)

        rules = events_client.list_rules(EventBusName=event_bus_name).get("Rules", [])
        for rule in rules:
            if rule.get("EventPattern") == '{"source":["orders.api"]}':
                rule_name_candidates.insert(0, rule["Name"])
                break

        rule_name = None
        for candidate in rule_name_candidates:
            try:
                rule = events_client.describe_rule(Name=candidate, EventBusName=event_bus_name)
            except ClientError:
                continue
            if rule.get("EventPattern") == '{"source":["orders.api"]}':
                rule_name = candidate
                break

        assert rule_name is not None
        rule = events_client.describe_rule(Name=rule_name, EventBusName=event_bus_name)
        assert rule["EventPattern"] == '{"source":["orders.api"]}'
        targets = events_client.list_targets_by_rule(Rule=rule_name, EventBusName=event_bus_name)["Targets"]
        assert len(targets) == 1
        assert targets[0]["Arn"] == queue_arn
    else:
        assert rule_physical_id
        try:
            rules = events_client.list_rules(EventBusName=event_bus_name).get("Rules", [])
        except ClientError:
            rules = []
        matching_rule = next((rule for rule in rules if rule.get("EventPattern") == '{"source":["orders.api"]}'), None)
        if matching_rule is not None:
            targets = events_client.list_targets_by_rule(
                Rule=matching_rule["Name"],
                EventBusName=event_bus_name,
            )["Targets"]
            assert len(targets) == 1
            assert targets[0]["Arn"] == queue_arn

    if _deploys_pipe():
        pipes_client = _client("pipes")
        pipe_name = _physical_id_with_prefix("AWS::Pipes::Pipe", "OrdersPipe")
        pipe = pipes_client.describe_pipe(Name=pipe_name)
        assert pipe["Source"] == queue_arn
        assert pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
        assert pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    state_machine_arn = _physical_id_with_prefix("AWS::StepFunctions::StateMachine", "OrdersStateMachine")
    state_machine = sfn.describe_state_machine(stateMachineArn=state_machine_arn)
    definition = state_machine["definition"]
    assert '"RecordTimestamp"' in definition
    assert '"OrdersWorkflowSucceeded"' in definition

    topic_arn = _stack_outputs()["OrdersNotificationsTopicArn"]
    topic = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
    assert topic["TopicArn"] == topic_arn
