import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def template():
    candidates = [ROOT / "template.json"]
    candidates.extend(sorted((ROOT / "cdk.out").glob("*.template.json")))

    for candidate in candidates:
        if candidate.exists():
            with candidate.open(encoding="utf-8") as file:
                return json.load(file)

    pytest.fail("No synthesized template found. Run `npx cdk synth` before unit tests.")


@pytest.fixture(scope="session")
def resources(template):
    return template["Resources"]


def by_type(resources, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in resources.items()
        if resource["Type"] == resource_type
    }


def props(resource):
    return resource.get("Properties", {})


def cdk_path(resource):
    return resource.get("Metadata", {}).get("aws:cdk:path", "")


def as_text(value):
    return json.dumps(value, sort_keys=True)


def one(resources, resource_type, predicate=lambda _logical_id, _resource: True):
    matches = [
        (logical_id, resource)
        for logical_id, resource in by_type(resources, resource_type).items()
        if predicate(logical_id, resource)
    ]
    assert len(matches) == 1, f"Expected one {resource_type}, found {len(matches)}"
    return matches[0]


def one_by_path(resources, resource_type, path_fragment):
    return one(
        resources,
        resource_type,
        lambda _logical_id, resource: path_fragment in cdk_path(resource),
    )


def statements(policy_resource):
    policy_document = props(policy_resource)["PolicyDocument"]
    statement = policy_document["Statement"]
    return statement if isinstance(statement, list) else [statement]


def action_set(statement):
    actions = statement["Action"]
    return set(actions if isinstance(actions, list) else [actions])


def all_policy_statements(resources):
    for logical_id, policy in by_type(resources, "AWS::IAM::Policy").items():
        for statement in statements(policy):
            yield logical_id, statement
    for logical_id, policy in by_type(resources, "AWS::SQS::QueuePolicy").items():
        for statement in statements(policy):
            yield logical_id, statement
    for logical_id, policy in by_type(resources, "AWS::S3::BucketPolicy").items():
        for statement in statements(policy):
            yield logical_id, statement


def role_ref_for(resources, role_path):
    logical_id, _role = one_by_path(resources, "AWS::IAM::Role", role_path)
    return {"Ref": logical_id}


def policies_for_role(resources, role_path):
    role_ref = role_ref_for(resources, role_path)
    return [
        policy
        for policy in by_type(resources, "AWS::IAM::Policy").values()
        if role_ref in props(policy).get("Roles", [])
    ]


def test_network_topology_and_private_connectivity(resources):
    vpcs = by_type(resources, "AWS::EC2::VPC")
    subnets = by_type(resources, "AWS::EC2::Subnet")
    endpoints = by_type(resources, "AWS::EC2::VPCEndpoint")

    assert len(vpcs) == 1
    assert next(iter(vpcs.values()))["Properties"]["CidrBlock"] == "10.40.0.0/16"
    assert len(subnets) == 4
    assert len(by_type(resources, "AWS::EC2::NatGateway")) == 1
    assert len(by_type(resources, "AWS::EC2::InternetGateway")) == 1

    availability_zone_indices = set()
    for subnet in subnets.values():
        availability_zone = props(subnet)["AvailabilityZone"]
        if isinstance(availability_zone, dict):
            availability_zone_indices.add(availability_zone["Fn::Select"][0])
        else:
            availability_zone_indices.add(availability_zone)
    assert len(availability_zone_indices) == 2

    public_subnets = [
        subnet for subnet in subnets.values()
        if any(tag["Key"] == "aws-cdk:subnet-type" and tag["Value"] == "Public" for tag in props(subnet)["Tags"])
    ]
    private_subnets = [
        subnet for subnet in subnets.values()
        if any(tag["Key"] == "aws-cdk:subnet-type" and tag["Value"] == "Private" for tag in props(subnet)["Tags"])
    ]
    isolated_subnets = [
        subnet for subnet in subnets.values()
        if any(tag["Key"] == "aws-cdk:subnet-type" and tag["Value"] == "Isolated" for tag in props(subnet)["Tags"])
    ]

    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert len(isolated_subnets) == 0
    assert all(props(subnet)["MapPublicIpOnLaunch"] is True for subnet in public_subnets)
    assert all(props(subnet)["MapPublicIpOnLaunch"] is False for subnet in private_subnets)

    endpoint_types = {props(endpoint)["VpcEndpointType"] for endpoint in endpoints.values()}
    assert endpoint_types == {"Gateway", "Interface"}
    assert any(".s3" in as_text(props(endpoint)["ServiceName"]) for endpoint in endpoints.values())
    assert any("secretsmanager" in as_text(props(endpoint)["ServiceName"]) for endpoint in endpoints.values())


def test_security_groups_are_tightly_scoped(resources):
    lambda_sg_id, lambda_sg = one_by_path(resources, "AWS::EC2::SecurityGroup", "/LambdaRunspaceSecurityGroup/Resource")
    db_sg_id, _db_sg = one_by_path(resources, "AWS::EC2::SecurityGroup", "/DatabaseSecurityGroup/Resource")

    assert props(lambda_sg)["SecurityGroupEgress"] == [
        {
            "CidrIp": "0.0.0.0/0",
            "Description": "Allow all outbound traffic by default",
            "IpProtocol": "-1",
        }
    ]
    assert "SecurityGroupIngress" not in props(lambda_sg)

    ingress_rules = by_type(resources, "AWS::EC2::SecurityGroupIngress")
    assert all(lambda_sg_id not in as_text(props(rule).get("GroupId")) for rule in ingress_rules.values())

    db_ingress = [
        rule for rule in ingress_rules.values()
        if db_sg_id in as_text(props(rule).get("GroupId"))
    ]
    assert len(db_ingress) == 1
    assert props(db_ingress[0])["FromPort"] == 5432
    assert props(db_ingress[0])["ToPort"] == 5432
    assert props(db_ingress[0])["IpProtocol"] == "tcp"
    assert lambda_sg_id in as_text(props(db_ingress[0])["SourceSecurityGroupId"])


def test_api_gateway_stage_routes_and_access_logging(resources):
    assert len(by_type(resources, "AWS::ApiGateway::RestApi")) == 1

    _, stage = one(resources, "AWS::ApiGateway::Stage")
    assert props(stage)["StageName"] == "standby"
    assert props(stage)["AccessLogSetting"]["DestinationArn"]

    _, api_log_group = one_by_path(resources, "AWS::Logs::LogGroup", "/ApiAccessLogGroup/Resource")
    assert props(api_log_group)["RetentionInDays"] == 7
    assert "KmsKeyId" not in props(api_log_group)

    _, health_resource = one(resources, "AWS::ApiGateway::Resource", lambda _id, resource: props(resource).get("PathPart") == "health")
    _, orders_resource = one(resources, "AWS::ApiGateway::Resource", lambda _id, resource: props(resource).get("PathPart") == "orders")

    _, health_method = one(
        resources,
        "AWS::ApiGateway::Method",
        lambda _id, resource: props(resource)["HttpMethod"] == "GET"
        and props(resource)["ResourceId"] == {"Ref": one(resources, "AWS::ApiGateway::Resource", lambda _i, r: r == health_resource)[0]},
    )
    _, order_method = one(
        resources,
        "AWS::ApiGateway::Method",
        lambda _id, resource: props(resource)["HttpMethod"] == "POST"
        and props(resource)["ResourceId"] == {"Ref": one(resources, "AWS::ApiGateway::Resource", lambda _i, r: r == orders_resource)[0]},
    )

    assert "HealthHandler" in as_text(props(health_method)["Integration"])
    assert "OrderHandler" in as_text(props(order_method)["Integration"])


def test_lambda_runtime_sizing_network_and_environment(resources):
    lambda_sg_id, _lambda_sg = one_by_path(resources, "AWS::EC2::SecurityGroup", "/LambdaRunspaceSecurityGroup/Resource")

    expected = {
        "/HealthHandler/Resource": {
            "memory": 128,
            "timeout": 5,
            "env": {"AWS_ENDPOINT", "SECRET_ARN", "DATABASE_ENDPOINT_ADDRESS", "DATABASE_PORT"},
        },
        "/OrderHandler/Resource": {
            "memory": 256,
            "timeout": 10,
            "env": {"AWS_ENDPOINT", "ORDER_QUEUE_URL", "PIPE_SOURCE_QUEUE_URL", "EVENT_BUS_NAME"},
        },
        "/SecretsHelper/Resource": {
            "memory": 128,
            "timeout": 10,
            "env": {"AWS_ENDPOINT", "SECRET_ARN"},
        },
    }

    for path, requirements in expected.items():
        _, function = one_by_path(resources, "AWS::Lambda::Function", path)
        function_props = props(function)
        env = function_props["Environment"]["Variables"]

        assert function_props["Runtime"] == "nodejs20.x"
        assert function_props["PackageType"] if "PackageType" in function_props else True
        assert "ImageUri" not in function_props.get("Code", {})
        assert function_props["MemorySize"] == requirements["memory"]
        assert function_props["Timeout"] == requirements["timeout"]
        assert requirements["env"] <= set(env)
        assert lambda_sg_id in as_text(function_props["VpcConfig"]["SecurityGroupIds"])
        assert len(function_props["VpcConfig"]["SubnetIds"]) == 2


def test_lambda_log_groups_are_configured(resources):
    api_lambda_log_paths = ["/HealthHandlerLogGroup/Resource", "/OrderHandlerLogGroup/Resource"]
    for path in api_lambda_log_paths:
        _, log_group = one_by_path(resources, "AWS::Logs::LogGroup", path)
        assert props(log_group)["RetentionInDays"] == 14
        assert "KmsKeyId" not in props(log_group)

    _, helper_log_group = one_by_path(resources, "AWS::Logs::LogGroup", "/SecretsHelperLogGroup/Resource")
    assert props(helper_log_group)["RetentionInDays"] == 14
    assert "KmsKeyId" not in props(helper_log_group)


def test_sqs_eventbridge_and_queue_policy(resources):
    queues = by_type(resources, "AWS::SQS::Queue")
    assert len(queues) == 3

    _, order_queue = one_by_path(resources, "AWS::SQS::Queue", "/OrderQueue/Resource")
    _, _dlq = one_by_path(resources, "AWS::SQS::Queue", "/OrderDeadLetterQueue/Resource")
    _, _pipe_queue = one_by_path(resources, "AWS::SQS::Queue", "/PipeSourceQueue/Resource")
    assert "QueueName" not in props(order_queue)
    assert props(order_queue)["RedrivePolicy"]["maxReceiveCount"] == 3
    assert "OrderDeadLetterQueue" in as_text(props(order_queue)["RedrivePolicy"])

    _, rule = one(resources, "AWS::Events::Rule")
    assert props(rule)["EventPattern"] == {"source": ["orders.api"]}
    assert "OrderQueue" in as_text(props(rule)["Targets"])

    _, queue_policy = one_by_path(resources, "AWS::SQS::QueuePolicy", "/OrderQueue/Policy/Resource")
    send_statements = [
        statement for statement in statements(queue_policy)
        if "events.amazonaws.com" in as_text(statement.get("Principal"))
    ]
    assert len(send_statements) == 1
    statement = send_statements[0]
    assert {"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"} == action_set(statement)
    assert "OrderReceivedRule" in as_text(statement["Condition"])
    assert "OrderQueue" in as_text(statement["Resource"])


def test_order_handler_permissions_are_resource_scoped(resources):
    policies = policies_for_role(resources, "/OrderHandler/ServiceRole/Resource")
    assert len(policies) == 1
    policy_statements = statements(policies[0])

    sqs_targets = [
        statement for statement in policy_statements
        if "sqs:SendMessage" in action_set(statement)
    ]
    assert len(sqs_targets) == 2
    assert any("OrderQueue" in as_text(statement["Resource"]) for statement in sqs_targets)
    assert any("PipeSourceQueue" in as_text(statement["Resource"]) for statement in sqs_targets)
    assert all(action_set(statement) == {"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"} for statement in sqs_targets)

    event_targets = [
        statement for statement in policy_statements
        if action_set(statement) == {"events:PutEvents"}
    ]
    assert len(event_targets) == 1
    assert "OrderEventBus" in as_text(event_targets[0]["Resource"])


def test_recovery_state_machine_pipe_and_helper_permissions(resources):
    _, state_machine = one(resources, "AWS::StepFunctions::StateMachine")
    definition = as_text(props(state_machine)["DefinitionString"])
    assert props(state_machine)["StateMachineType"] == "STANDARD"
    assert "BeginRecovery" in definition
    assert "CheckSecrets" in definition
    assert "RecoveryReady" in definition
    assert "Wait" not in definition
    assert "Choice" not in definition

    _, pipe = one(resources, "AWS::Pipes::Pipe")
    pipe_props = props(pipe)
    assert pipe_props["DesiredState"] == "RUNNING"
    assert "PipeSourceQueue" in as_text(pipe_props["Source"])
    assert "SecretsHelper" in as_text(pipe_props["Enrichment"])
    assert "FailureRecoveryStateMachine" in as_text(pipe_props["Target"])
    assert pipe_props["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert pipe_props["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"

    pipe_policies = policies_for_role(resources, "/PipeRole/Resource")
    pipe_actions = [action_set(statement) for policy in pipe_policies for statement in statements(policy)]
    assert any({"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"} <= actions for actions in pipe_actions)
    assert {"lambda:InvokeFunction"} in pipe_actions
    assert {"states:StartExecution"} in pipe_actions

    helper_policies = policies_for_role(resources, "/SecretsHelper/ServiceRole/Resource")
    helper_statements = [statement for policy in helper_policies for statement in statements(policy)]
    secret_statements = [
        statement for statement in helper_statements
        if action_set(statement) == {"secretsmanager:GetSecretValue"}
    ]
    assert len(secret_statements) == 1
    assert "DatabaseSecret" in as_text(secret_statements[0]["Resource"])

    _, helper = one_by_path(resources, "AWS::Lambda::Function", "/SecretsHelper/Resource")
    helper_code = props(helper)["Code"]["ZipFile"]
    assert "GetSecretValueCommand" in helper_code
    assert "console.log" not in helper_code


def test_stateful_store_configuration(resources):
    _, secret = one(resources, "AWS::SecretsManager::Secret")
    generated = props(secret)["GenerateSecretString"]
    assert json.loads(generated["SecretStringTemplate"]) == {"username": "app_user"}
    assert generated["GenerateStringKey"] == "password"

    _, subnet_group = one(resources, "AWS::RDS::DBSubnetGroup")
    assert len(props(subnet_group)["SubnetIds"]) == 2

    _, db_instance = one(resources, "AWS::RDS::DBInstance")
    db_props = props(db_instance)
    assert db_props["Engine"] == "postgres"
    assert db_props["EngineVersion"].startswith("15")
    assert db_props["DBInstanceClass"] == "db.t3.micro"
    assert db_props["AllocatedStorage"] == "20"
    assert db_props["MultiAZ"] is False
    assert db_props["PubliclyAccessible"] is False
    assert db_props["BackupRetentionPeriod"] == 1
    assert db_props["DeletionProtection"] is False
    assert db_props["StorageEncrypted"] is True
    assert "DatabaseSecurityGroup" in as_text(db_props["VPCSecurityGroups"])
    assert "{{resolve:secretsmanager:" in as_text(db_props["MasterUserPassword"])


def test_analytics_snapshot_configuration(resources):
    _, bucket = one(resources, "AWS::S3::Bucket")
    bucket_props = props(bucket)
    assert bucket_props["VersioningConfiguration"]["Status"] == "Enabled"
    assert bucket_props["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert len(by_type(resources, "AWS::S3::BucketPolicy")) == 0

    _, glue_db = one(resources, "AWS::Glue::Database")
    glue_database_name = props(glue_db)["DatabaseInput"]["Name"]
    assert glue_database_name.endswith("_standby_logs_db")

    _, crawler = one(resources, "AWS::Glue::Crawler")
    crawler_props = props(crawler)
    assert crawler_props["DatabaseName"] == glue_database_name
    assert "Schedule" not in crawler_props
    assert "JdbcTargets" not in crawler_props["Targets"]
    assert "/orders/" in as_text(crawler_props["Targets"]["S3Targets"])

    _, workgroup = one(resources, "AWS::Athena::WorkGroup")
    workgroup_config = props(workgroup)["WorkGroupConfiguration"]
    assert props(workgroup)["State"] == "ENABLED"
    assert workgroup_config["EnforceWorkGroupConfiguration"] is True
    assert workgroup_config["PublishCloudWatchMetricsEnabled"] is True
    assert workgroup_config["ResultConfiguration"]["EncryptionConfiguration"]["EncryptionOption"] == "SSE_S3"
    assert "/athena-results/" in as_text(workgroup_config["ResultConfiguration"]["OutputLocation"])


def test_no_wildcard_iam_actions_or_plaintext_passwords(resources):
    for logical_id, statement in all_policy_statements(resources):
        actions = action_set(statement)
        assert "*" not in actions, f"{logical_id} contains Action '*'"

    template_text = as_text(resources)
    assert "plaintext" not in template_text.lower()
