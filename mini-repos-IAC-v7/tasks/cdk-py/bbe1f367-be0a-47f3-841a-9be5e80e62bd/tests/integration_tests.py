import os
import importlib.util
import sys
from pathlib import Path

os.environ.setdefault("HOME", "/tmp")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp")

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template


def _load_app_module():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    spec = importlib.util.spec_from_file_location("app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules["app"] = module
    spec.loader.exec_module(module)
    return module


app = _load_app_module()
SecurityBaselineStack = app.SecurityBaselineStack


def _template() -> Template:
    cdk_app = cdk.App()
    stack = SecurityBaselineStack(cdk_app, "SecurityBaselineStack")
    return Template.from_stack(stack)


def _template_json() -> dict:
    return _template().to_json()


def _find_role_logical_id(template: Template, prefix: str) -> str:
    matching_roles = [
        logical_id
        for logical_id in template.find_resources("AWS::IAM::Role")
        if logical_id.startswith(prefix)
    ]
    assert len(matching_roles) == 1, f"expected exactly one role with prefix {prefix}"
    return matching_roles[0]


def _policies_for_role(template: Template, role_logical_id: str) -> list[dict]:
    statements = []
    for policy in template.find_resources("AWS::IAM::Policy").values():
        role_refs = policy["Properties"].get("Roles", [])
        if {"Ref": role_logical_id} in role_refs:
            document = policy["Properties"]["PolicyDocument"]
            policy_statements = document["Statement"]
            if isinstance(policy_statements, dict):
                statements.append(policy_statements)
            else:
                statements.extend(policy_statements)
    return statements


def _statement_actions(statement: dict) -> list[str]:
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def _principal_service(role_resource: dict) -> str:
    principal = role_resource["Properties"]["AssumeRolePolicyDocument"]["Statement"][0][
        "Principal"
    ]["Service"]
    return principal[0] if isinstance(principal, list) else principal


def _logical_id_by_prefix(template_json: dict, resource_type: str, prefix: str) -> str:
    for logical_id, resource in template_json["Resources"].items():
        if resource["Type"] == resource_type and logical_id.startswith(prefix):
            return logical_id
    raise AssertionError(f"{resource_type} with prefix {prefix} not found")


def _statement_by_sid(template: Template, role_logical_id: str, sid: str) -> dict:
    for statement in _policies_for_role(template, role_logical_id):
        if statement.get("Sid") == sid:
            return statement
    raise AssertionError(f"statement {sid} not found for {role_logical_id}")


def test_acceptance_resource_counts():
    template = _template()

    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::ApiGatewayV2::Api", 1)
    template.resource_count_is("AWS::SQS::Queue", 1)
    template.resource_count_is("AWS::Pipes::Pipe", 1)
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.resource_count_is("AWS::RDS::DBInstance", 1)
    template.resource_count_is("AWS::SecretsManager::Secret", 1)
    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::EC2::SecurityGroup", 2)
    template.resource_count_is("AWS::Logs::LogGroup", 2)
    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::Glue::Crawler", 1)


def test_lambda_runspaces_use_zip_runtime_and_only_required_wiring():
    template_json = _template_json()
    ingest_handler_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "IngestHandler"
    )
    workflow_worker_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "WorkflowWorker"
    )
    db_instance_id = _logical_id_by_prefix(template_json, "AWS::RDS::DBInstance", "StatefulStore")

    ingest_properties = template_json["Resources"][ingest_handler_id]["Properties"]
    workflow_properties = template_json["Resources"][workflow_worker_id]["Properties"]

    assert ingest_properties["Runtime"] == "python3.12"
    assert ingest_properties["MemorySize"] == 256
    assert ingest_properties["Timeout"] == 10
    assert ingest_properties["Handler"] == "index.handler"
    assert "ZipFile" in ingest_properties["Code"]
    assert ingest_properties.get("PackageType", "Zip") == "Zip"
    assert len(ingest_properties["VpcConfig"]["SubnetIds"]) >= 2
    assert len(ingest_properties["VpcConfig"]["SecurityGroupIds"]) == 1
    ingest_environment = ingest_properties["Environment"]["Variables"]
    assert "QUEUE_URL" in ingest_environment
    assert set(ingest_environment) <= {"QUEUE_URL", "AWS_ENDPOINT"}

    assert workflow_properties["Runtime"] == "python3.12"
    assert workflow_properties["MemorySize"] == 256
    assert workflow_properties["Timeout"] == 15
    assert workflow_properties["Handler"] == "index.handler"
    assert "ZipFile" in workflow_properties["Code"]
    assert workflow_properties.get("PackageType", "Zip") == "Zip"
    assert len(workflow_properties["VpcConfig"]["SubnetIds"]) >= 2
    assert len(workflow_properties["VpcConfig"]["SecurityGroupIds"]) == 1
    workflow_environment = workflow_properties["Environment"]["Variables"]
    assert "DB_SECRET_ARN" in workflow_environment
    assert set(workflow_environment) <= {"DB_SECRET_ARN", "AWS_ENDPOINT", "DB_HOST"}
    assert len(workflow_environment) in {2, 3}
    assert any(
        value == {"Fn::GetAtt": [db_instance_id, "Endpoint.Address"]}
        for key, value in workflow_environment.items()
        if key not in {"DB_SECRET_ARN", "AWS_ENDPOINT"}
    )


def test_log_groups_match_the_two_lambda_runspaces_and_api_logs_are_explicit():
    template_json = _template_json()
    ingest_handler_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "IngestHandler"
    )
    workflow_worker_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "WorkflowWorker"
    )
    ingest_log_group_id = _logical_id_by_prefix(
        template_json, "AWS::Logs::LogGroup", "IngestHandlerLogGroup"
    )
    workflow_log_group_id = _logical_id_by_prefix(
        template_json, "AWS::Logs::LogGroup", "WorkflowWorkerLogGroup"
    )
    stage_id = _logical_id_by_prefix(template_json, "AWS::ApiGatewayV2::Stage", "IngressDefaultStage")

    log_groups = {
        ingest_log_group_id: template_json["Resources"][ingest_log_group_id]["Properties"],
        workflow_log_group_id: template_json["Resources"][workflow_log_group_id]["Properties"],
    }

    assert len(log_groups) == 2
    assert (
        log_groups[ingest_log_group_id]["LogGroupName"]
        == f"/aws/lambda/{template_json['Resources'][ingest_handler_id]['Properties']['FunctionName']}"
    )
    assert (
        log_groups[workflow_log_group_id]["LogGroupName"]
        == f"/aws/lambda/{template_json['Resources'][workflow_worker_id]['Properties']['FunctionName']}"
    )
    for resource in log_groups.values():
        assert resource["RetentionInDays"] == 14
        assert "KmsKeyId" not in resource

    stage_properties = template_json["Resources"][stage_id]["Properties"]
    assert stage_properties["AccessLogSettings"]["DestinationArn"] == {
        "Fn::GetAtt": [ingest_log_group_id, "Arn"]
    }
    access_log_format = stage_properties["AccessLogSettings"]["Format"]
    assert isinstance(access_log_format, str)
    assert access_log_format
    assert "$context." in access_log_format


def test_api_gateway_pipe_and_state_machine_wiring_are_exact():
    template_json = _template_json()
    ingest_handler_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "IngestHandler"
    )
    workflow_worker_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "WorkflowWorker"
    )
    queue_id = _logical_id_by_prefix(template_json, "AWS::SQS::Queue", "IngestQueue")
    state_machine_id = _logical_id_by_prefix(
        template_json, "AWS::StepFunctions::StateMachine", "WorkflowStateMachine"
    )
    integration_id = _logical_id_by_prefix(
        template_json, "AWS::ApiGatewayV2::Integration", "IngestLambdaIntegration"
    )
    route_id = _logical_id_by_prefix(template_json, "AWS::ApiGatewayV2::Route", "IngestRoute")
    pipe_id = _logical_id_by_prefix(template_json, "AWS::Pipes::Pipe", "IngestPipe")

    queue_properties = template_json["Resources"][queue_id]["Properties"]
    assert queue_properties["VisibilityTimeout"] == 30
    assert queue_properties["SqsManagedSseEnabled"] is True

    state_machine_properties = template_json["Resources"][state_machine_id]["Properties"]
    assert state_machine_properties["StateMachineType"] == "STANDARD"
    assert "RunWorkflowWorker" in str(state_machine_properties["DefinitionString"])
    assert "WorkflowSucceeded" in str(state_machine_properties["DefinitionString"])
    assert workflow_worker_id in str(state_machine_properties["DefinitionString"])

    integration_properties = template_json["Resources"][integration_id]["Properties"]
    assert integration_properties["IntegrationType"] == "AWS_PROXY"
    assert ingest_handler_id in str(integration_properties["IntegrationUri"])

    route_properties = template_json["Resources"][route_id]["Properties"]
    assert route_properties["RouteKey"] == "POST /ingest"
    assert route_properties["Target"] == {"Fn::Join": ["", ["integrations/", {"Ref": integration_id}]]}

    pipe_properties = template_json["Resources"][pipe_id]["Properties"]
    assert pipe_properties["Source"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert pipe_properties["Enrichment"] == {"Fn::GetAtt": [ingest_handler_id, "Arn"]}
    assert pipe_properties["Target"] == {"Ref": state_machine_id}
    assert set(pipe_properties["SourceParameters"]) == {"SqsQueueParameters"}
    assert set(pipe_properties["TargetParameters"]) == {"StepFunctionStateMachineParameters"}
    assert "FilterCriteria" not in pipe_properties
    assert (
        pipe_properties["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
        == "FIRE_AND_FORGET"
    )


def test_network_topology_and_security_group_isolation():
    template = _template()
    template_json = _template_json()
    lambda_security_group_id = _logical_id_by_prefix(
        template_json, "AWS::EC2::SecurityGroup", "LambdaSecurityGroup"
    )
    stateful_store_security_group_id = _logical_id_by_prefix(
        template_json, "AWS::EC2::SecurityGroup", "StatefulStoreSecurityGroup"
    )
    nat_gateway_id = _logical_id_by_prefix(
        template_json, "AWS::EC2::NatGateway", "SecurityBaselineVpcPublicSubnet1NATGateway"
    )
    db_subnet_group_id = _logical_id_by_prefix(
        template_json, "AWS::RDS::DBSubnetGroup", "StatefulStoreSubnetGroup"
    )
    db_instance_id = _logical_id_by_prefix(template_json, "AWS::RDS::DBInstance", "StatefulStore")

    template.has_resource_properties(
        "AWS::EC2::SecurityGroupIngress",
        {
            "IpProtocol": "tcp",
            "FromPort": 5432,
            "ToPort": 5432,
            "SourceSecurityGroupId": Match.any_value(),
        },
    )
    template.has_resource_properties(
        "AWS::EC2::SecurityGroupEgress",
        {
            "IpProtocol": "tcp",
            "FromPort": 5432,
            "ToPort": 5432,
            "DestinationSecurityGroupId": Match.any_value(),
        },
    )
    assert nat_gateway_id
    lambda_security_group = template_json["Resources"][lambda_security_group_id]["Properties"]
    assert "SecurityGroupIngress" not in lambda_security_group
    assert lambda_security_group["VpcId"] == {
        "Ref": _logical_id_by_prefix(template_json, "AWS::EC2::VPC", "SecurityBaselineVpc")
    }
    lambda_sg_egress_rules = list(lambda_security_group.get("SecurityGroupEgress", []))
    lambda_sg_egress_rules.extend(
        resource["Properties"]
        for resource in template_json["Resources"].values()
        if resource["Type"] == "AWS::EC2::SecurityGroupEgress"
        and resource["Properties"]["GroupId"]
        == {"Fn::GetAtt": [lambda_security_group_id, "GroupId"]}
    )
    assert len(lambda_sg_egress_rules) == 2
    assert any(
        rule.get("IpProtocol") == "tcp"
        and rule.get("FromPort") == 443
        and rule.get("ToPort") == 443
        and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in lambda_sg_egress_rules
    )
    assert any(
        rule.get("IpProtocol") == "tcp"
        and rule.get("FromPort") == 5432
        and rule.get("ToPort") == 5432
        and rule.get("DestinationSecurityGroupId")
        == {"Fn::GetAtt": [stateful_store_security_group_id, "GroupId"]}
        for rule in lambda_sg_egress_rules
    )

    private_subnet_resources = [
        resource
        for logical_id, resource in template_json["Resources"].items()
        if resource["Type"] == "AWS::EC2::Subnet" and "PrivateSubnet" in logical_id
    ]
    assert len(private_subnet_resources) >= 2
    for subnet in private_subnet_resources:
        availability_zone = subnet["Properties"]["AvailabilityZone"]
        assert not isinstance(availability_zone, str)
        assert "Fn::GetAZs" in str(availability_zone)

    for function in template.find_resources("AWS::Lambda::Function").values():
        vpc_config = function["Properties"]["VpcConfig"]
        assert len(vpc_config["SubnetIds"]) >= 2
        assert len(vpc_config["SecurityGroupIds"]) == 1
        for subnet in vpc_config["SubnetIds"]:
            assert "PrivateSubnet" in str(subnet)
        assert vpc_config["SecurityGroupIds"] == [
            {"Fn::GetAtt": [lambda_security_group_id, "GroupId"]}
        ]

    db_subnet_group = template_json["Resources"][db_subnet_group_id]["Properties"]
    assert len(db_subnet_group["SubnetIds"]) >= 2
    for subnet in db_subnet_group["SubnetIds"]:
        assert "PrivateSubnet" in str(subnet)

    db_properties = template_json["Resources"][db_instance_id]["Properties"]
    assert db_properties["DBSubnetGroupName"] == {"Ref": db_subnet_group_id}
    assert db_properties["VPCSecurityGroups"] == [
        {"Fn::GetAtt": [stateful_store_security_group_id, "GroupId"]}
    ]
    db_security_group = template_json["Resources"][stateful_store_security_group_id]["Properties"]
    assert any(
        rule.get("IpProtocol") == "-1" and rule.get("CidrIp") == "0.0.0.0/0"
        for rule in db_security_group.get("SecurityGroupEgress", [])
    )


def test_rds_secret_and_crawler_target_configuration():
    template = _template()
    template_json = _template_json()
    secret_id = _logical_id_by_prefix(template_json, "AWS::SecretsManager::Secret", "DatabaseSecret")
    db_instance_id = _logical_id_by_prefix(template_json, "AWS::RDS::DBInstance", "StatefulStore")
    bucket_id = _logical_id_by_prefix(template_json, "AWS::S3::Bucket", "CrawlerBucket")

    template.has_resource_properties(
        "AWS::RDS::DBInstance",
        {
            "Engine": "postgres",
            "DBInstanceClass": "db.t3.micro",
            "AllocatedStorage": "20",
            "MultiAZ": False,
            "PubliclyAccessible": False,
            "DeletionProtection": False,
            "StorageEncrypted": True,
        },
    )
    secret_properties = template_json["Resources"][secret_id]["Properties"]
    assert "GenerateSecretString" in secret_properties
    assert "SecretString" not in secret_properties
    assert "username" in secret_properties["GenerateSecretString"]["SecretStringTemplate"]
    assert secret_properties["GenerateSecretString"]["GenerateStringKey"] == "password"

    db_properties = template_json["Resources"][db_instance_id]["Properties"]
    assert secret_id in str(db_properties["MasterUsername"])
    assert secret_id in str(db_properties["MasterUserPassword"])
    assert "resolve:secretsmanager" in str(db_properties["MasterUsername"])
    assert "resolve:secretsmanager" in str(db_properties["MasterUserPassword"])

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        }
                    }
                ]
            },
            "VersioningConfiguration": {"Status": "Enabled"},
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )
    bucket_properties = template_json["Resources"][bucket_id]["Properties"]
    assert "LifecycleConfiguration" not in bucket_properties
    crawler = next(iter(template.find_resources("AWS::Glue::Crawler").values()))
    crawler_target_path = crawler["Properties"]["Targets"]["S3Targets"][0]["Path"]
    assert "s3://" in str(crawler_target_path)
    assert bucket_id in str(crawler_target_path)


def test_iam_permissions_are_scoped_to_each_runspace():
    template = _template()
    template_json = _template_json()
    ingest_log_group_id = _logical_id_by_prefix(
        template_json, "AWS::Logs::LogGroup", "IngestHandlerLogGroup"
    )
    workflow_log_group_id = _logical_id_by_prefix(
        template_json, "AWS::Logs::LogGroup", "WorkflowWorkerLogGroup"
    )
    queue_id = _logical_id_by_prefix(template_json, "AWS::SQS::Queue", "IngestQueue")
    secret_id = _logical_id_by_prefix(template_json, "AWS::SecretsManager::Secret", "DatabaseSecret")
    ingest_handler_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "IngestHandler"
    )
    workflow_worker_id = _logical_id_by_prefix(
        template_json, "AWS::Lambda::Function", "WorkflowWorker"
    )
    state_machine_id = _logical_id_by_prefix(
        template_json, "AWS::StepFunctions::StateMachine", "WorkflowStateMachine"
    )

    ingest_role = _find_role_logical_id(template, "IngestHandlerRole")
    workflow_role = _find_role_logical_id(template, "WorkflowWorkerRole")
    pipe_role = _find_role_logical_id(template, "EventBridgePipeRole")
    state_machine_role = _find_role_logical_id(template, "WorkflowStateMachineRole")

    role_resources = template.find_resources("AWS::IAM::Role")
    assert (
        sum(
            1
            for role in role_resources.values()
            if _principal_service(role) == "lambda.amazonaws.com"
        )
        == 2
    )
    assert (
        sum(
            1
            for role in role_resources.values()
            if _principal_service(role) == "pipes.amazonaws.com"
        )
        == 1
    )
    assert (
        sum(
            1
            for role in role_resources.values()
            if _principal_service(role) == "states.amazonaws.com"
        )
        == 1
    )

    ingest_actions = {
        action for statement in _policies_for_role(template, ingest_role) for action in _statement_actions(statement)
    }
    workflow_actions = {
        action for statement in _policies_for_role(template, workflow_role) for action in _statement_actions(statement)
    }
    pipe_actions = {
        action for statement in _policies_for_role(template, pipe_role) for action in _statement_actions(statement)
    }
    state_machine_actions = {
        action
        for statement in _policies_for_role(template, state_machine_role)
        for action in _statement_actions(statement)
    }
    vpc_networking_actions = {
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "ec2:AssignPrivateIpAddresses",
        "ec2:UnassignPrivateIpAddresses",
    }

    assert "sqs:SendMessage" in ingest_actions
    assert "states:StartExecution" not in ingest_actions
    assert ingest_actions <= (
        vpc_networking_actions
        | {"logs:CreateLogStream", "logs:PutLogEvents", "sqs:SendMessage", "secretsmanager:GetSecretValue"}
    )

    assert "secretsmanager:GetSecretValue" in workflow_actions
    assert "sqs:SendMessage" not in workflow_actions
    assert "states:StartExecution" not in workflow_actions
    assert workflow_actions <= (
        vpc_networking_actions
        | {"logs:CreateLogStream", "logs:PutLogEvents", "secretsmanager:GetSecretValue"}
    )

    assert "sqs:ReceiveMessage" in pipe_actions
    assert "lambda:InvokeFunction" in pipe_actions
    assert "states:StartExecution" in pipe_actions

    assert state_machine_actions == {"lambda:InvokeFunction"}

    ingest_log_statement = _statement_by_sid(template, ingest_role, "WriteIngestLogs")
    assert ingest_log_statement["Resource"] == [
        {"Fn::GetAtt": [ingest_log_group_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [ingest_log_group_id, "Arn"]}, ":*"]]},
    ]
    assert _statement_by_sid(template, ingest_role, "SendToIngressQueue")["Resource"] == {
        "Fn::GetAtt": [queue_id, "Arn"]
    }
    ingest_secret_statements = [
        statement
        for statement in _policies_for_role(template, ingest_role)
        if "secretsmanager:GetSecretValue" in _statement_actions(statement)
    ]
    assert len(ingest_secret_statements) <= 1
    if ingest_secret_statements:
        assert ingest_secret_statements[0]["Resource"] == {"Ref": secret_id}

    workflow_log_statement = _statement_by_sid(template, workflow_role, "WriteWorkflowLogs")
    assert workflow_log_statement["Resource"] == [
        {"Fn::GetAtt": [workflow_log_group_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [workflow_log_group_id, "Arn"]}, ":*"]]},
    ]
    assert _statement_by_sid(template, workflow_role, "ReadDatabaseSecret")["Resource"] == {
        "Ref": secret_id
    }

    assert _statement_by_sid(template, pipe_role, "ReadIngressQueue")["Resource"] == {
        "Fn::GetAtt": [queue_id, "Arn"]
    }
    assert _statement_by_sid(template, pipe_role, "InvokeEnrichmentLambda")["Resource"] == {
        "Fn::GetAtt": [ingest_handler_id, "Arn"]
    }
    assert _statement_by_sid(template, pipe_role, "StartWorkflowExecution")["Resource"] == {
        "Ref": state_machine_id
    }

    for statement in _policies_for_role(template, state_machine_role):
        assert statement["Resource"] in [
            {"Fn::GetAtt": [workflow_worker_id, "Arn"]},
            {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": [workflow_worker_id, "Arn"]}, ":*"],
                ]
            },
            [
                {"Fn::GetAtt": [workflow_worker_id, "Arn"]},
                {
                    "Fn::Join": [
                        "",
                        [{"Fn::GetAtt": [workflow_worker_id, "Arn"]}, ":*"],
                    ]
                },
            ],
        ]

    for role_logical_id in [ingest_role, workflow_role, pipe_role, state_machine_role]:
        for statement in _policies_for_role(template, role_logical_id):
            if statement["Resource"] == "*":
                assert set(_statement_actions(statement)) <= vpc_networking_actions


def test_state_machine_definition_and_no_retain_behaviour():
    template = _template()
    template_json = _template_json()

    definition = next(iter(template.find_resources("AWS::StepFunctions::StateMachine").values()))["Properties"][
        "DefinitionString"
    ]
    assert "RunWorkflowWorker" in str(definition)
    assert "WorkflowSucceeded" in str(definition)

    for resource in template_json["Resources"].values():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}
