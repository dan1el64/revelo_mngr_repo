from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import re
import sys

import aws_cdk as cdk
from aws_cdk.assertions import Template


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"
APP_SPEC = spec_from_file_location("app", APP_PATH)
APP_MODULE = module_from_spec(APP_SPEC)
sys.modules["app"] = APP_MODULE
assert APP_SPEC and APP_SPEC.loader
APP_SPEC.loader.exec_module(APP_MODULE)
PocStack = APP_MODULE.PocStack


def synth_template(**stack_kwargs) -> dict:
    app = cdk.App()
    stack = PocStack(app, "TestPocStack", **stack_kwargs)
    return Template.from_stack(stack).to_json()


def resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def single_resource(template: dict, resource_type: str) -> tuple[str, dict]:
    resources = resources_by_type(template, resource_type)
    assert len(resources) == 1
    return next(iter(resources.items()))


def sg_refs_by_description(template: dict) -> dict:
    refs = {}
    for logical_id, resource in resources_by_type(template, "AWS::EC2::SecurityGroup").items():
        refs[resource["Properties"]["GroupDescription"]] = {"Fn::GetAtt": [logical_id, "GroupId"]}
    return refs


def private_subnet_refs(template: dict) -> list[dict]:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if not resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def public_subnet_refs(template: dict) -> list[dict]:
    return [
        {"Ref": logical_id}
        for logical_id, resource in resources_by_type(template, "AWS::EC2::Subnet").items()
        if resource["Properties"]["MapPublicIpOnLaunch"]
    ]


def policy_documents(template: dict) -> list[dict]:
    return [
        resource["Properties"]["PolicyDocument"]
        for resource in resources_by_type(template, "AWS::IAM::Policy").values()
    ]


def lambda_resources_by_shape(template: dict) -> dict:
    lambdas = list(resources_by_type(template, "AWS::Lambda::Function").values())
    backend = next(
        resource
        for resource in lambdas
        if resource["Properties"].get("ReservedConcurrentExecutions") == 20
    )
    processor = next(
        resource
        for resource in lambdas
        if resource["Properties"]["MemorySize"] == 512
    )
    return {"backend": backend, "processor": processor}


def attached_policy_for_role(template: dict, role_logical_id: str) -> dict:
    for resource in resources_by_type(template, "AWS::IAM::Policy").values():
        if resource["Properties"]["Roles"] == [{"Ref": role_logical_id}]:
            return resource["Properties"]["PolicyDocument"]
    raise AssertionError(f"No IAM policy attached to role {role_logical_id}")


def log_statement_from_policy(policy_document: dict) -> dict:
    for statement in policy_document["Statement"]:
        actions = statement["Action"]
        if actions == ["logs:CreateLogStream", "logs:PutLogEvents"]:
            return statement
    raise AssertionError("No logs statement found in policy")


def statement_by_actions(policy_document: dict, actions: list[str]) -> dict:
    for statement in policy_document["Statement"]:
        if statement["Action"] == actions:
            return statement
    raise AssertionError(f"No statement found for actions {actions}")


def log_group_logical_id_by_name(template: dict, log_group_name: str) -> str:
    for logical_id, resource in resources_by_type(template, "AWS::Logs::LogGroup").items():
        if resource["Properties"]["LogGroupName"] == log_group_name:
            return logical_id
    raise AssertionError(f"Log group {log_group_name} not found")


def log_group_logical_id_for_lambda(template: dict, lambda_resource: dict) -> str:
    function_name = lambda_resource["Properties"]["FunctionName"]
    return log_group_logical_id_by_name(template, f"/aws/lambda/{function_name}")


def test_frontend_and_api_gateway_contract():
    template = synth_template(aws_region="us-east-1")
    sg_refs = sg_refs_by_description(template)

    cluster_logical_id, _ = single_resource(template, "AWS::ECS::Cluster")
    _, load_balancer = single_resource(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    _, listener = single_resource(template, "AWS::ElasticLoadBalancingV2::Listener")
    _, target_group = single_resource(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
    _, service = single_resource(template, "AWS::ECS::Service")
    _, task_definition = single_resource(template, "AWS::ECS::TaskDefinition")
    rest_api_logical_id, rest_api = single_resource(template, "AWS::ApiGateway::RestApi")
    stage_logical_id, stage = single_resource(template, "AWS::ApiGateway::Stage")
    target_group_logical_id, _ = single_resource(template, "AWS::ElasticLoadBalancingV2::TargetGroup")

    assert load_balancer["Properties"]["Scheme"] == "internet-facing"
    assert load_balancer["Properties"]["Type"] == "application"
    assert sorted(str(subnet_id) for subnet_id in load_balancer["Properties"]["Subnets"]) == sorted(
        str(subnet_id) for subnet_id in public_subnet_refs(template)
    )
    assert listener["Properties"]["Port"] == 80
    assert listener["Properties"]["Protocol"] == "HTTP"
    assert target_group["Properties"]["Port"] == 80
    assert target_group["Properties"]["TargetType"] == "ip"
    assert target_group["Properties"]["HealthCheckPath"] == "/"
    assert target_group["Properties"]["Matcher"]["HttpCode"] == "200-399"

    awsvpc = service["Properties"]["NetworkConfiguration"]["AwsvpcConfiguration"]
    assert service["Properties"]["DesiredCount"] == 2
    assert service["Properties"]["LaunchType"] == "FARGATE"
    assert service["Properties"]["Cluster"] == {"Ref": cluster_logical_id}
    assert awsvpc["AssignPublicIp"] == "DISABLED"
    assert sg_refs["Frontend ALB security group"] in awsvpc["SecurityGroups"]
    assert sorted(str(subnet_id) for subnet_id in awsvpc["Subnets"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )

    container = task_definition["Properties"]["ContainerDefinitions"][0]
    assert task_definition["Properties"]["Cpu"] == "512"
    assert task_definition["Properties"]["Memory"] == "1024"
    assert isinstance(container["Image"], str)
    assert container["Image"]
    assert (
        container["Image"].startswith("public.ecr.aws/")
        or re.match(r"^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9._-]+)?$", container["Image"])
    )
    assert not re.match(r"^\d+\.dkr\.ecr\.[^.]+\.amazonaws\.com/", container["Image"])
    assert container["PortMappings"] == [{"ContainerPort": 80, "Protocol": "tcp"}]
    frontend_log_group_logical_id = container["LogConfiguration"]["Options"]["awslogs-group"]["Ref"]
    frontend_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[frontend_log_group_logical_id]
    assert frontend_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in frontend_log_group["Properties"]

    env_vars = {item["Name"]: item["Value"] for item in container["Environment"]}
    assert env_vars["BACKEND_API_BASE_URL"] == {
        "Fn::Join": [
            "",
            [
                "https://",
                {"Ref": rest_api_logical_id},
                ".execute-api.",
                {"Ref": "AWS::Region"},
                ".",
                {"Ref": "AWS::URLSuffix"},
                "/",
                {"Ref": stage_logical_id},
                "/",
            ],
        ]
    }

    assert rest_api["Properties"]["EndpointConfiguration"]["Types"] == ["REGIONAL"]
    assert any(
        setting["HttpMethod"] == "*"
        and setting["ResourcePath"] == "/*"
        and setting["LoggingLevel"] == "INFO"
        for setting in stage["Properties"]["MethodSettings"]
    )
    assert stage["Properties"]["AccessLogSetting"]["DestinationArn"]

    api_resources = resources_by_type(template, "AWS::ApiGateway::Resource")
    path_parts = {resource["Properties"]["PathPart"] for resource in api_resources.values()}
    assert path_parts == {"health", "items"}

    resources = resources_by_type(template, "AWS::ApiGateway::Resource")
    health_resource_id = next(
        logical_id for logical_id, resource in resources.items() if resource["Properties"]["PathPart"] == "health"
    )
    items_resource_id = next(
        logical_id for logical_id, resource in resources.items() if resource["Properties"]["PathPart"] == "items"
    )
    methods = list(resources_by_type(template, "AWS::ApiGateway::Method").values())
    assert any(
        method["Properties"]["HttpMethod"] == "GET"
        and method["Properties"]["ResourceId"] == {"Ref": health_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert any(
        method["Properties"]["HttpMethod"] == "GET"
        and method["Properties"]["ResourceId"] == {"Ref": items_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert any(
        method["Properties"]["HttpMethod"] == "POST"
        and method["Properties"]["ResourceId"] == {"Ref": items_resource_id}
        and method["Properties"]["Integration"]["Type"] == "AWS_PROXY"
        for method in methods
    )
    assert listener["Properties"]["DefaultActions"] == [
        {"TargetGroupArn": {"Ref": target_group_logical_id}, "Type": "forward"}
    ]
    assert any(
        lb["ContainerPort"] == 80 and lb["TargetGroupArn"] == {"Ref": target_group_logical_id}
        for lb in service["Properties"]["LoadBalancers"]
    )

    access_log_destination = stage["Properties"]["AccessLogSetting"]["DestinationArn"]
    access_log_group_logical_id = access_log_destination["Fn::GetAtt"][0]
    access_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[access_log_group_logical_id]
    assert access_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in access_log_group["Properties"]


def test_backend_lambda_code_and_vpc_configuration():
    template = synth_template(aws_region="us-east-1")
    lambdas = lambda_resources_by_shape(template)
    sg_refs = sg_refs_by_description(template)
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")

    backend = lambdas["backend"]
    code = str(backend["Properties"]["Code"]["ZipFile"])
    backend_log_group_logical_id = log_group_logical_id_for_lambda(template, backend)
    backend_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[backend_log_group_logical_id]

    assert backend["Properties"]["Runtime"] == "python3.12"
    assert backend["Properties"]["Architectures"] == ["arm64"]
    assert backend["Properties"]["MemorySize"] == 1024
    assert backend["Properties"]["Timeout"] == 10
    assert backend["Properties"]["ReservedConcurrentExecutions"] == 20
    assert backend["Properties"]["VpcConfig"]["SecurityGroupIds"] == [
        sg_refs["Backend service security group"]
    ]
    assert sorted(str(subnet_id) for subnet_id in backend["Properties"]["VpcConfig"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    assert backend_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in backend_log_group["Properties"]

    env_vars = backend["Properties"]["Environment"]["Variables"]
    assert env_vars["DB_NAME"] == "appdb"
    assert env_vars["EVENTS_SOURCE"] == "app.backend"
    assert env_vars["EVENTS_DETAIL_TYPE"] == "item.created"
    assert env_vars["DB_SECRET_ARN"] == {"Ref": secret_logical_id}
    assert env_vars["QUEUE_URL"] == {"Ref": queue_logical_id}

    assert 'path.endswith("/health")' in code
    assert '{"ok": True}' in code
    assert 'path.endswith("/items")' in code
    assert 'boto3.client("secretsmanager")' in code
    assert 'get_secret_value(SecretId=secret_arn)' in code
    assert 'os.environ["DB_SECRET_ARN"]' in code
    assert 'credentials["username"]' in code
    assert 'credentials["password"]' in code
    assert "LIMIT 20" in code
    assert "INSERT INTO" in code
    assert "RETURNING id" in code
    assert 'boto3.client("events").put_events' in code
    assert '"Source": os.environ["EVENTS_SOURCE"]' in code
    assert '"DetailType": os.environ["EVENTS_DETAIL_TYPE"]' in code
    assert re.search(r'"Detail"\s*:\s*json\.dumps\(\{\s*"id"\s*:', code)


def test_persistence_eventing_and_data_catalog_contract():
    template = synth_template(aws_region="us-east-1")
    sg_refs = sg_refs_by_description(template)
    lambdas = lambda_resources_by_shape(template)

    queue_logical_id, queue = single_resource(template, "AWS::SQS::Queue")
    _, rule = single_resource(template, "AWS::Events::Rule")
    _, event_source_mapping = single_resource(template, "AWS::Lambda::EventSourceMapping")
    _, bucket = single_resource(template, "AWS::S3::Bucket")
    _, glue_database = single_resource(template, "AWS::Glue::Database")
    _, crawler = single_resource(template, "AWS::Glue::Crawler")
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    _, db_subnet_group = single_resource(template, "AWS::RDS::DBSubnetGroup")
    _, db_instance = single_resource(template, "AWS::RDS::DBInstance")

    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert queue["Properties"]["MessageRetentionPeriod"] == 345600
    assert rule["Properties"]["EventPattern"] == {
        "detail-type": ["item.created"],
        "source": ["app.backend"],
    }
    assert len(rule["Properties"]["Targets"]) == 1
    assert rule["Properties"]["Targets"][0]["Arn"] == {"Fn::GetAtt": [queue_logical_id, "Arn"]}
    assert event_source_mapping["Properties"]["BatchSize"] == 10
    assert event_source_mapping["Properties"]["EventSourceArn"] == {
        "Fn::GetAtt": [queue_logical_id, "Arn"]
    }

    assert bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert "VersioningConfiguration" not in bucket["Properties"]

    assert "Schedule" not in crawler["Properties"]
    assert len(crawler["Properties"]["Targets"]["S3Targets"]) == 1
    glue_database_name = glue_database["Properties"]["DatabaseInput"]["Name"]
    crawler_name = crawler["Properties"]["Name"]
    assert crawler["Properties"]["DatabaseName"] == glue_database_name
    assert "/data/" in str(crawler["Properties"]["Targets"]["S3Targets"][0]["Path"])

    assert db_instance["Properties"]["DBInstanceClass"] == "db.t3.micro"
    assert db_instance["Properties"]["AllocatedStorage"] == "20"
    assert db_instance["Properties"]["StorageType"] == "gp2"
    assert db_instance["Properties"]["Engine"] == "postgres"
    assert str(db_instance["Properties"]["EngineVersion"]).startswith("16")
    assert db_instance["Properties"]["DBName"] == "appdb"
    assert db_instance["Properties"]["PubliclyAccessible"] is False
    assert db_instance["Properties"]["BackupRetentionPeriod"] == 0
    assert db_instance["Properties"]["DeleteAutomatedBackups"] is True
    assert "DBSnapshotIdentifier" not in db_instance["Properties"]
    assert db_instance["Properties"]["VPCSecurityGroups"] == [sg_refs["Database security group"]]
    assert sorted(str(subnet_id) for subnet_id in db_subnet_group["Properties"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    assert "{{resolve:secretsmanager:" in str(db_instance["Properties"]["MasterUsername"])
    assert "{{resolve:secretsmanager:" in str(db_instance["Properties"]["MasterUserPassword"])
    assert secret_logical_id in str(db_instance["Properties"]["MasterUsername"])
    assert secret_logical_id in str(db_instance["Properties"]["MasterUserPassword"])

    processor = lambdas["processor"]
    processor_code = str(processor["Properties"]["Code"]["ZipFile"])
    processor_log_group_logical_id = log_group_logical_id_for_lambda(template, processor)
    processor_log_group = resources_by_type(template, "AWS::Logs::LogGroup")[processor_log_group_logical_id]
    assert processor["Properties"]["Runtime"] == "python3.12"
    assert processor["Properties"]["Architectures"] == ["arm64"]
    assert processor["Properties"]["MemorySize"] == 512
    assert processor["Properties"]["Timeout"] == 10
    assert processor["Properties"]["VpcConfig"]["SecurityGroupIds"] == [
        sg_refs["Backend service security group"]
    ]
    assert sorted(str(subnet_id) for subnet_id in processor["Properties"]["VpcConfig"]["SubnetIds"]) == sorted(
        str(subnet_id) for subnet_id in private_subnet_refs(template)
    )
    assert "print(" in processor_code or "logging." in processor_code
    assert processor_log_group["Properties"]["RetentionInDays"] == 7
    assert "KmsKeyId" not in processor_log_group["Properties"]


def test_iam_policies_are_scoped_to_stack_resources():
    template = synth_template(aws_region="us-east-1")
    documents = policy_documents(template)
    statements = [
        statement for document in documents for statement in document["Statement"]
    ]
    secret_logical_id, _ = single_resource(template, "AWS::SecretsManager::Secret")
    queue_logical_id, _ = single_resource(template, "AWS::SQS::Queue")
    bucket_logical_id, _ = single_resource(template, "AWS::S3::Bucket")
    _, glue_database = single_resource(template, "AWS::Glue::Database")
    _, crawler = single_resource(template, "AWS::Glue::Crawler")
    glue_database_name = glue_database["Properties"]["DatabaseInput"]["Name"]
    crawler_name = crawler["Properties"]["Name"]
    lambdas = lambda_resources_by_shape(template)
    backend_role_logical_id = lambdas["backend"]["Properties"]["Role"]["Fn::GetAtt"][0]
    processor_role_logical_id = lambdas["processor"]["Properties"]["Role"]["Fn::GetAtt"][0]
    backend_policy = attached_policy_for_role(template, backend_role_logical_id)
    processor_policy = attached_policy_for_role(template, processor_role_logical_id)
    backend_logs_statement = log_statement_from_policy(backend_policy)
    processor_logs_statement = log_statement_from_policy(processor_policy)
    backend_ec2_statement = statement_by_actions(
        backend_policy,
        [
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
        ],
    )
    backend_log_group_logical_id = log_group_logical_id_for_lambda(template, lambdas["backend"])
    processor_log_group_logical_id = log_group_logical_id_for_lambda(template, lambdas["processor"])

    assert any(
        statement["Action"] == "secretsmanager:GetSecretValue"
        and statement["Resource"] == {"Ref": secret_logical_id}
        for statement in statements
    )
    assert any(
        statement["Action"] == "events:PutEvents"
        and statement["Resource"] == {
            "Fn::Join": [
                "",
                [
                    "arn:",
                    {"Ref": "AWS::Partition"},
                    ":events:",
                    {"Ref": "AWS::Region"},
                    ":",
                    {"Ref": "AWS::AccountId"},
                    ":event-bus/default",
                ],
            ]
        }
        for statement in statements
    )
    assert not any("kms:Decrypt" in str(statement["Action"]) for statement in backend_policy["Statement"])
    assert not any("kms:GenerateDataKey" in str(statement["Action"]) for statement in backend_policy["Statement"])
    assert not any("rds-db:connect" in str(statement["Action"]) for statement in statements)
    assert backend_ec2_statement["Resource"] == "*"
    assert any(
        statement["Action"] == [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
        ]
        and statement["Resource"] == {"Fn::GetAtt": [queue_logical_id, "Arn"]}
        for statement in statements
    )
    assert backend_logs_statement["Resource"] == [
        {"Fn::GetAtt": [backend_log_group_logical_id, "Arn"]},
        {
            "Fn::Join": [
                "",
                [{"Fn::GetAtt": [backend_log_group_logical_id, "Arn"]}, ":*"],
            ]
        },
    ]
    assert processor_logs_statement["Resource"] == [
        {"Fn::GetAtt": [processor_log_group_logical_id, "Arn"]},
        {
            "Fn::Join": [
                "",
                [{"Fn::GetAtt": [processor_log_group_logical_id, "Arn"]}, ":*"],
            ]
        },
    ]
    assert any(
        statement["Action"] == ["s3:GetObject", "s3:ListBucket"]
        and statement["Resource"] == [
            {"Fn::GetAtt": [bucket_logical_id, "Arn"]},
            {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": [bucket_logical_id, "Arn"]}, "/*"],
                ]
            },
        ]
        for statement in statements
    )
    glue_statement = next(
        statement
        for statement in statements
        if all(action.startswith("glue:") for action in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]))
    )
    glue_actions = glue_statement["Action"]
    if isinstance(glue_actions, str):
        glue_actions = [glue_actions]
    assert "glue:*" not in glue_actions
    assert all(action.startswith("glue:") for action in glue_actions)
    assert any(
        action in glue_actions
        for action in [
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:DeleteTable",
            "glue:BatchCreatePartition",
            "glue:BatchDeletePartition",
            "glue:BatchUpdatePartition",
        ]
    )
    glue_resources = glue_statement["Resource"]
    assert "*" not in glue_resources
    assert all(str(resource) != "*" for resource in glue_resources)
    assert any(glue_database_name in str(resource) for resource in glue_resources)
    assert any(crawler_name in str(resource) for resource in glue_resources)
