import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_ENV_NAME = "_".join(["AWS", "ENDPOINT"])


def load_template() -> dict:
    explicit_template = ROOT / "template.json"
    if explicit_template.exists():
        return json.loads(explicit_template.read_text())

    synthesized_templates = sorted((ROOT / "cdk.out").glob("*.template.json"))
    if synthesized_templates:
        return json.loads(synthesized_templates[0].read_text())

    raise FileNotFoundError("template.json was not found. Run `cdk synth` first.")


def resources_by_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def source_text() -> str:
    return (ROOT / "app.ts").read_text()


def source_without_inline_templates() -> str:
    source = source_text()
    source = re.sub(r"return `.*?`\.trim\(\);", "", source, flags=re.DOTALL)
    return source


def inline_template_process_env_vars(source: str) -> set[str]:
    matches = re.findall(r"return `(.*?)`\.trim\(\);", source, flags=re.DOTALL)
    combined = "\n".join(matches)
    return set(re.findall(r"process\.env\.(\w+)", combined))


def resource_by_logical_id_fragment(template: dict, resource_type: str, fragment: str) -> dict:
    for logical_id, resource in resources_by_type(template, resource_type).items():
        if fragment in logical_id:
            return resource
    raise AssertionError(f"{resource_type} with fragment {fragment} was not found")


def logical_id_by_fragment(template: dict, resource_type: str, fragment: str) -> str:
    for logical_id in resources_by_type(template, resource_type).keys():
        if fragment in logical_id:
            return logical_id
    raise AssertionError(f"{resource_type} with fragment {fragment} was not found")


def ref_to(template: dict, resource_type: str, fragment: str) -> dict:
    return {"Ref": logical_id_by_fragment(template, resource_type, fragment)}


def getatt_of(template: dict, resource_type: str, fragment: str, attribute: str) -> dict:
    return {"Fn::GetAtt": [logical_id_by_fragment(template, resource_type, fragment), attribute]}


def policy_statements(template: dict) -> list[dict]:
    statements: list[dict] = []
    for resource in resources_by_type(template, "AWS::IAM::Policy").values():
        statements.extend(resource["Properties"]["PolicyDocument"]["Statement"])
    return statements


def policy_by_logical_id_fragment(template: dict, fragment: str) -> dict:
    for logical_id, resource in resources_by_type(template, "AWS::IAM::Policy").items():
        if fragment in logical_id:
            return resource
    raise AssertionError(f"IAM policy containing {fragment} was not found")


def actions_for_statement(statement: dict) -> list[str]:
    actions = statement["Action"]
    return actions if isinstance(actions, list) else [actions]


def assert_no_plaintext_passwords(value, path: str = "template") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if "password" in key.lower() and isinstance(child, str):
                assert "{{resolve:secretsmanager:" in child.lower(), child_path
            assert_no_plaintext_passwords(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            assert_no_plaintext_passwords(child, f"{path}[{index}]")


def test_source_file_structure_and_input_contract():
    root_ts_files = sorted(path.name for path in ROOT.glob("*.ts"))
    assert root_ts_files == ["app.ts"]

    cdk_config = json.loads((ROOT / "cdk.json").read_text())
    assert cdk_config["app"] == "npx ts-node app.ts"

    source = source_text()
    assert "AWS_REGION" in source
    assert "ENDPOINT" in source
    assert "AWS_ACCESS_KEY_ID" in source
    assert "AWS_SECRET_ACCESS_KEY" in source
    assert "us-east-1" in source
    assert "HostedZone.fromLookup" not in source
    assert "HostedZone.fromHostedZoneAttributes" not in source
    assert "fromHostedZoneId" not in source

    explicit_client_blocks = re.findall(r"new\s+\w+Client\s*\(\s*\{.*?\}\s*\)", source, flags=re.DOTALL)
    assert explicit_client_blocks
    assert all("endpoint:" in block for block in explicit_client_blocks)
    assert all("region:" in block for block in explicit_client_blocks)
    assert "const endpointEnvName = ['AWS', 'ENDPOINT'].join('_');" in source
    assert "endpoint: process.env[endpointEnvName] || undefined," in source

    non_inline_source = source_without_inline_templates()
    consumed_process_env = set(re.findall(r"process\.env\.(\w+)", non_inline_source))
    assert consumed_process_env <= {
        "AWS_REGION",
        "AWS_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    }
    inline_consumed_process_env = inline_template_process_env_vars(source)
    assert inline_consumed_process_env <= {
        "AWS_REGION",
        "ORDER_QUEUE_URL",
        "DB_SECRET_ARN",
        "DB_HOST",
        "DB_PORT",
    }
    assert "process.env.ORDER_QUEUE_URL" in source
    assert "process.env.DB_SECRET_ARN" in source
    assert "process.env.DB_HOST" in source
    assert "process.env.DB_PORT" in source
    assert "terminationProtection: true" not in source
    assert "RemovalPolicy.RETAIN" not in source
    assert "RemovalPolicy.SNAPSHOT" not in source
    assert "unsafeUnwrap(" not in source
    assert "unsafePlainText(" not in source

    assert "if (method === 'GET' && path === '/health')" in source
    assert "return jsonResponse(200, { status: 'ok' })" in source
    assert "if (method === 'GET' && path === '/')" in source
    assert "return jsonResponse(200, { region: dependencies.region })" in source
    assert "if (method === 'POST' && path === '/orders')" in source
    assert "sqsClient.send(new SendMessageCommand({" in source
    assert "QueueUrl: process.env.ORDER_QUEUE_URL" in source
    assert "MessageBody: event.body ?? ''" in source
    assert "return jsonResponse(202, { status: 'accepted' })" in source


def test_template_has_expected_topology_counts():
    template = load_template()
    assert set(template.get("Parameters", {}).keys()) <= {"BootstrapVersion"}
    serialized_template = json.dumps(template)
    assert "Fn::ImportValue" not in serialized_template
    assert "{{resolve:ssm:" not in serialized_template.lower()

    assert len(resources_by_type(template, "AWS::EC2::VPC")) == 1
    assert len(resources_by_type(template, "AWS::EC2::Subnet")) == 4
    assert len(resources_by_type(template, "AWS::EC2::NatGateway")) == 1
    assert len(resources_by_type(template, "AWS::EC2::SecurityGroup")) == 2
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")) == 1
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::Listener")) == 1
    assert len(resources_by_type(template, "AWS::ElasticLoadBalancingV2::TargetGroup")) == 1
    assert len(resources_by_type(template, "AWS::Lambda::Function")) == 2
    assert len(resources_by_type(template, "AWS::S3::Bucket")) == 3
    assert len(resources_by_type(template, "AWS::CloudFront::Distribution")) == 1
    assert len(resources_by_type(template, "AWS::Route53::HostedZone")) == 1
    assert len(resources_by_type(template, "AWS::Route53::RecordSet")) == 1
    assert len(resources_by_type(template, "AWS::SQS::Queue")) == 1
    assert len(resources_by_type(template, "AWS::StepFunctions::StateMachine")) == 1
    assert len(resources_by_type(template, "AWS::Pipes::Pipe")) == 1
    assert len(resources_by_type(template, "AWS::RDS::DBInstance")) == 1
    assert len(resources_by_type(template, "AWS::SecretsManager::Secret")) == 1
    assert len(resources_by_type(template, "AWS::Glue::Database")) == 1
    assert len(resources_by_type(template, "AWS::Glue::Crawler")) == 1
    assert len(resources_by_type(template, "AWS::Athena::WorkGroup")) == 1
    assert len(resources_by_type(template, "AWS::CloudFront::OriginAccessControl")) == 1
    assert len(resources_by_type(template, "AWS::RDS::DBSubnetGroup")) == 1
    assert len(resources_by_type(template, "AWS::CertificateManager::Certificate")) == 0
    assert len(resources_by_type(template, "AWS::EC2::VPCPeeringConnection")) == 0
    assert len(resources_by_type(template, "AWS::EC2::VPNGateway")) == 0
    assert len(resources_by_type(template, "AWS::EC2::TransitGateway")) == 0

    typed_forbidden_name_properties = {
        "AWS::S3::Bucket": {"BucketName"},
        "AWS::Lambda::Function": {"FunctionName"},
        "AWS::SQS::Queue": {"QueueName"},
        "AWS::RDS::DBInstance": {"DBInstanceIdentifier"},
        "AWS::ElasticLoadBalancingV2::LoadBalancer": {"LoadBalancerName"},
        "AWS::ElasticLoadBalancingV2::TargetGroup": {"TargetGroupName"},
        "AWS::StepFunctions::StateMachine": {"StateMachineName"},
    }
    for resource in template["Resources"].values():
        props = resource.get("Properties", {})
        forbidden = typed_forbidden_name_properties.get(resource["Type"], set())
        assert forbidden.isdisjoint(props.keys())


def test_template_has_networking_and_security_constraints():
    template = load_template()

    vpc = next(iter(resources_by_type(template, "AWS::EC2::VPC").values()))
    assert vpc["Properties"]["EnableDnsHostnames"] is True
    assert vpc["Properties"]["EnableDnsSupport"] is True

    subnets = list(resources_by_type(template, "AWS::EC2::Subnet").values())
    public_subnets = [subnet for subnet in subnets if subnet["Properties"]["MapPublicIpOnLaunch"] is True]
    private_subnets = [subnet for subnet in subnets if subnet["Properties"]["MapPublicIpOnLaunch"] is False]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2

    availability_zones = {
        json.dumps(subnet["Properties"]["AvailabilityZone"], sort_keys=True)
        for subnet in subnets
    }
    assert len(availability_zones) == 2

    nat_routes = [
        route
        for route in resources_by_type(template, "AWS::EC2::Route").values()
        if route["Properties"].get("DestinationCidrBlock") == "0.0.0.0/0"
        and "NatGatewayId" in route["Properties"]
    ]
    nat_route_subnets = {route["Properties"]["RouteTableId"]["Ref"] for route in nat_routes}
    private_subnet_refs = {
        json.dumps({"Ref": logical_id}, sort_keys=True)
        for logical_id, subnet in resources_by_type(template, "AWS::EC2::Subnet").items()
        if subnet["Properties"]["MapPublicIpOnLaunch"] is False
    }
    private_route_tables = {
        association["Properties"]["RouteTableId"]["Ref"]
        for association in resources_by_type(template, "AWS::EC2::SubnetRouteTableAssociation").values()
        if json.dumps(association["Properties"]["SubnetId"], sort_keys=True) in private_subnet_refs
    }
    assert nat_route_subnets == private_route_tables
    nat_gateway = next(iter(resources_by_type(template, "AWS::EC2::NatGateway").values()))
    public_subnet_refs = [
        {"Ref": logical_id}
        for logical_id, subnet in resources_by_type(template, "AWS::EC2::Subnet").items()
        if subnet["Properties"]["MapPublicIpOnLaunch"] is True
    ]
    assert nat_gateway["Properties"]["SubnetId"] in public_subnet_refs

    ingress_rules = list(resources_by_type(template, "AWS::EC2::SecurityGroupIngress").values())
    backend_sg = resource_by_logical_id_fragment(template, "AWS::EC2::SecurityGroup", "BackendSecurityGroup")
    db_sg = resource_by_logical_id_fragment(template, "AWS::EC2::SecurityGroup", "DatabaseSecurityGroup")
    backend_group_id = getatt_of(template, "AWS::EC2::SecurityGroup", "BackendSecurityGroup", "GroupId")
    db_group_id = getatt_of(template, "AWS::EC2::SecurityGroup", "DatabaseSecurityGroup", "GroupId")

    postgres_rules = [
        rule
        for rule in ingress_rules
        if rule["Properties"].get("GroupId") == db_group_id
    ]
    assert len(postgres_rules) == 1
    postgres_rule = postgres_rules[0]["Properties"]
    assert postgres_rule["FromPort"] == 5432
    assert postgres_rule["ToPort"] == 5432
    assert postgres_rule["IpProtocol"] == "tcp"
    assert postgres_rule["SourceSecurityGroupId"] == backend_group_id
    assert "CidrIp" not in postgres_rule

    backend_ingress_rules = [
        rule
        for rule in ingress_rules
        if rule["Properties"].get("GroupId") == backend_group_id
    ]
    assert backend_ingress_rules == []

    assert backend_sg["Properties"]["VpcId"] == ref_to(template, "AWS::EC2::VPC", "ApplicationVpc")
    assert db_sg["Properties"]["VpcId"] == ref_to(template, "AWS::EC2::VPC", "ApplicationVpc")
    assert backend_sg["Properties"]["SecurityGroupEgress"]


def test_template_has_compute_frontend_and_async_configuration():
    template = load_template()
    backend_group_id = getatt_of(template, "AWS::EC2::SecurityGroup", "BackendSecurityGroup", "GroupId")
    public_subnet_1 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPublicSubnet1Subnet")
    public_subnet_2 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPublicSubnet2Subnet")
    private_subnet_1 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPrivateWithEgressSubnet1Subnet")
    private_subnet_2 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPrivateWithEgressSubnet2Subnet")
    queue_arn = getatt_of(template, "AWS::SQS::Queue", "OrderQueue", "Arn")
    enrichment_arn = getatt_of(template, "AWS::Lambda::Function", "EnrichmentFunction", "Arn")
    state_machine_ref = ref_to(template, "AWS::StepFunctions::StateMachine", "ProcessingStateMachine")
    secret_ref = ref_to(template, "AWS::SecretsManager::Secret", "DatabaseSecret")
    distribution_domain_name = getatt_of(
        template,
        "AWS::CloudFront::Distribution",
        "FrontendDistribution",
        "DomainName",
    )

    target_group = next(iter(resources_by_type(template, "AWS::ElasticLoadBalancingV2::TargetGroup").values()))
    assert target_group["Properties"]["TargetType"] == "lambda"
    assert target_group["Properties"]["HealthCheckEnabled"] is True
    assert target_group["Properties"]["HealthCheckPath"] == "/health"
    assert target_group["Properties"]["Matcher"]["HttpCode"] == "200"

    load_balancer = next(iter(resources_by_type(template, "AWS::ElasticLoadBalancingV2::LoadBalancer").values()))
    assert load_balancer["Properties"]["Scheme"] == "internet-facing"
    assert set(json.dumps(subnet_ref, sort_keys=True) for subnet_ref in load_balancer["Properties"]["Subnets"]) == {
        json.dumps(public_subnet_1, sort_keys=True),
        json.dumps(public_subnet_2, sort_keys=True),
    }

    functions = resources_by_type(template, "AWS::Lambda::Function")
    for resource in functions.values():
        props = resource["Properties"]
        assert props["Runtime"] == "nodejs20.x"
        assert props["MemorySize"] == 512
        assert props["Timeout"] == 10
        assert "VpcConfig" in props
        assert props["VpcConfig"]["SecurityGroupIds"] == [backend_group_id]
        assert props["VpcConfig"]["SubnetIds"] == [private_subnet_1, private_subnet_2]

    log_groups = resources_by_type(template, "AWS::Logs::LogGroup")
    lambda_log_groups = [
        log_group
        for logical_id, log_group in log_groups.items()
        if "BackendLogGroup" in logical_id or "EnrichmentLogGroup" in logical_id
    ]
    assert len(lambda_log_groups) == 2
    for log_group in lambda_log_groups:
        props = log_group["Properties"]
        assert props["RetentionInDays"] == 14
        assert "KmsKeyId" not in props

    listener = next(iter(resources_by_type(template, "AWS::ElasticLoadBalancingV2::Listener").values()))
    target_group_logical_id = next(iter(resources_by_type(template, "AWS::ElasticLoadBalancingV2::TargetGroup").keys()))
    assert listener["Properties"]["Port"] == 80
    assert listener["Properties"]["Protocol"] == "HTTP"
    assert listener["Properties"]["DefaultActions"] == [
        {
            "TargetGroupArn": {"Ref": target_group_logical_id},
            "Type": "forward",
        }
    ]

    backend_function = next(
        resource for logical_id, resource in functions.items() if "BackendFunction" in logical_id
    )
    enrichment_function = next(
        resource for logical_id, resource in functions.items() if "EnrichmentFunction" in logical_id
    )
    backend_variables = backend_function["Properties"]["Environment"]["Variables"]
    assert ENDPOINT_ENV_NAME in backend_variables
    assert backend_variables["ORDER_QUEUE_URL"] == ref_to(template, "AWS::SQS::Queue", "OrderQueue")
    assert backend_variables["DB_SECRET_ARN"] == ref_to(template, "AWS::SecretsManager::Secret", "DatabaseSecret")
    assert backend_variables["DB_HOST"] == getatt_of(template, "AWS::RDS::DBInstance", "ApplicationDatabase", "Endpoint.Address")
    assert backend_variables["DB_PORT"] == "5432"
    enrichment_variables = enrichment_function["Properties"]["Environment"]["Variables"]
    assert ENDPOINT_ENV_NAME in enrichment_variables

    queue = next(iter(resources_by_type(template, "AWS::SQS::Queue").values()))
    assert queue["Properties"]["MessageRetentionPeriod"] == 345600
    assert queue["Properties"]["VisibilityTimeout"] == 30
    assert queue["Properties"]["SqsManagedSseEnabled"] is True

    state_machine = next(iter(resources_by_type(template, "AWS::StepFunctions::StateMachine").values()))
    definition_parts = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
    resolved_definition = "".join(
        "__ENRICHMENT_ARN__"
        if part == enrichment_arn
        else "aws"
        if isinstance(part, dict)
        else part
        for part in definition_parts
        if isinstance(part, str) or isinstance(part, dict)
    )
    definition = json.loads(resolved_definition)
    assert state_machine["Properties"]["StateMachineType"] == "STANDARD"
    assert definition["TimeoutSeconds"] == 30
    assert len(definition["States"]) == 1
    only_state_name = definition["StartAt"]
    only_state = definition["States"][only_state_name]
    assert only_state["Type"] == "Task"
    assert only_state["End"] is True
    assert only_state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert only_state["Parameters"]["FunctionName"] == "__ENRICHMENT_ARN__"
    assert only_state["Parameters"]["Payload.$"] == "$"

    pipe_resources = list(resources_by_type(template, "AWS::Pipes::Pipe").values())
    assert len(pipe_resources) == 1
    pipe = pipe_resources[0]
    assert pipe["Properties"]["Source"] == queue_arn
    assert pipe["Properties"]["Enrichment"] == enrichment_arn
    assert pipe["Properties"]["Target"] == state_machine_ref
    assert pipe["Properties"]["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 1
    assert (
        pipe["Properties"]["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"]
        == "FIRE_AND_FORGET"
    )

    distribution = next(iter(resources_by_type(template, "AWS::CloudFront::Distribution").values()))
    origin_access_control_logical_id = next(iter(resources_by_type(template, "AWS::CloudFront::OriginAccessControl").keys()))
    origin_access_control = next(iter(resources_by_type(template, "AWS::CloudFront::OriginAccessControl").values()))
    origins = distribution["Properties"]["DistributionConfig"]["Origins"]
    default_behavior = distribution["Properties"]["DistributionConfig"]["DefaultCacheBehavior"]
    oac_config = origin_access_control["Properties"]["OriginAccessControlConfig"]
    assert len(origins) == 1
    assert origins[0]["DomainName"] == getatt_of(template, "AWS::S3::Bucket", "FrontendBucket", "RegionalDomainName")
    assert oac_config["OriginAccessControlOriginType"] == "s3"
    assert oac_config["SigningBehavior"] == "always"
    assert oac_config["SigningProtocol"] == "sigv4"
    assert default_behavior["AllowedMethods"] == ["GET", "HEAD"]
    assert default_behavior["ViewerProtocolPolicy"] == "redirect-to-https"
    viewer_certificate = distribution["Properties"]["DistributionConfig"].get("ViewerCertificate", {})
    assert viewer_certificate.get("CloudFrontDefaultCertificate") is True

    frontend_bucket = resource_by_logical_id_fragment(template, "AWS::S3::Bucket", "FrontendBucket")
    assert frontend_bucket["Properties"]["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert frontend_bucket["Properties"]["WebsiteConfiguration"] == {
        "ErrorDocument": "index.html",
        "IndexDocument": "index.html",
    }

    bucket_policy = next(iter(resources_by_type(template, "AWS::S3::BucketPolicy").values()))
    statement = bucket_policy["Properties"]["PolicyDocument"]["Statement"][0]
    assert statement["Principal"] == {"Service": "cloudfront.amazonaws.com"}
    assert statement["Action"] == "s3:GetObject"
    source_arn = statement["Condition"]["StringEquals"]["AWS:SourceArn"]
    assert source_arn == getatt_of(
        template,
        "AWS::CloudFront::Distribution",
        "FrontendDistribution",
        "Arn",
    ) or (
        source_arn["Fn::Join"][1][-1]
        == ref_to(template, "AWS::CloudFront::Distribution", "FrontendDistribution")
    )
    assert "OriginAccessControlId" in origins[0]

    send_message_statements = [
        statement
        for statement in policy_by_logical_id_fragment(
            template,
            "BackendFunctionServiceRoleDefaultPolicy",
        )["Properties"]["PolicyDocument"]["Statement"]
        if "sqs:SendMessage" in actions_for_statement(statement)
    ]
    assert len(send_message_statements) == 1
    assert send_message_statements[0]["Resource"] != "*"
    assert sum(
        1
        for statement in policy_statements(template)
        if "sqs:SendMessage" in actions_for_statement(statement)
    ) == 1

    secret_read_statements = [
        statement
        for statement in policy_by_logical_id_fragment(
            template,
            "BackendFunctionServiceRoleDefaultPolicy",
        )["Properties"]["PolicyDocument"]["Statement"]
        if "secretsmanager:GetSecretValue" in actions_for_statement(statement)
    ]
    assert len(secret_read_statements) == 1
    assert secret_read_statements[0]["Resource"] == secret_ref

    pipe_statements = policy_by_logical_id_fragment(template, "PipeRole")["Properties"]["PolicyDocument"]["Statement"]
    non_wildcard_pipe_statements = [statement for statement in pipe_statements if statement["Resource"] != "*"]
    assert any(statement["Resource"] == queue_arn for statement in non_wildcard_pipe_statements)
    assert any(statement["Resource"] == enrichment_arn for statement in non_wildcard_pipe_statements)
    assert any(statement["Resource"] == state_machine_ref for statement in non_wildcard_pipe_statements)



def test_template_has_data_analytics_and_destroyable_settings():
    template = load_template()
    assert_no_plaintext_passwords(template)
    db_sg_group_id = getatt_of(template, "AWS::EC2::SecurityGroup", "DatabaseSecurityGroup", "GroupId")
    secret_ref = ref_to(template, "AWS::SecretsManager::Secret", "DatabaseSecret")
    private_subnet_1 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPrivateWithEgressSubnet1Subnet")
    private_subnet_2 = ref_to(template, "AWS::EC2::Subnet", "ApplicationVpcPrivateWithEgressSubnet2Subnet")
    analytics_bucket_ref = ref_to(template, "AWS::S3::Bucket", "AnalyticsInputBucket")
    analytics_bucket_arn = getatt_of(template, "AWS::S3::Bucket", "AnalyticsInputBucket", "Arn")
    athena_results_bucket_ref = ref_to(template, "AWS::S3::Bucket", "AthenaResultsBucket")

    db_instance = next(iter(resources_by_type(template, "AWS::RDS::DBInstance").values()))
    db_props = db_instance["Properties"]
    assert db_props["Engine"] == "postgres"
    assert str(db_props["EngineVersion"]).startswith("16")
    assert db_props["DBInstanceClass"] == "db.t3.micro"
    assert db_props["AllocatedStorage"] == "20"
    assert db_props["StorageType"] == "gp3"
    assert db_props["MultiAZ"] is False
    assert db_props["PubliclyAccessible"] is False
    assert db_props["BackupRetentionPeriod"] == 1
    assert db_props["PreferredBackupWindow"] == "03:00-04:00"
    assert db_props["DeletionProtection"] is False
    assert db_props["DBSubnetGroupName"] == ref_to(template, "AWS::RDS::DBSubnetGroup", "DatabaseSubnetGroup")
    assert db_props["VPCSecurityGroups"] == [db_sg_group_id]
    serialized_master_password = json.dumps(db_props["MasterUserPassword"])
    assert "{{resolve:secretsmanager:" in serialized_master_password
    assert any(
        part == secret_ref
        for part in db_props["MasterUserPassword"]["Fn::Join"][1]
        if isinstance(part, dict)
    )
    assert db_props["MasterUsername"] == "appuser"

    db_subnet_group = next(iter(resources_by_type(template, "AWS::RDS::DBSubnetGroup").values()))
    assert db_subnet_group["Properties"]["SubnetIds"] == [private_subnet_1, private_subnet_2]

    secret = next(iter(resources_by_type(template, "AWS::SecretsManager::Secret").values()))
    generated = secret["Properties"]["GenerateSecretString"]
    assert generated["GenerateStringKey"] == "password"
    assert json.loads(generated["SecretStringTemplate"]) == {"username": "appuser"}

    analytics_bucket = resource_by_logical_id_fragment(template, "AWS::S3::Bucket", "AnalyticsInputBucket")
    athena_results_bucket = resource_by_logical_id_fragment(template, "AWS::S3::Bucket", "AthenaResultsBucket")
    expected_block_public_access = {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert analytics_bucket["Properties"]["PublicAccessBlockConfiguration"] == expected_block_public_access
    assert athena_results_bucket["Properties"]["PublicAccessBlockConfiguration"] == expected_block_public_access

    crawler = next(iter(resources_by_type(template, "AWS::Glue::Crawler").values()))
    crawler_role = resource_by_logical_id_fragment(template, "AWS::IAM::Role", "GlueCrawlerRole")
    s3_targets = crawler["Properties"]["Targets"]["S3Targets"]
    assert crawler["Properties"]["Role"] == getatt_of(template, "AWS::IAM::Role", "GlueCrawlerRole", "Arn")
    assert crawler_role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "glue.amazonaws.com"
    }
    assert len(s3_targets) == 1
    assert "Path" in s3_targets[0]
    assert crawler["Properties"]["DatabaseName"] == {"Ref": "AnalyticsDatabase"}
    assert "JdbcTargets" not in crawler["Properties"]["Targets"]
    crawler_path_parts = s3_targets[0]["Path"]["Fn::Join"][1]
    assert crawler_path_parts[0] == "s3://"
    assert crawler_path_parts[1] == analytics_bucket_ref

    workgroup = next(iter(resources_by_type(template, "AWS::Athena::WorkGroup").values()))
    workgroup_config = workgroup["Properties"]["WorkGroupConfiguration"]
    assert workgroup_config["EnforceWorkGroupConfiguration"] is True
    output_location_parts = workgroup_config["ResultConfiguration"]["OutputLocation"]["Fn::Join"][1]
    assert output_location_parts[0] == "s3://"
    assert output_location_parts[1] == athena_results_bucket_ref
    assert output_location_parts[2] == "/results/"

    hosted_zone = next(iter(resources_by_type(template, "AWS::Route53::HostedZone").values()))
    assert "VPCs" not in hosted_zone["Properties"]
    assert hosted_zone["Properties"].get("PrivateZone") is not True

    record_set = next(iter(resources_by_type(template, "AWS::Route53::RecordSet").values()))
    assert record_set["Properties"]["Type"] == "A"
    assert record_set["Properties"]["HostedZoneId"] == ref_to(template, "AWS::Route53::HostedZone", "ApplicationHostedZone")
    assert record_set["Properties"]["AliasTarget"]["DNSName"] == getatt_of(
        template,
        "AWS::CloudFront::Distribution",
        "FrontendDistribution",
        "DomainName",
    )
    hosted_zone_id = record_set["Properties"]["AliasTarget"]["HostedZoneId"]
    assert hosted_zone_id == "Z2FDTNDATAQYW2" or hosted_zone_id == {
        "Fn::FindInMap": [
            "AWSCloudFrontPartitionHostedZoneIdMap",
            {"Ref": "AWS::Partition"},
            "zoneId",
        ]
    }

    queue = next(iter(resources_by_type(template, "AWS::SQS::Queue").values()))
    assert queue["Properties"].get("FifoQueue") is not True
    assert queue["Properties"]["SqsManagedSseEnabled"] is True

    for logical_id, resource in template["Resources"].items():
        assert resource.get("DeletionPolicy") not in {"Retain", "Snapshot"}, logical_id
        assert resource.get("UpdateReplacePolicy") not in {"Retain", "Snapshot"}, logical_id


def test_backend_handler_rejects_unrecognized_routes():
    """Negative-path: the handler must return 404 for routes outside its contract.

    Verifies both the source-code fallthrough and that no IAM policy grants
    wildcard SQS permissions (which would be the wrong default for unknown callers).
    """
    source = source_text()

    # The handler must have an explicit 404 fallthrough for unrecognized routes
    assert "return jsonResponse(404, { error: 'Not found' })" in source

    # DELETE and PATCH are not in the route table — they must fall through to 404
    assert "method === 'DELETE'" not in source
    assert "method === 'PATCH'" not in source

    # No IAM statement grants sqs:SendMessage with a wildcard resource
    template = load_template()
    for statement in policy_statements(template):
        if "sqs:SendMessage" in actions_for_statement(statement):
            assert statement.get("Resource") != "*", (
                "sqs:SendMessage must be scoped to a specific queue ARN, not '*'"
            )
