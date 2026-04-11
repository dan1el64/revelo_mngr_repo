"""Template-level unit tests for the enterprise portal CDK stack."""

import pathlib
import re
import sys

import aws_cdk as cdk
from aws_cdk import assertions

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app import BACKEND_CODE, ENRICHMENT_CODE, INIT_CODE, InlineLambdaBundling, ThreeTierStack


CACHING_DISABLED_POLICY_ID = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"


def synth_stack():
    app = cdk.App()
    return ThreeTierStack(
        app,
        "UnitTestStack",
        env=cdk.Environment(account="000000000000", region="us-east-1"),
    )


def synth_template():
    return assertions.Template.from_stack(synth_stack()).to_json()


def resources_of(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def private_subnet_refs(template):
    subnets = resources_of(template, "AWS::EC2::Subnet")
    return [
        {"Ref": logical_id}
        for logical_id, subnet in subnets.items()
        if subnet["Properties"].get("MapPublicIpOnLaunch") is not True
    ]


def test_vpc_subnets_nat_and_endpoints_are_exact():
    template = synth_template()

    vpcs = resources_of(template, "AWS::EC2::VPC")
    subnets = resources_of(template, "AWS::EC2::Subnet")
    nat_gateways = resources_of(template, "AWS::EC2::NatGateway")
    endpoints = resources_of(template, "AWS::EC2::VPCEndpoint")

    assert len(vpcs) == 1
    assert next(iter(vpcs.values()))["Properties"]["CidrBlock"] == "10.20.0.0/16"
    assert len(subnets) == 4

    public_subnets = [
        subnet for subnet in subnets.values()
        if subnet["Properties"].get("MapPublicIpOnLaunch") is True
    ]
    private_subnets = [
        subnet for subnet in subnets.values()
        if subnet["Properties"].get("MapPublicIpOnLaunch") is not True
    ]
    assert len(public_subnets) == 2
    assert len(private_subnets) == 2
    assert {s["Properties"]["AvailabilityZone"]["Fn::Select"][0] for s in subnets.values()} == {0, 1}
    assert {s["Properties"]["AvailabilityZone"]["Fn::Select"][0] for s in public_subnets} == {0, 1}
    assert {s["Properties"]["AvailabilityZone"]["Fn::Select"][0] for s in private_subnets} == {0, 1}

    assert len(nat_gateways) == 1
    nat_gateway_ref = {"Ref": next(iter(nat_gateways))}
    routes = resources_of(template, "AWS::EC2::Route")
    nat_routes = [
        route for route in routes.values()
        if route["Properties"].get("DestinationCidrBlock") == "0.0.0.0/0"
        and route["Properties"].get("NatGatewayId") == nat_gateway_ref
    ]
    assert len(nat_routes) == 2

    private_refs = private_subnet_refs(template)
    route_associations = resources_of(template, "AWS::EC2::SubnetRouteTableAssociation")
    private_route_table_refs = [
        assoc["Properties"]["RouteTableId"]
        for assoc in route_associations.values()
        if assoc["Properties"]["SubnetId"] in private_refs
    ]

    interface_endpoints = [
        endpoint for endpoint in endpoints.values()
        if endpoint["Properties"]["VpcEndpointType"] == "Interface"
    ]
    gateway_endpoints = [
        endpoint for endpoint in endpoints.values()
        if endpoint["Properties"]["VpcEndpointType"] == "Gateway"
    ]
    assert len(interface_endpoints) == 2
    assert len(gateway_endpoints) == 1
    assert sorted(e["Properties"]["ServiceName"]["Fn::Sub"] for e in interface_endpoints) == [
        "com.amazonaws.${AWS::Region}.logs",
        "com.amazonaws.${AWS::Region}.secretsmanager",
    ]
    assert all(endpoint["Properties"]["SubnetIds"] == private_refs for endpoint in interface_endpoints)
    assert gateway_endpoints[0]["Properties"]["ServiceName"]["Fn::Sub"] == "com.amazonaws.${AWS::Region}.s3"
    assert gateway_endpoints[0]["Properties"]["RouteTableIds"] == private_route_table_refs


def test_frontend_bucket_cloudfront_oac_and_policy():
    template = synth_template()

    buckets = resources_of(template, "AWS::S3::Bucket")
    bucket_policies = resources_of(template, "AWS::S3::BucketPolicy")
    oacs = resources_of(template, "AWS::CloudFront::OriginAccessControl")
    distributions = resources_of(template, "AWS::CloudFront::Distribution")

    assert len(oacs) == 1
    assert len(distributions) == 1

    distribution = next(iter(distributions.values()))["Properties"]["DistributionConfig"]
    s3_origin = next(origin for origin in distribution["Origins"] if "S3OriginConfig" in origin)
    frontend_bucket_id = s3_origin["DomainName"]["Fn::GetAtt"][0]
    frontend_bucket = buckets[frontend_bucket_id]["Properties"]

    assert frontend_bucket["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert frontend_bucket["VersioningConfiguration"]["Status"] == "Enabled"
    assert frontend_bucket["BucketEncryption"]["ServerSideEncryptionConfiguration"][0][
        "ServerSideEncryptionByDefault"
    ]["SSEAlgorithm"] == "AES256"

    assert distribution["DefaultRootObject"] == "index.html"
    assert distribution["DefaultCacheBehavior"]["ViewerProtocolPolicy"] == "redirect-to-https"
    assert distribution["DefaultCacheBehavior"]["AllowedMethods"] == ["GET", "HEAD"]
    assert s3_origin["OriginAccessControlId"] == {"Ref": next(iter(oacs))}

    api_behaviors = [
        behavior for behavior in distribution["CacheBehaviors"]
        if behavior["PathPattern"] == "/api/*"
    ]
    assert len(api_behaviors) == 1
    assert api_behaviors[0]["AllowedMethods"] == ["GET", "HEAD", "OPTIONS", "POST"]
    assert api_behaviors[0]["CachePolicyId"] == CACHING_DISABLED_POLICY_ID

    origins = {origin["Id"]: origin for origin in distribution["Origins"]}
    backend_origin = origins[api_behaviors[0]["TargetOriginId"]]
    assert backend_origin["DomainName"]["Fn::Join"][1] == [
        {"Ref": "BackendRestApi"},
        ".execute-api.",
        {"Ref": "AWS::Region"},
        ".amazonaws.com",
    ]

    frontend_policies = [
        policy for policy in bucket_policies.values()
        if policy["Properties"]["Bucket"] == {"Ref": frontend_bucket_id}
    ]
    assert len(frontend_policies) == 1
    statements = frontend_policies[0]["Properties"]["PolicyDocument"]["Statement"]
    cloudfront_read = [s for s in statements if s["Action"] == "s3:GetObject"]
    assert len(cloudfront_read) == 1
    assert cloudfront_read[0]["Principal"] == {"Service": "cloudfront.amazonaws.com"}
    assert "AWS:SourceArn" in cloudfront_read[0]["Condition"]["StringEquals"]
    assert all(statement["Principal"] != "*" for statement in statements)


def test_backend_lambda_api_gateway_and_logs():
    template = synth_template()

    functions = resources_of(template, "AWS::Lambda::Function")
    props = functions["BackendApiLambda"]["Properties"]

    assert props["Runtime"] == "python3.12"
    assert props["Architectures"] == ["arm64"]
    assert props["MemorySize"] == 512
    assert props["Timeout"] == 10
    assert props["ReservedConcurrentExecutions"] == 5
    assert props["LoggingConfig"]["LogGroup"] == {"Ref": "BackendLambdaLogGroup"}
    assert props["Environment"]["Variables"]["AWS_REGION"] == "us-east-1"
    assert "AWS_ENDPOINT" in props["Environment"]["Variables"]
    assert "DB_ENDPOINT_ADDRESS" in props["Environment"]["Variables"]
    assert "DB_SECRET_ARN" in props["Environment"]["Variables"]

    assert 'path.endswith("/health")' in BACKEND_CODE
    assert '"status": "ok"' in BACKEND_CODE
    assert 'path.endswith("/items")' in BACKEND_CODE
    assert "INSERT INTO items (name) VALUES (:name) RETURNING id" in BACKEND_CODE
    assert '"id"' in BACKEND_CODE
    assert '"name"' in BACKEND_CODE
    assert "get_secret_value" in BACKEND_CODE
    assert 'SecretId=os.environ["DB_SECRET_ARN"]' in BACKEND_CODE
    assert 'os.environ.get("AWS_ENDPOINT")' in BACKEND_CODE
    assert 'kwargs["endpoint_url"] = endpoint' in BACKEND_CODE
    assert "pg8000.native.Connection" in BACKEND_CODE

    rest_apis = resources_of(template, "AWS::ApiGateway::RestApi")
    stages = resources_of(template, "AWS::ApiGateway::Stage")
    methods = resources_of(template, "AWS::ApiGateway::Method")
    resources = resources_of(template, "AWS::ApiGateway::Resource")
    assert len(rest_apis) == 1
    assert next(iter(rest_apis.values()))["Properties"]["EndpointConfiguration"]["Types"] == ["REGIONAL"]
    assert len(stages) == 1
    stage_props = next(iter(stages.values()))["Properties"]
    assert stage_props["StageName"] == "prod"
    assert stage_props["AccessLogSetting"]["DestinationArn"] == {"Fn::GetAtt": ["ApiGatewayAccessLogGroup", "Arn"]}
    assert stage_props["MethodSettings"][0]["MetricsEnabled"] is True
    assert {r["Properties"]["PathPart"] for r in resources.values()} == {"health", "items"}

    resources_by_ref = {
        logical_id: resource["Properties"]["PathPart"]
        for logical_id, resource in resources.items()
    }
    method_by_path = {
        resources_by_ref[method["Properties"]["ResourceId"]["Ref"]]: method["Properties"]
        for method in methods.values()
    }
    assert method_by_path["health"]["HttpMethod"] == "GET"
    assert method_by_path["items"]["HttpMethod"] == "POST"
    assert all(method["Integration"]["Type"] == "AWS_PROXY" for method in method_by_path.values())
    assert all(method["Integration"]["IntegrationHttpMethod"] == "POST" for method in method_by_path.values())

    log_groups = resources_of(template, "AWS::Logs::LogGroup")
    lambda_log_group_refs = [
        function["Properties"]["LoggingConfig"]["LogGroup"]
        for function in functions.values()
        if "LoggingConfig" in function["Properties"]
    ]
    assert sorted(ref["Ref"] for ref in lambda_log_group_refs) == [
        "BackendLambdaLogGroup",
        "EnrichmentLambdaLogGroup",
    ]
    assert log_groups["BackendLambdaLogGroup"]["Properties"]["RetentionInDays"] == 14
    assert log_groups["EnrichmentLambdaLogGroup"]["Properties"]["RetentionInDays"] == 14
    assert log_groups["ApiGatewayAccessLogGroup"]["Properties"]["RetentionInDays"] == 14
    assert "KmsKeyId" not in log_groups["BackendLambdaLogGroup"]["Properties"]
    assert "KmsKeyId" not in log_groups["EnrichmentLambdaLogGroup"]["Properties"]
    assert "KmsKeyId" not in log_groups["ApiGatewayAccessLogGroup"]["Properties"]


def test_database_secret_and_initializer():
    template = synth_template()

    secrets = resources_of(template, "AWS::SecretsManager::Secret")
    dbs = resources_of(template, "AWS::RDS::DBInstance")
    custom_resources = resources_of(template, "AWS::CloudFormation::CustomResource")
    functions = resources_of(template, "AWS::Lambda::Function")
    subnets = resources_of(template, "AWS::EC2::Subnet")

    assert len(secrets) == 1
    secret = next(iter(secrets.values()))["Properties"]["GenerateSecretString"]
    assert secret["GenerateStringKey"] == "password"
    assert secret["SecretStringTemplate"] == '{"username": "appuser"}'

    assert len(dbs) == 1
    db = next(iter(dbs.values()))["Properties"]
    assert db["Engine"] == "postgres"
    assert db["EngineVersion"] == "16"
    assert db["DBInstanceClass"] == "db.t3.micro"
    assert db["AllocatedStorage"] == "20"
    assert db["StorageType"] == "gp2"
    assert db["MultiAZ"] is False
    assert db["PubliclyAccessible"] is False
    assert db["DeletionProtection"] is False
    assert db["StorageEncrypted"] is True
    assert db["DBSubnetGroupName"] == {"Ref": "DatabaseSubnetGroup"}
    assert db["VPCSecurityGroups"] == [{"Fn::GetAtt": ["DatabaseSecurityGroup", "GroupId"]}]
    assert db["MasterUsername"] == "appuser"
    assert "plaintext" not in str(db["MasterUserPassword"]).lower()
    source = pathlib.Path(__file__).resolve().parents[1].joinpath("app.py").read_text()
    hardcoded_passwords = re.findall(
        r"""password["']?\s*[:=]\s*["'][^"']+["']""",
        source,
        flags=re.IGNORECASE,
    )
    assert hardcoded_passwords == []
    for forbidden_secret_coercion in [
        "secret_value_from_json",
        ".to_string(",
        "unsafe_unwrap",
        "SecretValue.unsafe_plain_text",
    ]:
        assert forbidden_secret_coercion not in source

    subnet_groups = resources_of(template, "AWS::RDS::DBSubnetGroup")
    assert all(
        subnets[subnet["Ref"]]["Properties"].get("MapPublicIpOnLaunch") is not True
        for subnet in next(iter(subnet_groups.values()))["Properties"]["SubnetIds"]
    )

    assert len(custom_resources) == 1
    init_lambda = functions["DatabaseInitLambda"]["Properties"]
    assert init_lambda["VpcConfig"]["SubnetIds"] == private_subnet_refs(template)
    assert init_lambda["VpcConfig"]["SecurityGroupIds"] == [
        {"Fn::GetAtt": ["BackendLambdaSecurityGroup", "GroupId"]}
    ]
    assert init_lambda["Environment"]["Variables"]["DB_SECRET_ARN"] == {"Ref": "DatabaseCredentialsSecret"}
    assert init_lambda["Environment"]["Variables"]["DB_ENDPOINT_ADDRESS"] == {
        "Fn::GetAtt": ["DatabaseInstance", "Endpoint.Address"]
    }
    assert "CREATE TABLE IF NOT EXISTS items" in INIT_CODE
    assert "BIGSERIAL PRIMARY KEY" in INIT_CODE
    assert "TIMESTAMPTZ NOT NULL DEFAULT now()" in INIT_CODE
    assert "get_secret_value" in INIT_CODE
    assert "pg8000.native.Connection" in INIT_CODE
    assert 'os.environ.get("AWS_ENDPOINT")' in INIT_CODE
    assert '"SUCCESS", {"skipped": str(exc)}' in INIT_CODE


def test_security_groups_are_least_privilege_for_database_path():
    template = synth_template()

    security_groups = resources_of(template, "AWS::EC2::SecurityGroup")
    egress_rules = resources_of(template, "AWS::EC2::SecurityGroupEgress")
    ingress_rules = resources_of(template, "AWS::EC2::SecurityGroupIngress")

    assert set(security_groups) == {
        "AlbSecurityGroup",
        "BackendLambdaSecurityGroup",
        "DatabaseSecurityGroup",
    }
    assert security_groups["AlbSecurityGroup"]["Properties"]["SecurityGroupIngress"][0]["FromPort"] == 80
    assert security_groups["AlbSecurityGroup"]["Properties"]["SecurityGroupEgress"] == [
        {"CidrIp": "0.0.0.0/0", "IpProtocol": "-1"}
    ]
    assert security_groups["BackendLambdaSecurityGroup"]["Properties"]["SecurityGroupEgress"] == []
    assert "SecurityGroupIngress" not in security_groups["BackendLambdaSecurityGroup"]["Properties"]
    assert security_groups["DatabaseSecurityGroup"]["Properties"]["SecurityGroupEgress"] == [
        {"CidrIp": "0.0.0.0/0", "IpProtocol": "-1"}
    ]
    assert len(egress_rules) == 2
    assert any(
        rule["Properties"]["ToPort"] == 5432 and "DestinationSecurityGroupId" in rule["Properties"]
        for rule in egress_rules.values()
    )
    assert any(
        rule["Properties"]["ToPort"] == 443 and rule["Properties"]["CidrIp"] == "0.0.0.0/0"
        for rule in egress_rules.values()
    )
    assert len(ingress_rules) == 1
    db_ingress = next(iter(ingress_rules.values()))["Properties"]
    assert db_ingress["FromPort"] == 5432
    assert db_ingress["SourceSecurityGroupId"] == {"Fn::GetAtt": ["BackendLambdaSecurityGroup", "GroupId"]}


def test_region_and_endpoint_defaults_and_overrides_are_reflected_in_lambda_environment(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT", raising=False)
    default_functions = resources_of(synth_template(), "AWS::Lambda::Function")
    assert default_functions["BackendApiLambda"]["Properties"]["Environment"]["Variables"]["AWS_REGION"] == "us-east-1"
    assert default_functions["BackendApiLambda"]["Properties"]["Environment"]["Variables"]["AWS_ENDPOINT"] == ""

    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ENDPOINT", "https://endpoint.example.test")
    functions = resources_of(synth_template(), "AWS::Lambda::Function")
    for function_id in ["BackendApiLambda", "EnrichmentLambda", "DatabaseInitLambda"]:
        variables = functions[function_id]["Properties"]["Environment"]["Variables"]
        assert variables["AWS_REGION"] == "eu-west-1"
        assert variables["AWS_ENDPOINT"] == "https://endpoint.example.test"


def test_aws_credentials_inputs_are_supported_without_lambda_leakage(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")

    functions = resources_of(synth_template(), "AWS::Lambda::Function")
    for function in functions.values():
        variables = function["Properties"].get("Environment", {}).get("Variables", {})
        assert "AWS_ACCESS_KEY_ID" not in variables
        assert "AWS_SECRET_ACCESS_KEY" not in variables


def test_template_has_no_resource_retention_policies():
    template = synth_template()
    for resource in template["Resources"].values():
        assert resource.get("DeletionPolicy") is None
        assert resource.get("UpdateReplacePolicy") is None
    for resource_type in [
        "AWS::Logs::LogGroup",
        "AWS::S3::Bucket",
        "AWS::RDS::DBInstance",
    ]:
        for resource in resources_of(template, resource_type).values():
            assert resource.get("DeletionPolicy") is None
            assert resource.get("UpdateReplacePolicy") is None


def test_stack_termination_protection_is_disabled():
    assert synth_stack().termination_protection is False


def test_lambda_deployments_are_zip_assets():
    template = synth_template()
    functions = resources_of(template, "AWS::Lambda::Function")
    assert set(functions) == {"BackendApiLambda", "EnrichmentLambda", "DatabaseInitLambda"}
    for function in functions.values():
        properties = function["Properties"]
        assert properties.get("PackageType", "Zip") == "Zip"
        assert "ImageUri" not in properties["Code"]
        assert "S3Bucket" in properties["Code"]
        assert "S3Key" in properties["Code"]


def test_lambda_asset_bundling_includes_postgres_dependency(tmp_path):
    packages = ("pg8000", "scramp", "asn1crypto")
    for source in [BACKEND_CODE, INIT_CODE]:
        output = tmp_path / hashlib_source_name(source)
        assert InlineLambdaBundling(source, packages).try_bundle(str(output))
        assert (output / "index.py").exists()
        assert (output / "pg8000").exists()
        for package_name in packages:
            assert any(
                path.name.startswith(package_name) and path.name.endswith(".dist-info")
                for path in output.iterdir()
            )


def test_enrichment_lambda_asset_bundling_produces_zip_source(tmp_path):
    output = tmp_path / "enrichment"
    assert InlineLambdaBundling(ENRICHMENT_CODE).try_bundle(str(output))
    assert (output / "index.py").exists()


def hashlib_source_name(source):
    return str(abs(hash(source)))


def test_resource_names_are_deterministic():
    assert synth_template() == synth_template()
    template = synth_template()
    name_like_keys = {
        "BucketName",
        "FunctionName",
        "QueueName",
        "StateMachineName",
        "TrailName",
        "ConfigRuleName",
        "DBInstanceIdentifier",
        "Name",
    }
    hash_like_suffix = re.compile(r"[-_][0-9a-f]{8,}$", re.IGNORECASE)
    timestamp_like_suffix = re.compile(r"[-_](20\d{6,12}|\d{10,})$")
    region_suffix = re.compile(r"-(us|eu|ap|sa|ca|me|af|il)-[a-z]+-\d$")
    for resource in template["Resources"].values():
        for key, value in resource.get("Properties", {}).items():
            if key in name_like_keys and isinstance(value, str):
                assert not hash_like_suffix.search(value)
                assert not timestamp_like_suffix.search(value)
                assert not region_suffix.search(value)


def test_single_file_infrastructure_constraint():
    root = pathlib.Path(__file__).resolve().parents[1]
    infrastructure_files = [
        path for path in root.glob("*.py")
        if path.name != "app.py" and "aws_cdk" in path.read_text()
    ]
    assert infrastructure_files == []
