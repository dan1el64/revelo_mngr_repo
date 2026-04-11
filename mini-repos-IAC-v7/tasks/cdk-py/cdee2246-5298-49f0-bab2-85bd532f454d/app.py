#!/usr/bin/env python3
"""Single-file AWS CDK app for an internal three-tier enterprise portal."""

import json
import os
import hashlib
import importlib.metadata as importlib_metadata
import importlib.util
import pathlib
import shutil

import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct
import jsii

from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_cloudtrail as cloudtrail
from aws_cdk import aws_config as config
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_pipes as pipes
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn


def configured_region() -> str:
    return os.environ.get("AWS_REGION", "us-east-1")


def configured_endpoint() -> str:
    return os.environ.get("AWS_ENDPOINT", "")


BACKEND_CODE = r"""
import json
import os

import boto3
import pg8000.native


def aws_client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


def response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(payload),
    }


def load_db_credentials():
    secret = aws_client("secretsmanager").get_secret_value(
        SecretId=os.environ["DB_SECRET_ARN"]
    )
    return json.loads(secret["SecretString"])


def create_item(name):
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")

    credentials = load_db_credentials()
    conn = pg8000.native.Connection(
        host=os.environ["DB_ENDPOINT_ADDRESS"],
        user=credentials["username"],
        password=credentials["password"],
        database=os.environ.get("DB_NAME", "portal"),
        port=5432,
        timeout=5,
    )
    try:
        result = conn.run(
            "INSERT INTO items (name) VALUES (:name) RETURNING id",
            name=name,
        )
        item_id = result[0][0]
        return {"id": item_id, "name": name}
    finally:
        conn.close()


def handler(event, context):
    if event.get("action") == "create_item":
        name = event.get("name") or event.get("Payload", {}).get("name")
        return create_item(name)

    method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("resource") or event.get("path") or event.get("rawPath") or ""

    if method == "GET" and path.endswith("/health"):
        return response(200, {"status": "ok"})

    if method == "POST" and path.endswith("/items"):
        body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            import base64
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body)
        item = create_item(payload.get("name"))
        return response(201, item)

    return response(404, {"error": "not found"})
"""


ENRICHMENT_CODE = r"""
import json
import os

import boto3


def aws_client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


def parse_payload(event):
    if isinstance(event, list) and event:
        return parse_payload(event[0])
    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if isinstance(body, str):
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {"name": body}
        return body
    if isinstance(event, dict) and "Records" in event and event["Records"]:
        return parse_payload(event["Records"][0])
    return event if isinstance(event, dict) else {}


def handler(event, context):
    aws_client("sts")
    payload = parse_payload(event)
    name = payload.get("name") or payload.get("detail", {}).get("name") or "unnamed"
    return {"name": str(name).strip()}
"""


INIT_CODE = r"""
import json
import os
import urllib.request

import boto3
import pg8000.native


def aws_client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT")
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


def send_response(event, context, status, data=None, physical_id=None):
    body = json.dumps({
        "Status": status,
        "Reason": "See CloudWatch Logs for details",
        "PhysicalResourceId": physical_id or "items-table-initializer",
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": False,
        "Data": data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        event["ResponseURL"],
        data=body,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(body))},
    )
    urllib.request.urlopen(req, timeout=10)


def handler(event, context):
    try:
        if event.get("RequestType") == "Delete":
            send_response(event, context, "SUCCESS")
            return

        try:
            aws_client("rds").describe_db_instances(
                DBInstanceIdentifier=os.environ["DB_INSTANCE_ID"]
            )
        except Exception as exc:
            if os.environ.get("AWS_ENDPOINT"):
                send_response(event, context, "SUCCESS", {"skipped": str(exc)})
                return
            raise
        secret = aws_client("secretsmanager").get_secret_value(
            SecretId=os.environ["DB_SECRET_ARN"]
        )
        credentials = json.loads(secret["SecretString"])
        try:
            conn = pg8000.native.Connection(
                host=os.environ["DB_ENDPOINT_ADDRESS"],
                user=credentials["username"],
                password=credentials["password"],
                database=os.environ.get("DB_NAME", "portal"),
                port=5432,
                timeout=5,
            )
        except Exception as exc:
            if os.environ.get("AWS_ENDPOINT"):
                send_response(event, context, "SUCCESS", {"skipped": str(exc)})
                return
            raise
        try:
            conn.run(
                "CREATE TABLE IF NOT EXISTS items ("
                "id BIGSERIAL PRIMARY KEY,"
                "name TEXT NOT NULL,"
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()"
                ")"
            )
        finally:
            conn.close()
        send_response(event, context, "SUCCESS")
    except Exception as exc:
        send_response(event, context, "FAILED", {"error": str(exc)})
"""


def assume_policy(service: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def inline_policy(name: str, document: dict) -> iam.CfnRole.PolicyProperty:
    return iam.CfnRole.PolicyProperty(policy_name=name, policy_document=document)


def copy_python_package(package_name: str, output_dir: pathlib.Path) -> None:
    spec = importlib.util.find_spec(package_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Python package {package_name} is required for Lambda bundling")

    package_path = pathlib.Path(spec.origin)
    if package_path.name == "__init__.py":
        source = package_path.parent
        destination = output_dir / source.name
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    else:
        shutil.copy2(package_path, output_dir / package_path.name)

    try:
        distribution = importlib_metadata.distribution(package_name)
    except importlib_metadata.PackageNotFoundError:
        return

    copied_metadata_dirs: set[str] = set()
    for distribution_file in distribution.files or []:
        metadata_dir = pathlib.PurePosixPath(str(distribution_file)).parts[0]
        if not (metadata_dir.endswith(".dist-info") or metadata_dir.endswith(".egg-info")):
            continue
        if metadata_dir in copied_metadata_dirs:
            continue
        copied_metadata_dirs.add(metadata_dir)
        metadata_source = pathlib.Path(distribution.locate_file(metadata_dir))
        metadata_destination = output_dir / metadata_dir
        if metadata_source.is_dir():
            if metadata_destination.exists():
                shutil.rmtree(metadata_destination)
            shutil.copytree(metadata_source, metadata_destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))


@jsii.implements(cdk.ILocalBundling)
class InlineLambdaBundling:
    def __init__(self, source_code: str, packages: tuple[str, ...] = ()) -> None:
        self.source_code = source_code
        self.packages = packages

    def try_bundle(self, output_dir: str, *_args, **_kwargs) -> bool:
        asset_output = pathlib.Path(output_dir)
        asset_output.mkdir(parents=True, exist_ok=True)
        (asset_output / "index.py").write_text(self.source_code, encoding="utf-8")
        for package_name in self.packages:
            copy_python_package(package_name, asset_output)
        return True


def bundled_lambda_code(scope: Construct, source_code: str, packages: tuple[str, ...] = ()) -> lambda_.CfnFunction.CodeProperty:
    asset_hash = hashlib.sha256(
        (source_code + "|" + ",".join(packages)).encode("utf-8")
    ).hexdigest()
    asset_code = lambda_.Code.from_asset(
        ".",
        asset_hash=asset_hash,
        asset_hash_type=cdk.AssetHashType.CUSTOM,
        bundling=cdk.BundlingOptions(
            image=lambda_.Runtime.PYTHON_3_12.bundling_image,
            local=InlineLambdaBundling(source_code, packages),
            command=["bash", "-c", "cp -R /asset-input/. /asset-output/"],
        ),
    )
    asset_scope = Construct(scope, f"BundledLambdaAsset{asset_hash[:12]}")
    location = asset_code.bind(asset_scope).s3_location
    if location is None:
        raise RuntimeError("Lambda asset bundling did not produce an S3 location")
    return lambda_.CfnFunction.CodeProperty(
        s3_bucket=location.bucket_name,
        s3_key=location.object_key,
        s3_object_version=location.object_version,
    )


class ThreeTierStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_region_value = configured_region()
        aws_endpoint_value = configured_endpoint()
        account_id = cdk.Aws.ACCOUNT_ID
        region = cdk.Aws.REGION

        vpc = ec2.CfnVPC(
            self,
            "Vpc",
            cidr_block="10.20.0.0/16",
            enable_dns_hostnames=True,
            enable_dns_support=True,
            tags=[cdk.CfnTag(key="Name", value="enterprise-portal-vpc")],
        )

        internet_gateway = ec2.CfnInternetGateway(self, "InternetGateway")
        ec2.CfnVPCGatewayAttachment(
            self,
            "VpcGatewayAttachment",
            vpc_id=vpc.ref,
            internet_gateway_id=internet_gateway.ref,
        )

        public_subnet_a = ec2.CfnSubnet(
            self,
            "PublicSubnetA",
            vpc_id=vpc.ref,
            cidr_block="10.20.0.0/24",
            availability_zone=cdk.Fn.select(0, cdk.Fn.get_azs(region)),
            map_public_ip_on_launch=True,
        )
        public_subnet_b = ec2.CfnSubnet(
            self,
            "PublicSubnetB",
            vpc_id=vpc.ref,
            cidr_block="10.20.1.0/24",
            availability_zone=cdk.Fn.select(1, cdk.Fn.get_azs(region)),
            map_public_ip_on_launch=True,
        )
        private_subnet_a = ec2.CfnSubnet(
            self,
            "PrivateSubnetA",
            vpc_id=vpc.ref,
            cidr_block="10.20.10.0/24",
            availability_zone=cdk.Fn.select(0, cdk.Fn.get_azs(region)),
        )
        private_subnet_b = ec2.CfnSubnet(
            self,
            "PrivateSubnetB",
            vpc_id=vpc.ref,
            cidr_block="10.20.11.0/24",
            availability_zone=cdk.Fn.select(1, cdk.Fn.get_azs(region)),
        )

        public_route_table = ec2.CfnRouteTable(self, "PublicRouteTable", vpc_id=vpc.ref)
        private_route_table_a = ec2.CfnRouteTable(self, "PrivateRouteTableA", vpc_id=vpc.ref)
        private_route_table_b = ec2.CfnRouteTable(self, "PrivateRouteTableB", vpc_id=vpc.ref)

        public_route = ec2.CfnRoute(
            self,
            "PublicDefaultRoute",
            route_table_id=public_route_table.ref,
            destination_cidr_block="0.0.0.0/0",
            gateway_id=internet_gateway.ref,
        )
        public_route.add_dependency(internet_gateway)

        ec2.CfnSubnetRouteTableAssociation(
            self,
            "PublicSubnetARouteTableAssociation",
            subnet_id=public_subnet_a.ref,
            route_table_id=public_route_table.ref,
        )
        ec2.CfnSubnetRouteTableAssociation(
            self,
            "PublicSubnetBRouteTableAssociation",
            subnet_id=public_subnet_b.ref,
            route_table_id=public_route_table.ref,
        )
        ec2.CfnSubnetRouteTableAssociation(
            self,
            "PrivateSubnetARouteTableAssociation",
            subnet_id=private_subnet_a.ref,
            route_table_id=private_route_table_a.ref,
        )
        ec2.CfnSubnetRouteTableAssociation(
            self,
            "PrivateSubnetBRouteTableAssociation",
            subnet_id=private_subnet_b.ref,
            route_table_id=private_route_table_b.ref,
        )

        nat_eip = ec2.CfnEIP(self, "NatGatewayEip", domain="vpc")
        nat_gateway = ec2.CfnNatGateway(
            self,
            "NatGateway",
            subnet_id=public_subnet_a.ref,
            allocation_id=nat_eip.attr_allocation_id,
        )
        nat_gateway.add_dependency(public_route)

        ec2.CfnRoute(
            self,
            "PrivateSubnetADefaultRoute",
            route_table_id=private_route_table_a.ref,
            destination_cidr_block="0.0.0.0/0",
            nat_gateway_id=nat_gateway.ref,
        )
        ec2.CfnRoute(
            self,
            "PrivateSubnetBDefaultRoute",
            route_table_id=private_route_table_b.ref,
            destination_cidr_block="0.0.0.0/0",
            nat_gateway_id=nat_gateway.ref,
        )

        ec2.CfnVPCEndpoint(
            self,
            "SecretsManagerEndpoint",
            vpc_id=vpc.ref,
            service_name=cdk.Fn.sub("com.amazonaws.${AWS::Region}.secretsmanager"),
            vpc_endpoint_type="Interface",
            private_dns_enabled=True,
            subnet_ids=[private_subnet_a.ref, private_subnet_b.ref],
        )
        ec2.CfnVPCEndpoint(
            self,
            "CloudWatchLogsEndpoint",
            vpc_id=vpc.ref,
            service_name=cdk.Fn.sub("com.amazonaws.${AWS::Region}.logs"),
            vpc_endpoint_type="Interface",
            private_dns_enabled=True,
            subnet_ids=[private_subnet_a.ref, private_subnet_b.ref],
        )
        ec2.CfnVPCEndpoint(
            self,
            "S3GatewayEndpoint",
            vpc_id=vpc.ref,
            service_name=cdk.Fn.sub("com.amazonaws.${AWS::Region}.s3"),
            vpc_endpoint_type="Gateway",
            route_table_ids=[private_route_table_a.ref, private_route_table_b.ref],
        )

        alb_sg = ec2.CfnSecurityGroup(
            self,
            "AlbSecurityGroup",
            group_description="ALB security group",
            vpc_id=vpc.ref,
            security_group_ingress=[
                ec2.CfnSecurityGroup.IngressProperty(
                    ip_protocol="tcp", from_port=80, to_port=80, cidr_ip="0.0.0.0/0"
                )
            ],
            security_group_egress=[
                ec2.CfnSecurityGroup.EgressProperty(
                    ip_protocol="-1", cidr_ip="0.0.0.0/0"
                )
            ],
        )
        backend_sg = ec2.CfnSecurityGroup(
            self,
            "BackendLambdaSecurityGroup",
            group_description="Backend Lambda security group",
            vpc_id=vpc.ref,
            security_group_egress=[],
        )
        database_sg = ec2.CfnSecurityGroup(
            self,
            "DatabaseSecurityGroup",
            group_description="Database security group",
            vpc_id=vpc.ref,
            security_group_egress=[
                ec2.CfnSecurityGroup.EgressProperty(
                    ip_protocol="-1", cidr_ip="0.0.0.0/0"
                )
            ],
        )
        ec2.CfnSecurityGroupEgress(
            self,
            "BackendToDatabaseEgress",
            group_id=backend_sg.attr_group_id,
            ip_protocol="tcp",
            from_port=5432,
            to_port=5432,
            destination_security_group_id=database_sg.attr_group_id,
        )
        ec2.CfnSecurityGroupEgress(
            self,
            "BackendHttpsEgress",
            group_id=backend_sg.attr_group_id,
            ip_protocol="tcp",
            from_port=443,
            to_port=443,
            cidr_ip="0.0.0.0/0",
        )
        ec2.CfnSecurityGroupIngress(
            self,
            "DatabaseFromBackendIngress",
            group_id=database_sg.attr_group_id,
            ip_protocol="tcp",
            from_port=5432,
            to_port=5432,
            source_security_group_id=backend_sg.attr_group_id,
        )

        portal_bucket = s3.CfnBucket(
            self,
            "FrontendBucket",
            public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="AES256"
                        )
                    )
                ]
            ),
            versioning_configuration=s3.CfnBucket.VersioningConfigurationProperty(
                status="Enabled"
            ),
        )
        config_bucket = s3.CfnBucket(
            self,
            "ConfigDeliveryBucket",
            public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="AES256"
                        )
                    )
                ]
            ),
        )
        trail_bucket = s3.CfnBucket(
            self,
            "CloudTrailDeliveryBucket",
            public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="AES256"
                        )
                    )
                ]
            ),
        )

        oac = cloudfront.CfnOriginAccessControl(
            self,
            "FrontendOriginAccessControl",
            origin_access_control_config=cloudfront.CfnOriginAccessControl.OriginAccessControlConfigProperty(
                name="enterprise-portal-frontend-oac",
                origin_access_control_origin_type="s3",
                signing_behavior="always",
                signing_protocol="sigv4",
            ),
        )

        backend_log_group = logs.CfnLogGroup(
            self,
            "BackendLambdaLogGroup",
            retention_in_days=14,
        )
        enrichment_log_group = logs.CfnLogGroup(
            self,
            "EnrichmentLambdaLogGroup",
            retention_in_days=14,
        )
        api_log_group = logs.CfnLogGroup(
            self,
            "ApiGatewayAccessLogGroup",
            retention_in_days=14,
        )
        trail_log_group = logs.CfnLogGroup(
            self,
            "CloudTrailLogGroup",
            retention_in_days=30,
        )
        workflow_log_group = logs.CfnLogGroup(
            self,
            "WorkflowLogGroup",
            retention_in_days=14,
        )

        db_secret = secretsmanager.CfnSecret(
            self,
            "DatabaseCredentialsSecret",
            generate_secret_string=secretsmanager.CfnSecret.GenerateSecretStringProperty(
                secret_string_template=json.dumps({"username": "appuser"}),
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=32,
            ),
        )

        subnet_group = rds.CfnDBSubnetGroup(
            self,
            "DatabaseSubnetGroup",
            db_subnet_group_description="Private subnets for enterprise portal database",
            subnet_ids=[private_subnet_a.ref, private_subnet_b.ref],
        )
        database = rds.CfnDBInstance(
            self,
            "DatabaseInstance",
            engine="postgres",
            engine_version="16",
            db_instance_class="db.t3.micro",
            allocated_storage="20",
            storage_type="gp2",
            multi_az=False,
            publicly_accessible=False,
            deletion_protection=False,
            storage_encrypted=True,
            db_name="portal",
            db_subnet_group_name=subnet_group.ref,
            vpc_security_groups=[database_sg.attr_group_id],
            master_username="appuser",
            master_user_password=cdk.Fn.sub(
                "{{resolve:secretsmanager:${SecretArn}:SecretString:password}}",
                {"SecretArn": db_secret.ref},
            ),
        )

        lambda_network_actions = [
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
            "ec2:AssignPrivateIpAddresses",
            "ec2:UnassignPrivateIpAddresses",
        ]
        backend_role = iam.CfnRole(
            self,
            "BackendLambdaRole",
            assume_role_policy_document=assume_policy("lambda.amazonaws.com"),
            policies=[
                inline_policy(
                    "BackendLambdaPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                                "Resource": cdk.Fn.sub(
                                    "${LogArn}:*",
                                    {"LogArn": backend_log_group.attr_arn},
                                ),
                            },
                            {
                                "Effect": "Allow",
                                "Action": "secretsmanager:GetSecretValue",
                                "Resource": db_secret.ref,
                            },
                            {
                                "Effect": "Allow",
                                "Action": lambda_network_actions,
                                "Resource": "*",
                            },
                        ],
                    },
                )
            ],
        )
        enrichment_role = iam.CfnRole(
            self,
            "EnrichmentLambdaRole",
            assume_role_policy_document=assume_policy("lambda.amazonaws.com"),
            policies=[
                inline_policy(
                    "EnrichmentLambdaPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                                "Resource": cdk.Fn.sub(
                                    "${LogArn}:*",
                                    {"LogArn": enrichment_log_group.attr_arn},
                                ),
                            }
                        ],
                    },
                )
            ],
        )

        backend_lambda = lambda_.CfnFunction(
            self,
            "BackendApiLambda",
            runtime="python3.12",
            architectures=["arm64"],
            handler="index.handler",
            code=bundled_lambda_code(self, BACKEND_CODE, ("pg8000", "scramp", "asn1crypto")),
            role=backend_role.attr_arn,
            memory_size=512,
            timeout=10,
            reserved_concurrent_executions=5,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "DB_ENDPOINT_ADDRESS": database.attr_endpoint_address,
                    "DB_SECRET_ARN": db_secret.ref,
                    "DB_NAME": "portal",
                    "AWS_REGION": aws_region_value,
                    "AWS_ENDPOINT": aws_endpoint_value,
                }
            ),
            logging_config=lambda_.CfnFunction.LoggingConfigProperty(
                log_group=backend_log_group.ref
            ),
            vpc_config=lambda_.CfnFunction.VpcConfigProperty(
                subnet_ids=[private_subnet_a.ref, private_subnet_b.ref],
                security_group_ids=[backend_sg.attr_group_id],
            ),
        )
        backend_lambda.add_dependency(backend_log_group)
        backend_lambda.add_dependency(database)

        enrichment_lambda = lambda_.CfnFunction(
            self,
            "EnrichmentLambda",
            runtime="python3.12",
            architectures=["arm64"],
            handler="index.handler",
            code=bundled_lambda_code(self, ENRICHMENT_CODE),
            role=enrichment_role.attr_arn,
            memory_size=256,
            timeout=5,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={"AWS_REGION": aws_region_value, "AWS_ENDPOINT": aws_endpoint_value}
            ),
            logging_config=lambda_.CfnFunction.LoggingConfigProperty(
                log_group=enrichment_log_group.ref
            ),
        )
        enrichment_lambda.add_dependency(enrichment_log_group)

        init_role = iam.CfnRole(
            self,
            "DatabaseInitLambdaRole",
            assume_role_policy_document=assume_policy("lambda.amazonaws.com"),
            policies=[
                inline_policy(
                    "DatabaseInitPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "secretsmanager:GetSecretValue",
                                "Resource": db_secret.ref,
                            },
                            {
                                "Effect": "Allow",
                                "Action": "rds:DescribeDBInstances",
                                "Resource": cdk.Fn.sub(
                                    "arn:${AWS::Partition}:rds:${AWS::Region}:${AWS::AccountId}:db:${DbInstanceId}",
                                    {"DbInstanceId": database.ref},
                                ),
                            },
                        ],
                    },
                )
            ],
        )
        init_lambda = lambda_.CfnFunction(
            self,
            "DatabaseInitLambda",
            runtime="python3.12",
            architectures=["arm64"],
            handler="index.handler",
            code=bundled_lambda_code(self, INIT_CODE, ("pg8000", "scramp", "asn1crypto")),
            role=init_role.attr_arn,
            memory_size=256,
            timeout=60,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "DB_INSTANCE_ID": database.ref,
                    "DB_ENDPOINT_ADDRESS": database.attr_endpoint_address,
                    "DB_SECRET_ARN": db_secret.ref,
                    "DB_NAME": "portal",
                    "AWS_REGION": aws_region_value,
                    "AWS_ENDPOINT": aws_endpoint_value,
                }
            ),
            vpc_config=lambda_.CfnFunction.VpcConfigProperty(
                subnet_ids=[private_subnet_a.ref, private_subnet_b.ref],
                security_group_ids=[backend_sg.attr_group_id],
            ),
        )
        init_lambda.add_dependency(database)
        cdk.CfnCustomResource(
            self,
            "DatabaseInitializer",
            service_token=init_lambda.attr_arn,
        )

        rest_api = apigw.CfnRestApi(
            self,
            "BackendRestApi",
            name="enterprise-portal-backend-api",
            endpoint_configuration=apigw.CfnRestApi.EndpointConfigurationProperty(
                types=["REGIONAL"]
            ),
        )
        health_resource = apigw.CfnResource(
            self,
            "HealthResource",
            parent_id=rest_api.attr_root_resource_id,
            path_part="health",
            rest_api_id=rest_api.ref,
        )
        items_resource = apigw.CfnResource(
            self,
            "ItemsResource",
            parent_id=rest_api.attr_root_resource_id,
            path_part="items",
            rest_api_id=rest_api.ref,
        )
        lambda_integration_uri = cdk.Fn.sub(
            "arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${FunctionArn}/invocations",
            {"FunctionArn": backend_lambda.attr_arn},
        )
        health_method = apigw.CfnMethod(
            self,
            "HealthGetMethod",
            authorization_type="NONE",
            http_method="GET",
            resource_id=health_resource.ref,
            rest_api_id=rest_api.ref,
            integration=apigw.CfnMethod.IntegrationProperty(
                type="AWS_PROXY",
                integration_http_method="POST",
                uri=lambda_integration_uri,
            ),
        )
        items_method = apigw.CfnMethod(
            self,
            "ItemsPostMethod",
            authorization_type="NONE",
            http_method="POST",
            resource_id=items_resource.ref,
            rest_api_id=rest_api.ref,
            integration=apigw.CfnMethod.IntegrationProperty(
                type="AWS_PROXY",
                integration_http_method="POST",
                uri=lambda_integration_uri,
            ),
        )
        api_deployment = apigw.CfnDeployment(
            self,
            "BackendApiDeployment",
            rest_api_id=rest_api.ref,
        )
        api_deployment.add_dependency(health_method)
        api_deployment.add_dependency(items_method)

        api_logs_role = iam.CfnRole(
            self,
            "ApiGatewayLogsRole",
            assume_role_policy_document=assume_policy("apigateway.amazonaws.com"),
            managed_policy_arns=[
                cdk.Fn.sub(
                    "arn:${AWS::Partition}:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
                )
            ],
        )
        apigw.CfnAccount(
            self,
            "ApiGatewayAccount",
            cloud_watch_role_arn=api_logs_role.attr_arn,
        )
        api_stage = apigw.CfnStage(
            self,
            "BackendApiProdStage",
            rest_api_id=rest_api.ref,
            deployment_id=api_deployment.ref,
            stage_name="prod",
            access_log_setting=apigw.CfnStage.AccessLogSettingProperty(
                destination_arn=api_log_group.attr_arn,
                format=json.dumps(
                    {
                        "requestId": "$context.requestId",
                        "httpMethod": "$context.httpMethod",
                        "resourcePath": "$context.resourcePath",
                        "status": "$context.status",
                    }
                ),
            ),
            method_settings=[
                apigw.CfnStage.MethodSettingProperty(
                    resource_path="/*",
                    http_method="*",
                    metrics_enabled=True,
                    logging_level="INFO",
                )
            ],
        )
        api_stage.add_dependency(api_log_group)

        lambda_.CfnPermission(
            self,
            "ApiGatewayInvokeBackendPermission",
            action="lambda:InvokeFunction",
            function_name=backend_lambda.attr_arn,
            principal="apigateway.amazonaws.com",
            source_arn=cdk.Fn.sub(
                "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${ApiId}/*/*/*",
                {"ApiId": rest_api.ref},
            ),
        )

        alb = elbv2.CfnLoadBalancer(
            self,
            "ApplicationLoadBalancer",
            scheme="internet-facing",
            type="application",
            subnets=[public_subnet_a.ref, public_subnet_b.ref],
            security_groups=[alb_sg.attr_group_id],
        )
        alb_target_group = elbv2.CfnTargetGroup(
            self,
            "BackendLambdaTargetGroup",
            target_type="lambda",
            targets=[elbv2.CfnTargetGroup.TargetDescriptionProperty(id=backend_lambda.attr_arn)],
        )
        lambda_.CfnPermission(
            self,
            "AlbInvokeBackendPermission",
            action="lambda:InvokeFunction",
            function_name=backend_lambda.attr_arn,
            principal="elasticloadbalancing.amazonaws.com",
            source_arn=alb_target_group.ref,
        )
        http_listener = elbv2.CfnListener(
            self,
            "AlbHttpListener",
            load_balancer_arn=alb.ref,
            port=80,
            protocol="HTTP",
            default_actions=[
                elbv2.CfnListener.ActionProperty(
                    type="forward", target_group_arn=alb_target_group.ref
                )
            ],
        )
        elbv2.CfnListenerRule(
            self,
            "AlbBackendListenerRule",
            listener_arn=http_listener.ref,
            priority=1,
            conditions=[
                elbv2.CfnListenerRule.RuleConditionProperty(
                    field="path-pattern", values=["/*"]
                )
            ],
            actions=[
                elbv2.CfnListenerRule.ActionProperty(
                    type="forward", target_group_arn=alb_target_group.ref
                )
            ],
        )

        queue = sqs.CfnQueue(
            self,
            "ItemsQueue",
            visibility_timeout=30,
            sqs_managed_sse_enabled=True,
        )

        state_machine_role = iam.CfnRole(
            self,
            "WorkflowStateMachineRole",
            assume_role_policy_document=assume_policy("states.amazonaws.com"),
            policies=[
                inline_policy(
                    "WorkflowPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "lambda:InvokeFunction",
                                "Resource": [enrichment_lambda.attr_arn, backend_lambda.attr_arn],
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "logs:CreateLogDelivery",
                                    "logs:GetLogDelivery",
                                    "logs:UpdateLogDelivery",
                                    "logs:DeleteLogDelivery",
                                    "logs:ListLogDeliveries",
                                    "logs:PutResourcePolicy",
                                    "logs:DescribeResourcePolicies",
                                    "logs:DescribeLogGroups",
                                ],
                                "Resource": "*",
                            },
                        ],
                    },
                )
            ],
        )
        workflow_definition = {
            "StartAt": "EnrichMessage",
            "States": {
                "EnrichMessage": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": "${EnrichmentFunctionArn}",
                        "Payload.$": "$",
                    },
                    "Next": "CreateItem",
                },
                "CreateItem": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": "${BackendFunctionArn}",
                        "Payload": {
                            "action": "create_item",
                            "name.$": "$.Payload.name",
                        },
                    },
                    "End": True,
                },
            },
        }
        state_machine = sfn.CfnStateMachine(
            self,
            "ProcessingStateMachine",
            state_machine_type="STANDARD",
            role_arn=state_machine_role.attr_arn,
            definition_string=json.dumps(workflow_definition),
            definition_substitutions={
                "EnrichmentFunctionArn": enrichment_lambda.attr_arn,
                "BackendFunctionArn": backend_lambda.attr_arn,
            },
            logging_configuration=sfn.CfnStateMachine.LoggingConfigurationProperty(
                level="ALL",
                include_execution_data=True,
                destinations=[
                    sfn.CfnStateMachine.LogDestinationProperty(
                        cloud_watch_logs_log_group=sfn.CfnStateMachine.CloudWatchLogsLogGroupProperty(
                            log_group_arn=workflow_log_group.attr_arn
                        )
                    )
                ],
            ),
        )

        pipe_role = iam.CfnRole(
            self,
            "EventPipeRole",
            assume_role_policy_document=assume_policy("pipes.amazonaws.com"),
            policies=[
                inline_policy(
                    "EventPipePolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "sqs:ReceiveMessage",
                                    "sqs:DeleteMessage",
                                    "sqs:GetQueueAttributes",
                                    "sqs:ChangeMessageVisibility",
                                ],
                                "Resource": queue.attr_arn,
                            },
                            {
                                "Effect": "Allow",
                                "Action": "lambda:InvokeFunction",
                                "Resource": enrichment_lambda.attr_arn,
                            },
                            {
                                "Effect": "Allow",
                                "Action": "states:StartExecution",
                                "Resource": state_machine.attr_arn,
                            },
                        ],
                    },
                )
            ],
        )
        pipes.CfnPipe(
            self,
            "ItemsProcessingPipe",
            role_arn=pipe_role.attr_arn,
            source=queue.attr_arn,
            enrichment=enrichment_lambda.attr_arn,
            target=state_machine.attr_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=1
                )
            ),
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                step_function_state_machine_parameters=pipes.CfnPipe.PipeTargetStateMachineParametersProperty(
                    invocation_type="FIRE_AND_FORGET"
                )
            ),
        )

        trail_role = iam.CfnRole(
            self,
            "CloudTrailLogsRole",
            assume_role_policy_document=assume_policy("cloudtrail.amazonaws.com"),
            policies=[
                inline_policy(
                    "CloudTrailLogsPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                                "Resource": cdk.Fn.sub(
                                    "${LogArn}:*",
                                    {"LogArn": trail_log_group.attr_arn},
                                ),
                            }
                        ],
                    },
                )
            ],
        )
        trail = cloudtrail.CfnTrail(
            self,
            "ManagementTrail",
            is_logging=True,
            is_multi_region_trail=False,
            include_global_service_events=True,
            s3_bucket_name=trail_bucket.ref,
            cloud_watch_logs_log_group_arn=trail_log_group.attr_arn,
            cloud_watch_logs_role_arn=trail_role.attr_arn,
            event_selectors=[
                cloudtrail.CfnTrail.EventSelectorProperty(
                    include_management_events=True,
                    read_write_type="WriteOnly",
                )
            ],
        )

        config_role = iam.CfnRole(
            self,
            "ConfigRecorderRole",
            assume_role_policy_document=assume_policy("config.amazonaws.com"),
            policies=[
                inline_policy(
                    "ConfigRecorderPolicy",
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetBucketAcl",
                                    "s3:ListBucket",
                                    "s3:PutObject",
                                    "s3:GetBucketLocation",
                                ],
                                "Resource": [
                                    config_bucket.attr_arn,
                                    cdk.Fn.sub(
                                        "${BucketArn}/*",
                                        {"BucketArn": config_bucket.attr_arn},
                                    ),
                                ],
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "config:Put*",
                                    "config:Get*",
                                    "config:List*",
                                    "config:Describe*",
                                    "config:BatchGet*",
                                    "tag:GetResources",
                                ],
                                "Resource": "*",
                            },
                        ],
                    },
                )
            ],
        )
        recorder = config.CfnConfigurationRecorder(
            self,
            "ConfigurationRecorder",
            role_arn=config_role.attr_arn,
            recording_group=config.CfnConfigurationRecorder.RecordingGroupProperty(
                all_supported=True,
                include_global_resource_types=True,
            ),
        )
        delivery_channel = config.CfnDeliveryChannel(
            self,
            "ConfigDeliveryChannel",
            s3_bucket_name=config_bucket.ref,
        )
        delivery_channel.add_dependency(recorder)
        config_rule = config.CfnConfigRule(
            self,
            "CloudTrailEnabledRule",
            source=config.CfnConfigRule.SourceProperty(
                owner="AWS",
                source_identifier="CLOUD_TRAIL_ENABLED",
            ),
        )
        config_rule.add_dependency(trail)

        distribution = cloudfront.CfnDistribution(
            self,
            "FrontendDistribution",
            distribution_config=cloudfront.CfnDistribution.DistributionConfigProperty(
                enabled=True,
                default_root_object="index.html",
                origins=[
                    cloudfront.CfnDistribution.OriginProperty(
                        id="frontend-s3-origin",
                        domain_name=portal_bucket.attr_regional_domain_name,
                        origin_access_control_id=oac.ref,
                        s3_origin_config=cloudfront.CfnDistribution.S3OriginConfigProperty(
                            origin_access_identity=""
                        ),
                    ),
                    cloudfront.CfnDistribution.OriginProperty(
                        id="backend-api-origin",
                        domain_name=cdk.Fn.join(
                            "",
                            [
                                rest_api.ref,
                                ".execute-api.",
                                region,
                                ".amazonaws.com",
                            ],
                        ),
                        origin_path="/prod",
                        custom_origin_config=cloudfront.CfnDistribution.CustomOriginConfigProperty(
                            origin_protocol_policy="https-only",
                            https_port=443,
                            origin_ssl_protocols=["TLSv1.2"],
                        ),
                    ),
                ],
                default_cache_behavior=cloudfront.CfnDistribution.DefaultCacheBehaviorProperty(
                    target_origin_id="frontend-s3-origin",
                    viewer_protocol_policy="redirect-to-https",
                    allowed_methods=["GET", "HEAD"],
                    cached_methods=["GET", "HEAD"],
                    compress=True,
                    forwarded_values=cloudfront.CfnDistribution.ForwardedValuesProperty(
                        query_string=False
                    ),
                ),
                cache_behaviors=[
                    cloudfront.CfnDistribution.CacheBehaviorProperty(
                        path_pattern="/api/*",
                        target_origin_id="backend-api-origin",
                        viewer_protocol_policy="redirect-to-https",
                        allowed_methods=["GET", "HEAD", "OPTIONS", "POST"],
                        cached_methods=["GET", "HEAD", "OPTIONS"],
                        cache_policy_id="4135ea2d-6df8-44a3-9df3-4b5a84be39ad",
                    )
                ],
            ),
        )

        frontend_bucket_policy = s3.CfnBucketPolicy(
            self,
            "FrontendBucketPolicy",
            bucket=portal_bucket.ref,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowCloudFrontReadViaOac",
                        "Effect": "Allow",
                        "Principal": {"Service": "cloudfront.amazonaws.com"},
                        "Action": "s3:GetObject",
                        "Resource": cdk.Fn.sub(
                            "${BucketArn}/*", {"BucketArn": portal_bucket.attr_arn}
                        ),
	                        "Condition": {
	                            "StringEquals": {
	                                "AWS:SourceArn": cdk.Fn.sub(
	                                    "arn:${AWS::Partition}:cloudfront::${AWS::AccountId}:distribution/${DistributionId}",
	                                    {"DistributionId": distribution.ref},
	                                )
	                            }
	                        },
	                    },
	                ],
	            },
	        )
        trail_bucket_policy = s3.CfnBucketPolicy(
            self,
            "CloudTrailDeliveryBucketPolicy",
            bucket=trail_bucket.ref,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowCloudTrailBucketAclCheck",
                        "Effect": "Allow",
                        "Principal": {"Service": "cloudtrail.amazonaws.com"},
                        "Action": "s3:GetBucketAcl",
                        "Resource": trail_bucket.attr_arn,
                    },
                    {
                        "Sid": "AllowCloudTrailWrite",
                        "Effect": "Allow",
                        "Principal": {"Service": "cloudtrail.amazonaws.com"},
                        "Action": "s3:PutObject",
                        "Resource": cdk.Fn.sub(
                            "${BucketArn}/AWSLogs/${AWS::AccountId}/*",
                            {"BucketArn": trail_bucket.attr_arn},
                        ),
                        "Condition": {
                            "StringEquals": {"s3:x-amz-acl": "bucket-owner-full-control"}
                        },
                    },
                ],
            },
        )
        trail.add_dependency(trail_bucket_policy)


def main() -> None:
    app = cdk.App()
    ThreeTierStack(
        app,
        "ThreeTierStack",
        env=cdk.Environment(
            account=os.environ.get("CDK_DEFAULT_ACCOUNT", "000000000000"),
            region=configured_region(),
        ),
    )
    app.synth()


if __name__ == "__main__":
    main()
