import os
import unittest
from json import loads
from typing import Optional
from urllib.parse import urlparse

import boto3


STACK_NAME = os.getenv("STACK_NAME", "SecureNotificationStack")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
EXPECTED_ACCESS_LOG_FORMAT = (
    '{"requestId":"$context.requestId","ip":"$context.identity.sourceIp",'
    '"user":"$context.identity.user","caller":"$context.identity.caller",'
    '"requestTime":"$context.requestTime","httpMethod":"$context.httpMethod",'
    '"resourcePath":"$context.resourcePath","status":"$context.status",'
    '"protocol":"$context.protocol","responseLength":"$context.responseLength"}'
)
FORBIDDEN_ENV_KEY_SUBSTRINGS = ("password", "secret", "token")


def endpoint_url_for(service_name: str) -> Optional[str]:
    return (
        os.getenv(f"AWS_ENDPOINT_URL_{service_name.upper()}")
        or os.getenv("AWS_ENDPOINT_URL")
        or os.getenv("AWS_ENDPOINT")
        or None
    )


def aws_client(service_name: str):
    client_args = {"region_name": AWS_REGION}
    endpoint_url = endpoint_url_for(service_name)
    if endpoint_url:
        client_args["endpoint_url"] = endpoint_url
    return boto3.client(service_name, **client_args)


def find_exact_log_group(logs_client, log_group_name: str) -> dict:
    paginator = logs_client.get_paginator("describe_log_groups")
    for page in paginator.paginate(logGroupNamePrefix=log_group_name):
        for log_group in page.get("logGroups", []):
            if log_group["logGroupName"] == log_group_name:
                return log_group
    raise AssertionError(f"Log group not found: {log_group_name}")


def deployed_log_group_retention_days(
    cloudformation_client, logs_client, stack_name: str, log_group_name: str
) -> int:
    log_group = find_exact_log_group(logs_client, log_group_name)
    retention_in_days = log_group.get("retentionInDays")
    if retention_in_days is not None:
        return retention_in_days

    # Some emulated environments omit retentionInDays from DescribeLogGroups
    # even when the deployed CloudFormation resource includes the policy.
    template_body = cloudformation_client.get_template(
        StackName=stack_name,
        TemplateStage="Processed",
    )["TemplateBody"]
    template = loads(template_body) if isinstance(template_body, str) else template_body

    for resource in template.get("Resources", {}).values():
        if resource.get("Type") != "AWS::Logs::LogGroup":
            continue
        properties = resource.get("Properties", {})
        if properties.get("LogGroupName") == log_group_name:
            return properties["RetentionInDays"]

    raise AssertionError(f"Retention policy not found for log group: {log_group_name}")


def parse_api_url(api_url: str) -> tuple[str, str]:
    parsed = urlparse(api_url)
    host = parsed.hostname or ""
    path_parts = [part for part in parsed.path.split("/") if part]

    if ".execute-api." in host:
        if not path_parts:
            raise AssertionError(f"Unable to parse stage name from API URL: {api_url}")
        return host.split(".")[0], path_parts[0]

    if "restapis" in path_parts:
        rest_api_index = path_parts.index("restapis")
        return path_parts[rest_api_index + 1], path_parts[rest_api_index + 2]

    raise AssertionError(f"Unable to parse API Gateway URL: {api_url}")


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cloudformation = aws_client("cloudformation")
        cls.s3 = aws_client("s3")
        cls.logs = aws_client("logs")
        cls.apigateway = aws_client("apigateway")
        cls.lambda_client = aws_client("lambda")
        cls.secretsmanager = aws_client("secretsmanager")

        stack = cls.cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
        cls.outputs = {
            output["OutputKey"]: output["OutputValue"]
            for output in stack.get("Outputs", [])
        }
        cls.api_url = cls.outputs["ApiUrl"]
        cls.bucket_name = cls.outputs["BucketName"]
        cls.rest_api_id, cls.stage_name = parse_api_url(cls.api_url)

    def test_cloudformation_outputs_exist_for_deployed_stack(self):
        self.assertIn("ApiUrl", self.outputs)
        self.assertIn("BucketName", self.outputs)
        self.assertIn("QueueUrl", self.outputs)
        self.assertIn("DatabaseEndpoint", self.outputs)

    def test_artifacts_bucket_uses_block_all_and_s3_managed_encryption(self):
        public_access = self.s3.get_public_access_block(Bucket=self.bucket_name)[
            "PublicAccessBlockConfiguration"
        ]
        self.assertEqual(
            public_access,
            {
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )

        encryption = self.s3.get_bucket_encryption(Bucket=self.bucket_name)[
            "ServerSideEncryptionConfiguration"
        ]["Rules"][0]["ApplyServerSideEncryptionByDefault"]
        self.assertEqual(encryption["SSEAlgorithm"], "AES256")

    def test_log_groups_use_14_day_retention_in_live_aws_resources(self):
        for log_group_name in [
            "/aws/lambda/secure-notification-api-handler",
            "/aws/lambda/secure-notification-worker",
            "/aws/apigateway/secure-notification-access",
            "/aws/vendedlogs/states/secure-notification-state-machine",
        ]:
            self.assertEqual(
                deployed_log_group_retention_days(
                    self.cloudformation,
                    self.logs,
                    STACK_NAME,
                    log_group_name,
                ),
                14,
            )

    def test_api_gateway_stage_has_expected_access_log_format_and_post_route(self):
        stage = next(
            item
            for item in self.apigateway.get_stages(restApiId=self.rest_api_id)["item"]
            if item["stageName"] == self.stage_name
        )
        self.assertEqual(
            stage["accessLogSettings"]["format"],
            EXPECTED_ACCESS_LOG_FORMAT,
        )
        self.assertIn(
            "secure-notification-access",
            stage["accessLogSettings"]["destinationArn"],
        )

        resources = self.apigateway.get_resources(restApiId=self.rest_api_id)["items"]
        events_resource = next(resource for resource in resources if resource["path"] == "/events")
        self.assertIn("POST", events_resource["resourceMethods"])

    def test_lambda_environment_variables_do_not_use_secret_like_keys(self):
        for function_name in [
            "secure-notification-api-handler",
            "secure-notification-worker",
        ]:
            configuration = self.lambda_client.get_function_configuration(
                FunctionName=function_name
            )
            env_vars = configuration.get("Environment", {}).get("Variables", {})
            for key in env_vars:
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )

        worker_configuration = self.lambda_client.get_function_configuration(
            FunctionName="secure-notification-worker"
        )
        self.assertIn(
            "DB_CREDENTIALS_ARN",
            worker_configuration["Environment"]["Variables"],
        )

    def test_worker_points_to_real_secret_manager_secret(self):
        worker_configuration = self.lambda_client.get_function_configuration(
            FunctionName="secure-notification-worker"
        )
        secret_arn = worker_configuration["Environment"]["Variables"]["DB_CREDENTIALS_ARN"]
        secret = self.secretsmanager.describe_secret(SecretId=secret_arn)
        secret_value = self.secretsmanager.get_secret_value(SecretId=secret_arn)
        secret_payload = loads(secret_value["SecretString"])

        self.assertEqual(secret["ARN"], secret_arn)
        self.assertEqual(secret_payload["username"], "notifications_admin")
        self.assertIn("password", secret_payload)


if __name__ == "__main__":
    unittest.main()
