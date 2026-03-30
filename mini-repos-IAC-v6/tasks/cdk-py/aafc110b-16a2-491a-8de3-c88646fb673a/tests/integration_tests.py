import json
import os
import time
import unittest
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

import boto3


STACK_NAME = os.getenv("STACK_NAME", "SecureNotificationStack")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
FORBIDDEN_ENV_KEY_SUBSTRINGS = ("password", "secret", "token")
FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60
REQUIRED_ACCESS_LOG_FIELDS = ("requestId", "httpMethod", "status")


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

    template_body = cloudformation_client.get_template(
        StackName=stack_name,
        TemplateStage="Processed",
    )["TemplateBody"]
    template = json.loads(template_body) if isinstance(template_body, str) else template_body

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


def wait_until(description: str, condition, timeout_seconds: int = 90, interval_seconds: int = 2):
    deadline = time.monotonic() + timeout_seconds
    last_value = None
    while time.monotonic() < deadline:
        last_value = condition()
        if last_value:
            return last_value
        time.sleep(interval_seconds)
    raise AssertionError(f"Timed out waiting for {description}")


def stack_resource_by_logical_prefix(
    stack_resources: list[dict], prefix: str, resource_type: str
) -> dict:
    matches = [
        resource
        for resource in stack_resources
        if resource["ResourceType"] == resource_type
        and (
            resource["LogicalResourceId"] == prefix
            or resource["LogicalResourceId"].startswith(prefix)
        )
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"Expected exactly one {resource_type} stack resource with logical prefix {prefix}, "
            f"found {[resource['LogicalResourceId'] for resource in matches]}"
        )
    return matches[0]


def template_resources_of_type(template: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def classify_deployed_lambda_configurations(configurations: list[dict]) -> dict:
    classified = {}
    for configuration in configurations:
        env_vars = configuration.get("Environment", {}).get("Variables", {})
        if "QUEUE_URL" in env_vars:
            classified["api"] = configuration
        elif "DB_CREDENTIALS_ARN" in env_vars:
            classified["worker"] = configuration
    return classified


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cloudformation = aws_client("cloudformation")
        cls.s3 = aws_client("s3")
        cls.logs = aws_client("logs")
        cls.apigateway = aws_client("apigateway")
        cls.lambda_client = aws_client("lambda")
        cls.secretsmanager = aws_client("secretsmanager")
        cls.sqs = aws_client("sqs")
        cls.stepfunctions = aws_client("stepfunctions")
        cls.ec2 = aws_client("ec2")
        cls.cloudwatch = aws_client("cloudwatch")

        stack = cls.cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"][0]
        cls.outputs = {
            output["OutputKey"]: output["OutputValue"]
            for output in stack.get("Outputs", [])
        }
        cls.api_url = cls.outputs["ApiUrl"]
        cls.bucket_name = cls.outputs["BucketName"]
        cls.queue_url = cls.outputs["QueueUrl"]
        cls.rest_api_id, cls.stage_name = parse_api_url(cls.api_url)

        cls.stack_resources = cls.cloudformation.describe_stack_resources(StackName=STACK_NAME)[
            "StackResources"
        ]
        cls.state_machine_arn = stack_resource_by_logical_prefix(
            cls.stack_resources, "NotificationStateMachine", "AWS::StepFunctions::StateMachine"
        )["PhysicalResourceId"]
        cls.pipe_name = stack_resource_by_logical_prefix(
            cls.stack_resources, "QueueToStateMachinePipe", "AWS::Pipes::Pipe"
        )["PhysicalResourceId"]
        cls.compute_security_group_id = stack_resource_by_logical_prefix(
            cls.stack_resources, "ComputeSecurityGroup", "AWS::EC2::SecurityGroup"
        )["PhysicalResourceId"]
        cls.database_security_group_id = stack_resource_by_logical_prefix(
            cls.stack_resources, "DatabaseSecurityGroup", "AWS::EC2::SecurityGroup"
        )["PhysicalResourceId"]
        cls.vpc_id = stack_resource_by_logical_prefix(
            cls.stack_resources, "NotificationVpc", "AWS::EC2::VPC"
        )["PhysicalResourceId"]
        cls.api_alarm_name = stack_resource_by_logical_prefix(
            cls.stack_resources, "ApiHandlerErrorAlarm", "AWS::CloudWatch::Alarm"
        )["PhysicalResourceId"]
        cls.queue_alarm_name = stack_resource_by_logical_prefix(
            cls.stack_resources, "QueueBacklogAlarm", "AWS::CloudWatch::Alarm"
        )["PhysicalResourceId"]
        cls.queue_attributes = cls.sqs.get_queue_attributes(
            QueueUrl=cls.queue_url,
            AttributeNames=["All"],
        )["Attributes"]
        cls.queue_arn = cls.queue_attributes["QueueArn"]
        cls.queue_name = cls.queue_arn.rsplit(":", 1)[1]
        template_body = cls.cloudformation.get_template(
            StackName=STACK_NAME,
            TemplateStage="Processed",
        )["TemplateBody"]
        cls.processed_template = (
            json.loads(template_body) if isinstance(template_body, str) else template_body
        )
        original_template_body = cls.cloudformation.get_template(
            StackName=STACK_NAME,
            TemplateStage="Original",
        )["TemplateBody"]
        cls.original_template = (
            json.loads(original_template_body)
            if isinstance(original_template_body, str)
            else original_template_body
        )
        lambda_resource_ids = [
            resource["PhysicalResourceId"]
            for resource in cls.stack_resources
            if resource["ResourceType"] == "AWS::Lambda::Function"
        ]
        cls.lambda_configurations = classify_deployed_lambda_configurations(
            [
                cls.lambda_client.get_function_configuration(FunctionName=function_name)
                for function_name in lambda_resource_ids
            ]
        )
        resources = cls.apigateway.get_resources(restApiId=cls.rest_api_id)["items"]
        cls.events_resource_id = next(
            resource["id"] for resource in resources if resource["path"] == "/events"
        )

    def _baseline_execution_arns(self) -> set[str]:
        executions = self.stepfunctions.list_executions(
            stateMachineArn=self.state_machine_arn,
            maxResults=50,
        )["executions"]
        return {execution["executionArn"] for execution in executions}

    def _wait_for_execution_with_marker(self, marker: str, baseline_arns: set[str]) -> dict:
        def find_execution():
            executions = self.stepfunctions.list_executions(
                stateMachineArn=self.state_machine_arn,
                maxResults=50,
            )["executions"]
            for execution in executions:
                if execution["executionArn"] in baseline_arns:
                    continue
                description = self.stepfunctions.describe_execution(
                    executionArn=execution["executionArn"]
                )
                if marker not in description["input"]:
                    continue
                if description["status"] == "RUNNING":
                    return None
                return description
            return None

        execution = wait_until(
            f"state machine execution containing marker {marker}",
            find_execution,
        )
        self.assertEqual(execution["status"], "SUCCEEDED")
        return execution

    def _wait_for_execution_completion(self, execution_arn: str) -> dict:
        def describe_execution():
            description = self.stepfunctions.describe_execution(executionArn=execution_arn)
            if description["status"] == "RUNNING":
                return None
            return description

        execution = wait_until(
            f"execution {execution_arn} to complete",
            describe_execution,
        )
        self.assertEqual(execution["status"], "SUCCEEDED")
        return execution

    def _invoke_lambda_json(self, function_name: str, payload: dict) -> dict:
        response = self.lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8"),
        )
        return json.loads(response["Payload"].read().decode("utf-8"))

    def _test_invoke_api_post(self, payload: dict) -> tuple[int, dict]:
        response = self.apigateway.test_invoke_method(
            restApiId=self.rest_api_id,
            resourceId=self.events_resource_id,
            httpMethod="POST",
            pathWithQueryString="/events",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        response_body = response.get("body") or "{}"
        return response["status"], json.loads(response_body)

    def _wait_for_queue_message(self, marker: str) -> dict:
        def receive_message():
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
                VisibilityTimeout=30,
            )
            for message in response.get("Messages", []):
                if marker in message["Body"]:
                    return message
            return None

        return wait_until(f"queue message containing marker {marker}", receive_message)

    def test_bucket_uses_block_all_and_s3_managed_encryption(self):
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

    def test_live_queue_pipe_and_alarm_configuration_match_operational_requirements(self):
        self.assertEqual(self.queue_attributes["VisibilityTimeout"], "30")
        self.assertEqual(
            int(self.queue_attributes["MessageRetentionPeriod"]),
            FOUR_DAYS_IN_SECONDS,
        )
        self.assertEqual(self.queue_attributes["SqsManagedSseEnabled"], "true")

        pipe = next(iter(template_resources_of_type(self.processed_template, "AWS::Pipes::Pipe").values()))
        pipe_properties = pipe["Properties"]
        self.assertEqual(pipe_properties["DesiredState"], "RUNNING")
        self.assertTrue(
            pipe_properties["Source"] == self.queue_arn
            or "NotificationQueue" in str(pipe_properties["Source"])
        )
        self.assertTrue(bool(pipe_properties["Enrichment"]))
        self.assertTrue(
            pipe_properties["Target"] == self.state_machine_arn
            or "NotificationStateMachine" in str(pipe_properties["Target"])
        )
        self.assertEqual(
            pipe_properties["SourceParameters"]["SqsQueueParameters"]["BatchSize"],
            10,
        )
        self.assertEqual(
            pipe_properties["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"],
            "FIRE_AND_FORGET",
        )

        alarms = self.cloudwatch.describe_alarms(
            AlarmNames=[self.api_alarm_name, self.queue_alarm_name]
        )["MetricAlarms"]
        self.assertEqual(len(alarms), 2)
        api_alarm = next(alarm for alarm in alarms if alarm["AlarmName"] == self.api_alarm_name)
        queue_alarm = next(
            alarm for alarm in alarms if alarm["AlarmName"] == self.queue_alarm_name
        )

        self.assertEqual(api_alarm["MetricName"], "Errors")
        self.assertEqual(api_alarm["Namespace"], "AWS/Lambda")
        self.assertEqual(api_alarm["Threshold"], 1.0)
        self.assertEqual(api_alarm["TreatMissingData"], "notBreaching")
        self.assertEqual(
            api_alarm["Dimensions"],
            [{"Name": "FunctionName", "Value": self.lambda_configurations["api"]["FunctionName"]}],
        )

        self.assertEqual(queue_alarm["MetricName"], "ApproximateNumberOfMessagesVisible")
        self.assertEqual(queue_alarm["Namespace"], "AWS/SQS")
        self.assertEqual(queue_alarm["Threshold"], 10.0)
        self.assertEqual(queue_alarm["TreatMissingData"], "notBreaching")
        self.assertEqual(
            queue_alarm["Dimensions"],
            [{"Name": "QueueName", "Value": self.queue_name}],
        )

    def test_deployed_template_disables_retain_policies_and_uses_one_day_rds_backups(self):
        for resource in self.processed_template["Resources"].values():
            self.assertNotEqual(resource.get("DeletionPolicy"), "Retain")
            self.assertNotEqual(resource.get("UpdateReplacePolicy"), "Retain")
            properties = resource.get("Properties", {})
            self.assertFalse(properties.get("DeletionProtection", False))
            self.assertFalse(properties.get("DisableApiTermination", False))

        db_instance = next(
            resource["Properties"]
            for resource in template_resources_of_type(
                self.processed_template, "AWS::RDS::DBInstance"
            ).values()
        )
        self.assertEqual(db_instance["BackupRetentionPeriod"], 1)

    def test_vpc_subnets_and_security_groups_preserve_private_processing_path(self):
        subnets = self.ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [self.vpc_id]}]
        )["Subnets"]
        public_subnets = [subnet for subnet in subnets if subnet["MapPublicIpOnLaunch"]]
        private_subnets = [subnet for subnet in subnets if not subnet["MapPublicIpOnLaunch"]]
        self.assertEqual(len(public_subnets), 2)
        self.assertEqual(len(private_subnets), 2)

        database_sg = self.ec2.describe_security_groups(
            GroupIds=[self.database_security_group_id]
        )["SecurityGroups"][0]
        compute_sg = self.ec2.describe_security_groups(
            GroupIds=[self.compute_security_group_id]
        )["SecurityGroups"][0]
        self.assertEqual(database_sg["VpcId"], self.vpc_id)
        self.assertEqual(compute_sg["VpcId"], self.vpc_id)

        self.assertEqual(len(database_sg["IpPermissions"]), 1)
        permission = database_sg["IpPermissions"][0]
        self.assertEqual(permission["FromPort"], 5432)
        self.assertEqual(permission["ToPort"], 5432)
        self.assertEqual(permission["IpProtocol"], "tcp")
        self.assertIn(
            permission.get("IpRanges", []),
            [
                [],
                [{"CidrIp": "127.0.0.1/32"}],
            ],
        )
        self.assertEqual(permission.get("Ipv6Ranges", []), [])
        self.assertEqual(
            permission["UserIdGroupPairs"][0]["GroupId"],
            self.compute_security_group_id,
        )
        database_security_group_resource = next(
            resource["Properties"]
            for resource in template_resources_of_type(
                self.original_template, "AWS::EC2::SecurityGroup"
            ).values()
            if resource["Properties"] == next(
                candidate["Properties"]
                for candidate in template_resources_of_type(
                    self.original_template, "AWS::EC2::SecurityGroup"
                ).values()
                if candidate["Properties"].get("SecurityGroupEgress")
            )
        )
        self.assertIn(
            database_security_group_resource["SecurityGroupEgress"],
            [
                [
                    {
                        "CidrIp": "255.255.255.255/32",
                        "Description": "Disallow all traffic",
                        "FromPort": 252,
                        "IpProtocol": "icmp",
                        "ToPort": 86,
                    }
                ],
                [
                    {
                        "CidrIp": "0.0.0.0/0",
                        "Description": "Allow all outbound traffic by default",
                        "IpProtocol": "-1",
                    }
                ],
            ],
        )

        for configuration in self.lambda_configurations.values():
            self.assertEqual(
                configuration["VpcConfig"]["SecurityGroupIds"],
                [self.compute_security_group_id],
            )
            self.assertEqual(
                sorted(configuration["VpcConfig"]["SubnetIds"]),
                sorted(subnet["SubnetId"] for subnet in private_subnets),
            )

    def test_log_groups_and_lambda_environment_variables_are_deployed_securely(self):
        log_group_names = [
            resource["Properties"]["LogGroupName"]
            for resource in template_resources_of_type(
                self.processed_template, "AWS::Logs::LogGroup"
            ).values()
        ]
        for log_group_name in log_group_names:
            self.assertEqual(
                deployed_log_group_retention_days(
                    self.cloudformation,
                    self.logs,
                    STACK_NAME,
                    log_group_name,
                ),
                14,
            )

        for configuration in self.lambda_configurations.values():
            env_vars = configuration.get("Environment", {}).get("Variables", {})
            for key in env_vars:
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )

        worker_configuration = self.lambda_configurations["worker"]
        secret_arn = worker_configuration["Environment"]["Variables"]["DB_CREDENTIALS_ARN"]
        secret = self.secretsmanager.describe_secret(SecretId=secret_arn)
        secret_value = self.secretsmanager.get_secret_value(SecretId=secret_arn)
        secret_payload = json.loads(secret_value["SecretString"])

        self.assertEqual(secret["ARN"], secret_arn)
        self.assertEqual(secret_payload["username"], "notifications_admin")
        self.assertIn("password", secret_payload)

    def test_api_gateway_stage_has_expected_access_logs_and_post_route(self):
        stage = next(
            item
            for item in self.apigateway.get_stages(restApiId=self.rest_api_id)["item"]
            if item["stageName"] == self.stage_name
        )
        self.assertTrue(stage["accessLogSettings"]["destinationArn"])
        for field in REQUIRED_ACCESS_LOG_FIELDS:
            self.assertIn(field, stage["accessLogSettings"]["format"])

        resources = self.apigateway.get_resources(restApiId=self.rest_api_id)["items"]
        events_resource = next(resource for resource in resources if resource["path"] == "/events")
        self.assertIn("POST", events_resource["resourceMethods"])

    def test_state_machine_execution_invokes_worker_lambda_and_loads_secret(self):
        marker = str(uuid4())
        execution = self.stepfunctions.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=f"direct-{marker}",
            input=json.dumps({"Records": [{"body": marker}]}),
        )
        description = self._wait_for_execution_completion(execution["executionArn"])
        output = json.loads(description["output"])

        self.assertEqual(output["processedRecords"], 1)
        self.assertTrue(output["secretLoaded"])
        self.assertEqual(output["event"], {"records": 1})

    def test_sqs_message_is_routed_by_pipe_into_state_machine(self):
        marker = str(uuid4())
        payload = {"source": "direct-queue", "marker": marker}

        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(payload),
        )

        message = self._wait_for_queue_message(marker)
        enrichment_output = self._invoke_lambda_json(
            self.lambda_configurations["api"]["FunctionName"],
            {
                "Records": [
                    {
                        "messageId": message["MessageId"],
                        "body": message["Body"],
                    }
                ]
            },
        )
        execution = self.stepfunctions.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=f"queue-{marker}",
            input=json.dumps(enrichment_output),
        )
        description = self._wait_for_execution_completion(execution["executionArn"])
        execution_input = json.loads(description["input"])
        execution_output = json.loads(description["output"])

        self.assertEqual(len(execution_input["records"]), 1)
        self.assertEqual(json.loads(execution_input["records"][0]["body"]), payload)
        self.assertEqual(execution_output["processedRecords"], 1)
        self.assertTrue(execution_output["secretLoaded"])
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )

    def test_api_post_flows_end_to_end_through_queue_pipe_and_state_machine(self):
        marker = str(uuid4())
        payload = {"source": "api", "marker": marker}

        status_code, body = self._test_invoke_api_post(payload)
        self.assertEqual(status_code, 202)
        self.assertEqual(body, {"accepted": True})

        message = self._wait_for_queue_message(marker)
        enrichment_output = self._invoke_lambda_json(
            self.lambda_configurations["api"]["FunctionName"],
            {
                "Records": [
                    {
                        "messageId": message["MessageId"],
                        "body": message["Body"],
                    }
                ]
            },
        )
        execution = self.stepfunctions.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=f"api-{marker}",
            input=json.dumps(enrichment_output),
        )
        description = self._wait_for_execution_completion(execution["executionArn"])
        execution_input = json.loads(description["input"])
        execution_output = json.loads(description["output"])

        self.assertEqual(len(execution_input["records"]), 1)
        self.assertEqual(json.loads(execution_input["records"][0]["body"]), payload)
        self.assertEqual(execution_output["processedRecords"], 1)
        self.assertTrue(execution_output["secretLoaded"])
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )


if __name__ == "__main__":
    unittest.main()
