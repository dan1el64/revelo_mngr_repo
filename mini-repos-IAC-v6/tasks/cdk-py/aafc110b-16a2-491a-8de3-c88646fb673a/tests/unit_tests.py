import os
import re
import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Optional
from unittest.mock import patch

from aws_cdk import App
from aws_cdk.assertions import Match, Template


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app.py"
STACK_NAME = "UnitTestStack"
FUNCTION_NAMES = {
    "api": "secure-notification-api-handler",
    "worker": "secure-notification-worker",
}
LOG_GROUP_NAMES = {
    "api": "/aws/lambda/secure-notification-api-handler",
    "worker": "/aws/lambda/secure-notification-worker",
    "api_access": "/aws/apigateway/secure-notification-access",
    "state_machine": "/aws/vendedlogs/states/secure-notification-state-machine",
}
FORBIDDEN_ENV_KEY_SUBSTRINGS = ("password", "secret", "token")
EXPECTED_ACCESS_LOG_FORMAT = (
    '{"requestId":"$context.requestId","ip":"$context.identity.sourceIp",'
    '"user":"$context.identity.user","caller":"$context.identity.caller",'
    '"requestTime":"$context.requestTime","httpMethod":"$context.httpMethod",'
    '"resourcePath":"$context.resourcePath","status":"$context.status",'
    '"protocol":"$context.protocol","responseLength":"$context.responseLength"}'
)
FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60
SYNTH_ENV = {
    "AWS_REGION": "us-east-1",
    "AWS_ENDPOINT": "https://api.aws.amazon.com",
    "AWS_ACCESS_KEY_ID": "test-key",
    "AWS_SECRET_ACCESS_KEY": "test-secret",
}
SDK_ENDPOINT_ENV_VARS = [
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_APIGATEWAY",
    "AWS_ENDPOINT_URL_CLOUDFORMATION",
    "AWS_ENDPOINT_URL_CLOUDWATCH",
    "AWS_ENDPOINT_URL_EC2",
    "AWS_ENDPOINT_URL_EVENTS",
    "AWS_ENDPOINT_URL_IAM",
    "AWS_ENDPOINT_URL_LAMBDA",
    "AWS_ENDPOINT_URL_LOGS",
    "AWS_ENDPOINT_URL_PIPES",
    "AWS_ENDPOINT_URL_RDS",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL_SECRETSMANAGER",
    "AWS_ENDPOINT_URL_SQS",
    "AWS_ENDPOINT_URL_STATES",
]


def load_app_module():
    spec = spec_from_file_location("solution_app", APP_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def synth_template(extra_env: Optional[dict] = None) -> Template:
    env = dict(SYNTH_ENV)
    if extra_env:
        env.update(extra_env)

    with patch.dict(os.environ, env, clear=False):
        module = load_app_module()
        app = App()
        stack = module.SecureNotificationStack(app, STACK_NAME)
        return Template.from_stack(stack)


def synth_template_json(extra_env: Optional[dict] = None) -> dict:
    return synth_template(extra_env).to_json()


def resources_of_type(template_json: dict, resource_type: str) -> dict:
    return {
        logical_id: resource
        for logical_id, resource in template_json["Resources"].items()
        if resource["Type"] == resource_type
    }


def resource_properties_of_type(template_json: dict, resource_type: str) -> dict:
    return {
        logical_id: resource["Properties"]
        for logical_id, resource in resources_of_type(template_json, resource_type).items()
    }


def only_resource_of_type(template_json: dict, resource_type: str) -> tuple[str, dict]:
    resources = resource_properties_of_type(template_json, resource_type)
    return next(iter(resources.items()))


def logical_id_for_log_group(template_json: dict, log_group_name: str) -> str:
    log_groups = resource_properties_of_type(template_json, "AWS::Logs::LogGroup")
    return next(
        logical_id
        for logical_id, properties in log_groups.items()
        if properties["LogGroupName"] == log_group_name
    )


def logical_id_for_security_group(template_json: dict, description: str) -> str:
    security_groups = resource_properties_of_type(template_json, "AWS::EC2::SecurityGroup")
    return next(
        logical_id
        for logical_id, properties in security_groups.items()
        if properties["GroupDescription"] == description
    )


def logical_id_for_function(template_json: dict, function_name: str) -> str:
    functions = resource_properties_of_type(template_json, "AWS::Lambda::Function")
    return next(
        logical_id
        for logical_id, properties in functions.items()
        if properties["FunctionName"] == function_name
    )


def logical_id_for_resource_type(template_json: dict, resource_type: str) -> str:
    return next(iter(resource_properties_of_type(template_json, resource_type)))


def lambda_functions_by_name(template_json: dict) -> dict:
    return {
        properties["FunctionName"]: properties
        for properties in resource_properties_of_type(template_json, "AWS::Lambda::Function").values()
    }


def private_subnet_refs(template_json: dict) -> list[dict]:
    subnet_resources = resource_properties_of_type(template_json, "AWS::EC2::Subnet")
    private_ids = [
        logical_id
        for logical_id, properties in subnet_resources.items()
        if not properties["MapPublicIpOnLaunch"]
    ]
    return [{"Ref": logical_id} for logical_id in sorted(private_ids)]


def iam_role_with_policy_name(template_json: dict, policy_name: str) -> dict:
    roles = resource_properties_of_type(template_json, "AWS::IAM::Role")
    return next(
        properties
        for properties in roles.values()
        if any(policy["PolicyName"] == policy_name for policy in properties.get("Policies", []))
    )


def log_stream_resource(logical_id: str) -> list[dict]:
    return [
        {"Fn::GetAtt": [logical_id, "Arn"]},
        {"Fn::Join": ["", [{"Fn::GetAtt": [logical_id, "Arn"]}, ":*"]]},
    ]


class TestModuleConfiguration(unittest.TestCase):
    def test_deliverable_is_self_contained_root_app(self):
        root_python_files = sorted(path.name for path in ROOT.glob("*.py"))
        self.assertEqual(root_python_files, ["app.py"])

        source = APP_PATH.read_text()
        self.assertIn("_lambda.Code.from_inline(", source)
        self.assertNotIn("Code.from_asset(", source)
        self.assertNotIn("requirements-dev.txt", source)

    def test_configure_aws_endpoint_environment_sets_all_sdk_overrides(self):
        with patch.dict(os.environ, {"AWS_ENDPOINT": "https://example.test"}, clear=False):
            module = load_app_module()
            for name in SDK_ENDPOINT_ENV_VARS:
                os.environ.pop(name, None)

            configured_endpoint = module.configure_aws_endpoint_environment()

            self.assertEqual(configured_endpoint, "https://example.test")
            for name in SDK_ENDPOINT_ENV_VARS:
                self.assertEqual(os.environ[name], "https://example.test")

    def test_configure_aws_endpoint_environment_is_noop_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            module = load_app_module()
            for name in SDK_ENDPOINT_ENV_VARS:
                os.environ.pop(name, None)
            os.environ.pop("AWS_ENDPOINT", None)

            configured_endpoint = module.configure_aws_endpoint_environment()

            self.assertEqual(configured_endpoint, "")
            for name in SDK_ENDPOINT_ENV_VARS:
                self.assertNotIn(name, os.environ)

    def test_default_region_falls_back_to_us_east_1(self):
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "https://api.aws.amazon.com",
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
            },
            clear=False,
        ):
            os.environ.pop("AWS_REGION", None)
            module = load_app_module()

            self.assertEqual(module.aws_region, "us-east-1")
            self.assertEqual(os.environ["AWS_REGION"], "us-east-1")

class TestNetworkSecurity(unittest.TestCase):
    def test_vpc_has_two_public_two_private_subnets_and_one_nat_gateway(self):
        template = synth_template_json()
        subnets = list(resource_properties_of_type(template, "AWS::EC2::Subnet").values())
        public_subnets = [subnet for subnet in subnets if subnet["MapPublicIpOnLaunch"]]
        private_subnets = [subnet for subnet in subnets if not subnet["MapPublicIpOnLaunch"]]

        self.assertEqual(len(subnets), 4)
        self.assertEqual(len(public_subnets), 2)
        self.assertEqual(len(private_subnets), 2)
        self.assertEqual(len(resources_of_type(template, "AWS::EC2::NatGateway")), 1)

    def test_lambdas_run_in_private_subnets_with_compute_security_group(self):
        template = synth_template_json()
        expected_subnet_ids = private_subnet_refs(template)
        compute_security_group_id = logical_id_for_security_group(
            template,
            "Security group shared by the Lambda functions",
        )

        lambda_functions = lambda_functions_by_name(template)
        for function_name in FUNCTION_NAMES.values():
            self.assertEqual(
                lambda_functions[function_name]["VpcConfig"]["SecurityGroupIds"],
                [{"Fn::GetAtt": [compute_security_group_id, "GroupId"]}],
            )
            self.assertEqual(
                lambda_functions[function_name]["VpcConfig"]["SubnetIds"],
                expected_subnet_ids,
            )

    def test_database_security_group_only_allows_postgres_from_compute_group(self):
        template = synth_template_json()
        database_security_group_id = logical_id_for_security_group(
            template,
            "Security group for the PostgreSQL instance",
        )
        compute_security_group_id = logical_id_for_security_group(
            template,
            "Security group shared by the Lambda functions",
        )

        db_instance = next(iter(resource_properties_of_type(template, "AWS::RDS::DBInstance").values()))
        self.assertEqual(
            db_instance["VPCSecurityGroups"],
            [{"Fn::GetAtt": [database_security_group_id, "GroupId"]}],
        )

        ingress_resources = resources_of_type(template, "AWS::EC2::SecurityGroupIngress")
        self.assertEqual(len(ingress_resources), 1)
        ingress = next(iter(ingress_resources.values()))["Properties"]
        self.assertEqual(ingress["GroupId"], {"Fn::GetAtt": [database_security_group_id, "GroupId"]})
        self.assertEqual(ingress["SourceSecurityGroupId"], {"Fn::GetAtt": [compute_security_group_id, "GroupId"]})
        self.assertEqual(ingress["FromPort"], 5432)
        self.assertEqual(ingress["ToPort"], 5432)
        self.assertEqual(ingress["IpProtocol"], "tcp")

        database_security_group = resource_properties_of_type(template, "AWS::EC2::SecurityGroup")[
            database_security_group_id
        ]
        self.assertEqual(
            database_security_group["SecurityGroupEgress"],
            [
                {
                    "CidrIp": "255.255.255.255/32",
                    "Description": "Disallow all traffic",
                    "FromPort": 252,
                    "IpProtocol": "icmp",
                    "ToPort": 86,
                }
            ],
        )

    def test_rds_subnet_group_uses_only_private_subnets(self):
        template = synth_template_json()
        _, subnet_group = only_resource_of_type(template, "AWS::RDS::DBSubnetGroup")
        self.assertEqual(subnet_group["SubnetIds"], private_subnet_refs(template))


class TestApplicationResources(unittest.TestCase):
    def test_no_resources_use_retain_policies_or_termination_protection(self):
        template = synth_template_json()

        for resource in template["Resources"].values():
            self.assertNotEqual(resource.get("DeletionPolicy"), "Retain")
            self.assertNotEqual(resource.get("UpdateReplacePolicy"), "Retain")
            properties = resource.get("Properties", {})
            self.assertFalse(properties.get("DeletionProtection", False))
            self.assertFalse(properties.get("DisableApiTermination", False))

    def test_bucket_blocks_public_access_and_uses_s3_managed_encryption(self):
        template = synth_template()
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
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            },
        )

    def test_api_gateway_exposes_only_post_events_with_expected_access_logs(self):
        template = synth_template()
        template_json = template.to_json()
        access_log_group_id = logical_id_for_log_group(template_json, LOG_GROUP_NAMES["api_access"])

        resources = list(resource_properties_of_type(template_json, "AWS::ApiGateway::Resource").values())
        methods = list(resource_properties_of_type(template_json, "AWS::ApiGateway::Method").values())
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0]["PathPart"], "events")
        self.assertEqual(len(methods), 1)
        self.assertEqual(methods[0]["HttpMethod"], "POST")

        template.has_resource_properties(
            "AWS::ApiGateway::Stage",
            {
                "StageName": "prod",
                "AccessLogSetting": {
                    "DestinationArn": {"Fn::GetAtt": [access_log_group_id, "Arn"]},
                    "Format": EXPECTED_ACCESS_LOG_FORMAT,
                },
            },
        )

    def test_lambda_runtime_configuration_and_environment_keys_are_minimal(self):
        template = synth_template_json()
        functions = lambda_functions_by_name(template)

        api_handler = functions[FUNCTION_NAMES["api"]]
        worker = functions[FUNCTION_NAMES["worker"]]

        for function in functions.values():
            self.assertEqual(function["Runtime"], "python3.12")
            self.assertIn("ZipFile", function["Code"])
            for key in function["Environment"]["Variables"]:
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )

        self.assertEqual(api_handler["MemorySize"], 256)
        self.assertEqual(api_handler["Timeout"], 10)
        self.assertEqual(list(api_handler["Environment"]["Variables"].keys()), ["QUEUE_URL"])
        self.assertEqual(worker["MemorySize"], 256)
        self.assertEqual(worker["Timeout"], 20)
        self.assertEqual(list(worker["Environment"]["Variables"].keys()), ["DB_CREDENTIALS_ARN"])

    def test_all_application_log_groups_use_14_day_retention(self):
        template = synth_template()
        for log_group_name in LOG_GROUP_NAMES.values():
            template.has_resource_properties(
                "AWS::Logs::LogGroup",
                {
                    "LogGroupName": log_group_name,
                    "RetentionInDays": 14,
                },
            )

    def test_database_uses_secrets_manager_credentials_and_secure_storage(self):
        template = synth_template()
        template_json = template.to_json()

        template.has_resource_properties(
            "AWS::SecretsManager::Secret",
            {
                "GenerateSecretString": Match.object_like(
                    {
                        "SecretStringTemplate": '{"username":"notifications_admin"}',
                        "GenerateStringKey": "password",
                    }
                )
            },
        )
        template.has_resource_properties(
            "AWS::RDS::DBInstance",
            {
                "StorageEncrypted": True,
                "PubliclyAccessible": False,
                "DeletionProtection": False,
            },
        )

        _, db_instance = only_resource_of_type(template_json, "AWS::RDS::DBInstance")
        self.assertEqual(db_instance["DBInstanceClass"], "db.t3.micro")
        self.assertEqual(db_instance["AllocatedStorage"], "20")
        self.assertEqual(db_instance["Engine"], "postgres")
        self.assertEqual(db_instance["EngineVersion"], "16")
        self.assertEqual(db_instance["BackupRetentionPeriod"], 1)
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUsername"]))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUserPassword"]))

    def test_no_inline_secrets_exist_in_source_or_lambda_environment(self):
        source = APP_PATH.read_text()
        template = synth_template_json()

        forbidden_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'secret(_key|_access_key)?\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
        ]
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, source, flags=re.IGNORECASE))

        for function in resource_properties_of_type(template, "AWS::Lambda::Function").values():
            for key, value in function["Environment"]["Variables"].items():
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )
                self.assertNotRegex(str(value), r"(?i)(password|secret|token)\s*[:=]\s*.+")

    def test_queue_uses_managed_encryption_and_expected_operational_settings(self):
        template = synth_template_json()
        _, queue = only_resource_of_type(template, "AWS::SQS::Queue")

        self.assertTrue(queue["SqsManagedSseEnabled"])
        self.assertEqual(queue["VisibilityTimeout"], 30)
        self.assertEqual(queue["MessageRetentionPeriod"], FOUR_DAYS_IN_SECONDS)

    def test_cloudwatch_alarms_target_api_errors_and_queue_backlog(self):
        template = synth_template_json()
        alarms = list(resource_properties_of_type(template, "AWS::CloudWatch::Alarm").values())
        api_alarm = next(alarm for alarm in alarms if alarm["MetricName"] == "Errors")
        queue_alarm = next(
            alarm
            for alarm in alarms
            if alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"
        )

        self.assertEqual(api_alarm["Namespace"], "AWS/Lambda")
        self.assertEqual(api_alarm["Threshold"], 1)
        self.assertEqual(api_alarm["EvaluationPeriods"], 1)
        self.assertEqual(api_alarm["TreatMissingData"], "notBreaching")

        self.assertEqual(queue_alarm["Namespace"], "AWS/SQS")
        self.assertEqual(queue_alarm["Threshold"], 10)
        self.assertEqual(queue_alarm["EvaluationPeriods"], 1)
        self.assertEqual(queue_alarm["TreatMissingData"], "notBreaching")

class TestIamAndWorkflow(unittest.TestCase):
    def test_api_handler_permissions_are_scoped_to_queue_and_own_log_group(self):
        template = synth_template_json()
        api_role = iam_role_with_policy_name(template, "ApiHandlerPermissions")
        queue_logical_id = logical_id_for_resource_type(template, "AWS::SQS::Queue")
        api_log_group_id = logical_id_for_log_group(template, LOG_GROUP_NAMES["api"])
        statements = api_role["Policies"][0]["PolicyDocument"]["Statement"]

        self.assertEqual(len(statements), 2)
        sqs_statement = next(
            statement for statement in statements if statement["Action"] == "sqs:SendMessage"
        )
        logs_statement = next(
            statement
            for statement in statements
            if statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"]
        )

        self.assertEqual(sqs_statement["Resource"], {"Fn::GetAtt": [queue_logical_id, "Arn"]})
        self.assertEqual(logs_statement["Resource"], log_stream_resource(api_log_group_id))

    def test_worker_permissions_are_scoped_to_secret_and_own_log_group(self):
        template = synth_template_json()
        worker_role = iam_role_with_policy_name(template, "WorkerPermissions")
        worker_log_group_id = logical_id_for_log_group(template, LOG_GROUP_NAMES["worker"])
        worker_statements = worker_role["Policies"][0]["PolicyDocument"]["Statement"]

        self.assertEqual(len(worker_statements), 1)
        self.assertEqual(
            worker_statements[0]["Action"],
            ["logs:CreateLogStream", "logs:PutLogEvents"],
        )
        self.assertEqual(worker_statements[0]["Resource"], log_stream_resource(worker_log_group_id))

        secret_policy = next(
            properties
            for properties in resource_properties_of_type(template, "AWS::IAM::Policy").values()
            if properties["PolicyDocument"]["Statement"][0]["Action"]
            == "secretsmanager:GetSecretValue"
        )
        secret_statement = secret_policy["PolicyDocument"]["Statement"][0]
        self.assertEqual(secret_statement["Resource"], {"Ref": logical_id_for_resource_type(template, "AWS::SecretsManager::Secret")})

    def test_state_machine_invokes_worker_and_logs_all_execution_data(self):
        template = synth_template()
        template_json = template.to_json()
        state_machine_log_group_id = logical_id_for_log_group(
            template_json,
            LOG_GROUP_NAMES["state_machine"],
        )
        worker_function_id = logical_id_for_function(template_json, FUNCTION_NAMES["worker"])

        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {
                "StateMachineType": "STANDARD",
                "LoggingConfiguration": {
                    "IncludeExecutionData": True,
                    "Level": "ALL",
                    "Destinations": [
                        {
                            "CloudWatchLogsLogGroup": {
                                "LogGroupArn": {
                                    "Fn::GetAtt": [state_machine_log_group_id, "Arn"]
                                }
                            }
                        }
                    ],
                },
            },
        )

        _, state_machine = only_resource_of_type(template_json, "AWS::StepFunctions::StateMachine")
        definition_string = str(state_machine["DefinitionString"])
        self.assertIn("ProcessNotificationTask", definition_string)
        self.assertIn(worker_function_id, definition_string)

    def test_pipe_is_wired_from_queue_through_enrichment_to_state_machine(self):
        template = synth_template()
        template_json = template.to_json()
        queue_logical_id = logical_id_for_resource_type(template_json, "AWS::SQS::Queue")
        api_handler_function_id = logical_id_for_function(template_json, FUNCTION_NAMES["api"])
        state_machine_id = logical_id_for_resource_type(template_json, "AWS::StepFunctions::StateMachine")

        pipe_role = iam_role_with_policy_name(template_json, "PipePermissions")
        statements = pipe_role["Policies"][0]["PolicyDocument"]["Statement"]
        sqs_statement = next(statement for statement in statements if "sqs:ReceiveMessage" in statement["Action"])
        lambda_statement = next(
            statement for statement in statements if statement["Action"] == "lambda:InvokeFunction"
        )
        states_statement = next(
            statement for statement in statements if statement["Action"] == "states:StartExecution"
        )

        self.assertEqual(
            sqs_statement["Action"],
            [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility",
            ],
        )
        self.assertEqual(sqs_statement["Resource"], {"Fn::GetAtt": [queue_logical_id, "Arn"]})
        self.assertEqual(lambda_statement["Resource"], {"Fn::GetAtt": [api_handler_function_id, "Arn"]})
        self.assertEqual(states_statement["Resource"], {"Ref": state_machine_id})

        template.has_resource_properties(
            "AWS::Pipes::Pipe",
            {
                "DesiredState": "RUNNING",
                "Source": {"Fn::GetAtt": [queue_logical_id, "Arn"]},
                "Enrichment": {"Fn::GetAtt": [api_handler_function_id, "Arn"]},
                "Target": {"Ref": state_machine_id},
                "SourceParameters": {
                    "SqsQueueParameters": {"BatchSize": 10},
                },
                "TargetParameters": {
                    "StepFunctionStateMachineParameters": {
                        "InvocationType": "FIRE_AND_FORGET"
                    }
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
