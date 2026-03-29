import os
import re
import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import patch

from aws_cdk import App
from aws_cdk.assertions import Match, Template


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app.py"
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
EXPECTED_ACCESS_LOG_FORMAT = (
    '{"requestId":"$context.requestId","ip":"$context.identity.sourceIp",'
    '"user":"$context.identity.user","caller":"$context.identity.caller",'
    '"requestTime":"$context.requestTime","httpMethod":"$context.httpMethod",'
    '"resourcePath":"$context.resourcePath","status":"$context.status",'
    '"protocol":"$context.protocol","responseLength":"$context.responseLength"}'
)
FORBIDDEN_ENV_KEY_SUBSTRINGS = ("password", "secret", "token")


def load_app_module():
    spec = spec_from_file_location("solution_app", APP_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def synth_template() -> Template:
    with patch.dict(os.environ, SYNTH_ENV, clear=False):
        module = load_app_module()
        app = App()
        stack = module.SecureNotificationStack(app, "UnitTestStack")
        return Template.from_stack(stack)


def synth_template_json() -> dict:
    return synth_template().to_json()


def resources_of_type(template_json: dict, resource_type: str) -> dict:
    return {
        logical_id: resource["Properties"]
        for logical_id, resource in template_json["Resources"].items()
        if resource["Type"] == resource_type
    }


def lambda_functions_by_name(template_json: dict) -> dict:
    return {
        properties["FunctionName"]: properties
        for properties in resources_of_type(template_json, "AWS::Lambda::Function").values()
    }


def logical_id_for_log_group(template_json: dict, log_group_name: str) -> str:
    log_groups = resources_of_type(template_json, "AWS::Logs::LogGroup")
    return next(
        logical_id
        for logical_id, properties in log_groups.items()
        if properties["LogGroupName"] == log_group_name
    )


def logical_id_for_security_group(template_json: dict, description: str) -> str:
    security_groups = resources_of_type(template_json, "AWS::EC2::SecurityGroup")
    return next(
        logical_id
        for logical_id, properties in security_groups.items()
        if properties["GroupDescription"] == description
    )


def logical_id_for_function(template_json: dict, function_name: str) -> str:
    functions = resources_of_type(template_json, "AWS::Lambda::Function")
    return next(
        logical_id
        for logical_id, properties in functions.items()
        if properties["FunctionName"] == function_name
    )


def logical_id_for_resource_type(template_json: dict, resource_type: str) -> str:
    return next(iter(resources_of_type(template_json, resource_type)))


def private_subnet_refs(template_json: dict) -> list[dict]:
    subnets = resources_of_type(template_json, "AWS::EC2::Subnet")
    private_ids = [
        logical_id
        for logical_id, properties in subnets.items()
        if not properties["MapPublicIpOnLaunch"]
    ]
    return [{"Ref": logical_id} for logical_id in sorted(private_ids)]


def iam_role_with_policy_name(template_json: dict, policy_name: str) -> dict:
    roles = resources_of_type(template_json, "AWS::IAM::Role")
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


class TestSecureNotificationStack(unittest.TestCase):
    def test_deliverable_is_single_root_app_py_file_and_self_contained(self):
        root_python_files = sorted(path.name for path in ROOT.glob("*.py"))
        self.assertEqual(root_python_files, ["app.py"])

        source = APP_PATH.read_text()
        self.assertNotIn("Code.from_asset(", source)
        self.assertIn("_lambda.Code.from_inline(", source)
        self.assertNotIn('code=_lambda.Code.from_asset("', source)
        self.assertNotIn("Code.from_asset('", source)
        self.assertNotIn("requirements-dev.txt", source)

    def test_aws_endpoint_configures_sdk_endpoint_environment(self):
        with patch.dict(os.environ, {"AWS_ENDPOINT": "https://example.test"}, clear=False):
            module = load_app_module()
            for name in SDK_ENDPOINT_ENV_VARS:
                os.environ.pop(name, None)

            configured_endpoint = module.configure_aws_endpoint_environment()

            self.assertEqual(configured_endpoint, "https://example.test")
            for name in SDK_ENDPOINT_ENV_VARS:
                self.assertEqual(os.environ[name], "https://example.test")

    def test_default_region_is_us_east_1_when_unset(self):
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

    def test_network_topology_is_exact(self):
        template = synth_template_json()
        subnets = list(resources_of_type(template, "AWS::EC2::Subnet").values())
        public_subnets = [subnet for subnet in subnets if subnet["MapPublicIpOnLaunch"]]
        private_subnets = [subnet for subnet in subnets if not subnet["MapPublicIpOnLaunch"]]

        self.assertEqual(len(subnets), 4)
        self.assertEqual(len(public_subnets), 2)
        self.assertEqual(len(private_subnets), 2)

        az_indexes = {
            subnet["AvailabilityZone"]["Fn::Select"][0]
            for subnet in subnets
            if isinstance(subnet.get("AvailabilityZone"), dict)
        }
        self.assertEqual(az_indexes, {0, 1})

        nat_gateways = resources_of_type(template, "AWS::EC2::NatGateway")
        self.assertEqual(len(nat_gateways), 1)

    def test_security_groups_are_exact_and_assigned_correctly(self):
        template = synth_template_json()

        security_groups = resources_of_type(template, "AWS::EC2::SecurityGroup")
        self.assertEqual(len(security_groups), 2)

        compute_security_group_id = logical_id_for_security_group(
            template,
            "Security group shared by the Lambda functions",
        )
        database_security_group_id = logical_id_for_security_group(
            template,
            "Security group for the PostgreSQL instance",
        )

        lambda_functions = lambda_functions_by_name(template)
        for function_name in [
            "secure-notification-api-handler",
            "secure-notification-worker",
        ]:
            self.assertEqual(
                lambda_functions[function_name]["VpcConfig"]["SecurityGroupIds"],
                [{"Fn::GetAtt": [compute_security_group_id, "GroupId"]}],
            )

        db_instance = next(iter(resources_of_type(template, "AWS::RDS::DBInstance").values()))
        self.assertEqual(
            db_instance["VPCSecurityGroups"],
            [{"Fn::GetAtt": [database_security_group_id, "GroupId"]}],
        )

    def test_both_lambdas_are_placed_in_vpc_private_subnets_with_compute_security_group(self):
        template = synth_template_json()
        compute_security_group_id = logical_id_for_security_group(
            template,
            "Security group shared by the Lambda functions",
        )
        expected_subnet_ids = private_subnet_refs(template)

        lambda_functions = resources_of_type(template, "AWS::Lambda::Function")
        self.assertEqual(len(lambda_functions), 2)
        for function in lambda_functions.values():
            self.assertEqual(
                function["VpcConfig"]["SecurityGroupIds"],
                [{"Fn::GetAtt": [compute_security_group_id, "GroupId"]}],
            )
            self.assertEqual(function["VpcConfig"]["SubnetIds"], expected_subnet_ids)

    def test_api_gateway_structure_is_exact(self):
        template = synth_template_json()

        rest_apis = resources_of_type(template, "AWS::ApiGateway::RestApi")
        resources = list(resources_of_type(template, "AWS::ApiGateway::Resource").values())
        methods = list(resources_of_type(template, "AWS::ApiGateway::Method").values())

        self.assertEqual(len(rest_apis), 1)
        self.assertEqual(len(resources), 1)
        self.assertEqual(len(methods), 1)
        self.assertEqual(resources[0]["PathPart"], "events")
        self.assertEqual(methods[0]["HttpMethod"], "POST")

    def test_no_retain_policies_or_deletion_protection(self):
        template = synth_template_json()

        for resource in template["Resources"].values():
            self.assertNotEqual(resource.get("DeletionPolicy"), "Retain")
            self.assertNotEqual(resource.get("UpdateReplacePolicy"), "Retain")

        db_instances = list(resources_of_type(template, "AWS::RDS::DBInstance").values())
        self.assertEqual(len(db_instances), 1)
        self.assertFalse(db_instances[0]["DeletionProtection"])

    def test_artifacts_bucket_blocks_public_access_and_uses_s3_managed_encryption(self):
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

    def test_lambda_runtime_and_packaging_requirements(self):
        template = synth_template_json()
        lambda_functions = list(resources_of_type(template, "AWS::Lambda::Function").values())
        self.assertEqual(len(lambda_functions), 2)

        for function in lambda_functions:
            self.assertEqual(function["Runtime"], "python3.12")
            self.assertIn("ZipFile", function["Code"])
            self.assertNotIn("ImageUri", function["Code"])
            self.assertNotEqual(function.get("PackageType"), "Image")
            for key in function["Environment"]["Variables"]:
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )

        api_handler = next(
            function
            for function in lambda_functions
            if function["FunctionName"] == "secure-notification-api-handler"
        )
        worker = next(
            function
            for function in lambda_functions
            if function["FunctionName"] == "secure-notification-worker"
        )

        self.assertEqual(api_handler["MemorySize"], 256)
        self.assertEqual(api_handler["Timeout"], 10)
        self.assertEqual(list(api_handler["Environment"]["Variables"].keys()), ["QUEUE_URL"])
        self.assertEqual(worker["MemorySize"], 256)
        self.assertEqual(worker["Timeout"], 20)
        self.assertEqual(
            list(worker["Environment"]["Variables"].keys()),
            ["DB_CREDENTIALS_ARN"],
        )

    def test_lambda_log_groups_have_14_day_retention(self):
        template = synth_template()

        template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/aws/lambda/secure-notification-api-handler",
                "RetentionInDays": 14,
            },
        )
        template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/aws/lambda/secure-notification-worker",
                "RetentionInDays": 14,
            },
        )

    def test_state_machine_log_group_has_14_day_retention(self):
        template = synth_template()

        template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/aws/vendedlogs/states/secure-notification-state-machine",
                "RetentionInDays": 14,
            },
        )

    def test_database_uses_secret_for_master_credentials(self):
        template = synth_template()

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
            },
        )

        db_instance = next(iter(resources_of_type(template.to_json(), "AWS::RDS::DBInstance").values()))
        self.assertEqual(db_instance["DBInstanceClass"], "db.t3.micro")
        self.assertEqual(db_instance["AllocatedStorage"], "20")
        self.assertEqual(db_instance["Engine"], "postgres")
        self.assertEqual(db_instance["EngineVersion"], "16")
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUsername"]))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUserPassword"]))
        self.assertEqual(db_instance["BackupRetentionPeriod"], 1)

    def test_no_inline_secrets_outside_secrets_manager(self):
        source = APP_PATH.read_text()
        template = synth_template_json()

        forbidden_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'secret(_key|_access_key)?\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
        ]
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, source, flags=re.IGNORECASE))

        db_instance = next(iter(resources_of_type(template, "AWS::RDS::DBInstance").values()))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUsername"]))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUserPassword"]))

        lambda_functions = resources_of_type(template, "AWS::Lambda::Function")
        for function in lambda_functions.values():
            env_vars = function["Environment"]["Variables"]
            for key, value in env_vars.items():
                lowered_key = key.lower()
                self.assertFalse(
                    any(fragment in lowered_key for fragment in FORBIDDEN_ENV_KEY_SUBSTRINGS)
                )
                self.assertNotRegex(str(value), r"(?i)(password|secret|token)\s*[:=]\s*.+")

    def test_rds_subnet_group_uses_only_private_with_egress_subnets(self):
        template = synth_template_json()
        subnet_group = next(iter(resources_of_type(template, "AWS::RDS::DBSubnetGroup").values()))
        self.assertEqual(subnet_group["SubnetIds"], private_subnet_refs(template))

    def test_queue_and_alarm_requirements(self):
        template = synth_template_json()

        queue_logical_id = logical_id_for_resource_type(template, "AWS::SQS::Queue")
        queue = next(iter(resources_of_type(template, "AWS::SQS::Queue").values()))
        self.assertTrue(queue["SqsManagedSseEnabled"])
        self.assertEqual(queue["VisibilityTimeout"], 30)
        self.assertEqual(queue["MessageRetentionPeriod"], 345600)

        alarms = list(resources_of_type(template, "AWS::CloudWatch::Alarm").values())
        api_alarm = next(alarm for alarm in alarms if alarm["MetricName"] == "Errors")
        queue_alarm = next(
            alarm
            for alarm in alarms
            if alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"
        )

        self.assertEqual(api_alarm["Namespace"], "AWS/Lambda")
        self.assertEqual(api_alarm["Period"], 60)
        self.assertEqual(api_alarm["Threshold"], 1)
        self.assertEqual(api_alarm["EvaluationPeriods"], 1)
        self.assertEqual(
            api_alarm["Dimensions"],
            [
                {
                    "Name": "FunctionName",
                    "Value": {"Ref": logical_id_for_function(template, "secure-notification-api-handler")},
                }
            ],
        )

        self.assertEqual(queue_alarm["Namespace"], "AWS/SQS")
        self.assertEqual(queue_alarm["Period"], 60)
        self.assertEqual(queue_alarm["Threshold"], 10)
        self.assertEqual(queue_alarm["EvaluationPeriods"], 1)
        self.assertEqual(
            queue_alarm["Dimensions"],
            [
                {
                    "Name": "QueueName",
                    "Value": {"Fn::GetAtt": [queue_logical_id, "QueueName"]},
                }
            ],
        )

    def test_worker_event_source_mapping_uses_sqs_batch_size_10(self):
        template = synth_template()

        template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {
                "BatchSize": 10,
                "EventSourceArn": Match.any_value(),
                "FunctionName": Match.any_value(),
            },
        )

    def test_api_handler_permissions_are_scoped_to_queue_and_own_log_group(self):
        template = synth_template_json()
        api_role = iam_role_with_policy_name(template, "ApiHandlerPermissions")
        queue_logical_id = logical_id_for_resource_type(template, "AWS::SQS::Queue")
        api_log_group_id = logical_id_for_log_group(
            template,
            "/aws/lambda/secure-notification-api-handler",
        )
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

        self.assertEqual(
            sqs_statement["Resource"],
            {"Fn::GetAtt": [queue_logical_id, "Arn"]},
        )
        self.assertEqual(logs_statement["Resource"], log_stream_resource(api_log_group_id))

    def test_worker_permissions_are_scoped_to_queue_secret_and_own_log_group(self):
        template = synth_template_json()
        worker_role = iam_role_with_policy_name(template, "WorkerPermissions")
        queue_logical_id = logical_id_for_resource_type(template, "AWS::SQS::Queue")
        worker_log_group_id = logical_id_for_log_group(
            template,
            "/aws/lambda/secure-notification-worker",
        )
        worker_statements = worker_role["Policies"][0]["PolicyDocument"]["Statement"]
        sqs_statement = next(
            statement
            for statement in worker_statements
            if statement["Action"]
            == ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        )
        logs_statement = next(
            statement
            for statement in worker_statements
            if statement["Action"] == ["logs:CreateLogStream", "logs:PutLogEvents"]
        )

        self.assertEqual(
            sqs_statement["Resource"],
            {"Fn::GetAtt": [queue_logical_id, "Arn"]},
        )
        self.assertEqual(logs_statement["Resource"], log_stream_resource(worker_log_group_id))

        secret_policy = next(
            properties
            for properties in resources_of_type(template, "AWS::IAM::Policy").values()
            if properties["PolicyDocument"]["Statement"][0]["Action"]
            == "secretsmanager:GetSecretValue"
        )
        secret_statement = secret_policy["PolicyDocument"]["Statement"][0]
        self.assertEqual(secret_statement["Action"], "secretsmanager:GetSecretValue")
        self.assertEqual(
            secret_statement["Resource"],
            {"Ref": logical_id_for_resource_type(template, "AWS::SecretsManager::Secret")},
        )

    def test_state_machine_and_pipe_are_wired_correctly(self):
        template_obj = synth_template()
        template = template_obj.to_json()
        worker_function_id = logical_id_for_function(template, "secure-notification-worker")
        state_machine_log_group_id = logical_id_for_log_group(
            template,
            "/aws/vendedlogs/states/secure-notification-state-machine",
        )
        state_machine_id = logical_id_for_resource_type(template, "AWS::StepFunctions::StateMachine")

        template_obj.has_resource_properties(
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

        state_machine = next(iter(resources_of_type(template, "AWS::StepFunctions::StateMachine").values()))
        definition_string = str(state_machine["DefinitionString"])
        self.assertIn("ProcessNotificationTask", definition_string)
        self.assertIn("StartAt", definition_string)
        self.assertIn("End", definition_string)
        self.assertIn("Type", definition_string)
        self.assertIn("Task", definition_string)
        self.assertIn(worker_function_id, definition_string)
        self.assertIn(
            {"Fn::GetAtt": [worker_function_id, "Arn"]},
            state_machine["DefinitionString"]["Fn::Join"][1],
        )

        pipe = next(iter(resources_of_type(template, "AWS::Pipes::Pipe").values()))
        self.assertEqual(pipe["Target"], {"Ref": state_machine_id})

    def test_pipe_role_permissions_are_scoped_to_queue_lambda_and_state_machine(self):
        template_obj = synth_template()
        template = template_obj.to_json()
        pipe_role = iam_role_with_policy_name(template, "PipePermissions")
        statements = pipe_role["Policies"][0]["PolicyDocument"]["Statement"]
        queue_logical_id = logical_id_for_resource_type(template, "AWS::SQS::Queue")
        api_handler_function_id = logical_id_for_function(
            template,
            "secure-notification-api-handler",
        )
        state_machine_id = logical_id_for_resource_type(template, "AWS::StepFunctions::StateMachine")

        sqs_statement = next(
            statement
            for statement in statements
            if "sqs:ReceiveMessage" in statement["Action"]
        )
        lambda_statement = next(
            statement for statement in statements if statement["Action"] == "lambda:InvokeFunction"
        )
        states_statement = next(
            statement for statement in statements if statement["Action"] == "states:StartExecution"
        )

        self.assertEqual(
            sqs_statement["Resource"],
            {"Fn::GetAtt": [queue_logical_id, "Arn"]},
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
        self.assertEqual(
            lambda_statement["Resource"],
            {"Fn::GetAtt": [api_handler_function_id, "Arn"]},
        )
        self.assertEqual(lambda_statement["Action"], "lambda:InvokeFunction")
        self.assertEqual(states_statement["Resource"], {"Ref": state_machine_id})
        self.assertEqual(states_statement["Action"], "states:StartExecution")
        self.assertEqual(len(statements), 3)

        template_obj.has_resource_properties(
            "AWS::Pipes::Pipe",
            {
                "DesiredState": "RUNNING",
                "Source": Match.any_value(),
                "Enrichment": Match.any_value(),
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

    def test_api_gateway_access_logging_uses_14_day_log_group_and_expected_format(self):
        template = synth_template()
        template_json = template.to_json()
        access_log_group_id = logical_id_for_log_group(
            template_json,
            "/aws/apigateway/secure-notification-access",
        )

        template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/aws/apigateway/secure-notification-access",
                "RetentionInDays": 14,
            },
        )
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


if __name__ == "__main__":
    unittest.main()
