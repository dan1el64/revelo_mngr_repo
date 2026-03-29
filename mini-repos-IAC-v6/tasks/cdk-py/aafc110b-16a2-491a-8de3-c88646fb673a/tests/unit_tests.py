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


def load_app_module():
    spec = spec_from_file_location("solution_app", APP_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def synth_template() -> Template:
    with patch.dict(
        os.environ,
        {
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": "https://api.aws.amazon.com",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        },
        clear=False,
    ):
        module = load_app_module()
        app = App()
        stack = module.SecureNotificationStack(app, "UnitTestStack")
        return Template.from_stack(stack)


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
            os.environ.pop("AWS_ENDPOINT_URL", None)

            module.configure_aws_endpoint_environment()

            self.assertEqual(os.environ["AWS_ENDPOINT_URL"], "https://example.test")

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
        template = synth_template().to_json()

        subnets = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::EC2::Subnet"
        ]
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

        nat_gateways = [
            resource
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::EC2::NatGateway"
        ]
        self.assertEqual(len(nat_gateways), 1)

    def test_security_groups_are_exact_and_assigned_correctly(self):
        template = synth_template().to_json()

        security_groups = {
            logical_id: resource["Properties"]
            for logical_id, resource in template["Resources"].items()
            if resource["Type"] == "AWS::EC2::SecurityGroup"
        }
        self.assertEqual(len(security_groups), 2)
        self.assertIn("ComputeSecurityGroupF0F5C976", security_groups)
        self.assertIn("DatabaseSecurityGroup7319C0F6", security_groups)

        lambda_functions = {
            resource["Properties"]["FunctionName"]: resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Lambda::Function"
        }
        for function_name in [
            "secure-notification-api-handler",
            "secure-notification-worker",
        ]:
            self.assertEqual(
                lambda_functions[function_name]["VpcConfig"]["SecurityGroupIds"],
                [{"Fn::GetAtt": ["ComputeSecurityGroupF0F5C976", "GroupId"]}],
            )

        db_instance = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::RDS::DBInstance"
        )
        self.assertEqual(
            db_instance["VPCSecurityGroups"],
            [{"Fn::GetAtt": ["DatabaseSecurityGroup7319C0F6", "GroupId"]}],
        )

    def test_both_lambdas_are_placed_in_vpc_private_subnets_with_compute_security_group(self):
        template = synth_template().to_json()

        expected_vpc_config = {
            "SecurityGroupIds": [{"Fn::GetAtt": ["ComputeSecurityGroupF0F5C976", "GroupId"]}],
            "SubnetIds": [
                {"Ref": "NotificationVpcPrivateWithEgressSubnet1SubnetC72AD487"},
                {"Ref": "NotificationVpcPrivateWithEgressSubnet2Subnet50E991CB"},
            ],
        }

        lambda_functions = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Lambda::Function"
        ]
        self.assertEqual(len(lambda_functions), 2)
        for function in lambda_functions:
            self.assertEqual(function["VpcConfig"], expected_vpc_config)

    def test_api_gateway_structure_is_exact(self):
        template = synth_template().to_json()

        rest_apis = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::ApiGateway::RestApi"
        ]
        resources = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::ApiGateway::Resource"
        ]
        methods = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::ApiGateway::Method"
        ]

        self.assertEqual(len(rest_apis), 1)
        self.assertEqual(len(resources), 1)
        self.assertEqual(len(methods), 1)
        self.assertEqual(resources[0]["PathPart"], "events")
        self.assertEqual(methods[0]["HttpMethod"], "POST")

    def test_no_retain_policies_or_deletion_protection(self):
        template = synth_template().to_json()

        for resource in template["Resources"].values():
            self.assertNotEqual(resource.get("DeletionPolicy"), "Retain")
            self.assertNotEqual(resource.get("UpdateReplacePolicy"), "Retain")

        db_instances = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::RDS::DBInstance"
        ]
        self.assertEqual(len(db_instances), 1)
        self.assertFalse(db_instances[0]["DeletionProtection"])

    def test_lambda_runtime_and_packaging_requirements(self):
        template = synth_template().to_json()

        lambda_functions = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Lambda::Function"
        ]
        self.assertEqual(len(lambda_functions), 2)

        for function in lambda_functions:
            self.assertEqual(function["Runtime"], "python3.12")
            self.assertIn("ZipFile", function["Code"])
            self.assertNotIn("ImageUri", function["Code"])
            self.assertNotEqual(function.get("PackageType"), "Image")

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

        db_instance = next(
            resource["Properties"]
            for resource in template.to_json()["Resources"].values()
            if resource["Type"] == "AWS::RDS::DBInstance"
        )
        self.assertEqual(db_instance["DBInstanceClass"], "db.t3.micro")
        self.assertEqual(db_instance["AllocatedStorage"], "20")
        self.assertEqual(db_instance["Engine"], "postgres")
        self.assertEqual(db_instance["EngineVersion"], "16")
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUsername"]))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUserPassword"]))
        self.assertEqual(db_instance["BackupRetentionPeriod"], 1)

    def test_no_inline_secrets_outside_secrets_manager(self):
        source = APP_PATH.read_text()
        template = synth_template().to_json()

        forbidden_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'secret(_key|_access_key)?\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
        ]
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, source, flags=re.IGNORECASE))

        db_instance = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::RDS::DBInstance"
        )
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUsername"]))
        self.assertIn("resolve:secretsmanager", str(db_instance["MasterUserPassword"]))

        lambda_functions = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Lambda::Function"
        ]
        for function in lambda_functions:
            env_vars = function["Environment"]["Variables"]
            self.assertNotIn("PASSWORD", env_vars)
            self.assertNotIn("DB_PASSWORD", env_vars)
            self.assertNotIn("SECRET", env_vars)
            self.assertNotIn("TOKEN", env_vars)

    def test_rds_subnet_group_uses_only_private_with_egress_subnets(self):
        template = synth_template().to_json()

        subnet_group = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::RDS::DBSubnetGroup"
        )
        self.assertEqual(
            subnet_group["SubnetIds"],
            [
                {"Ref": "NotificationVpcPrivateWithEgressSubnet1SubnetC72AD487"},
                {"Ref": "NotificationVpcPrivateWithEgressSubnet2Subnet50E991CB"},
            ],
        )

    def test_queue_and_alarm_requirements(self):
        template = synth_template().to_json()

        queue = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::SQS::Queue"
        )
        self.assertTrue(queue["SqsManagedSseEnabled"])
        self.assertEqual(queue["VisibilityTimeout"], 30)
        self.assertEqual(queue["MessageRetentionPeriod"], 345600)

        alarms = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::CloudWatch::Alarm"
        ]
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
            [{"Name": "FunctionName", "Value": {"Ref": "ApiHandlerFunction9E589C02"}}],
        )

        self.assertEqual(queue_alarm["Namespace"], "AWS/SQS")
        self.assertEqual(queue_alarm["Period"], 60)
        self.assertEqual(queue_alarm["Threshold"], 10)
        self.assertEqual(queue_alarm["EvaluationPeriods"], 1)

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
        template = synth_template().to_json()

        api_role = template["Resources"]["ApiHandlerRole7074C5BF"]["Properties"]
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
            {"Fn::GetAtt": ["NotificationQueue36610CC1", "Arn"]},
        )
        self.assertEqual(
            logs_statement["Resource"],
            [
                {"Fn::GetAtt": ["ApiHandlerLogGroup4D57C896", "Arn"]},
                {
                    "Fn::Join": [
                        "",
                        [{"Fn::GetAtt": ["ApiHandlerLogGroup4D57C896", "Arn"]}, ":*"],
                    ]
                },
            ],
        )

    def test_worker_permissions_are_scoped_to_queue_secret_and_own_log_group(self):
        template = synth_template().to_json()

        worker_role = template["Resources"]["WorkerRole8DD27D41"]["Properties"]
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
            {"Fn::GetAtt": ["NotificationQueue36610CC1", "Arn"]},
        )
        self.assertEqual(
            logs_statement["Resource"],
            [
                {"Fn::GetAtt": ["WorkerLogGroup31FDBE4A", "Arn"]},
                {
                    "Fn::Join": [
                        "",
                        [{"Fn::GetAtt": ["WorkerLogGroup31FDBE4A", "Arn"]}, ":*"],
                    ]
                },
            ],
        )

        secret_policy = template["Resources"]["WorkerRoleDefaultPolicy1750E153"]["Properties"]
        secret_statement = secret_policy["PolicyDocument"]["Statement"][0]
        self.assertEqual(secret_statement["Action"], "secretsmanager:GetSecretValue")
        self.assertEqual(secret_statement["Resource"], {"Ref": "DatabaseSecret86DBB7B3"})

    def test_state_machine_and_pipe_are_wired_correctly(self):
        template = synth_template()
        template_json = template.to_json()

        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {
                "StateMachineType": "STANDARD",
                "LoggingConfiguration": {
                    "IncludeExecutionData": True,
                    "Level": "ALL",
                    "Destinations": Match.any_value(),
                },
            },
        )
        template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/aws/vendedlogs/states/secure-notification-state-machine",
                "RetentionInDays": 14,
            },
        )
        state_machine = next(
            resource["Properties"]
            for resource in template_json["Resources"].values()
            if resource["Type"] == "AWS::StepFunctions::StateMachine"
        )
        self.assertEqual(
            state_machine["LoggingConfiguration"]["Destinations"],
            [
                {
                    "CloudWatchLogsLogGroup": {
                        "LogGroupArn": {
                            "Fn::GetAtt": ["StateMachineLogGroup15B91BCB", "Arn"]
                        }
                    }
                }
            ],
        )
        definition_string = str(state_machine["DefinitionString"])
        self.assertIn("ProcessNotificationTask", definition_string)
        self.assertIn("WorkerFunctionACE6A4B0", definition_string)
        self.assertIn("StartAt", definition_string)
        self.assertIn("End", definition_string)
        self.assertIn("Type", definition_string)
        self.assertIn("Task", definition_string)
        self.assertEqual(
            state_machine["DefinitionString"]["Fn::Join"][1][1],
            {"Fn::GetAtt": ["WorkerFunctionACE6A4B0", "Arn"]},
        )

    def test_pipe_role_permissions_are_scoped_to_queue_lambda_and_state_machine(self):
        template_obj = synth_template()
        template = template_obj.to_json()

        pipe_role = template["Resources"]["PipeRole4D7B8476"]["Properties"]
        statements = pipe_role["Policies"][0]["PolicyDocument"]["Statement"]

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
            {"Fn::GetAtt": ["NotificationQueue36610CC1", "Arn"]},
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
            {"Fn::GetAtt": ["ApiHandlerFunction9E589C02", "Arn"]},
        )
        self.assertEqual(lambda_statement["Action"], "lambda:InvokeFunction")
        self.assertEqual(
            states_statement["Resource"],
            {"Ref": "NotificationStateMachine3933CE78"},
        )
        self.assertEqual(states_statement["Action"], "states:StartExecution")
        self.assertEqual(len(statements), 3)
        template_obj.has_resource_properties(
            "AWS::Pipes::Pipe",
            {
                "DesiredState": "RUNNING",
                "Source": Match.any_value(),
                "Enrichment": Match.any_value(),
                "Target": Match.any_value(),
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
        pipe = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Pipes::Pipe"
        )
        self.assertEqual(
            pipe["Enrichment"],
            {"Fn::GetAtt": ["ApiHandlerFunction9E589C02", "Arn"]},
        )
        self.assertEqual(
            pipe["Target"],
            {"Ref": "NotificationStateMachine3933CE78"},
        )
        self.assertNotEqual(pipe["Target"], pipe["Enrichment"])

    def test_api_gateway_access_logging_uses_14_day_log_group(self):
        template = synth_template()

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
                    "DestinationArn": Match.any_value(),
                    "Format": Match.any_value(),
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
