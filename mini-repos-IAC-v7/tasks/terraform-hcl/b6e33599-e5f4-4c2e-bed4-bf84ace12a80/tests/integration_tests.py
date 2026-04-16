import json
import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path, terraform_args):
    if path.exists():
        return json.loads(path.read_text())

    result = subprocess.run(
        ["terraform", *terraform_args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    path.write_text(result.stdout)
    return json.loads(result.stdout)


def load_state():
    return _load_json(ROOT / "state.json", ["show", "-json"])


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def resources_by_address(resources):
    return {resource["address"]: resource for resource in resources}


def load_policy(resource):
    return json.loads(resource["values"]["policy"])


def normalize_actions(actions):
    if isinstance(actions, str):
        return [actions]
    return actions


class TestTerraformStateCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = load_state()
        cls.main_tf = (ROOT / "main.tf").read_text()
        cls.resources = list(iter_resources(cls.state["values"]["root_module"]))
        cls.resource_map = resources_by_address(cls.resources)

    def assertMainTfMatches(self, pattern):
        self.assertRegex(self.main_tf, pattern)

    def get_resource(self, address):
        resource = self.resource_map.get(address)
        if resource is None and not address.endswith("[0]"):
            resource = self.resource_map.get(f"{address}[0]")
        return resource

    def get_values(self, address):
        resource = self.get_resource(address)
        return resource["values"] if resource else None

    def assert_policy_allows_only_expected_star_resources(self, policy):
        allowed_star_action_sets = {
            frozenset(
                [
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses",
                ]
            ),
            frozenset(
                [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                ]
            ),
        }
        for statement in policy["Statement"]:
            if statement["Resource"] == "*":
                self.assertIn(frozenset(normalize_actions(statement["Action"])), allowed_star_action_sets)

    def test_deployed_resource_counts_and_topology(self):
        counts = {}
        for resource in self.resources:
            counts[resource["type"]] = counts.get(resource["type"], 0) + 1

        strict_counts = {
            "aws_vpc": 1,
            "aws_subnet": 4,
            "aws_internet_gateway": 1,
            "aws_route_table": 2,
            "aws_vpc_endpoint": 5,
            "aws_security_group": 3,
            "aws_sqs_queue": 1,
            "aws_s3_bucket": 1,
        }
        for resource_type, expected in strict_counts.items():
            self.assertEqual(counts.get(resource_type), expected, resource_type)

        self.assertEqual(counts.get("aws_nat_gateway", 0), 0)
        self.assertIn(counts.get("aws_iam_role", 0), (0, 4))
        for resource_type, expected in {
            "aws_lambda_function": 2,
            "aws_lambda_event_source_mapping": 1,
            "aws_sfn_state_machine": 1,
            "aws_secretsmanager_secret": 1,
            "aws_secretsmanager_secret_version": 1,
            "aws_api_gateway_rest_api": 1,
            "aws_api_gateway_resource": 1,
            "aws_api_gateway_method": 1,
            "aws_api_gateway_integration": 1,
            "aws_api_gateway_stage": 1,
            "aws_api_gateway_method_settings": 1,
        }.items():
            self.assertIn(counts.get(resource_type, 0), range(0, expected + 1), resource_type)

        self.assertIn(counts.get("aws_db_subnet_group", 0), (0, 1))
        self.assertIn(counts.get("aws_db_instance", 0), (0, 1))
        self.assertEqual(counts.get("aws_db_subnet_group", 0), counts.get("aws_db_instance", 0))

        self.assertMainTfMatches(r'resource "aws_lambda_function" "ingest"')
        self.assertMainTfMatches(r'resource "aws_lambda_function" "worker"')
        self.assertMainTfMatches(r'resource "aws_secretsmanager_secret" "db_credentials"')
        self.assertMainTfMatches(r'resource "aws_secretsmanager_secret_version" "db_credentials"')
        self.assertMainTfMatches(r'resource "aws_sfn_state_machine" "processing"')
        self.assertMainTfMatches(r'resource "aws_iam_role" "ingest_lambda"')
        self.assertMainTfMatches(r'resource "aws_iam_role" "worker_lambda"')
        self.assertMainTfMatches(r'resource "aws_iam_role" "step_functions"')

        public_a = self.resource_map["aws_subnet.public_a"]["values"]
        public_b = self.resource_map["aws_subnet.public_b"]["values"]
        private_a = self.resource_map["aws_subnet.private_a"]["values"]
        private_b = self.resource_map["aws_subnet.private_b"]["values"]

        self.assertNotEqual(public_a["availability_zone"], public_b["availability_zone"])
        self.assertEqual(public_a["availability_zone"], private_a["availability_zone"])
        self.assertEqual(public_b["availability_zone"], private_b["availability_zone"])

    def test_route_tables_and_vpc_endpoints_are_wired_correctly(self):
        public_rt = self.resource_map["aws_route_table.public"]["values"]
        private_rt = self.resource_map["aws_route_table.private"]["values"]
        public_associations = {
            self.resource_map["aws_route_table_association.public_a"]["values"]["subnet_id"],
            self.resource_map["aws_route_table_association.public_b"]["values"]["subnet_id"],
        }
        private_associations = {
            self.resource_map["aws_route_table_association.private_a"]["values"]["subnet_id"],
            self.resource_map["aws_route_table_association.private_b"]["values"]["subnet_id"],
        }

        self.assertEqual(public_rt["route"][0]["cidr_block"], "0.0.0.0/0")
        self.assertEqual(public_associations, {
            self.resource_map["aws_subnet.public_a"]["values"]["id"],
            self.resource_map["aws_subnet.public_b"]["values"]["id"],
        })
        self.assertEqual(private_rt["route"], [])
        self.assertEqual(private_associations, {
            self.resource_map["aws_subnet.private_a"]["values"]["id"],
            self.resource_map["aws_subnet.private_b"]["values"]["id"],
        })
        self.assertEqual(
            {
                self.resource_map["aws_route_table_association.private_a"]["values"]["route_table_id"],
                self.resource_map["aws_route_table_association.private_b"]["values"]["route_table_id"],
            },
            {private_rt["id"]},
        )

        private_subnets = {
            self.resource_map["aws_subnet.private_a"]["values"]["id"],
            self.resource_map["aws_subnet.private_b"]["values"]["id"],
        }
        endpoint_sg = self.resource_map["aws_security_group.interface_endpoints"]["values"]["id"]

        for address in [
            "aws_vpc_endpoint.secretsmanager",
            "aws_vpc_endpoint.sqs",
            "aws_vpc_endpoint.states",
            "aws_vpc_endpoint.logs",
        ]:
            values = self.resource_map[address]["values"]
            self.assertTrue(values["private_dns_enabled"])
            self.assertEqual(set(values["subnet_ids"]), private_subnets)
            self.assertEqual(values["security_group_ids"], [endpoint_sg])

        self.assertEqual(
            self.resource_map["aws_vpc_endpoint.s3"]["values"]["route_table_ids"],
            [private_rt["id"]],
        )
        self.assertEqual(
            self.resource_map["aws_security_group.interface_endpoints"]["values"]["egress"][0]["cidr_blocks"],
            ["0.0.0.0/0"],
        )
        self.assertEqual(
            self.resource_map["aws_security_group.db"]["values"]["egress"][0]["cidr_blocks"],
            ["0.0.0.0/0"],
        )

    def test_serverless_compute_and_logging_configuration(self):
        ingest = self.get_values("aws_lambda_function.ingest")
        worker = self.get_values("aws_lambda_function.worker")
        mapping = self.get_values("aws_lambda_event_source_mapping.worker_from_sqs")
        queue = self.resource_map["aws_sqs_queue.processing"]["values"]
        state_machine = self.get_values("aws_sfn_state_machine.processing")
        api_method = self.get_values("aws_api_gateway_method.ingest_post")
        api_integration = self.get_values("aws_api_gateway_integration.ingest_lambda")
        api_stage = self.get_values("aws_api_gateway_stage.v1")
        api_settings = self.get_values("aws_api_gateway_method_settings.all")

        self.assertMainTfMatches(r'resource "aws_lambda_function" "ingest" \{[\s\S]*?runtime\s*=\s*"python3\.12"[\s\S]*?handler\s*=\s*"index\.handler"[\s\S]*?memory_size\s*=\s*256[\s\S]*?timeout\s*=\s*10')
        self.assertMainTfMatches(r'resource "aws_lambda_function" "worker" \{[\s\S]*?runtime\s*=\s*"python3\.12"[\s\S]*?handler\s*=\s*"index\.handler"[\s\S]*?memory_size\s*=\s*256[\s\S]*?timeout\s*=\s*10')
        self.assertMainTfMatches(r'resource "aws_lambda_event_source_mapping" "worker_from_sqs" \{[\s\S]*?batch_size\s*=\s*10')
        self.assertMainTfMatches(r'resource "aws_api_gateway_resource" "ingest" \{[\s\S]*?path_part\s*=\s*"ingest"')
        self.assertMainTfMatches(r'resource "aws_api_gateway_method" "ingest_post" \{[\s\S]*?http_method\s*=\s*"POST"')
        self.assertMainTfMatches(r'resource "aws_api_gateway_integration" "ingest_lambda" \{[\s\S]*?integration_http_method\s*=\s*"POST"')
        self.assertMainTfMatches(r'resource "aws_api_gateway_integration" "ingest_lambda" \{[\s\S]*?type\s*=\s*"AWS_PROXY"')
        self.assertMainTfMatches(r'resource "aws_api_gateway_stage" "v1" \{[\s\S]*?stage_name\s*=\s*"v1"')
        self.assertMainTfMatches(r'resource "aws_api_gateway_method_settings" "all" \{[\s\S]*?method_path\s*=\s*"\*/\*"[\s\S]*?logging_level\s*=')
        self.assertMainTfMatches(r'resource "aws_sfn_state_machine" "processing" \{[\s\S]*?type\s*=\s*"STANDARD"')

        if ingest and worker:
            self.assertEqual(ingest["runtime"], "python3.12")
            self.assertEqual(worker["runtime"], "python3.12")
            self.assertEqual(ingest["package_type"], "Zip")
            self.assertEqual(worker["package_type"], "Zip")
            self.assertEqual(
                set(ingest["vpc_config"][0]["subnet_ids"]),
                {
                    self.resource_map["aws_subnet.private_a"]["values"]["id"],
                    self.resource_map["aws_subnet.private_b"]["values"]["id"],
                },
            )
            self.assertEqual(
                worker["vpc_config"][0]["security_group_ids"],
                [self.resource_map["aws_security_group.lambda"]["values"]["id"]],
            )

        if mapping and worker:
            self.assertEqual(mapping["event_source_arn"], queue["arn"])
            self.assertEqual(mapping["function_arn"], worker["arn"])
            self.assertEqual(mapping["batch_size"], 10)

        if api_method and api_integration and api_stage and api_settings:
            self.assertEqual(api_method["http_method"], "POST")
            self.assertEqual(api_integration["type"], "AWS_PROXY")
            self.assertEqual(api_integration["integration_http_method"], "POST")
            self.assertEqual(api_stage["stage_name"], "v1")
            self.assertIn(api_settings["settings"][0]["logging_level"], ("INFO", "ERROR"))

        if state_machine and worker:
            self.assertEqual(state_machine["type"], "STANDARD")
            self.assertIn(worker["arn"], state_machine["definition"])
            self.assertEqual(
                state_machine["logging_configuration"][0]["log_destination"],
                f'{self.resource_map["aws_cloudwatch_log_group.step_functions"]["values"]["arn"]}:*',
            )

        for address in [
            "aws_cloudwatch_log_group.ingest_lambda",
            "aws_cloudwatch_log_group.worker_lambda",
            "aws_cloudwatch_log_group.step_functions",
            "aws_cloudwatch_log_group.api_gateway_execution",
        ]:
            values = self.resource_map[address]["values"]
            self.assertEqual(values["retention_in_days"], 14)
            self.assertFalse(values.get("kms_key_id"))

    def test_storage_database_and_secret_resources_are_hardened(self):
        bucket = self.resource_map["aws_s3_bucket.data"]["values"]
        encryption = self.resource_map["aws_s3_bucket_server_side_encryption_configuration.data"]["values"]
        public_access = self.resource_map["aws_s3_bucket_public_access_block.data"]["values"]
        ownership = self.resource_map["aws_s3_bucket_ownership_controls.data"]["values"]
        secret = self.get_values("aws_secretsmanager_secret.db_credentials")

        self.assertTrue(bucket["force_destroy"])
        self.assertEqual(
            encryption["rule"][0]["apply_server_side_encryption_by_default"][0]["sse_algorithm"],
            "AES256",
        )
        self.assertTrue(public_access["block_public_acls"])
        self.assertTrue(public_access["block_public_policy"])
        self.assertTrue(public_access["ignore_public_acls"])
        self.assertTrue(public_access["restrict_public_buckets"])
        self.assertEqual(ownership["rule"][0]["object_ownership"], "BucketOwnerEnforced")
        self.assertNotIn('resource "aws_s3_bucket_policy"', self.main_tf)

        self.assertMainTfMatches(r'resource "aws_secretsmanager_secret" "db_credentials" \{[\s\S]*?recovery_window_in_days\s*=\s*0')
        self.assertMainTfMatches(
            r'resource "aws_secretsmanager_secret_version" "db_credentials" \{[\s\S]*?secret_string\s*=\s*jsonencode\(\{[\s\S]*?username\s*=\s*"appuser"[\s\S]*?password\s*=\s*random_password\.db_password\.result'
        )
        if secret:
            self.assertEqual(secret["recovery_window_in_days"], 0)

        db_subnet_group = self.get_resource("aws_db_subnet_group.main")
        db = self.get_resource("aws_db_instance.main")
        if db_subnet_group and db:
            db_subnet_group_values = db_subnet_group["values"]
            db_values = db["values"]
            self.assertEqual(
                set(db_subnet_group_values["subnet_ids"]),
                {
                    self.resource_map["aws_subnet.private_a"]["values"]["id"],
                    self.resource_map["aws_subnet.private_b"]["values"]["id"],
                },
            )
            self.assertEqual(db_values["engine"], "postgres")
            self.assertEqual(db_values["engine_version"], "15.5")
            self.assertEqual(db_values["instance_class"], "db.t3.micro")
            self.assertEqual(db_values["allocated_storage"], 20)
            self.assertTrue(db_values["storage_encrypted"])
            self.assertFalse(db_values["publicly_accessible"])
            self.assertEqual(db_values["backup_retention_period"], 0)
            self.assertFalse(db_values["deletion_protection"])
            self.assertTrue(db_values["skip_final_snapshot"])
            self.assertEqual(
                db_values["vpc_security_group_ids"],
                [self.resource_map["aws_security_group.db"]["values"]["id"]],
            )
        else:
            self.assertMainTfMatches(
                r'resource "aws_db_subnet_group" "main" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\]'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?engine\s*=\s*"postgres"'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?engine_version\s*=\s*"15\.5"'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?instance_class\s*=\s*"db\.t3\.micro"'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?allocated_storage\s*=\s*20'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?storage_encrypted\s*=\s*true'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?publicly_accessible\s*=\s*false'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?backup_retention_period\s*=\s*0'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?deletion_protection\s*=\s*false'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?skip_final_snapshot\s*=\s*true'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?username\s*=\s*jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\["username"\]'
            )
            self.assertMainTfMatches(
                r'resource "aws_db_instance" "main" \{[\s\S]*?password\s*=\s*jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\["password"\]'
            )

    def test_iam_policies_stay_scoped_to_required_actions_and_resources(self):
        queue_arn = self.resource_map["aws_sqs_queue.processing"]["values"]["arn"]
        bucket_arn = self.resource_map["aws_s3_bucket.data"]["values"]["arn"]
        secret = self.get_values("aws_secretsmanager_secret.db_credentials")
        worker = self.get_values("aws_lambda_function.worker")
        state_machine = self.get_values("aws_sfn_state_machine.processing")
        ingest_policy_resource = self.resource_map.get("aws_iam_role_policy.ingest_lambda")
        worker_policy_resource = self.resource_map.get("aws_iam_role_policy.worker_lambda")
        step_functions_policy_resource = self.resource_map.get("aws_iam_role_policy.step_functions")

        self.assertMainTfMatches(r'resource "aws_iam_role_policy" "ingest_lambda" \{')
        self.assertMainTfMatches(r'sqs:SendMessage')
        self.assertMainTfMatches(r'states:StartExecution')
        self.assertMainTfMatches(r'resource "aws_iam_role_policy" "worker_lambda" \{')
        self.assertMainTfMatches(r'secretsmanager:GetSecretValue')
        self.assertMainTfMatches(r's3:PutObject')
        self.assertMainTfMatches(r'resource "aws_iam_role_policy" "step_functions" \{')
        self.assertMainTfMatches(r'lambda:InvokeFunction')
        self.assertMainTfMatches(
            r'resource "aws_iam_role_policy" "ingest_lambda" \{[\s\S]*?logs:CreateLogStream[\s\S]*?logs:PutLogEvents[\s\S]*?Resource\s*=\s*"\$\{aws_cloudwatch_log_group\.ingest_lambda\.arn\}:\*"'
        )
        self.assertMainTfMatches(
            r'resource "aws_iam_role_policy" "worker_lambda" \{[\s\S]*?logs:CreateLogStream[\s\S]*?logs:PutLogEvents[\s\S]*?Resource\s*=\s*"\$\{aws_cloudwatch_log_group\.worker_lambda\.arn\}:\*"'
        )
        self.assertMainTfMatches(
            r'resource "aws_iam_role_policy" "step_functions" \{[\s\S]*?logs:CreateLogDelivery[\s\S]*?logs:DescribeLogGroups[\s\S]*?Resource\s*=\s*"\*"'
        )

        parsed_policies = []
        for resource in [ingest_policy_resource, worker_policy_resource, step_functions_policy_resource]:
            if resource:
                parsed_policies.append(load_policy(resource))

        for policy in parsed_policies:
            for statement in policy["Statement"]:
                self.assertNotIn("*", normalize_actions(statement["Action"]))
            self.assert_policy_allows_only_expected_star_resources(policy)

        if ingest_policy_resource and state_machine:
            ingest_policy = load_policy(ingest_policy_resource)
            self.assertIn(
                {
                    "Effect": "Allow",
                    "Action": ["states:StartExecution"],
                    "Resource": state_machine["arn"],
                },
                ingest_policy["Statement"],
            )
            self.assertIn(
                {
                    "Effect": "Allow",
                    "Action": ["sqs:SendMessage"],
                    "Resource": queue_arn,
                },
                ingest_policy["Statement"],
            )

        if worker_policy_resource:
            worker_policy = load_policy(worker_policy_resource)
            if secret:
                self.assertIn(
                    {
                        "Effect": "Allow",
                        "Action": [
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:DescribeSecret",
                        ],
                        "Resource": secret["arn"],
                    },
                    worker_policy["Statement"],
                )
            self.assertIn(
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:ChangeMessageVisibility",
                    ],
                    "Resource": queue_arn,
                },
                worker_policy["Statement"],
            )
            self.assertIn(
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": f"{bucket_arn}/processed/*",
                },
                worker_policy["Statement"],
            )

        if step_functions_policy_resource and worker:
            step_functions_policy = load_policy(step_functions_policy_resource)
            self.assertIn(
                {
                    "Effect": "Allow",
                    "Action": ["lambda:InvokeFunction"],
                    "Resource": worker["arn"],
                },
                step_functions_policy["Statement"],
            )


if __name__ == "__main__":
    unittest.main()
