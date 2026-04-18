import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path, terraform_args):
    if path.exists():
        return json.loads(path.read_text())

    fallback = ROOT / terraform_args[-1] if terraform_args[-1].endswith(".tfplan") else None
    if fallback is not None and not fallback.exists():
        raise AssertionError(
            f"Missing {path.name}. Generate it first with terraform plan/show."
        )

    result = subprocess.run(
        ["terraform", *terraform_args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    path.write_text(result.stdout)
    return json.loads(result.stdout)


def load_plan():
    return _load_json(ROOT / "plan.json", ["show", "-json", ".tfplan"])


def iter_resources(module):
    for resource in module.get("resources", []):
        yield resource
    for child in module.get("child_modules", []):
        yield from iter_resources(child)


def resources_by_address(resources):
    return {resource["address"]: resource for resource in resources}


class TestTerraformSourceContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = load_plan()
        cls.main_tf = (ROOT / "main.tf").read_text()
        cls.aws_region = cls.plan.get("variables", {}).get("aws_region", {}).get("value", "us-east-1")
        cls.planned_resources = list(iter_resources(cls.plan["planned_values"]["root_module"]))
        cls.resource_map = resources_by_address(cls.planned_resources)

    def assertMainTfMatches(self, pattern):
        self.assertRegex(self.main_tf, pattern)

    def test_only_main_tf_exists_and_variables_match_contract(self):
        tf_files = sorted(path.name for path in ROOT.glob("*.tf"))
        self.assertEqual(tf_files, ["main.tf"])

        source_extensions = {".py", ".sh", ".js", ".ts", ".mjs", ".cjs", ".rb", ".go", ".java"}
        ignored_source_dirs = {"tests", ".terraform", ".pytest_cache", "__pycache__"}
        helper_files = sorted(
            path.relative_to(ROOT).as_posix()
            for path in ROOT.rglob("*")
            if path.is_file()
            and path.suffix in source_extensions
            and not set(path.relative_to(ROOT).parts).intersection(ignored_source_dirs)
        )
        self.assertEqual(helper_files, [])

        if shutil.which("git") is not None:
            tracked_files_result = subprocess.run(
                ["git", "ls-files"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            tracked_helper_files = sorted(
                path
                for path in tracked_files_result.stdout.splitlines()
                if Path(path).suffix in source_extensions
                and not set(Path(path).parts).intersection(ignored_source_dirs)
            )
            self.assertEqual(tracked_helper_files, [])
        self.assertNotRegex(
            self.main_tf,
            r"\$\{path\.module\}/[^\"']+\.(py|sh|js|ts|mjs|cjs|rb|go|java)\b",
        )
        self.assertNotIn("rds_compat_api.py", self.main_tf)

        variable_names = set(self.plan["configuration"]["root_module"]["variables"].keys())
        self.assertEqual(
            variable_names,
            {
                "aws_region",
                "aws_endpoint",
                "aws_access_key_id",
                "aws_secret_access_key",
            },
        )
        self.assertNotIn('output "', self.main_tf)
        self.assertNotIn('module "', self.main_tf)
        self.assertMainTfMatches(r'variable "aws_region" \{[\s\S]*?default\s*=\s*"us-east-1"')

    def test_provider_uses_only_allowed_inputs(self):
        self.assertRegex(self.main_tf, r"region\s*=\s*var\.aws_region")
        self.assertRegex(self.main_tf, r"access_key\s*=\s*var\.aws_access_key_id")
        self.assertRegex(self.main_tf, r"secret_key\s*=\s*var\.aws_secret_access_key")
        self.assertIn("for_each = var.aws_endpoint == null ? [] : [var.aws_endpoint]", self.main_tf)

        for service in [
            "apigateway",
            "cloudwatchlogs",
            "ec2",
            "iam",
            "lambda",
            "s3",
            "secretsmanager",
            "sfn",
            "sqs",
            "sts",
        ]:
            self.assertRegex(self.main_tf, rf"{service}\s*=\s*endpoints\.value")
        self.assertRegex(self.main_tf, r"rds\s*=\s*local\.rds_endpoint")

    def test_network_topology_matches_prompt(self):
        vpc = self.resource_map["aws_vpc.main"]["values"]
        self.assertEqual(vpc["cidr_block"], "10.20.0.0/16")
        self.assertTrue(vpc["enable_dns_support"])
        self.assertTrue(vpc["enable_dns_hostnames"])

        expected_subnets = {
            "aws_subnet.public_a": "10.20.0.0/24",
            "aws_subnet.public_b": "10.20.1.0/24",
            "aws_subnet.private_a": "10.20.10.0/24",
            "aws_subnet.private_b": "10.20.11.0/24",
        }
        for address, cidr in expected_subnets.items():
            self.assertEqual(self.resource_map[address]["values"]["cidr_block"], cidr)

        self.assertEqual(len([r for r in self.planned_resources if r["type"] == "aws_vpc"]), 1)
        self.assertEqual(len([r for r in self.planned_resources if r["type"] == "aws_subnet"]), 4)
        self.assertEqual(len([r for r in self.planned_resources if r["type"] == "aws_internet_gateway"]), 1)
        self.assertMainTfMatches(r'data "aws_availability_zones" "available"')
        self.assertMainTfMatches(r'resource "aws_subnet" "public_a" \{[\s\S]*?availability_zone\s*=\s*data\.aws_availability_zones\.available\.names\[0\]')
        self.assertMainTfMatches(r'resource "aws_subnet" "public_b" \{[\s\S]*?availability_zone\s*=\s*data\.aws_availability_zones\.available\.names\[1\]')
        self.assertMainTfMatches(r'resource "aws_subnet" "private_a" \{[\s\S]*?availability_zone\s*=\s*data\.aws_availability_zones\.available\.names\[0\]')
        self.assertMainTfMatches(r'resource "aws_subnet" "private_b" \{[\s\S]*?availability_zone\s*=\s*data\.aws_availability_zones\.available\.names\[1\]')

        public_route_table = self.resource_map["aws_route_table.public"]["values"]
        private_route_table = self.resource_map["aws_route_table.private"]["values"]
        self.assertEqual(len(public_route_table.get("route", [])), 1)
        self.assertEqual(public_route_table["route"][0]["cidr_block"], "0.0.0.0/0")
        self.assertEqual(private_route_table.get("route", []), [])

        self.assertMainTfMatches(
            r'resource "aws_route_table" "private" \{[\s\S]*?vpc_id = aws_vpc\.main\.id[\s\S]*?\}'
        )

    def test_vpc_endpoints_and_security_groups_match_requirements(self):
        expected_interface_services = {
            "aws_vpc_endpoint.secretsmanager": f"com.amazonaws.{self.aws_region}.secretsmanager",
            "aws_vpc_endpoint.sqs": f"com.amazonaws.{self.aws_region}.sqs",
            "aws_vpc_endpoint.states": f"com.amazonaws.{self.aws_region}.states",
            "aws_vpc_endpoint.logs": f"com.amazonaws.{self.aws_region}.logs",
        }
        for address, service_name in expected_interface_services.items():
            values = self.resource_map[address]["values"]
            self.assertEqual(values["vpc_endpoint_type"], "Interface")
            self.assertEqual(values["service_name"], service_name)
            self.assertTrue(values["private_dns_enabled"])
            if "subnet_ids" in values:
                self.assertEqual(len(values["subnet_ids"]), 2)
            if "security_group_ids" in values:
                self.assertEqual(len(values["security_group_ids"]), 1)

        s3_endpoint = self.resource_map["aws_vpc_endpoint.s3"]["values"]
        self.assertEqual(s3_endpoint["vpc_endpoint_type"], "Gateway")
        self.assertEqual(s3_endpoint["service_name"], f"com.amazonaws.{self.aws_region}.s3")
        endpoint_sg = self.resource_map["aws_security_group.interface_endpoints"]["values"]
        db_sg = self.resource_map["aws_security_group.db"]["values"]
        if "route_table_ids" in s3_endpoint:
            self.assertEqual(len(s3_endpoint["route_table_ids"]), 1)

        self.assertMainTfMatches(
            r'resource "aws_vpc_endpoint" "secretsmanager" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.interface_endpoints\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_vpc_endpoint" "sqs" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.interface_endpoints\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_vpc_endpoint" "states" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.interface_endpoints\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_vpc_endpoint" "logs" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.interface_endpoints\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_vpc_endpoint" "s3" \{[\s\S]*?route_table_ids\s*=\s*\[aws_route_table\.private\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_route_table_association" "private_a" \{[\s\S]*?route_table_id\s*=\s*aws_route_table\.private\.id'
        )
        self.assertMainTfMatches(
            r'resource "aws_route_table_association" "private_b" \{[\s\S]*?route_table_id\s*=\s*aws_route_table\.private\.id'
        )

        self.assertEqual(endpoint_sg["ingress"][0]["from_port"], 443)
        self.assertEqual(endpoint_sg["ingress"][0]["to_port"], 443)
        self.assertEqual(endpoint_sg["ingress"][0]["protocol"], "tcp")
        if "security_groups" in endpoint_sg["ingress"][0]:
            self.assertEqual(len(endpoint_sg["ingress"][0]["security_groups"]), 1)
        self.assertMainTfMatches(
            r'resource "aws_security_group" "interface_endpoints" \{[\s\S]*?ingress \{[\s\S]*?from_port\s*=\s*443[\s\S]*?to_port\s*=\s*443[\s\S]*?protocol\s*=\s*"tcp"[\s\S]*?security_groups\s*=\s*\[aws_security_group\.lambda\.id\]'
        )
        self.assertEqual(endpoint_sg["egress"][0]["from_port"], 0)
        self.assertEqual(endpoint_sg["egress"][0]["to_port"], 0)
        self.assertEqual(endpoint_sg["egress"][0]["protocol"], "-1")
        self.assertEqual(endpoint_sg["egress"][0]["cidr_blocks"], ["0.0.0.0/0"])

        self.assertEqual(db_sg["ingress"][0]["from_port"], 5432)
        self.assertEqual(db_sg["ingress"][0]["to_port"], 5432)
        self.assertEqual(db_sg["ingress"][0]["protocol"], "tcp")
        if "security_groups" in db_sg["ingress"][0]:
            self.assertEqual(len(db_sg["ingress"][0]["security_groups"]), 1)
        self.assertMainTfMatches(
            r'resource "aws_security_group" "db" \{[\s\S]*?ingress \{[\s\S]*?from_port\s*=\s*5432[\s\S]*?to_port\s*=\s*5432[\s\S]*?protocol\s*=\s*"tcp"[\s\S]*?security_groups\s*=\s*\[aws_security_group\.lambda\.id\]'
        )
        self.assertEqual(db_sg["egress"][0]["from_port"], 0)
        self.assertEqual(db_sg["egress"][0]["to_port"], 0)
        self.assertEqual(db_sg["egress"][0]["protocol"], "-1")
        self.assertEqual(db_sg["egress"][0]["cidr_blocks"], ["0.0.0.0/0"])

    def test_serverless_and_data_plane_configuration(self):
        ingest = self.resource_map["aws_lambda_function.ingest"]["values"]
        worker = self.resource_map["aws_lambda_function.worker"]["values"]
        queue = self.resource_map["aws_sqs_queue.processing"]["values"]
        state_machine = self.resource_map["aws_sfn_state_machine.processing"]["values"]
        api_stage = self.resource_map["aws_api_gateway_stage.v1"]["values"]
        method_settings = self.resource_map["aws_api_gateway_method_settings.all"]["values"]

        for function in (ingest, worker):
            self.assertEqual(function["runtime"], "python3.12")
            self.assertEqual(function["handler"], "index.handler")
            self.assertEqual(function["memory_size"], 256)
            self.assertEqual(function["timeout"], 10)
            self.assertEqual(len(function.get("vpc_config", [])), 1)
            if "subnet_ids" in function["vpc_config"][0]:
                self.assertEqual(len(function["vpc_config"][0]["subnet_ids"]), 2)
            if "security_group_ids" in function["vpc_config"][0]:
                self.assertEqual(len(function["vpc_config"][0]["security_group_ids"]), 1)

        self.assertMainTfMatches(
            r'resource "aws_lambda_function" "ingest" \{[\s\S]*?vpc_config \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.lambda\.id\]'
        )
        self.assertMainTfMatches(
            r'resource "aws_lambda_function" "worker" \{[\s\S]*?vpc_config \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\][\s\S]*?security_group_ids\s*=\s*\[aws_security_group\.lambda\.id\]'
        )

        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 1209600)
        self.assertTrue(queue["sqs_managed_sse_enabled"])

        self.assertEqual(state_machine["type"], "STANDARD")
        if "definition" in state_machine:
            self.assertIn("arn:aws:states:::lambda:invoke", state_machine["definition"])
        self.assertMainTfMatches(
            r'resource "aws_sfn_state_machine" "processing" \{[\s\S]*?type\s*=\s*"STANDARD"[\s\S]*?arn:aws:states:::lambda:invoke'
        )
        self.assertIn("aws_lambda_function.worker", self.main_tf)

        self.assertMainTfMatches(
            r'resource "aws_api_gateway_resource" "ingest" \{[\s\S]*?path_part\s*=\s*"ingest"'
        )
        self.assertEqual(api_stage["stage_name"], "v1")
        self.assertEqual(method_settings["method_path"], "*/*")
        self.assertIn(method_settings["settings"][0]["logging_level"], ("INFO", "ERROR"))

        db_resources = [resource for resource in self.planned_resources if resource["type"] == "aws_db_instance"]
        self.assertEqual(len(db_resources), 1)
        db_values = self.resource_map["aws_db_instance.main"]["values"]
        self.assertEqual(db_values["engine"], "postgres")
        self.assertEqual(db_values["engine_version"], "15.5")
        self.assertEqual(db_values["instance_class"], "db.t3.micro")
        self.assertEqual(db_values["allocated_storage"], 20)
        self.assertTrue(db_values["storage_encrypted"])
        self.assertFalse(db_values["publicly_accessible"])
        self.assertEqual(db_values["backup_retention_period"], 0)
        self.assertFalse(db_values["deletion_protection"])
        self.assertTrue(db_values["skip_final_snapshot"])

    def test_storage_secrets_logging_and_inline_code_requirements(self):
        self.assertIn('resource "random_password" "db_password"', self.main_tf)
        self.assertIn("special = false", self.main_tf)
        self.assertIn('username = "appuser"', self.main_tf)
        self.assertNotIn('password = "', self.main_tf)
        self.assertNotIn('resource "aws_s3_bucket_policy"', self.main_tf)
        self.assertMainTfMatches(
            r'resource "aws_secretsmanager_secret_version" "db_credentials" \{[\s\S]*?secret_string\s*=\s*jsonencode\(\{[\s\S]*?username\s*=\s*"appuser"[\s\S]*?password\s*=\s*random_password\.db_password\.result'
        )
        self.assertMainTfMatches(
            r'resource "aws_db_instance" "main" \{[\s\S]*?username\s*=\s*jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\["username"\][\s\S]*?password\s*=\s*jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\["password"\]'
        )

        self.assertIn('boto3.client("sqs", endpoint_url=endpoint_url)', self.main_tf)
        self.assertIn("send_message(", self.main_tf)
        self.assertIn('boto3.client("stepfunctions", endpoint_url=endpoint_url)', self.main_tf)
        self.assertIn("start_execution(", self.main_tf)
        self.assertIn('boto3.client("secretsmanager", endpoint_url=endpoint_url)', self.main_tf)
        self.assertIn("get_secret_value(", self.main_tf)
        self.assertIn('boto3.client("s3", endpoint_url=endpoint_url)', self.main_tf)
        self.assertIn("put_object(", self.main_tf)
        self.assertIn('os.environ.get("AWS_ENDPOINT_URL") or None', self.main_tf)
        self.assertIn('processed/{context.aws_request_id}.json', self.main_tf)

        bucket = self.resource_map["aws_s3_bucket.data"]["values"]
        encryption = self.resource_map["aws_s3_bucket_server_side_encryption_configuration.data"]["values"]
        public_access = self.resource_map["aws_s3_bucket_public_access_block.data"]["values"]
        ownership = self.resource_map["aws_s3_bucket_ownership_controls.data"]["values"]

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

        db_subnet_groups = [
            resource for resource in self.planned_resources if resource["type"] == "aws_db_subnet_group"
        ]
        self.assertEqual(len(db_subnet_groups), 1)
        db_subnet_group = self.resource_map["aws_db_subnet_group.main"]
        if "subnet_ids" in db_subnet_group["values"]:
            self.assertEqual(len(db_subnet_group["values"]["subnet_ids"]), 2)
        self.assertMainTfMatches(
            r'resource "aws_db_subnet_group" "main" \{[\s\S]*?subnet_ids\s*=\s*\[aws_subnet\.private_a\.id, aws_subnet\.private_b\.id\]'
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


if __name__ == "__main__":
    unittest.main()
