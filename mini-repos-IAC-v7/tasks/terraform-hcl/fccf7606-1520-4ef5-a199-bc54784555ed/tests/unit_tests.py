import json
import os
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plan.json"
MAIN_TF = ROOT / "main.tf"


def _load_plan():
    if not PLAN_PATH.exists():
        raise AssertionError("plan.json not found. Run terraform plan and terraform show -json .tfplan > plan.json first.")
    return json.loads(PLAN_PATH.read_text())


def _walk_modules(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_walk_modules(child))
    return resources


def _planned_resources(plan):
    root_module = plan.get("planned_values", {}).get("root_module", {})
    return _walk_modules(root_module)


def _resource_map(resources):
    return {(resource["type"], resource["name"]): resource for resource in resources}


def _resource_values(resources, resource_type, resource_name):
    resource = _resource_map(resources)[(resource_type, resource_name)]
    return resource["values"]


def _is_local_mode():
    return bool(os.environ.get("TF_VAR_aws_endpoint") or os.environ.get("AWS_ENDPOINT_URL"))


class TestTerraformModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = _load_plan()
        cls.resources = _planned_resources(cls.plan)
        cls.resource_map = _resource_map(cls.resources)
        cls.main_tf = MAIN_TF.read_text()
        cls.local_mode = _is_local_mode()

    def test_single_file_and_provider_contract(self):
        tf_files = sorted(path.name for path in ROOT.glob("*.tf"))
        self.assertEqual(tf_files, ["main.tf"], "The prompt requires exactly one Terraform file named main.tf")

        self.assertIn('variable "aws_region"', self.main_tf)
        self.assertIn('default = "us-east-1"', self.main_tf)
        self.assertIn('variable "aws_endpoint"', self.main_tf)
        self.assertIn('default = ""', self.main_tf)
        self.assertIn('variable "aws_access_key_id"', self.main_tf)
        self.assertIn('variable "aws_secret_access_key"', self.main_tf)
        self.assertIn("sensitive = true", self.main_tf)
        self.assertIn('provider "aws"', self.main_tf)
        self.assertIn('region     = var.aws_region', self.main_tf)
        self.assertIn('var.aws_endpoint != ""', self.main_tf)
        self.assertEqual(self.main_tf.count('provider "'), 1)
        self.assertIn('local_mode = var.aws_endpoint != ""', self.main_tf)

    def test_foundational_network_resources(self):
        vpcs = [r for r in self.resources if r["type"] == "aws_vpc"]
        self.assertEqual(len(vpcs), 1)

        vpc = _resource_values(self.resources, "aws_vpc", "main")
        self.assertEqual(vpc["cidr_block"], "10.0.0.0/16")
        self.assertTrue(vpc["enable_dns_hostnames"])
        self.assertTrue(vpc["enable_dns_support"])

        subnets = [r for r in self.resources if r["type"] == "aws_subnet"]
        self.assertEqual(len(subnets), 4)

        subnet_cidrs = {
            _resource_values(self.resources, "aws_subnet", "public_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "public_b")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_b")["cidr_block"],
        }
        self.assertEqual(subnet_cidrs, {"10.0.0.0/24", "10.0.1.0/24", "10.0.10.0/24", "10.0.11.0/24"})

        route_tables = [r for r in self.resources if r["type"] == "aws_route_table"]
        self.assertEqual(len(route_tables), 2)
        self.assertIn(("aws_internet_gateway", "main"), self.resource_map)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))

        routes = [r for r in self.resources if r["type"] == "aws_route"]
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["values"]["destination_cidr_block"], "0.0.0.0/0")

        associations = [r for r in self.resources if r["type"] == "aws_route_table_association"]
        self.assertEqual(len(associations), 4)

        security_groups = [r for r in self.resources if r["type"] == "aws_security_group"]
        self.assertEqual(len(security_groups), 3)

        lambda_sg = _resource_values(self.resources, "aws_security_group", "lambda")
        self.assertEqual(len(lambda_sg["egress"]), 1)
        self.assertEqual(lambda_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])

        rds_sg = _resource_values(self.resources, "aws_security_group", "rds")
        redis_sg = _resource_values(self.resources, "aws_security_group", "redis")
        self.assertEqual(rds_sg["ingress"][0]["from_port"], 5432)
        self.assertEqual(redis_sg["ingress"][0]["from_port"], 6379)
        self.assertEqual(rds_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])
        self.assertEqual(redis_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])

        endpoints = [r for r in self.resources if r["type"] == "aws_vpc_endpoint"]
        self.assertEqual(len(endpoints), 3)
        endpoint_services = {resource["values"]["service_name"].rsplit(".", 1)[-1] for resource in endpoints}
        self.assertEqual(endpoint_services, {"logs", "secretsmanager", "sqs"})

    def test_data_store_configuration_and_secret_generation(self):
        secrets = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret"]
        secret_versions = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret_version"]
        self.assertEqual(len(secrets), 2)
        self.assertEqual(len(secret_versions), 2)

        self.assertIn('data "aws_secretsmanager_random_password" "rds"', self.main_tf)
        self.assertIn('data "aws_secretsmanager_random_password" "redshift"', self.main_tf)
        self.assertIn("exclude_punctuation = true", self.main_tf)
        self.assertNotIn('require("aws-sdk")', self.main_tf)
        self.assertNotIn("sha256(", self.main_tf)
        self.assertIn('resource "aws_db_instance" "postgres"', self.main_tf)
        self.assertIn('resource "aws_elasticache_replication_group" "redis"', self.main_tf)
        self.assertIn('resource "aws_redshift_cluster" "analytics"', self.main_tf)

        if self.local_mode:
            self.assertNotIn(("aws_db_instance", "postgres"), self.resource_map)
            self.assertNotIn(("aws_elasticache_replication_group", "redis"), self.resource_map)
            self.assertNotIn(("aws_redshift_cluster", "analytics"), self.resource_map)
            return

        postgres = _resource_values(self.resources, "aws_db_instance", "postgres")
        self.assertEqual(postgres["engine"], "postgres")
        self.assertEqual(postgres["engine_version"], "15.4")
        self.assertEqual(postgres["instance_class"], "db.t3.micro")
        self.assertEqual(postgres["allocated_storage"], 20)
        self.assertEqual(postgres["storage_type"], "gp3")
        self.assertFalse(postgres["publicly_accessible"])
        self.assertEqual(postgres["backup_retention_period"], 1)
        self.assertFalse(postgres["multi_az"])
        self.assertFalse(postgres["deletion_protection"])
        self.assertTrue(postgres["skip_final_snapshot"])
        self.assertEqual(postgres["enabled_cloudwatch_logs_exports"], ["postgresql"])

        redis = _resource_values(self.resources, "aws_elasticache_replication_group", "redis")
        self.assertEqual(redis["engine"], "redis")
        self.assertEqual(redis["engine_version"], "7.1")
        self.assertEqual(redis["node_type"], "cache.t3.micro")
        self.assertEqual(redis["num_cache_clusters"], 1)
        self.assertFalse(redis["automatic_failover_enabled"])
        self.assertTrue(redis["at_rest_encryption_enabled"])
        self.assertTrue(redis["transit_encryption_enabled"])

        redshift = _resource_values(self.resources, "aws_redshift_cluster", "analytics")
        self.assertEqual(redshift["node_type"], "dc2.large")
        self.assertEqual(redshift["cluster_type"], "single-node")
        self.assertEqual(redshift["database_name"], "appanalytics")
        self.assertTrue(redshift["encrypted"])
        self.assertFalse(redshift["publicly_accessible"])
        self.assertTrue(redshift["skip_final_snapshot"])

    def test_api_lambda_queue_and_frontend_contract(self):
        queues = [r for r in self.resources if r["type"] == "aws_sqs_queue"]
        self.assertEqual(len(queues), 1)

        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 345600)

        lambda_fns = [r for r in self.resources if r["type"] == "aws_lambda_function"]
        self.assertEqual(len(lambda_fns), 3)

        api_handler = _resource_values(self.resources, "aws_lambda_function", "api_handler")
        worker = _resource_values(self.resources, "aws_lambda_function", "worker_processor")
        enrichment = _resource_values(self.resources, "aws_lambda_function", "enrichment")

        self.assertEqual(api_handler["runtime"], "nodejs20.x")
        self.assertEqual(api_handler["handler"], "index.handler")
        self.assertEqual(api_handler["memory_size"], 256)
        self.assertEqual(api_handler["timeout"], 10)

        self.assertEqual(worker["runtime"], "nodejs20.x")
        self.assertEqual(worker["handler"], "index.handler")
        self.assertEqual(worker["memory_size"], 256)
        self.assertEqual(worker["timeout"], 20)

        self.assertEqual(enrichment["runtime"], "nodejs20.x")
        self.assertEqual(enrichment["handler"], "index.handler")
        self.assertEqual(enrichment["memory_size"], 256)
        self.assertEqual(enrichment["timeout"], 10)

        event_source_mappings = [r for r in self.resources if r["type"] == "aws_lambda_event_source_mapping"]
        self.assertEqual(len(event_source_mappings), 1)
        event_source_mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")
        self.assertEqual(event_source_mapping["batch_size"], 5)

        self.assertIn('resource "aws_apigatewayv2_api" "orders"', self.main_tf)
        self.assertIn('route_key = "POST /api/orders"', self.main_tf)
        self.assertIn('auto_deploy = true', self.main_tf)
        self.assertIn('resource "aws_cloudfront_distribution" "frontend"', self.main_tf)
        self.assertIn('fetch("/api/orders"', self.main_tf)
        self.assertIn('@aws-sdk/client-sqs', self.main_tf)
        self.assertIn('@aws-sdk/client-secrets-manager', self.main_tf)
        self.assertNotIn('require("aws-sdk")', self.main_tf)

        if self.local_mode:
            self.assertNotIn(("aws_apigatewayv2_api", "orders"), self.resource_map)
            self.assertNotIn(("aws_cloudfront_distribution", "frontend"), self.resource_map)
        else:
            self.assertIn(("aws_apigatewayv2_api", "orders"), self.resource_map)
            self.assertIn(("aws_cloudfront_distribution", "frontend"), self.resource_map)

    def test_iam_scoping(self):
        for expected in [
            'resource "aws_iam_role_policy" "api_handler"',
            'resource "aws_iam_role_policy" "worker_processor"',
            'resource "aws_iam_role_policy" "enrichment"',
            'resource "aws_iam_role_policy" "step_functions"',
            'resource "aws_iam_role_policy" "pipes"',
            'resource "aws_iam_role_policy" "glue_secret"',
            '"sqs:SendMessage"',
            '"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"',
            '"secretsmanager:GetSecretValue"',
            '"lambda:InvokeFunction"',
            '"states:StartExecution"',
            "aws_sqs_queue.orders.arn",
            "aws_secretsmanager_secret.rds.arn",
            "aws_secretsmanager_secret.redshift.arn",
            "aws_lambda_function.enrichment.arn",
            "aws_sfn_state_machine.orders.arn",
        ]:
            self.assertIn(expected, self.main_tf)

        iam_roles = [r for r in self.resources if r["type"] == "aws_iam_role"]
        self.assertEqual(len(iam_roles), 6)

    def test_outputs_and_observability(self):
        for output_name in [
            'output "cloudfront_distribution_domain_name"',
            'output "http_api_endpoint_url"',
            'output "rds_endpoint_address"',
            'output "redshift_endpoint_address"',
            'output "sqs_queue_url"',
        ]:
            self.assertIn(output_name, self.main_tf)

        alarms = [r for r in self.resources if r["type"] == "aws_cloudwatch_metric_alarm"]
        self.assertEqual(len(alarms), 5 if self.local_mode else 12)

        log_groups = [r for r in self.resources if r["type"] == "aws_cloudwatch_log_group"]
        self.assertGreaterEqual(len(log_groups), 6)
        for log_group in log_groups:
            self.assertEqual(log_group["values"]["retention_in_days"], 14)

        if self.local_mode:
            self.assertNotIn(("aws_glue_crawler", "analytics"), self.resource_map)
            self.assertNotIn(("aws_glue_connection", "redshift"), self.resource_map)
        else:
            self.assertIn(("aws_glue_crawler", "analytics"), self.resource_map)
            self.assertIn(("aws_glue_connection", "redshift"), self.resource_map)

    def test_negative_path_handling_in_inline_lambda_code(self):
        self.assertIn('return jsonResponse(400, { error: "invalid-json" })', self.main_tf)
        self.assertIn('return jsonResponse(400, { error: "invalid-payload" })', self.main_tf)
        self.assertIn('console.error("invalid-record-body", error);', self.main_tf)
        self.assertIn('console.error("missing-order-id");', self.main_tf)


if __name__ == "__main__":
    unittest.main()
