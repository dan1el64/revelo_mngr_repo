import json
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


class TestTerraformModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = _load_plan()
        cls.resources = _planned_resources(cls.plan)
        cls.resource_map = _resource_map(cls.resources)
        cls.main_tf = MAIN_TF.read_text()

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
        # Exactly one AWS provider block — no additional providers in the configuration
        self.assertEqual(self.main_tf.count('provider "'), 1)

    def test_foundational_network_resources(self):
        # Exactly 1 VPC
        vpcs = [r for r in self.resources if r["type"] == "aws_vpc"]
        self.assertEqual(len(vpcs), 1)

        vpc = _resource_values(self.resources, "aws_vpc", "main")
        self.assertEqual(vpc["cidr_block"], "10.0.0.0/16")
        self.assertTrue(vpc["enable_dns_hostnames"])
        self.assertTrue(vpc["enable_dns_support"])

        # Exactly 4 subnets: 2 public, 2 private
        subnets = [r for r in self.resources if r["type"] == "aws_subnet"]
        self.assertEqual(len(subnets), 4)

        subnet_cidrs = {
            _resource_values(self.resources, "aws_subnet", "public_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "public_b")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_b")["cidr_block"],
        }
        self.assertEqual(subnet_cidrs, {"10.0.0.0/24", "10.0.1.0/24", "10.0.10.0/24", "10.0.11.0/24"})

        # Exactly 2 route tables; no NAT gateway
        route_tables = [r for r in self.resources if r["type"] == "aws_route_table"]
        self.assertEqual(len(route_tables), 2)
        self.assertIn(("aws_internet_gateway", "main"), self.resource_map)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))

        # Exactly 1 default route (public → IGW); private route table has none
        routes = [r for r in self.resources if r["type"] == "aws_route"]
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["values"]["destination_cidr_block"], "0.0.0.0/0")

        # All 4 subnets associated to a route table
        associations = [r for r in self.resources if r["type"] == "aws_route_table_association"]
        self.assertEqual(len(associations), 4)

        security_groups = [r for r in self.resources if r["type"] == "aws_security_group"]
        self.assertEqual(len(security_groups), 3)

        lambda_sg = _resource_values(self.resources, "aws_security_group", "lambda")
        egress_rules = lambda_sg["egress"]
        self.assertEqual(len(egress_rules), 1)
        self.assertEqual(egress_rules[0]["cidr_blocks"], ["10.0.0.0/16"])

        rds_sg = _resource_values(self.resources, "aws_security_group", "rds")
        redis_sg = _resource_values(self.resources, "aws_security_group", "redis")
        self.assertEqual(rds_sg["ingress"][0]["from_port"], 5432)
        self.assertEqual(redis_sg["ingress"][0]["from_port"], 6379)

        # RDS and Redis SGs must restrict egress to the VPC CIDR only
        self.assertEqual(len(rds_sg["egress"]), 1)
        self.assertEqual(rds_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])
        self.assertEqual(len(redis_sg["egress"]), 1)
        self.assertEqual(redis_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])

    def test_data_store_configuration(self):
        secrets = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret"]
        secret_versions = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret_version"]
        self.assertEqual(len(secrets), 2)
        self.assertEqual(len(secret_versions), 2)
        for expected in [
            'resource "aws_db_instance" "postgres"',
            'engine                          = "postgres"',
            'engine_version                  = "15.4"',
            'instance_class                  = "db.t3.micro"',
            'allocated_storage               = 20',
            'storage_type                    = "gp3"',
            'publicly_accessible             = false',
            'backup_retention_period         = 1',
            'multi_az                        = false',
            'deletion_protection             = false',
            'skip_final_snapshot             = true',
            'enabled_cloudwatch_logs_exports = ["postgresql"]',
            'resource "aws_elasticache_replication_group" "redis"',
            'engine                     = "redis"',
            'engine_version             = "7.1"',
            'node_type                  = "cache.t3.micro"',
            'num_cache_clusters         = 1',
            'automatic_failover_enabled = false',
            'at_rest_encryption_enabled = true',
            'transit_encryption_enabled = true',
            'resource "aws_redshift_cluster" "analytics"',
            'node_type                 = "dc2.large"',
            'cluster_type              = "single-node"',
            'database_name             = "appanalytics"',
            'encrypted                 = true',
            'publicly_accessible       = false',
            'skip_final_snapshot       = true',
            'subnet_ids = local.private_subnet_ids',
        ]:
            self.assertIn(expected, self.main_tf)

    def test_api_lambda_and_queue_contract(self):
        # Exactly 1 SQS standard queue
        queues = [r for r in self.resources if r["type"] == "aws_sqs_queue"]
        self.assertEqual(len(queues), 1)

        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 345600)

        # Exactly 3 Lambda functions
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

        self.assertGreaterEqual(self.main_tf.count("subnet_ids         = local.private_subnet_ids"), 3)
        self.assertGreaterEqual(self.main_tf.count("security_group_ids = [aws_security_group.lambda.id]"), 3)

        # Exactly 1 event source mapping (queue → worker_processor)
        event_source_mappings = [r for r in self.resources if r["type"] == "aws_lambda_event_source_mapping"]
        self.assertEqual(len(event_source_mappings), 1)
        event_source_mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")
        self.assertEqual(event_source_mapping["batch_size"], 5)

        self.assertIn('resource "aws_apigatewayv2_api" "orders"', self.main_tf)
        self.assertIn('protocol_type = "HTTP"', self.main_tf)
        self.assertIn('route_key = "POST /api/orders"', self.main_tf)
        self.assertIn('auto_deploy = true', self.main_tf)
        self.assertIn("fetch(\"/api/orders\"", self.main_tf)
        self.assertIn("statusCode: 202", self.main_tf)
        self.assertIn("orderId", self.main_tf)

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

        # Exactly 6 IAM roles (api_handler, worker_processor, enrichment, step_functions, pipes, glue)
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

        # Exactly 12 CloudWatch metric alarms declared in source
        self.assertEqual(self.main_tf.count('resource "aws_cloudwatch_metric_alarm"'), 12)

        # At least 6 log groups always planned; each must enforce 14-day retention
        log_groups = [r for r in self.resources if r["type"] == "aws_cloudwatch_log_group"]
        self.assertGreaterEqual(len(log_groups), 6)
        for lg in log_groups:
            self.assertEqual(
                lg["values"]["retention_in_days"],
                14,
                f"Log group '{lg['values']['name']}' must have retention_in_days = 14",
            )

        self.assertIn('resource "aws_glue_crawler" "analytics"', self.main_tf)
        self.assertIn('name = "analytics_catalog"', self.main_tf)


if __name__ == "__main__":
    unittest.main()
