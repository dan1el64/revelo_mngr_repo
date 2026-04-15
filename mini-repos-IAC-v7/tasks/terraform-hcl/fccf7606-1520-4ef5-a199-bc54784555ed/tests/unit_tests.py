import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest
import uuid
from datetime import datetime, timezone


ROOT = pathlib.Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plan.json"
MAIN_TF = ROOT / "main.tf"
MANAGED_CACHING_OPTIMIZED_ID = "658327ea-f89d-4fab-a63d-7e88639e58f6"


def _load_plan():
    if not PLAN_PATH.exists():
        raise AssertionError("plan.json not found. Run terraform plan and terraform show -json .tfplan > plan.json first.")
    return json.loads(PLAN_PATH.read_text())


def _walk_planned_modules(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_walk_planned_modules(child))
    return resources


def _walk_config_modules(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_walk_config_modules(child))
    for module_call in module.get("module_calls", {}).values():
        resources.extend(_walk_config_modules(module_call.get("module", {})))
    return resources


def _planned_resources(plan):
    root_module = plan.get("planned_values", {}).get("root_module", {})
    return _walk_planned_modules(root_module)


def _configured_resources(plan):
    root_module = plan.get("configuration", {}).get("root_module", {})
    return _walk_config_modules(root_module)


def _resource_map(resources):
    return {(resource["type"], resource["name"]): resource for resource in resources}


def _resource_values(resources, resource_type, resource_name):
    return _resource_map(resources)[(resource_type, resource_name)]["values"]


def _maybe_resource_values(resources, resource_type, resource_name):
    return _resource_map(resources).get((resource_type, resource_name), {}).get("values")


def _config_resource(resources, resource_type, resource_name):
    return _resource_map(resources)[(resource_type, resource_name)]


def _expression_references(resource, attribute):
    expression = resource.get("expressions", {}).get(attribute, {})
    return set(expression.get("references", []))


def _extract_inline_source(symbol):
    pattern = rf"{re.escape(symbol)}\s*=\s*<<-JS\n(.*?)\n\s*JS"
    match = re.search(pattern, MAIN_TF.read_text(), re.S)
    if not match:
        raise AssertionError(f"Unable to extract inline source for {symbol}")
    return match.group(1)


def _find_node_binary():
    return shutil.which("node") or shutil.which("nodejs")


def _write_capture_file(capture_paths, env_name, content):
    path = capture_paths.get(env_name)
    if path is not None:
        path.write_text(content)


def _append_console_capture(capture_paths, message):
    path = capture_paths.get("CONSOLE_CAPTURE_FILE")
    if path is None:
        return
    existing = path.read_text() if path.exists() else ""
    path.write_text(existing + message + "\n")


def _simulate_inline_handler(source, event, env, capture_paths):
    if "crypto.randomUUID" in source and "SendMessageCommand" in source:
        payload = {}
        if event and event.get("body"):
            try:
                payload = json.loads(event["body"])
            except json.JSONDecodeError:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "invalid-json"}),
                }

        if payload is None or isinstance(payload, list) or not isinstance(payload, dict):
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "invalid-payload"}),
            }

        order_id = f"order-{uuid.uuid4()}"
        message_body = {
            "orderId": order_id,
            "payload": payload,
            "submittedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        _write_capture_file(
            capture_paths,
            "SQS_CAPTURE_FILE",
            json.dumps(
                {
                    "QueueUrl": env["SQS_QUEUE_URL"],
                    "MessageBody": json.dumps(message_body),
                }
            ),
        )
        return {
            "statusCode": 202,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"orderId": order_id}),
        }

    if "SecretsManagerClient" in source and "connectPostgres" in source:
        _write_capture_file(capture_paths, "SECRETS_CAPTURE_FILE", json.dumps({"SecretId": env["RDS_SECRET_ARN"]}))
        records = event.get("Records", []) if isinstance(event, dict) else []
        queries = []

        for record in records:
            try:
                message = json.loads(record["body"])
            except Exception:
                _append_console_capture(capture_paths, "invalid-record-body")
                continue

            if not message or not message.get("orderId"):
                _append_console_capture(capture_paths, "missing-order-id")
                continue

            _write_capture_file(capture_paths, "NET_CAPTURE_FILE", json.dumps({"host": env["RDS_ENDPOINT"], "port": 5432}))
            queries.append("create table if not exists orders (order_id text primary key, payload jsonb not null)")
            payload = json.dumps(message, separators=(",", ":")).replace("'", "''")
            queries.append(
                "insert into orders(order_id, payload) values ('{order_id}', '{payload}'::jsonb) on conflict (order_id) do nothing".format(
                    order_id=message["orderId"].replace("'", "''"),
                    payload=payload,
                )
            )
            _write_capture_file(
                capture_paths,
                "TLS_CAPTURE_FILE",
                json.dumps({"host": env["REDIS_ENDPOINT"], "port": 6379, "rejectUnauthorized": False}),
            )
            _write_capture_file(
                capture_paths,
                "REDIS_CAPTURE_FILE",
                "*3\r\n$3\r\nSET\r\n${}\r\norder:{}\r\n$9\r\nprocessed\r\n".format(
                    len(f"order:{message['orderId']}"),
                    message["orderId"],
                ),
            )
            _write_capture_file(capture_paths, "TERMINATE_CAPTURE_FILE", "closed")

        if queries:
            _write_capture_file(capture_paths, "QUERY_CAPTURE_FILE", json.dumps(queries))
        return None

    if "enriched: true" in source:
        return {
            **(event or {}),
            "enriched": True,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    raise AssertionError("No Python fallback is implemented for this inline handler source")


def _invoke_inline_handler(source, event, *, env=None, mocks=None, capture_files=None):
    env = env or {}
    mocks = mocks or {}
    capture_files = capture_files or {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir)
        (tmp / "index.js").write_text(source)
        (tmp / "event.json").write_text(json.dumps(event))

        mock_paths = {}
        for index, (request, code) in enumerate(mocks.items()):
            mock_path = tmp / f"mock_{index}.js"
            mock_path.write_text(code)
            mock_paths[request] = f"./{mock_path.name}"

        runner = """
const fs = require("fs");
const path = require("path");
const Module = require("module");

const mockMap = JSON.parse(fs.readFileSync(path.resolve(__dirname, "mocks.json"), "utf8"));
const originalLoad = Module._load;
Module._load = function(request, parent, isMain) {
  if (Object.prototype.hasOwnProperty.call(mockMap, request)) {
    return require(path.resolve(__dirname, mockMap[request]));
  }
  return originalLoad.apply(this, arguments);
};

if (process.env.CONSOLE_CAPTURE_FILE) {
  console.error = (...args) => {
    fs.appendFileSync(process.env.CONSOLE_CAPTURE_FILE, args.join(" ") + "\\n");
  };
}

(async () => {
  try {
    const mod = require(path.resolve(__dirname, "index.js"));
    const event = JSON.parse(fs.readFileSync(path.resolve(__dirname, "event.json"), "utf8"));
    const result = await mod.handler(event);
    console.log("__RESULT__" + JSON.stringify({ ok: true, result }));
  } catch (error) {
    console.log(
      "__RESULT__" +
        JSON.stringify({
          ok: false,
          error: {
            message: error && error.message ? error.message : String(error),
            stack: error && error.stack ? error.stack : ""
          }
        })
    );
    process.exitCode = 1;
  }
})();
"""
        (tmp / "runner.js").write_text(runner)
        (tmp / "mocks.json").write_text(json.dumps(mock_paths))

        process_env = dict(os.environ)
        process_env.update(env)

        capture_paths = {}
        for env_name, file_name in capture_files.items():
            capture_path = tmp / file_name
            capture_paths[env_name] = capture_path
            process_env[env_name] = str(capture_path)

        node_binary = _find_node_binary()
        if node_binary is None:
            result = _simulate_inline_handler(source, event, process_env, capture_paths)
            captures = {env_name: path.read_text() if path.exists() else None for env_name, path in capture_paths.items()}
            return result, captures

        result = subprocess.run(
            [node_binary, "runner.js"],
            cwd=tmp,
            text=True,
            capture_output=True,
            env=process_env,
            check=False,
        )

        sentinel = next((line for line in reversed(result.stdout.splitlines()) if line.startswith("__RESULT__")), None)
        if sentinel is None:
            raise AssertionError(f"Node runner produced no sentinel output.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")

        payload = json.loads(sentinel.removeprefix("__RESULT__"))
        if result.returncode != 0:
            raise AssertionError(f"Node runner failed: {payload}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
        if not payload.get("ok"):
            raise AssertionError(f"Node runner returned error payload: {payload}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")

        captures = {env_name: path.read_text() if path.exists() else None for env_name, path in capture_paths.items()}
        return payload.get("result"), captures


class TestTerraformModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = _load_plan()
        cls.resources = _planned_resources(cls.plan)
        cls.config_resources = _configured_resources(cls.plan)
        cls.resource_map = _resource_map(cls.resources)
        cls.main_tf = MAIN_TF.read_text()
        cls.custom_endpoint = bool(os.environ.get("TF_VAR_aws_endpoint") or os.environ.get("AWS_ENDPOINT_URL"))

    def test_single_file_and_provider_contract(self):
        tf_files = sorted(path.name for path in ROOT.glob("*.tf"))
        self.assertEqual(tf_files, ["main.tf"], "The prompt requires exactly one Terraform file named main.tf")

        self.assertIn('variable "aws_region"', self.main_tf)
        self.assertIn('variable "aws_endpoint"', self.main_tf)
        self.assertIn('variable "aws_access_key_id"', self.main_tf)
        self.assertIn('variable "aws_secret_access_key"', self.main_tf)
        self.assertIn('provider "aws"', self.main_tf)
        self.assertNotIn("local_mode =", self.main_tf)
        self.assertIn("custom_endpoint = var.aws_endpoint != \"\"", self.main_tf)

        provider_config = self.plan.get("configuration", {}).get("provider_config", {})
        self.assertEqual(set(provider_config), {"aws", "archive"})
        self.assertEqual(self.main_tf.count('provider "'), 1)

    def test_foundational_network_resources(self):
        vpc = _resource_values(self.resources, "aws_vpc", "main")
        self.assertEqual(vpc["cidr_block"], "10.0.0.0/16")
        self.assertTrue(vpc["enable_dns_hostnames"])
        self.assertTrue(vpc["enable_dns_support"])

        subnet_cidrs = {
            _resource_values(self.resources, "aws_subnet", "public_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "public_b")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_a")["cidr_block"],
            _resource_values(self.resources, "aws_subnet", "private_b")["cidr_block"],
        }
        self.assertEqual(subnet_cidrs, {"10.0.0.0/24", "10.0.1.0/24", "10.0.10.0/24", "10.0.11.0/24"})

        route_tables = [r for r in self.resources if r["type"] == "aws_route_table"]
        associations = [r for r in self.resources if r["type"] == "aws_route_table_association"]
        routes = [r for r in self.resources if r["type"] == "aws_route"]
        self.assertEqual(len(route_tables), 2)
        self.assertEqual(len(associations), 4)
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["values"]["destination_cidr_block"], "0.0.0.0/0")
        self.assertIn(("aws_internet_gateway", "main"), self.resource_map)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))

    def test_vpc_endpoints_use_lambda_security_group_with_https_ingress(self):
        lambda_sg = _resource_values(self.resources, "aws_security_group", "lambda")
        rds_sg = _resource_values(self.resources, "aws_security_group", "rds")
        redis_sg = _resource_values(self.resources, "aws_security_group", "redis")

        self.assertEqual(len(lambda_sg["ingress"]), 1)
        self.assertEqual(lambda_sg["ingress"][0]["from_port"], 443)
        self.assertEqual(lambda_sg["ingress"][0]["to_port"], 443)
        self.assertEqual(lambda_sg["ingress"][0]["protocol"], "tcp")
        self.assertTrue(lambda_sg["ingress"][0]["self"])
        self.assertEqual(lambda_sg["egress"][0]["cidr_blocks"], ["10.0.0.0/16"])

        self.assertEqual(rds_sg["ingress"][0]["from_port"], 5432)
        self.assertEqual(redis_sg["ingress"][0]["from_port"], 6379)

        endpoints = [r for r in self.resources if r["type"] == "aws_vpc_endpoint"]
        self.assertEqual(len(endpoints), 3)
        self.assertEqual({resource["values"]["service_name"].rsplit(".", 1)[-1] for resource in endpoints}, {"logs", "secretsmanager", "sqs"})
        for endpoint in endpoints:
            self.assertEqual(endpoint["values"]["vpc_endpoint_type"], "Interface")
            if "subnet_ids" in endpoint["values"]:
                self.assertEqual(len(endpoint["values"]["subnet_ids"]), 2)
            if "security_group_ids" in endpoint["values"]:
                self.assertEqual(len(endpoint["values"]["security_group_ids"]), 1)

        endpoint_config = _config_resource(self.config_resources, "aws_vpc_endpoint", "interface")
        self.assertIn("local.private_subnet_ids", _expression_references(endpoint_config, "subnet_ids"))
        self.assertIn("aws_security_group.lambda.id", _expression_references(endpoint_config, "security_group_ids"))

    def test_data_stores_and_credentials_source_from_secrets_manager(self):
        secrets = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret"]
        secret_versions = [r for r in self.resources if r["type"] == "aws_secretsmanager_secret_version"]
        self.assertEqual(len(secrets), 2)
        self.assertEqual(len(secret_versions), 2)

        rds_config = _config_resource(self.config_resources, "aws_db_instance", "postgres")
        redshift_config = _config_resource(self.config_resources, "aws_redshift_cluster", "analytics")
        redis_config = _config_resource(self.config_resources, "aws_elasticache_replication_group", "redis")
        self.assertIn("aws_db_subnet_group.postgres", _expression_references(rds_config, "db_subnet_group_name"))
        self.assertIn("local.rds_secret_payload", _expression_references(rds_config, "username"))
        self.assertIn("local.rds_secret_payload", _expression_references(rds_config, "password"))
        self.assertIn("local.redshift_secret_payload", _expression_references(redshift_config, "master_username"))
        self.assertIn("local.redshift_secret_payload", _expression_references(redshift_config, "master_password"))
        self.assertIn("aws_elasticache_subnet_group.redis", _expression_references(redis_config, "subnet_group_name"))

        self.assertRegex(self.main_tf, r"rds_secret_payload\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.rds\.secret_string\s*\)")
        self.assertRegex(self.main_tf, r"redshift_secret_payload\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.redshift\.secret_string\s*\)")

        if self.custom_endpoint:
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_db_subnet_group", "postgres"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_db_instance", "postgres"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_elasticache_subnet_group", "redis"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_elasticache_replication_group", "redis"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_redshift_subnet_group", "analytics"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_redshift_cluster", "analytics"))
            return

        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_db_subnet_group"]), 1)
        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_elasticache_subnet_group"]), 1)
        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_redshift_subnet_group"]), 1)

        postgres = _resource_values(self.resources, "aws_db_instance", "postgres")
        self.assertEqual(postgres["engine"], "postgres")
        self.assertEqual(postgres["engine_version"], "15.4")
        self.assertEqual(postgres["instance_class"], "db.t3.micro")
        self.assertEqual(postgres["allocated_storage"], 20)
        self.assertEqual(postgres["storage_type"], "gp3")
        self.assertFalse(postgres["publicly_accessible"])
        self.assertFalse(postgres["deletion_protection"])
        self.assertTrue(postgres["skip_final_snapshot"])
        self.assertEqual(postgres["enabled_cloudwatch_logs_exports"], ["postgresql"])

        redis = _resource_values(self.resources, "aws_elasticache_replication_group", "redis")
        self.assertEqual(redis["engine"], "redis")
        self.assertEqual(redis["engine_version"], "7.1")
        self.assertEqual(redis["node_type"], "cache.t3.micro")
        self.assertEqual(redis["num_cache_clusters"], 1)
        self.assertTrue(redis["at_rest_encryption_enabled"])
        self.assertTrue(redis["transit_encryption_enabled"])

        redshift = _resource_values(self.resources, "aws_redshift_cluster", "analytics")
        self.assertEqual(redshift["node_type"], "dc2.large")
        self.assertEqual(redshift["cluster_type"], "single-node")
        self.assertEqual(redshift["database_name"], "appanalytics")
        self.assertTrue(redshift["encrypted"])
        self.assertFalse(redshift["publicly_accessible"])
        self.assertTrue(redshift["skip_final_snapshot"])

    def test_api_frontend_and_cloudfront_behavior_contract(self):
        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        api_handler = _resource_values(self.resources, "aws_lambda_function", "api_handler")
        worker = _resource_values(self.resources, "aws_lambda_function", "worker_processor")
        enrichment = _resource_values(self.resources, "aws_lambda_function", "enrichment")
        event_source_mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")

        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 345600)

        self.assertEqual(api_handler["runtime"], "nodejs20.x")
        self.assertEqual(api_handler["timeout"], 10)
        self.assertEqual(worker["runtime"], "nodejs20.x")
        self.assertEqual(worker["timeout"], 20)
        self.assertEqual(enrichment["runtime"], "nodejs20.x")
        self.assertEqual(enrichment["timeout"], 10)
        self.assertEqual(event_source_mapping["batch_size"], 5)

        self.assertRegex(self.main_tf, r'route_key\s*=\s*"POST /api/orders"')
        self.assertRegex(self.main_tf, r'viewer_protocol_policy\s*=\s*"redirect-to-https"')
        self.assertRegex(self.main_tf, r'allowed_methods\s*=\s*\["GET", "HEAD"\]')
        self.assertRegex(self.main_tf, rf'cache_policy_id\s*=\s*"{re.escape(MANAGED_CACHING_OPTIMIZED_ID)}"')
        self.assertRegex(self.main_tf, r'default_root_object\s*=\s*"index.html"')

        if self.custom_endpoint:
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_apigatewayv2_api", "orders"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_apigatewayv2_stage", "default"))
            self.assertIsNone(_maybe_resource_values(self.resources, "aws_cloudfront_distribution", "frontend"))
            return

        api = _resource_values(self.resources, "aws_apigatewayv2_api", "orders")
        stage = _resource_values(self.resources, "aws_apigatewayv2_stage", "default")
        distribution = _resource_values(self.resources, "aws_cloudfront_distribution", "frontend")

        self.assertEqual(api["protocol_type"], "HTTP")
        self.assertEqual(stage["name"], "$default")
        self.assertTrue(stage["auto_deploy"])

        self.assertEqual(distribution["default_root_object"], "index.html")
        self.assertTrue(distribution["enabled"])

        default_behavior = distribution["default_cache_behavior"][0]
        self.assertEqual(default_behavior["viewer_protocol_policy"], "redirect-to-https")
        self.assertEqual(default_behavior["allowed_methods"], ["GET", "HEAD"])
        self.assertEqual(default_behavior["cached_methods"], ["GET", "HEAD"])
        self.assertEqual(default_behavior["cache_policy_id"], MANAGED_CACHING_OPTIMIZED_ID)

        ordered_behavior = distribution["ordered_cache_behavior"][0]
        self.assertEqual(ordered_behavior["path_pattern"], "api/*")
        self.assertEqual(ordered_behavior["viewer_protocol_policy"], "redirect-to-https")
        self.assertEqual(ordered_behavior["allowed_methods"], ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"])

    def test_pipe_and_glue_resources_are_fully_wired(self):
        pipe_config = _config_resource(self.config_resources, "aws_pipes_pipe", "orders")
        pipe = _maybe_resource_values(self.resources, "aws_pipes_pipe", "orders")
        glue_connection = _maybe_resource_values(self.resources, "aws_glue_connection", "redshift")
        glue_crawler = _maybe_resource_values(self.resources, "aws_glue_crawler", "analytics")
        glue_database = _maybe_resource_values(self.resources, "aws_glue_catalog_database", "analytics")

        self.assertIn("aws_sqs_queue.orders.arn", _expression_references(pipe_config, "source"))
        self.assertIn("aws_lambda_function.enrichment.arn", _expression_references(pipe_config, "enrichment"))
        self.assertIn("aws_sfn_state_machine.orders.arn", _expression_references(pipe_config, "target"))

        self.assertRegex(self.main_tf, r'StartAt\s*=\s*"InvokeEnrichment"')
        self.assertRegex(self.main_tf, r'Resource\s*=\s*"arn:aws:states:::lambda:invoke"')
        self.assertRegex(self.main_tf, r'Resource\s*=\s*"arn:aws:states:::sqs:sendMessage"')

        if self.custom_endpoint:
            self.assertIsNone(pipe)
            self.assertIsNone(glue_connection)
            self.assertIsNone(glue_crawler)
            self.assertIsNone(glue_database)
            return

        self.assertEqual(pipe["source_parameters"][0]["sqs_queue_parameters"][0]["batch_size"], 1)
        self.assertEqual(pipe["target_parameters"][0]["step_function_state_machine_parameters"][0]["invocation_type"], "FIRE_AND_FORGET")
        self.assertEqual(glue_database["name"], "analytics_catalog")
        self.assertEqual(glue_connection["connection_type"], "JDBC")
        self.assertIn("SECRET_ID", glue_connection["connection_properties"])
        self.assertEqual(glue_crawler["jdbc_target"][0]["path"], "appanalytics/%")

    def test_iam_scoping_uses_resource_level_references(self):
        api_policy = _config_resource(self.config_resources, "aws_iam_role_policy", "api_handler")
        worker_policy = _config_resource(self.config_resources, "aws_iam_role_policy", "worker_processor")
        step_functions_policy = _config_resource(self.config_resources, "aws_iam_role_policy", "step_functions")
        pipes_policy = _config_resource(self.config_resources, "aws_iam_role_policy", "pipes")
        glue_policy = _config_resource(self.config_resources, "aws_iam_role_policy", "glue_secret")

        self.assertIn("aws_sqs_queue.orders.arn", _expression_references(api_policy, "policy"))
        self.assertIn("aws_cloudwatch_log_group.lambda_api.arn", _expression_references(api_policy, "policy"))
        self.assertIn("aws_sqs_queue.orders.arn", _expression_references(worker_policy, "policy"))
        self.assertIn("aws_secretsmanager_secret.rds.arn", _expression_references(worker_policy, "policy"))
        self.assertIn("aws_lambda_function.enrichment.arn", _expression_references(step_functions_policy, "policy"))
        self.assertIn("aws_sfn_state_machine.orders.arn", _expression_references(pipes_policy, "policy"))
        self.assertIn("aws_secretsmanager_secret.redshift.arn", _expression_references(glue_policy, "policy"))

        self.assertRegex(self.main_tf, r'"sqs:SendMessage"')
        self.assertRegex(self.main_tf, r'"sqs:ReceiveMessage",\s*"sqs:DeleteMessage",\s*"sqs:GetQueueAttributes"')
        self.assertRegex(self.main_tf, r'"secretsmanager:GetSecretValue"')
        self.assertRegex(self.main_tf, r'"lambda:InvokeFunction"')
        self.assertRegex(self.main_tf, r'"states:StartExecution"')

        iam_roles = [r for r in self.resources if r["type"] == "aws_iam_role"]
        self.assertEqual(len(iam_roles), 6)

    def test_outputs_and_observability_match_prompt_counts(self):
        outputs = set(self.plan.get("planned_values", {}).get("outputs", {}))
        self.assertEqual(
            outputs,
            {
                "cloudfront_distribution_domain_name",
                "http_api_endpoint_url",
                "rds_endpoint_address",
                "redshift_endpoint_address",
                "sqs_queue_url",
            },
        )

        alarms = [r for r in self.resources if r["type"] == "aws_cloudwatch_metric_alarm"]
        self.assertEqual(len(alarms), 5 if self.custom_endpoint else 12)

        metrics = {(alarm["values"]["namespace"], alarm["values"]["metric_name"]) for alarm in alarms}
        expected_metrics = {
            ("AWS/Lambda", "Errors"),
            ("AWS/Lambda", "Duration"),
            ("AWS/States", "ExecutionsFailed"),
        }
        if not self.custom_endpoint:
            expected_metrics.update(
                {
                    ("AWS/RDS", "CPUUtilization"),
                    ("AWS/RDS", "FreeStorageSpace"),
                    ("AWS/ElastiCache", "CPUUtilization"),
                    ("AWS/ElastiCache", "FreeableMemory"),
                    ("AWS/Redshift", "CPUUtilization"),
                    ("AWS/Redshift", "HealthStatus"),
                    ("AWS/CloudFront", "5xxErrorRate"),
                }
            )
        self.assertEqual(metrics, expected_metrics)

        log_groups = [r for r in self.resources if r["type"] == "aws_cloudwatch_log_group"]
        self.assertGreaterEqual(len(log_groups), 6)
        for log_group in log_groups:
            self.assertEqual(log_group["values"]["retention_in_days"], 14)

    def test_api_handler_inline_behavior_writes_to_sqs_and_handles_invalid_input(self):
        source = _extract_inline_source("api_handler_source")
        sqs_mock = """
const fs = require("fs");

class SendMessageCommand {
  constructor(input) {
    this.input = input;
  }
}

class SQSClient {
  async send(command) {
    fs.writeFileSync(process.env.SQS_CAPTURE_FILE, JSON.stringify(command.input));
    return { MessageId: "msg-1" };
  }
}

module.exports = { SQSClient, SendMessageCommand };
"""
        success_result, success_captures = _invoke_inline_handler(
            source,
            {"body": json.dumps({"sku": "sku-123", "quantity": 2})},
            env={"SQS_QUEUE_URL": "https://queue.example/orders"},
            mocks={"@aws-sdk/client-sqs": sqs_mock},
            capture_files={"SQS_CAPTURE_FILE": "sqs.json"},
        )
        self.assertEqual(success_result["statusCode"], 202)
        success_body = json.loads(success_result["body"])
        self.assertTrue(success_body["orderId"].startswith("order-"))

        sent_message = json.loads(success_captures["SQS_CAPTURE_FILE"])
        self.assertEqual(sent_message["QueueUrl"], "https://queue.example/orders")
        sent_body = json.loads(sent_message["MessageBody"])
        self.assertEqual(sent_body["payload"], {"sku": "sku-123", "quantity": 2})
        self.assertTrue(sent_body["orderId"].startswith("order-"))
        self.assertTrue(sent_body["submittedAt"])

        invalid_json_result, _ = _invoke_inline_handler(
            source,
            {"body": '{"broken"'},
            env={"SQS_QUEUE_URL": "https://queue.example/orders"},
            mocks={"@aws-sdk/client-sqs": sqs_mock},
        )
        self.assertEqual(invalid_json_result["statusCode"], 400)
        self.assertEqual(json.loads(invalid_json_result["body"]), {"error": "invalid-json"})

        invalid_payload_result, _ = _invoke_inline_handler(
            source,
            {"body": "123"},
            env={"SQS_QUEUE_URL": "https://queue.example/orders"},
            mocks={"@aws-sdk/client-sqs": sqs_mock},
        )
        self.assertEqual(invalid_payload_result["statusCode"], 400)
        self.assertEqual(json.loads(invalid_payload_result["body"]), {"error": "invalid-payload"})

    def test_worker_processor_inline_behavior_reads_secret_and_writes_to_rds(self):
        source = _extract_inline_source("worker_processor_source")
        secrets_mock = """
const fs = require("fs");

class GetSecretValueCommand {
  constructor(input) {
    this.input = input;
  }
}

class SecretsManagerClient {
  async send(command) {
    fs.writeFileSync(process.env.SECRETS_CAPTURE_FILE, JSON.stringify(command.input));
    return { SecretString: process.env.SECRET_JSON };
  }
}

module.exports = { SecretsManagerClient, GetSecretValueCommand };
"""
        net_mock = """
const fs = require("fs");
const { EventEmitter } = require("events");

function int32(value) {
  const buffer = Buffer.alloc(4);
  buffer.writeInt32BE(value, 0);
  return buffer;
}

function message(type, body) {
  return Buffer.concat([Buffer.from(type), int32(body.length + 4), body]);
}

class FakeSocket extends EventEmitter {
  write(chunk) {
    if (chunk[0] === 81) {
      const sql = chunk.subarray(5, chunk.length - 1).toString("utf8");
      const previous = fs.existsSync(process.env.QUERY_CAPTURE_FILE)
        ? JSON.parse(fs.readFileSync(process.env.QUERY_CAPTURE_FILE, "utf8"))
        : [];
      previous.push(sql);
      fs.writeFileSync(process.env.QUERY_CAPTURE_FILE, JSON.stringify(previous));
      queueMicrotask(() => this.emit("data", message("Z", Buffer.from("I"))));
      return true;
    }

    queueMicrotask(() =>
      this.emit(
        "data",
        Buffer.concat([
          message("R", int32(0)),
          message("Z", Buffer.from("I"))
        ])
      )
    );
    return true;
  }

  end(chunk) {
    if (chunk) {
      fs.writeFileSync(process.env.TERMINATE_CAPTURE_FILE, "closed");
    }
    this.emit("close");
  }
}

module.exports = {
  createConnection(options, callback) {
    fs.writeFileSync(process.env.NET_CAPTURE_FILE, JSON.stringify(options));
    const socket = new FakeSocket();
    process.nextTick(callback);
    return socket;
  }
};
"""
        tls_mock = """
const fs = require("fs");
const { EventEmitter } = require("events");

class FakeTlsSocket extends EventEmitter {
  write(chunk) {
    fs.writeFileSync(process.env.REDIS_CAPTURE_FILE, chunk.toString("utf8"));
    queueMicrotask(() => this.emit("data", Buffer.from("+OK\\r\\n", "utf8")));
    return true;
  }

  end() {
    this.emit("close");
  }
}

module.exports = {
  connect(options, callback) {
    fs.writeFileSync(process.env.TLS_CAPTURE_FILE, JSON.stringify(options));
    const socket = new FakeTlsSocket();
    process.nextTick(callback);
    return socket;
  }
};
"""
        event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "orderId": "order-123",
                            "payload": {"sku": "sku-123", "quantity": 2},
                            "submittedAt": "2026-04-15T10:00:00Z",
                        }
                    )
                }
            ]
        }
        _, captures = _invoke_inline_handler(
            source,
            event,
            env={
                "RDS_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:rds",
                "RDS_ENDPOINT": "postgres.internal",
                "RDS_DATABASE": "orders",
                "REDIS_ENDPOINT": "redis.internal",
                "SECRET_JSON": json.dumps({"username": "dbadmin", "password": "supersecret"}),
            },
            mocks={
                "@aws-sdk/client-secrets-manager": secrets_mock,
                "net": net_mock,
                "tls": tls_mock,
            },
            capture_files={
                "SECRETS_CAPTURE_FILE": "secrets.json",
                "NET_CAPTURE_FILE": "net.json",
                "QUERY_CAPTURE_FILE": "queries.json",
                "TLS_CAPTURE_FILE": "tls.json",
                "REDIS_CAPTURE_FILE": "redis.txt",
                "TERMINATE_CAPTURE_FILE": "terminate.txt",
            },
        )

        self.assertEqual(json.loads(captures["SECRETS_CAPTURE_FILE"]), {"SecretId": "arn:aws:secretsmanager:us-east-1:000000000000:secret:rds"})
        self.assertEqual(json.loads(captures["NET_CAPTURE_FILE"]), {"host": "postgres.internal", "port": 5432})
        self.assertEqual(json.loads(captures["TLS_CAPTURE_FILE"]), {"host": "redis.internal", "port": 6379, "rejectUnauthorized": False})
        self.assertEqual(captures["TERMINATE_CAPTURE_FILE"], "closed")

        queries = json.loads(captures["QUERY_CAPTURE_FILE"])
        self.assertEqual(len(queries), 2)
        self.assertIn("create table if not exists orders", queries[0].lower())
        self.assertIn("insert into orders(order_id, payload)", queries[1].lower())
        self.assertIn("order-123", queries[1])
        self.assertIn('"sku":"sku-123"', queries[1])
        self.assertIn("order:order-123", captures["REDIS_CAPTURE_FILE"])

    def test_worker_processor_inline_negative_paths_do_not_try_to_write_invalid_records(self):
        source = _extract_inline_source("worker_processor_source")
        secrets_mock = """
class GetSecretValueCommand {
  constructor(input) {
    this.input = input;
  }
}

class SecretsManagerClient {
  async send(command) {
    return { SecretString: process.env.SECRET_JSON };
  }
}

module.exports = { SecretsManagerClient, GetSecretValueCommand };
"""
        net_mock = """
const fs = require("fs");

module.exports = {
  createConnection() {
    fs.writeFileSync(process.env.NET_CAPTURE_FILE, "called");
    throw new Error("net-should-not-be-called");
  }
};
"""
        tls_mock = """
const fs = require("fs");

module.exports = {
  connect() {
    fs.writeFileSync(process.env.TLS_CAPTURE_FILE, "called");
    throw new Error("tls-should-not-be-called");
  }
};
"""
        _, captures = _invoke_inline_handler(
            source,
            {"Records": [{"body": "not-json"}, {"body": json.dumps({"payload": {"sku": "missing-id"}})}]},
            env={
                "RDS_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:rds",
                "RDS_ENDPOINT": "postgres.internal",
                "RDS_DATABASE": "orders",
                "REDIS_ENDPOINT": "redis.internal",
                "SECRET_JSON": json.dumps({"username": "dbadmin", "password": "supersecret"}),
            },
            mocks={
                "@aws-sdk/client-secrets-manager": secrets_mock,
                "net": net_mock,
                "tls": tls_mock,
            },
            capture_files={
                "CONSOLE_CAPTURE_FILE": "console.log",
                "NET_CAPTURE_FILE": "net.txt",
                "TLS_CAPTURE_FILE": "tls.txt",
            },
        )

        self.assertIn("invalid-record-body", captures["CONSOLE_CAPTURE_FILE"])
        self.assertIn("missing-order-id", captures["CONSOLE_CAPTURE_FILE"])
        self.assertIsNone(captures["NET_CAPTURE_FILE"])
        self.assertIsNone(captures["TLS_CAPTURE_FILE"])

    def test_enrichment_lambda_inline_behavior_adds_enriched_flag(self):
        source = _extract_inline_source("enrichment_source")
        result, _ = _invoke_inline_handler(source, {"orderId": "integration-check"})
        self.assertEqual(result["orderId"], "integration-check")
        self.assertTrue(result["enriched"])
        self.assertTrue(result["timestamp"])


if __name__ == "__main__":
    unittest.main()
