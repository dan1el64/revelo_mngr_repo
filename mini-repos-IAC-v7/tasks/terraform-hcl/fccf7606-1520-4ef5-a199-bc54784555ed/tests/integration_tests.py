import json
import os
import pathlib
import time
import urllib.error
import urllib.request
import unittest

import boto3


ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"
RAW_STATE = ROOT / "terraform.tfstate"


def _load_state_document():
    if STATE_JSON.exists():
        return json.loads(STATE_JSON.read_text())
    if RAW_STATE.exists():
        return json.loads(RAW_STATE.read_text())
    raise AssertionError("No Terraform state found. Expected state.json or terraform.tfstate after terraform apply.")


def _flatten_show_json(module):
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_flatten_show_json(child))
    return resources


def _flatten_raw_state(module):
    resources = []
    module_path = module.get("path", [])
    for resource in module.get("resources", module.get("values", {}).get("root_module", {}).get("resources", [])):
        for instance in resource.get("instances", []):
            resources.append(
                {
                    "address": resource.get("address", ".".join(module_path + [resource["type"], resource["name"]]).strip(".")),
                    "type": resource["type"],
                    "name": resource["name"],
                    "values": instance.get("attributes", {}),
                }
            )
    for child in module.get("child_modules", []):
        resources.extend(_flatten_raw_state(child))
    return resources


def _resources_and_outputs(document):
    if "values" in document:
        root_module = document.get("values", {}).get("root_module", {})
        resources = _flatten_show_json(root_module)
        outputs = document.get("values", {}).get("outputs", {})
        return resources, outputs

    resources = _flatten_raw_state(document)
    outputs = document.get("outputs", {})
    return resources, outputs


def _resource_values(resources, resource_type, resource_name):
    for resource in resources:
        if resource["type"] == resource_type and resource["name"] == resource_name:
            return resource["values"]
    raise AssertionError(f"Resource {resource_type}.{resource_name} not found in Terraform state")


def _find_resource(resources, resource_type, resource_name):
    for resource in resources:
        if resource["type"] == resource_type and resource["name"] == resource_name:
            return resource["values"]
    return None


def _output_value(outputs, name, default=None):
    if name not in outputs:
        return default
    value = outputs[name]
    return value["value"] if isinstance(value, dict) and "value" in value else value


def _aws_endpoint():
    return os.environ.get("TF_VAR_aws_endpoint") or os.environ.get("AWS_ENDPOINT_URL") or ""


def _aws_region():
    return os.environ.get("TF_VAR_aws_region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def _lambda_client():
    kwargs = {"region_name": _aws_region()}
    endpoint = _aws_endpoint()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("lambda", **kwargs)


def _post_json(url, payload, *, expect_error=False):
    request = urllib.request.Request(
        url,
        data=payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if not expect_error:
            raise
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _post_with_retry(url, payload, *, expect_error=False, attempts=5):
    last_error = None
    for _ in range(attempts):
        try:
            return _post_json(url, payload, expect_error=expect_error)
        except Exception as exc:  # pragma: no cover
            last_error = exc
            time.sleep(2)
    raise last_error


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = _load_state_document()
        cls.resources, cls.outputs = _resources_and_outputs(cls.document)
        cls.local_mode = bool(_aws_endpoint())
        cls.http_api_url = _output_value(cls.outputs, "http_api_endpoint_url")

    def test_frontend_stack_is_deployed(self):
        bucket = _resource_values(self.resources, "aws_s3_bucket", "frontend")
        versioning = _resource_values(self.resources, "aws_s3_bucket_versioning", "frontend")
        public_access = _resource_values(self.resources, "aws_s3_bucket_public_access_block", "frontend")

        self.assertTrue(bucket["force_destroy"])
        self.assertEqual(versioning["versioning_configuration"][0]["status"], "Enabled")
        self.assertTrue(public_access["block_public_acls"])
        self.assertTrue(public_access["block_public_policy"])

        objects = {r["values"]["key"] for r in self.resources if r["type"] == "aws_s3_object"}
        self.assertEqual(objects, {"index.html", "app.js"})

        distribution = _find_resource(self.resources, "aws_cloudfront_distribution", "frontend")
        oac = [r for r in self.resources if r["type"] == "aws_cloudfront_origin_access_control"]
        if self.local_mode:
            self.assertIsNone(distribution)
            self.assertEqual(len(oac), 0)
        else:
            self.assertEqual(distribution["default_root_object"], "index.html")
            self.assertTrue(distribution["enabled"])
            self.assertEqual(len(oac), 1)

    def test_network_and_private_access_controls(self):
        endpoints = [r["values"] for r in self.resources if r["type"] == "aws_vpc_endpoint"]
        self.assertEqual(len(endpoints), 3)

        service_suffixes = {endpoint["service_name"].rsplit(".", 1)[-1] for endpoint in endpoints}
        self.assertEqual(service_suffixes, {"logs", "secretsmanager", "sqs"})

        for endpoint in endpoints:
            self.assertEqual(endpoint["vpc_endpoint_type"], "Interface")
            self.assertEqual(len(endpoint["subnet_ids"]), 2)
            self.assertEqual(len(endpoint["security_group_ids"]), 1)

        route_tables = [r for r in self.resources if r["type"] == "aws_route_table"]
        self.assertEqual(len(route_tables), 2)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))

        if not self.local_mode:
            db_instance = _resource_values(self.resources, "aws_db_instance", "postgres")
            redshift = _resource_values(self.resources, "aws_redshift_cluster", "analytics")
            self.assertFalse(db_instance["publicly_accessible"])
            self.assertFalse(redshift["publicly_accessible"])

    def test_data_stores_are_deployed_not_stubbed(self):
        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_secretsmanager_secret"]), 2)
        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_secretsmanager_secret_version"]), 2)

        if self.local_mode:
            self.assertEqual(len([r for r in self.resources if r["type"] == "aws_db_subnet_group"]), 0)
            self.assertEqual(len([r for r in self.resources if r["type"] == "aws_elasticache_subnet_group"]), 0)
            self.assertEqual(len([r for r in self.resources if r["type"] == "aws_redshift_subnet_group"]), 0)
            return

        db_instance = _resource_values(self.resources, "aws_db_instance", "postgres")
        redis = _resource_values(self.resources, "aws_elasticache_replication_group", "redis")
        redshift = _resource_values(self.resources, "aws_redshift_cluster", "analytics")

        self.assertEqual(db_instance["engine"], "postgres")
        self.assertEqual(db_instance["engine_version"], "15.4")
        self.assertEqual(db_instance["instance_class"], "db.t3.micro")
        self.assertEqual(db_instance["allocated_storage"], 20)
        self.assertTrue(db_instance["skip_final_snapshot"])
        self.assertFalse(db_instance["deletion_protection"])

        self.assertEqual(redis["engine"], "redis")
        self.assertEqual(redis["engine_version"], "7.1")
        self.assertEqual(redis["node_type"], "cache.t3.micro")
        self.assertTrue(redis["at_rest_encryption_enabled"])
        self.assertTrue(redis["transit_encryption_enabled"])

        self.assertEqual(redshift["cluster_type"], "single-node")
        self.assertEqual(redshift["database_name"], "appanalytics")
        self.assertEqual(redshift["node_type"], "dc2.large")
        self.assertTrue(redshift["encrypted"])
        self.assertTrue(redshift["skip_final_snapshot"])

    def test_api_queue_and_lambda_integration_resources(self):
        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")

        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 345600)
        self.assertEqual(mapping["batch_size"], 5)

        lambdas = [r["values"] for r in self.resources if r["type"] == "aws_lambda_function"]
        self.assertEqual(len(lambdas), 3)

        runtimes = {lambda_fn["runtime"] for lambda_fn in lambdas}
        self.assertEqual(runtimes, {"nodejs20.x"})

        timeouts = {lambda_fn["function_name"]: lambda_fn["timeout"] for lambda_fn in lambdas}
        self.assertEqual(timeouts["api-handler"], 10)
        self.assertEqual(timeouts["worker-processor"], 20)
        self.assertEqual(timeouts["enrichment-lambda"], 10)

        api = _find_resource(self.resources, "aws_apigatewayv2_api", "orders")
        stage = _find_resource(self.resources, "aws_apigatewayv2_stage", "default")
        if self.local_mode:
            self.assertIsNone(api)
            self.assertIsNone(stage)
        else:
            self.assertEqual(api["protocol_type"], "HTTP")
            self.assertEqual(stage["name"], "$default")
            self.assertTrue(stage["auto_deploy"])

    def test_http_api_accepts_valid_json_payload(self):
        if self.local_mode or not self.http_api_url or self.http_api_url == "endpoint-disabled":
            self.assertEqual(self.http_api_url, "endpoint-disabled")
            self.assertIsNone(_find_resource(self.resources, "aws_apigatewayv2_api", "orders"))
            return

        status, payload = _post_with_retry(f"{self.http_api_url}/api/orders", {"sku": "demo", "quantity": 1})
        self.assertEqual(status, 202)
        self.assertIn("orderId", payload)
        self.assertTrue(payload["orderId"])

    def test_http_api_rejects_invalid_json_payload(self):
        if self.local_mode or not self.http_api_url or self.http_api_url == "endpoint-disabled":
            self.assertEqual(self.http_api_url, "endpoint-disabled")
            self.assertIsNone(_find_resource(self.resources, "aws_apigatewayv2_api", "orders"))
            return

        status, payload = _post_with_retry(f"{self.http_api_url}/api/orders", b'{"broken"', expect_error=True)
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "invalid-json"})

    def test_enrichment_lambda_invocation_is_behavioral(self):
        enrichment = _resource_values(self.resources, "aws_lambda_function", "enrichment")
        response = _lambda_client().invoke(
            FunctionName=enrichment["function_name"],
            Payload=json.dumps({"orderId": "integration-check"}).encode("utf-8"),
        )
        payload = json.loads(response["Payload"].read().decode("utf-8"))
        self.assertTrue(payload["enriched"])
        self.assertEqual(payload["orderId"], "integration-check")
        self.assertTrue(payload["timestamp"])

    def test_state_machine_glue_and_outputs(self):
        state_machine = _resource_values(self.resources, "aws_sfn_state_machine", "orders")
        definition = json.loads(state_machine["definition"])

        self.assertEqual(state_machine["type"], "STANDARD")
        self.assertEqual(definition["StartAt"], "InvokeEnrichment")
        self.assertEqual(definition["States"]["InvokeEnrichment"]["Resource"], "arn:aws:states:::lambda:invoke")
        self.assertEqual(definition["States"]["SendToQueue"]["Resource"], "arn:aws:states:::sqs:sendMessage")

        pipe = _find_resource(self.resources, "aws_pipes_pipe", "orders")
        glue_connections = [r for r in self.resources if r["type"] == "aws_glue_connection"]
        glue_crawlers = [r for r in self.resources if r["type"] == "aws_glue_crawler"]
        glue_databases = [r for r in self.resources if r["type"] == "aws_glue_catalog_database"]

        if self.local_mode:
            self.assertIsNone(pipe)
            self.assertEqual(len(glue_connections), 0)
            self.assertEqual(len(glue_crawlers), 0)
            self.assertEqual(len(glue_databases), 0)
        else:
            self.assertEqual(pipe["source_parameters"][0]["sqs_queue_parameters"][0]["batch_size"], 1)
            self.assertEqual(pipe["target_parameters"][0]["step_function_state_machine_parameters"][0]["invocation_type"], "FIRE_AND_FORGET")
            self.assertEqual(len(glue_connections), 1)
            self.assertEqual(len(glue_crawlers), 1)
            self.assertEqual(len(glue_databases), 1)

        required_outputs = {
            "cloudfront_distribution_domain_name",
            "http_api_endpoint_url",
            "rds_endpoint_address",
            "redshift_endpoint_address",
            "sqs_queue_url",
        }
        self.assertEqual(set(self.outputs), required_outputs)
        for name, value in self.outputs.items():
            actual = value["value"] if isinstance(value, dict) and "value" in value else value
            self.assertIsNotNone(actual)
            if name == "sqs_queue_url":
                self.assertNotEqual(actual, "endpoint-disabled")
            elif self.local_mode:
                self.assertEqual(actual, "endpoint-disabled")
            else:
                self.assertNotEqual(actual, "endpoint-disabled")

    def test_alarms_and_roles_match_prompt_counts(self):
        alarms = [r["values"] for r in self.resources if r["type"] == "aws_cloudwatch_metric_alarm"]
        iam_roles = [r for r in self.resources if r["type"] == "aws_iam_role"]

        self.assertEqual(len(alarms), 5 if self.local_mode else 12)
        self.assertEqual(len(iam_roles), 6)

        metrics = {(alarm["namespace"], alarm["metric_name"]) for alarm in alarms}
        self.assertIn(("AWS/Lambda", "Errors"), metrics)
        self.assertIn(("AWS/Lambda", "Duration"), metrics)
        self.assertIn(("AWS/States", "ExecutionsFailed"), metrics)
        if not self.local_mode:
            self.assertIn(("AWS/RDS", "CPUUtilization"), metrics)
            self.assertIn(("AWS/RDS", "FreeStorageSpace"), metrics)
            self.assertIn(("AWS/ElastiCache", "CPUUtilization"), metrics)
            self.assertIn(("AWS/ElastiCache", "FreeableMemory"), metrics)
            self.assertIn(("AWS/Redshift", "CPUUtilization"), metrics)
            self.assertIn(("AWS/Redshift", "HealthStatus"), metrics)
            self.assertIn(("AWS/CloudFront", "5xxErrorRate"), metrics)


if __name__ == "__main__":
    unittest.main()
