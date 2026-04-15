import json
import os
import pathlib
import time
import unittest
import uuid

import boto3


ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"
RAW_STATE = ROOT / "terraform.tfstate"
MANAGED_CACHING_OPTIMIZED_ID = "658327ea-f89d-4fab-a63d-7e88639e58f6"


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


def _maybe_resource_values(resources, resource_type, resource_name):
    for resource in resources:
        if resource["type"] == resource_type and resource["name"] == resource_name:
            return resource["values"]
    return None


def _output_value(outputs, name):
    value = outputs[name]
    return value["value"] if isinstance(value, dict) and "value" in value else value


def _lambda_environment_variables(lambda_resource):
    environment = lambda_resource.get("environment")
    if isinstance(environment, list) and environment:
        return environment[0].get("variables", {})
    if isinstance(environment, dict):
        return environment.get("variables", {})
    return {}


def _aws_endpoint():
    return os.environ.get("TF_VAR_aws_endpoint") or os.environ.get("AWS_ENDPOINT_URL") or ""


def _using_custom_endpoint():
    return bool(_aws_endpoint())


def _aws_region():
    return os.environ.get("TF_VAR_aws_region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def _client(service_name):
    kwargs = {"region_name": _aws_region()}
    endpoint = _aws_endpoint()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service_name, **kwargs)


def _api_gateway_event(body):
    return {
        "version": "2.0",
        "routeKey": "POST /api/orders",
        "rawPath": "/api/orders",
        "headers": {"content-type": "application/json"},
        "requestContext": {"http": {"method": "POST", "path": "/api/orders"}},
        "body": json.dumps(body) if not isinstance(body, str) else body,
    }


def _invoke_lambda(lambda_client, function_name, payload):
    response = lambda_client.invoke(FunctionName=function_name, Payload=json.dumps(payload).encode("utf-8"))
    raw_payload = response["Payload"].read().decode("utf-8")
    return json.loads(raw_payload) if raw_payload else None


def _set_event_source_mapping_enabled(lambda_client, mapping_uuid, enabled):
    lambda_client.update_event_source_mapping(UUID=mapping_uuid, Enabled=enabled)
    deadline = time.time() + 60
    expected_state = "Enabled" if enabled else "Disabled"

    while time.time() < deadline:
        mapping = lambda_client.get_event_source_mapping(UUID=mapping_uuid)
        state = mapping.get("State", "")
        if state == expected_state or (not enabled and state in {"Disabling", "Disabled"}):
            return
        time.sleep(2)

    raise AssertionError(f"Event source mapping {mapping_uuid} did not reach {expected_state}")


def _receive_matching_message(sqs_client, queue_url, predicate, timeout_seconds=30):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=20,
        )
        for message in response.get("Messages", []):
            body = json.loads(message["Body"])
            if predicate(body):
                return message, body
        time.sleep(1)
    raise AssertionError("Timed out waiting for expected SQS message")


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = _load_state_document()
        cls.resources, cls.outputs = _resources_and_outputs(cls.document)
        cls.lambda_client = _client("lambda")
        cls.sqs_client = _client("sqs")
        cls.secrets_client = _client("secretsmanager")

    def test_frontend_stack_and_cloudfront_behavior_are_deployed(self):
        bucket = _resource_values(self.resources, "aws_s3_bucket", "frontend")
        versioning = _resource_values(self.resources, "aws_s3_bucket_versioning", "frontend")
        public_access = _resource_values(self.resources, "aws_s3_bucket_public_access_block", "frontend")
        distribution = _maybe_resource_values(self.resources, "aws_cloudfront_distribution", "frontend")
        oac = _maybe_resource_values(self.resources, "aws_cloudfront_origin_access_control", "frontend")

        self.assertTrue(bucket["force_destroy"])
        self.assertEqual(versioning["versioning_configuration"][0]["status"], "Enabled")
        self.assertTrue(public_access["block_public_acls"])
        self.assertTrue(public_access["block_public_policy"])

        objects = {r["values"]["key"] for r in self.resources if r["type"] == "aws_s3_object"}
        self.assertEqual(objects, {"index.html", "app.js"})

        if _using_custom_endpoint():
            self.assertIsNone(distribution)
            self.assertIsNone(oac)
            self.assertEqual(_output_value(self.outputs, "cloudfront_distribution_domain_name"), "endpoint-disabled")
            return

        self.assertEqual(oac["signing_behavior"], "always")
        self.assertEqual(oac["signing_protocol"], "sigv4")
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

    def test_network_private_access_and_endpoint_security_group_contract(self):
        endpoints = [r["values"] for r in self.resources if r["type"] == "aws_vpc_endpoint"]
        lambda_sg = _resource_values(self.resources, "aws_security_group", "lambda")
        rds_sg = _resource_values(self.resources, "aws_security_group", "rds")
        redis_sg = _resource_values(self.resources, "aws_security_group", "redis")
        db_instance = _maybe_resource_values(self.resources, "aws_db_instance", "postgres")
        redshift = _maybe_resource_values(self.resources, "aws_redshift_cluster", "analytics")

        self.assertEqual(len(endpoints), 3)
        self.assertEqual({endpoint["service_name"].rsplit(".", 1)[-1] for endpoint in endpoints}, {"logs", "secretsmanager", "sqs"})
        for endpoint in endpoints:
            self.assertEqual(endpoint["vpc_endpoint_type"], "Interface")
            self.assertEqual(len(endpoint["subnet_ids"]), 2)
            self.assertEqual(len(endpoint["security_group_ids"]), 1)

        self.assertEqual(lambda_sg["ingress"][0]["from_port"], 443)
        self.assertEqual(lambda_sg["ingress"][0]["to_port"], 443)
        self.assertTrue(lambda_sg["ingress"][0]["self"])
        self.assertEqual(rds_sg["ingress"][0]["from_port"], 5432)
        self.assertEqual(redis_sg["ingress"][0]["from_port"], 6379)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))
        if _using_custom_endpoint():
            self.assertIsNone(db_instance)
            self.assertIsNone(redshift)
            return

        self.assertFalse(db_instance["publicly_accessible"])
        self.assertFalse(redshift["publicly_accessible"])

    def test_data_stores_secrets_and_glue_connection_use_secret_arns(self):
        rds_secret = _resource_values(self.resources, "aws_secretsmanager_secret", "rds")
        redshift_secret = _resource_values(self.resources, "aws_secretsmanager_secret", "redshift")
        worker_lambda = _resource_values(self.resources, "aws_lambda_function", "worker_processor")
        glue_connection = _maybe_resource_values(self.resources, "aws_glue_connection", "redshift")
        redshift_cluster = _maybe_resource_values(self.resources, "aws_redshift_cluster", "analytics")

        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_secretsmanager_secret"]), 2)
        self.assertEqual(len([r for r in self.resources if r["type"] == "aws_secretsmanager_secret_version"]), 2)

        rds_secret_value = json.loads(self.secrets_client.get_secret_value(SecretId=rds_secret["arn"])["SecretString"])
        redshift_secret_value = json.loads(self.secrets_client.get_secret_value(SecretId=redshift_secret["arn"])["SecretString"])
        self.assertEqual(set(rds_secret_value), {"username", "password"})
        self.assertEqual(set(redshift_secret_value), {"username", "password"})

        environment_variables = _lambda_environment_variables(worker_lambda)
        self.assertEqual(environment_variables["RDS_SECRET_ARN"], rds_secret["arn"])
        self.assertEqual(environment_variables["RDS_DATABASE"], "orders")
        if _using_custom_endpoint():
            self.assertEqual(environment_variables["RDS_ENDPOINT"], "endpoint-disabled")
            self.assertEqual(environment_variables["REDIS_ENDPOINT"], "endpoint-disabled")
            self.assertIsNone(glue_connection)
            self.assertIsNone(redshift_cluster)
            self.assertEqual(_output_value(self.outputs, "rds_endpoint_address"), "endpoint-disabled")
            self.assertEqual(_output_value(self.outputs, "redshift_endpoint_address"), "endpoint-disabled")
            return

        self.assertEqual(glue_connection["connection_properties"]["SECRET_ID"], redshift_secret["arn"])
        self.assertIn(redshift_cluster["endpoint"], glue_connection["connection_properties"]["JDBC_CONNECTION_URL"])

    def test_api_handler_direct_invocation_returns_202_and_writes_to_sqs(self):
        api_handler = _resource_values(self.resources, "aws_lambda_function", "api_handler")
        event_source_mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")
        queue_url = _output_value(self.outputs, "sqs_queue_url")
        trace_id = f"trace-{uuid.uuid4()}"

        _set_event_source_mapping_enabled(self.lambda_client, event_source_mapping["uuid"], False)
        try:
            response = _invoke_lambda(
                self.lambda_client,
                api_handler["function_name"],
                _api_gateway_event({"sku": "demo", "quantity": 1, "traceId": trace_id}),
            )
            self.assertEqual(response["statusCode"], 202)

            payload = json.loads(response["body"])
            self.assertIn("orderId", payload)
            self.assertTrue(payload["orderId"].startswith("order-"))

            message, body = _receive_matching_message(
                self.sqs_client,
                queue_url,
                lambda candidate: candidate.get("orderId") == payload["orderId"] and candidate.get("payload", {}).get("traceId") == trace_id,
            )
            self.assertEqual(body["payload"], {"sku": "demo", "quantity": 1, "traceId": trace_id})
            self.assertTrue(body["submittedAt"])
            self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        finally:
            _set_event_source_mapping_enabled(self.lambda_client, event_source_mapping["uuid"], True)

    def test_api_handler_rejects_invalid_json(self):
        api_handler = _resource_values(self.resources, "aws_lambda_function", "api_handler")
        response = _invoke_lambda(self.lambda_client, api_handler["function_name"], _api_gateway_event('{"broken"'))
        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(json.loads(response["body"]), {"error": "invalid-json"})

    def test_enrichment_lambda_invocation_is_behavioral(self):
        enrichment = _resource_values(self.resources, "aws_lambda_function", "enrichment")
        payload = _invoke_lambda(self.lambda_client, enrichment["function_name"], {"orderId": "integration-check"})
        self.assertEqual(payload["orderId"], "integration-check")
        self.assertTrue(payload["enriched"])
        self.assertTrue(payload["timestamp"])

    def test_state_machine_pipe_and_outputs_are_wired_end_to_end(self):
        state_machine = _resource_values(self.resources, "aws_sfn_state_machine", "orders")
        pipe = _maybe_resource_values(self.resources, "aws_pipes_pipe", "orders")
        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        enrichment = _resource_values(self.resources, "aws_lambda_function", "enrichment")
        required_outputs = {
            "cloudfront_distribution_domain_name",
            "http_api_endpoint_url",
            "rds_endpoint_address",
            "redshift_endpoint_address",
            "sqs_queue_url",
        }

        definition = json.loads(state_machine["definition"])
        self.assertEqual(state_machine["type"], "STANDARD")
        self.assertEqual(definition["StartAt"], "InvokeEnrichment")
        self.assertEqual(definition["States"]["InvokeEnrichment"]["Resource"], "arn:aws:states:::lambda:invoke")
        self.assertEqual(definition["States"]["SendToQueue"]["Resource"], "arn:aws:states:::sqs:sendMessage")
        self.assertEqual(set(self.outputs), required_outputs)

        if _using_custom_endpoint():
            self.assertIsNone(pipe)
            self.assertEqual(_output_value(self.outputs, "http_api_endpoint_url"), "endpoint-disabled")
            self.assertEqual(_output_value(self.outputs, "cloudfront_distribution_domain_name"), "endpoint-disabled")
            self.assertEqual(_output_value(self.outputs, "rds_endpoint_address"), "endpoint-disabled")
            self.assertEqual(_output_value(self.outputs, "redshift_endpoint_address"), "endpoint-disabled")
            self.assertTrue(_output_value(self.outputs, "sqs_queue_url"))
            return

        self.assertEqual(pipe["source"], queue["arn"])
        self.assertEqual(pipe["enrichment"], enrichment["arn"])
        self.assertEqual(pipe["target"], state_machine["arn"])
        self.assertEqual(pipe["source_parameters"][0]["sqs_queue_parameters"][0]["batch_size"], 1)
        self.assertEqual(pipe["target_parameters"][0]["step_function_state_machine_parameters"][0]["invocation_type"], "FIRE_AND_FORGET")
        for output_name in required_outputs:
            self.assertTrue(_output_value(self.outputs, output_name))

    def test_alarms_and_roles_match_prompt_counts(self):
        alarms = [r["values"] for r in self.resources if r["type"] == "aws_cloudwatch_metric_alarm"]
        iam_roles = [r for r in self.resources if r["type"] == "aws_iam_role"]

        self.assertEqual(len(alarms), 5 if _using_custom_endpoint() else 12)
        self.assertEqual(len(iam_roles), 6)

        metrics = {(alarm["namespace"], alarm["metric_name"]) for alarm in alarms}
        expected_metrics = {
            ("AWS/Lambda", "Errors"),
            ("AWS/Lambda", "Duration"),
            ("AWS/States", "ExecutionsFailed"),
        }
        if not _using_custom_endpoint():
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


if __name__ == "__main__":
    unittest.main()
