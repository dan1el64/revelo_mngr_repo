import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_JSON = ROOT / "state.json"
RAW_STATE = ROOT / "terraform.tfstate"
MAIN_TF = ROOT / "main.tf"


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


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = _load_state_document()
        cls.resources, cls.outputs = _resources_and_outputs(cls.document)
        cls.main_tf = MAIN_TF.read_text()

    def test_s3_bucket_and_frontend_assets(self):
        bucket = _resource_values(self.resources, "aws_s3_bucket", "frontend")
        versioning = _resource_values(self.resources, "aws_s3_bucket_versioning", "frontend")
        public_access = _resource_values(self.resources, "aws_s3_bucket_public_access_block", "frontend")

        self.assertTrue(bucket["force_destroy"])
        self.assertEqual(versioning["versioning_configuration"][0]["status"], "Enabled")
        self.assertTrue(public_access["block_public_acls"])
        self.assertTrue(public_access["block_public_policy"])

        objects = [r["values"]["key"] for r in self.resources if r["type"] == "aws_s3_object"]
        self.assertEqual(set(objects), {"index.html", "app.js"})
        self.assertIn('resource "aws_cloudfront_distribution" "frontend"', self.main_tf)
        self.assertIn('resource "aws_cloudfront_origin_access_control" "frontend"', self.main_tf)

    def test_vpc_and_interface_endpoints_configuration(self):
        endpoints = [r["values"] for r in self.resources if r["type"] == "aws_vpc_endpoint"]
        self.assertEqual(len(endpoints), 3)

        service_names = {endpoint["service_name"] for endpoint in endpoints}
        # Verify the required services without hardcoding the region
        service_suffixes = {sn.rsplit(".", 1)[-1] for sn in service_names}
        self.assertEqual(service_suffixes, {"logs", "secretsmanager", "sqs"})

        for endpoint in endpoints:
            self.assertEqual(endpoint["vpc_endpoint_type"], "Interface")
            self.assertEqual(len(endpoint["subnet_ids"]), 2)
            self.assertEqual(len(endpoint["security_group_ids"]), 1)

        route_tables = [r for r in self.resources if r["type"] == "aws_route_table"]
        self.assertEqual(len(route_tables), 2)
        self.assertFalse(any(r["type"] == "aws_nat_gateway" for r in self.resources))

    def test_data_store_contract_declared_in_source(self):
        for expected in [
            'resource "aws_db_instance" "postgres"',
            'resource "aws_elasticache_replication_group" "redis"',
            'resource "aws_redshift_cluster" "analytics"',
            'engine                          = "postgres"',
            'engine_version                  = "15.4"',
            'engine                     = "redis"',
            'engine_version             = "7.1"',
            'database_name             = "appanalytics"',
        ]:
            self.assertIn(expected, self.main_tf)

    def test_api_lambda_queue_and_outputs(self):
        queue = _resource_values(self.resources, "aws_sqs_queue", "orders")
        mapping = _resource_values(self.resources, "aws_lambda_event_source_mapping", "worker_orders")

        self.assertEqual(queue["visibility_timeout_seconds"], 30)
        self.assertEqual(queue["message_retention_seconds"], 345600)
        self.assertEqual(mapping["batch_size"], 5)
        self.assertIn('resource "aws_apigatewayv2_api" "orders"', self.main_tf)
        self.assertIn('route_key = "POST /api/orders"', self.main_tf)
        self.assertIn('auto_deploy = true', self.main_tf)

        required_outputs = {
            "cloudfront_distribution_domain_name",
            "http_api_endpoint_url",
            "rds_endpoint_address",
            "redshift_endpoint_address",
            "sqs_queue_url",
        }
        self.assertEqual(set(self.outputs), required_outputs)
        for value in self.outputs.values():
            actual = value["value"] if isinstance(value, dict) and "value" in value else value
            self.assertIsNotNone(actual)
        cloudfront_output = self.outputs["cloudfront_distribution_domain_name"]
        actual_cloudfront = cloudfront_output["value"] if isinstance(cloudfront_output, dict) else cloudfront_output
        self.assertTrue(actual_cloudfront)

    def test_state_machine_configuration(self):
        state_machine = _resource_values(self.resources, "aws_sfn_state_machine", "orders")
        definition = json.loads(state_machine["definition"])

        self.assertEqual(state_machine["type"], "STANDARD")
        self.assertEqual(definition["StartAt"], "InvokeEnrichment")
        self.assertEqual(definition["States"]["InvokeEnrichment"]["Resource"], "arn:aws:states:::lambda:invoke")
        self.assertEqual(definition["States"]["SendToQueue"]["Resource"], "arn:aws:states:::sqs:sendMessage")
        self.assertIn('resource "aws_pipes_pipe" "orders"', self.main_tf)
        self.assertIn('batch_size = 1', self.main_tf)
        self.assertIn('invocation_type = "FIRE_AND_FORGET"', self.main_tf)

    def test_alarms_roles_and_glue_contract(self):
        self.assertIn('resource "aws_glue_crawler" "analytics"', self.main_tf)
        self.assertIn('resource "aws_glue_connection" "redshift"', self.main_tf)
        self.assertIn('SECRET_ID', self.main_tf)

        # In local mode only the 5 non-conditional alarms are deployed:
        # api_errors, api_duration, worker_errors, worker_duration, sfn_failed
        alarms = [r["values"] for r in self.resources if r["type"] == "aws_cloudwatch_metric_alarm"]
        self.assertEqual(len(alarms), 5)

        roles = [r for r in self.resources if r["type"] == "aws_iam_role"]
        self.assertEqual(len(roles), 6)


if __name__ == "__main__":
    unittest.main()
