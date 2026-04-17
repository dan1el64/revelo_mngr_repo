import json
import os
import subprocess
import time
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import boto3
from botocore.config import Config


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


def normalize_actions(actions):
    if isinstance(actions, str):
        return [actions]
    return actions


def normalize_resources(resources):
    if isinstance(resources, str):
        return [resources]
    return resources


def load_policy(resource):
    return json.loads(resource["values"]["policy"])


def aws_endpoint_url():
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("TF_VAR_aws_endpoint") or None


def aws_region():
    return (
        os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or os.environ.get("TF_VAR_aws_region")
        or "us-east-1"
    )


def aws_client(service):
    kwargs = {
        "region_name": aws_region(),
        "config": Config(retries={"max_attempts": 3, "mode": "standard"}),
    }
    endpoint_url = aws_endpoint_url()
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(service, **kwargs)


def wait_until(assertion, timeout_seconds=120, interval_seconds=2):
    deadline = time.monotonic() + timeout_seconds
    last_error = None

    while time.monotonic() < deadline:
        try:
            result = assertion()
            if result:
                return result
        except AssertionError as error:
            last_error = error
        time.sleep(interval_seconds)

    if last_error is not None:
        raise last_error
    raise AssertionError(f"Condition was not met within {timeout_seconds} seconds")


class TestEndToEndRuntimeFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = load_state()
        cls.resources = list(iter_resources(cls.state["values"]["root_module"]))
        cls.resource_map = resources_by_address(cls.resources)
        cls.s3 = aws_client("s3")
        cls.sfn = aws_client("stepfunctions")

    def values(self, address):
        return self.resource_map[address]["values"]

    def invoke_api(self, payload):
        api = self.values("aws_api_gateway_rest_api.main")
        stage = self.values("aws_api_gateway_stage.v1")
        endpoint_url = aws_endpoint_url()

        if endpoint_url:
            url = (
                f"{endpoint_url.rstrip('/')}/restapis/"
                f"{api['id']}/{stage['stage_name']}/_user_request_/ingest"
            )
        elif stage.get("invoke_url"):
            url = f"{stage['invoke_url'].rstrip('/')}/ingest"
        else:
            self.fail("API Gateway stage did not expose an invoke URL")

        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status, response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            self.fail(f"API returned HTTP {error.code}: {body}")

    def processed_objects_containing(self, bucket_name, marker):
        paginator = self.s3.get_paginator("list_objects_v2")
        matches = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix="processed/"):
            for item in page.get("Contents", []):
                key = item["Key"]
                response = self.s3.get_object(Bucket=bucket_name, Key=key)
                body = response["Body"].read().decode("utf-8")
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError:
                    continue
                if marker in json.dumps(payload, sort_keys=True):
                    matches.append((key, payload))

        return matches

    def wait_for_successful_execution(self, state_machine_arn, marker):
        def find_execution():
            paginator = self.sfn.get_paginator("list_executions")
            for page in paginator.paginate(
                stateMachineArn=state_machine_arn,
                statusFilter="SUCCEEDED",
                PaginationConfig={"PageSize": 20},
            ):
                for execution in page.get("executions", []):
                    details = self.sfn.describe_execution(
                        executionArn=execution["executionArn"]
                    )
                    if marker in details.get("input", ""):
                        return details
            return None

        return wait_until(
            find_execution,
            timeout_seconds=120,
            interval_seconds=2,
        )

    def test_api_ingest_drives_queue_step_functions_and_s3_persistence(self):
        queue = self.values("aws_sqs_queue.processing")
        bucket = self.values("aws_s3_bucket.data")
        state_machine = self.values("aws_sfn_state_machine.processing")
        marker = f"integration-{uuid.uuid4()}"
        request_payload = {
            "request_id": marker,
            "tenant_id": "tenant-a",
            "document": {"kind": "invoice", "amount": 42},
        }

        status, body = self.invoke_api(request_payload)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"status": "accepted"})

        execution = self.wait_for_successful_execution(state_machine["arn"], marker)
        execution_output = json.loads(execution["output"])
        execution_body = json.loads(execution_output["body"])
        self.assertTrue(execution_body["key"].startswith("processed/"))
        self.assertIn(marker, execution["input"])

        step_function_object = self.s3.get_object(
            Bucket=bucket["bucket"],
            Key=execution_body["key"],
        )
        step_function_payload = json.loads(
            step_function_object["Body"].read().decode("utf-8")
        )
        self.assertEqual(step_function_payload["username"], "appuser")
        self.assertEqual(step_function_payload["record_count"], 0)
        self.assertIn(marker, json.dumps(step_function_payload["event"]))

        def find_queue_worker_object():
            matches = self.processed_objects_containing(bucket["bucket"], marker)
            for key, payload in matches:
                records = payload.get("event", {}).get("Records", [])
                if records:
                    return key, payload
            self.assertTrue(matches, "No processed S3 object contained the API marker")
            raise AssertionError("No processed S3 object came from the SQS event source")

        queue_key, queue_payload = wait_until(
            find_queue_worker_object,
            timeout_seconds=120,
            interval_seconds=2,
        )
        self.assertTrue(queue_key.startswith("processed/"))
        self.assertEqual(queue_payload["username"], "appuser")
        self.assertEqual(queue_payload["record_count"], 1)

        sqs_record = queue_payload["event"]["Records"][0]
        self.assertEqual(sqs_record["eventSource"], "aws:sqs")
        self.assertEqual(sqs_record["eventSourceARN"], queue["arn"])
        self.assertIn(marker, sqs_record["body"])


class TestIamRuntimeContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = load_state()
        cls.resources = list(iter_resources(cls.state["values"]["root_module"]))
        cls.resource_map = resources_by_address(cls.resources)

    def values(self, address):
        resource = self.resource_map.get(address)
        if resource is None and not address.endswith("[0]"):
            resource = self.resource_map.get(f"{address}[0]")
        return resource["values"] if resource else None

    def policy(self, address):
        return load_policy(self.resource_map[address])

    def assert_statement(self, policy, *, actions, resource):
        expected = {
            "Effect": "Allow",
            "Action": actions,
            "Resource": resource,
        }
        self.assertIn(expected, policy["Statement"])

    def assert_only_expected_wildcard_resources(self, policy, expected_action_sets):
        for statement in policy["Statement"]:
            resources = normalize_resources(statement["Resource"])
            if resources == ["*"]:
                self.assertIn(
                    frozenset(normalize_actions(statement["Action"])),
                    expected_action_sets,
                )

    def test_application_permissions_are_scoped_to_terminal_resources(self):
        queue = self.values("aws_sqs_queue.processing")
        bucket = self.values("aws_s3_bucket.data")
        secret = self.values("aws_secretsmanager_secret.db_credentials")
        worker = self.values("aws_lambda_function.worker")
        state_machine = self.values("aws_sfn_state_machine.processing")
        ingest_logs = self.values("aws_cloudwatch_log_group.ingest_lambda")
        worker_logs = self.values("aws_cloudwatch_log_group.worker_lambda")
        step_logs = self.values("aws_cloudwatch_log_group.step_functions")

        ingest_policy = self.policy("aws_iam_role_policy.ingest_lambda")
        worker_policy = self.policy("aws_iam_role_policy.worker_lambda")
        step_policy = self.policy("aws_iam_role_policy.step_functions")

        eni_actions = frozenset(
            [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
                "ec2:AssignPrivateIpAddresses",
                "ec2:UnassignPrivateIpAddresses",
            ]
        )
        delivery_actions = frozenset(
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
        )

        for policy in [ingest_policy, worker_policy, step_policy]:
            for statement in policy["Statement"]:
                self.assertNotIn("*", normalize_actions(statement["Action"]))

        self.assert_only_expected_wildcard_resources(ingest_policy, {eni_actions})
        self.assert_only_expected_wildcard_resources(worker_policy, {eni_actions})
        self.assert_only_expected_wildcard_resources(step_policy, {delivery_actions})

        self.assert_statement(
            ingest_policy,
            actions=["sqs:SendMessage"],
            resource=queue["arn"],
        )
        self.assert_statement(
            ingest_policy,
            actions=["states:StartExecution"],
            resource=state_machine["arn"],
        )
        self.assert_statement(
            ingest_policy,
            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
            resource=f"{ingest_logs['arn']}:*",
        )

        self.assert_statement(
            worker_policy,
            actions=[
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility",
            ],
            resource=queue["arn"],
        )
        self.assert_statement(
            worker_policy,
            actions=["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
            resource=secret["arn"],
        )
        self.assert_statement(
            worker_policy,
            actions=["s3:PutObject"],
            resource=f"{bucket['arn']}/processed/*",
        )
        self.assert_statement(
            worker_policy,
            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
            resource=f"{worker_logs['arn']}:*",
        )

        self.assert_statement(
            step_policy,
            actions=["lambda:InvokeFunction"],
            resource=worker["arn"],
        )
        self.assertEqual(
            state_machine["logging_configuration"][0]["log_destination"],
            f"{step_logs['arn']}:*",
        )


if __name__ == "__main__":
    unittest.main()
