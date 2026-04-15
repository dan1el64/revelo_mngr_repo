import json
import os
import re
from pathlib import Path
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = (REPO_ROOT / "main.tf").read_text()
VARIABLES_TF = (REPO_ROOT / "variables.tf").read_text()


def test_ingest_handler_publish_event_for_each_environment():
    """The ingest handler should publish one OrderCreated event to the configured bus."""
    from ingest_lambda import handler

    for env in ["dev", "test", "prod"]:
        with patch("boto3.client") as mock_client, patch.dict(
            os.environ,
            {"EVENTBUS_NAME": f"{env}-bus"},
            clear=False,
        ):
            event = {"order_id": "123", "amount": 100.0}
            mock_client.return_value.put_events.return_value = {
                "Entries": [{"EventId": "test-event"}]
            }

            result = handler(event, None)

            mock_client.assert_called_once_with("events")
            mock_client.return_value.put_events.assert_called_once_with(
                Entries=[
                    {
                        "Source": "app.orders",
                        "DetailType": "OrderCreated",
                        "Detail": str(event),
                        "EventBusName": f"{env}-bus",
                    }
                ]
            )
            assert len(result["Entries"]) == 1


def test_enrichment_handler_valid_message_round_trips_json():
    """The enrichment handler should accept valid JSON payloads."""
    from enrichment_lambda import handler

    event = {"body": '{"order_id": "456", "amount": 200.0}'}
    result = handler(event, None)

    assert result["statusCode"] < 400
    assert json.loads(result["body"]) == {"order_id": "456", "amount": 200.0}


def test_enrichment_handler_invalid_json_is_rejected():
    """The enrichment handler should reject malformed messages."""
    from enrichment_lambda import handler

    event = {"body": "invalid-json"}
    result = handler(event, None)

    assert result["statusCode"] >= 400
    assert result["body"]


def test_worker_handler_success_uses_secret_and_persists_message():
    """The worker handler should read the secret, persist the message, and commit."""
    from worker_lambda import handler

    with patch("worker_lambda.boto3.client") as mock_secrets, patch(
        "worker_lambda.get_db_connection"
    ) as mock_connect:
        secret = {"username": "test", "password": "test"}
        mock_secrets.return_value.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }

        mock_cursor = Mock()
        mock_connection = Mock()
        mock_cursor_context = Mock()
        type(mock_cursor_context).__enter__ = Mock(return_value=mock_cursor)
        type(mock_cursor_context).__exit__ = Mock(return_value=None)
        mock_connection.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_connection

        event = {
            "body": '{"order_id":"789","amount": 300.0}',
            "SECRET_ARN": "test-arn",
            "DB_ENDPOINT": "test-endpoint",
        }

        result = handler(event, None)

        mock_secrets.return_value.get_secret_value.assert_called_once_with(
            SecretId="test-arn"
        )
        mock_connect.assert_called_once_with(secret, "test-endpoint")
        mock_cursor.execute.assert_called_once()

        sql, parameters = mock_cursor.execute.call_args.args
        assert "INSERT INTO orders" in sql
        assert parameters[0] == "789"
        assert json.loads(parameters[1]) == {"order_id": "789", "amount": 300.0}

        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()
        assert result["statusCode"] == 200


def test_worker_handler_database_error_rolls_back():
    """The worker handler should roll back and surface a failure on persistence errors."""
    from worker_lambda import handler

    with patch("worker_lambda.boto3.client") as mock_secrets, patch(
        "worker_lambda.get_db_connection"
    ) as mock_connect:
        mock_secrets.return_value.get_secret_value.return_value = {
            "SecretString": '{"username":"test","password":"test"}'
        }

        mock_cursor = Mock()
        mock_cursor.execute = Mock(side_effect=Exception("Database error"))
        mock_connection = Mock()
        mock_cursor_context = Mock()
        type(mock_cursor_context).__enter__ = Mock(return_value=mock_cursor)
        type(mock_cursor_context).__exit__ = Mock(return_value=None)
        mock_connection.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_connection

        event = {
            "body": '{"order_id":"error"}',
            "SECRET_ARN": "test-arn",
            "DB_ENDPOINT": "test-endpoint",
        }

        result = handler(event, None)

        mock_connection.rollback.assert_called_once()
        assert result["statusCode"] == 500


def test_terraform_defines_exactly_expected_variables():
    """The Terraform variable surface should match the required four inputs."""
    variable_names = re.findall(r'variable\s+"([^"]+)"', VARIABLES_TF)
    assert variable_names == [
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    ]
    assert 'default     = "us-east-1"' in VARIABLES_TF


def test_terraform_provider_uses_the_expected_inputs():
    """The provider should be wired from the declared variables."""
    for snippet in [
        "region     = var.aws_region",
        "access_key = var.aws_access_key_id",
        "secret_key = var.aws_secret_access_key",
        "apigateway     = var.aws_endpoint",
        "lambda         = var.aws_endpoint",
        "sqs            = var.aws_endpoint",
        "sfn            = var.aws_endpoint",
    ]:
        assert snippet in MAIN_TF


def test_terraform_avoids_nat_and_destroy_protection_patterns():
    """The stack should avoid NAT gateways and sticky destroy protection."""
    assert 'resource "aws_nat_gateway"' not in MAIN_TF
    assert "deletion_protection = true" not in MAIN_TF
    assert "prevent_destroy" not in MAIN_TF
    assert "name_prefix" not in MAIN_TF


def test_terraform_declares_explicit_log_groups_with_14_day_retention_and_no_kms():
    """Lambda, Step Functions, and API logging should use explicit log groups."""
    for resource_name in ["lambda", "enrichment", "worker", "stepfunctions", "api_logging"]:
        assert f'resource "aws_cloudwatch_log_group" "{resource_name}"' in MAIN_TF

    assert MAIN_TF.count("retention_in_days = 14") >= 5
    assert "kms_key_id" not in MAIN_TF


def test_terraform_scopes_core_iam_policies_and_api_invoke_permission():
    """Inline IAM policies should point to environment-scoped resources."""
    assert 'Action   = "events:PutEvents"' in MAIN_TF
    assert "Resource = aws_cloudwatch_event_bus.this[each.key].arn" in MAIN_TF

    assert 'Action   = "secretsmanager:GetSecretValue"' in MAIN_TF
    assert "Resource = aws_secretsmanager_secret.rds[each.key].arn" in MAIN_TF

    assert 'Action   = "lambda:InvokeFunction"' in MAIN_TF
    assert "Resource = aws_lambda_function.worker[each.key].arn" in MAIN_TF

    assert 'source_arn    = "${aws_api_gateway_rest_api.this[each.key].execution_arn}/*/POST/orders"' in MAIN_TF

    wildcard_resources = re.findall(r'Resource\s*=\s*"\*"', MAIN_TF)
    assert len(wildcard_resources) == 1
    assert 'resource "aws_iam_role_policy" "stepfunctions_logs"' in MAIN_TF


def test_terraform_declares_inline_lambda_archives_and_pipe_shape():
    """The Terraform file should remain self-contained and declare the pipe topology."""
    for archive_name in ["ingest_lambda", "enrichment_lambda", "worker_lambda"]:
        assert f'data "archive_file" "{archive_name}"' in MAIN_TF
        assert 'source_content_filename = "index.py"' in MAIN_TF

    assert 'resource "aws_pipes_pipe" "this"' in MAIN_TF
    assert "source     = aws_sqs_queue.this[each.key].arn" in MAIN_TF
    assert "target     = aws_sfn_state_machine.this[each.key].arn" in MAIN_TF
    assert "enrichment = aws_lambda_function.enrichment[each.key].arn" in MAIN_TF
    assert 'invocation_type = "FIRE_AND_FORGET"' in MAIN_TF
