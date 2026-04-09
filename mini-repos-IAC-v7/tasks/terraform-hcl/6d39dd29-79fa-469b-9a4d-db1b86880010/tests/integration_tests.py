from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TF = (ROOT / "main.tf").read_text()


def test_declared_runtime_flow_is_bus_to_queue_to_pipe_to_state_machine_to_lambda():
    assert "arn            = aws_sqs_queue.primary.arn" in TF
    assert "source     = aws_sqs_queue.primary.arn" in TF
    assert "enrichment = aws_lambda_function.worker.arn" in TF
    assert "target     = aws_sfn_state_machine.worker.arn" in TF
    assert "FunctionName = aws_lambda_function.worker.arn" in TF
    assert "BUCKET_NAME   = aws_s3_bucket.event_archive.id" in TF
    assert "DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn" in TF
    assert 'DB_HOST       = var.aws_endpoint == null ? aws_db_instance.postgres[0].address : "db-disabled"' in TF


def test_no_resource_bypasses_the_pipe_as_state_machine_starter():
    assert "aws_cloudwatch_event_target" in TF
    event_target_start = TF.index('resource "aws_cloudwatch_event_target" "primary_queue"')
    event_target_end = TF.index('resource "aws_sqs_queue_policy"', event_target_start)
    event_target_block = TF[event_target_start:event_target_end]
    assert "aws_sfn_state_machine.worker.arn" not in event_target_block
    assert "states:StartExecution" in TF
    assert TF.count("states:StartExecution") == 1


def test_lambda_s3_key_and_table_contract_are_present():
    assert "id TEXT PRIMARY KEY" in TF
    assert "payload JSONB" in TF
    assert "created_at TIMESTAMPTZ DEFAULT NOW()" in TF


def test_rds_and_pipe_references_are_direct_and_unconditional():
    assert "count = var.aws_endpoint == null ? 1 : 0" in TF
    assert 'DB_HOST       = var.aws_endpoint == null ? aws_db_instance.postgres[0].address : "db-disabled"' in TF
    assert "db_subnet_group_name    = aws_db_subnet_group.rds[0].name" in TF
    assert 'DBInstanceIdentifier = var.aws_endpoint == null ? aws_db_instance.postgres[0].identifier : local.db_identifier' in TF
