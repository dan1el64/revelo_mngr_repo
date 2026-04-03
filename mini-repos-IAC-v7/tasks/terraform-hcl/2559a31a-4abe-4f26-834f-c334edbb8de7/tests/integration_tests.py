import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"


def read_main_tf():
    return MAIN_TF.read_text()


def resource_block_text(tf_text, resource_type, name):
    pattern = rf'resource "{re.escape(resource_type)}" "{re.escape(name)}" \{{(.*?)\n\}}'
    match = re.search(pattern, tf_text, re.DOTALL)
    assert match is not None
    return match.group(1)


def test_cross_resource_event_flow_graph_is_consistent():
    tf_text = read_main_tf()
    event_target = resource_block_text(tf_text, "aws_cloudwatch_event_target", "queue")
    pipe = resource_block_text(tf_text, "aws_pipes_pipe", "intake")
    state_machine = resource_block_text(tf_text, "aws_sfn_state_machine", "processing")

    assert "rule           = aws_cloudwatch_event_rule.intake_requested.name" in event_target
    assert "event_bus_name = aws_cloudwatch_event_rule.intake_requested.event_bus_name" in event_target
    assert "arn            = aws_sqs_queue.intake.arn" in event_target
    assert "source     = aws_sqs_queue.intake.arn" in pipe
    assert "enrichment = aws_lambda_function.enrichment.arn" in pipe
    assert "target     = aws_sfn_state_machine.processing.arn" in pipe
    assert "FunctionName = aws_lambda_function.validation.arn" in state_machine
    assert 'log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"' in state_machine


def test_cross_resource_security_graph_is_consistent():
    tf_text = read_main_tf()
    validation_lambda = resource_block_text(tf_text, "aws_lambda_function", "validation")
    db_instance = resource_block_text(tf_text, "aws_db_instance", "postgres")
    db_subnet_group = resource_block_text(tf_text, "aws_db_subnet_group", "database")
    db_ingress = resource_block_text(tf_text, "aws_vpc_security_group_ingress_rule", "data_store_postgres")
    worker_to_db = resource_block_text(tf_text, "aws_vpc_security_group_egress_rule", "workers_postgres")
    lambda_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "validation_lambda_errors")
    sfn_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "step_functions_failed")

    assert "SECRET_ARN = aws_secretsmanager_secret.database.arn" in validation_lambda
    assert "DB_HOST    = aws_db_instance.postgres.address" in validation_lambda
    assert "db_subnet_group_name     = aws_db_subnet_group.database.name" in db_instance
    assert "port                     = 5432" in db_instance
    assert "subnet_ids  = [aws_subnet.private_a.id, aws_subnet.private_b.id]" in db_subnet_group
    assert 'referenced_security_group_id = aws_security_group.serverless_workers.id' in db_ingress
    assert 'referenced_security_group_id = aws_security_group.data_store.id' in worker_to_db
    assert "FunctionName = aws_lambda_function.validation.function_name" in lambda_alarm
    assert "StateMachineArn = aws_sfn_state_machine.processing.arn" in sfn_alarm
