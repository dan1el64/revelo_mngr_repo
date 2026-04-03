import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"


def read_main_tf():
    return MAIN_TF.read_text()


def resource_names(tf_text, resource_type):
    pattern = rf'^resource "{re.escape(resource_type)}" "([^"]+)"'
    return re.findall(pattern, tf_text, re.MULTILINE)


def resource_block_text(tf_text, resource_type, name):
    pattern = rf'(?ms)^resource "{re.escape(resource_type)}" "{re.escape(name)}" \{{\n(.*?)^}}'
    match = re.search(pattern, tf_text)
    assert match is not None, f"Missing resource {resource_type}.{name}"
    return match.group(1)


def provider_block_text(tf_text, name):
    pattern = rf'(?ms)^provider "{re.escape(name)}" \{{\n(.*?)^}}'
    match = re.search(pattern, tf_text)
    assert match is not None, f"Missing provider {name}"
    return match.group(1)


def role_policy_blocks_for_role(tf_text, role_ref):
    pattern = r'(?ms)^resource "aws_iam_role_policy" "([^"]+)" \{\n(.*?)^}'
    return [
        body
        for _, body in re.findall(pattern, tf_text)
        if f"role = {role_ref}" in body
    ]


def variable_names(tf_text):
    return re.findall(r'^variable "([^"]+)"', tf_text, re.MULTILINE)


def ingress_rule_blocks(tf_text):
    return {
        name: resource_block_text(tf_text, "aws_vpc_security_group_ingress_rule", name)
        for name in resource_names(tf_text, "aws_vpc_security_group_ingress_rule")
    }


def egress_rule_blocks(tf_text):
    return {
        name: resource_block_text(tf_text, "aws_vpc_security_group_egress_rule", name)
        for name in resource_names(tf_text, "aws_vpc_security_group_egress_rule")
    }


def interface_endpoint_blocks(tf_text):
    blocks = {}
    for name in resource_names(tf_text, "aws_vpc_endpoint"):
        block = resource_block_text(tf_text, "aws_vpc_endpoint", name)
        if 'vpc_endpoint_type   = "Interface"' in block:
            blocks[name] = block
    return blocks


def test_provider_and_input_contract_are_explicit():
    tf_text = read_main_tf()
    provider_aws = provider_block_text(tf_text, "aws")

    assert set(variable_names(tf_text)) == {
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    }
    assert 'default = "us-east-1"' in tf_text
    assert re.search(r'^data "', tf_text, re.MULTILINE) is None
    assert re.search(r"region\s*=\s*var\.aws_region", provider_aws) is not None
    assert re.search(r"access_key\s*=\s*var\.aws_access_key_id", provider_aws) is not None
    assert re.search(r"secret_key\s*=\s*var\.aws_secret_access_key", provider_aws) is not None
    assert "events         = var.aws_endpoint" in provider_aws
    assert "lambda         = var.aws_endpoint" in provider_aws
    assert "logs           = var.aws_endpoint" in provider_aws
    assert "pipes          = var.aws_endpoint" in provider_aws
    assert "rds            = var.aws_endpoint" in provider_aws
    assert "s3             = var.aws_endpoint" in provider_aws
    assert "secretsmanager = var.aws_endpoint" in provider_aws
    assert "sfn            = var.aws_endpoint" in provider_aws
    assert "sqs            = var.aws_endpoint" in provider_aws


def test_private_networking_and_endpoint_access_are_scoped():
    tf_text = read_main_tf()
    subnet_a = resource_block_text(tf_text, "aws_subnet", "private_a")
    subnet_b = resource_block_text(tf_text, "aws_subnet", "private_b")
    route_table = resource_block_text(tf_text, "aws_route_table", "private")
    s3_endpoint = resource_block_text(tf_text, "aws_vpc_endpoint", "s3")
    interface_endpoints = interface_endpoint_blocks(tf_text)
    ingress_rules = ingress_rule_blocks(tf_text)
    egress_rules = egress_rule_blocks(tf_text)

    assert resource_names(tf_text, "aws_vpc") == ["intake"]
    assert sorted(resource_names(tf_text, "aws_subnet")) == ["private_a", "private_b"]
    assert resource_names(tf_text, "aws_route_table") == ["private"]
    assert route_table.strip() == "vpc_id = aws_vpc.intake.id"
    assert 'cidr_block           = "10.0.0.0/16"' in tf_text
    assert 'cidr_block              = "10.0.1.0/24"' in subnet_a
    assert 'cidr_block              = "10.0.2.0/24"' in subnet_b
    assert "map_public_ip_on_launch = false" in subnet_a
    assert "map_public_ip_on_launch = false" in subnet_b
    assert "aws_internet_gateway" not in tf_text
    assert "aws_nat_gateway" not in tf_text
    assert set(interface_endpoints) == {"events", "logs", "secretsmanager", "sqs", "states"}
    assert 'service_name      = "com.amazonaws.${var.aws_region}.s3"' in s3_endpoint
    assert 'vpc_endpoint_type = "Gateway"' in s3_endpoint
    assert "route_table_ids   = [aws_route_table.private.id]" in s3_endpoint

    endpoint_security_groups = set()
    for block in interface_endpoints.values():
        assert "vpc_id              = aws_vpc.intake.id" in block
        assert 'private_dns_enabled = true' in block
        assert re.search(
            r"subnet_ids\s*=\s*\[aws_subnet\.private_a\.id,\s*aws_subnet\.private_b\.id\]",
            block,
        ) is not None
        endpoint_security_groups.update(
            re.findall(r"aws_security_group\.([^.]+)\.id", block)
        )

    assert endpoint_security_groups, "Interface endpoints must declare a security group"
    for security_group_name in endpoint_security_groups:
        assert any(
            f"security_group_id = aws_security_group.{security_group_name}.id" in rule
            and "cidr_ipv4         = aws_vpc.intake.cidr_block" in rule
            and "from_port         = 443" in rule
            and "to_port           = 443" in rule
            for rule in ingress_rules.values()
        ), f"Missing VPC-only HTTPS ingress for endpoint security group {security_group_name}"

    https_ingress_rules = [
        rule
        for rule in ingress_rules.values()
        if "from_port         = 443" in rule and "to_port           = 443" in rule
    ]
    assert https_ingress_rules, "Expected at least one HTTPS ingress rule for interface endpoints"
    assert all("cidr_ipv4         = aws_vpc.intake.cidr_block" in rule for rule in https_ingress_rules)
    assert re.search(r'cidr_ipv4\s*=\s*"0\.0\.0\.0/0"', "\n".join(https_ingress_rules)) is None
    assert any(
        "security_group_id            = aws_security_group.data_store.id" in rule
        and "referenced_security_group_id = aws_security_group.serverless_workers.id" in rule
        and "from_port                    = 5432" in rule
        and "to_port                      = 5432" in rule
        for rule in ingress_rules.values()
    )
    assert any(
        "security_group_id            = aws_security_group.serverless_workers.id" in rule
        and "referenced_security_group_id = aws_security_group.data_store.id" in rule
        and "from_port                    = 5432" in rule
        and "to_port                      = 5432" in rule
        for rule in egress_rules.values()
    )


def test_eventbridge_to_sqs_contract_is_scoped_to_resource_blocks():
    tf_text = read_main_tf()
    queue = resource_block_text(tf_text, "aws_sqs_queue", "intake")
    event_rule = resource_block_text(tf_text, "aws_cloudwatch_event_rule", "intake_requested")
    event_target = resource_block_text(tf_text, "aws_cloudwatch_event_target", "queue")
    queue_policy = resource_block_text(tf_text, "aws_sqs_queue_policy", "intake")
    pipe = resource_block_text(tf_text, "aws_pipes_pipe", "intake")

    assert "visibility_timeout_seconds = 60" in queue
    assert "message_retention_seconds  = 345600" in queue
    assert 'event_bus_name = "default"' in event_rule
    assert 'source        = ["com.acme.intake"]' in event_rule
    assert '"detail-type" = ["IntakeRequested"]' in event_rule
    assert "rule           = aws_cloudwatch_event_rule.intake_requested.name" in event_target
    assert "event_bus_name = aws_cloudwatch_event_rule.intake_requested.event_bus_name" in event_target
    assert "arn            = aws_sqs_queue.intake.arn" in event_target
    assert re.search(
        r'Service\s*=\s*"events\.amazonaws\.com".*?'
        r'Action\s*=\s*"sqs:SendMessage".*?'
        r'Resource\s*=\s*aws_sqs_queue\.intake\.arn.*?'
        r'"aws:SourceArn"\s*=\s*aws_cloudwatch_event_rule\.intake_requested\.arn',
        queue_policy,
        re.DOTALL,
    ) is not None
    assert 'Principal = "*"' not in queue_policy
    assert "source_parameters {" in pipe
    assert "sqs_queue_parameters {" in pipe
    assert "batch_size = 1" in pipe


def test_secret_and_database_configuration_avoid_literal_credentials():
    tf_text = read_main_tf()
    secret_version = resource_block_text(tf_text, "aws_secretsmanager_secret_version", "database")
    db_subnet_group = resource_block_text(tf_text, "aws_db_subnet_group", "database")
    db_instance = resource_block_text(tf_text, "aws_db_instance", "postgres")

    assert resource_names(tf_text, "aws_secretsmanager_secret") == ["database"]
    assert resource_names(tf_text, "aws_secretsmanager_secret_version") == ["database"]
    assert resource_names(tf_text, "aws_db_subnet_group") == ["database"]
    assert resource_names(tf_text, "aws_db_instance") == ["postgres"]
    assert "username = \"intake_admin\"" in secret_version
    assert re.search(r"password\s*=", secret_version) is not None
    assert re.search(r'password\s*=\s*"', secret_version) is None
    assert re.search(r"password\s*=\s*var\.", secret_version) is None
    assert "subnet_ids  = [aws_subnet.private_a.id, aws_subnet.private_b.id]" in db_subnet_group
    assert 'engine                   = "postgres"' in db_instance
    assert 'instance_class           = "db.t3.micro"' in db_instance
    assert "allocated_storage        = 20" in db_instance
    assert 'storage_type             = "gp3"' in db_instance
    assert "port                     = 5432" in db_instance
    assert "publicly_accessible      = false" in db_instance
    assert "vpc_security_group_ids   = [aws_security_group.data_store.id]" in db_instance
    assert "db_subnet_group_name     = aws_db_subnet_group.database[0].name" in db_instance
    assert 'password = "' not in db_instance
    assert "password                 = jsondecode(aws_secretsmanager_secret_version.database.secret_string)[\"password\"]" in db_instance


def test_lambda_packaging_is_fully_automated_within_terraform():
    tf_text = read_main_tf()
    artifacts = resource_block_text(tf_text, "terraform_data", "lambda_artifacts")
    enrichment_lambda = resource_block_text(tf_text, "aws_lambda_function", "enrichment")
    validation_lambda = resource_block_text(tf_text, "aws_lambda_function", "validation")

    assert 'provisioner "local-exec"' in artifacts
    assert 'mkdir -p "${path.module}/.artifacts/enrichment" "${path.module}/.artifacts/validation"' in artifacts
    assert 'filename         = "${path.module}/.artifacts/enrichment.zip"' in enrichment_lambda
    assert 'filename         = "${path.module}/.artifacts/validation.zip"' in validation_lambda
    assert "terraform_data.lambda_artifacts" in enrichment_lambda
    assert "terraform_data.lambda_artifacts" in validation_lambda


def test_lambda_runtime_vpc_and_structured_logging_are_declared():
    tf_text = read_main_tf()
    enrichment_lambda = resource_block_text(tf_text, "aws_lambda_function", "enrichment")
    validation_lambda = resource_block_text(tf_text, "aws_lambda_function", "validation")

    assert sorted(resource_names(tf_text, "aws_lambda_function")) == ["enrichment", "validation"]
    assert 'package_type     = "Zip"' in enrichment_lambda
    assert 'runtime          = "python3.12"' in enrichment_lambda
    assert "memory_size      = 256" in enrichment_lambda
    assert "timeout          = 10" in enrichment_lambda
    assert "role             = aws_iam_role.serverless_workers.arn" in enrichment_lambda
    assert 'subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in enrichment_lambda
    assert 'security_group_ids = [aws_security_group.serverless_workers.id]' in enrichment_lambda
    assert 'package_type     = "Zip"' in validation_lambda
    assert 'runtime          = "python3.12"' in validation_lambda
    assert "memory_size      = 256" in validation_lambda
    assert "timeout          = 15" in validation_lambda
    assert "SECRET_ARN = aws_secretsmanager_secret.database.arn" in validation_lambda
    assert "DB_HOST" in validation_lambda
    assert re.search(
        r'"function": "enrichment".*?'
        r'"request_id": context\.aws_request_id.*?'
        r'"received_type": type\(event\).__name__.*?'
        r'print\(json\.dumps\(record, separators=\(",", ":"\)\)\)',
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'boto3\.client\("secretsmanager"\).*?'
        r'os\.environ\["SECRET_ARN"\].*?'
        r'os\.environ\["DB_HOST"\].*?'
        r'socket\.create_connection\(\(endpoint, 5432\), timeout=3\)',
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'"function": "validation".*?'
        r'"request_id": context\.aws_request_id.*?'
        r'"status": status.*?'
        r'print\(json\.dumps\(result, separators=\(",", ":"\)\)\).*?'
        r'except Exception as exc:.*?'
        r'"error": str\(exc\).*?'
        r'print\(json\.dumps\(result, separators=\(",", ":"\)\)\)',
        tf_text,
        re.DOTALL,
    ) is not None


def test_iam_policies_remain_scoped_and_include_step_functions_logging_permissions():
    tf_text = read_main_tf()
    serverless_workers_role = resource_block_text(tf_text, "aws_iam_role", "serverless_workers")
    pipes_role = resource_block_text(tf_text, "aws_iam_role", "pipes")
    serverless_workers_policy = "\n".join(
        role_policy_blocks_for_role(tf_text, "aws_iam_role.serverless_workers.id")
    )
    pipes_policy = "\n".join(role_policy_blocks_for_role(tf_text, "aws_iam_role.pipes.id"))

    assert 'Service = "lambda.amazonaws.com"' in serverless_workers_role
    assert '"pipes.amazonaws.com"' in pipes_role
    assert '"states.amazonaws.com"' in pipes_role
    assert '"logs:CreateLogStream"' in serverless_workers_policy
    assert '"logs:PutLogEvents"' in serverless_workers_policy
    assert "${aws_cloudwatch_log_group.enrichment.arn}:*" in serverless_workers_policy
    assert "${aws_cloudwatch_log_group.validation.arn}:*" in serverless_workers_policy
    assert '"secretsmanager:GetSecretValue"' in serverless_workers_policy
    assert "Resource = aws_secretsmanager_secret.database.arn" in serverless_workers_policy
    assert '"ec2:CreateNetworkInterface"' in serverless_workers_policy
    assert re.search(
        r'Sid\s*=\s*"ManageVpcEnis".*?Resource\s*=\s*"\*"',
        serverless_workers_policy,
        re.DOTALL,
    ) is not None
    assert '"sqs:ReceiveMessage"' in pipes_policy
    assert '"sqs:DeleteMessage"' in pipes_policy
    assert '"sqs:GetQueueAttributes"' in pipes_policy
    assert "Resource = aws_sqs_queue.intake.arn" in pipes_policy
    assert "aws_lambda_function.enrichment.arn" in pipes_policy
    assert "aws_lambda_function.validation.arn" in pipes_policy
    assert "aws_sfn_state_machine.processing.arn" in pipes_policy
    assert '"states:StartExecution"' in pipes_policy
    assert '"logs:CreateLogDelivery"' in pipes_policy
    assert '"logs:GetLogDelivery"' in pipes_policy
    assert '"logs:UpdateLogDelivery"' in pipes_policy
    assert '"logs:PutResourcePolicy"' in pipes_policy
    assert re.search(
        r'Sid\s*=\s*"DeliverStateMachineLogs".*?Resource\s*=\s*"\*"',
        pipes_policy,
        re.DOTALL,
    ) is not None
    assert re.search(r'Action\s*=\s*"\*"', pipes_policy) is None
    assert re.search(r'Action\s*=\s*"\*"', serverless_workers_policy) is None


def test_state_machine_pipe_and_observability_resources_are_wired():
    tf_text = read_main_tf()
    state_machine = resource_block_text(tf_text, "aws_sfn_state_machine", "processing")
    pipe = resource_block_text(tf_text, "aws_pipes_pipe", "intake")
    lambda_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "validation_lambda_errors")
    sfn_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "step_functions_failed")
    enrichment_logs = resource_block_text(tf_text, "aws_cloudwatch_log_group", "enrichment")
    validation_logs = resource_block_text(tf_text, "aws_cloudwatch_log_group", "validation")
    step_functions_logs = resource_block_text(tf_text, "aws_cloudwatch_log_group", "step_functions")

    assert resource_names(tf_text, "aws_sfn_state_machine") == ["processing"]
    assert resource_names(tf_text, "aws_pipes_pipe") == ["intake"]
    assert 'type     = "STANDARD"' in state_machine
    assert 'Resource = "arn:aws:states:::lambda:invoke"' in state_machine
    assert "FunctionName = aws_lambda_function.validation.arn" in state_machine
    assert 'Type  = "Fail"' in state_machine
    assert 'log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"' in state_machine
    assert 'level                  = "ALL"' in state_machine
    assert 'include_execution_data = true' in state_machine
    assert "role_arn   = aws_iam_role.pipes.arn" in pipe
    assert "source     = aws_sqs_queue.intake.arn" in pipe
    assert "enrichment = aws_lambda_function.enrichment.arn" in pipe
    assert "target     = aws_sfn_state_machine.processing.arn" in pipe
    assert "step_function_state_machine_parameters {" in pipe
    assert 'invocation_type = "FIRE_AND_FORGET"' in pipe
    assert "retention_in_days = 14" in enrichment_logs
    assert "retention_in_days = 14" in validation_logs
    assert "retention_in_days = 14" in step_functions_logs
    assert "kms_key_id" not in enrichment_logs
    assert "kms_key_id" not in validation_logs
    assert "kms_key_id" not in step_functions_logs
    assert 'metric_name         = "Errors"' in lambda_alarm
    assert 'namespace           = "AWS/Lambda"' in lambda_alarm
    assert "FunctionName = aws_lambda_function.validation.function_name" in lambda_alarm
    assert 'metric_name         = "ExecutionsFailed"' in sfn_alarm
    assert 'namespace           = "AWS/States"' in sfn_alarm
    assert "StateMachineArn = aws_sfn_state_machine.processing.arn" in sfn_alarm
