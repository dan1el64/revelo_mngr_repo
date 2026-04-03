import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
VARIABLES_TF = ROOT / "variables.tf"
ROOT_TF_FILES = sorted(path.name for path in ROOT.glob("*.tf"))


def read_main_tf():
    return MAIN_TF.read_text()


def resource_blocks(tf_text, resource_type):
    pattern = rf'resource "{re.escape(resource_type)}" "([^"]+)"'
    return re.findall(pattern, tf_text)


def resource_block_text(tf_text, resource_type, name):
    pattern = rf'resource "{re.escape(resource_type)}" "{re.escape(name)}" \{{(.*?)\n\}}'
    match = re.search(pattern, tf_text, re.DOTALL)
    assert match is not None
    return match.group(1)


def provider_block_text(tf_text, name):
    pattern = rf'provider "{re.escape(name)}" \{{(.*?)\n\}}'
    match = re.search(pattern, tf_text, re.DOTALL)
    assert match is not None
    return match.group(1)


def role_policy_blocks_for_role(tf_text, role_ref):
    pattern = (
        r'resource "aws_iam_role_policy" "[^"]+" \{'
        r'(.*?)'
        r"\n\}"
    )
    return [
        block
        for block in re.findall(pattern, tf_text, re.DOTALL)
        if f"role = {role_ref}" in block
    ]


def resource_types(tf_text):
    return re.findall(r'resource "(aws_[^"]+)" "[^"]+"', tf_text)


def variable_blocks(tf_text):
    return re.findall(r'variable "([^"]+)"', tf_text)


def contains_all(tf_text, snippets):
    return all(snippet in tf_text for snippet in snippets)


def test_input_contract_and_no_hidden_inputs():
    tf_text = read_main_tf()
    vars_found = set(variable_blocks(tf_text))

    assert not VARIABLES_TF.exists()
    assert ROOT_TF_FILES == ["main.tf"]
    assert vars_found == {
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    }
    assert 'default = "us-east-1"' in tf_text
    for locals_body in re.findall(r"locals\s*{(.*?)}", tf_text, re.DOTALL):
        assert "var." not in locals_body
    assert re.search(r'^data "', tf_text, re.MULTILINE) is None


def test_required_provider_variables_have_no_defaults_except_region():
    tf_text = read_main_tf()

    for variable_name in ["aws_endpoint", "aws_access_key_id", "aws_secret_access_key"]:
        variable_block = re.search(
            rf'variable "{re.escape(variable_name)}" \{{(.*?)\n\}}',
            tf_text,
            re.DOTALL,
        )
        assert variable_block is not None
        assert "default" not in variable_block.group(1)


def test_provider_uses_declared_region():
    tf_text = read_main_tf()
    provider_aws = provider_block_text(tf_text, "aws")

    assert re.search(r"region\s*=\s*var\.aws_region", provider_aws) is not None
    assert re.search(r"access_key\s*=\s*var\.aws_access_key_id", provider_aws) is not None
    assert re.search(r"secret_key\s*=\s*var\.aws_secret_access_key", provider_aws) is not None
    assert "skip_credentials_validation = true" in provider_aws
    assert "skip_metadata_api_check     = true" in provider_aws
    assert "skip_requesting_account_id  = true" in provider_aws
    assert "skip_region_validation      = true" in provider_aws
    assert "s3_use_path_style           = true" in provider_aws
    assert "endpoints {" in provider_aws
    assert contains_all(
        provider_aws,
        [
            "ec2            = var.aws_endpoint",
            "events         = var.aws_endpoint",
            "lambda         = var.aws_endpoint",
            "logs           = var.aws_endpoint",
            "pipes          = var.aws_endpoint",
            "rds            = var.aws_endpoint",
            "s3             = var.aws_endpoint",
            "secretsmanager = var.aws_endpoint",
            "sfn            = var.aws_endpoint",
            "sqs            = var.aws_endpoint",
            "sts            = var.aws_endpoint",
        ],
    )


def test_required_providers_include_random_for_password_generation():
    tf_text = read_main_tf()
    random_password_names = resource_blocks(tf_text, "random_password")

    assert 'source  = "hashicorp/random"' in tf_text
    assert len(random_password_names) >= 1
    assert resource_block_text(tf_text, "random_password", random_password_names[0]) is not None


def test_only_expected_aws_services_are_defined():
    tf_text = read_main_tf()
    present_types = set(resource_types(tf_text))

    assert {
        "aws_cloudwatch_event_rule",
        "aws_cloudwatch_event_target",
        "aws_cloudwatch_log_group",
        "aws_cloudwatch_metric_alarm",
        "aws_db_instance",
        "aws_db_subnet_group",
        "aws_iam_role",
        "aws_iam_role_policy",
        "aws_lambda_function",
        "aws_pipes_pipe",
        "aws_route_table",
        "aws_route_table_association",
        "aws_security_group",
        "aws_secretsmanager_secret",
        "aws_secretsmanager_secret_version",
        "aws_sfn_state_machine",
        "aws_sqs_queue",
        "aws_sqs_queue_policy",
        "aws_subnet",
        "aws_vpc",
        "aws_vpc_endpoint",
        "aws_vpc_security_group_egress_rule",
        "aws_vpc_security_group_ingress_rule",
    }.issubset(present_types)
    assert "aws_internet_gateway" not in present_types
    assert "aws_nat_gateway" not in present_types
    assert "aws_s3_bucket" not in present_types


def test_network_isolation_and_explicit_egress_match_contract():
    tf_text = read_main_tf()
    subnet_a = resource_block_text(tf_text, "aws_subnet", "private_a")
    subnet_b = resource_block_text(tf_text, "aws_subnet", "private_b")
    route_assoc_a = resource_block_text(tf_text, "aws_route_table_association", "private_a")
    route_assoc_b = resource_block_text(tf_text, "aws_route_table_association", "private_b")
    s3_endpoint = resource_block_text(tf_text, "aws_vpc_endpoint", "s3")
    interface_endpoint_names = ["events", "logs", "secretsmanager", "sqs", "states"]

    assert resource_blocks(tf_text, "aws_vpc") == ["intake"]
    assert sorted(resource_blocks(tf_text, "aws_subnet")) == ["private_a", "private_b"]
    assert resource_blocks(tf_text, "aws_route_table") == ["private"]
    assert sorted(resource_blocks(tf_text, "aws_route_table_association")) == ["private_a", "private_b"]
    assert sorted(resource_blocks(tf_text, "aws_security_group")) == ["data_store", "serverless_workers"]
    assert len(resource_blocks(tf_text, "aws_vpc_security_group_egress_rule")) >= 2
    assert sorted(resource_blocks(tf_text, "aws_vpc_endpoint")) == [
        "events",
        "logs",
        "s3",
        "secretsmanager",
        "sqs",
        "states",
    ]
    assert 'cidr_block           = "10.0.0.0/16"' in tf_text
    assert 'cidr_block              = "10.0.1.0/24"' in subnet_a
    assert 'availability_zone       = "${var.aws_region}a"' in subnet_a
    assert 'cidr_block              = "10.0.2.0/24"' in subnet_b
    assert 'availability_zone       = "${var.aws_region}b"' in subnet_b
    assert "subnet_id      = aws_subnet.private_a.id" in route_assoc_a
    assert "route_table_id = aws_route_table.private.id" in route_assoc_a
    assert "subnet_id      = aws_subnet.private_b.id" in route_assoc_b
    assert "route_table_id = aws_route_table.private.id" in route_assoc_b
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?'
        r"security_group_id\s*=\s*aws_security_group\.serverless_workers\.id.*?"
        r"cidr_ipv4\s*=\s*aws_vpc\.intake\.cidr_block.*?"
        r"from_port\s*=\s*443.*?"
        r"to_port\s*=\s*443",
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'resource "aws_vpc_security_group_egress_rule" "[^"]+" \{.*?'
        r"security_group_id\s*=\s*aws_security_group\.serverless_workers\.id.*?"
        r"cidr_ipv4\s*=\s*aws_vpc\.intake\.cidr_block.*?"
        r"from_port\s*=\s*443.*?"
        r"to_port\s*=\s*443",
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?'
        r"security_group_id\s*=\s*aws_security_group\.data_store\.id.*?"
        r"referenced_security_group_id\s*=\s*aws_security_group\.serverless_workers\.id.*?"
        r"from_port\s*=\s*5432.*?"
        r"to_port\s*=\s*5432",
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'resource "aws_vpc_security_group_egress_rule" "[^"]+" \{.*?'
        r"security_group_id\s*=\s*aws_security_group\.serverless_workers\.id.*?"
        r"referenced_security_group_id\s*=\s*aws_security_group\.data_store\.id.*?"
        r"from_port\s*=\s*5432.*?"
        r"to_port\s*=\s*5432",
        tf_text,
        re.DOTALL,
    ) is not None
    for endpoint_name in interface_endpoint_names:
        endpoint_block = resource_block_text(tf_text, "aws_vpc_endpoint", endpoint_name)
        assert "vpc_id              = aws_vpc.intake.id" in endpoint_block
        assert 'private_dns_enabled = true' in endpoint_block
        assert re.search(
            r"subnet_ids\s*=\s*\[aws_subnet\.private_a\.id,\s*aws_subnet\.private_b\.id\]",
            endpoint_block,
        ) is not None
        assert re.search(
            r"security_group_ids\s*=\s*\[aws_security_group\.serverless_workers\.id\]",
            endpoint_block,
        ) is not None
    assert 'service_name      = "com.amazonaws.${var.aws_region}.s3"' in s3_endpoint
    assert 'vpc_endpoint_type = "Gateway"' in s3_endpoint
    assert "route_table_ids   = [aws_route_table.private.id]" in s3_endpoint
    assert "private_dns_enabled" not in s3_endpoint
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?'
        r'cidr_ipv4\s*=\s*"0\.0\.0\.0/0".*?'
        r"from_port\s*=\s*(443|5432).*?"
        r"to_port\s*=\s*(443|5432)",
        tf_text,
        re.DOTALL,
    ) is None


def test_eventbridge_to_sqs_entrypoint_is_wired_and_scoped():
    tf_text = read_main_tf()
    event_rule = resource_block_text(tf_text, "aws_cloudwatch_event_rule", "intake_requested")
    event_target = resource_block_text(tf_text, "aws_cloudwatch_event_target", "queue")
    queue_policy = resource_block_text(tf_text, "aws_sqs_queue_policy", "intake")

    assert resource_blocks(tf_text, "aws_sqs_queue") == ["intake"]
    assert contains_all(
        event_rule,
        [
            'event_bus_name = "default"',
            'source        = ["com.acme.intake"]',
            '"detail-type" = ["IntakeRequested"]',
        ],
    )
    assert contains_all(
        tf_text,
        [
            "visibility_timeout_seconds = 60",
            "message_retention_seconds  = 345600",
        ],
    )
    assert "rule           = aws_cloudwatch_event_rule.intake_requested.name" in event_target
    assert "event_bus_name = aws_cloudwatch_event_rule.intake_requested.event_bus_name" in event_target
    assert "arn            = aws_sqs_queue.intake.arn" in event_target
    assert re.search(
        r'Sid\s*=\s*"AllowOnlyIntakeRule".*?'
        r'Effect\s*=\s*"Allow".*?'
        r'Service\s*=\s*"events\.amazonaws\.com".*?'
        r'Action\s*=\s*"sqs:SendMessage".*?'
        r'Resource\s*=\s*aws_sqs_queue\.intake\.arn.*?'
        r'"aws:SourceArn"\s*=\s*aws_cloudwatch_event_rule\.intake_requested\.arn',
        queue_policy,
        re.DOTALL,
    ) is not None
    assert 'Principal = "*"' not in queue_policy
    assert 'Resource = "*"' not in queue_policy
    assert '"sqs:*"' not in queue_policy


def test_iam_is_least_privilege_and_wildcards_are_only_unavoidable():
    tf_text = read_main_tf()
    serverless_workers_role = resource_block_text(tf_text, "aws_iam_role", "serverless_workers")
    pipes_role = resource_block_text(tf_text, "aws_iam_role", "pipes")
    serverless_workers_policies = role_policy_blocks_for_role(tf_text, "aws_iam_role.serverless_workers.id")
    pipes_policies = role_policy_blocks_for_role(tf_text, "aws_iam_role.pipes.id")
    serverless_workers_policy = "\n".join(serverless_workers_policies)
    pipes_policy_text = "\n".join(pipes_policies)
    write_lambda_logs_statement = re.search(
        r'Sid\s*=\s*"WriteLambdaLogs".*?Resource\s*=\s*\[(.*?)\]',
        serverless_workers_policy,
        re.DOTALL,
    )

    assert sorted(resource_blocks(tf_text, "aws_iam_role")) == ["pipes", "serverless_workers"]
    assert len(resource_blocks(tf_text, "aws_iam_role_policy")) >= 2
    assert len(serverless_workers_policies) >= 1
    assert len(pipes_policies) >= 1
    assert "aws_iam_role_policy_attachment" not in tf_text
    assert "aws_iam_policy_attachment" not in tf_text
    assert re.search(r"""['"][A-Za-z0-9-]+:\*['"]""", tf_text) is None
    assert re.search(r'Action\s*=\s*"\*"', tf_text) is None
    assert re.search(r'Action\s*=\s*\[[^\]]*"\*"[^\]]*\]', tf_text, re.DOTALL) is None
    assert 'Service = "lambda.amazonaws.com"' in serverless_workers_role
    assert '"pipes.amazonaws.com"' in pipes_role or 'Service = "pipes.amazonaws.com"' in pipes_role
    assert contains_all(
        serverless_workers_policy,
        [
            'Sid    = "ManageVpcEnis"',
            '"ec2:CreateNetworkInterface"',
            '"ec2:DescribeNetworkInterfaces"',
            '"ec2:DeleteNetworkInterface"',
            '"secretsmanager:GetSecretValue"',
            "Resource = aws_secretsmanager_secret.database.arn",
        ],
    )
    assert 'Sid    = "WriteLambdaLogs"' in serverless_workers_policy
    assert write_lambda_logs_statement is not None
    assert '"logs:CreateLogStream"' in serverless_workers_policy
    assert '"logs:PutLogEvents"' in serverless_workers_policy
    assert "${aws_cloudwatch_log_group.enrichment.arn}:*" in serverless_workers_policy
    assert "${aws_cloudwatch_log_group.validation.arn}:*" in serverless_workers_policy
    assert 'Resource = "*"' not in write_lambda_logs_statement.group(0)
    assert '"logs:CreateLogGroup"' not in serverless_workers_policy
    assert '"sqs:SendMessage"' not in serverless_workers_policy
    assert '"secretsmanager:GetSecretValue"' not in pipes_policy_text
    assert '"sqs:ReceiveMessage"' in pipes_policy_text
    assert '"sqs:DeleteMessage"' in pipes_policy_text
    assert '"sqs:GetQueueAttributes"' in pipes_policy_text
    assert "Resource = aws_sqs_queue.intake.arn" in pipes_policy_text
    assert '"lambda:InvokeFunction"' in pipes_policy_text
    assert "aws_lambda_function.enrichment.arn" in pipes_policy_text
    assert '"states:StartExecution"' in pipes_policy_text
    assert "Resource = aws_sfn_state_machine.processing.arn" in pipes_policy_text
    assert re.search(
        r'Sid\s*=\s*"ManageVpcEnis".*?Resource\s*=\s*"\*"',
        serverless_workers_policy,
        re.DOTALL,
    ) is not None


def test_compute_runtime_and_orchestration_behavior_are_declared():
    tf_text = read_main_tf()
    queue = resource_block_text(tf_text, "aws_sqs_queue", "intake")
    enrichment_lambda = resource_block_text(tf_text, "aws_lambda_function", "enrichment")
    validation_lambda = resource_block_text(tf_text, "aws_lambda_function", "validation")
    state_machine = resource_block_text(tf_text, "aws_sfn_state_machine", "processing")
    pipe = resource_block_text(tf_text, "aws_pipes_pipe", "intake")

    assert sorted(resource_blocks(tf_text, "aws_lambda_function")) == ["enrichment", "validation"]
    assert resource_blocks(tf_text, "aws_sfn_state_machine") == ["processing"]
    assert resource_blocks(tf_text, "aws_pipes_pipe") == ["intake"]
    assert contains_all(
        tf_text,
        [
            'package_type     = "Zip"',
            "memory_size      = 256",
            "timeout          = 10",
            "timeout          = 15",
            'runtime          = "python3.12"',
        ],
    )
    assert re.search(
        r'"function": "enrichment".*?print\(json\.dumps\(',
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'"function": "validation".*?get_secret_value\(.*?socket\.[A-Za-z_]+\(.*?5432.*?print\(json\.dumps\(',
        tf_text,
        re.DOTALL,
    ) is not None
    assert re.search(
        r'"function": "validation".*?print\(json\.dumps\(.*?\)\).*?except .*?print\(json\.dumps\(.*?\)\)',
        tf_text,
        re.DOTALL,
    ) is not None
    assert 'runtime          = "python3.12"' in enrichment_lambda
    assert "role             = aws_iam_role.serverless_workers.arn" in enrichment_lambda
    assert "memory_size      = 256" in enrichment_lambda
    assert "timeout          = 10" in enrichment_lambda
    assert 'subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in enrichment_lambda
    assert 'security_group_ids = [aws_security_group.serverless_workers.id]' in enrichment_lambda
    assert "SECRET_ARN" not in enrichment_lambda
    assert "DB_HOST" not in enrichment_lambda
    assert 'runtime          = "python3.12"' in validation_lambda
    assert "role             = aws_iam_role.serverless_workers.arn" in validation_lambda
    assert "memory_size      = 256" in validation_lambda
    assert "timeout          = 15" in validation_lambda
    assert "SECRET_ARN = aws_secretsmanager_secret.database.arn" in validation_lambda
    assert re.search(
        r'DB_HOST\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*"database\.internal"\s*:\s*aws_db_instance\.postgres\[0\]\.address',
        validation_lambda,
    ) is not None
    assert 'subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]' in validation_lambda
    assert 'security_group_ids = [aws_security_group.serverless_workers.id]' in validation_lambda
    assert 'Resource = "arn:aws:states:::lambda:invoke"' in state_machine
    assert 'type     = "STANDARD"' in state_machine
    assert "FunctionName = aws_lambda_function.validation.arn" in state_machine
    assert 'Type  = "Fail"' in state_machine
    assert 'logging_configuration {' in state_machine
    assert 'log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"' in state_machine
    assert 'level                  = "ALL"' in state_machine
    assert 'include_execution_data = true' in state_machine
    assert "role_arn   = aws_iam_role.pipes.arn" in pipe
    assert 'source     = aws_sqs_queue.intake.arn' in pipe
    assert 'enrichment = aws_lambda_function.enrichment.arn' in pipe
    assert 'aws_lambda_function.validation.arn' not in pipe
    assert 'target     = aws_sfn_state_machine.processing.arn' in pipe
    assert 'target_parameters {' in pipe
    assert 'step_function_state_machine_parameters {' in pipe
    assert 'invocation_type = "FIRE_AND_FORGET"' in pipe
    assert re.search(
        r'count\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*0\s*:\s*1',
        pipe,
    ) is not None


def test_state_machine_asl_success_and_failure_paths_are_wired():
    tf_text = read_main_tf()
    state_machine = resource_block_text(tf_text, "aws_sfn_state_machine", "processing")

    assert re.search(
        r"States\s*=\s*\{.*?"
        r'Type\s*=\s*"Task".*?'
        r'Resource\s*=\s*"arn:aws:states:::lambda:invoke".*?'
        r"FunctionName\s*=\s*aws_lambda_function\.validation\.arn.*?"
        r"Catch\s*=\s*\[.*?Next\s*=\s*\"[^\"]+\".*?\].*?"
        r"End\s*=\s*true.*?"
        r'Type\s*=\s*"Fail"',
        state_machine,
        re.DOTALL,
    ) is not None


def test_secret_database_and_destroy_settings_match_contract():
    tf_text = read_main_tf()

    assert resource_blocks(tf_text, "aws_secretsmanager_secret") == ["database"]
    assert resource_blocks(tf_text, "aws_secretsmanager_secret_version") == ["database"]
    assert resource_blocks(tf_text, "aws_db_subnet_group") == ["database"]
    assert resource_blocks(tf_text, "aws_db_instance") == ["postgres"]
    assert contains_all(
        tf_text,
        [
            "secret_string = jsonencode({",
            "username = ",
            "password = random_password.database.result",
            'username                 = jsondecode(aws_secretsmanager_secret_version.database.secret_string)["username"]',
            'password                 = jsondecode(aws_secretsmanager_secret_version.database.secret_string)["password"]',
            'instance_class           = "db.t3.micro"',
            'engine                   = "postgres"',
            "allocated_storage        = 20",
            'storage_type             = "gp3"',
            "port                     = 5432",
            "multi_az                 = false",
            "publicly_accessible      = false",
            "vpc_security_group_ids   = [aws_security_group.data_store.id]",
            "backup_retention_period  = 0",
            "deletion_protection      = false",
            "skip_final_snapshot      = true",
            "recovery_window_in_days = 0",
        ],
    )
    assert re.search(
        r'count\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*0\s*:\s*1',
        resource_block_text(tf_text, "aws_db_subnet_group", "database"),
    ) is not None
    assert re.search(
        r'count\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*0\s*:\s*1',
        resource_block_text(tf_text, "aws_db_instance", "postgres"),
    ) is not None
    assert "db_subnet_group_name     = aws_db_subnet_group.database[0].name" in tf_text
    assert 'password = "' not in tf_text
    assert re.search(r"password\s*=\s*tostring\(\s*\"", tf_text) is None
    assert "termination_protection" not in tf_text
    assert "final_snapshot_identifier" not in tf_text
    assert "prevent_destroy = true" not in tf_text
    assert re.search(r"lifecycle\s*{[^}]*prevent_destroy", tf_text, re.DOTALL) is None


def test_security_group_rules_do_not_open_unexpected_ports_or_cidrs():
    tf_text = read_main_tf()

    assert sorted(resource_blocks(tf_text, "aws_vpc_security_group_ingress_rule")) == [
        "data_store_postgres",
        "workers_endpoint_https",
    ]
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?from_port\s*=\s*22',
        tf_text,
        re.DOTALL,
    ) is None
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?from_port\s*=\s*80',
        tf_text,
        re.DOTALL,
    ) is None
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?from_port\s*=\s*3306',
        tf_text,
        re.DOTALL,
    ) is None
    assert re.search(
        r'resource "aws_vpc_security_group_ingress_rule" "[^"]+" \{.*?cidr_ipv4\s*=\s*"0\.0\.0\.0/0"',
        tf_text,
        re.DOTALL,
    ) is None


def test_cross_resource_wiring_uses_terraform_references_not_hardcoded_ids():
    tf_text = read_main_tf()
    event_target = resource_block_text(tf_text, "aws_cloudwatch_event_target", "queue")
    pipe = resource_block_text(tf_text, "aws_pipes_pipe", "intake")
    db_instance = resource_block_text(tf_text, "aws_db_instance", "postgres")

    assert 'arn            = "' not in event_target
    assert 'source     = "' not in pipe
    assert 'enrichment = "' not in pipe
    assert 'target     = "' not in pipe
    assert 'vpc_security_group_ids   = ["' not in db_instance
    assert 'db_subnet_group_name     = "' not in db_instance


def test_observability_resources_are_explicit_and_unencrypted():
    tf_text = read_main_tf()
    lambda_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "validation_lambda_errors")
    sfn_alarm = resource_block_text(tf_text, "aws_cloudwatch_metric_alarm", "step_functions_failed")

    assert sorted(resource_blocks(tf_text, "aws_cloudwatch_log_group")) == [
        "enrichment",
        "step_functions",
        "validation",
    ]
    assert sorted(resource_blocks(tf_text, "aws_cloudwatch_metric_alarm")) == [
        "step_functions_failed",
        "validation_lambda_errors",
    ]
    assert tf_text.count("retention_in_days = 14") == 3
    assert contains_all(
        lambda_alarm,
        [
            'metric_name         = "Errors"',
            'namespace           = "AWS/Lambda"',
            'statistic           = "Sum"',
            "FunctionName = aws_lambda_function.validation.function_name",
            "period              = 300",
            "evaluation_periods  = 1",
            "threshold           = 1",
            'comparison_operator = "GreaterThanOrEqualToThreshold"',
        ],
    )
    assert re.search(
        r'count\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*0\s*:\s*1',
        lambda_alarm,
    ) is not None
    assert contains_all(
        sfn_alarm,
        [
            'metric_name         = "ExecutionsFailed"',
            'namespace           = "AWS/States"',
            'statistic           = "Sum"',
            "StateMachineArn = aws_sfn_state_machine.processing.arn",
            "period              = 300",
            "evaluation_periods  = 1",
            "threshold           = 1",
            'comparison_operator = "GreaterThanOrEqualToThreshold"',
        ],
    )
    assert re.search(
        r'count\s*=\s*length\(trimspace\(var\.aws_endpoint\)\)\s*>\s*0\s*\?\s*0\s*:\s*1',
        sfn_alarm,
    ) is not None
    assert "kms_key_id" not in tf_text
