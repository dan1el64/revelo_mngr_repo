import re
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
TF = MAIN_TF.read_text()


RESOURCE_RE = re.compile(r'resource\s+"([^"]+)"\s+"([^"]+)"\s*\{')
DATA_RE = re.compile(r'data\s+"([^"]+)"\s+"([^"]+)"\s*\{')


def resource_names(resource_type):
    return [name for found_type, name in RESOURCE_RE.findall(TF) if found_type == resource_type]


def assert_resource_count(resource_type, expected):
    names = resource_names(resource_type)
    assert len(names) == expected, f"{resource_type}: expected {expected}, found {names}"


def resource_inventory():
    inventory = {}
    for resource_type, _ in RESOURCE_RE.findall(TF):
        inventory[resource_type] = inventory.get(resource_type, 0) + 1
    return inventory


def block(prefix, *labels):
    quoted_labels = "".join(rf'\s+"{re.escape(label)}"' for label in labels)
    match = re.search(rf'{prefix}{quoted_labels}\s*\{{', TF)
    assert match, f"missing block: {prefix} {' '.join(labels)}"
    start = match.end() - 1
    depth = 0
    for index in range(start, len(TF)):
        if TF[index] == "{":
            depth += 1
        elif TF[index] == "}":
            depth -= 1
            if depth == 0:
                return TF[start : index + 1]
    raise AssertionError(f"unterminated block: {prefix} {' '.join(labels)}")


def resource_block(resource_type, name):
    return block(r"resource", resource_type, name)


def provider_block(name):
    return block(r"provider", name)


def test_single_terraform_file_and_allowed_variables_only():
    assert [path.name for path in ROOT.glob("*.tf")] == ["main.tf"]
    assert not (ROOT / "variables.tf").exists()
    assert not (ROOT / "lambda_function.py").exists()

    variables = re.findall(r'variable\s+"([^"]+)"', TF)
    for required in ["aws_region", "aws_access_key_id", "aws_secret_access_key"]:
        assert required in variables, f"required variable '{required}' is missing"
    # aws_endpoint may exist as an optional override but must not be required (must default to null)
    if "aws_endpoint" in variables:
        assert re.search(r'\bdefault\s*=\s*null\b', resource_context("aws_endpoint")), \
            "aws_endpoint must have default = null (optional, not required)"
    # no extra variables beyond the allowed set
    allowed = {"aws_endpoint", "aws_region", "aws_access_key_id", "aws_secret_access_key"}
    assert set(variables).issubset(allowed), f"unexpected variables: {set(variables) - allowed}"
    assert re.search(r'\bdefault\s*=\s*"us-east-1"', resource_context("aws_region"))
    assert "name_prefix" not in variables
    assert "bucket_name" not in variables


def test_exact_resource_inventory_matches_prompt_scope():
    assert resource_inventory() == {
        "aws_cloudwatch_event_bus": 1,
        "aws_cloudwatch_event_rule": 1,
        "aws_cloudwatch_event_target": 1,
        "aws_cloudwatch_log_group": 2,
        "aws_cloudwatch_metric_alarm": 3,
        "aws_db_instance": 1,
        "aws_db_subnet_group": 1,
        "aws_eip": 1,
        "aws_iam_role": 3,
        "aws_iam_role_policy": 3,
        "aws_internet_gateway": 1,
        "aws_lambda_function": 1,
        "aws_nat_gateway": 1,
        "aws_pipes_pipe": 1,
        "aws_route_table": 2,
        "aws_route_table_association": 4,
        "aws_s3_bucket": 1,
        "aws_s3_bucket_policy": 1,
        "aws_s3_bucket_public_access_block": 1,
        "aws_s3_bucket_server_side_encryption_configuration": 1,
        "aws_secretsmanager_secret": 1,
        "aws_secretsmanager_secret_version": 1,
        "aws_security_group": 2,
        "aws_sfn_state_machine": 1,
        "aws_sns_topic": 1,
        "aws_sns_topic_subscription": 1,
        "aws_sqs_queue": 2,
        "aws_sqs_queue_policy": 1,
        "aws_subnet": 4,
        "aws_vpc": 1,
        "aws_vpc_endpoint": 1,
        "random_password": 1,
    }
    assert DATA_RE.findall(TF) == [("archive_file", "worker_zip")]


def resource_context(variable_name):
    return block(r"variable", variable_name)


def test_provider_is_single_regioned_and_endpoint_driven():
    assert TF.count('provider "aws"') == 1
    provider = provider_block("aws")

    assert "region     = var.aws_region" in provider
    assert "access_key = var.aws_access_key_id" in provider
    assert "secret_key = var.aws_secret_access_key" in provider
    for service in [
        "cloudwatch",
        "ec2",
        "events",
        "iam",
        "lambda",
        "logs",
        "pipes",
        "rds",
        "s3",
        "secretsmanager",
        "sns",
        "sqs",
        "stepfunctions",
        "sts",
    ]:
        assert re.search(rf"\b{service}\s+=\s+var\.aws_endpoint\b", provider)


def test_connectivity_mesh_topology_is_exact_and_routed():
    for resource_type, expected in {
        "aws_vpc": 1,
        "aws_subnet": 4,
        "aws_internet_gateway": 1,
        "aws_eip": 1,
        "aws_nat_gateway": 1,
        "aws_route_table": 2,
        "aws_route_table_association": 4,
        "aws_vpc_endpoint": 1,
    }.items():
        assert_resource_count(resource_type, expected)

    assert 'Name = "Connectivity Mesh"' in resource_block("aws_vpc", "connectivity_mesh")

    public_a = resource_block("aws_subnet", "public_a")
    public_b = resource_block("aws_subnet", "public_b")
    private_a = resource_block("aws_subnet", "private_a")
    private_b = resource_block("aws_subnet", "private_b")
    assert 'availability_zone       = "${var.aws_region}a"' in public_a
    assert 'availability_zone       = "${var.aws_region}b"' in public_b
    assert 'availability_zone       = "${var.aws_region}a"' in private_a
    assert 'availability_zone       = "${var.aws_region}b"' in private_b
    assert "map_public_ip_on_launch = true" in public_a
    assert "map_public_ip_on_launch = true" in public_b
    assert "map_public_ip_on_launch = false" in private_a
    assert "map_public_ip_on_launch = false" in private_b

    nat = resource_block("aws_nat_gateway", "public_a")
    assert "subnet_id     = aws_subnet.public_a.id" in nat

    public_rt = resource_block("aws_route_table", "public")
    private_rt = resource_block("aws_route_table", "private")
    assert 'cidr_block = "0.0.0.0/0"' in public_rt
    assert "gateway_id = aws_internet_gateway.connectivity_mesh.id" in public_rt
    assert 'cidr_block     = "0.0.0.0/0"' in private_rt
    assert "nat_gateway_id = aws_nat_gateway.public_a.id" in private_rt

    for assoc, subnet, route_table in [
        ("public_a", "public_a", "public"),
        ("public_b", "public_b", "public"),
        ("private_a", "private_a", "private"),
        ("private_b", "private_b", "private"),
    ]:
        assoc_block = resource_block("aws_route_table_association", assoc)
        assert f"subnet_id      = aws_subnet.{subnet}.id" in assoc_block
        assert f"route_table_id = aws_route_table.{route_table}.id" in assoc_block

    endpoint = resource_block("aws_vpc_endpoint", "s3")
    assert 'service_name      = "com.amazonaws.${var.aws_region}.s3"' in endpoint
    assert 'vpc_endpoint_type = "Gateway"' in endpoint
    assert "aws_route_table.public.id" in endpoint
    assert "aws_route_table.private.id" in endpoint


def test_s3_gateway_endpoint_attaches_exactly_both_route_tables():
    endpoint = resource_block("aws_vpc_endpoint", "s3")
    route_table_ids = re.search(r"route_table_ids\s*=\s*\[(?P<body>.*?)\]", endpoint, re.DOTALL)
    assert route_table_ids, "aws_vpc_endpoint.s3 must declare route_table_ids"

    referenced_ids = re.findall(r"aws_route_table\.[^.]+\.(?:id)", route_table_ids.group("body"))
    assert referenced_ids == [
        "aws_route_table.public.id",
        "aws_route_table.private.id",
    ]


def test_security_groups_are_exact_and_database_ingress_is_lambda_only():
    assert_resource_count("aws_security_group", 2)
    lambda_sg = resource_block("aws_security_group", "lambda")
    rds_sg = resource_block("aws_security_group", "rds")

    assert "ingress" not in lambda_sg
    for sg in [lambda_sg, rds_sg]:
        assert 'from_port   = 0' in sg
        assert 'to_port     = 0' in sg
        assert 'protocol    = "-1"' in sg
        assert 'cidr_blocks = ["0.0.0.0/0"]' in sg

    assert rds_sg.count("ingress {") == 1
    assert "from_port       = 5432" in rds_sg
    assert "to_port         = 5432" in rds_sg
    assert 'protocol        = "tcp"' in rds_sg
    assert "security_groups = [aws_security_group.lambda.id]" in rds_sg
    assert "cidr_blocks" not in rds_sg.split("ingress {", 1)[1].split("}", 1)[0]


def test_lambda_security_group_has_zero_ingress_blocks():
    lambda_sg = resource_block("aws_security_group", "lambda")
    assert lambda_sg.count("ingress {") == 0


def test_s3_archive_bucket_is_encrypted_private_and_tls_only():
    assert_resource_count("aws_s3_bucket", 1)
    assert_resource_count("aws_s3_bucket_server_side_encryption_configuration", 1)
    assert_resource_count("aws_s3_bucket_public_access_block", 1)
    assert_resource_count("aws_s3_bucket_policy", 1)

    bucket = resource_block("aws_s3_bucket", "event_archive")
    encryption = resource_block("aws_s3_bucket_server_side_encryption_configuration", "event_archive")
    public_access = resource_block("aws_s3_bucket_public_access_block", "event_archive")
    policy = resource_block("aws_s3_bucket_policy", "event_archive_tls")

    assert 'sse_algorithm = "AES256"' in encryption
    for setting in [
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ]:
        assert re.search(rf"\b{setting}\s+=\s+true\b", public_access)
    assert "DenyInsecureTransport" in policy
    assert '"aws:SecureTransport" = "false"' in policy
    assert '"${aws_s3_bucket.event_archive.arn}/*"' in policy
    # TLS-deny must cover both the bucket root ARN and all objects (arn/*)
    assert policy.count("aws_s3_bucket.event_archive.arn") == 2


def test_sqs_eventbridge_rule_and_pipe_are_fully_wired():
    assert_resource_count("aws_sqs_queue", 2)
    assert_resource_count("aws_cloudwatch_event_bus", 1)
    assert_resource_count("aws_cloudwatch_event_rule", 1)
    assert_resource_count("aws_cloudwatch_event_target", 1)
    assert_resource_count("aws_sqs_queue_policy", 1)
    assert_resource_count("aws_pipes_pipe", 1)

    dlq = resource_block("aws_sqs_queue", "dead_letter")
    primary = resource_block("aws_sqs_queue", "primary")
    rule = resource_block("aws_cloudwatch_event_rule", "ingest_work_item")
    target = resource_block("aws_cloudwatch_event_target", "primary_queue")
    queue_policy = resource_block("aws_sqs_queue_policy", "allow_eventbridge_ingest_rule")
    pipe = resource_block("aws_pipes_pipe", "ingest")

    assert "message_retention_seconds = 1209600" in dlq
    assert "visibility_timeout_seconds = 60" in primary
    assert "message_retention_seconds  = 345600" in primary
    assert "sqs_managed_sse_enabled    = true" in primary
    assert "fifo_queue" not in primary
    assert "deadLetterTargetArn = aws_sqs_queue.dead_letter.arn" in primary
    assert "maxReceiveCount     = 3" in primary

    assert "aws_cloudwatch_event_bus.ingest.name" in rule
    assert 'source        = ["app.ingest"]' in rule
    assert '"detail-type" = ["work-item"]' in rule
    assert "arn            = aws_sqs_queue.primary.arn" in target
    assert "Service = \"events.amazonaws.com\"" in queue_policy
    assert 'Effect = "Allow"' in queue_policy
    assert 'Action   = "sqs:SendMessage"' in queue_policy
    assert "Resource = aws_sqs_queue.primary.arn" in queue_policy
    assert '"aws:SourceArn" = aws_cloudwatch_event_rule.ingest_work_item.arn' in queue_policy

    assert re.search(r"\bsource\s+=\s+aws_sqs_queue\.primary\.arn\b", pipe)
    assert "count = var.aws_endpoint == null ? 1 : 0" in pipe
    assert "role_arn   = aws_iam_role.pipes.arn" in pipe
    assert "enrichment = aws_lambda_function.worker.arn" in pipe
    assert "target     = aws_sfn_state_machine.worker.arn" in pipe
    assert "batch_size                         = 1" in pipe
    assert "maximum_batching_window_in_seconds = 1" in pipe
    assert 'invocation_type = "FIRE_AND_FORGET"' in pipe


def test_lambda_package_state_machine_and_logs_match_runtime_contract():
    assert_resource_count("aws_lambda_function", 1)
    assert_resource_count("aws_sfn_state_machine", 1)
    assert_resource_count("aws_cloudwatch_log_group", 2)
    assert TF.count('data "archive_file" "worker_zip"') == 1

    lambda_fn = resource_block("aws_lambda_function", "worker")
    state_machine = resource_block("aws_sfn_state_machine", "worker")
    lambda_log = resource_block("aws_cloudwatch_log_group", "lambda")
    sfn_log = resource_block("aws_cloudwatch_log_group", "step_functions")

    assert 'runtime          = "python3.11"' in lambda_fn
    assert "memory_size      = 256" in lambda_fn
    assert "timeout          = 20" in lambda_fn
    assert "aws_subnet.private_a.id" in lambda_fn
    assert "aws_subnet.private_b.id" in lambda_fn
    assert "security_group_ids = [aws_security_group.lambda.id]" in lambda_fn
    assert "BUCKET_NAME   = aws_s3_bucket.event_archive.id" in lambda_fn
    assert "DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn" in lambda_fn
    assert 'DB_HOST       = var.aws_endpoint == null ? aws_db_instance.postgres[0].address : "db-disabled"' in lambda_fn
    assert 'DB_WRITE_MODE = var.aws_endpoint == null ? "enabled" : "disabled"' in lambda_fn

    assert 'type     = "STANDARD"' in state_machine
    assert 'Resource = "arn:aws:states:::lambda:invoke"' in state_machine
    assert "FunctionName = aws_lambda_function.worker.arn" in state_machine
    assert "log_destination        = \"${aws_cloudwatch_log_group.step_functions.arn}:*\"" in state_machine

    for log_block in [lambda_log, sfn_log]:
        assert "retention_in_days = 14" in log_block
        assert "kms_key_id" not in log_block

    source = re.search(r"content\s+=\s+<<-PY\n(?P<code>.*?)\n\s+PY", TF, re.DOTALL).group("code")
    compile(textwrap.dedent(source), "lambda_function.py", "exec")
    for expected in [
        'boto3.client("s3")',
        "put_object",
        'boto3.client("secretsmanager")',
        "CREATE TABLE IF NOT EXISTS ingest_events",
        "payload JSONB",
        "INSERT INTO ingest_events",
        "ON CONFLICT (id) DO UPDATE",
    ]:
        assert expected in source
    assert "id TEXT PRIMARY KEY" in source
    assert "created_at TIMESTAMPTZ DEFAULT NOW()" in source
    assert 'if os.environ.get("DB_WRITE_MODE", "enabled") != "enabled":' in source


def test_state_machine_passes_full_input_execution_id_and_timestamp_to_lambda():
    state_machine = resource_block("aws_sfn_state_machine", "worker")
    assert '"payload.$"      = "$"' in state_machine
    assert '"execution_id.$" = "$$.Execution.Id"' in state_machine
    assert '"timestamp.$"    = "$$.State.EnteredTime"' in state_machine
    assert "OutputPath = \"$.Payload\"" in state_machine


def test_lambda_runtime_uses_secret_credentials_and_no_inline_db_credentials():
    lambda_fn = resource_block("aws_lambda_function", "worker")
    secret_version = resource_block("aws_secretsmanager_secret_version", "db_credentials")
    rds = resource_block("aws_db_instance", "postgres")

    assert "DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn" in lambda_fn
    assert "password" not in lambda_fn.split("environment {", 1)[1].split("}", 1)[0].lower()
    assert 'DB_WRITE_MODE = var.aws_endpoint == null ? "enabled" : "disabled"' in lambda_fn
    assert "secret_string = jsonencode" in secret_version
    assert "username = local.db_username" in secret_version
    assert "password = random_password.db_password.result" in secret_version
    assert "username                = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string).username" in rds
    assert "password                = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string).password" in rds


def test_iam_roles_are_exact_service_roles_and_no_admin_policy_is_granted():
    assert_resource_count("aws_iam_role", 3)

    lambda_role = resource_block("aws_iam_role", "lambda")
    sfn_role = resource_block("aws_iam_role", "step_functions")
    pipes_role = resource_block("aws_iam_role", "pipes")
    assert 'Service = "lambda.amazonaws.com"' in lambda_role
    assert 'Service = "states.amazonaws.com"' in sfn_role
    assert 'Service = "pipes.amazonaws.com"' in pipes_role

    assert "iam:*" not in TF
    assert '"*:*"' not in TF
    assert "AdministratorAccess" not in TF

    lambda_policy = resource_block("aws_iam_role_policy", "lambda_execution")
    assert "logs:CreateLogStream" in lambda_policy
    assert "logs:PutLogEvents" in lambda_policy
    assert 'Resource = "${aws_cloudwatch_log_group.lambda.arn}:*"' in lambda_policy
    assert 'Action   = "s3:PutObject"' in lambda_policy
    assert 'Resource = "${aws_s3_bucket.event_archive.arn}/*"' in lambda_policy
    assert 'Action   = "secretsmanager:GetSecretValue"' in lambda_policy
    assert "Resource = aws_secretsmanager_secret.db_credentials.arn" in lambda_policy
    assert "ec2:CreateNetworkInterface" in lambda_policy
    assert "ec2:DeleteNetworkInterface" in lambda_policy
    assert "ec2:DescribeNetworkInterfaces" in lambda_policy

    sfn_policy = resource_block("aws_iam_role_policy", "step_functions_execution")
    assert 'Action   = "lambda:InvokeFunction"' in sfn_policy
    assert "Resource = aws_lambda_function.worker.arn" in sfn_policy
    assert "logs:CreateLogDelivery" in sfn_policy

    pipes_policy = resource_block("aws_iam_role_policy", "pipes_execution")
    for action in ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]:
        assert action in pipes_policy
    assert "Resource = aws_sqs_queue.primary.arn" in pipes_policy
    assert 'Action   = "lambda:InvokeFunction"' in pipes_policy
    assert "Resource = aws_lambda_function.worker.arn" in pipes_policy
    assert 'Action   = "states:StartExecution"' in pipes_policy
    assert "Resource = aws_sfn_state_machine.worker.arn" in pipes_policy


def test_rds_postgres_and_secret_contract_are_exact():
    assert_resource_count("aws_db_instance", 1)
    assert_resource_count("aws_db_subnet_group", 1)
    assert_resource_count("aws_secretsmanager_secret", 1)
    assert_resource_count("aws_secretsmanager_secret_version", 1)
    assert_resource_count("random_password", 1)

    subnet_group = resource_block("aws_db_subnet_group", "rds")
    secret = resource_block("aws_secretsmanager_secret", "db_credentials")
    secret_version = resource_block("aws_secretsmanager_secret_version", "db_credentials")
    rds = resource_block("aws_db_instance", "postgres")

    assert "aws_subnet.private_a.id" in subnet_group
    assert "aws_subnet.private_b.id" in subnet_group
    assert "count = var.aws_endpoint == null ? 1 : 0" in subnet_group
    assert "recovery_window_in_days = 0" in secret
    assert "username = local.db_username" in secret_version
    assert "password = random_password.db_password.result" in secret_version

    for name, value in [
        ("engine", '"postgres"'),
        ("engine_version", '"15.4"'),
        ("instance_class", '"db.t3.micro"'),
        ("allocated_storage", "20"),
        ("storage_type", '"gp2"'),
        ("multi_az", "false"),
        ("publicly_accessible", "false"),
        ("storage_encrypted", "true"),
        ("deletion_protection", "false"),
        ("skip_final_snapshot", "true"),
        ("backup_retention_period", "0"),
    ]:
        assert re.search(rf"\b{name}\s+=\s+{re.escape(value)}(?=\s|$)", rds)
    assert "count = var.aws_endpoint == null ? 1 : 0" in rds
    assert re.search(r"\bvpc_security_group_ids\s+=\s+\[aws_security_group\.rds\.id\]", rds)
    assert "db_subnet_group_name    = aws_db_subnet_group.rds[0].name" in rds
    assert re.search(
        r"\busername\s+=\s+jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\.username",
        rds,
    )
    assert re.search(
        r"\bpassword\s+=\s+jsondecode\(aws_secretsmanager_secret_version\.db_credentials\.secret_string\)\.password",
        rds,
    )


def test_observability_alarms_and_sns_notifications_are_exact():
    assert_resource_count("aws_cloudwatch_metric_alarm", 3)
    assert_resource_count("aws_sns_topic", 1)
    assert_resource_count("aws_sns_topic_subscription", 1)

    lambda_alarm = resource_block("aws_cloudwatch_metric_alarm", "lambda_errors")
    sfn_alarm = resource_block("aws_cloudwatch_metric_alarm", "step_functions_failures")
    rds_alarm = resource_block("aws_cloudwatch_metric_alarm", "rds_cpu")
    subscription = resource_block("aws_sns_topic_subscription", "email")

    for alarm in [lambda_alarm, sfn_alarm, rds_alarm]:
        assert "period              = 300" in alarm
        assert "evaluation_periods  = 1" in alarm
        assert 'comparison_operator = "GreaterThanOrEqualToThreshold"' in alarm
        assert "alarm_actions       = [aws_sns_topic.alarms.arn]" in alarm

    assert 'namespace           = "AWS/Lambda"' in lambda_alarm
    assert 'metric_name         = "Errors"' in lambda_alarm
    assert "threshold           = 1" in lambda_alarm
    assert "FunctionName = aws_lambda_function.worker.function_name" in lambda_alarm

    assert 'namespace           = "AWS/States"' in sfn_alarm
    assert 'metric_name         = "ExecutionsFailed"' in sfn_alarm
    assert "threshold           = 1" in sfn_alarm
    assert "StateMachineArn = aws_sfn_state_machine.worker.arn" in sfn_alarm

    assert 'namespace           = "AWS/RDS"' in rds_alarm
    assert 'metric_name         = "CPUUtilization"' in rds_alarm
    assert "threshold           = 80" in rds_alarm
    assert 'DBInstanceIdentifier = var.aws_endpoint == null ? aws_db_instance.postgres[0].identifier : local.db_identifier' in rds_alarm

    assert 'protocol  = "email"' in subscription
    assert 'endpoint  = "alerts@example.com"' in subscription


def test_event_rule_pattern_is_exact_and_complete():
    rule = resource_block("aws_cloudwatch_event_rule", "ingest_work_item")
    assert 'source        = ["app.ingest"]' in rule
    assert '"detail-type" = ["work-item"]' in rule
    event_pattern = re.search(r"event_pattern\s*=\s*jsonencode\(\{(?P<body>.*?)\}\)", rule, re.DOTALL)
    assert event_pattern, "aws_cloudwatch_event_rule.ingest_work_item must define event_pattern"
    keys = re.findall(r'^\s*("?detail-type"?|source)\s*=', event_pattern.group("body"), re.MULTILINE)
    assert keys == ["source", '"detail-type"']


def test_teardown_stays_simple_and_state_machine_has_no_direct_trigger():
    forbidden = [
        "prevent_destroy",
        "deletion_protection    = true",
        "deletion_protection = true",
        "skip_destroy",
        "retain_on_delete",
        "aws_lambda_permission",
        "aws_lambda_event_source_mapping",
        "aws_s3_bucket_object_lock_configuration",
        "aws_backup",
    ]
    for item in forbidden:
        assert item not in TF

    event_target = resource_block("aws_cloudwatch_event_target", "primary_queue")
    assert "aws_sfn_state_machine.worker.arn" not in event_target
    assert "aws_lambda_function.worker.arn" not in event_target


def test_state_machine_start_permission_exists_only_on_pipes_role():
    role_policies = re.findall(r'resource\s+"aws_iam_role_policy"\s+"([^"]+)"', TF)
    start_execution_policies = []
    for policy_name in role_policies:
        policy = resource_block("aws_iam_role_policy", policy_name)
        if "states:StartExecution" in policy:
            start_execution_policies.append(policy_name)

    assert start_execution_policies == ["pipes_execution"]
    assert "Resource = aws_sfn_state_machine.worker.arn" in resource_block("aws_iam_role_policy", "pipes_execution")
