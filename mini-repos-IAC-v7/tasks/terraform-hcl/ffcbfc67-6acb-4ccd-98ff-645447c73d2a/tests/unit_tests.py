import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
VARIABLES_TF = ROOT / "variables.tf"


def read_text(path: Path) -> str:
    return path.read_text()


def combined_tf_text() -> str:
    return f"{read_text(VARIABLES_TF)}\n{read_text(MAIN_TF)}"


def _extract_blocks(text: str, block_kind: str):
    lines = text.splitlines()
    blocks = []
    index = 0

    while index < len(lines):
        line = lines[index]
        match = None

        if block_kind in {"resource", "data"}:
            match = re.match(rf'^{block_kind} "([^"]+)" "([^"]+)" \{{$', line)
            if match:
                identifier = (match.group(1), match.group(2))
        else:
            match = re.match(rf'^{block_kind} "([^"]+)" \{{$', line)
            if match:
                identifier = (match.group(1),)

        if not match:
            index += 1
            continue

        start = index
        brace_depth = line.count("{") - line.count("}")
        heredoc_end = None
        index += 1

        while index < len(lines) and brace_depth > 0:
            current = lines[index]
            if heredoc_end:
                if current.strip() == heredoc_end:
                    heredoc_end = None
                index += 1
                continue

            heredoc_match = re.search(r"<<-?([A-Z0-9_]+)", current)
            if heredoc_match:
                heredoc_end = heredoc_match.group(1)
                index += 1
                continue

            brace_depth += current.count("{") - current.count("}")
            index += 1

        block_text = "\n".join(lines[start:index])
        blocks.append((*identifier, block_text))

    return blocks


def resource_blocks(resource_type: str):
    return [
        (name, body)
        for current_type, name, body in _extract_blocks(read_text(MAIN_TF), "resource")
        if current_type == resource_type
    ]


def data_blocks(data_type: str):
    return [
        (name, body)
        for current_type, name, body in _extract_blocks(read_text(MAIN_TF), "data")
        if current_type == data_type
    ]


def provider_block(name: str) -> str:
    for current_name, body in _extract_blocks(read_text(MAIN_TF), "provider"):
        if current_name == name:
            return body
    raise AssertionError(f"Missing provider {name}")


def variable_blocks():
    return {name: body for name, body in _extract_blocks(combined_tf_text(), "variable")}


def output_blocks():
    return {name: body for name, body in _extract_blocks(read_text(MAIN_TF), "output")}


def resource_body(resource_type: str, name: str) -> str:
    for current_name, body in resource_blocks(resource_type):
        if current_name == name:
            return body
    raise AssertionError(f"Missing resource {resource_type}.{name}")


def single_resource_block(resource_type: str):
    blocks = resource_blocks(resource_type)
    assert len(blocks) == 1, f"Expected exactly 1 {resource_type}, got {len(blocks)}"
    return blocks[0]


def extract_ref(block: str, pattern: str, description: str) -> str:
    match = re.search(pattern, block, re.DOTALL)
    assert match is not None, f"Missing {description}"
    return match.group(1)


def find_first(items, predicate, description: str):
    for item in items:
        if predicate(item):
            return item
    raise AssertionError(f"Unable to find {description}")


def find_role_policies_for(role_name: str):
    matches = []
    for _, body in resource_blocks("aws_iam_role_policy"):
        if re.search(
            rf"role\s*=\s*aws_iam_role\.{re.escape(role_name)}\.(id|name)",
            body,
        ):
            matches.append(body)
    return matches


def role_policy_attachments_for(role_name: str):
    matches = []
    for _, body in resource_blocks("aws_iam_role_policy_attachment"):
        if re.search(
            rf"role\s*=\s*aws_iam_role\.{re.escape(role_name)}\.(id|name)",
            body,
        ):
            matches.append(body)
    return matches


def assert_only_service_principal(role_body: str, service_principal: str):
    assert re.search(r"assume_role_policy\s*=\s*jsonencode\(", role_body)
    services = set(re.findall(r'"Service"\s*:\s*"([^"]+)"', role_body))
    if not services:
        services = set(re.findall(r'Service\s*=\s*"([^"]+)"', role_body))
    assert services == {service_principal}
    assert '"AWS"' not in role_body
    assert '"Federated"' not in role_body
    assert '"*"' not in role_body


def source_locals():
    matches = re.findall(
        r'^\s*([A-Za-z0-9_]+)\s*=\s*<<-PY\n(.*?)\n\s*PY',
        read_text(MAIN_TF),
        re.MULTILINE | re.DOTALL,
    )
    return {name: body for name, body in matches}


def optional_indexed_ref(resource_type: str, name: str, attribute_pattern: str) -> str:
    return (
        rf"(one\(\s*{resource_type}\.{name}\[\*\]\.{attribute_pattern}\s*\)"
        rf"|{resource_type}\.{name}(?:\[\d+\])?\.{attribute_pattern})"
    )


def test_input_contract_and_single_main_tf_alignment():
    variables = variable_blocks()
    expected_variables = {
        "aws_region",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_endpoint",
    }

    assert set(variables) == expected_variables
    assert 'default     = "us-east-1"' in variables["aws_region"]
    assert "sensitive   = true" in variables["aws_access_key_id"]
    assert "sensitive   = true" in variables["aws_secret_access_key"]
    assert "type        = string" in variables["aws_endpoint"]
    assert "default     = null" in variables["aws_endpoint"]


def test_provider_uses_the_required_variables_and_endpoint_override():
    provider = provider_block("aws")

    assert re.search(r"region\s*=\s*var\.aws_region", provider)
    assert re.search(r"access_key\s*=\s*var\.aws_access_key_id", provider)
    assert re.search(r"secret_key\s*=\s*var\.aws_secret_access_key", provider)
    assert "endpoints {" in provider

    for service_name in [
        "apigatewayv2",
        "cloudwatchlogs",
        "ec2",
        "iam",
        "lambda",
        "pipes",
        "rds",
        "secretsmanager",
        "sfn",
        "sqs",
        "sts",
    ]:
        assert re.search(rf"{service_name}\s*=\s*var\.aws_endpoint", provider)


def test_network_foundation_and_security_group_scope_are_declared():
    _, vpc = single_resource_block("aws_vpc")
    subnets = resource_blocks("aws_subnet")
    processing_group = resource_body("aws_security_group", "processing_units")
    storage_group = resource_body("aws_security_group", "storage_layer")
    ingress_rules = [body for _, body in resource_blocks("aws_vpc_security_group_ingress_rule")]
    egress_rules = [body for _, body in resource_blocks("aws_vpc_security_group_egress_rule")]

    assert 'cidr_block           = "10.20.0.0/16"' in vpc
    assert "enable_dns_hostnames = true" in vpc
    assert "enable_dns_support   = true" in vpc

    assert len(subnets) == 2, f"Expected exactly 2 private subnets, got {len(subnets)}"
    subnet_bodies = [body for _, body in subnets]
    assert any('cidr_block              = "10.20.1.0/24"' in body for body in subnet_bodies)
    assert any('cidr_block              = "10.20.2.0/24"' in body for body in subnet_bodies)
    assert all("map_public_ip_on_launch = false" in body for body in subnet_bodies)

    az_values = []
    for body in subnet_bodies:
        match = re.search(r"availability_zone\s*=\s*(.+)", body)
        assert match is not None, "Each subnet must declare an availability_zone"
        az_values.append(match.group(1).strip())
    assert len(set(az_values)) == 2, "Subnets must use different availability zones"

    assert "vpc_id      = aws_vpc.cloud_boundaries.id" in processing_group
    assert "vpc_id      = aws_vpc.cloud_boundaries.id" in storage_group

    assert any(
        "security_group_id = aws_security_group.processing_units.id" in rule
        and 'cidr_ipv4         = "0.0.0.0/0"' in rule
        and "from_port         = 443" in rule
        and "to_port           = 443" in rule
        for rule in egress_rules
    )
    assert any(
        "security_group_id            = aws_security_group.processing_units.id" in rule
        and "referenced_security_group_id = aws_security_group.storage_layer.id" in rule
        and "from_port                    = 5432" in rule
        and "to_port                      = 5432" in rule
        for rule in egress_rules
    )
    assert any(
        "security_group_id = aws_security_group.storage_layer.id" in rule
        and 'cidr_ipv4         = "0.0.0.0/0"' in rule
        and "from_port         = 443" in rule
        and "to_port           = 443" in rule
        for rule in egress_rules
    )

    storage_ingress_rules = [
        rule
        for rule in ingress_rules
        if "security_group_id            = aws_security_group.storage_layer.id" in rule
    ]
    assert storage_ingress_rules, "Storage security group must have a scoped ingress rule"
    assert any(
        "referenced_security_group_id = aws_security_group.processing_units.id" in rule
        and "from_port                    = 5432" in rule
        and "to_port                      = 5432" in rule
        for rule in storage_ingress_rules
    )
    assert all('cidr_ipv4         = "0.0.0.0/0"' not in rule for rule in storage_ingress_rules)


def test_storage_layer_uses_generated_secret_material_and_private_rds():
    _, secret_version = single_resource_block("aws_secretsmanager_secret_version")
    _, password = single_resource_block("random_password")
    _, subnet_group = single_resource_block("aws_db_subnet_group")
    _, db_instance = single_resource_block("aws_db_instance")

    assert "length  = 24" in password or "length = 24" in password
    assert "special = true" in password

    assert "secret_string = jsonencode(" in secret_version
    assert re.search(r"username\s*=\s*\"payments_app\"", secret_version)
    assert re.search(r"password\s*=\s*random_password\.[^.]+\.result", secret_version)
    assert "aws_db_instance." not in secret_version, "Secret material must not depend on the DB instance"

    assert re.search(
        r"subnet_ids\s*=\s*\[[^\]]*aws_subnet\.[^.]+\.id[^\]]*aws_subnet\.[^.]+\.id[^\]]*\]",
        subnet_group,
        re.DOTALL,
    )
    assert re.search(r'engine\s*=\s*"postgres"', db_instance)
    assert re.search(r'engine_version\s*=\s*"15\.4"', db_instance)
    assert re.search(r'instance_class\s*=\s*"db\.t3\.micro"', db_instance)
    assert re.search(r"allocated_storage\s*=\s*20", db_instance)
    assert re.search(r'storage_type\s*=\s*"gp2"', db_instance)
    assert re.search(r"publicly_accessible\s*=\s*false", db_instance)
    assert re.search(r"skip_final_snapshot\s*=\s*true", db_instance)
    assert "deletion_protection" not in db_instance
    assert re.search(
        rf"db_subnet_group_name\s*=\s*{optional_indexed_ref('aws_db_subnet_group', 'storage_layer', '(name|id)')}",
        db_instance,
    )
    assert re.search(
        r"vpc_security_group_ids\s*=\s*\[\s*aws_security_group\.storage_layer\.id\s*\]",
        db_instance,
    )
    assert re.search(
        r'username\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.[^.]+\.secret_string\s*\)(\["username"\]|\.username)',
        db_instance,
    )
    assert re.search(
        r'password\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.[^.]+\.secret_string\s*\)(\["password"\]|\.password)',
        db_instance,
    )
    assert 'password = "' not in db_instance


def test_log_groups_have_required_retention_without_kms():
    application_logs = resource_body("aws_cloudwatch_log_group", "application")
    api_access_logs = resource_body("aws_cloudwatch_log_group", "api_access")
    stage = resource_body("aws_apigatewayv2_stage", "default")

    assert len(resource_blocks("aws_cloudwatch_log_group")) == 2
    assert 'retention_in_days = 14' in application_logs
    assert 'retention_in_days = 14' in api_access_logs
    assert "kms_key_id" not in application_logs
    assert "kms_key_id" not in api_access_logs
    assert "destination_arn = aws_cloudwatch_log_group.api_access.arn" in stage


def test_api_gateway_queue_and_routes_are_explicit():
    _, api = single_resource_block("aws_apigatewayv2_api")
    _, stage = single_resource_block("aws_apigatewayv2_stage")
    _, queue = single_resource_block("aws_sqs_queue")
    integrations = resource_blocks("aws_apigatewayv2_integration")
    routes = resource_blocks("aws_apigatewayv2_route")

    assert 'protocol_type = "HTTP"' in api
    assert "visibility_timeout_seconds = 60" in queue
    assert "message_retention_seconds  = 1209600" in queue
    assert 'name        = "$default"' in stage
    assert "auto_deploy = true" in stage
    assert "access_log_settings" in stage

    assert len(integrations) == 2, f"Expected exactly 2 API integrations, got {len(integrations)}"
    assert len(routes) == 2, f"Expected exactly 2 API routes, got {len(routes)}"

    route_keys = {
        extract_ref(body, r'route_key\s*=\s*"([^"]+)"', f"route key for {name}")
        for name, body in routes
    }
    assert route_keys == {"POST /submit", "GET /health"}

    submit_route = resource_body("aws_apigatewayv2_route", "submit")
    health_route = resource_body("aws_apigatewayv2_route", "health")
    assert re.search(
        r'target\s*=\s*"integrations/\$\{(one\(aws_apigatewayv2_integration\.submit\[\*\]\.id\)|aws_apigatewayv2_integration\.submit(?:\[\d+\])?\.id)\}"',
        submit_route,
    )
    assert re.search(
        r'target\s*=\s*"integrations/\$\{(one\(aws_apigatewayv2_integration\.health\[\*\]\.id\)|aws_apigatewayv2_integration\.health(?:\[\d+\])?\.id)\}"',
        health_route,
    )

    assert "SQS-SendMessage" in resource_body("aws_apigatewayv2_integration", "submit")
    assert re.search(
        r"integration_uri\s*=\s*aws_lambda_function\.health\.invoke_arn",
        resource_body("aws_apigatewayv2_integration", "health"),
    )


def test_api_gateway_to_sqs_iam_scope_is_least_privilege():
    queue_policy = resource_body("aws_sqs_queue_policy", "intake")
    sqs_integration = resource_body("aws_apigatewayv2_integration", "submit")
    permission = resource_body("aws_lambda_permission", "api_health")

    api_role_name = extract_ref(
        sqs_integration,
        r"credentials_arn\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "API Gateway credentials role",
    )
    api_role = resource_body("aws_iam_role", api_role_name)
    api_role_policies = find_role_policies_for(api_role_name)

    assert_only_service_principal(api_role, "apigateway.amazonaws.com")
    assert api_role_policies, "Expected an inline policy attached to the API Gateway role"
    assert any('"sqs:SendMessage"' in body for body in api_role_policies)
    assert all(set(re.findall(r'"sqs:[A-Za-z]+"' , body)) <= {'"sqs:SendMessage"'} for body in api_role_policies)
    assert any(re.search(r"Resource\s*=\s*aws_sqs_queue\.[^.]+\.arn", body) for body in api_role_policies)

    assert re.search(r"queue_url\s*=\s*aws_sqs_queue\.[^.]+\.id", queue_policy)
    assert '"sqs:SendMessage"' in queue_policy
    assert re.search(r"Resource\s*=\s*aws_sqs_queue\.[^.]+\.arn", queue_policy)
    assert re.search(
        r"Principal\s*=\s*\{[^}]*AWS\s*=\s*aws_iam_role\." + re.escape(api_role_name) + r"\.arn",
        queue_policy,
        re.DOTALL,
    )
    assert re.search(
        r'source_arn\s*=\s*"\$\{one\(aws_apigatewayv2_api\.front_door\[\*\]\.execution_arn\)\}/\*/GET/health"',
        permission,
    )


def test_lambda_packaging_worker_vpc_and_health_invocation_contract():
    lambdas = resource_blocks("aws_lambda_function")
    event_source_mapping = resource_body("aws_lambda_event_source_mapping", "worker")
    permission = resource_body("aws_lambda_permission", "api_health")

    assert len(lambdas) == 3, f"Expected exactly 3 Lambda functions, got {len(lambdas)}"
    assert len(data_blocks("archive_file")) == 3, "All Lambda artifacts must be packaged with archive_file"

    for lambda_name, body in lambdas:
        assert re.search(r'runtime\s*=\s*"python3\.12"', body), f"{lambda_name} must use python3.12"
        assert re.search(r'handler\s*=\s*"app\.handler"', body), f"{lambda_name} must use app.handler"
        assert "data.archive_file." in body, f"{lambda_name} must be deployed from an archive_file artifact"

    worker_name = extract_ref(
        event_source_mapping,
        r"function_name\s*=\s*aws_lambda_function\.([^.]+)\.(arn|function_name)",
        "worker lambda reference",
    )
    health_name = extract_ref(
        permission,
        r"function_name\s*=\s*aws_lambda_function\.([^.]+)\.(function_name|arn)",
        "health lambda reference",
    )

    worker_body = resource_body("aws_lambda_function", worker_name)
    health_body = resource_body("aws_lambda_function", health_name)
    enrichment_body = resource_body("aws_lambda_function", "enrichment")

    worker_role_name = extract_ref(worker_body, r"role\s*=\s*aws_iam_role\.([^.]+)\.arn", "worker IAM role")
    health_role_name = extract_ref(health_body, r"role\s*=\s*aws_iam_role\.([^.]+)\.arn", "health IAM role")
    enrichment_role_name = extract_ref(
        enrichment_body,
        r"role\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "enrichment IAM role",
    )

    worker_role = resource_body("aws_iam_role", worker_role_name)
    health_role = resource_body("aws_iam_role", health_role_name)
    enrichment_role = resource_body("aws_iam_role", enrichment_role_name)
    worker_policies = find_role_policies_for(worker_role_name)
    health_policies = find_role_policies_for(health_role_name)
    health_attachments = role_policy_attachments_for(health_role_name)

    assert "batch_size                         = 5" in event_source_mapping
    assert "maximum_batching_window_in_seconds = 5" in event_source_mapping
    assert "event_source_arn                   = aws_sqs_queue.intake.arn" in event_source_mapping

    assert "vpc_config" in worker_body
    assert re.search(
        r"subnet_ids\s*=\s*\[[^\]]*aws_subnet\.[^.]+\.id[^\]]*aws_subnet\.[^.]+\.id[^\]]*\]",
        worker_body,
        re.DOTALL,
    )
    assert re.search(
        r"security_group_ids\s*=\s*\[\s*aws_security_group\.processing_units\.id\s*\]",
        worker_body,
    )
    assert "vpc_config" not in health_body
    assert worker_name != health_name
    assert worker_role_name != health_role_name

    assert any('"sqs:ReceiveMessage"' in body for body in worker_policies)
    assert any('"sqs:DeleteMessage"' in body for body in worker_policies)
    assert any('"sqs:GetQueueAttributes"' in body for body in worker_policies)
    assert any('"secretsmanager:GetSecretValue"' in body for body in worker_policies)
    assert any(re.search(r"Resource\s*=\s*aws_sqs_queue\.[^.]+\.arn", body) for body in worker_policies)
    assert any(re.search(r"Resource\s*=\s*aws_secretsmanager_secret\.[^.]+\.arn", body) for body in worker_policies)

    assert all("sqs:" not in body for body in health_policies)
    assert all("secretsmanager:" not in body for body in health_policies)
    assert all("rds" not in body.lower() for body in health_policies)
    assert health_attachments, "Health role must have at least one managed policy attachment"
    assert all("AWSLambdaBasicExecutionRole" in body for body in health_attachments)
    assert all(not re.search(r"(sqs|secret|rds)", body, re.IGNORECASE) for body in health_attachments)

    for role_body in [worker_role, health_role, enrichment_role]:
        assert_only_service_principal(role_body, "lambda.amazonaws.com")

    assert re.search(r"memory_size\s*=\s*256", enrichment_body)
    assert re.search(r"timeout\s*=\s*15", enrichment_body)


def test_lambda_stub_logic_matches_required_behavior():
    local_sources = source_locals()
    worker_source = find_first(
        local_sources.values(),
        lambda body: 'boto3.client("secretsmanager")' in body,
        "worker source local",
    )
    health_source = find_first(
        local_sources.values(),
        lambda body: '"statusCode": 200' in body,
        "health source local",
    )
    enrichment_source = find_first(
        local_sources.values(),
        lambda body: "return event" in body,
        "enrichment source local",
    )

    assert 'os.environ["SECRET_ARN"]' in worker_source
    assert 'os.environ["DB_HOST"]' in worker_source
    assert "json.loads" in worker_source
    assert "socket.create_connection" in worker_source
    assert "5432" in worker_source
    assert '"status": "processed"' in worker_source

    assert "return {" in health_source
    assert '"statusCode": 200' in health_source
    assert '"body": json.dumps({"status": "ok"})' in health_source

    assert "return event" in enrichment_source


def test_step_functions_and_pipe_permissions_are_scoped():
    state_machine = resource_body("aws_sfn_state_machine", "processing")
    pipe = resource_body("aws_pipes_pipe", "processing")
    pipe_role_name = extract_ref(pipe, r"role_arn\s*=\s*aws_iam_role\.([^.]+)\.arn", "pipe IAM role")
    state_machine_role_name = extract_ref(
        state_machine,
        r"role_arn\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "state machine IAM role",
    )

    pipe_role = resource_body("aws_iam_role", pipe_role_name)
    state_machine_role = resource_body("aws_iam_role", state_machine_role_name)
    pipe_policies = find_role_policies_for(pipe_role_name)

    assert 'type     = "STANDARD"' in state_machine or 'type = "STANDARD"' in state_machine
    assert len(re.findall(r'Type\s*=\s*"Pass"', state_machine)) == 1
    assert len(re.findall(r'Type\s*=\s*"Succeed"', state_machine)) == 1
    assert_only_service_principal(state_machine_role, "states.amazonaws.com")
    assert_only_service_principal(pipe_role, "pipes.amazonaws.com")

    assert re.search(r"source\s*=\s*aws_sqs_queue\.[^.]+\.arn", pipe)
    assert re.search(r"enrichment\s*=\s*aws_lambda_function\.enrichment\.arn", pipe)
    assert re.search(r"target\s*=\s*aws_sfn_state_machine\.processing\.arn", pipe)

    assert pipe_policies, "Expected a scoped inline policy for the pipe role"
    joined_policies = "\n".join(pipe_policies)
    for action in ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]:
        assert f'"{action}"' in joined_policies
    assert '"lambda:InvokeFunction"' in joined_policies
    assert '"states:StartExecution"' in joined_policies
    assert re.search(r"Resource\s*=\s*aws_sqs_queue\.intake\.arn", joined_policies)
    assert "Resource = \"*\"" not in joined_policies
    assert "aws_lambda_function.enrichment.arn" in joined_policies
    assert "aws_sfn_state_machine.processing.arn" in joined_policies


def test_outputs_and_security_guardrails_are_present():
    tf_text = read_text(MAIN_TF)
    outputs = output_blocks()
    worker_policy = resource_body("aws_iam_role_policy", "worker")

    assert set(outputs) == {
        "http_api_endpoint_url",
        "sqs_queue_url",
        "rds_endpoint_address",
        "secrets_manager_secret_arn",
    }
    assert re.search(
        r"(aws_apigatewayv2_(api|stage)\.[^.]+(?:\[\*\])?\.(api_endpoint|invoke_url)|local\.http_api_endpoint_url)",
        outputs["http_api_endpoint_url"],
    )
    assert re.search(r"aws_sqs_queue\.[^.]+\.url", outputs["sqs_queue_url"])
    assert re.search(
        r"(aws_db_instance\.[^.]+(?:\[\*\])?\.address|local\.rds_endpoint_address)",
        outputs["rds_endpoint_address"],
    )
    assert re.search(r"aws_secretsmanager_secret\.[^.]+\.arn", outputs["secrets_manager_secret_arn"])

    assert 'Action = "*"' not in tf_text
    assert '"Action":"*"' not in tf_text
    assert len(re.findall(r'Resource\s*=\s*"\*"', tf_text)) == 1
    assert re.search(
        r'Sid\s*=\s*"ManageVpcEnis".*?# EC2 network-interface APIs do not support resource-level scoping\.\s*Resource\s*=\s*"\*"',
        worker_policy,
        re.DOTALL,
    )
    assert "rds-db:connect" not in tf_text
