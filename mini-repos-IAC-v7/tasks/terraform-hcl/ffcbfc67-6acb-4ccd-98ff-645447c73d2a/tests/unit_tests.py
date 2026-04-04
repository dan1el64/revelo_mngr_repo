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
            match = re.match(
                rf'^{block_kind} "([^"]+)" "([^"]+)" \{{$',
                line,
            )
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
    text = combined_tf_text()
    return {name: body for name, body in _extract_blocks(text, "variable")}


def output_blocks():
    return {name: body for name, body in _extract_blocks(read_text(MAIN_TF), "output")}


def rule_blocks(resource_type: str):
    return [body for _, body in resource_blocks(resource_type)]


def single_resource_block(resource_type: str):
    blocks = resource_blocks(resource_type)
    assert len(blocks) == 1, f"Expected exactly 1 {resource_type}, got {len(blocks)}"
    return blocks[0]


def resource_body(resource_type: str, name: str) -> str:
    for current_name, body in resource_blocks(resource_type):
        if current_name == name:
            return body
    raise AssertionError(f"Missing resource {resource_type}.{name}")


def names_for(resource_type: str):
    return [name for name, _ in resource_blocks(resource_type)]


def extract_ref(block: str, pattern: str, description: str) -> str:
    match = re.search(pattern, block, re.DOTALL)
    assert match is not None, f"Missing {description}"
    return match.group(1)


def find_role_policies_for(role_name: str):
    matches = []
    for _, body in resource_blocks("aws_iam_role_policy"):
        if re.search(
            rf"role\s*=\s*aws_iam_role\.{re.escape(role_name)}\.(id|name)",
            body,
        ):
            matches.append(body)
    return matches


def has_output_for(pattern: str) -> bool:
    return any(re.search(pattern, body) for body in output_blocks().values())


def assert_only_service_principal(role_body: str, service_principal: str):
    assert re.search(r"assume_role_policy\s*=\s*jsonencode\(", role_body)
    services = set(re.findall(r'"Service"\s*:\s*"([^"]+)"', role_body))
    if not services:
        services = set(re.findall(r'Service\s*=\s*"([^"]+)"', role_body))
    assert services == {service_principal}
    assert '"AWS"' not in role_body
    assert '"Federated"' not in role_body
    assert '"*"' not in role_body


def optional_indexed_ref(resource_type: str, name: str, attribute_pattern: str) -> str:
    return rf"(one\(\s*{resource_type}\.{name}\[\*\]\.{attribute_pattern}\s*\)|{resource_type}\.{name}(?:\[\d+\])?\.{attribute_pattern})"


def test_input_contract_and_single_main_tf_alignment():
    variables = variable_blocks()

    assert {"aws_region", "aws_access_key_id", "aws_secret_access_key"}.issubset(variables)
    assert 'default     = "us-east-1"' in variables["aws_region"]


def test_provider_uses_the_required_variables():
    provider = provider_block("aws")

    assert re.search(r"region\s*=\s*var\.aws_region", provider)
    assert re.search(r"access_key\s*=\s*var\.aws_access_key_id", provider)
    assert re.search(r"secret_key\s*=\s*var\.aws_secret_access_key", provider)


def test_network_foundation_and_security_group_scope_are_declared():
    _, vpc = single_resource_block("aws_vpc")
    subnets = resource_blocks("aws_subnet")
    processing_group = resource_body("aws_security_group", "processing_units")
    storage_group = resource_body("aws_security_group", "storage_layer")
    ingress_rules = rule_blocks("aws_vpc_security_group_ingress_rule")
    egress_rules = rule_blocks("aws_vpc_security_group_egress_rule")

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
    assert len(set(az_values)) == 2, "Subnets must be placed in different availability zones"

    assert len(resource_blocks("aws_security_group")) == 2
    assert "vpc_id      = aws_vpc.cloud_boundaries.id" in processing_group
    assert "vpc_id      = aws_vpc.cloud_boundaries.id" in storage_group

    processing_egress = [
        rule
        for rule in egress_rules
        if re.search(r"^\s*security_group_id\s*=\s*aws_security_group\.processing_units\.id", rule, re.MULTILINE)
    ]
    storage_egress = [
        rule
        for rule in egress_rules
        if re.search(r"^\s*security_group_id\s*=\s*aws_security_group\.storage_layer\.id", rule, re.MULTILINE)
    ]
    storage_ingress = [
        rule
        for rule in ingress_rules
        if re.search(r"^\s*security_group_id\s*=\s*aws_security_group\.storage_layer\.id", rule, re.MULTILINE)
    ]

    assert len(egress_rules) == 3
    assert len(ingress_rules) == 1
    assert len(processing_egress) == 2
    assert len(storage_egress) == 1
    assert len(storage_ingress) == 1

    assert any(
        'cidr_ipv4         = "0.0.0.0/0"' in rule
        and "from_port         = 443" in rule
        and "to_port           = 443" in rule
        for rule in processing_egress
    )
    assert any(
        "referenced_security_group_id = aws_security_group.storage_layer.id" in rule
        and "from_port                    = 5432" in rule
        and "to_port                      = 5432" in rule
        for rule in processing_egress
    )
    assert (
        'cidr_ipv4         = "0.0.0.0/0"' in storage_egress[0]
        and "from_port         = 443" in storage_egress[0]
        and "to_port           = 443" in storage_egress[0]
    )
    assert (
        "referenced_security_group_id = aws_security_group.processing_units.id" in storage_ingress[0]
        and "from_port                    = 5432" in storage_ingress[0]
        and "to_port                      = 5432" in storage_ingress[0]
    )
    assert 'cidr_ipv4         = "0.0.0.0/0"' not in storage_ingress[0]


def test_storage_layer_uses_generated_secret_material_and_private_rds():
    tf_text = read_text(MAIN_TF)
    _, secret = single_resource_block("aws_secretsmanager_secret")
    _, secret_version = single_resource_block("aws_secretsmanager_secret_version")
    _, password = single_resource_block("random_password")
    _, subnet_group = single_resource_block("aws_db_subnet_group")
    _, db_instance = single_resource_block("aws_db_instance")

    assert secret
    assert "length  = 24" in password or "length = 24" in password
    assert "special = true" in password
    assert "secret_string = jsonencode(" in secret_version
    assert re.search(r"username\s*=", secret_version)
    assert re.search(r"password\s*=\s*random_password\.[^.]+\.result", secret_version)

    assert re.search(r"subnet_ids\s*=\s*\[[^\]]*aws_subnet\.[^.]+\.id[^\]]*aws_subnet\.[^.]+\.id[^\]]*\]", subnet_group, re.DOTALL)
    assert re.search(r'engine\s*=\s*"postgres"', db_instance)
    assert re.search(r'engine_version\s*=\s*"15\.4"', db_instance)
    assert re.search(r'instance_class\s*=\s*"db\.t3\.micro"', db_instance)
    assert re.search(r"allocated_storage\s*=\s*20", db_instance)
    assert re.search(r'storage_type\s*=\s*"gp2"', db_instance)
    assert re.search(r"publicly_accessible\s*=\s*false", db_instance)
    assert re.search(r"enabled_cloudwatch_logs_exports\s*=\s*\[[^\]]*\"postgresql\"[^\]]*\]", db_instance)
    assert re.search(
        rf"db_subnet_group_name\s*=\s*{optional_indexed_ref('aws_db_subnet_group', '[^.]+', '(name|id)')}",
        db_instance,
    )
    assert re.search(
        r"vpc_security_group_ids\s*=\s*\[\s*aws_security_group\.[^.]+\.id\s*\]",
        db_instance,
    ), "RDS must reference exactly one DB security group"
    assert re.search(
        r'username\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.[^.]+\.secret_string\s*\)(\["username"\]|\.username)',
        db_instance,
    )
    assert re.search(
        r'password\s*=\s*jsondecode\(\s*aws_secretsmanager_secret_version\.[^.]+\.secret_string\s*\)(\["password"\]|\.password)',
        db_instance,
    )
    assert 'password = "' not in db_instance
    assert re.search(r"skip_final_snapshot\s*=\s*true", db_instance)
    assert "deletion_protection = true" not in tf_text


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
    assert re.search(r"destination_arn\s*=\s*aws_cloudwatch_log_group\.[^.]+\.arn", stage)

    assert len(integrations) == 2, f"Expected exactly 2 API integrations, got {len(integrations)}"
    assert len(routes) == 2, f"Expected exactly 2 API routes, got {len(routes)}"

    route_bodies = [body for _, body in routes]
    submit_route = next(body for body in route_bodies if 'route_key = "POST /submit"' in body)
    health_route = next(body for body in route_bodies if 'route_key = "GET /health"' in body)
    assert re.search(
        r'target\s*=\s*"integrations/\$\{(one\(aws_apigatewayv2_integration\.submit\[\*\]\.id\)|aws_apigatewayv2_integration\.submit(?:\[\d+\])?\.id)\}"',
        submit_route,
    )
    assert re.search(
        r'target\s*=\s*"integrations/\$\{(one\(aws_apigatewayv2_integration\.health\[\*\]\.id\)|aws_apigatewayv2_integration\.health(?:\[\d+\])?\.id)\}"',
        health_route,
    )

    integration_bodies = [body for _, body in integrations]
    assert any("SQS-SendMessage" in body for body in integration_bodies), (
        "POST /submit must use an explicit SQS service integration"
    )
    assert any(
        re.search(r"integration_uri\s*=\s*aws_lambda_function\.[^.]+\.invoke_arn", body)
        for body in integration_bodies
    ), "GET /health must use a Lambda proxy integration"


def test_api_gateway_to_sqs_iam_scope_is_least_privilege():
    _, queue_policy = single_resource_block("aws_sqs_queue_policy")
    integration_bodies = [body for _, body in resource_blocks("aws_apigatewayv2_integration")]
    sqs_integration = next(
        (body for body in integration_bodies if "SQS-SendMessage" in body),
        None,
    )

    assert sqs_integration is not None, "Missing SQS API Gateway integration"
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
    assert re.search(r"Principal\s*=\s*\{[^}]*AWS\s*=\s*aws_iam_role\." + re.escape(api_role_name) + r"\.arn", queue_policy, re.DOTALL)


def test_lambda_packaging_worker_vpc_and_health_invocation_contract():
    lambdas = resource_blocks("aws_lambda_function")
    _, event_source_mapping = single_resource_block("aws_lambda_event_source_mapping")
    _, permission = single_resource_block("aws_lambda_permission")

    assert len(lambdas) == 3, f"Expected exactly 3 Lambda functions, got {len(lambdas)}"
    assert len(data_blocks("archive_file")) == 3, "All Lambda artifacts must be packaged with archive_file"

    for lambda_name, body in lambdas:
        assert re.search(r'runtime\s*=\s*"python3\.12"', body)
        assert re.search(r'handler\s*=\s*"app\.handler"', body)
        assert "data.archive_file." in body, f"{lambda_name} must be deployed from an archive_file artifact"

    worker_name = extract_ref(
        event_source_mapping,
        r"function_name\s*=\s*aws_lambda_function\.([^.]+)\.(arn|function_name)",
        "worker lambda reference",
    )
    worker_body = resource_body("aws_lambda_function", worker_name)
    worker_role_name = extract_ref(
        worker_body,
        r"role\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "worker IAM role",
    )
    worker_policies = find_role_policies_for(worker_role_name)

    assert "batch_size                         = 5" in event_source_mapping
    assert "maximum_batching_window_in_seconds = 5" in event_source_mapping
    assert "vpc_config" in worker_body
    assert re.search(r"subnet_ids\s*=\s*\[[^\]]*aws_subnet\.[^.]+\.id[^\]]*aws_subnet\.[^.]+\.id[^\]]*\]", worker_body, re.DOTALL)
    assert re.search(r"security_group_ids\s*=\s*\[\s*aws_security_group\.[^.]+\.id\s*\]", worker_body)
    assert any('"sqs:ReceiveMessage"' in body for body in worker_policies)
    assert any('"sqs:DeleteMessage"' in body for body in worker_policies)
    assert any('"sqs:GetQueueAttributes"' in body for body in worker_policies)
    assert any('"secretsmanager:GetSecretValue"' in body for body in worker_policies)
    assert any(re.search(rf"aws_sqs_queue\.[^.]+\.arn", body) for body in worker_policies)
    assert any(re.search(rf"aws_secretsmanager_secret\.[^.]+\.arn", body) for body in worker_policies)
    assert all("rds-db:connect" not in body for body in worker_policies)

    health_name = extract_ref(
        permission,
        r"function_name\s*=\s*aws_lambda_function\.([^.]+)\.(function_name|arn)",
        "health lambda permission target",
    )
    health_body = resource_body("aws_lambda_function", health_name)
    health_role_name = extract_ref(
        health_body,
        r"role\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "health IAM role",
    )
    health_policies = find_role_policies_for(health_role_name)
    enrichment_body = resource_body("aws_lambda_function", "enrichment")
    enrichment_role_name = extract_ref(
        enrichment_body,
        r"role\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "enrichment IAM role",
    )
    enrichment_role = resource_body("aws_iam_role", enrichment_role_name)

    assert health_name != worker_name, "Worker and health Lambdas must be different functions"
    assert "vpc_config" not in health_body, "Health Lambda must not be placed in the VPC"
    assert health_role_name != worker_role_name, "Worker and health Lambdas must use separate roles"
    assert all("sqs:" not in body for body in health_policies)
    assert all("secretsmanager:" not in body for body in health_policies)
    assert all("rds" not in body.lower() for body in health_policies)

    attachments = resource_blocks("aws_iam_role_policy_attachment")
    attachment_bodies = [body for _, body in attachments]
    assert any(
        worker_role_name in body and "AWSLambdaBasicExecutionRole" in body
        for body in attachment_bodies
    )
    assert any(
        health_role_name in body and "AWSLambdaBasicExecutionRole" in body
        for body in attachment_bodies
    )
    assert any(
        enrichment_role_name in body and "AWSLambdaBasicExecutionRole" in body
        for body in attachment_bodies
    )

    worker_role = resource_body("aws_iam_role", worker_role_name)
    health_role = resource_body("aws_iam_role", health_role_name)
    for role_body in [worker_role, health_role, enrichment_role]:
        assert_only_service_principal(role_body, "lambda.amazonaws.com")

    assert re.search(r"memory_size\s*=\s*256", enrichment_body)
    assert re.search(r"timeout\s*=\s*15", enrichment_body)

    assert '"apigateway.amazonaws.com"' in permission
    assert "GET/health" in permission


def test_lambda_stub_logic_matches_the_prompt():
    tf_text = read_text(MAIN_TF)
    worker_source = extract_ref(
        tf_text,
        r'worker_source\s*=\s*<<-PY\n(.*?)\n\s*PY',
        "worker source",
    )
    health_source = extract_ref(
        tf_text,
        r'health_source\s*=\s*<<-PY\n(.*?)\n\s*PY',
        "health source",
    )
    enrichment_source = extract_ref(
        tf_text,
        r'enrichment_source\s*=\s*<<-PY\n(.*?)\n\s*PY',
        "enrichment source",
    )

    assert 'boto3.client("secretsmanager")' in worker_source
    assert 'os.environ["SECRET_ARN"]' in worker_source
    assert worker_source.count("print(") == 1
    assert '"function": "worker"' in worker_source
    assert health_source.count("print(") == 1
    assert '"function": "health"' in health_source
    assert '"statusCode": 200' in health_source
    assert '"body": json.dumps({"status": "ok"})' in health_source
    assert enrichment_source.count("print(") == 1
    assert '"function": "enrichment"' in enrichment_source
    assert "return event" in enrichment_source


def test_step_functions_and_pipe_permissions_are_scoped():
    _, state_machine = single_resource_block("aws_sfn_state_machine")
    _, pipe = single_resource_block("aws_pipes_pipe")
    _, event_source_mapping = single_resource_block("aws_lambda_event_source_mapping")
    _, permission = single_resource_block("aws_lambda_permission")

    assert 'type     = "STANDARD"' in state_machine or 'type = "STANDARD"' in state_machine
    assert len(re.findall(r'Type\s*=\s*"Pass"', state_machine)) == 1
    assert len(re.findall(r'Type\s*=\s*"Succeed"', state_machine)) == 1

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
    enrichment_name = extract_ref(
        pipe,
        r"enrichment\s*=\s*aws_lambda_function\.([^.]+)\.arn",
        "enrichment lambda reference",
    )
    assert len({worker_name, health_name, enrichment_name}) == 3
    state_machine_name = extract_ref(
        pipe,
        r"target\s*=\s*aws_sfn_state_machine\.([^.]+)\.arn",
        "Step Functions target reference",
    )
    pipe_role_name = extract_ref(
        pipe,
        r"role_arn\s*=\s*aws_iam_role\.([^.]+)\.arn",
        "pipe IAM role",
    )
    pipe_role = resource_body("aws_iam_role", pipe_role_name)
    pipe_policies = find_role_policies_for(pipe_role_name)

    assert_only_service_principal(pipe_role, "pipes.amazonaws.com")
    assert re.search(r"source\s*=\s*aws_sqs_queue\.[^.]+\.arn", pipe)
    assert pipe_policies, "Expected a scoped inline policy for the pipe role"
    joined = "\n".join(pipe_policies)
    assert '"sqs:ReceiveMessage"' in joined
    assert '"sqs:DeleteMessage"' in joined
    assert '"sqs:GetQueueAttributes"' in joined
    assert '"lambda:InvokeFunction"' in joined
    assert '"states:StartExecution"' in joined
    assert re.search(r"aws_sqs_queue\.[^.]+\.arn", joined)
    assert f"aws_lambda_function.{enrichment_name}.arn" in joined
    assert f"aws_sfn_state_machine.{state_machine_name}.arn" in joined


def test_outputs_and_security_guardrails_are_present():
    tf_text = read_text(MAIN_TF)
    hardcoded_arns = set(re.findall(r'arn:aws:[^"\s]+', tf_text))
    outputs = output_blocks()

    assert "http_api_endpoint_url" in outputs
    assert "sqs_queue_url" in outputs
    assert "rds_endpoint_address" in outputs
    assert "secrets_manager_secret_arn" in outputs
    assert re.search(r"(aws_apigatewayv2_(api|stage)\.[^.]+(?:\[\*\])?\.(api_endpoint|invoke_url)|local\.http_api_endpoint_url)", outputs["http_api_endpoint_url"])
    assert re.search(r"aws_sqs_queue\.[^.]+\.url", outputs["sqs_queue_url"])
    assert re.search(r"(aws_db_instance\.[^.]+(?:\[\*\])?\.address|local\.rds_endpoint_address)", outputs["rds_endpoint_address"])
    assert re.search(r"aws_secretsmanager_secret\.[^.]+\.arn", outputs["secrets_manager_secret_arn"])

    assert 'Action = "*"' not in tf_text
    assert '"Action":"*"' not in tf_text
    if 'Resource = "*"' in tf_text:
        assert "resource-level scoping" in tf_text.lower()
    assert "prevent_destroy" not in tf_text
    assert "deletion_protection = true" not in tf_text
    assert not re.search(r"\b\d{12}\b", tf_text)
    assert hardcoded_arns <= {"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"}
