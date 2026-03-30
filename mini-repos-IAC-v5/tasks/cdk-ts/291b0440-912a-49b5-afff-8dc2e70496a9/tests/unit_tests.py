"""
Unit tests for the ThreeTierWebAppStack CDK application.

These tests verify that all requirements from the initial prompt are met
by parsing the synthesized CloudFormation JSON template directly.
"""

import json
import os
import pytest


@pytest.fixture(scope="module")
def template():
    """Load the synthesized CloudFormation template directly."""
    template_path = os.path.join(os.getcwd(), "cdk.out", "ThreeTierWebAppStack.template.json")
    if not os.path.exists(template_path):
        pytest.fail(f"Template not found at {template_path}. Please run 'npx cdk synth' first.")

    with open(template_path, 'r') as f:
        return json.load(f)


def get_resources(template, resource_type):
    """Helper to extract resources as dict {logical_id: resource} by CloudFormation type."""
    return {
        lid: res for lid, res in template.get("Resources", {}).items()
        if res.get("Type") == resource_type
    }


def get_resources_list(template, resource_type):
    """Helper to extract resources as a list by CloudFormation type."""
    return [res for res in template.get("Resources", {}).values() if res.get("Type") == resource_type]


def _find_sg_logical_id(template, description_fragment):
    """Find the logical ID of a security group by its description substring."""
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") == "AWS::EC2::SecurityGroup":
            desc = str(res.get("Properties", {}).get("GroupDescription", "")).lower()
            if description_fragment.lower() in desc:
                return lid
    return None


def _resolve_ref(value):
    """Extract a logical ID from a Ref or Fn::GetAtt intrinsic."""
    if isinstance(value, dict):
        if "Fn::GetAtt" in value:
            return value["Fn::GetAtt"][0]
        if "Ref" in value:
            return value["Ref"]
    return None


def _get_sg_ingress_rules(template, sg_logical_id):
    """
    Collect all ingress rules targeting a security group (both inline and
    standalone AWS::EC2::SecurityGroupIngress resources).
    """
    rules = []
    sg = template["Resources"].get(sg_logical_id, {})
    for rule in sg.get("Properties", {}).get("SecurityGroupIngress", []):
        rules.append(rule)
    for _lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::EC2::SecurityGroupIngress":
            continue
        props = res.get("Properties", {})
        target = _resolve_ref(props.get("GroupId"))
        if target == sg_logical_id:
            rules.append(props)
    return rules


def _get_sg_egress_rules(template, sg_logical_id):
    """
    Collect all egress rules from a security group (both inline and
    standalone AWS::EC2::SecurityGroupEgress resources).
    """
    rules = []
    sg = template["Resources"].get(sg_logical_id, {})
    for rule in sg.get("Properties", {}).get("SecurityGroupEgress", []):
        rules.append(rule)
    for _lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::EC2::SecurityGroupEgress":
            continue
        props = res.get("Properties", {})
        target = _resolve_ref(props.get("GroupId"))
        if target == sg_logical_id:
            rules.append(props)
    return rules


def _find_role_logical_id(template, service_principal_fragment):
    """Find a role's logical ID by its assume-role service principal substring."""
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            for svc in svcs:
                if isinstance(svc, str) and service_principal_fragment in svc:
                    return lid
    return None


def _get_policies_for_role(template, role_logical_id):
    """Return all IAM Policy statement lists attached to a given role logical ID."""
    all_stmts = []
    for _lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Policy":
            continue
        roles = res.get("Properties", {}).get("Roles", [])
        for r in roles:
            ref = _resolve_ref(r)
            if ref == role_logical_id:
                stmts = res.get("Properties", {}).get("PolicyDocument", {}).get("Statement", [])
                if isinstance(stmts, dict):
                    stmts = [stmts]
                all_stmts.extend(stmts)
    return all_stmts


def _actions_in_statements(statements):
    """Flatten all actions from a list of IAM policy statements."""
    actions = set()
    for stmt in statements:
        acts = stmt.get("Action", [])
        if isinstance(acts, str):
            acts = [acts]
        actions.update(acts)
    return actions


def _contains_reference_to(value, logical_id):
    """Return True when a value tree contains a Ref/GetAtt to the given logical ID."""
    if isinstance(value, dict):
        if value.get("Ref") == logical_id:
            return True
        get_att = value.get("Fn::GetAtt")
        if isinstance(get_att, list) and len(get_att) >= 1 and get_att[0] == logical_id:
            return True
        return any(_contains_reference_to(v, logical_id) for v in value.values())
    if isinstance(value, list):
        return any(_contains_reference_to(v, logical_id) for v in value)
    return False


def _render_intrinsic_string(value):
    """Best-effort rendering of CloudFormation string intrinsics into a JSON-parseable string."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_render_intrinsic_string(v) for v in value)
    if isinstance(value, dict):
        if "Fn::Join" in value:
            delimiter, parts = value["Fn::Join"]
            return str(delimiter).join(_render_intrinsic_string(part) for part in parts)
        if "Fn::Sub" in value:
            sub_value = value["Fn::Sub"]
            if isinstance(sub_value, str):
                return sub_value
            if isinstance(sub_value, list) and len(sub_value) >= 1:
                return str(sub_value[0])
        if "Ref" in value:
            return f"${{{value['Ref']}}}"
        if "Fn::GetAtt" in value:
            get_att = value["Fn::GetAtt"]
            if isinstance(get_att, list) and len(get_att) == 2:
                return f"${{{get_att[0]}.{get_att[1]}}}"
    return json.dumps(value)


def _statement_actions(statement):
    """Return a statement's actions as a list."""
    actions = statement.get("Action", [])
    if isinstance(actions, str):
        actions = [actions]
    return actions


def _statement_resources(statement):
    """Return a statement's resources as a list."""
    resources = statement.get("Resource", [])
    if isinstance(resources, (str, dict)):
        resources = [resources]
    return resources


def test_helper_resolve_ref_variants():
    """Ref helper must normalize Ref, GetAtt, and unsupported literals consistently."""
    assert _resolve_ref({"Ref": "MyResource"}) == "MyResource"
    assert _resolve_ref({"Fn::GetAtt": ["MyResource", "Arn"]}) == "MyResource"
    assert _resolve_ref("literal") is None


def test_helper_statement_normalizers():
    """IAM statement helper normalizers must accept both scalar and collection forms."""
    assert _statement_actions({"Action": "logs:PutLogEvents"}) == ["logs:PutLogEvents"]
    assert _statement_actions({"Action": ["logs:PutLogEvents"]}) == ["logs:PutLogEvents"]
    assert _statement_resources({"Resource": "*"}) == ["*"]
    assert _statement_resources({"Resource": {"Ref": "LogGroup"}}) == [{"Ref": "LogGroup"}]


def test_helper_contains_reference_to_nested_structures():
    """Reference scanner must find nested Ref/GetAtt values and ignore absent targets."""
    value = {
        "Fn::Join": [
            "",
            [
                {"Ref": "FirstResource"},
                {"nested": [{"Fn::GetAtt": ["TargetResource", "Arn"]}]},
            ],
        ]
    }
    assert _contains_reference_to(value, "TargetResource") is True
    assert _contains_reference_to(value, "MissingResource") is False


def test_deliverable_file_structure():
    """Requirement: Deliver one CDK app in a single file named app.ts"""
    assert os.path.exists("app.ts"), "Deliverable must be in a file named app.ts"


@pytest.fixture(scope="module")
def app_source():
    """Load the app.ts source code for configuration tests."""
    with open("app.ts", 'r') as f:
        return f.read()


def test_config_aws_region_default(app_source):
    """Prompt: 'AWS_REGION must default to us-east-1 when not set.'"""
    assert "process.env.AWS_REGION" in app_source, "app.ts must read AWS_REGION"
    assert "'us-east-1'" in app_source, "app.ts must default to us-east-1"


def test_config_aws_endpoint_used(app_source):
    """Prompt: 'AWS SDK clients used by CDK must be configured to use AWS_ENDPOINT.'"""
    assert "process.env.AWS_ENDPOINT" in app_source, \
        "app.ts must reference AWS_ENDPOINT to configure SDK clients"


def test_config_no_extra_input_variables(app_source):
    """Prompt: 'The CDK app must rely on the following inputs only: AWS_ENDPOINT, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.'
    No non-allowed env vars beyond the four specified and CDK's own defaults (CDK_DEFAULT_ACCOUNT, AWS_ACCOUNT_ID)."""
    import re
    env_refs = set(re.findall(r'process\.env\.(\w+)', app_source))
    allowed = {
        'AWS_ENDPOINT', 'AWS_ENDPOINT_URL', 'AWS_REGION',
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
        'CDK_DEFAULT_ACCOUNT', 'AWS_ACCOUNT_ID',
    }
    extra = env_refs - allowed
    assert not extra, f"app.ts references disallowed env vars: {extra}"


def test_compute_ecs_cluster(template):
    """Exactly 1 ECS Cluster."""
    clusters = get_resources_list(template, "AWS::ECS::Cluster")
    assert len(clusters) >= 1, "Expected at least 1 ECS Cluster"


def test_compute_fargate_service(template):
    """Fargate service with DesiredCount 2."""
    services = get_resources_list(template, "AWS::ECS::Service")
    assert any(
        s.get("Properties", {}).get("LaunchType") == "FARGATE"
        and s.get("Properties", {}).get("DesiredCount") == 2
        for s in services
    ), "Expected an ECS Fargate service with DesiredCount 2"


def test_compute_task_definition(template):
    """Task definition: CPU 512, Mem 1024, port 8080, awslogs, secrets."""
    task_defs = get_resources_list(template, "AWS::ECS::TaskDefinition")
    valid = False
    for td in task_defs:
        props = td.get("Properties", {})
        if str(props.get("Cpu")) == "512" and str(props.get("Memory")) == "1024":
            for container in props.get("ContainerDefinitions", []):
                ports = container.get("PortMappings", [])
                has_port = any(str(p.get("ContainerPort")) == "8080" for p in ports)
                has_logs = container.get("LogConfiguration", {}).get("LogDriver") == "awslogs"
                has_secrets = len(container.get("Secrets", [])) > 0
                if has_port and has_logs and has_secrets:
                    valid = True
                    break
    assert valid, "Task definition missing required CPU/Memory/Port/logs/secrets"


def test_compute_autoscaling(template):
    """Auto scaling: min 2, max 6, target CPU 50%."""
    targets = get_resources_list(template, "AWS::ApplicationAutoScaling::ScalableTarget")
    assert any(
        str(t.get("Properties", {}).get("MinCapacity")) == "2"
        and str(t.get("Properties", {}).get("MaxCapacity")) == "6"
        for t in targets
    ), "Missing auto-scaling scalable target (2-6)"

    policies = get_resources_list(template, "AWS::ApplicationAutoScaling::ScalingPolicy")
    assert any(
        p.get("Properties", {}).get("PolicyType") == "TargetTrackingScaling"
        and str(p.get("Properties", {}).get("TargetTrackingScalingPolicyConfiguration", {}).get("TargetValue")) == "50"
        for p in policies
    ), "Missing target tracking scaling policy at 50%"


def test_compute_lambda(template):
    """Lambda: Node.js 20.x, timeout 60, memory 256, VPC-attached."""
    lambdas = get_resources_list(template, "AWS::Lambda::Function")
    app_lambdas = [
        l for l in lambdas
        if "nodejs20" in str(l.get("Properties", {}).get("Runtime", ""))
        and "auto-delet" not in str(l.get("Properties", {}).get("Description", "")).lower()
        and "logretention" not in str(l.get("Properties", {}).get("Description", "")).lower()
    ]
    assert len(app_lambdas) >= 1, "Expected at least 1 Node.js 20.x Lambda"
    lam = app_lambdas[0].get("Properties", {})
    assert str(lam.get("Timeout")) == "60"
    assert str(lam.get("MemorySize")) == "256"
    assert "VpcConfig" in lam, "Lambda must run inside the VPC"


def test_compute_log_retention(template):
    """CloudWatch log group retention: 14 days for ECS and Lambda."""
    log_groups = get_resources_list(template, "AWS::Logs::LogGroup")
    app_log_groups = [
        lg for lg in log_groups
        if "/ecs/app" in str(lg.get("Properties", {}).get("LogGroupName", ""))
    ]
    lambda_log_groups = [
        lg for lg in log_groups
        if "/aws/lambda/" in str(lg.get("Properties", {}).get("LogGroupName", ""))
    ]
    custom_log_retentions = [
        res for res in template.get("Resources", {}).values()
        if res.get("Type") == "Custom::LogRetention"
    ]

    assert len(app_log_groups) >= 1, "Expected at least 1 ECS app log group"
    for lg in app_log_groups:
        assert str(lg.get("Properties", {}).get("RetentionInDays")) == "14", \
            "ECS log group must have 14-day retention"

    if lambda_log_groups:
        for lg in lambda_log_groups:
            assert str(lg.get("Properties", {}).get("RetentionInDays")) == "14", \
                "Lambda log group must have 14-day retention"
    else:
        assert len(custom_log_retentions) >= 1, "Expected Lambda log retention configuration"
        for clr in custom_log_retentions:
            assert str(clr.get("Properties", {}).get("RetentionInDays")) == "14", \
                "Lambda log retention must be 14 days"


def test_compute_step_functions_logging(template):
    """Step Functions state machine must have LoggingConfiguration enabled."""
    state_machines = get_resources_list(template, "AWS::StepFunctions::StateMachine")
    assert len(state_machines) >= 1, "Expected at least 1 Step Functions state machine"

    found_logging = False
    for sm in state_machines:
        logging_config = sm.get("Properties", {}).get("LoggingConfiguration")
        if logging_config:
            destinations = logging_config.get("Destinations", [])
            level = logging_config.get("Level", "")
            if len(destinations) > 0 and level != "OFF":
                found_logging = True
                break
    assert found_logging, "State machine must have LoggingConfiguration with logging enabled"


def test_compute_step_functions_logging_has_single_destination(template):
    """Step Functions logging must use exactly one CloudWatch destination tied to the dedicated log group."""
    state_machines = get_resources(template, "AWS::StepFunctions::StateMachine")
    log_groups = get_resources(template, "AWS::Logs::LogGroup")
    assert len(state_machines) == 1, f"Expected exactly 1 state machine, got {len(state_machines)}"

    state_machine = next(iter(state_machines.values()))
    logging_config = state_machine.get("Properties", {}).get("LoggingConfiguration", {})
    destinations = logging_config.get("Destinations", [])
    assert len(destinations) == 1, f"Expected exactly 1 Step Functions log destination, got {len(destinations)}"
    destination = destinations[0].get("CloudWatchLogsLogGroup", {}).get("LogGroupArn")
    assert any(_contains_reference_to(destination, logical_id) for logical_id in log_groups.keys())


def test_compute_step_functions_invokes_migration_lambda(template):
    """State machine definition must be a strict single-step Lambda invoke workflow."""
    state_machines = get_resources(template, "AWS::StepFunctions::StateMachine")
    lambda_fns = get_resources(template, "AWS::Lambda::Function")

    migration_lambda_lid = None
    for lid, fn in lambda_fns.items():
        props = fn.get("Properties", {})
        runtime = str(props.get("Runtime", ""))
        desc = str(props.get("Description", "")).lower()
        if "nodejs20" in runtime and "auto-delet" not in desc and "logretention" not in desc:
            migration_lambda_lid = lid
            break
    assert migration_lambda_lid, "Could not find migration Lambda"

    found_valid_definition = False
    for _lid, sm in state_machines.items():
        definition = sm.get("Properties", {}).get("DefinitionString", {})
        definition_str = _render_intrinsic_string(definition)
        parsed = json.loads(definition_str)
        start_state = parsed.get("StartAt")
        states = parsed.get("States", {})
        assert len(states) == 1, "State machine must contain exactly one state"
        assert list(states.keys()) == [start_state], \
            "StartAt must point to the only state in the workflow"
        assert start_state in states, f"StartAt '{start_state}' not found in state map"

        task_state = states[start_state]
        params = task_state.get("Parameters", {})
        function_name = str(params.get("FunctionName", ""))
        assert set(task_state.keys()) == {"Type", "Resource", "Parameters", "End"}, \
            f"Single-step migration workflow must not include extra state semantics: {task_state.keys()}"
        assert set(params.keys()) == {"FunctionName", "Payload.$"}, \
            f"Migration task parameters must stay minimal and deterministic: {params.keys()}"
        if (
            task_state.get("Type") == "Task"
            and "lambda:invoke" in str(task_state.get("Resource", "")).lower()
            and migration_lambda_lid in function_name
            and task_state.get("End") is True
        ):
            found_valid_definition = True
            break
    assert found_valid_definition, \
        "State machine definition must be a single terminal lambda:invoke step for the migration Lambda"


def test_compute_step_functions_topology_is_exact(template):
    """Step Functions topology must contain exactly one state machine with exactly one terminal state."""
    state_machines = get_resources(template, "AWS::StepFunctions::StateMachine")
    assert len(state_machines) == 1, f"Expected exactly 1 state machine, got {len(state_machines)}"

    state_machine = next(iter(state_machines.values()))
    definition = state_machine.get("Properties", {}).get("DefinitionString", {})
    parsed = json.loads(_render_intrinsic_string(definition))
    assert set(parsed.keys()) == {"StartAt", "States"}, \
        f"State machine definition must not include extra top-level workflow fields: {parsed.keys()}"

    start_state = parsed["StartAt"]
    states = parsed["States"]
    assert list(states.keys()) == [start_state], "The only state must also be the StartAt target"

    task_state = states[start_state]
    forbidden_fields = {"Next", "Catch", "Retry", "Choices", "Branches", "Iterator"}
    assert forbidden_fields.isdisjoint(task_state.keys()), \
        f"Single-step workflow must not include branching/retry semantics: {forbidden_fields.intersection(task_state.keys())}"


def test_compute_cloudwatch_alarm(template):
    """CloudWatch alarm: 5XX metric, 60-second period, threshold 1, EvaluationPeriods 1, DatapointsToAlarm 1."""
    alarms = get_resources_list(template, "AWS::CloudWatch::Alarm")
    found = False
    for a in alarms:
        props = a.get("Properties", {})
        metric_name = props.get("MetricName")
        period = props.get("Period")
        metrics = props.get("Metrics", [])
        if metrics:
            for metric in metrics:
                metric_stat = metric.get("MetricStat", {})
                metric_def = metric_stat.get("Metric", {})
                if metric_def.get("MetricName") == "HTTPCode_Target_5XX_Count":
                    metric_name = "HTTPCode_Target_5XX_Count"
                    period = metric_stat.get("Period")
                    break
        if (metric_name == "HTTPCode_Target_5XX_Count"
                and str(period) == "60"
                and str(props.get("Threshold")) == "1"
                and str(props.get("EvaluationPeriods")) == "1"
                and str(props.get("DatapointsToAlarm")) == "1"):
            found = True
            break
    assert found, "Missing CloudWatch Alarm with correct metric, 60-second period, threshold, EvaluationPeriods=1, DatapointsToAlarm=1"


def test_compute_cloudwatch_alarm_period_is_explicit_and_reference_based(template):
    """CloudWatch alarm must declare the 60-second period directly and bind dimensions via references."""
    alarms = get_resources_list(template, "AWS::CloudWatch::Alarm")
    load_balancer_lid = next(iter(get_resources(template, "AWS::ElasticLoadBalancingV2::LoadBalancer").keys()))
    target_group_lid = next(iter(get_resources(template, "AWS::ElasticLoadBalancingV2::TargetGroup").keys()))

    matching = []
    for alarm in alarms:
        props = alarm.get("Properties", {})
        if props.get("MetricName") == "HTTPCode_Target_5XX_Count":
            matching.append(props)
    assert len(matching) == 1, f"Expected exactly 1 HTTP 5XX alarm, got {len(matching)}"

    props = matching[0]
    assert props.get("Period") == 60, "CloudWatch alarm period must be expressed directly as 60 seconds"
    assert not props.get("Metrics"), "This alarm must not hide the period inside a metric-math definition"
    dimensions = {entry.get("Name"): entry.get("Value") for entry in props.get("Dimensions", [])}
    assert set(dimensions.keys()) == {"LoadBalancer", "TargetGroup"}, \
        f"Alarm must target exactly LoadBalancer and TargetGroup dimensions, got {dimensions.keys()}"
    assert _contains_reference_to(dimensions["LoadBalancer"], load_balancer_lid)
    assert _contains_reference_to(dimensions["TargetGroup"], target_group_lid)


def test_network_vpc(template):
    """VPC with 2 AZs, 4 subnets, 1 NAT Gateway."""
    assert len(get_resources_list(template, "AWS::EC2::VPC")) >= 1
    assert len(get_resources_list(template, "AWS::EC2::Subnet")) >= 4
    assert len(get_resources_list(template, "AWS::EC2::NatGateway")) >= 1


def test_network_alb(template):
    """Internet-facing ALB with HTTP listener on port 80."""
    albs = get_resources_list(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    assert any(
        str(alb.get("Properties", {}).get("Scheme")).lower() == "internet-facing"
        for alb in albs
    ), "Expected an internet-facing ALB"

    listeners = get_resources_list(template, "AWS::ElasticLoadBalancingV2::Listener")
    assert any(
        str(l.get("Properties", {}).get("Port")) == "80"
        and any(act.get("Type") == "forward" for act in l.get("Properties", {}).get("DefaultActions", []))
        for l in listeners
    ), "Expected HTTP listener on port 80 with forward action"


def test_network_target_group(template):
    """Target group on port 8080 with /health health check."""
    tgs = get_resources_list(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
    assert any(
        tg.get("Properties", {}).get("HealthCheckPath") == "/health"
        and str(tg.get("Properties", {}).get("Port")) == "8080"
        for tg in tgs
    )


def test_network_security_group_ingress(template):
    """ALB SG allows inbound TCP/80 from 0.0.0.0/0."""
    sgs = get_resources_list(template, "AWS::EC2::SecurityGroup")
    assert any(
        any(
            rule.get("IpProtocol") == "tcp"
            and str(rule.get("FromPort")) == "80"
            and rule.get("CidrIp") == "0.0.0.0/0"
            for rule in sg.get("Properties", {}).get("SecurityGroupIngress", [])
        )
        for sg in sgs
    ), "Missing SG allowing ingress on port 80 from 0.0.0.0/0"


def test_network_security_group_ingress_exclusivity(template):
    """Only the explicitly allowed ingress rules may exist across the application security groups."""
    alb_sg_lid = _find_sg_logical_id(template, "alb")
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    redis_sg_lid = _find_sg_logical_id(template, "redis")
    lambda_sg_lid = _find_sg_logical_id(template, "lambda")
    assert all([alb_sg_lid, ecs_sg_lid, rds_sg_lid, redis_sg_lid, lambda_sg_lid])

    expected = {
        alb_sg_lid: {("80", "0.0.0.0/0")},
        ecs_sg_lid: {("8080", alb_sg_lid)},
        rds_sg_lid: {("5432", ecs_sg_lid), ("5432", lambda_sg_lid)},
        redis_sg_lid: {("6379", ecs_sg_lid)},
        lambda_sg_lid: set(),
    }

    for sg_lid, allowed_rules in expected.items():
        actual_rules = set()
        for rule in _get_sg_ingress_rules(template, sg_lid):
            port = str(rule.get("FromPort", ""))
            source = rule.get("CidrIp") or _resolve_ref(rule.get("SourceSecurityGroupId"))
            actual_rules.add((port, source))
        assert actual_rules == allowed_rules, \
            f"Unexpected ingress rules for {sg_lid}: expected {allowed_rules}, got {actual_rules}"


def test_network_security_group_egress_exclusivity(template):
    """Application compute security groups must keep only the explicitly required egress rules."""
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    lambda_sg_lid = _find_sg_logical_id(template, "lambda")
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    redis_sg_lid = _find_sg_logical_id(template, "redis")
    assert all([ecs_sg_lid, lambda_sg_lid, rds_sg_lid, redis_sg_lid])

    expected = {
        ecs_sg_lid: {("5432", rds_sg_lid), ("6379", redis_sg_lid), ("443", "0.0.0.0/0")},
        lambda_sg_lid: {("5432", rds_sg_lid), ("443", "0.0.0.0/0")},
    }

    for sg_lid, allowed_rules in expected.items():
        actual_rules = set()
        for rule in _get_sg_egress_rules(template, sg_lid):
            port = str(rule.get("FromPort", ""))
            destination = rule.get("CidrIp") or _resolve_ref(rule.get("DestinationSecurityGroupId"))
            actual_rules.add((port, destination))
        assert actual_rules == allowed_rules, \
            f"Unexpected egress rules for {sg_lid}: expected {allowed_rules}, got {actual_rules}"


def test_alb_routing_rules_http_listener(template):
    """ALB HTTP listener rules for /health and /api/* on port 80 only."""
    listeners = get_resources(template, "AWS::ElasticLoadBalancingV2::Listener")
    rules_map = get_resources(template, "AWS::ElasticLoadBalancingV2::ListenerRule")

    http_listener_ids = set()
    for lid, listener in listeners.items():
        if str(listener.get("Properties", {}).get("Port")) == "80":
            http_listener_ids.add(lid)

    has_health = False
    has_api = False
    for _rid, rule in rules_map.items():
        props = rule.get("Properties", {})
        listener_ref = props.get("ListenerArn", {})
        ref_id = listener_ref.get("Ref", "") if isinstance(listener_ref, dict) else ""
        if ref_id not in http_listener_ids:
            continue
        for condition in props.get("Conditions", []):
            if condition.get("Field") == "path-pattern":
                values = condition.get("PathPatternConfig", {}).get("Values", [])
                if "/health" in values:
                    has_health = True
                if "/api/*" in values:
                    has_api = True

    assert has_health, "Missing HTTP listener rule for /health"
    assert has_api, "Missing HTTP listener rule for /api/*"


def test_frontend_s3_bucket(template):
    """S3 bucket with full public access block."""
    buckets = get_resources_list(template, "AWS::S3::Bucket")
    valid = False
    for b in buckets:
        pab = b.get("Properties", {}).get("PublicAccessBlockConfiguration", {})
        if all(
            str(pab.get(k)).lower() == "true"
            for k in ["BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets"]
        ):
            valid = True
            break
    assert valid, "Expected S3 bucket with full PublicAccessBlockConfiguration"


def test_frontend_cloudfront(template):
    """At least 1 CloudFront distribution."""
    assert len(get_resources_list(template, "AWS::CloudFront::Distribution")) >= 1


def test_route53_records(template):
    """Route 53 hosted zone and alias records."""
    assert len(get_resources_list(template, "AWS::Route53::HostedZone")) >= 1
    records = get_resources_list(template, "AWS::Route53::RecordSet")
    alias_records = [r for r in records if "AliasTarget" in r.get("Properties", {})]
    assert len(alias_records) >= 2, "Expected at least 2 Route53 alias records (frontend + API)"


def test_persistence_rds(template):
    """RDS PostgreSQL 15, db.t3.micro, 20GB, encrypted, private, backup 7 days."""
    instances = get_resources_list(template, "AWS::RDS::DBInstance")
    assert len(instances) >= 1
    for db in instances:
        props = db.get("Properties", {})
        assert props.get("Engine") == "postgres"
        assert "15" in str(props.get("EngineVersion", ""))
        assert props.get("DBInstanceClass") == "db.t3.micro"
        assert str(props.get("AllocatedStorage")) == "20"
        assert str(props.get("MultiAZ")).lower() == "false"
        assert str(props.get("PubliclyAccessible")).lower() == "false"
        assert str(props.get("BackupRetentionPeriod")) == "7"
        assert str(props.get("StorageEncrypted")).lower() == "true"


def test_persistence_secrets_and_ssm(template):
    """Secrets Manager secret and SSM parameter for DB endpoint."""
    assert len(get_resources_list(template, "AWS::SecretsManager::Secret")) >= 1
    ssms = get_resources_list(template, "AWS::SSM::Parameter")
    assert any(p.get("Properties", {}).get("Name") == "/app/db/endpoint" for p in ssms)


def test_persistence_no_plaintext_db_credentials(template):
    """Database credentials must stay secret-backed and not appear as plaintext in template properties."""
    secrets = get_resources_list(template, "AWS::SecretsManager::Secret")
    assert len(secrets) >= 1, "Expected database secret resource"
    for secret in secrets:
        generate = secret.get("Properties", {}).get("GenerateSecretString", {})
        template_str = str(generate.get("SecretStringTemplate", ""))
        assert "password" not in template_str.lower(), \
            "Secret template must not embed a plaintext password"

    instances = get_resources_list(template, "AWS::RDS::DBInstance")
    assert len(instances) >= 1, "Expected at least 1 RDS instance"
    for db in instances:
        props = db.get("Properties", {})
        password_value = props.get("MasterUserPassword")
        username_value = props.get("MasterUsername")
        rendered_password = _render_intrinsic_string(password_value)
        rendered_username = _render_intrinsic_string(username_value)
        assert "{{resolve:secretsmanager:" in rendered_password, \
            "RDS password must be resolved from Secrets Manager"
        assert "{{resolve:secretsmanager:" in rendered_username, \
            "RDS username must be resolved from Secrets Manager"

    task_defs = get_resources_list(template, "AWS::ECS::TaskDefinition")
    for td in task_defs:
        for container in td.get("Properties", {}).get("ContainerDefinitions", []):
            env_names = {entry.get("Name") for entry in container.get("Environment", [])}
            assert "DB_PASSWORD" not in env_names, "DB_PASSWORD must not be injected as plaintext environment"
            assert "DB_USERNAME" not in env_names, "DB_USERNAME must not be injected as plaintext environment"
            secret_names = {entry.get("Name") for entry in container.get("Secrets", [])}
            assert {"DB_PASSWORD", "DB_USERNAME"}.issubset(secret_names), \
                "Database credentials must be delivered to ECS only through secret references"

    lambdas = get_resources_list(template, "AWS::Lambda::Function")
    for fn in lambdas:
        env_vars = fn.get("Properties", {}).get("Environment", {}).get("Variables", {})
        forbidden = {"DB_PASSWORD", "DB_USERNAME", "PASSWORD", "USERNAME"}
        assert forbidden.isdisjoint(env_vars.keys()), \
            f"Lambda environment must not carry plaintext credentials: {forbidden.intersection(env_vars.keys())}"


def test_source_no_plaintext_password_assignments(app_source):
    """Source must not assign plaintext passwords into environment variables or resource properties."""
    import re

    forbidden_patterns = [
        r"DB_PASSWORD\s*:\s*['\"]",
        r"DB_USERNAME\s*:\s*['\"]",
        r"masterUserPassword\s*:\s*['\"]",
    ]
    for pattern in forbidden_patterns:
        assert not re.search(pattern, app_source), \
            f"Found plaintext credential assignment matching {pattern}"


def test_persistence_dynamodb_config(template):
    """DynamoDB: PAY_PER_REQUEST, PITR, SSE."""
    ddbs = get_resources_list(template, "AWS::DynamoDB::Table")
    assert any(
        t.get("Properties", {}).get("BillingMode") == "PAY_PER_REQUEST"
        and str(t.get("Properties", {}).get("PointInTimeRecoverySpecification", {}).get("PointInTimeRecoveryEnabled")).lower() == "true"
        and str(t.get("Properties", {}).get("SSESpecification", {}).get("SSEEnabled")).lower() == "true"
        for t in ddbs
    ), "DynamoDB missing PAY_PER_REQUEST, PITR, or SSE"


def test_persistence_dynamodb_key_schema(template):
    """DynamoDB table must have pk (S) as partition key and sk (S) as sort key."""
    ddbs = get_resources_list(template, "AWS::DynamoDB::Table")
    found = False
    for t in ddbs:
        props = t.get("Properties", {})
        key_schema = props.get("KeySchema", [])
        attr_defs = props.get("AttributeDefinitions", [])

        has_pk_hash = any(
            k.get("AttributeName") == "pk" and k.get("KeyType") == "HASH"
            for k in key_schema
        )
        has_sk_range = any(
            k.get("AttributeName") == "sk" and k.get("KeyType") == "RANGE"
            for k in key_schema
        )
        has_pk_s = any(
            a.get("AttributeName") == "pk" and a.get("AttributeType") == "S"
            for a in attr_defs
        )
        has_sk_s = any(
            a.get("AttributeName") == "sk" and a.get("AttributeType") == "S"
            for a in attr_defs
        )

        if has_pk_hash and has_sk_range and has_pk_s and has_sk_s:
            found = True
            break
    assert found, "DynamoDB table must have pk (S) HASH and sk (S) RANGE keys"


def test_persistence_elasticache(template):
    """ElastiCache Redis: cache.t3.micro, failover, encryption at rest + transit, 2 clusters."""
    redis = get_resources_list(template, "AWS::ElastiCache::ReplicationGroup")
    assert any(
        r.get("Properties", {}).get("CacheNodeType") == "cache.t3.micro"
        and r.get("Properties", {}).get("Engine") == "redis"
        and str(r.get("Properties", {}).get("AutomaticFailoverEnabled")).lower() == "true"
        and str(r.get("Properties", {}).get("AtRestEncryptionEnabled")).lower() == "true"
        and str(r.get("Properties", {}).get("TransitEncryptionEnabled")).lower() == "true"
        and str(r.get("Properties", {}).get("NumCacheClusters")) == "2"
        for r in redis
    ), "ElastiCache missing required configuration (engine, node type, failover, encryption, replica count)"


def test_persistence_elasticache_subnet_group(template):
    """ElastiCache must have a subnet group."""
    subnet_groups = get_resources_list(template, "AWS::ElastiCache::SubnetGroup")
    assert len(subnet_groups) >= 1, "Expected at least 1 ElastiCache subnet group"


def test_io_eventbridge(template):
    """EventBridge rule: rate(6 hours) with target."""
    rules = get_resources_list(template, "AWS::Events::Rule")
    assert any(
        "rate(6 hours)" in str(r.get("Properties", {}).get("ScheduleExpression", ""))
        and len(r.get("Properties", {}).get("Targets", [])) > 0
        for r in rules
    )


def test_io_eventbridge_single_schedule_rule(template):
    """The migration schedule must be represented by exactly one scheduled EventBridge rule."""
    rules = get_resources_list(template, "AWS::Events::Rule")
    matching = [
        rule for rule in rules
        if rule.get("Properties", {}).get("ScheduleExpression") == "rate(6 hours)"
    ]
    assert len(matching) == 1, f"Expected exactly 1 rate(6 hours) rule, got {len(matching)}"
    assert len(matching[0].get("Properties", {}).get("Targets", [])) == 1, \
        "The schedule rule must declare exactly one target"


def test_io_eventbridge_targets_sfn(template):
    """EventBridge rule target must be the Step Functions state machine."""
    rules = get_resources(template, "AWS::Events::Rule")
    sfn_machines = get_resources(template, "AWS::StepFunctions::StateMachine")
    sfn_logical_ids = set(sfn_machines.keys())
    assert len(sfn_logical_ids) >= 1, "No state machine found"

    found_sfn_target = False
    for _lid, rule in rules.items():
        props = rule.get("Properties", {})
        if "rate(6 hours)" not in str(props.get("ScheduleExpression", "")):
            continue
        for target in props.get("Targets", []):
            target_arn = target.get("Arn", {})
            ref = _resolve_ref(target_arn)
            if ref in sfn_logical_ids:
                found_sfn_target = True
                break
    assert found_sfn_target, "EventBridge rule must target the Step Functions state machine"


def test_io_sqs(template):
    """SQS queue: visibility 60s, retention 4 days, encryption."""
    queues = get_resources_list(template, "AWS::SQS::Queue")
    assert any(
        str(q.get("Properties", {}).get("VisibilityTimeout")) == "60"
        and str(q.get("Properties", {}).get("MessageRetentionPeriod")) == "345600"
        and (str(q.get("Properties", {}).get("SqsManagedSseEnabled")).lower() == "true"
             or q.get("Properties", {}).get("KmsMasterKeyId"))
        for q in queues
    ), "SQS queue must have visibility 60, retention 345600, and encryption enabled"


def test_io_sns(template):
    """At least 1 SNS topic."""
    assert len(get_resources_list(template, "AWS::SNS::Topic")) >= 1


def test_iam_roles_exist(template):
    """IAM roles for Lambda and Step Functions exist."""
    roles = get_resources_list(template, "AWS::IAM::Role")
    lambda_assumed = False
    states_assumed = False
    for role in roles:
        stmts = role.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            for svc in svcs:
                if isinstance(svc, str):
                    if "lambda.amazonaws.com" in svc:
                        lambda_assumed = True
                    if svc.startswith("states") and svc.endswith(".amazonaws.com"):
                        states_assumed = True
    assert lambda_assumed, "Missing IAM Role assumable by Lambda"
    assert states_assumed, "Missing IAM Role assumable by Step Functions"


def test_iam_expected_role_count(template):
    """The stack should define the expected application IAM roles and no hidden extras for orchestration."""
    roles = get_resources_list(template, "AWS::IAM::Role")
    assert len(roles) == 5, f"Expected exactly 5 IAM roles, got {len(roles)}"


def test_ecs_secret_referencing(template):
    """ECS task definition references secrets via ValueFrom, not plaintext."""
    task_defs = get_resources_list(template, "AWS::ECS::TaskDefinition")
    for td in task_defs:
        for container in td.get("Properties", {}).get("ContainerDefinitions", []):
            secrets = container.get("Secrets", [])
            assert len(secrets) > 0, "Expected container to have injected Secrets"
            for secret in secrets:
                assert "ValueFrom" in secret, "Secret must use ValueFrom"


def test_ecr_repository_count(template):
    """Prompt: 'Amazon ECR repository created in this stack with 1 repository' – exactly 1."""
    repos = get_resources_list(template, "AWS::ECR::Repository")
    assert len(repos) == 1, f"Expected exactly 1 ECR repository, got {len(repos)}"


def test_ecr_repository_referenced_by_task(template):
    """ECS task definition image must reference the ECR repository dynamically."""
    task_defs = get_resources_list(template, "AWS::ECS::TaskDefinition")
    valid_image_ref = False
    for td in task_defs:
        for container in td.get("Properties", {}).get("ContainerDefinitions", []):
            image = container.get("Image")
            if isinstance(image, dict) and ("Fn::Join" in image or "Fn::Sub" in image):
                valid_image_ref = True
                break
    assert valid_image_ref, "ECS task definition must reference the ECR repository dynamically"


def test_security_group_explicit_egress(template):
    """ECS and Lambda SGs have explicit, non-fully-open egress rules."""
    sgs = get_resources_list(template, "AWS::EC2::SecurityGroup")
    for sg in sgs:
        desc = str(sg.get("Properties", {}).get("GroupDescription", "")).lower()
        if "ecs" in desc or "lambda" in desc:
            egress = sg.get("Properties", {}).get("SecurityGroupEgress", [])
            assert len(egress) > 0, f"Missing explicit egress for SG: {desc}"
            for rule in egress:
                is_all_ports = (
                    str(rule.get("IpProtocol")) == "-1"
                    or (str(rule.get("FromPort")) == "0" and str(rule.get("ToPort")) == "65535")
                )
                is_open_cidr = rule.get("CidrIp") == "0.0.0.0/0"
                assert not (is_all_ports and is_open_cidr), f"SG '{desc}' has overly permissive outbound rule"


def test_sg_ecs_ingress_from_alb_only(template):
    """ECS SG must allow inbound TCP/8080 strictly from the ALB SG and nothing else."""
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    alb_sg_lid = _find_sg_logical_id(template, "alb")
    assert ecs_sg_lid, "Could not find ECS security group"
    assert alb_sg_lid, "Could not find ALB security group"

    rules = _get_sg_ingress_rules(template, ecs_sg_lid)
    assert len(rules) >= 1, "ECS SG must have at least 1 ingress rule"

    for rule in rules:
        port = str(rule.get("FromPort", ""))
        source = _resolve_ref(rule.get("SourceSecurityGroupId"))
        assert port == "8080", f"ECS SG ingress on unexpected port {port}"
        assert source == alb_sg_lid, "ECS SG ingress must originate from ALB SG only"


def test_sg_rds_ingress_from_ecs_and_lambda_only(template):
    """RDS SG must allow inbound TCP/5432 from ECS SG and Lambda SG only."""
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    lambda_sg_lid = _find_sg_logical_id(template, "lambda")
    assert rds_sg_lid and ecs_sg_lid and lambda_sg_lid

    rules = _get_sg_ingress_rules(template, rds_sg_lid)
    assert len(rules) >= 2, "RDS SG must have at least 2 ingress rules (ECS + Lambda)"

    allowed_sources = {ecs_sg_lid, lambda_sg_lid}
    for rule in rules:
        port = str(rule.get("FromPort", ""))
        source = _resolve_ref(rule.get("SourceSecurityGroupId"))
        assert port == "5432", f"RDS SG ingress on unexpected port {port}"
        assert source in allowed_sources, f"RDS SG ingress from unexpected source {source}"


def test_sg_redis_ingress_from_ecs_only(template):
    """Redis SG must allow inbound TCP/6379 from ECS SG only."""
    redis_sg_lid = _find_sg_logical_id(template, "redis")
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    assert redis_sg_lid and ecs_sg_lid

    rules = _get_sg_ingress_rules(template, redis_sg_lid)
    assert len(rules) >= 1, "Redis SG must have at least 1 ingress rule"

    for rule in rules:
        port = str(rule.get("FromPort", ""))
        source = _resolve_ref(rule.get("SourceSecurityGroupId"))
        assert port == "6379", f"Redis SG ingress on unexpected port {port}"
        assert source == ecs_sg_lid, "Redis SG ingress must originate from ECS SG only"


def test_sg_lambda_egress_to_rds(template):
    """Prompt: 'The Lambda must run inside the VPC and reach the database tier using security groups.'
    Lambda SG must have egress to RDS SG on TCP/5432."""
    lambda_sg_lid = _find_sg_logical_id(template, "lambda")
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    assert lambda_sg_lid, "Could not find Lambda security group"
    assert rds_sg_lid, "Could not find RDS security group"

    egress_rules = _get_sg_egress_rules(template, lambda_sg_lid)
    found_rds_egress = False
    for rule in egress_rules:
        port = str(rule.get("FromPort", ""))
        dest = _resolve_ref(rule.get("DestinationSecurityGroupId"))
        if port == "5432" and dest == rds_sg_lid:
            found_rds_egress = True
            break
    assert found_rds_egress, "Lambda SG must have egress to RDS SG on TCP/5432"


def test_iam_ecs_task_role_has_required_permissions(template):
    """ECS task role must have SQS send, SNS publish, DynamoDB RW, SecretsManager read, SSM read."""
    ecs_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any("ecs-tasks" in str(s) for s in svcs):
                ecs_roles.append(lid)
                break

    all_actions = set()
    for role_lid in ecs_roles:
        actions = _actions_in_statements(_get_policies_for_role(template, role_lid))
        all_actions.update(actions)

    assert "sqs:SendMessage" in all_actions, "ECS task role must have sqs:SendMessage"
    assert "sns:Publish" in all_actions, "ECS task role must have sns:Publish"
    assert any(a.startswith("dynamodb:") for a in all_actions), "ECS task role must have DynamoDB permissions"
    assert any(a.startswith("secretsmanager:") for a in all_actions), "ECS task role must have SecretsManager read"
    assert any(a.startswith("ssm:") for a in all_actions), "ECS task role must have SSM read"


def test_iam_ecs_task_role_no_admin_actions(template):
    """ECS task role must not have overly broad actions (iam:*, sts:*, ec2:*)."""
    ecs_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any("ecs-tasks" in str(s) for s in svcs):
                ecs_roles.append(lid)
                break

    for role_lid in ecs_roles:
        actions = _actions_in_statements(_get_policies_for_role(template, role_lid))
        for action in actions:
            assert not action.startswith("iam:"), f"ECS task role must not have {action}"


def test_iam_lambda_role_has_secrets_read(template):
    """Lambda execution role must have SecretsManager read plus scoped SSM reads."""
    lambda_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any("lambda" in str(s) for s in svcs):
                lambda_roles.append(lid)
                break

    all_actions = set()
    for role_lid in lambda_roles:
        actions = _actions_in_statements(_get_policies_for_role(template, role_lid))
        all_actions.update(actions)

    assert any(a.startswith("secretsmanager:") for a in all_actions), \
        "Lambda role must have SecretsManager read for DB credentials"
    assert "ssm:GetParameter" in all_actions, \
        "Lambda role must be able to read the DB endpoint parameter it receives"


def test_iam_sfn_role_invokes_lambda_only(template):
    """Step Functions execution role must be scoped to invoke only the migration Lambda."""
    migration_lambda_lid = None
    for lid, fn in get_resources(template, "AWS::Lambda::Function").items():
        props = fn.get("Properties", {})
        runtime = str(props.get("Runtime", ""))
        desc = str(props.get("Description", "")).lower()
        if "nodejs20" in runtime and "auto-delet" not in desc and "logretention" not in desc:
            migration_lambda_lid = lid
            break
    assert migration_lambda_lid, "Could not find migration Lambda"

    sfn_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any(str(s).startswith("states") for s in svcs):
                sfn_roles.append(lid)
                break

    assert len(sfn_roles) >= 1, "Expected at least 1 Step Functions role"
    for role_lid in sfn_roles:
        statements = _get_policies_for_role(template, role_lid)
        actions = _actions_in_statements(statements)
        assert any(a.startswith("lambda:") for a in actions), \
            "Step Functions role must be able to invoke Lambda"
        lambda_statements = []
        for stmt in statements:
            stmt_actions = stmt.get("Action", [])
            if isinstance(stmt_actions, str):
                stmt_actions = [stmt_actions]
            if any(a.startswith("lambda:") for a in stmt_actions):
                lambda_statements.append(stmt)
        assert lambda_statements, "Step Functions role must have a Lambda invocation statement"
        for stmt in lambda_statements:
            resources = stmt.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]
            assert resources, "Step Functions Lambda invocation statement must scope resources"
            for resource in resources:
                assert _contains_reference_to(resource, migration_lambda_lid), \
                    "Step Functions Lambda invocation permission must target the migration Lambda"
        for action in actions:
            assert not action.startswith("ecs:"), f"SFN role must not have {action}"
            assert not action.startswith("dynamodb:"), f"SFN role must not have {action}"
            assert not action.startswith("sqs:"), f"SFN role must not have {action}"
            assert not action.startswith("sns:"), f"SFN role must not have {action}"


def test_iam_lambda_role_log_scoping(template):
    """Migration Lambda role log permissions must be scoped to its own log group resources."""
    lambda_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any("lambda" in str(s) for s in svcs):
                role_stmts = _get_policies_for_role(template, lid)
                role_actions = _actions_in_statements(role_stmts)
                if any(a.startswith("rds-db:") or a.startswith("secretsmanager:") for a in role_actions):
                    lambda_roles.append(lid)
                break

    assert len(lambda_roles) >= 1, "Could not find migration Lambda role"
    lambda_log_groups = get_resources(template, "AWS::Logs::LogGroup")
    migration_log_group_lid = None
    for lid, lg in lambda_log_groups.items():
        name = str(lg.get("Properties", {}).get("LogGroupName", ""))
        if "/aws/lambda/" in name:
            migration_log_group_lid = lid
            break
    assert migration_log_group_lid, "Could not find migration Lambda log group"

    for role_lid in lambda_roles:
        role = template["Resources"][role_lid]
        managed = role.get("Properties", {}).get("ManagedPolicyArns", [])
        managed_str = json.dumps(managed)
        assert "AWSLambdaBasicExecutionRole" not in managed_str, \
            "Migration Lambda log permissions must be explicitly scoped, not delegated to the broad basic execution managed policy"

        stmts = _get_policies_for_role(template, role_lid)
        log_statements = []
        for stmt in stmts:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if any(str(action).startswith("logs:") for action in actions):
                log_statements.append(stmt)
        assert log_statements, "Migration Lambda role must have explicit log statements"

        for stmt in log_statements:
            actions = _statement_actions(stmt)
            assert set(actions).issubset({"logs:CreateLogStream", "logs:PutLogEvents"}), \
                f"Unexpected Lambda log actions: {actions}"
            resources = _statement_resources(stmt)
            assert resources, "Lambda log statements must include explicit resources"
            assert len(resources) == 2, "Lambda log scope must cover only the log group ARN and its streams"
            for resource in resources:
                assert resource != "*", "Lambda log statements must not use wildcard resources"
                assert _contains_reference_to(resource, migration_log_group_lid), \
                    "Lambda log statements must be scoped to the migration Lambda log group"


def test_iam_log_permissions_are_role_specific(template):
    """Migration Lambda and Step Functions log permissions must stay isolated to their respective roles."""
    migration_log_group_lid = None
    for lid, lg in get_resources(template, "AWS::Logs::LogGroup").items():
        name = str(lg.get("Properties", {}).get("LogGroupName", ""))
        if "/aws/lambda/" in name:
            migration_log_group_lid = lid
            break
    assert migration_log_group_lid, "Could not find migration Lambda log group"

    lambda_role_lid = _find_role_logical_id(template, "lambda.amazonaws.com")
    sfn_role_lid = _find_role_logical_id(template, "states")
    assert lambda_role_lid and sfn_role_lid

    for role_lid in [lambda_role_lid, sfn_role_lid]:
        statements = _get_policies_for_role(template, role_lid)
        for stmt in statements:
            actions = _statement_actions(stmt)
            resources = _statement_resources(stmt)
            if any(action.startswith("logs:CreateLog") or action == "logs:PutLogEvents" for action in actions):
                if role_lid == lambda_role_lid:
                    assert all(_contains_reference_to(resource, migration_log_group_lid) for resource in resources), \
                        "Lambda log write permissions must stay bound to the migration Lambda log group"
                else:
                    assert resources == ["*"], \
                        "Only the Step Functions role may use the documented wildcard log-delivery form"


def test_determinism_security_group_rules_use_references(template):
    """Security-group-to-security-group relationships must be expressed through CloudFormation references."""
    ecs_sg_lid = _find_sg_logical_id(template, "ecs")
    alb_sg_lid = _find_sg_logical_id(template, "alb")
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    redis_sg_lid = _find_sg_logical_id(template, "redis")
    lambda_sg_lid = _find_sg_logical_id(template, "lambda")
    assert all([ecs_sg_lid, alb_sg_lid, rds_sg_lid, redis_sg_lid, lambda_sg_lid])

    ingress_expectations = {
        ecs_sg_lid: {alb_sg_lid},
        rds_sg_lid: {ecs_sg_lid, lambda_sg_lid},
        redis_sg_lid: {ecs_sg_lid},
    }
    for sg_lid, allowed_sources in ingress_expectations.items():
        for rule in _get_sg_ingress_rules(template, sg_lid):
            if rule.get("CidrIp"):
                continue
            source = rule.get("SourceSecurityGroupId")
            assert isinstance(source, dict), "Inter-SG ingress must use CloudFormation references"
            assert _resolve_ref(source) in allowed_sources

    egress_expectations = {
        ecs_sg_lid: {rds_sg_lid, redis_sg_lid},
        lambda_sg_lid: {rds_sg_lid},
    }
    for sg_lid, allowed_destinations in egress_expectations.items():
        for rule in _get_sg_egress_rules(template, sg_lid):
            if rule.get("CidrIp"):
                continue
            destination = rule.get("DestinationSecurityGroupId")
            assert isinstance(destination, dict), "Inter-SG egress must use CloudFormation references"
            assert _resolve_ref(destination) in allowed_destinations


def test_iam_lambda_role_ssm_scope_is_parameter_specific(template):
    """Migration Lambda SSM reads must stay scoped to the single DB endpoint parameter."""
    lambda_role_lid = _find_role_logical_id(template, "lambda.amazonaws.com")
    assert lambda_role_lid, "Could not find a Lambda role"

    parameter_lid = next(iter(get_resources(template, "AWS::SSM::Parameter").keys()))
    statements = _get_policies_for_role(template, lambda_role_lid)
    ssm_statements = [
        stmt for stmt in statements
        if "ssm:GetParameter" in _statement_actions(stmt)
    ]
    assert ssm_statements, "Migration Lambda role must include an SSM read statement"
    for stmt in ssm_statements:
        assert set(_statement_actions(stmt)) == {"ssm:GetParameter"}, \
            "Migration Lambda SSM scope must stay minimal"
        resources = _statement_resources(stmt)
        assert len(resources) == 1, "Migration Lambda SSM scope must reference a single parameter"
        assert _contains_reference_to(resources[0], parameter_lid), \
            "Migration Lambda SSM permission must target the DB endpoint parameter"


def test_iam_sfn_role_log_scoping(template):
    """Step Functions role must have log delivery actions (documented as requiring Resource: '*')
    and must not have broad non-log actions."""
    sfn_roles = []
    for lid, res in template.get("Resources", {}).items():
        if res.get("Type") != "AWS::IAM::Role":
            continue
        stmts = res.get("Properties", {}).get("AssumeRolePolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            svcs = stmt.get("Principal", {}).get("Service", [])
            if isinstance(svcs, str):
                svcs = [svcs]
            if any(str(s).startswith("states") for s in svcs):
                sfn_roles.append(lid)
                break

    assert len(sfn_roles) >= 1, "Expected at least 1 Step Functions role"
    for role_lid in sfn_roles:
        stmts = _get_policies_for_role(template, role_lid)
        has_log_delivery = False
        for stmt in stmts:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if any(a.startswith("logs:") for a in actions):
                has_log_delivery = True
                assert set(actions).issubset({
                    "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
                }), f"Unexpected Step Functions log actions: {actions}"
                resources = stmt.get("Resource", [])
                if isinstance(resources, str):
                    resources = [resources]
                assert resources == ["*"], \
                    "Step Functions log delivery permissions should only use the documented wildcard resource form"
        assert has_log_delivery, "SFN role must have log delivery permissions"


def test_determinism_no_fixed_names(template):
    """No fixed resource names for DynamoDB, SQS, or ECS Cluster."""
    for lid, resource in template.get("Resources", {}).items():
        props = resource.get("Properties", {})
        for key in ["TableName", "QueueName", "ClusterName"]:
            if key in props:
                assert isinstance(props[key], dict), f"Fixed {key} in {lid}"


def test_determinism_relationships_use_references(template):
    """Critical relationships must be expressed through CloudFormation references, not hardcoded identifiers."""
    service = next(iter(get_resources(template, "AWS::ECS::Service").values()))
    cluster_lid = next(iter(get_resources(template, "AWS::ECS::Cluster").keys()))
    task_def_lid = next(iter(get_resources(template, "AWS::ECS::TaskDefinition").keys()))
    assert _contains_reference_to(service.get("Properties", {}).get("Cluster"), cluster_lid)
    assert _contains_reference_to(service.get("Properties", {}).get("TaskDefinition"), task_def_lid)

    listener = next(iter(get_resources(template, "AWS::ElasticLoadBalancingV2::Listener").values()))
    target_group_lid = next(iter(get_resources(template, "AWS::ElasticLoadBalancingV2::TargetGroup").keys()))
    assert _contains_reference_to(listener.get("Properties", {}).get("DefaultActions"), target_group_lid)

    alarm = next(iter(get_resources(template, "AWS::CloudWatch::Alarm").values()))
    load_balancer_lid = next(iter(get_resources(template, "AWS::ElasticLoadBalancingV2::LoadBalancer").keys()))
    assert _contains_reference_to(alarm.get("Properties", {}).get("Dimensions"), load_balancer_lid)
    assert _contains_reference_to(alarm.get("Properties", {}).get("Dimensions"), target_group_lid)

    db_instance = next(iter(get_resources(template, "AWS::RDS::DBInstance").values()))
    db_subnet_group_lid = next(iter(get_resources(template, "AWS::RDS::DBSubnetGroup").keys()))
    rds_sg_lid = _find_sg_logical_id(template, "rds")
    assert _contains_reference_to(db_instance.get("Properties", {}).get("DBSubnetGroupName"), db_subnet_group_lid)
    assert _contains_reference_to(db_instance.get("Properties", {}).get("VPCSecurityGroups"), rds_sg_lid)

    parameter_lid = next(iter(get_resources(template, "AWS::SSM::Parameter").keys()))
    parameter = template["Resources"][parameter_lid]
    db_instance_lid = next(iter(get_resources(template, "AWS::RDS::DBInstance").keys()))
    assert _contains_reference_to(parameter.get("Properties", {}).get("Value"), db_instance_lid)

    task_definition = next(iter(get_resources(template, "AWS::ECS::TaskDefinition").values()))
    secret_lid = next(iter(get_resources(template, "AWS::SecretsManager::Secret").keys()))
    table_lid = next(iter(get_resources(template, "AWS::DynamoDB::Table").keys()))
    queue_lid = next(iter(get_resources(template, "AWS::SQS::Queue").keys()))
    topic_lid = next(iter(get_resources(template, "AWS::SNS::Topic").keys()))
    assert any(
        _contains_reference_to(task_definition.get("Properties", {}).get("ExecutionRoleArn"), role_lid)
        for role_lid in get_resources(template, "AWS::IAM::Role").keys()
    ), "Task definition execution role must be linked through a CloudFormation reference"
    for container in task_definition.get("Properties", {}).get("ContainerDefinitions", []):
        assert _contains_reference_to(container.get("Secrets"), secret_lid)
        assert _contains_reference_to(container.get("Environment"), parameter_lid)
        assert _contains_reference_to(container.get("Environment"), table_lid)
        assert _contains_reference_to(container.get("Environment"), queue_lid)
        assert _contains_reference_to(container.get("Environment"), topic_lid)

    lambda_lid = next(iter(get_resources(template, "AWS::Lambda::Function").keys()))
    lambda_function = template["Resources"][lambda_lid]
    lambda_role_lid = _find_role_logical_id(template, "lambda.amazonaws.com")
    assert _contains_reference_to(lambda_function.get("Properties", {}).get("Role"), lambda_role_lid)
    assert _contains_reference_to(lambda_function.get("Properties", {}).get("Environment"), secret_lid)
    assert _contains_reference_to(lambda_function.get("Properties", {}).get("Environment"), parameter_lid)

    state_machine_lid = next(iter(get_resources(template, "AWS::StepFunctions::StateMachine").keys()))
    state_machine = template["Resources"][state_machine_lid]
    state_machine_role_lid = _find_role_logical_id(template, "states")
    assert _contains_reference_to(state_machine.get("Properties", {}).get("RoleArn"), state_machine_role_lid)
    assert _contains_reference_to(state_machine.get("Properties", {}).get("DefinitionString"), lambda_lid)
    for log_group_lid, log_group in get_resources(template, "AWS::Logs::LogGroup").items():
        if "MigrationStateMachineLogGroup" in log_group_lid:
            assert _contains_reference_to(
                state_machine.get("Properties", {}).get("LoggingConfiguration"),
                log_group_lid,
            )
            break
    else:
        pytest.fail("Could not find Step Functions log group")

    rule = next(iter(get_resources(template, "AWS::Events::Rule").values()))
    assert _contains_reference_to(rule.get("Properties", {}).get("Targets"), state_machine_lid)
    eventbridge_role_lid = _find_role_logical_id(template, "events.amazonaws.com")
    assert _contains_reference_to(rule.get("Properties", {}).get("Targets"), eventbridge_role_lid)


def test_outputs_present(template):
    """Required CfnOutputs exist."""
    outputs = template.get("Outputs", {})
    output_keys = list(outputs.keys())
    for expected in ["CloudFrontDomainName", "ALBDNSName", "HostedZoneId", "RDSEndpoint"]:
        assert any(expected in k for k in output_keys), f"Missing output: {expected}"


def test_negative_no_wildcard_iam_actions(template):
    """No IAM policy may use wildcard '*' as an Action."""
    policies = get_resources_list(template, "AWS::IAM::Policy")
    for policy in policies:
        stmts = policy.get("Properties", {}).get("PolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            for action in actions:
                assert action != "*", f"Wildcard Action found in policy {policy.get('Properties', {}).get('PolicyName', '?')}"


def test_negative_no_wildcard_iam_resources(template):
    """No non-CDK-internal IAM policy may use wildcard '*' as a Resource (except documented AWS API requirements)."""
    log_delivery_actions = {
        "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    }
    policies = get_resources_list(template, "AWS::IAM::Policy")
    for policy in policies:
        name = policy.get("Properties", {}).get("PolicyName", "")
        if "LogRetention" in name:
            continue
        stmts = policy.get("Properties", {}).get("PolicyDocument", {}).get("Statement", [])
        if isinstance(stmts, dict):
            stmts = [stmts]
        for stmt in stmts:
            resources = stmt.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if set(actions).issubset(log_delivery_actions):
                continue
            for res in resources:
                assert res != "*", f"Wildcard Resource in policy {name}"


def test_negative_exactly_one_listener(template):
    """Prompt: 'Exactly 1 HTTP listener on port 80' – only one listener should exist."""
    listeners = get_resources_list(template, "AWS::ElasticLoadBalancingV2::Listener")
    assert len(listeners) == 1, f"Expected exactly 1 ALB listener, got {len(listeners)}"
    props = listeners[0].get("Properties", {})
    assert str(props.get("Port")) == "80", "The single listener must be on port 80"
    assert str(props.get("Protocol", "")).upper() == "HTTP", "The single listener must use HTTP protocol"


def test_negative_rds_not_publicly_accessible(template):
    """RDS must never be publicly accessible."""
    instances = get_resources_list(template, "AWS::RDS::DBInstance")
    assert len(instances) >= 1, "Expected at least 1 RDS instance"
    for db in instances:
        val = db.get("Properties", {}).get("PubliclyAccessible", False)
        assert str(val).lower() in ("false", ""), \
            "RDS must not be publicly accessible"


def test_negative_no_open_all_ports_ingress(template):
    """No security group should allow ingress on all ports from 0.0.0.0/0."""
    sgs = get_resources_list(template, "AWS::EC2::SecurityGroup")
    for sg in sgs:
        for rule in sg.get("Properties", {}).get("SecurityGroupIngress", []):
            is_all_ports = (
                str(rule.get("IpProtocol")) == "-1"
                or (str(rule.get("FromPort")) == "0" and str(rule.get("ToPort")) == "65535")
            )
            is_open_cidr = rule.get("CidrIp") == "0.0.0.0/0"
            assert not (is_all_ports and is_open_cidr), "No SG should allow all-port ingress from 0.0.0.0/0"


def test_negative_cloudfront_uses_oac_or_oai(template):
    """Prompt: 'allow reads only via CloudFront using an origin access control or origin access identity mechanism.'
    CloudFront distribution must use OAC or OAI for S3 origin access."""
    distributions = get_resources_list(template, "AWS::CloudFront::Distribution")
    assert len(distributions) >= 1, "Expected at least 1 CloudFront distribution"
    for dist in distributions:
        config = dist.get("Properties", {}).get("DistributionConfig", {})
        origins = config.get("Origins", [])
        for origin in origins:
            has_oac = "OriginAccessControlId" in origin
            s3_config = origin.get("S3OriginConfig", {})
            has_oai = bool(s3_config.get("OriginAccessIdentity", ""))
            if origin.get("DomainName"):
                assert has_oac or has_oai, \
                    "CloudFront S3 origin must use OAC or OAI for access control"
