import os
import importlib.util
import re
import sys
from pathlib import Path

os.environ.setdefault("HOME", "/tmp")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp")


def _load_app_module():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    spec = importlib.util.spec_from_file_location("app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules["app"] = module
    spec.loader.exec_module(module)
    return module


app = _load_app_module()


def test_environment_helpers_use_expected_inputs(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT", raising=False)

    environment = app.build_lambda_environment({"QUEUE_URL": "queue-url"})

    assert app.get_aws_region() == "us-east-1"
    assert environment == {"QUEUE_URL": "queue-url"}


def test_environment_helpers_honor_explicit_values(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ENDPOINT", "https://endpoint.internal")

    environment = app.build_lambda_environment({"DB_HOST": "db.internal"})

    assert app.get_aws_region() == "eu-west-1"
    assert environment == {
        "AWS_ENDPOINT": "https://endpoint.internal",
        "DB_HOST": "db.internal",
    }


def test_inline_lambda_code_declares_metrics_and_sdk_configuration():
    ingest_code = app.ingest_handler_code()
    workflow_code = app.workflow_worker_code()

    assert 'Namespace=METRIC_NAMESPACE' in ingest_code
    assert 'METRIC_NAME = "AcceptedRequests"' in ingest_code
    assert 'endpoint_url=AWS_ENDPOINT' in ingest_code
    assert 'QUEUE_URL = os.environ["QUEUE_URL"]' in ingest_code

    assert 'Namespace=METRIC_NAMESPACE' in workflow_code
    assert 'METRIC_NAME = "TasksProcessed"' in workflow_code
    assert 'endpoint_url=AWS_ENDPOINT' in workflow_code
    assert 'DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]' in workflow_code


def test_only_allowed_inputs_are_used_and_no_inline_secret_literals_exist():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text()
    combined_source = "\n".join(
        [app_source, app.ingest_handler_code(), app.workflow_worker_code()]
    )

    env_names = set(
        re.findall(r'os\.getenv\("([A-Z0-9_]+)"', combined_source)
        + re.findall(r'os\.environ(?:\.get)?\["([A-Z0-9_]+)"\]', combined_source)
        + re.findall(r'os\.environ\.get\("([A-Z0-9_]+)"', combined_source)
    )
    assert env_names <= {
        "AWS_ENDPOINT",
        "AWS_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "QUEUE_URL",
        "DB_SECRET_ARN",
        "DB_HOST",
    }

    suspicious_secret_literals = [
        r'password\s*=\s*["\'][^"\']+["\']',
        r'secret\s*=\s*["\'][^"\']+["\']',
        r'aws_secret_access_key\s*=\s*["\'][^"\']+["\']',
    ]
    for pattern in suspicious_secret_literals:
        assert re.search(pattern, combined_source, flags=re.IGNORECASE) is None


def test_physical_resource_names_are_not_hardcoded_in_source():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text()
    hardcoded_name_patterns = [
        r'function_name\s*=\s*["\']',
        r'queue_name\s*=\s*["\']',
        r'bucket_name\s*=\s*["\']',
        r'secret_name\s*=\s*["\']',
        r'state_machine_name\s*=\s*["\']',
        r'api_name\s*=\s*["\']',
    ]
    for pattern in hardcoded_name_patterns:
        assert re.search(pattern, app_source) is None


def test_stack_is_implemented_only_in_app_py():
    repo_root = Path(__file__).resolve().parents[1]
    python_files = sorted(
        path.relative_to(repo_root).as_posix()
        for path in repo_root.rglob("*.py")
        if "tests/" not in path.relative_to(repo_root).as_posix()
    )

    assert python_files == ["app.py"]


def test_build_app_registers_the_stack():
    cdk_app = app.build_app()

    assert any(child.node.id == "SecurityBaselineStack" for child in cdk_app.node.children)


def test_main_synthesizes_the_app(monkeypatch):
    class FakeApp:
        def __init__(self):
            self.synth_called = False

        def synth(self):
            self.synth_called = True

    fake_app = FakeApp()
    monkeypatch.setattr(app, "build_app", lambda: fake_app)

    app.main()

    assert fake_app.synth_called is True


# ---------------------------------------------------------------------------
# Negative-path and boundary tests
# ---------------------------------------------------------------------------


def test_get_aws_endpoint_returns_empty_string_when_unset(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT", raising=False)
    assert app.get_aws_endpoint() == ""


def test_build_lambda_environment_with_empty_extra_and_no_endpoint(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT", raising=False)
    assert app.build_lambda_environment({}) == {}


def test_build_lambda_environment_env_endpoint_wins_over_extra_key(monkeypatch):
    """Regression: double-update bug (app.py env update clobbering) must not exist.

    Passing AWS_ENDPOINT inside *extra* must be overwritten by the env-var value,
    not allowed to survive via a redundant environment.update(extra) call.
    """
    monkeypatch.setenv("AWS_ENDPOINT", "https://env.internal")
    env = app.build_lambda_environment({"AWS_ENDPOINT": "https://caller.internal"})
    assert env["AWS_ENDPOINT"] == "https://env.internal"


def test_generated_name_truncates_at_max_length():
    import aws_cdk as cdk

    cdk_app = cdk.App()
    stack = cdk.Stack(cdk_app, "TestStack")
    name = app.generated_name(stack, "suffix", max_length=8)
    assert len(name) <= 8


def test_generated_name_result_is_lowercase_and_within_default_limit():
    import aws_cdk as cdk

    cdk_app = cdk.App()
    stack = cdk.Stack(cdk_app, "TestStack")
    name = app.generated_name(stack, "worker")
    assert name == name.lower()
    assert len(name) <= 64


# ---------------------------------------------------------------------------
# IAM helper function assertions
# ---------------------------------------------------------------------------


def _minimal_role(stack):
    import aws_cdk as cdk
    from aws_cdk import aws_iam as iam

    return iam.Role(stack, "TestRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))


def _synth_policies(stack):
    import aws_cdk as cdk
    from aws_cdk.assertions import Template

    t = Template.from_stack(stack).to_json()
    return [
        r["Properties"]["PolicyDocument"]["Statement"]
        for r in t["Resources"].values()
        if r["Type"] == "AWS::IAM::Policy"
    ]


def _flatten_statements(stack):
    result = []
    for stmts in _synth_policies(stack):
        if isinstance(stmts, list):
            result.extend(stmts)
        else:
            result.append(stmts)
    return result


def test_add_lambda_vpc_permissions_grants_exactly_required_ec2_actions():
    import aws_cdk as cdk

    cdk_app = cdk.App()
    stack = cdk.Stack(cdk_app, "TestStack")
    role = _minimal_role(stack)
    app.add_lambda_vpc_permissions(role)

    statements = _flatten_statements(stack)
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["Sid"] == "VpcNetworking"
    actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
    assert set(actions) == {
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "ec2:AssignPrivateIpAddresses",
        "ec2:UnassignPrivateIpAddresses",
    }
    # ec2 Describe actions require *, but verify resource is explicitly set
    assert stmt["Resource"] == "*"


def test_add_log_write_permissions_scopes_actions_to_log_group_arn():
    import aws_cdk as cdk
    from aws_cdk import aws_logs as logs

    cdk_app = cdk.App()
    stack = cdk.Stack(cdk_app, "TestStack")
    role = _minimal_role(stack)
    log_group = logs.LogGroup(stack, "TestLogGroup")
    app.add_log_write_permissions(role, log_group, "WriteLogs")

    statements = _flatten_statements(stack)
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["Sid"] == "WriteLogs"
    actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
    assert set(actions) == {"logs:CreateLogStream", "logs:PutLogEvents"}
    # resource must NOT be wildcard — scoped to the specific log group
    resources = stmt["Resource"] if isinstance(stmt["Resource"], list) else [stmt["Resource"]]
    assert len(resources) == 2  # log group ARN + ARN:* stream wildcard
    resource_strs = [str(r) for r in resources]
    assert any("*" not in r for r in resource_strs), "at least one resource should be the exact log group ARN"
