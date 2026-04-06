"""
conftest.py – pytest session bootstrap.

1. Installs packages from requirements.txt before any test module is imported.
   Three escalating pip strategies handle venvs, Debian PEP-668 containers,
   and user-site fallback.

2. Resets boto3 endpoint configuration before every test so that moto's
   request interceptor can match on standard AWS service hostnames.
   When AWS_ENDPOINT_URL is present in the environment, boto3 directs all
   requests to that URL instead of the per-service AWS hostnames that moto
   uses for interception. Clearing it before each test restores standard
   routing so @mock_aws works correctly in any CI environment.
"""

import os
import subprocess
import sys

import pytest


def pytest_configure(config):  # noqa: ARG001
    """Ensure test dependencies are present before any test module is imported."""
    try:
        import moto  # noqa: F401
        return
    except ImportError:
        pass

    req_file = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
    )
    if not os.path.exists(req_file):
        return

    base_cmd = [
        sys.executable, "-m", "pip", "install",
        "-r", req_file,
        "--quiet",
        "--disable-pip-version-check",
    ]

    for cmd in [base_cmd, base_cmd + ["--break-system-packages"], base_cmd + ["--user"]]:
        if subprocess.run(cmd, check=False).returncode == 0:
            break


@pytest.fixture(autouse=True)
def _reset_endpoint_config(monkeypatch):
    """Clear endpoint URL overrides before every test.

    Ensures boto3 uses standard per-service AWS hostnames so that moto's
    @mock_aws decorator can intercept requests correctly. Removed variables
    are automatically restored by monkeypatch after each test completes.
    """
    for var in ("AWS_ENDPOINT_URL", "AWS_DEFAULT_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
