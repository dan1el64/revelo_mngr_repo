"""Unit tests for Terraform / config validation. Run after harness plan step; tests use plan.json."""

import json
from pathlib import Path

import pytest

# Plan is produced by the harness (syntax_plan step) before unit tests run.
# In the harness, everything runs in a container at /work, so plan.json is in the repo root.
_REPO_DIR = Path(__file__).resolve().parent.parent
PLAN_JSON = _REPO_DIR / "plan.json"


def test_list_not_empty():
    """Example unit test."""
    items = ["main.tf", "variables.tf"]
    assert len(items) >= 1


def test_plan_json_exists_and_valid():
    """Plan output from harness must be valid JSON."""
    data = json.loads(PLAN_JSON.read_text())
    assert "resource_changes" in data


def test_plan_contains_s3_bucket():
    """Plan must include the S3 bucket resource."""
    data = json.loads(PLAN_JSON.read_text())
    changes = data.get("resource_changes") or []
    addresses = [c["address"] for c in changes]
    assert any("aws_s3_bucket.main" in addr for addr in addresses), (
        f"Expected aws_s3_bucket.main in plan, got: {addresses}"
    )


def test_plan_bucket_has_name_prefix():
    """Planned bucket name must use the name_prefix variable value to avoid collisions."""
    data = json.loads(PLAN_JSON.read_text())

    # Extract name_prefix from plan configuration
    variables = data.get("configuration", {}).get("root_module", {}).get("variables", {})
    name_prefix_var = variables.get("name_prefix", {})
    name_prefix = name_prefix_var.get("default") or name_prefix_var.get("value")

    # If not in configuration, try to get from planned_values or variables
    if not name_prefix:
        # Try variables section at top level
        vars_section = data.get("variables", {})
        name_prefix = vars_section.get("name_prefix", {}).get("value")

    assert name_prefix, "name_prefix variable must be set in plan"

    # Find the bucket resource in plan
    for c in data.get("resource_changes") or []:
        if "aws_s3_bucket.main" in c.get("address", ""):
            after = (c.get("change") or {}).get("after") or {}
            bucket_name = after.get("bucket") or ""

            # Verify bucket name starts with the name_prefix
            assert bucket_name.startswith(f"{name_prefix}-"), (
                f"Bucket name must start with '{name_prefix}-', got: {bucket_name!r}"
            )
            return

    pytest.fail("aws_s3_bucket.main not found in resource_changes")
