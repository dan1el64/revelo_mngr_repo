"""Unit tests for CDK template. Run after harness synth step; tests use template.json."""

import json
import os
from pathlib import Path

import pytest

_REPO_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_JSON = _REPO_DIR / "template.json"


def test_template_json_exists_and_valid():
    """Synthesized template must be valid JSON (CloudFormation format)."""
    if not TEMPLATE_JSON.exists():
        pytest.skip("template.json not found - synth step may have been skipped")
    data = json.loads(TEMPLATE_JSON.read_text())
    assert "Resources" in data


def test_template_contains_s3_bucket():
    """Template must include an S3 bucket resource."""
    if not TEMPLATE_JSON.exists():
        pytest.skip("template.json not found - synth step may have been skipped")
    data = json.loads(TEMPLATE_JSON.read_text())
    resources = data.get("Resources") or {}
    bucket_logical_ids = [
        lid for lid, props in resources.items()
        if props.get("Type") == "AWS::S3::Bucket"
    ]
    assert len(bucket_logical_ids) >= 1, (
        f"Expected at least one AWS::S3::Bucket in template, got: {list(resources.keys())}"
    )


def test_template_bucket_has_name_prefix():
    """Bucket resource must use a name that includes the name prefix (for collision avoidance)."""
    if not TEMPLATE_JSON.exists():
        pytest.skip("template.json not found - synth step may have been skipped")
    name_prefix = os.environ.get("NAME_PREFIX", "dev")
    assert name_prefix, "NAME_PREFIX environment variable must be set"

    data = json.loads(TEMPLATE_JSON.read_text())
    resources = data.get("Resources") or {}
    for logical_id, props in resources.items():
        if props.get("Type") != "AWS::S3::Bucket":
            continue
        bucket_name = (props.get("Properties") or {}).get("BucketName")
        # CDK may use Ref; if it's a literal string, check prefix
        if isinstance(bucket_name, str) and bucket_name:
            assert bucket_name.startswith(f"{name_prefix}-"), (
                f"Bucket name must start with '{name_prefix}-', got: {bucket_name!r}"
            )
            return
    # If no literal BucketName found (e.g. CDK used Ref), require NAME_PREFIX in app code
    app_java = _REPO_DIR / "src/main/java/com/example/App.java"
    if app_java.exists():
        content = app_java.read_text()
        assert "NAME_PREFIX" in content, "App must use NAME_PREFIX from environment"
    else:
        pytest.fail("No AWS::S3::Bucket with literal BucketName found in template")


def test_template_has_bucket_name_output():
    """Stack should export BucketName output for integration tests."""
    if not TEMPLATE_JSON.exists():
        pytest.skip("template.json not found - synth step may have been skipped")
    data = json.loads(TEMPLATE_JSON.read_text())
    outputs = data.get("Outputs") or {}
    assert "BucketName" in outputs or any("bucket" in k.lower() for k in outputs), (
        "Template should define BucketName (or similar) output for integration tests"
    )
