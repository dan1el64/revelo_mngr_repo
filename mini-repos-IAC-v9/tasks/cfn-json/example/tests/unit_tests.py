"""Unit tests for CloudFormation template (JSON). Run after validate step; tests use template.json."""

import json
from pathlib import Path

import pytest

_REPO_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_JSON = _REPO_DIR / "template.json"


def _load_template():
    assert TEMPLATE_JSON.exists(), "template.json not found"
    with open(TEMPLATE_JSON) as f:
        return json.load(f)


def test_template_exists_and_valid():
    """Template file must exist and be valid JSON."""
    assert TEMPLATE_JSON.exists(), "template.json must exist"
    data = _load_template()
    assert isinstance(data, dict), "Template must be a JSON object"


def test_template_contains_s3_bucket():
    """Template must define an S3 bucket resource."""
    data = _load_template()
    resources = data.get("Resources") or {}
    found = any(
        res.get("Type") == "AWS::S3::Bucket"
        for res in resources.values()
    )
    assert found, f"Expected AWS::S3::Bucket in Resources, got: {list(resources.keys())}"


def test_template_bucket_has_name_prefix():
    """Bucket name must use NamePrefix parameter (string or intrinsic e.g. Fn::Sub)."""
    data = _load_template()
    params = data.get("Parameters") or {}
    assert "NamePrefix" in params, "Template must have Parameters.NamePrefix"
    resources = data.get("Resources") or {}
    for name, res in resources.items():
        if res.get("Type") == "AWS::S3::Bucket":
            props = res.get("Properties") or {}
            bucket_name = props.get("BucketName")
            assert bucket_name is not None, (
                f"Bucket resource {name} must set BucketName (e.g. Fn::Sub with NamePrefix)"
            )
            return
    pytest.fail("AWS::S3::Bucket not found in Resources")


def test_template_has_bucket_name_output():
    """Template must output BucketName for integration tests."""
    data = _load_template()
    outputs = data.get("Outputs") or {}
    assert "BucketName" in outputs, "Template must have Outputs.BucketName"
