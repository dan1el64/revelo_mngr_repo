"""Integration tests (run after deployment). Use boto3 to verify deployed resources."""

import json
import os
from pathlib import Path

import boto3
import pytest

_REPO_DIR = Path(__file__).resolve().parent.parent
STATE_JSON = _REPO_DIR / "state.json"


def _s3_client():
    """S3 client configured for the provider via env."""
    endpoint = (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
        or os.environ.get("TF_VAR_aws_endpoint")
    )
    region = os.environ.get("AWS_REGION") or os.environ.get("TF_VAR_aws_region") or "us-east-1"
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


def _bucket_name_from_state():
    """Read bucket name from Terraform state.json (aws_s3_bucket.main)."""
    if not STATE_JSON.exists():
        return None
    data = json.loads(STATE_JSON.read_text())
    resources = (
        data.get("values", {}).get("root_module", {}).get("resources", [])
    )
    for r in resources:
        if r.get("type") == "aws_s3_bucket" and r.get("name") == "main":
            return (r.get("values") or {}).get("bucket")
    return None


def test_terraform_apply_succeeded():
    """Apply must have produced state.json so we know what was deployed."""
    assert STATE_JSON.exists(), (
        f"state.json not found at {STATE_JSON}. "
        "Run 'terraform apply' and then 'terraform show -json > state.json' before integration tests."
    )
    data = json.loads(STATE_JSON.read_text())
    assert "values" in data, "state.json must have 'values' key"
    assert "root_module" in data["values"], "state.json must have 'root_module'"


def test_s3_bucket_deployed():
    """S3 bucket from state must exist in the backend and be accessible via boto3."""
    bucket_name = _bucket_name_from_state()
    assert bucket_name, (
        "Could not determine bucket name from state.json (missing aws_s3_bucket.main)"
    )

    s3 = _s3_client()
    s3.head_bucket(Bucket=bucket_name)


def test_bucket_tags():
    """Bucket must have the required tags (Name, Project) when queried via boto3."""
    bucket_name = _bucket_name_from_state()
    assert bucket_name, "Could not determine bucket name from state.json"

    s3 = _s3_client()
    resp = s3.get_bucket_tagging(Bucket=bucket_name)
    tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}

    assert "Name" in tags, "Bucket must have 'Name' tag"
    assert "Project" in tags, "Bucket must have 'Project' tag"
