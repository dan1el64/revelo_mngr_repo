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
    endpoint = os.environ.get("AWS_ENDPOINT")
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


def _bucket_name_from_state():
    """Read bucket name from state.json (first S3 bucket resource in deployment)."""
    if not STATE_JSON.exists():
        return None
    data = json.loads(STATE_JSON.read_text())
    resources = data.get("deployment", {}).get("resources", [])
    for r in resources:
        if r.get("type") == "aws:s3/bucket:Bucket":
            out = r.get("outputs") or {}
            return out.get("bucket") or out.get("id")
    return None


def test_pulumi_up_succeeded():
    """Deploy must have produced state.json so we know what was deployed."""
    assert STATE_JSON.exists(), (
        f"state.json not found at {STATE_JSON}. "
        "Run 'pulumi up' and then 'pulumi stack export > state.json' before integration tests."
    )
    data = json.loads(STATE_JSON.read_text())
    assert "deployment" in data, "state.json must have 'deployment' key"


def test_s3_bucket_deployed():
    """S3 bucket from state must exist in the backend and be accessible via boto3."""
    bucket_name = _bucket_name_from_state()
    assert bucket_name, "Could not determine bucket name from state.json (missing bucketName output or S3 bucket resource)"

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
