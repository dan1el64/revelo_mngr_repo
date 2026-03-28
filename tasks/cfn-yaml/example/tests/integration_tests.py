"""Integration tests (run after deployment). Use boto3 to verify deployed stack and resources."""

import json
import os
from pathlib import Path

import boto3
import pytest

_REPO_DIR = Path(__file__).resolve().parent.parent
STACK_OUTPUTS = _REPO_DIR / "stack_outputs.json"


def _bucket_name_from_outputs():
    """Read BucketName from stack_outputs.json (array of {OutputKey, OutputValue})."""
    if not STACK_OUTPUTS.exists():
        return None
    data = json.loads(STACK_OUTPUTS.read_text())
    if not isinstance(data, list):
        return None
    for out in data:
        if out.get("OutputKey") == "BucketName":
            return out.get("OutputValue")
    return None


def _s3_client():
    """S3 client configured for the provider via env."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT")
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


def test_stack_deployed():
    """Deploy step must have produced stack_outputs.json with BucketName."""
    assert STACK_OUTPUTS.exists(), (
        "stack_outputs.json not found. Ensure create-stack completed and describe-stacks wrote outputs."
    )
    bucket_name = _bucket_name_from_outputs()
    assert bucket_name, (
        "Could not find deployed stack with BucketName output. "
        "Ensure template has Outputs.BucketName and deploy completed."
    )


def test_s3_bucket_deployed():
    """S3 bucket from stack output must exist in the provider."""
    bucket_name = _bucket_name_from_outputs()
    assert bucket_name, "Could not determine bucket name from stack outputs"
    s3 = _s3_client()
    s3.head_bucket(Bucket=bucket_name)


def test_bucket_tags():
    """Bucket must have the required tags (Name, Project) when queried via boto3."""
    bucket_name = _bucket_name_from_outputs()
    assert bucket_name, "Could not determine bucket name from stack outputs"

    s3 = _s3_client()
    resp = s3.get_bucket_tagging(Bucket=bucket_name)
    tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}

    assert "Name" in tags, "Bucket must have 'Name' tag"
    assert "Project" in tags, "Bucket must have 'Project' tag"
