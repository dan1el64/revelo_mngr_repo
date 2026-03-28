"""Integration tests (run after deployment). Use boto3 to verify deployed resources."""

import os
from pathlib import Path

import boto3
import pytest

_REPO_DIR = Path(__file__).resolve().parent.parent


def _cfn_client():
    """CloudFormation client for the provider via env."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT")
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(
        "cloudformation",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def _s3_client():
    """S3 client for the provider via env."""
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


def _get_bucket_name_from_stack():
    """Find the deployed CDK stack and return its BucketName output."""
    cfn = _cfn_client()
    paginator = cfn.get_paginator("list_stacks")
    for page in paginator.paginate(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE"]):
        for summary in page.get("StackSummaries", []):
            name = summary.get("StackName", "")
            if "AppStack" in name or "app" in name.lower():
                out = cfn.describe_stacks(StackName=name)
                stacks = out.get("Stacks", [])
                if not stacks:
                    continue
                outputs = {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}
                if "BucketName" in outputs:
                    return outputs["BucketName"]
    return None


def test_stack_deployed():
    """At least one stack (AppStack) must be deployed."""
    bucket_name = _get_bucket_name_from_stack()
    assert bucket_name, (
        "Could not find deployed stack with BucketName output. "
        "Ensure cdklocal deploy completed and the stack exports BucketName."
    )


def test_s3_bucket_deployed():
    """S3 bucket from stack output must exist in the backend and be accessible."""
    bucket_name = _get_bucket_name_from_stack()
    assert bucket_name, "Could not determine bucket name from stack outputs"

    s3 = _s3_client()
    s3.head_bucket(Bucket=bucket_name)


def test_bucket_tags():
    """Bucket must have the required tags (Name, Project) when queried via boto3."""
    bucket_name = _get_bucket_name_from_stack()
    assert bucket_name, "Could not determine bucket name from stack outputs"

    s3 = _s3_client()
    resp = s3.get_bucket_tagging(Bucket=bucket_name)
    tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}

    assert "Name" in tags, "Bucket must have 'Name' tag"
    assert "Project" in tags, "Bucket must have 'Project' tag"
