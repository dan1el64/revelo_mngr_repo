"""Unit tests for Pulumi JavaScript. Run after harness preview step; tests use preview.json."""

import json
import os
from pathlib import Path

import pytest

# Preview is produced by the harness (syntax_plan step) before unit tests run.
_REPO_DIR = Path(__file__).resolve().parent.parent
PREVIEW_JSON = _REPO_DIR / "preview.json"
APP_JS = _REPO_DIR / "app.js"


def test_list_not_empty():
    """Example unit test."""
    items = ["app.js", "Pulumi.yaml", "package.json"]
    assert len(items) >= 1


def test_preview_json_exists_and_valid():
    """Preview output from harness must be valid JSON."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    data = json.loads(PREVIEW_JSON.read_text())
    assert isinstance(data, (dict, list))


def test_javascript_entry_exists():
    """JavaScript entry point must exist."""
    assert APP_JS.exists(), "app.js not found at project root"


def test_preview_contains_s3_bucket():
    """Preview must include the S3 bucket resource."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    data = json.loads(PREVIEW_JSON.read_text())

    resources = []
    if isinstance(data, list):
        for event in data:
            if event.get("resourceChanges"):
                resources.extend(event["resourceChanges"].values())
    elif isinstance(data, dict):
        if "steps" in data:
            resources = [step.get("newState", {}) for step in data.get("steps", [])]
        elif "resourceChanges" in data:
            resources = list(data["resourceChanges"].values())

    bucket_found = False
    for resource in resources:
        resource_type = resource.get("type", "")
        resource_urn = resource.get("urn", "")
        if "s3" in resource_type.lower() and "bucket" in resource_type.lower():
            bucket_found = True
            break
        if "s3/bucket" in resource_urn.lower() or "s3:bucket" in resource_urn.lower():
            bucket_found = True
            break

    assert bucket_found, f"Expected S3 bucket in preview, found {len(resources)} resources"


def test_preview_bucket_has_name_prefix():
    """Planned bucket name must use the namePrefix config to avoid collisions."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    name_prefix = os.environ.get("NAME_PREFIX", "dev")
    assert name_prefix, "NAME_PREFIX environment variable must be set"

    data = json.loads(PREVIEW_JSON.read_text())

    resources = []
    if isinstance(data, list):
        for event in data:
            if event.get("resourceChanges"):
                resources.extend(event["resourceChanges"].values())
    elif isinstance(data, dict):
        if "steps" in data:
            resources = [step.get("newState", {}) for step in data.get("steps", [])]
        elif "resourceChanges" in data:
            resources = list(data["resourceChanges"].values())

    bucket_name = None
    for resource in resources:
        resource_type = resource.get("type", "")
        resource_urn = resource.get("urn", "")
        is_bucket = (
            ("s3" in resource_type.lower() and "bucket" in resource_type.lower())
            or "s3/bucket" in resource_urn.lower()
            or "s3:bucket" in resource_urn.lower()
        )
        if is_bucket:
            inputs = resource.get("inputs", {})
            outputs = resource.get("outputs", {})
            bucket_name = (
                inputs.get("bucket")
                or outputs.get("bucket")
                or inputs.get("bucketName")
                or outputs.get("bucketName")
            )
            if bucket_name:
                break

    if bucket_name:
        assert bucket_name.startswith(f"{name_prefix}-"), (
            f"Bucket name must start with '{name_prefix}-', got: {bucket_name!r}"
        )
    else:
        assert APP_JS.exists(), "app.js not found"
        content = APP_JS.read_text()
        assert "namePrefix" in content or "NAME_PREFIX" in content, (
            "app.js must use namePrefix config or NAME_PREFIX environment variable"
        )
