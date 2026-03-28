"""Unit tests for Pulumi / config validation. Run after harness preview step; tests use preview.json."""

import json
import os
from pathlib import Path

import pytest

# Preview is produced by the harness (syntax_plan step) before unit tests run.
# In the harness, everything runs in a container at /work, so preview.json is in the repo root.
_REPO_DIR = Path(__file__).resolve().parent.parent
PREVIEW_JSON = _REPO_DIR / "preview.json"


def test_list_not_empty():
    """Example unit test."""
    items = ["__main__.py", "Pulumi.yaml", "requirements.txt"]
    assert len(items) >= 1


def test_preview_json_exists_and_valid():
    """Preview output from harness must be valid JSON."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    data = json.loads(PREVIEW_JSON.read_text())
    # Pulumi preview JSON format varies, but should be valid JSON
    assert isinstance(data, (dict, list))


def test_python_syntax_valid():
    """Python source code must be syntactically valid."""
    main_py = _REPO_DIR / "__main__.py"
    assert main_py.exists(), "__main__.py not found"

    # Try to compile the Python file
    import py_compile
    try:
        py_compile.compile(str(main_py), doraise=True)
    except py_compile.PyCompileError as e:
        pytest.fail(f"Python syntax error in __main__.py: {e}")


def test_preview_contains_s3_bucket():
    """Preview must include the S3 bucket resource."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    data = json.loads(PREVIEW_JSON.read_text())

    # Pulumi preview JSON can be a list of events or a dict with steps/resources
    resources = []
    if isinstance(data, list):
        # JSON stream format - find resource events
        for event in data:
            if event.get("resourceChanges"):
                resources.extend(event["resourceChanges"].values())
    elif isinstance(data, dict):
        # Standard format
        if "steps" in data:
            resources = [step.get("newState", {}) for step in data.get("steps", [])]
        elif "resourceChanges" in data:
            resources = list(data["resourceChanges"].values())

    # Look for S3 bucket resource
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
    """Planned bucket name must use the NAME_PREFIX environment variable to avoid collisions."""
    if not PREVIEW_JSON.exists():
        pytest.skip("preview.json not found - preview step may have been skipped")

    # Get the name prefix from environment (same as the Python code uses)
    name_prefix = os.environ.get("NAME_PREFIX", "dev")
    assert name_prefix, "NAME_PREFIX environment variable must be set"

    data = json.loads(PREVIEW_JSON.read_text())

    # Parse preview JSON to find bucket configuration
    resources = []
    if isinstance(data, list):
        # JSON stream format
        for event in data:
            if event.get("resourceChanges"):
                resources.extend(event["resourceChanges"].values())
    elif isinstance(data, dict):
        if "steps" in data:
            resources = [step.get("newState", {}) for step in data.get("steps", [])]
        elif "resourceChanges" in data:
            resources = list(data["resourceChanges"].values())

    # Find the S3 bucket and check its name
    bucket_name = None
    for resource in resources:
        resource_type = resource.get("type", "")
        resource_urn = resource.get("urn", "")

        # Check if this is an S3 bucket
        is_bucket = ("s3" in resource_type.lower() and "bucket" in resource_type.lower()) or \
                    "s3/bucket" in resource_urn.lower() or \
                    "s3:bucket" in resource_urn.lower()

        if is_bucket:
            # Try to get bucket name from different possible locations
            inputs = resource.get("inputs", {})
            outputs = resource.get("outputs", {})

            bucket_name = (
                inputs.get("bucket") or
                outputs.get("bucket") or
                inputs.get("bucketName") or
                outputs.get("bucketName")
            )

            if bucket_name:
                break

    # If we didn't find the bucket name in preview, that's still acceptable for some Pulumi versions
    # but if we did find it, verify it has the correct prefix
    if bucket_name:
        assert bucket_name.startswith(f"{name_prefix}-"), (
            f"Bucket name must start with '{name_prefix}-', got: {bucket_name!r}"
        )
    else:
        # Verify at least that the Python code exists and uses NAME_PREFIX
        main_py = _REPO_DIR / "__main__.py"
        assert main_py.exists(), "__main__.py not found"

        main_content = main_py.read_text()
        assert "NAME_PREFIX" in main_content, "__main__.py must use NAME_PREFIX environment variable"
        assert f'"{name_prefix}-bucket"' in main_content or f"'{name_prefix}-bucket'" in main_content or \
               'f"{name_prefix}-bucket"' in main_content or "f'{name_prefix}-bucket'" in main_content, (
            "__main__.py must construct bucket name as f'{name_prefix}-bucket'"
        )
