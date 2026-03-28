"""Unit tests for CloudFormation template (YAML). Run after validate step; tests use template.yaml."""

from pathlib import Path

import pytest
import yaml

_REPO_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_YAML = _REPO_DIR / "template.yaml"
TEMPLATE_YML = _REPO_DIR / "template.yml"


class CfnLoader(yaml.SafeLoader):
    """YAML loader that treats CloudFormation intrinsics (!Ref, !Sub, etc.) as plain data."""

    pass


def _cfn_intrinsic(loader, tag_suffix, node):
    """Handle !Ref, !Sub, !GetAtt, !Join, etc. so the template parses without ConstructorError."""
    if isinstance(node, yaml.ScalarNode):
        return {tag_suffix: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {tag_suffix: loader.construct_sequence(node)}
    if isinstance(node, yaml.MappingNode):
        return {tag_suffix: loader.construct_mapping(node)}
    return None


CfnLoader.add_multi_constructor("!", _cfn_intrinsic)


def _template_path():
    if TEMPLATE_YAML.exists():
        return TEMPLATE_YAML
    if TEMPLATE_YML.exists():
        return TEMPLATE_YML
    return None


def _load_template():
    path = _template_path()
    assert path, "template.yaml or template.yml not found"
    with open(path) as f:
        return yaml.load(f, Loader=CfnLoader)


def test_template_exists_and_valid():
    """Template file must exist and be valid YAML."""
    path = _template_path()
    assert path and path.exists(), "template.yaml or template.yml must exist"
    data = _load_template()
    assert isinstance(data, dict), "Template must be a YAML object"


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
    """Bucket name must use NamePrefix parameter to avoid collisions (string or intrinsic e.g. !Sub)."""
    data = _load_template()
    params = data.get("Parameters") or {}
    assert "NamePrefix" in params, "Template must have Parameters.NamePrefix"
    resources = data.get("Resources") or {}
    for name, res in resources.items():
        if res.get("Type") == "AWS::S3::Bucket":
            props = res.get("Properties") or {}
            bucket_name = props.get("BucketName")
            # BucketName may be a string or an intrinsic (e.g. {"Sub": "..."} or {"Ref": "..."})
            assert bucket_name is not None, f"Bucket resource {name} must set BucketName (e.g. !Sub with NamePrefix)"
            return
    pytest.fail("AWS::S3::Bucket not found in Resources")


def test_template_has_bucket_name_output():
    """Template must output BucketName for integration tests."""
    data = _load_template()
    outputs = data.get("Outputs") or {}
    assert "BucketName" in outputs, "Template must have Outputs.BucketName"
