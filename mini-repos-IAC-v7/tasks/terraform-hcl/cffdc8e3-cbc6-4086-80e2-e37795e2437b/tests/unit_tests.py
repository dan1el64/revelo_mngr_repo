import re

from tf_helpers import (
    assignment,
    block,
    combined_text,
    resource_blocks,
    variable_block,
    variable_names,
)


def test_only_expected_input_variables_exist():
    assert variable_names() == {
        "aws_region",
        "aws_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
    }

    aws_region = variable_block("aws_region")
    assert assignment(aws_region, "default") == '"us-east-1"'


def test_provider_uses_required_inputs_and_endpoint_routing():
    provider = block("provider", type_name="aws")

    assert "region" in provider and "var.aws_region" in provider
    assert "access_key" in provider and "var.aws_access_key_id" in provider
    assert "secret_key" in provider and "var.aws_secret_access_key" in provider
    assert "var.aws_endpoint" in provider, "aws_endpoint must be used by provider routing"
    assert "endpoints {" in provider, "provider must use service endpoint routing"
    assert re.search(r"^\s*endpoint\s*=", provider, re.MULTILINE) is None


def test_no_forbidden_uniqueness_or_protection_patterns_are_present():
    text = combined_text()

    forbidden_snippets = [
        "timestamp(",
        "uuid(",
        'resource "random_id"',
        'resource "random_string"',
        'resource "random_pet"',
        'resource "aws_nat_gateway"',
        "prevent_destroy = true",
        "deletion_protection = true",
        "delete_termination_protection = true",
        "retain_on_delete = true",
    ]

    for snippet in forbidden_snippets:
        assert snippet not in text, f"Forbidden pattern found: {snippet}"


def test_cloudwatch_log_groups_are_fixed_to_required_retention_without_kms():
    log_groups = resource_blocks("aws_cloudwatch_log_group")
    assert len(log_groups) == 4, "Prompt requires exactly four managed log groups"

    for log_group in log_groups:
        assert assignment(log_group, "retention_in_days") == "14"
        assert "kms_key_id" not in log_group

