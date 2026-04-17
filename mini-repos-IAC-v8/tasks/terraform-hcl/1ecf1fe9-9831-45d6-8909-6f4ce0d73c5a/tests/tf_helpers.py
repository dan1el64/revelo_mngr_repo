from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = REPO_ROOT / "main.tf"


def read_main_tf() -> str:
    return MAIN_TF.read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def _load_json(filename: str) -> dict:
    path = REPO_ROOT / filename
    assert path.exists(), f"{filename} was not found. Generate it with terraform show -json before running this suite."
    return json.loads(path.read_text(encoding="utf-8"))


def _collect_module_resources(module: dict) -> list[dict]:
    resources = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        resources.extend(_collect_module_resources(child))
    return resources


def planned_resources() -> list[dict]:
    plan = _load_json("plan.json")
    return _collect_module_resources(plan["planned_values"]["root_module"])


def state_resources() -> list[dict]:
    state = _load_json("state.json")
    return _collect_module_resources(state["values"]["root_module"])


def resources_of_type(resources: list[dict], type_name: str, mode: str | None = None) -> list[dict]:
    matches = [resource for resource in resources if resource.get("type") == type_name]
    if mode is not None:
        matches = [resource for resource in matches if resource.get("mode") == mode]
    return matches


def one_block(value):
    if isinstance(value, list):
        assert len(value) == 1, f"Expected exactly one nested block, found {len(value)}"
        return value[0]
    return value


def parse_json_string(value: str) -> dict:
    return json.loads(value)


def policy_documents(resources: list[dict]) -> list[tuple[str, dict]]:
    documents = []
    for resource in resources:
        if resource.get("type") not in {
            "aws_iam_policy",
            "aws_iam_role_policy",
            "aws_sns_topic_policy",
            "aws_sqs_queue_policy",
        }:
            continue

        values = resource.get("values", {})
        policy = values.get("policy")
        if policy:
            documents.append((resource["address"], json.loads(policy)))

    return documents


def security_group_rules(resources: list[dict], security_group_id: str, direction: str) -> list[dict]:
    assert direction in {"ingress", "egress"}

    collected = []

    for resource in resources_of_type(resources, "aws_security_group"):
        values = resource.get("values", {})
        if values.get("id") != security_group_id:
            continue

        for rule in values.get(direction, []):
            collected.append(
                {
                    "from_port": rule.get("from_port"),
                    "to_port": rule.get("to_port"),
                    "protocol": rule.get("protocol"),
                    "cidr_ipv4": (rule.get("cidr_blocks") or [None])[0],
                    "source_security_group_id": (rule.get("security_groups") or [None])[0],
                }
            )

    standalone_type = f"aws_vpc_security_group_{direction}_rule"
    for resource in resources_of_type(resources, standalone_type):
        values = resource.get("values", {})
        if values.get("security_group_id") != security_group_id:
            continue

        collected.append(
            {
                "from_port": values.get("from_port"),
                "to_port": values.get("to_port"),
                "protocol": values.get("ip_protocol"),
                "cidr_ipv4": values.get("cidr_ipv4"),
                "source_security_group_id": values.get("referenced_security_group_id"),
            }
        )

    return collected
