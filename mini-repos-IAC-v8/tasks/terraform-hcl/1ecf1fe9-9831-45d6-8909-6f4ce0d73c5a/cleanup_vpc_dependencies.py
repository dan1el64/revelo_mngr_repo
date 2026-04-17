#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


def _client(config: dict):
    return boto3.client(
        "ec2",
        endpoint_url=config["endpoint"],
        region_name=config["region"],
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}),
    )


def _safe(call, *args, **kwargs):
    try:
        return call(*args, **kwargs)
    except (ClientError, EndpointConnectionError):
        return None


def _vpc_endpoint_ids(ec2, vpc_id: str) -> list[str]:
    response = _safe(
        ec2.describe_vpc_endpoints,
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
    )
    if not response:
        return []
    return [endpoint["VpcEndpointId"] for endpoint in response.get("VpcEndpoints", [])]


def _network_interfaces(ec2, vpc_id: str) -> list[dict]:
    response = _safe(
        ec2.describe_network_interfaces,
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
    )
    if not response:
        return []
    return response.get("NetworkInterfaces", [])


def _internet_gateways(ec2, vpc_id: str) -> list[dict]:
    response = _safe(
        ec2.describe_internet_gateways,
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}],
    )
    if not response:
        return []
    return response.get("InternetGateways", [])


def _delete_network_interfaces(ec2, vpc_id: str) -> None:
    for interface in _network_interfaces(ec2, vpc_id):
        attachment = interface.get("Attachment") or {}
        attachment_id = attachment.get("AttachmentId")
        if attachment_id:
            _safe(ec2.detach_network_interface, AttachmentId=attachment_id, Force=True)
        _safe(ec2.delete_network_interface, NetworkInterfaceId=interface["NetworkInterfaceId"])


def _delete_vpc_endpoints(ec2, vpc_id: str) -> None:
    endpoint_ids = _vpc_endpoint_ids(ec2, vpc_id)
    if endpoint_ids:
        _safe(ec2.delete_vpc_endpoints, VpcEndpointIds=endpoint_ids)


def _delete_internet_gateways(ec2, vpc_id: str) -> None:
    for gateway in _internet_gateways(ec2, vpc_id):
        gateway_id = gateway["InternetGatewayId"]
        _safe(ec2.detach_internet_gateway, InternetGatewayId=gateway_id, VpcId=vpc_id)
        _safe(ec2.delete_internet_gateway, InternetGatewayId=gateway_id)


def _delete_leftovers(config: dict) -> None:
    ec2 = _client(config)
    vpc_id = config["vpc_id"]

    for _ in range(5):
        _delete_vpc_endpoints(ec2, vpc_id)
        _delete_network_interfaces(ec2, vpc_id)
        _delete_internet_gateways(ec2, vpc_id)
        _safe(ec2.delete_vpc, VpcId=vpc_id)
        time.sleep(0.5)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: cleanup_vpc_dependencies.py '<json-config>'", file=sys.stderr)
        return 2

    config = json.loads(sys.argv[1])
    _delete_leftovers(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
