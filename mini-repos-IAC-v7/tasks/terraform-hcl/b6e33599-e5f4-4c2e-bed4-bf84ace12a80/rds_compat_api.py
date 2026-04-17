#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen
from xml.sax.saxutils import escape


STATE_PATH = Path("/tmp/saas_backend_rds_compat_state.json")
ACCOUNT_ID = "000000000000"
REGION = "us-east-1"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"subnet_groups": {}, "instances": {}, "tags": {}}


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _empty_state() -> dict:
    return {"subnet_groups": {}, "instances": {}, "tags": {}}


def _text(params: dict, key: str, default: str = "") -> str:
    values = params.get(key, [default])
    return values[0] if values else default


def _members(params: dict, prefix: str) -> list[str]:
    values = []
    index = 1
    while f"{prefix}.member.{index}" in params:
        values.append(_text(params, f"{prefix}.member.{index}"))
        index += 1
    return values


def _response(action: str, result: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<{action}Response xmlns="http://rds.amazonaws.com/doc/2014-10-31/">'
        f"<{action}Result>{result}</{action}Result>"
        "<ResponseMetadata><RequestId>compat-request</RequestId></ResponseMetadata>"
        f"</{action}Response>"
    ).encode("utf-8")


def _error(code: str, message: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<ErrorResponse>"
        f"<Error><Type>Sender</Type><Code>{escape(code)}</Code><Message>{escape(message)}</Message></Error>"
        "<RequestId>compat-request</RequestId>"
        "</ErrorResponse>"
    ).encode("utf-8")


def _subnet_group_body(item: dict) -> str:
    subnets = "".join(
        "<Subnet>"
        f"<SubnetIdentifier>{escape(subnet)}</SubnetIdentifier>"
        "<SubnetStatus>Active</SubnetStatus>"
        "<SubnetAvailabilityZone><Name>us-east-1a</Name></SubnetAvailabilityZone>"
        "</Subnet>"
        for subnet in item.get("subnets", [])
    )
    return (
        f"<DBSubnetGroupName>{escape(item['name'])}</DBSubnetGroupName>"
        "<DBSubnetGroupDescription>Managed by Terraform</DBSubnetGroupDescription>"
        "<VpcId>vpc-compat</VpcId>"
        "<SubnetGroupStatus>Complete</SubnetGroupStatus>"
        f"<Subnets>{subnets}</Subnets>"
        f"<DBSubnetGroupArn>arn:aws:rds:{REGION}:{ACCOUNT_ID}:subgrp:{escape(item['name'])}</DBSubnetGroupArn>"
    )


def _subnet_group_xml(item: dict) -> str:
    return f"<DBSubnetGroup>{_subnet_group_body(item)}</DBSubnetGroup>"


def _instance_xml(item: dict) -> str:
    security_groups = "".join(
        "<VpcSecurityGroupMembership>"
        f"<VpcSecurityGroupId>{escape(group)}</VpcSecurityGroupId>"
        "<Status>active</Status>"
        "</VpcSecurityGroupMembership>"
        for group in item.get("security_groups", [])
    )
    return (
        "<DBInstance>"
        f"<DBInstanceIdentifier>{escape(item['identifier'])}</DBInstanceIdentifier>"
        f"<DBInstanceClass>{escape(item['instance_class'])}</DBInstanceClass>"
        f"<Engine>{escape(item['engine'])}</Engine>"
        "<DBInstanceStatus>available</DBInstanceStatus>"
        f"<MasterUsername>{escape(item['username'])}</MasterUsername>"
        f"<Endpoint><Address>{escape(item['identifier'])}.compat.internal</Address><Port>5432</Port></Endpoint>"
        f"<AllocatedStorage>{item['allocated_storage']}</AllocatedStorage>"
        f"<InstanceCreateTime>{item['created_at']}</InstanceCreateTime>"
        "<BackupRetentionPeriod>0</BackupRetentionPeriod>"
        "<DBSecurityGroups/>"
        f"<VpcSecurityGroups>{security_groups}</VpcSecurityGroups>"
        "<DBParameterGroups><DBParameterGroup><DBParameterGroupName>default.postgres15</DBParameterGroupName><ParameterApplyStatus>in-sync</ParameterApplyStatus></DBParameterGroup></DBParameterGroups>"
        "<PendingModifiedValues/>"
        f"<DBSubnetGroup>{_subnet_group_body(item['subnet_group'])}</DBSubnetGroup>"
        "<PreferredMaintenanceWindow>sun:05:00-sun:06:00</PreferredMaintenanceWindow>"
        f"<LatestRestorableTime>{item['created_at']}</LatestRestorableTime>"
        "<MultiAZ>false</MultiAZ>"
        f"<EngineVersion>{escape(item['engine_version'])}</EngineVersion>"
        "<AutoMinorVersionUpgrade>true</AutoMinorVersionUpgrade>"
        "<PubliclyAccessible>false</PubliclyAccessible>"
        f"<StorageType>{escape(item['storage_type'])}</StorageType>"
        "<StorageEncrypted>true</StorageEncrypted>"
        "<DbiResourceId>db-compat</DbiResourceId>"
        f"<DBInstanceArn>arn:aws:rds:{REGION}:{ACCOUNT_ID}:db:{escape(item['identifier'])}</DBInstanceArn>"
        "<IAMDatabaseAuthenticationEnabled>false</IAMDatabaseAuthenticationEnabled>"
        "<PerformanceInsightsEnabled>false</PerformanceInsightsEnabled>"
        "<DeletionProtection>false</DeletionProtection>"
        "</DBInstance>"
    )


class Handler(BaseHTTPRequestHandler):
    server_version = "RdsCompatApi/1.0"

    def log_message(self, fmt: str, *args) -> None:
        return

    def _params(self) -> dict:
        body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
        params = parse_qs(urlparse(self.path).query)
        params.update(parse_qs(body.decode("utf-8")))
        return params

    def _send(self, status: int, body: bytes, content_type: str = "text/xml") -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_xml(self, action: str, result: str) -> None:
        self._send(200, _response(action, result))

    def _send_error(self, code: str, message: str, status: int = 404) -> None:
        self._send(status, _error(code, message))

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, b"ok", "text/plain")
            return
        self._handle(self._params())

    def do_POST(self) -> None:
        self._handle(self._params())

    def _handle(self, params: dict) -> None:
        action = _text(params, "Action")
        state = _load_state()

        if action == "CreateDBSubnetGroup":
            name = _text(params, "DBSubnetGroupName")
            item = {"name": name, "subnets": _members(params, "SubnetIds")}
            state["subnet_groups"][name] = item
            _save_state(state)
            self._send_xml(action, _subnet_group_xml(item))
            return

        if action == "DescribeDBSubnetGroups":
            name = _text(params, "DBSubnetGroupName")
            groups = state["subnet_groups"]
            if name:
                if name not in groups:
                    self._send_error("DBSubnetGroupNotFoundFault", f"DBSubnetGroup {name} not found")
                    return
                body = _subnet_group_xml(groups[name])
            else:
                body = "".join(_subnet_group_xml(item) for item in groups.values())
            self._send_xml(action, f"<DBSubnetGroups>{body}</DBSubnetGroups>")
            return

        if action == "DeleteDBSubnetGroup":
            state["subnet_groups"].pop(_text(params, "DBSubnetGroupName"), None)
            _save_state(state)
            self._send_xml(action, "")
            return

        if action == "CreateDBInstance":
            identifier = _text(params, "DBInstanceIdentifier")
            subnet_group_name = _text(params, "DBSubnetGroupName")
            subnet_group = state["subnet_groups"].get(subnet_group_name, {"name": subnet_group_name, "subnets": []})
            item = {
                "identifier": identifier,
                "instance_class": _text(params, "DBInstanceClass", "db.t3.micro"),
                "engine": _text(params, "Engine", "postgres"),
                "engine_version": _text(params, "EngineVersion", "15.5"),
                "username": _text(params, "MasterUsername", "appuser"),
                "allocated_storage": int(_text(params, "AllocatedStorage", "20")),
                "storage_type": _text(params, "StorageType", "gp3"),
                "security_groups": _members(params, "VpcSecurityGroupIds"),
                "subnet_group": subnet_group,
                "created_at": _now(),
            }
            state["instances"][identifier] = item
            _save_state(state)
            self._send_xml(action, _instance_xml(item))
            return

        if action == "DescribeDBInstances":
            identifier = _text(params, "DBInstanceIdentifier")
            instances = state["instances"]
            if identifier:
                if identifier not in instances:
                    if len(instances) != 1:
                        self._send_error("DBInstanceNotFound", f"DBInstance {identifier} not found")
                        return
                    body = _instance_xml(next(iter(instances.values())))
                else:
                    body = _instance_xml(instances[identifier])
            else:
                body = "".join(_instance_xml(item) for item in instances.values())
            self._send_xml(action, f"<DBInstances>{body}</DBInstances>")
            return

        if action == "DeleteDBInstance":
            state["instances"].pop(_text(params, "DBInstanceIdentifier"), None)
            _save_state(state)
            self._send_xml(action, "")
            return

        if action in {"AddTagsToResource", "RemoveTagsFromResource"}:
            self._send_xml(action, "")
            return

        if action == "ListTagsForResource":
            self._send_xml(action, "<TagList/>")
            return

        self._send_error("InvalidAction", f"Unsupported action {action}", 400)


def serve(port: int) -> None:
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def start(port: int) -> None:
    health_url = f"http://127.0.0.1:{port}/health"
    _save_state(_empty_state())
    try:
        urlopen(health_url, timeout=1).read()
        return
    except Exception:
        pass

    subprocess.Popen(
        [sys.executable, __file__, "serve", str(port)],
        stdout=open("/tmp/saas_backend_rds_compat.log", "ab"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            urlopen(health_url, timeout=1).read()
            return
        except Exception:
            time.sleep(0.2)
    raise SystemExit("RDS compatibility API did not start")


if __name__ == "__main__":
    mode = sys.argv[1]
    port = int(sys.argv[2])
    if mode == "serve":
        serve(port)
    elif mode == "start":
        start(port)
    else:
        raise SystemExit(f"Unknown mode: {mode}")
