#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from xml.sax.saxutils import escape


STATE_PATH = Path("/tmp/aws_compat_api_state.json")
ACCOUNT_ID = "000000000000"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _timestamp() -> float:
    return time.time()


def _load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"rds": {}, "redshift": {}, "glue": {}, "pipes": {}}


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _text(params: dict, key: str, default: str = "") -> str:
    value = params.get(key, [default])
    if isinstance(value, list):
        return value[0] if value else default
    return value or default


def _members(params: dict, prefix: str) -> list[str]:
    values = []
    index = 1
    while True:
        key = f"{prefix}.member.{index}"
        if key not in params:
            return values
        values.append(_text(params, key))
        index += 1


def _xml_response(action: str, result: str) -> bytes:
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<{action}Response xmlns=\"http://amazonaws.com/doc/2012-12-01/\">"
        f"<{action}Result>{result}</{action}Result>"
        f"<ResponseMetadata><RequestId>compat-request</RequestId></ResponseMetadata>"
        f"</{action}Response>"
    ).encode("utf-8")


def _xml_error(code: str, message: str, status: int = 404) -> tuple[int, bytes]:
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<ErrorResponse><Error><Type>Sender</Type><Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message></Error>"
        f"<RequestId>compat-request</RequestId></ErrorResponse>"
    )
    return status, body.encode("utf-8")


def _rds_instance_xml(item: dict) -> str:
    sg_items = "".join(
        f"<VpcSecurityGroupMembership><VpcSecurityGroupId>{escape(sg)}</VpcSecurityGroupId>"
        f"<Status>active</Status></VpcSecurityGroupMembership>"
        for sg in item.get("security_groups", [])
    )
    subnet_items = "".join(
        f"<Subnet><SubnetIdentifier>{escape(subnet)}</SubnetIdentifier>"
        f"<SubnetStatus>Active</SubnetStatus><SubnetAvailabilityZone><Name>us-east-1a</Name>"
        f"</SubnetAvailabilityZone></Subnet>"
        for subnet in item.get("subnets", [])
    )
    return f"""
      <DBInstance>
        <DBInstanceIdentifier>{escape(item["identifier"])}</DBInstanceIdentifier>
        <DBInstanceClass>{escape(item["instance_class"])}</DBInstanceClass>
        <Engine>{escape(item["engine"])}</Engine>
        <DBInstanceStatus>available</DBInstanceStatus>
        <MasterUsername>{escape(item["username"])}</MasterUsername>
        <Endpoint><Address>{escape(item["address"])}</Address><Port>5432</Port><HostedZoneId>ZCOMPAT</HostedZoneId></Endpoint>
        <AllocatedStorage>{item["allocated_storage"]}</AllocatedStorage>
        <InstanceCreateTime>{item["created_at"]}</InstanceCreateTime>
        <PreferredBackupWindow>03:00-04:00</PreferredBackupWindow>
        <BackupRetentionPeriod>0</BackupRetentionPeriod>
        <DBSecurityGroups/>
        <VpcSecurityGroups>{sg_items}</VpcSecurityGroups>
        <DBParameterGroups><DBParameterGroup><DBParameterGroupName>default.postgres16</DBParameterGroupName><ParameterApplyStatus>in-sync</ParameterApplyStatus></DBParameterGroup></DBParameterGroups>
        <AvailabilityZone>us-east-1a</AvailabilityZone>
        <DBSubnetGroup><DBSubnetGroupName>{escape(item["subnet_group"])}</DBSubnetGroupName><DBSubnetGroupDescription>Managed by Terraform</DBSubnetGroupDescription><VpcId>vpc-compat</VpcId><SubnetGroupStatus>Complete</SubnetGroupStatus><Subnets>{subnet_items}</Subnets></DBSubnetGroup>
        <PreferredMaintenanceWindow>sun:05:00-sun:06:00</PreferredMaintenanceWindow>
        <PendingModifiedValues/>
        <LatestRestorableTime>{item["created_at"]}</LatestRestorableTime>
        <MultiAZ>false</MultiAZ>
        <EngineVersion>{escape(item["engine_version"])}</EngineVersion>
        <AutoMinorVersionUpgrade>true</AutoMinorVersionUpgrade>
        <PubliclyAccessible>false</PubliclyAccessible>
        <StorageType>gp2</StorageType>
        <DbInstancePort>0</DbInstancePort>
        <StorageEncrypted>true</StorageEncrypted>
        <DbiResourceId>db-compat</DbiResourceId>
        <CACertificateIdentifier>rds-ca-rsa2048-g1</CACertificateIdentifier>
        <CopyTagsToSnapshot>false</CopyTagsToSnapshot>
        <MonitoringInterval>0</MonitoringInterval>
        <DBInstanceArn>arn:aws:rds:us-east-1:000000000000:db:{escape(item["identifier"])}</DBInstanceArn>
        <IAMDatabaseAuthenticationEnabled>false</IAMDatabaseAuthenticationEnabled>
        <PerformanceInsightsEnabled>false</PerformanceInsightsEnabled>
        <DeletionProtection>false</DeletionProtection>
      </DBInstance>
    """


def _rds_subnet_group_xml(item: dict) -> str:
    subnet_items = "".join(
        f"<Subnet><SubnetIdentifier>{escape(subnet)}</SubnetIdentifier>"
        f"<SubnetStatus>Active</SubnetStatus><SubnetAvailabilityZone><Name>us-east-1a</Name>"
        f"</SubnetAvailabilityZone></Subnet>"
        for subnet in item.get("subnets", [])
    )
    return (
        f"<DBSubnetGroup><DBSubnetGroupName>{escape(item['name'])}</DBSubnetGroupName>"
        f"<DBSubnetGroupDescription>Managed by Terraform</DBSubnetGroupDescription>"
        f"<VpcId>vpc-compat</VpcId><SubnetGroupStatus>Complete</SubnetGroupStatus>"
        f"<Subnets>{subnet_items}</Subnets></DBSubnetGroup>"
    )


def _redshift_cluster_xml(item: dict) -> str:
    sg_items = "".join(
        f"<VpcSecurityGroup><VpcSecurityGroupId>{escape(sg)}</VpcSecurityGroupId><Status>active</Status></VpcSecurityGroup>"
        for sg in item.get("security_groups", [])
    )
    return f"""
      <Cluster>
        <ClusterIdentifier>{escape(item["identifier"])}</ClusterIdentifier>
        <ClusterType>single-node</ClusterType>
        <NodeType>{escape(item["node_type"])}</NodeType>
        <ClusterStatus>available</ClusterStatus>
        <ClusterAvailabilityStatus>Available</ClusterAvailabilityStatus>
        <MasterUsername>{escape(item["username"])}</MasterUsername>
        <DBName>{escape(item["database"])}</DBName>
        <Endpoint><Address>{escape(item["address"])}</Address><Port>5439</Port></Endpoint>
        <ClusterCreateTime>{item["created_at"]}</ClusterCreateTime>
        <AutomatedSnapshotRetentionPeriod>1</AutomatedSnapshotRetentionPeriod>
        <ClusterSecurityGroups/>
        <VpcSecurityGroups>{sg_items}</VpcSecurityGroups>
        <ClusterParameterGroups>
          <ClusterParameterGroup>
            <ParameterGroupName>default.redshift-1.0</ParameterGroupName>
            <ParameterApplyStatus>in-sync</ParameterApplyStatus>
          </ClusterParameterGroup>
        </ClusterParameterGroups>
        <ClusterNodes>
          <ClusterNode>
            <NodeRole>SHARED</NodeRole>
            <PrivateIPAddress>10.0.10.10</PrivateIPAddress>
          </ClusterNode>
        </ClusterNodes>
        <ClusterSubnetGroupName>{escape(item["subnet_group"])}</ClusterSubnetGroupName>
        <VpcId>vpc-compat</VpcId>
        <AvailabilityZone>us-east-1a</AvailabilityZone>
        <PreferredMaintenanceWindow>sun:05:00-sun:06:00</PreferredMaintenanceWindow>
        <PendingModifiedValues/>
        <ClusterVersion>1.0</ClusterVersion>
        <AllowVersionUpgrade>true</AllowVersionUpgrade>
        <NumberOfNodes>1</NumberOfNodes>
        <MultiAZ>Disabled</MultiAZ>
        <PubliclyAccessible>false</PubliclyAccessible>
        <Encrypted>true</Encrypted>
        <EnhancedVpcRouting>false</EnhancedVpcRouting>
        <MaintenanceTrackName>current</MaintenanceTrackName>
        <ManualSnapshotRetentionPeriod>-1</ManualSnapshotRetentionPeriod>
        <AvailabilityZoneRelocationStatus>disabled</AvailabilityZoneRelocationStatus>
        <ClusterNamespaceArn>arn:aws:redshift:us-east-1:000000000000:namespace:{escape(item["identifier"])}</ClusterNamespaceArn>
        <AquaConfiguration><AquaStatus>disabled</AquaStatus><AquaConfigurationStatus>auto</AquaConfigurationStatus></AquaConfiguration>
      </Cluster>
    """


def _redshift_subnet_group_xml(item: dict) -> str:
    subnet_items = "".join(
        f"<Subnet><SubnetIdentifier>{escape(subnet)}</SubnetIdentifier>"
        f"<SubnetAvailabilityZone><Name>us-east-1a</Name></SubnetAvailabilityZone>"
        f"<SubnetStatus>Active</SubnetStatus></Subnet>"
        for subnet in item.get("subnets", [])
    )
    return (
        f"<ClusterSubnetGroup><ClusterSubnetGroupName>{escape(item['name'])}</ClusterSubnetGroupName>"
        f"<Description>Managed by Terraform</Description><VpcId>vpc-compat</VpcId>"
        f"<SubnetGroupStatus>Complete</SubnetGroupStatus><Subnets>{subnet_items}</Subnets>"
        f"</ClusterSubnetGroup>"
    )


class Handler(BaseHTTPRequestHandler):
    server_version = "AwsCompatApi/1.0"

    def log_message(self, fmt: str, *args) -> None:
        return

    def _body(self) -> bytes:
        length = int(self.headers.get("content-length", "0") or "0")
        return self.rfile.read(length) if length else b""

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data: dict, status: int = 200) -> None:
        self._send(status, json.dumps(data, default=str).encode("utf-8"), "application/x-amz-json-1.1")

    def _send_json_not_found(self, resource: str, name: str) -> None:
        self._send_json(
            {
                "__type": "EntityNotFoundException",
                "Message": f"{resource} {name} not found",
            },
            400,
        )

    def _send_xml(self, action: str, result: str, status: int = 200) -> None:
        self._send(status, _xml_response(action, result), "text/xml")

    def _send_xml_error(self, code: str, message: str, status: int = 404) -> None:
        error_status, body = _xml_error(code, message, status)
        self._send(error_status, body, "text/xml")

    def _query_params(self) -> dict:
        body = self._body().decode("utf-8")
        query = urlparse(self.path).query
        params = parse_qs("&".join(part for part in (query, body) if part))
        return params

    def do_POST(self) -> None:
        target = self.headers.get("x-amz-target", "")
        if target:
            payload = json.loads(self._body().decode("utf-8") or "{}")
            service_action = target.split(".")[-1]
            if "Glue" in target:
                self._handle_glue(service_action, payload)
                return
            if "Pipes" in target or self.path.startswith("/v1/pipes"):
                self._handle_pipe_json(service_action, payload)
                return

        if self.path.startswith("/v1/pipes"):
            self._handle_pipe_json("CreatePipe", json.loads(self._body().decode("utf-8") or "{}"))
            return

        params = self._query_params()
        action = _text(params, "Action")
        if action.startswith("CreateDB") or action.startswith("DescribeDB") or action.startswith("DeleteDB") or action in {
            "AddTagsToResource",
            "ListTagsForResource",
            "RemoveTagsFromResource",
        }:
            self._handle_rds(action, params)
            return
        if "Cluster" in action or action in {"CreateTags", "DeleteTags", "DescribeTags"}:
            self._handle_redshift(action, params)
            return
        self._send_json({})

    def do_GET(self) -> None:
        if self.path.startswith("/tags/"):
            self._send_json({"Tags": {}})
            return
        if self.path.startswith("/v1/pipes/"):
            name = self.path.rsplit("/", 1)[-1]
            self._describe_pipe(name)
            return
        params = self._query_params()
        action = _text(params, "Action")
        if action.startswith("DescribeDB"):
            self._handle_rds(action, params)
            return
        if action.startswith("Describe"):
            self._handle_redshift(action, params)
            return
        self._send_json({})

    def do_DELETE(self) -> None:
        if self.path.startswith("/tags/"):
            self._send_json({})
            return
        if self.path.startswith("/v1/pipes/"):
            name = self.path.rsplit("/", 1)[-1]
            state = _load_state()
            pipe = state["pipes"].setdefault(
                name,
                {
                    "name": name,
                    "arn": f"arn:aws:pipes:us-east-1:{ACCOUNT_ID}:pipe/{name}",
                    "created_at": _timestamp(),
                },
            )
            pipe["current_state"] = "DELETING"
            pipe["desired_state"] = "STOPPED"
            pipe["last_modified_at"] = _timestamp()
            _save_state(state)
            self._send_json({"Arn": pipe.get("arn"), "Name": name})
            return
        self._send_json({})

    def _handle_rds(self, action: str, params: dict) -> None:
        state = _load_state()
        if action == "CreateDBSubnetGroup":
            name = _text(params, "DBSubnetGroupName")
            item = {
                "name": name,
                "subnets": _members(params, "SubnetIds"),
            }
            state["rds"].setdefault("subnet_groups", {})[name] = item
            _save_state(state)
            self._send_xml(action, _rds_subnet_group_xml(item))
            return
        if action == "DescribeDBSubnetGroups":
            groups = state["rds"].setdefault("subnet_groups", {})
            name = _text(params, "DBSubnetGroupName")
            if name:
                item = groups.get(name)
                if not item:
                    self._send_xml_error("DBSubnetGroupNotFoundFault", f"DBSubnetGroup {name} not found")
                    return
                body = _rds_subnet_group_xml(item)
            else:
                body = "".join(_rds_subnet_group_xml(item) for item in groups.values())
            self._send_xml(action, f"<DBSubnetGroups>{body}</DBSubnetGroups>")
            return
        if action == "DeleteDBSubnetGroup":
            state["rds"].setdefault("subnet_groups", {}).pop(_text(params, "DBSubnetGroupName"), None)
            _save_state(state)
            self._send_xml(action, "")
            return
        if action == "CreateDBInstance":
            identifier = _text(params, "DBInstanceIdentifier")
            item = {
                "identifier": identifier,
                "allocated_storage": int(_text(params, "AllocatedStorage", "20")),
                "engine": _text(params, "Engine", "postgres"),
                "engine_version": _text(params, "EngineVersion", "16.3"),
                "instance_class": _text(params, "DBInstanceClass", "db.t3.micro"),
                "subnet_group": _text(params, "DBSubnetGroupName"),
                "security_groups": _members(params, "VpcSecurityGroupIds"),
                "username": _text(params, "MasterUsername", "orders_admin"),
                "address": f"{identifier}.compat.internal",
                "created_at": _now(),
                "subnets": [],
            }
            item["subnets"] = state["rds"].setdefault("subnet_groups", {}).get(item["subnet_group"], {}).get("subnets", [])
            state["rds"].setdefault("instances", {})[identifier] = item
            _save_state(state)
            self._send_xml(action, _rds_instance_xml(item))
            return
        if action == "DescribeDBInstances":
            instances = state["rds"].setdefault("instances", {})
            identifier = _text(params, "DBInstanceIdentifier")
            if identifier:
                item = instances.get(identifier)
                if not item:
                    self._send_xml_error("DBInstanceNotFound", f"DBInstance {identifier} not found")
                    return
                body = _rds_instance_xml(item)
            else:
                body = "".join(_rds_instance_xml(item) for item in instances.values())
            self._send_xml(action, f"<DBInstances>{body}</DBInstances>")
            return
        if action == "DeleteDBInstance":
            state["rds"].setdefault("instances", {}).pop(_text(params, "DBInstanceIdentifier"), None)
            _save_state(state)
            self._send_xml(action, "")
            return
        if action == "ListTagsForResource":
            self._send_xml(action, "<TagList/>")
            return
        if action in {"AddTagsToResource", "RemoveTagsFromResource"}:
            self._send_xml(action, "")
            return
        self._send_xml(action, "")

    def _handle_redshift(self, action: str, params: dict) -> None:
        state = _load_state()
        if action == "CreateClusterSubnetGroup":
            name = _text(params, "ClusterSubnetGroupName")
            item = {
                "name": name,
                "subnets": _members(params, "SubnetIds"),
            }
            state["redshift"].setdefault("subnet_groups", {})[name] = item
            _save_state(state)
            self._send_xml(action, _redshift_subnet_group_xml(item))
            return
        if action == "DescribeClusterSubnetGroups":
            groups = state["redshift"].setdefault("subnet_groups", {})
            name = _text(params, "ClusterSubnetGroupName")
            if name:
                item = groups.get(name)
                if not item:
                    self._send_xml_error("ClusterSubnetGroupNotFound", f"ClusterSubnetGroup {name} not found")
                    return
                body = _redshift_subnet_group_xml(item)
            else:
                body = "".join(_redshift_subnet_group_xml(item) for item in groups.values())
            self._send_xml(action, f"<ClusterSubnetGroups>{body}</ClusterSubnetGroups>")
            return
        if action == "DeleteClusterSubnetGroup":
            state["redshift"].setdefault("subnet_groups", {}).pop(_text(params, "ClusterSubnetGroupName"), None)
            _save_state(state)
            self._send_xml(action, "")
            return
        if action == "CreateCluster":
            identifier = _text(params, "ClusterIdentifier")
            item = {
                "identifier": identifier,
                "node_type": _text(params, "NodeType", "dc2.large"),
                "subnet_group": _text(params, "ClusterSubnetGroupName"),
                "security_groups": _members(params, "VpcSecurityGroupIds"),
                "username": _text(params, "MasterUsername", "analytics_admin"),
                "database": _text(params, "DBName", "analytics"),
                "address": f"{identifier}.compat.internal",
                "created_at": _now(),
            }
            state["redshift"].setdefault("clusters", {})[identifier] = item
            _save_state(state)
            self._send_xml(action, _redshift_cluster_xml(item))
            return
        if action == "DescribeClusters":
            clusters = state["redshift"].setdefault("clusters", {})
            identifier = _text(params, "ClusterIdentifier")
            if identifier:
                item = clusters.get(identifier)
                if not item:
                    self._send_xml_error("ClusterNotFound", f"Cluster {identifier} not found")
                    return
                body = _redshift_cluster_xml(item)
            else:
                body = "".join(_redshift_cluster_xml(item) for item in clusters.values())
            self._send_xml(action, f"<Clusters>{body}</Clusters>")
            return
        if action == "DeleteCluster":
            state["redshift"].setdefault("clusters", {}).pop(_text(params, "ClusterIdentifier"), None)
            _save_state(state)
            self._send_xml(action, "")
            return
        if action == "DescribeTags":
            self._send_xml(action, "<TaggedResources/>")
            return
        if action in {"CreateTags", "DeleteTags"}:
            self._send_xml(action, "")
            return
        self._send_xml(action, "")

    def _handle_glue(self, action: str, payload: dict) -> None:
        state = _load_state()
        glue = state.setdefault("glue", {})
        if action == "CreateDatabase":
            database = payload.get("DatabaseInput", {})
            glue.setdefault("databases", {})[database["Name"]] = {
                "CatalogId": payload.get("CatalogId", ACCOUNT_ID),
                "Name": database["Name"],
            }
            _save_state(state)
            self._send_json({})
            return
        if action == "GetDatabase":
            name = payload["Name"]
            database = glue.setdefault("databases", {}).get(name)
            if not database:
                self._send_json_not_found("Database", name)
                return
            self._send_json({"Database": database})
            return
        if action == "DeleteDatabase":
            glue.setdefault("databases", {}).pop(payload["Name"], None)
            _save_state(state)
            self._send_json({})
            return
        if action == "CreateConnection":
            connection = payload.get("ConnectionInput", {})
            connection["CatalogId"] = payload.get("CatalogId", ACCOUNT_ID)
            glue.setdefault("connections", {})[connection["Name"]] = connection
            _save_state(state)
            self._send_json({})
            return
        if action == "GetConnection":
            name = payload["Name"]
            connection = glue.setdefault("connections", {}).get(name)
            if not connection:
                self._send_json_not_found("Connection", name)
                return
            self._send_json({"Connection": connection})
            return
        if action == "DeleteConnection":
            glue.setdefault("connections", {}).pop(payload["ConnectionName"], None)
            _save_state(state)
            self._send_json({})
            return
        if action == "CreateCrawler":
            crawler = {
                "Name": payload["Name"],
                "Role": payload["Role"],
                "DatabaseName": payload["DatabaseName"],
                "Targets": payload.get("Targets", {}),
                "State": "READY",
            }
            glue.setdefault("crawlers", {})[crawler["Name"]] = crawler
            _save_state(state)
            self._send_json({})
            return
        if action == "GetCrawler":
            name = payload["Name"]
            crawler = glue.setdefault("crawlers", {}).get(name)
            if not crawler:
                self._send_json_not_found("Crawler", name)
                return
            self._send_json({"Crawler": crawler})
            return
        if action == "DeleteCrawler":
            glue.setdefault("crawlers", {}).pop(payload["Name"], None)
            _save_state(state)
            self._send_json({})
            return
        if action == "GetTags":
            self._send_json({"Tags": {}})
            return
        if action in {"TagResource", "UntagResource"}:
            self._send_json({})
            return
        self._send_json({})

    def _handle_pipe_json(self, action: str, payload: dict) -> None:
        if action == "ListTagsForResource":
            self._send_json({"Tags": {}})
            return
        if action in {"TagResource", "UntagResource"}:
            self._send_json({})
            return
        if action in {"CreatePipe", "Create"}:
            state = _load_state()
            name = payload.get("Name") or payload.get("name") or self.path.rstrip("/").rsplit("/", 1)[-1]
            pipe = {
                "name": name,
                "arn": f"arn:aws:pipes:us-east-1:{ACCOUNT_ID}:pipe/{name}",
                "role_arn": payload.get("RoleArn"),
                "source": payload.get("Source"),
                "target": payload.get("Target"),
                "enrichment": payload.get("Enrichment"),
                "source_parameters": payload.get("SourceParameters", {}),
                "target_parameters": payload.get("TargetParameters", {}),
                "created_at": _timestamp(),
                "last_modified_at": _timestamp(),
                "current_state": "RUNNING",
                "desired_state": "RUNNING",
            }
            state["pipes"][name] = pipe
            _save_state(state)
            self._send_json({"Arn": pipe["arn"], "CreationTime": pipe["created_at"], "CurrentState": "RUNNING", "DesiredState": "RUNNING", "Name": name})
            return
        if action in {"DescribePipe", "Describe"}:
            self._describe_pipe(payload.get("Name") or payload.get("name"))
            return
        self._send_json({})

    def _describe_pipe(self, name: str) -> None:
        pipe = _load_state().setdefault("pipes", {}).get(name)
        if not pipe:
            self._send_json({"__type": "ResourceNotFoundException", "message": f"Pipe {name} not found"}, 404)
            return
        created_at = pipe.get("created_at")
        if not isinstance(created_at, (int, float)):
            created_at = _timestamp()
        self._send_json({
            "Arn": pipe["arn"],
            "CreationTime": created_at,
            "CurrentState": pipe.get("current_state", "RUNNING"),
            "DesiredState": pipe.get("desired_state", "RUNNING"),
            "Enrichment": pipe.get("enrichment"),
            "LastModifiedTime": pipe.get("last_modified_at", created_at),
            "Name": pipe["name"],
            "RoleArn": pipe.get("role_arn"),
            "Source": pipe.get("source"),
            "SourceParameters": pipe.get("source_parameters", {}),
            "Target": pipe.get("target"),
            "TargetParameters": pipe.get("target_parameters", {}),
        })


def _queue_name_from_arn(arn: str) -> str:
    return arn.rsplit(":", 1)[-1]


def _poll_pipes(gateway_endpoint: str) -> None:
    try:
        import boto3
    except Exception:
        return

    clients = {
        "sqs": boto3.client("sqs", endpoint_url=gateway_endpoint, region_name="us-east-1"),
        "sfn": boto3.client("stepfunctions", endpoint_url=gateway_endpoint, region_name="us-east-1"),
    }
    while True:
        try:
            state = _load_state()
            for pipe in list(state.get("pipes", {}).values()):
                source = pipe.get("source")
                target = pipe.get("target")
                if not source or not target:
                    continue
                queue_url = clients["sqs"].get_queue_url(QueueName=_queue_name_from_arn(source))["QueueUrl"]
                messages = clients["sqs"].receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1).get("Messages", [])
                for message in messages:
                    clients["sfn"].start_execution(stateMachineArn=target, input=message.get("Body", "{}"))
                    clients["sqs"].delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        except Exception:
            time.sleep(1)
        time.sleep(1)


def serve(port: int, gateway_endpoint: str) -> None:
    threading.Thread(target=_poll_pipes, args=(gateway_endpoint,), daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def start(port: int, gateway_endpoint: str) -> None:
    try:
        import urllib.request

        urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1)
        return
    except Exception:
        pass
    subprocess.Popen(
        [sys.executable, __file__, "serve", str(port), gateway_endpoint],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            import urllib.request

            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1)
            return
        except Exception:
            time.sleep(0.2)
    raise SystemExit("compatibility API did not start")


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "start":
        start(int(sys.argv[2]), sys.argv[3])
    elif command == "serve":
        serve(int(sys.argv[2]), sys.argv[3])
