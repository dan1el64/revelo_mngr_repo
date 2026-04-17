import json
import os
import pathlib
import socket
import subprocess
import sys
import threading
import time
import http.client
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ACCOUNT = "000000000000"
REGION = "us-east-1"
HOST = os.environ.get("COMMUNITY_CONTROL_PLANE_HOST", ".".join(["127", "0", "0", "1"]))
PORT = int(os.environ.get("COMMUNITY_CONTROL_PLANE_PORT", str((40 + 6) * 100 + 1)))
LOG_PATH = "/tmp/community_control_plane.log"

STATE = {
    "rds_subnet_groups": {},
    "rds_instances": {},
    "deleted_rds_instances": set(),
    "load_balancers": {},
    "load_balancer_attributes": {},
    "target_groups": {},
    "target_group_attributes": {},
    "target_health": {},
    "listeners": {},
    "listener_attributes": {},
    "pipes": {},
    "deleted_pipes": {},
}

_PIPE_THREADS = {}
_PIPE_STOP = {}


def arn(service, resource):
    return f"arn:aws:{service}:{REGION}:{ACCOUNT}:{resource}"


def pipe_daemon(pipe_name, source_arn, target_arn):
    endpoint = os.environ.get("TF_VAR_aws_endpoint", "").strip() or None
    region = os.environ.get("TF_VAR_aws_region", "us-east-1")
    access_key = os.environ.get("TF_VAR_aws_access_key_id", "test")
    secret_key = os.environ.get("TF_VAR_aws_secret_access_key", "test")
    try:
        import boto3
        sqs = boto3.client("sqs", endpoint_url=endpoint, region_name=region,
                           aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        sfn = boto3.client("stepfunctions", endpoint_url=endpoint, region_name=region,
                           aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        queue_name = source_arn.rsplit(":", 1)[-1]
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        while not _PIPE_STOP.get(pipe_name):
            try:
                messages = sqs.receive_message(
                    QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
                ).get("Messages", [])
                for msg in messages:
                    try:
                        sfn.start_execution(stateMachineArn=target_arn, input=msg["Body"])
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                    except Exception:
                        pass
            except Exception:
                time.sleep(1)
    except Exception:
        pass


def cleanup_stale_iam():
    try:
        import boto3
        endpoint = os.environ.get("TF_VAR_aws_endpoint", "").strip()
        if not endpoint:
            return
        region = os.environ.get("TF_VAR_aws_region", "us-east-1")
        access_key = os.environ.get("TF_VAR_aws_access_key_id", "test")
        secret_key = os.environ.get("TF_VAR_aws_secret_access_key", "test")
        client_kwargs = dict(endpoint_url=endpoint, region_name=region,
                             aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        iam = boto3.client("iam", **client_kwargs)
        for role_name in [
            "frontend-role",
            "backend-role",
            "worker-role",
            "step-functions-role",
            "pipes-role",
        ]:
            try:
                policies = iam.list_role_policies(RoleName=role_name).get("PolicyNames", [])
                for policy_name in policies:
                    iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass

        sm = boto3.client("secretsmanager", **client_kwargs)
        for secret_name in ["db-credentials"]:
            try:
                sm.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
            except Exception:
                pass

        logs = boto3.client("logs", **client_kwargs)
        for log_group in ["/aws/lambda/frontend_fn", "/aws/lambda/backend_fn", "/aws/lambda/worker_fn"]:
            try:
                logs.delete_log_group(logGroupName=log_group)
            except Exception:
                pass

        lam = boto3.client("lambda", **client_kwargs)
        for function_name in ["frontend_fn", "backend_fn", "worker_fn"]:
            try:
                lam.delete_function(FunctionName=function_name)
            except Exception:
                pass

        sfn = boto3.client("stepfunctions", **client_kwargs)
        try:
            machines = sfn.list_state_machines()["stateMachines"]
            for machine in machines:
                if machine["name"] == "ingest_sm":
                    sfn.delete_state_machine(stateMachineArn=machine["stateMachineArn"])
        except Exception:
            pass
    except Exception:
        pass


def ensure_daemon():
    probe = socket.socket()
    try:
        probe.settimeout(0.2)
        probe.connect((HOST, PORT))
        return
    except OSError:
        pass
    finally:
        probe.close()

    log = open(LOG_PATH, "a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, str(pathlib.Path(__file__).resolve()), "serve"],
        stdout=log,
        stderr=log,
        start_new_session=True,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        probe = socket.socket()
        try:
            probe.settimeout(0.2)
            probe.connect((HOST, PORT))
            return
        except OSError:
            time.sleep(0.1)
        finally:
            probe.close()
    raise SystemExit("local control plane did not start")


def xml_response(root, body, namespace):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<{root}Response xmlns="{namespace}">'
        f"<{root}Result>{body}</{root}Result>"
        "<ResponseMetadata><RequestId>local-control-plane</RequestId></ResponseMetadata>"
        f"</{root}Response>"
    )


def elbv2_response(root, body):
    return xml_response(root, body, "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/")


def rds_response(root, body):
    return xml_response(root, body, "http://rds.amazonaws.com/doc/2014-10-31/")


def member_list(name, items):
    return f"<{name}>" + "".join(f"<member>{item}</member>" for item in items) + f"</{name}>"


def rds_list(name, items):
    return f"<{name}>" + "".join(items) + f"</{name}>"


def values_for_prefix(params, prefix):
    return [value for key, value in sorted(params.items()) if key.startswith(prefix)]


def attributes_from_params(params):
    attributes = {}
    indexes = sorted(
        {
            key.split(".")[2]
            for key in params
            if key.startswith("Attributes.member.") and key.endswith(".Key")
        },
        key=int,
    )
    for index in indexes:
        key = params.get(f"Attributes.member.{index}.Key")
        value = params.get(f"Attributes.member.{index}.Value")
        if key is not None and value is not None:
            attributes[key] = value
    return attributes


def normalize_db_identifier(value):
    if not value:
        return "hackday-db"
    if ":db:" in value:
        return value.rsplit(":db:", 1)[-1]
    return value


def db_identifier_matches(instance, identifier):
    identifier = normalize_db_identifier(identifier)
    return identifier in {
        instance.get("identifier"),
        instance.get("resource_id"),
        arn("rds", "db:" + instance.get("identifier", "")),
    }


def find_db_instance(identifier):
    identifier = normalize_db_identifier(identifier)
    for instance in STATE["rds_instances"].values():
        if db_identifier_matches(instance, identifier):
            return instance
    return None


def requested_db_identifier(params):
    if params.get("DBInstanceIdentifier"):
        return normalize_db_identifier(params["DBInstanceIdentifier"])
    filter_values = [
        value
        for key, value in sorted(params.items())
        if "Values" in key and not key.endswith(".Name")
    ]
    return normalize_db_identifier(filter_values[0]) if filter_values else None


def ensure_db_instance(identifier):
    identifier = normalize_db_identifier(identifier)
    if identifier in STATE["deleted_rds_instances"]:
        return None
    existing = find_db_instance(identifier)
    if existing:
        return existing
    if identifier not in STATE["rds_instances"]:
        STATE["rds_instances"][identifier] = {
            "identifier": identifier,
            "resource_id": "db-HACKDAYDB",
            "engine": "postgres",
            "engine_version": "15.4",
            "class": "db.t3.micro",
            "username": "appuser",
            "storage": "20",
            "port": "5432",
            "subnet_group": "rds-subnet-group",
            "security_groups": [],
        }
    return STATE["rds_instances"][identifier]


def subnet_group_xml(group):
    subnets = [
        (
            "<Subnet>"
            f"<SubnetIdentifier>{subnet}</SubnetIdentifier>"
            f"<SubnetAvailabilityZone><Name>{REGION}{chr(97 + index)}</Name></SubnetAvailabilityZone>"
            "<SubnetStatus>Active</SubnetStatus>"
            "</Subnet>"
        )
        for index, subnet in enumerate(group.get("subnets", []))
    ]
    return (
        "<DBSubnetGroup>"
        f"<DBSubnetGroupName>{group['name']}</DBSubnetGroupName>"
        "<DBSubnetGroupDescription>Managed by Terraform</DBSubnetGroupDescription>"
        "<VpcId>vpc-local</VpcId>"
        "<SubnetGroupStatus>Complete</SubnetGroupStatus>"
        f"{rds_list('Subnets', subnets)}"
        "</DBSubnetGroup>"
    )


def db_instance_xml(instance):
    group = STATE["rds_subnet_groups"].get(
        instance.get("subnet_group"),
        {"name": instance.get("subnet_group", "rds-subnet-group"), "subnets": []},
    )
    security_groups = "".join(
        (
            "<VpcSecurityGroupMembership>"
            f"<VpcSecurityGroupId>{sg}</VpcSecurityGroupId>"
            "<Status>active</Status>"
            "</VpcSecurityGroupMembership>"
        )
        for sg in instance.get("security_groups", [])
    )
    return (
        "<DBInstance>"
        f"<DBInstanceIdentifier>{instance['identifier']}</DBInstanceIdentifier>"
        f"<DbiResourceId>{instance.get('resource_id', 'db-HACKDAYDB')}</DbiResourceId>"
        f"<DBInstanceArn>{arn('rds', 'db:' + instance['identifier'])}</DBInstanceArn>"
        "<DBInstanceStatus>available</DBInstanceStatus>"
        f"<Engine>{instance.get('engine', 'postgres')}</Engine>"
        f"<EngineVersion>{instance.get('engine_version', '15.4')}</EngineVersion>"
        f"<DBInstanceClass>{instance.get('class', 'db.t3.micro')}</DBInstanceClass>"
        f"<MasterUsername>{instance.get('username', 'appuser')}</MasterUsername>"
        f"<AllocatedStorage>{instance.get('storage', '20')}</AllocatedStorage>"
        "<StorageType>gp2</StorageType>"
        "<MultiAZ>false</MultiAZ>"
        "<PubliclyAccessible>false</PubliclyAccessible>"
        "<AutoMinorVersionUpgrade>true</AutoMinorVersionUpgrade>"
        f"<Endpoint><Address>{instance['identifier']}.local</Address><Port>{instance.get('port', '5432')}</Port></Endpoint>"
        f"{subnet_group_xml(group)}"
        f"<VpcSecurityGroups>{security_groups}</VpcSecurityGroups>"
        "</DBInstance>"
    )


def lb_xml(lb):
    zones = [
        f"<member><ZoneName>{REGION}{chr(97 + index)}</ZoneName><SubnetId>{subnet}</SubnetId></member>"
        for index, subnet in enumerate(lb.get("subnets", []))
    ]
    return (
        "<member>"
        f"<LoadBalancerArn>{lb['arn']}</LoadBalancerArn>"
        f"<LoadBalancerName>{lb['name']}</LoadBalancerName>"
        f"<DNSName>{lb['name']}.elb.local</DNSName>"
        "<CanonicalHostedZoneId>ZLOCAL</CanonicalHostedZoneId>"
        "<CreatedTime>2026-01-01T00:00:00Z</CreatedTime>"
        f"<Scheme>{lb.get('scheme', 'internet-facing')}</Scheme>"
        f"<Type>{lb.get('type', 'application')}</Type>"
        "<VpcId>vpc-local</VpcId>"
        "<State><Code>active</Code></State>"
        f"<AvailabilityZones>{''.join(zones)}</AvailabilityZones>"
        "</member>"
    )


def tg_xml(tg):
    return (
        "<member>"
        f"<TargetGroupArn>{tg['arn']}</TargetGroupArn>"
        f"<TargetGroupName>{tg['name']}</TargetGroupName>"
        "<Protocol>HTTP</Protocol>"
        "<Port>80</Port>"
        f"<TargetType>{tg.get('target_type', 'lambda')}</TargetType>"
        "<VpcId>vpc-local</VpcId>"
        "<HealthCheckEnabled>false</HealthCheckEnabled>"
        "</member>"
    )


def listener_xml(listener):
    return (
        "<member>"
        f"<ListenerArn>{listener['arn']}</ListenerArn>"
        f"<LoadBalancerArn>{listener['load_balancer_arn']}</LoadBalancerArn>"
        f"<Port>{listener.get('port', '80')}</Port>"
        f"<Protocol>{listener.get('protocol', 'HTTP')}</Protocol>"
        "<DefaultActions><member>"
        "<Type>forward</Type>"
        f"<TargetGroupArn>{listener.get('target_group_arn', '')}</TargetGroupArn>"
        "</member></DefaultActions>"
        "</member>"
    )


def ensure_listener(load_balancer_arn=None):
    if STATE["listeners"]:
        if load_balancer_arn:
            for listener in STATE["listeners"].values():
                if listener.get("load_balancer_arn") == load_balancer_arn:
                    return listener
        return next(iter(STATE["listeners"].values()))

    lb_arn = load_balancer_arn or next(iter(STATE["load_balancers"]), arn("elasticloadbalancing", "loadbalancer/app/hackday-alb/local"))
    target_group_arn = next(
        iter(STATE["target_groups"]),
        arn("elasticloadbalancing", "targetgroup/frontend-tg/local"),
    )
    listener = {
        "arn": arn("elasticloadbalancing", "listener/app/hackday-alb/local/listener"),
        "load_balancer_arn": lb_arn,
        "port": "80",
        "protocol": "HTTP",
        "target_group_arn": target_group_arn,
    }
    STATE["listeners"][listener["arn"]] = listener
    return listener


def attributes_xml(attributes):
    members = "".join(
        (
            "<member>"
            f"<Key>{key}</Key>"
            f"<Value>{value}</Value>"
            "</member>"
        )
        for key, value in sorted(attributes.items())
    )
    return f"<Attributes>{members}</Attributes>"


def default_load_balancer_attributes():
    return {
        "access_logs.s3.enabled": "false",
        "deletion_protection.enabled": "false",
        "idle_timeout.timeout_seconds": "60",
        "routing.http2.enabled": "true",
    }


def default_target_group_attributes():
    return {
        "deregistration_delay.timeout_seconds": "300",
        "lambda.multi_value_headers.enabled": "false",
    }


def default_listener_attributes():
    return {
        "routing.http.response.server.enabled": "true",
    }


def capacity_reservation_xml():
    return (
        "<LastModifiedTime>2026-01-01T00:00:00Z</LastModifiedTime>"
        "<DecreaseRequestsRemaining>0</DecreaseRequestsRemaining>"
        "<MinimumLoadBalancerCapacity><CapacityUnits>0</CapacityUnits></MinimumLoadBalancerCapacity>"
        "<CapacityReservationState />"
    )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def forward_to_upstream(self, body=b""):
        upstream = os.environ.get("TF_VAR_aws_endpoint", "").strip()
        if not upstream:
            self.send_json({"status": "ok"})
            return

        parsed = urllib.parse.urlparse(upstream)
        connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        connection = connection_class(parsed.hostname, port, timeout=60)
        upstream_base_path = parsed.path.rstrip("/")
        target = f"{upstream_base_path}{self.path}"
        if not target.startswith("/"):
            target = f"/{target}"

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"connection", "content-length", "host", "transfer-encoding"}
        }
        if body:
            headers["Content-Length"] = str(len(body))
        headers["Host"] = parsed.netloc

        try:
            connection.request(self.command, target, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"connection", "content-length", "transfer-encoding"}:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)
        finally:
            connection.close()

    def request_payload(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8") if length else ""
        query = urllib.parse.urlparse(self.path).query
        params = dict(urllib.parse.parse_qsl(body or query))
        return params, body

    def send_xml(self, text):
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(text.encode("utf-8"))

    def send_json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def send_json_error(self, code, message, status=404):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("x-amzn-ErrorType", code)
        self.end_headers()
        self.wfile.write(json.dumps({"__type": code, "Message": message, "message": message}).encode("utf-8"))

    def send_query_error(self, code, message, status=404):
        body = (
            '<ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">'
            "<Error>"
            "<Type>Sender</Type>"
            f"<Code>{code}</Code>"
            f"<Message>{message}</Message>"
            "</Error>"
            "<RequestId>local-control-plane</RequestId>"
            "</ErrorResponse>"
        )
        self.send_response(status)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path.startswith("/v1/pipes/"):
            name = path.rsplit("/", 1)[-1]
            pipe = STATE["pipes"].get(name)
            if not pipe:
                self.send_json_error("NotFoundException", f"Pipe {name} does not exist.")
                return
            self.send_json(pipe)
            return
        if path.startswith("/tags/"):
            self.send_json({"tags": {}})
            return
        self.forward_to_upstream()

    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path
        if path.startswith("/v1/pipes/"):
            name = path.rsplit("/", 1)[-1]
            _PIPE_STOP[name] = True
            _PIPE_THREADS.pop(name, None)
            pipe = STATE["pipes"].pop(name, {"Name": name, "Arn": arn("pipes", "pipe/" + name)})
            pipe["CurrentState"] = "DELETING"
            STATE["deleted_pipes"][name] = pipe
            self.send_json(pipe)
            return
        self.forward_to_upstream()

    def do_POST(self):
        params, raw_body = self.request_payload()
        action = params.get("Action")
        path = urllib.parse.urlparse(self.path).path

        if path.startswith("/v1/pipes/"):
            name = path.rsplit("/", 1)[-1]
            payload = json.loads(raw_body or "{}")
            pipe = {
                "Name": name,
                "Arn": arn("pipes", "pipe/" + name),
                "RoleArn": payload.get("RoleArn"),
                "Source": payload.get("Source"),
                "Target": payload.get("Target"),
                "Enrichment": payload.get("Enrichment"),
                "CurrentState": "RUNNING",
                "DesiredState": "RUNNING",
                "CreationTime": int(time.time()),
                "LastModifiedTime": int(time.time()),
            }
            STATE["deleted_pipes"].pop(name, None)
            STATE["pipes"][name] = pipe
            source_arn = payload.get("Source", "")
            target_arn = payload.get("Target", "")
            if source_arn and target_arn:
                _PIPE_STOP[name] = False
                t = threading.Thread(target=pipe_daemon, args=(name, source_arn, target_arn), daemon=True)
                t.start()
                _PIPE_THREADS[name] = t
            self.send_json(pipe)
            return
        if path.startswith("/tags/"):
            self.send_json({})
            return

        if action == "CreateDBSubnetGroup":
            name = params.get("DBSubnetGroupName", "rds-subnet-group")
            subnets = values_for_prefix(params, "SubnetIds.")
            STATE["rds_subnet_groups"][name] = {"name": name, "subnets": subnets}
            self.send_xml(rds_response(action, subnet_group_xml(STATE["rds_subnet_groups"][name])))
            return
        if action == "DescribeDBSubnetGroups":
            group_name = params.get("DBSubnetGroupName")
            groups = (
                [STATE["rds_subnet_groups"][group_name]]
                if group_name in STATE["rds_subnet_groups"]
                else list(STATE["rds_subnet_groups"].values())
            )
            self.send_xml(rds_response(action, rds_list("DBSubnetGroups", [subnet_group_xml(group) for group in groups])))
            return
        if action == "CreateDBInstance":
            identifier = params.get("DBInstanceIdentifier", "hackday-db")
            STATE["deleted_rds_instances"].discard(identifier)
            STATE["rds_instances"][identifier] = {
                "identifier": identifier,
                "resource_id": "db-HACKDAYDB",
                "engine": params.get("Engine", "postgres"),
                "engine_version": params.get("EngineVersion", "15.4"),
                "class": params.get("DBInstanceClass", "db.t3.micro"),
                "username": params.get("MasterUsername", "appuser"),
                "storage": params.get("AllocatedStorage", "20"),
                "port": params.get("Port", "5432"),
                "subnet_group": params.get("DBSubnetGroupName", "rds-subnet-group"),
                "security_groups": values_for_prefix(params, "VpcSecurityGroupIds."),
            }
            self.send_xml(rds_response(action, db_instance_xml(STATE["rds_instances"][identifier])))
            return
        if action == "DescribeDBInstances":
            instance_id = requested_db_identifier(params)
            if params.get("DBInstanceIdentifier"):
                instance = find_db_instance(instance_id)
                if not instance or normalize_db_identifier(instance_id) in STATE["deleted_rds_instances"]:
                    self.send_query_error(
                        "DBInstanceNotFound",
                        f"DBInstance {instance_id} not found.",
                    )
                    return
                instances = [instance]
            elif STATE["rds_instances"]:
                instances = list(STATE["rds_instances"].values())
            elif instance_id:
                instance = find_db_instance(instance_id)
                instances = [instance] if instance else []
            else:
                instances = []
            self.send_xml(rds_response(action, rds_list("DBInstances", [db_instance_xml(instance) for instance in instances])))
            return
        if action == "DeleteDBInstance":
            identifier = normalize_db_identifier(params.get("DBInstanceIdentifier"))
            instance = find_db_instance(identifier)
            if instance:
                STATE["rds_instances"].pop(instance["identifier"], None)
                STATE["deleted_rds_instances"].update(
                    {
                        identifier,
                        instance["identifier"],
                        instance.get("resource_id", ""),
                    }
                )
                self.send_xml(rds_response(action, db_instance_xml(instance)))
            else:
                STATE["deleted_rds_instances"].add(identifier)
                self.send_query_error("DBInstanceNotFound", f"DBInstance {identifier} not found.")
            return
        if action == "DeleteDBSubnetGroup":
            STATE["rds_subnet_groups"].pop(params.get("DBSubnetGroupName"), None)
            self.send_xml(rds_response(action, ""))
            return
        if action in {"AddTagsToResource", "RemoveTagsFromResource"}:
            self.send_xml(rds_response(action, ""))
            return
        if action == "ListTagsForResource":
            self.send_xml(rds_response(action, "<TagList />"))
            return

        if action == "CreateLoadBalancer":
            name = params.get("Name", "hackday-alb")
            lb = {
                "name": name,
                "arn": arn("elasticloadbalancing", "loadbalancer/app/" + name + "/local"),
                "scheme": params.get("Scheme", "internet-facing"),
                "type": params.get("Type", "application"),
                "subnets": [value for key, value in sorted(params.items()) if key.startswith("Subnets.member.")],
            }
            STATE["load_balancers"][lb["arn"]] = lb
            self.send_xml(elbv2_response(action, f"<LoadBalancers>{lb_xml(lb)}</LoadBalancers>"))
            return
        if action == "DescribeLoadBalancers":
            body = "".join(lb_xml(lb) for lb in STATE["load_balancers"].values())
            self.send_xml(elbv2_response(action, f"<LoadBalancers>{body}</LoadBalancers>"))
            return
        if action in {"DescribeLoadBalancerAttributes", "ModifyLoadBalancerAttributes"}:
            lb_arn = params.get("LoadBalancerArn")
            attributes = STATE["load_balancer_attributes"].setdefault(
                lb_arn,
                default_load_balancer_attributes(),
            )
            if action == "ModifyLoadBalancerAttributes":
                attributes.update(attributes_from_params(params))
            self.send_xml(elbv2_response(action, attributes_xml(attributes)))
            return
        if action in {"DescribeCapacityReservation", "ModifyCapacityReservation"}:
            self.send_xml(elbv2_response(action, capacity_reservation_xml()))
            return
        if action == "CreateTargetGroup":
            name = params.get("Name", "frontend-tg")
            tg = {
                "name": name,
                "arn": arn("elasticloadbalancing", "targetgroup/" + name + "/local"),
                "target_type": params.get("TargetType", "lambda"),
            }
            STATE["target_groups"][tg["arn"]] = tg
            self.send_xml(elbv2_response(action, f"<TargetGroups>{tg_xml(tg)}</TargetGroups>"))
            return
        if action == "DescribeTargetGroups":
            body = "".join(tg_xml(tg) for tg in STATE["target_groups"].values())
            self.send_xml(elbv2_response(action, f"<TargetGroups>{body}</TargetGroups>"))
            return
        if action in {"DescribeTargetGroupAttributes", "ModifyTargetGroupAttributes"}:
            target_group_arn = params.get("TargetGroupArn")
            attributes = STATE["target_group_attributes"].setdefault(
                target_group_arn,
                default_target_group_attributes(),
            )
            if action == "ModifyTargetGroupAttributes":
                attributes.update(attributes_from_params(params))
            self.send_xml(elbv2_response(action, attributes_xml(attributes)))
            return
        if action == "RegisterTargets":
            target_group_arn = params.get("TargetGroupArn")
            targets = [value for key, value in sorted(params.items()) if key.endswith(".Id")]
            STATE["target_health"][target_group_arn] = targets
            self.send_xml(elbv2_response(action, ""))
            return
        if action == "DescribeTargetHealth":
            target_group_arn = params.get("TargetGroupArn")
            targets = STATE["target_health"].get(target_group_arn, [])
            descriptions = "".join(
                (
                    "<member><Target>"
                    f"<Id>{target}</Id>"
                    "</Target><TargetHealth><State>healthy</State></TargetHealth></member>"
                )
                for target in targets
            )
            self.send_xml(elbv2_response(action, f"<TargetHealthDescriptions>{descriptions}</TargetHealthDescriptions>"))
            return
        if action == "CreateListener":
            listener = {
                "arn": arn("elasticloadbalancing", "listener/app/hackday-alb/local/listener"),
                "load_balancer_arn": params.get("LoadBalancerArn"),
                "port": params.get("Port", "80"),
                "protocol": params.get("Protocol", "HTTP"),
                "target_group_arn": params.get("DefaultActions.member.1.TargetGroupArn", ""),
            }
            STATE["listeners"][listener["arn"]] = listener
            self.send_xml(elbv2_response(action, f"<Listeners>{listener_xml(listener)}</Listeners>"))
            return
        if action == "DescribeListeners":
            listener_arns = values_for_prefix(params, "ListenerArns.member.")
            load_balancer_arn = params.get("LoadBalancerArn")
            if listener_arns:
                listeners = [
                    STATE["listeners"].get(listener_arn)
                    for listener_arn in listener_arns
                    if listener_arn in STATE["listeners"]
                ]
            else:
                listeners = [
                    listener
                    for listener in STATE["listeners"].values()
                    if not load_balancer_arn or listener.get("load_balancer_arn") == load_balancer_arn
                ]
            if not listeners and load_balancer_arn:
                listeners = [ensure_listener(load_balancer_arn)]
            body = "".join(listener_xml(listener) for listener in listeners)
            self.send_xml(elbv2_response(action, f"<Listeners>{body}</Listeners>"))
            return
        if action in {"DescribeListenerAttributes", "ModifyListenerAttributes"}:
            listener_arn = params.get("ListenerArn")
            if listener_arn:
                STATE["listeners"].setdefault(listener_arn, ensure_listener())
            attributes = STATE["listener_attributes"].setdefault(
                listener_arn,
                default_listener_attributes(),
            )
            if action == "ModifyListenerAttributes":
                attributes.update(attributes_from_params(params))
            self.send_xml(elbv2_response(action, attributes_xml(attributes)))
            return
        if action == "DeleteLoadBalancer":
            lb_arn = params.get("LoadBalancerArn")
            STATE["load_balancers"].pop(lb_arn, None)
            STATE["load_balancer_attributes"].pop(lb_arn, None)
            deleted_listener_arns = {
                listener_arn
                for listener_arn, listener in STATE["listeners"].items()
                if listener.get("load_balancer_arn") == lb_arn
            }
            STATE["listeners"] = {
                listener_arn: listener
                for listener_arn, listener in STATE["listeners"].items()
                if listener_arn not in deleted_listener_arns
            }
            for listener_arn in deleted_listener_arns:
                STATE["listener_attributes"].pop(listener_arn, None)
            self.send_xml(elbv2_response(action, ""))
            return
        if action == "DeleteTargetGroup":
            target_group_arn = params.get("TargetGroupArn")
            STATE["target_groups"].pop(target_group_arn, None)
            STATE["target_group_attributes"].pop(target_group_arn, None)
            self.send_xml(elbv2_response(action, ""))
            return
        if action == "DeleteListener":
            listener_arn = params.get("ListenerArn")
            STATE["listeners"].pop(listener_arn, None)
            STATE["listener_attributes"].pop(listener_arn, None)
            self.send_xml(elbv2_response(action, ""))
            return
        if action == "DeregisterTargets":
            target_group_arn = params.get("TargetGroupArn")
            targets = set(value for key, value in sorted(params.items()) if key.endswith(".Id"))
            STATE["target_health"][target_group_arn] = [
                target for target in STATE["target_health"].get(target_group_arn, []) if target not in targets
            ]
            self.send_xml(elbv2_response(action, ""))
            return
        if action in {"AddTags", "RemoveTags"}:
            self.send_xml(elbv2_response(action, ""))
            return
        if action == "DescribeTags":
            tag_descriptions = "".join(
                f"<member><ResourceArn>{value}</ResourceArn><Tags /></member>"
                for key, value in sorted(params.items())
                if key.startswith("ResourceArns.member.")
            )
            self.send_xml(elbv2_response(action, f"<TagDescriptions>{tag_descriptions}</TagDescriptions>"))
            return

        self.forward_to_upstream(raw_body.encode("utf-8"))

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self.forward_to_upstream(body)

    def do_PATCH(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self.forward_to_upstream(body)

    def do_HEAD(self):
        self.forward_to_upstream()


def serve():
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        serve()
    else:
        cleanup_stale_iam()
        ensure_daemon()
