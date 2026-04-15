#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'InfrastructureAnalystStack');

const region = process.env.AWS_REGION ?? 'us-east-1';
const endpoint = process.env.AWS_ENDPOINT ?? '';
const python312Runtime = new lambda.Runtime('python3.12', lambda.RuntimeFamily.PYTHON, {
  supportsInlineCode: true,
});

const collectorFunctionName = 'infrastructure-analyst-collector';
const summaryFunctionName = 'infrastructure-analyst-summary';
const databaseName = 'inventory';

const vpc = new ec2.Vpc(stack, 'Vpc', {
  ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
  maxAzs: 2,
  natGateways: 1,
  subnetConfiguration: [
    {
      name: 'Public',
      subnetType: ec2.SubnetType.PUBLIC,
      cidrMask: 24,
    },
    {
      name: 'Private',
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      cidrMask: 24,
    },
  ],
});

const lambdaSecurityGroup = new ec2.SecurityGroup(stack, 'LambdaSecurityGroup', {
  vpc,
  description: 'Security group shared by the analyst Lambda functions',
  allowAllOutbound: true,
});

const databaseSecurityGroup = new ec2.SecurityGroup(stack, 'DatabaseSecurityGroup', {
  vpc,
  description: 'Security group for the analyst PostgreSQL instance',
  allowAllOutbound: true,
});
databaseSecurityGroup.addIngressRule(
  lambdaSecurityGroup,
  ec2.Port.tcp(5432),
  'Allow PostgreSQL only from the Lambda security group',
);

const dbSubnetGroup = new rds.SubnetGroup(stack, 'DatabaseSubnetGroup', {
  description: 'Private subnets for the analyst database',
  vpc,
  vpcSubnets: {
    subnets: vpc.privateSubnets,
  },
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

const databaseSecret = new secretsmanager.Secret(stack, 'DatabaseSecret', {
  generateSecretString: {
    secretStringTemplate: JSON.stringify({
      username: 'infraanalyst',
    }),
    generateStringKey: 'password',
    excludePunctuation: true,
  },
});
databaseSecret.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

const database = new rds.DatabaseInstance(stack, 'Database', {
  engine: rds.DatabaseInstanceEngine.postgres({
    version: rds.PostgresEngineVersion.VER_15,
  }),
  credentials: rds.Credentials.fromPassword(
    'infraanalyst',
    databaseSecret.secretValueFromJson('password'),
  ),
  instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
  vpc,
  subnetGroup: dbSubnetGroup,
  securityGroups: [databaseSecurityGroup],
  vpcSubnets: {
    subnets: vpc.privateSubnets,
  },
  databaseName,
  allocatedStorage: 20,
  storageType: rds.StorageType.GP3,
  multiAz: false,
  backupRetention: cdk.Duration.days(7),
  deleteAutomatedBackups: true,
  deletionProtection: false,
  publiclyAccessible: false,
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

const findingsBucket = new s3.Bucket(stack, 'FindingsBucket', {
  versioned: true,
  encryption: s3.BucketEncryption.S3_MANAGED,
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
  removalPolicy: cdk.RemovalPolicy.DESTROY,
  lifecycleRules: [
    {
      noncurrentVersionTransitions: [
        {
          storageClass: s3.StorageClass.INFREQUENT_ACCESS,
          transitionAfter: cdk.Duration.days(30),
        },
      ],
      noncurrentVersionExpiration: cdk.Duration.days(365),
    },
    {
      prefix: 'reports/',
      expiration: cdk.Duration.days(30),
    },
  ],
});

const collectorLogGroup = new logs.LogGroup(stack, 'CollectorLogGroup', {
  logGroupName: `/aws/lambda/${collectorFunctionName}`,
  retention: logs.RetentionDays.TWO_WEEKS,
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

const summaryLogGroup = new logs.LogGroup(stack, 'SummaryLogGroup', {
  logGroupName: `/aws/lambda/${summaryFunctionName}`,
  retention: logs.RetentionDays.TWO_WEEKS,
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

const lambdaEnvironment = {
  REPORT_BUCKET: findingsBucket.bucketName,
  DB_SECRET_ARN: databaseSecret.secretArn,
  DB_HOST: database.instanceEndpoint.hostname,
  DB_PORT: '5432',
  DB_NAME: databaseName,
  AWS_ENDPOINT: endpoint,
};

const commonPython = String.raw`
import base64
import boto3
import hashlib
import hmac
import json
import os
import socket
import ssl
import subprocess
import sys
import uuid
from datetime import datetime, timezone


def _endpoint_kwargs():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint_url = os.getenv("AWS_ENDPOINT")
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return kwargs


def _client(service_name):
    return boto3.client(service_name, **_endpoint_kwargs())


def _response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def _read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("PostgreSQL connection closed unexpectedly")
        data += chunk
    return data


def _read_message(sock):
    kind = _read_exact(sock, 1)
    length = int.from_bytes(_read_exact(sock, 4), "big")
    payload = _read_exact(sock, length - 4)
    return kind, payload


def _message(kind, payload=b""):
    return kind + (len(payload) + 4).to_bytes(4, "big") + payload


def _startup_message(user_name, db_name):
    payload = (196608).to_bytes(4, "big")
    params = {
        "user": user_name,
        "database": db_name,
        "client_encoding": "UTF8",
    }
    for key, value in params.items():
        payload += key.encode("utf-8") + b"\x00" + value.encode("utf-8") + b"\x00"
    payload += b"\x00"
    return len(payload + b"1234").to_bytes(4, "big") + payload


def _pg_error(payload):
    parts = []
    idx = 0
    while idx < len(payload):
        field_type = payload[idx:idx + 1]
        if field_type == b"\x00":
            break
        idx += 1
        end = payload.index(b"\x00", idx)
        parts.append(payload[idx:end].decode("utf-8", "ignore"))
        idx = end + 1
    return " | ".join(parts) or "PostgreSQL error"


def _md5_password_message(user_name, password, salt):
    inner = hashlib.md5((password + user_name).encode("utf-8")).hexdigest().encode("utf-8")
    outer = hashlib.md5(inner + salt).hexdigest()
    return _message(b"p", ("md5" + outer + "\x00").encode("utf-8"))


def _sasl_initial_message(client_first):
    mechanism = b"SCRAM-SHA-256\x00"
    initial_bytes = client_first.encode("utf-8")
    payload = mechanism + len(initial_bytes).to_bytes(4, "big") + initial_bytes
    return _message(b"p", payload)


def _sasl_response_message(client_final):
    return _message(b"p", client_final.encode("utf-8") + b"\x00")


def _ssl_socket(host_name, port_number):
    sock = socket.create_connection((host_name, port_number), timeout=10)
    sock.sendall((8).to_bytes(4, "big") + (80877103).to_bytes(4, "big"))
    response = _read_exact(sock, 1)
    if response == b"S":
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context.wrap_socket(sock, server_hostname=host_name)
    if response == b"N":
        return sock
    raise RuntimeError("Unexpected PostgreSQL SSL negotiation response")


def _load_secret():
    secret_response = _client("secretsmanager").get_secret_value(SecretId=os.environ["DB_SECRET_ARN"])
    return json.loads(secret_response["SecretString"])


def _quote(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


class PgConnection:
    def __init__(self, host, port, database_name, user_name, password):
        self.host = host
        self.port = port
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.sock = None

    def __enter__(self):
        self.sock = _ssl_socket(self.host, self.port)
        self.sock.sendall(_startup_message(self.user_name, self.database_name))
        self._authenticate()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.sock is not None:
            try:
                self.sock.sendall(_message(b"X"))
            except Exception:
                pass
            self.sock.close()

    def _authenticate(self):
        client_first_bare = None
        expected_server_signature = None
        while True:
            kind, payload = _read_message(self.sock)
            if kind == b"R":
                auth_type = int.from_bytes(payload[:4], "big")
                if auth_type == 0:
                    continue
                if auth_type == 5:
                    self.sock.sendall(_md5_password_message(self.user_name, self.password, payload[4:8]))
                    continue
                if auth_type == 10:
                    nonce = base64.b64encode(os.urandom(18)).decode("ascii").rstrip("=")
                    client_first_bare = "n=" + self.user_name + ",r=" + nonce
                    client_first = "n,," + client_first_bare
                    self.sock.sendall(_sasl_initial_message(client_first))
                    continue
                if auth_type == 11:
                    server_first = payload[4:].decode("utf-8")
                    attributes = dict(item.split("=", 1) for item in server_first.split(","))
                    salt = base64.b64decode(attributes["s"])
                    iterations = int(attributes["i"])
                    combined_nonce = attributes["r"]
                    client_final_without_proof = "c=biws,r=" + combined_nonce
                    salted_password = hashlib.pbkdf2_hmac(
                        "sha256",
                        self.password.encode("utf-8"),
                        salt,
                        iterations,
                    )
                    client_key = hmac.new(salted_password, b"Client Key", hashlib.sha256).digest()
                    stored_key = hashlib.sha256(client_key).digest()
                    auth_message = ",".join([client_first_bare, server_first, client_final_without_proof])
                    client_signature = hmac.new(
                        stored_key,
                        auth_message.encode("utf-8"),
                        hashlib.sha256,
                    ).digest()
                    client_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
                    server_key = hmac.new(salted_password, b"Server Key", hashlib.sha256).digest()
                    expected_server_signature = base64.b64encode(
                        hmac.new(server_key, auth_message.encode("utf-8"), hashlib.sha256).digest()
                    ).decode("ascii")
                    client_final = client_final_without_proof + ",p=" + base64.b64encode(client_proof).decode("ascii")
                    self.sock.sendall(_sasl_response_message(client_final))
                    continue
                if auth_type == 12:
                    server_final = payload[4:].decode("utf-8")
                    if expected_server_signature and ("v=" + expected_server_signature) not in server_final:
                        raise RuntimeError("PostgreSQL SCRAM server signature verification failed")
                    continue
                raise RuntimeError("Unsupported PostgreSQL authentication type: " + str(auth_type))

            if kind in (b"S", b"K", b"N"):
                continue
            if kind == b"E":
                raise RuntimeError(_pg_error(payload))
            if kind == b"Z":
                return

    def query(self, sql_text):
        self.sock.sendall(_message(b"Q", sql_text.encode("utf-8") + b"\x00"))
        columns = []
        rows = []
        while True:
            kind, payload = _read_message(self.sock)
            if kind == b"T":
                field_count = int.from_bytes(payload[:2], "big")
                cursor = 2
                columns = []
                for _ in range(field_count):
                    end = payload.index(b"\x00", cursor)
                    columns.append(payload[cursor:end].decode("utf-8"))
                    cursor = end + 19
                continue
            if kind == b"D":
                field_count = int.from_bytes(payload[:2], "big")
                cursor = 2
                row = []
                for _ in range(field_count):
                    size = int.from_bytes(payload[cursor:cursor + 4], "big", signed=True)
                    cursor += 4
                    if size == -1:
                        row.append(None)
                    else:
                        row.append(payload[cursor:cursor + size].decode("utf-8"))
                        cursor += size
                rows.append(row)
                continue
            if kind in (b"C", b"I", b"N", b"S"):
                continue
            if kind == b"E":
                raise RuntimeError(_pg_error(payload))
            if kind == b"Z":
                return columns, rows

    def execute(self, sql_text):
        self.query(sql_text)


def _ensure_schema(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS inventory_runs ("
        "run_id text PRIMARY KEY, "
        "created_at timestamptz NOT NULL, "
        "counts_by_service jsonb NOT NULL, "
        "s3_report_key text NOT NULL)"
    )


def _list_from_pages(page_iterator, key_name, value_name):
    items = []
    for page in page_iterator:
        for item in page.get(key_name, []):
            items.append(item[value_name])
    return items


def _list_s3_buckets():
    return [bucket["Name"] for bucket in _client("s3").list_buckets().get("Buckets", [])]


def _list_lambda_functions():
    return _list_from_pages(
        _client("lambda").get_paginator("list_functions").paginate(),
        "Functions",
        "FunctionName",
    )


def _list_sqs_queues():
    return _client("sqs").list_queues().get("QueueUrls", [])


def _list_eventbridge_rules():
    return _list_from_pages(
        _client("events").get_paginator("list_rules").paginate(),
        "Rules",
        "Name",
    )


def _list_rds_instances():
    return _list_from_pages(
        _client("rds").get_paginator("describe_db_instances").paginate(),
        "DBInstances",
        "DBInstanceIdentifier",
    )


def _list_redshift_clusters():
    return _list_from_pages(
        _client("redshift").get_paginator("describe_clusters").paginate(),
        "Clusters",
        "ClusterIdentifier",
    )


def _cli_discovery():
    command = [
        sys.executable,
        "-c",
        (
            "import boto3, json, os; "
            "kwargs={'region_name': os.getenv('AWS_REGION', 'us-east-1')}; "
            "endpoint=os.getenv('AWS_ENDPOINT'); "
            "kwargs.update({'endpoint_url': endpoint} if endpoint else {}); "
            "print(json.dumps(boto3.client('sts', **kwargs).get_caller_identity()))"
        ),
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    return {
        "command": " ".join(command),
        "stdout": completed.stdout.strip(),
    }


def _latest_report(bucket_name):
    s3_client = _client("s3")
    latest = None
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="reports/"):
        for item in page.get("Contents", []):
            if latest is None or item["LastModified"] > latest["LastModified"]:
                latest = item
    if latest is None:
        return None
    report = s3_client.get_object(Bucket=bucket_name, Key=latest["Key"])
    report_body = report["Body"].read().decode("utf-8")
    return {
        "key": latest["Key"],
        "body": json.loads(report_body),
    }
`;

const collectorSource = commonPython + String.raw`

def handler(event, context):
    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)
    inventory = {
        "s3_buckets": _list_s3_buckets(),
        "lambda_functions": _list_lambda_functions(),
        "sqs_queues": _list_sqs_queues(),
        "eventbridge_rules": _list_eventbridge_rules(),
        "rds_instances": _list_rds_instances(),
        "redshift_clusters": _list_redshift_clusters(),
    }
    counts_by_service = {
        "s3": len(inventory["s3_buckets"]),
        "lambda": len(inventory["lambda_functions"]),
        "sqs": len(inventory["sqs_queues"]),
        "eventbridge": len(inventory["eventbridge_rules"]),
        "rds": len(inventory["rds_instances"]),
        "redshift": len(inventory["redshift_clusters"]),
    }
    report_key = (
        "reports/"
        + created_at.strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + run_id
        + ".json"
    )
    report = {
        "run_id": run_id,
        "created_at": created_at.isoformat(),
        "region": os.getenv("AWS_REGION", "us-east-1"),
        "inventory": inventory,
        "counts_by_service": counts_by_service,
        "cli_discovery": _cli_discovery(),
    }

    _client("s3").put_object(
        Bucket=os.environ["REPORT_BUCKET"],
        Key=report_key,
        Body=json.dumps(report).encode("utf-8"),
        ContentType="application/json",
    )

    credentials = _load_secret()
    with PgConnection(
        os.environ["DB_HOST"],
        int(os.environ["DB_PORT"]),
        os.environ["DB_NAME"],
        credentials["username"],
        credentials["password"],
    ) as connection:
        _ensure_schema(connection)
        connection.execute(
            "INSERT INTO inventory_runs (run_id, created_at, counts_by_service, s3_report_key) VALUES ("
            + _quote(run_id)
            + ", "
            + _quote(created_at.isoformat())
            + ", "
            + _quote(json.dumps(counts_by_service))
            + "::jsonb, "
            + _quote(report_key)
            + ")"
        )

    return _response(
        200,
        {
            "latest_run_id": run_id,
            "latest_created_at": created_at.isoformat(),
            "counts_by_service": counts_by_service,
            "s3_report_key": report_key,
        },
    )
`;

const summarySource = commonPython + String.raw`

def handler(event, context):
    latest_report = _latest_report(os.environ["REPORT_BUCKET"])
    if latest_report is None:
        return _response(404, {"message": "No reports found"})

    credentials = _load_secret()
    with PgConnection(
        os.environ["DB_HOST"],
        int(os.environ["DB_PORT"]),
        os.environ["DB_NAME"],
        credentials["username"],
        credentials["password"],
    ) as connection:
        _ensure_schema(connection)
        columns, rows = connection.query(
            "SELECT run_id, created_at::text, counts_by_service::text, s3_report_key "
            "FROM inventory_runs ORDER BY created_at DESC LIMIT 1"
        )

    if not rows:
        return _response(
            404,
            {
                "message": "No database summary found",
                "s3_report_key": latest_report["key"],
            },
        )

    latest_row = dict(zip(columns, rows[0]))
    return _response(
        200,
        {
            "latest_run_id": latest_row["run_id"],
            "latest_created_at": latest_row["created_at"],
            "counts_by_service": json.loads(latest_row["counts_by_service"]),
            "s3_report_key": latest_row["s3_report_key"],
        },
    )
`;

const collectorRole = new iam.Role(stack, 'CollectorRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Execution role for the collector Lambda',
});

const summaryRole = new iam.Role(stack, 'SummaryRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Execution role for the summary Lambda',
});

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [`${collectorLogGroup.logGroupArn}:*`],
}));

summaryRole.addToPolicy(new iam.PolicyStatement({
  actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [`${summaryLogGroup.logGroupArn}:*`],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['ec2:CreateNetworkInterface', 'ec2:DescribeNetworkInterfaces', 'ec2:DeleteNetworkInterface'],
  resources: ['*'],
}));

summaryRole.addToPolicy(new iam.PolicyStatement({
  actions: ['ec2:CreateNetworkInterface', 'ec2:DescribeNetworkInterfaces', 'ec2:DeleteNetworkInterface'],
  resources: ['*'],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret'],
  resources: [databaseSecret.secretArn],
}));

summaryRole.addToPolicy(new iam.PolicyStatement({
  actions: ['secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret'],
  resources: [databaseSecret.secretArn],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:PutObject', 's3:AbortMultipartUpload'],
  resources: [findingsBucket.arnForObjects('*')],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:ListBucket'],
  resources: [findingsBucket.bucketArn],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:ListAllMyBuckets'],
  resources: ['*'],
}));

collectorRole.addToPolicy(new iam.PolicyStatement({
  actions: [
    'lambda:ListFunctions',
    'sqs:ListQueues',
    'events:ListRules',
    'rds:DescribeDBInstances',
    'redshift:DescribeClusters',
  ],
  resources: ['*'],
}));

summaryRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:GetObject'],
  resources: [findingsBucket.arnForObjects('*')],
}));

summaryRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:ListBucket'],
  resources: [findingsBucket.bucketArn],
}));

const collectorFunction = new lambda.Function(stack, 'CollectorFunction', {
  functionName: collectorFunctionName,
  runtime: python312Runtime,
  handler: 'index.handler',
  code: lambda.Code.fromInline(collectorSource),
  role: collectorRole,
  memorySize: 256,
  timeout: cdk.Duration.seconds(60),
  vpc,
  vpcSubnets: {
    subnets: vpc.privateSubnets,
  },
  securityGroups: [lambdaSecurityGroup],
  environment: lambdaEnvironment,
});

const summaryFunction = new lambda.Function(stack, 'SummaryFunction', {
  functionName: summaryFunctionName,
  runtime: python312Runtime,
  handler: 'index.handler',
  code: lambda.Code.fromInline(summarySource),
  role: summaryRole,
  memorySize: 128,
  timeout: cdk.Duration.seconds(30),
  vpc,
  vpcSubnets: {
    subnets: vpc.privateSubnets,
  },
  securityGroups: [lambdaSecurityGroup],
  environment: lambdaEnvironment,
});

const api = new apigateway.RestApi(stack, 'InfrastructureApi', {
  cloudWatchRole: false,
  deployOptions: {
    loggingLevel: apigateway.MethodLoggingLevel.INFO,
    dataTraceEnabled: true,
    metricsEnabled: true,
    methodOptions: {
      '/*/*': {
        loggingLevel: apigateway.MethodLoggingLevel.INFO,
        dataTraceEnabled: true,
        metricsEnabled: true,
      },
    },
  },
});

const summaryResource = api.root.addResource('summary');
summaryResource.addMethod('GET', new apigateway.LambdaIntegration(summaryFunction));

const runResource = api.root.addResource('run');
runResource.addMethod('POST', new apigateway.LambdaIntegration(collectorFunction));

const schedulerRoleArn = cdk.Fn.sub(
  'arn:${AWS::Partition}:iam::${AWS::AccountId}:role/aws-service-role/scheduler.amazonaws.com/AWSServiceRoleForScheduler',
);

const collectionSchedule = new scheduler.CfnSchedule(stack, 'CollectorSchedule', {
  flexibleTimeWindow: {
    mode: 'OFF',
  },
  scheduleExpression: 'rate(1 hour)',
  state: 'ENABLED',
  target: {
    arn: collectorFunction.functionArn,
    roleArn: schedulerRoleArn,
    input: JSON.stringify({
      source: 'schedule',
      operation: 'inventory-run',
    }),
  },
});

collectorFunction.addPermission('AllowSchedulerInvoke', {
  principal: new iam.ServicePrincipal('scheduler.amazonaws.com'),
  action: 'lambda:InvokeFunction',
  sourceArn: collectionSchedule.attrArn,
});

new cdk.CfnOutput(stack, 'ApiUrl', {
  value: api.url,
});

new cdk.CfnOutput(stack, 'FindingsBucketName', {
  value: findingsBucket.bucketName,
});

new cdk.CfnOutput(stack, 'CollectorFunctionName', {
  value: collectorFunction.functionName,
});

new cdk.CfnOutput(stack, 'SummaryFunctionName', {
  value: summaryFunction.functionName,
});

new cdk.CfnOutput(stack, 'DatabaseSecretArn', {
  value: databaseSecret.secretArn,
});

app.synth();
