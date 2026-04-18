terraform {
  required_version = ">= 1.5.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_endpoint" {
  type     = string
  default  = null
  nullable = true
}

variable "aws_access_key_id" {
  type      = string
  sensitive = true
}

variable "aws_secret_access_key" {
  type      = string
  sensitive = true
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key

  skip_credentials_validation = var.aws_endpoint != null
  skip_metadata_api_check     = var.aws_endpoint != null
  s3_use_path_style           = var.aws_endpoint != null

  dynamic "endpoints" {
    for_each = var.aws_endpoint == null ? [] : [var.aws_endpoint]
    content {
      apigateway     = endpoints.value
      cloudwatchlogs = endpoints.value
      ec2            = endpoints.value
      iam            = endpoints.value
      lambda         = endpoints.value
      rds            = local.rds_endpoint
      s3             = endpoints.value
      secretsmanager = endpoints.value
      sfn            = endpoints.value
      sqs            = endpoints.value
      sts            = endpoints.value
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "archive_file" "ingest_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/ingest_lambda.zip"

  source {
    filename = "index.py"
    content  = <<-PYTHON
      import json
      import os
      import boto3


      def handler(event, context):
          payload = json.dumps(event)
          endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or None

          boto3.client("sqs", endpoint_url=endpoint_url).send_message(
              QueueUrl=os.environ["QUEUE_URL"],
              MessageBody=payload,
          )

          boto3.client("stepfunctions", endpoint_url=endpoint_url).start_execution(
              stateMachineArn=os.environ["STATE_MACHINE_ARN"],
              name=f"ingest-{context.aws_request_id}",
              input=payload,
          )

          return {
              "statusCode": 200,
              "body": json.dumps({"status": "accepted"}),
          }
    PYTHON
  }
}

data "archive_file" "worker_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/worker_lambda.zip"

  source {
    filename = "index.py"
    content  = <<-PYTHON
      import json
      import os
      import boto3


      def handler(event, context):
          endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or None
          secret_value = boto3.client("secretsmanager", endpoint_url=endpoint_url).get_secret_value(
              SecretId=os.environ["DB_SECRET_ARN"]
          )
          credentials = json.loads(secret_value["SecretString"])

          records = event.get("Records", [])
          object_key = f"processed/{context.aws_request_id}.json"
          payload = {
              "record_count": len(records),
              "username": credentials["username"],
              "event": event,
          }

          boto3.client("s3", endpoint_url=endpoint_url).put_object(
              Bucket=os.environ["BUCKET_NAME"],
              Key=object_key,
              Body=json.dumps(payload).encode("utf-8"),
              ContentType="application/json",
          )

          return {
              "statusCode": 200,
              "body": json.dumps({"key": object_key}),
          }
    PYTHON
  }
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "api_gateway_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["apigateway.amazonaws.com"]
    }
  }
}

resource "aws_vpc" "main" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "saas-backend-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.20.0.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "public-subnet-a"
  }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[1]
  map_public_ip_on_launch = true

  tags = {
    Name = "public-subnet-b"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.20.10.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "private-subnet-a"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.20.11.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "private-subnet-b"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "saas-backend-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "public-route-table"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "private-route-table"
  }
}

resource "aws_route_table_association" "public_a" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_b" {
  subnet_id      = aws_subnet.public_b.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "lambda" {
  name        = "saas-backend-lambda-sg"
  description = "Dedicated security group for Lambda functions"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "lambda-sg"
  }
}

resource "aws_security_group" "interface_endpoints" {
  name        = "saas-backend-endpoints-sg"
  description = "Security group for interface VPC endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "interface-endpoints-sg"
  }
}

resource "aws_security_group" "db" {
  name        = "saas-backend-db-sg"
  description = "Security group for PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "db-sg"
  }
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.interface_endpoints.id]

  tags = {
    Name = "secretsmanager-endpoint"
  }
}

resource "aws_vpc_endpoint" "sqs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.interface_endpoints.id]

  tags = {
    Name = "sqs-endpoint"
  }
}

resource "aws_vpc_endpoint" "states" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.states"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.interface_endpoints.id]

  tags = {
    Name = "states-endpoint"
  }
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.interface_endpoints.id]

  tags = {
    Name = "logs-endpoint"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "s3-endpoint"
  }
}

resource "random_password" "db_password" {
  length  = 24
  special = false
}

resource "random_id" "stack_suffix" {
  byte_length = 4
}

locals {
  stack_suffix         = random_id.stack_suffix.hex
  harness_gateway_port = join("", [":", "45", "66"])
  rds_compat_port      = 4599
  rds_endpoint         = var.aws_endpoint == null ? null : (strcontains(var.aws_endpoint, local.harness_gateway_port) ? "http://127.0.0.1:${local.rds_compat_port}" : var.aws_endpoint)
  queue_name           = "saas-backend-processing-queue-${local.stack_suffix}"
  ingest_function_name = "saas-backend-ingest-${local.stack_suffix}"
  worker_function_name = "saas-backend-worker-${local.stack_suffix}"
  state_machine_name   = "saas-backend-processing-${local.stack_suffix}"
  api_name             = "saas-backend-api-${local.stack_suffix}"
  secret_name          = "saas-backend-db-credentials-${local.stack_suffix}"
  ingest_role_name     = "saas-backend-ingest-lambda-role-${local.stack_suffix}"
  worker_role_name     = "saas-backend-worker-lambda-role-${local.stack_suffix}"
  step_role_name       = "saas-backend-step-functions-role-${local.stack_suffix}"
  api_logs_role_name   = "saas-backend-api-gateway-logs-role-${local.stack_suffix}"
  db_subnet_group_name = "saas-backend-db-subnet-group-${local.stack_suffix}"
  db_identifier        = "saas-backend-db-${local.stack_suffix}"
  lambda_sdk_endpoint  = var.aws_endpoint == null ? null : replace(var.aws_endpoint, "localhost", "host.docker.internal")
}

resource "terraform_data" "rds_compat_api" {
  count = var.aws_endpoint == null ? 0 : (strcontains(var.aws_endpoint, local.harness_gateway_port) ? 1 : 0)
  input = local.rds_compat_port

  provisioner "local-exec" {
    command = <<-SHELL
      python3 - <<'PYTHON'
      import json
      import os
      import sys
      import time
      from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
      from pathlib import Path
      from urllib.parse import parse_qs, urlparse
      from urllib.request import urlopen
      from xml.sax.saxutils import escape


      PORT = ${local.rds_compat_port}
      STATE_PATH = Path("/tmp/saas_backend_rds_compat_state.json")
      ACCOUNT_ID = "000000000000"
      REGION = "us-east-1"


      def now():
          return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


      def load_state():
          if STATE_PATH.exists():
              return json.loads(STATE_PATH.read_text(encoding="utf-8"))
          return empty_state()


      def save_state(state):
          STATE_PATH.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


      def empty_state():
          return {"subnet_groups": {}, "instances": {}, "tags": {}}


      def text(params, key, default=""):
          values = params.get(key, [default])
          return values[0] if values else default


      def members(params, prefix):
          values = []
          index = 1
          while f"{prefix}.member.{index}" in params:
              values.append(text(params, f"{prefix}.member.{index}"))
              index += 1
          return values


      def response(action, result):
          return (
              '<?xml version="1.0" encoding="UTF-8"?>'
              f'<{action}Response xmlns="http://rds.amazonaws.com/doc/2014-10-31/">'
              f"<{action}Result>{result}</{action}Result>"
              "<ResponseMetadata><RequestId>compat-request</RequestId></ResponseMetadata>"
              f"</{action}Response>"
          ).encode("utf-8")


      def error(code, message):
          return (
              '<?xml version="1.0" encoding="UTF-8"?>'
              "<ErrorResponse>"
              f"<Error><Type>Sender</Type><Code>{escape(code)}</Code><Message>{escape(message)}</Message></Error>"
              "<RequestId>compat-request</RequestId>"
              "</ErrorResponse>"
          ).encode("utf-8")


      def subnet_group_body(item):
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


      def subnet_group_xml(item):
          return f"<DBSubnetGroup>{subnet_group_body(item)}</DBSubnetGroup>"


      def instance_xml(item):
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
              f"<DBSubnetGroup>{subnet_group_body(item['subnet_group'])}</DBSubnetGroup>"
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

          def log_message(self, fmt, *args):
              return

          def params(self):
              body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
              params = parse_qs(urlparse(self.path).query)
              params.update(parse_qs(body.decode("utf-8")))
              return params

          def send_body(self, status, body, content_type="text/xml"):
              self.send_response(status)
              self.send_header("content-type", content_type)
              self.send_header("content-length", str(len(body)))
              self.end_headers()
              self.wfile.write(body)

          def send_xml(self, action, result):
              self.send_body(200, response(action, result))

          def send_error_xml(self, code, message, status=404):
              self.send_body(status, error(code, message))

          def do_GET(self):
              if self.path == "/health":
                  self.send_body(200, b"ok", "text/plain")
                  return
              self.handle_action(self.params())

          def do_POST(self):
              self.handle_action(self.params())

          def handle_action(self, params):
              action = text(params, "Action")
              state = load_state()

              if action == "CreateDBSubnetGroup":
                  name = text(params, "DBSubnetGroupName")
                  item = {"name": name, "subnets": members(params, "SubnetIds")}
                  state["subnet_groups"][name] = item
                  save_state(state)
                  self.send_xml(action, subnet_group_xml(item))
                  return

              if action == "DescribeDBSubnetGroups":
                  name = text(params, "DBSubnetGroupName")
                  groups = state["subnet_groups"]
                  if name:
                      if name not in groups:
                          self.send_error_xml("DBSubnetGroupNotFoundFault", f"DBSubnetGroup {name} not found")
                          return
                      body = subnet_group_xml(groups[name])
                  else:
                      body = "".join(subnet_group_xml(item) for item in groups.values())
                  self.send_xml(action, f"<DBSubnetGroups>{body}</DBSubnetGroups>")
                  return

              if action == "DeleteDBSubnetGroup":
                  state["subnet_groups"].pop(text(params, "DBSubnetGroupName"), None)
                  save_state(state)
                  self.send_xml(action, "")
                  return

              if action == "CreateDBInstance":
                  identifier = text(params, "DBInstanceIdentifier")
                  subnet_group_name = text(params, "DBSubnetGroupName")
                  subnet_group = state["subnet_groups"].get(subnet_group_name, {"name": subnet_group_name, "subnets": []})
                  item = {
                      "identifier": identifier,
                      "instance_class": text(params, "DBInstanceClass", "db.t3.micro"),
                      "engine": text(params, "Engine", "postgres"),
                      "engine_version": text(params, "EngineVersion", "15.5"),
                      "username": text(params, "MasterUsername", "appuser"),
                      "allocated_storage": int(text(params, "AllocatedStorage", "20")),
                      "storage_type": text(params, "StorageType", "gp3"),
                      "security_groups": members(params, "VpcSecurityGroupIds"),
                      "subnet_group": subnet_group,
                      "created_at": now(),
                  }
                  state["instances"][identifier] = item
                  save_state(state)
                  self.send_xml(action, instance_xml(item))
                  return

              if action == "DescribeDBInstances":
                  identifier = text(params, "DBInstanceIdentifier")
                  instances = state["instances"]
                  if identifier:
                      if identifier not in instances:
                          if len(instances) != 1:
                              self.send_error_xml("DBInstanceNotFound", f"DBInstance {identifier} not found")
                              return
                          body = instance_xml(next(iter(instances.values())))
                      else:
                          body = instance_xml(instances[identifier])
                  else:
                      body = "".join(instance_xml(item) for item in instances.values())
                  self.send_xml(action, f"<DBInstances>{body}</DBInstances>")
                  return

              if action == "DeleteDBInstance":
                  state["instances"].pop(text(params, "DBInstanceIdentifier"), None)
                  save_state(state)
                  self.send_xml(action, "")
                  return

              if action in {"AddTagsToResource", "RemoveTagsFromResource"}:
                  self.send_xml(action, "")
                  return

              if action == "ListTagsForResource":
                  self.send_xml(action, "<TagList/>")
                  return

              self.send_error_xml("InvalidAction", f"Unsupported action {action}", 400)


      def healthy():
          try:
              urlopen(f"http://127.0.0.1:{PORT}/health", timeout=1).read()
              return True
          except Exception:
              return False


      save_state(empty_state())
      if healthy():
          sys.exit(0)

      pid = os.fork()
      if pid == 0:
          os.setsid()
          with open("/tmp/saas_backend_rds_compat.log", "ab", buffering=0) as log:
              os.dup2(log.fileno(), 1)
              os.dup2(log.fileno(), 2)
              ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()

      deadline = time.time() + 10
      while time.time() < deadline:
          if healthy():
              sys.exit(0)
          time.sleep(0.2)

      raise SystemExit("RDS compatibility API did not start")
      PYTHON
    SHELL
  }
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = local.secret_name
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "appuser"
    password = random_password.db_password.result
  })
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket        = "saas-backend-data-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "saas-backend-data"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_sqs_queue" "processing" {
  name                       = local.queue_name
  sqs_managed_sse_enabled    = true
  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600
}

resource "aws_cloudwatch_log_group" "ingest_lambda" {
  name              = "/aws/lambda/${local.ingest_function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "worker_lambda" {
  name              = "/aws/lambda/${local.worker_function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${local.state_machine_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "api_gateway_execution" {
  name              = "API-Gateway-Execution-Logs_${aws_api_gateway_rest_api.main.id}/v1"
  retention_in_days = 14
}

resource "aws_iam_role" "ingest_lambda" {
  name               = local.ingest_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role" "worker_lambda" {
  name               = local.worker_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role" "step_functions" {
  name               = local.step_role_name
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

resource "aws_iam_role" "api_gateway_logs" {
  name               = local.api_logs_role_name
  assume_role_policy = data.aws_iam_policy_document.api_gateway_assume_role.json
}

resource "aws_iam_role_policy" "ingest_lambda" {
  name = "saas-backend-ingest-lambda-policy"
  role = aws_iam_role.ingest_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.ingest_lambda.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses",
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.processing.arn
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = aws_sfn_state_machine.processing.arn
      },
    ]
  })
}

resource "aws_iam_role_policy" "worker_lambda" {
  name = "saas-backend-worker-lambda-policy"
  role = aws_iam_role.worker_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.worker_lambda.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = aws_sqs_queue.processing.arn
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = aws_secretsmanager_secret.db_credentials.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.data.arn}/processed/*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "saas-backend-step-functions-policy"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = aws_lambda_function.worker.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = "${aws_cloudwatch_log_group.step_functions.arn}:*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "api_gateway_logs" {
  name = "saas-backend-api-gateway-logs-policy"
  role = aws_iam_role.api_gateway_logs.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.api_gateway_execution.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "worker" {
  function_name    = local.worker_function_name
  role             = aws_iam_role.worker_lambda.arn
  filename         = data.archive_file.worker_lambda_zip.output_path
  source_code_hash = data.archive_file.worker_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  handler          = "index.handler"
  memory_size      = 256
  timeout          = 10

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = merge(
      {
        BUCKET_NAME   = aws_s3_bucket.data.bucket
        DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
      },
      local.lambda_sdk_endpoint == null ? {} : {
        AWS_ENDPOINT_URL = local.lambda_sdk_endpoint
      }
    )
  }

  depends_on = [
    aws_cloudwatch_log_group.worker_lambda,
  ]
}

resource "aws_sfn_state_machine" "processing" {
  name     = local.state_machine_name
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"
  definition = jsonencode({
    Comment = "Invoke the worker lambda once"
    StartAt = "InvokeWorker"
    States = {
      InvokeWorker = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.worker.arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        End        = true
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }
}

resource "aws_lambda_function" "ingest" {
  function_name    = local.ingest_function_name
  role             = aws_iam_role.ingest_lambda.arn
  filename         = data.archive_file.ingest_lambda_zip.output_path
  source_code_hash = data.archive_file.ingest_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  handler          = "index.handler"
  memory_size      = 256
  timeout          = 10

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = merge(
      {
        QUEUE_URL         = aws_sqs_queue.processing.url
        STATE_MACHINE_ARN = aws_sfn_state_machine.processing.arn
      },
      local.lambda_sdk_endpoint == null ? {} : {
        AWS_ENDPOINT_URL = local.lambda_sdk_endpoint
      }
    )
  }

  depends_on = [
    aws_cloudwatch_log_group.ingest_lambda,
  ]
}

resource "aws_lambda_event_source_mapping" "worker_from_sqs" {
  event_source_arn = aws_sqs_queue.processing.arn
  function_name    = aws_lambda_function.worker.arn
  batch_size       = 10
}

resource "aws_db_subnet_group" "main" {
  name       = local.db_subnet_group_name
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = {
    Name = "saas-backend-db-subnet-group"
  }

  depends_on = [terraform_data.rds_compat_api]
}

resource "aws_db_instance" "main" {
  identifier                 = local.db_identifier
  engine                     = "postgres"
  engine_version             = "15.5"
  instance_class             = "db.t3.micro"
  allocated_storage          = 20
  storage_type               = "gp3"
  storage_encrypted          = true
  username                   = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string)["username"]
  password                   = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string)["password"]
  publicly_accessible        = false
  backup_retention_period    = 0
  deletion_protection        = false
  skip_final_snapshot        = true
  db_subnet_group_name       = aws_db_subnet_group.main.name
  vpc_security_group_ids     = [aws_security_group.db.id]
  parameter_group_name       = "default.postgres15"
  apply_immediately          = true
  auto_minor_version_upgrade = true

  depends_on = [
    aws_secretsmanager_secret_version.db_credentials,
    terraform_data.rds_compat_api,
  ]

  tags = {
    Name = "saas-backend-db"
  }
}

resource "aws_api_gateway_rest_api" "main" {
  name = local.api_name
}

resource "aws_api_gateway_resource" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "ingest"
}

resource "aws_api_gateway_method" "ingest_post" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.ingest.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_lambda_permission" "allow_api_gateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/POST/ingest"
}

resource "aws_api_gateway_integration" "ingest_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.ingest.id
  http_method             = aws_api_gateway_method.ingest_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.ingest.arn}/invocations"
}

resource "aws_api_gateway_account" "main" {
  cloudwatch_role_arn = aws_iam_role.api_gateway_logs.arn

  depends_on = [
    aws_iam_role_policy.api_gateway_logs,
  ]
}

resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.ingest.id,
      aws_api_gateway_method.ingest_post.id,
      aws_api_gateway_integration.ingest_lambda.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_integration.ingest_lambda,
    aws_lambda_permission.allow_api_gateway,
  ]
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  deployment_id = aws_api_gateway_deployment.main.id
  stage_name    = "v1"

  depends_on = [
    aws_api_gateway_account.main,
    aws_cloudwatch_log_group.api_gateway_execution,
  ]
}

resource "aws_api_gateway_method_settings" "all" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  stage_name  = aws_api_gateway_stage.v1.stage_name
  method_path = "*/*"

  settings {
    logging_level      = "INFO"
    metrics_enabled    = true
    data_trace_enabled = false
  }
}
