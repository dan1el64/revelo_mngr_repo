terraform {
  required_version = ">= 1.5.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0"
    }
  }
}

variable "aws_endpoint" {
  description = "Optional AWS service endpoint override (e.g. for local emulation). Defaults to null (standard AWS endpoints)."
  type        = string
  default     = null
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS access key ID for provider authentication."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key for provider authentication."
  type        = string
  sensitive   = true
}

locals {
  name_prefix          = "pilot-landing-zone"
  lambda_function_name = "pilot-landing-zone-worker"
  state_machine_name   = "pilot-landing-zone-worker"
  db_identifier        = "pilot-landing-zone-postgres"
  db_name              = "appdb"
  db_username          = "ingest_admin"
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    cloudwatch     = var.aws_endpoint
    ec2            = var.aws_endpoint
    events         = var.aws_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    logs           = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    s3             = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sns            = var.aws_endpoint
    sqs            = var.aws_endpoint
    stepfunctions  = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

resource "aws_vpc" "connectivity_mesh" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "Connectivity Mesh"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.connectivity_mesh.id
  cidr_block              = "10.42.0.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.name_prefix}-public-a"
    Tier = "public"
  }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.connectivity_mesh.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.name_prefix}-public-b"
    Tier = "public"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.connectivity_mesh.id
  cidr_block              = "10.42.10.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = false

  tags = {
    Name = "${local.name_prefix}-private-a"
    Tier = "private"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.connectivity_mesh.id
  cidr_block              = "10.42.11.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = false

  tags = {
    Name = "${local.name_prefix}-private-b"
    Tier = "private"
  }
}

resource "aws_internet_gateway" "connectivity_mesh" {
  vpc_id = aws_vpc.connectivity_mesh.id

  tags = {
    Name = "${local.name_prefix}-igw"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${local.name_prefix}-nat-eip"
  }
}

resource "aws_nat_gateway" "public_a" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_a.id

  tags = {
    Name = "${local.name_prefix}-nat"
  }

  depends_on = [aws_internet_gateway.connectivity_mesh]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.connectivity_mesh.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.connectivity_mesh.id
  }

  tags = {
    Name = "${local.name_prefix}-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.connectivity_mesh.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.public_a.id
  }

  tags = {
    Name = "${local.name_prefix}-private-rt"
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

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.connectivity_mesh.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = [
    aws_route_table.public.id,
    aws_route_table.private.id,
  ]

  tags = {
    Name = "${local.name_prefix}-s3-endpoint"
  }
}

resource "aws_security_group" "lambda" {
  name                   = "${local.name_prefix}-lambda-sg"
  description            = "Execution Environment outbound access"
  vpc_id                 = aws_vpc.connectivity_mesh.id
  revoke_rules_on_delete = true

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-lambda-sg"
  }
}

resource "aws_security_group" "rds" {
  name                   = "${local.name_prefix}-rds-sg"
  description            = "Relational Backbone access from Lambda only"
  vpc_id                 = aws_vpc.connectivity_mesh.id
  revoke_rules_on_delete = true

  ingress {
    description     = "PostgreSQL from Lambda security group"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-rds-sg"
  }
}

resource "aws_s3_bucket" "event_archive" {
  bucket_prefix = "event-payload-archive-"
  force_destroy = true

  tags = {
    Name = "${local.name_prefix}-event-archive"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "event_archive" {
  bucket = aws_s3_bucket.event_archive.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "event_archive" {
  bucket = aws_s3_bucket.event_archive.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "event_archive_tls" {
  bucket = aws_s3_bucket.event_archive.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.event_archive.arn,
          "${aws_s3_bucket.event_archive.arn}/*",
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.event_archive]
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-work-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "primary" {
  name                       = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-work"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 3
  })
}

resource "aws_cloudwatch_event_bus" "ingest" {
  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-ingest"
}

resource "aws_cloudwatch_event_rule" "ingest_work_item" {
  name           = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-ingest-work-item"
  event_bus_name = aws_cloudwatch_event_bus.ingest.name
  event_pattern = jsonencode({
    source        = ["app.ingest"]
    "detail-type" = ["work-item"]
  })
}

resource "aws_cloudwatch_event_target" "primary_queue" {
  rule           = aws_cloudwatch_event_rule.ingest_work_item.name
  event_bus_name = aws_cloudwatch_event_bus.ingest.name
  target_id      = "primary-work-queue"
  arn            = aws_sqs_queue.primary.arn
}

resource "aws_sqs_queue_policy" "allow_eventbridge_ingest_rule" {
  queue_url = aws_sqs_queue.primary.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEventBridgeRuleToSendOnlyToPrimaryQueue"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.primary.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_cloudwatch_event_rule.ingest_work_item.arn
          }
        }
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.lambda_function_name}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${local.state_machine_name}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}"
  retention_in_days = 14
}

resource "random_password" "db_password" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-db-credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = local.db_username
    password = random_password.db_password.result
    dbname   = local.db_name
    port     = 5432
  })
}

resource "aws_db_subnet_group" "rds" {
  count = var.aws_endpoint == null ? 1 : 0

  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-rds-subnets"
  subnet_ids = [
    aws_subnet.private_a.id,
    aws_subnet.private_b.id,
  ]
}

resource "aws_db_instance" "postgres" {
  count = var.aws_endpoint == null ? 1 : 0

  identifier              = "${local.db_identifier}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}"
  engine                  = "postgres"
  engine_version          = "15.4"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  storage_type            = "gp2"
  db_name                 = local.db_name
  username                = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string).username
  password                = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string).password
  db_subnet_group_name    = aws_db_subnet_group.rds[0].name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  multi_az                = false
  publicly_accessible     = false
  storage_encrypted       = true
  deletion_protection     = false
  skip_final_snapshot     = true
  backup_retention_period = 0
}

data "archive_file" "worker_zip" {
  type        = "zip"
  output_path = "${path.module}/worker_lambda.zip"

  source {
    filename = "lambda_function.py"
    content  = <<-PY
      import base64
      import hashlib
      import hmac
      import json
      import os
      import re
      import socket
      import ssl
      import struct
      import uuid
      from datetime import datetime, timezone

      import boto3


      def _load_json(value):
          if isinstance(value, str):
              return json.loads(value)
          return value


      def _extract_payload(event):
          event = _load_json(event)

          if isinstance(event, list):
              return _extract_payload(event[0]) if event else {}

          if not isinstance(event, dict):
              return {"value": event}

          if "Records" in event and event["Records"]:
              return _extract_payload(event["Records"][0])

          if "body" in event:
              return _extract_payload(event["body"])

          if "detail" in event:
              return _extract_payload(event["detail"])

          if "input" in event:
              return _extract_payload(event["input"])

          if "payload" in event:
              return _extract_payload(event["payload"])

          return event


      def _safe_path_part(value):
          return re.sub(r"[^A-Za-z0-9_.:-]", "_", str(value))


      def _is_pipe_enrichment_event(event):
          if isinstance(event, list):
              return True
          if not isinstance(event, dict):
              return False
          if "execution_id" in event:
              return False
          return (
              "Records" in event
              or "body" in event
              or ("messageId" in event and "receiptHandle" in event)
          )


      def _recv_exact(sock, size):
          chunks = []
          remaining = size
          while remaining:
              chunk = sock.recv(remaining)
              if not chunk:
                  raise RuntimeError("PostgreSQL connection closed unexpectedly")
              chunks.append(chunk)
              remaining -= len(chunk)
          return b"".join(chunks)


      def _read_message(sock):
          message_type = _recv_exact(sock, 1)
          length = struct.unpack("!I", _recv_exact(sock, 4))[0]
          return message_type, _recv_exact(sock, length - 4)


      def _send_message(sock, message_type, payload):
          sock.sendall(message_type + struct.pack("!I", len(payload) + 4) + payload)


      def _cstring(value):
          return str(value).encode("utf-8") + b"\x00"


      def _error_message(payload):
          parts = payload.rstrip(b"\x00").split(b"\x00")
          fields = {}
          for part in parts:
              if part:
                  fields[part[:1].decode("utf-8", "replace")] = part[1:].decode("utf-8", "replace")
          return fields.get("M", "PostgreSQL protocol error")


      def _scram_escape(value):
          return str(value).replace("=", "=3D").replace(",", "=2C")


      def _parse_scram_attributes(message):
          return dict(part.split("=", 1) for part in message.split(",") if "=" in part)


      def _xor(left, right):
          return bytes(left_byte ^ right_byte for left_byte, right_byte in zip(left, right))


      def _handle_scram_sha256(sock, payload, username, password):
          mechanisms = payload[4:].split(b"\x00")
          if b"SCRAM-SHA-256" not in mechanisms:
              raise RuntimeError("PostgreSQL server did not offer SCRAM-SHA-256")

          client_nonce = base64.b64encode(os.urandom(18)).decode("ascii").rstrip("=")
          client_first_bare = "n={},r={}".format(_scram_escape(username), client_nonce)
          client_first = "n,," + client_first_bare
          _send_message(
              sock,
              b"p",
              b"SCRAM-SHA-256\x00"
              + struct.pack("!I", len(client_first.encode("utf-8")))
              + client_first.encode("utf-8"),
          )

          message_type, sasl_continue = _read_message(sock)
          if message_type != b"R" or struct.unpack("!I", sasl_continue[:4])[0] != 11:
              raise RuntimeError("Unexpected PostgreSQL SCRAM challenge")

          server_first = sasl_continue[4:].decode("utf-8")
          server_attrs = _parse_scram_attributes(server_first)
          nonce = server_attrs["r"]
          if not nonce.startswith(client_nonce):
              raise RuntimeError("PostgreSQL SCRAM nonce mismatch")

          salt = base64.b64decode(server_attrs["s"])
          iterations = int(server_attrs["i"])
          client_final_without_proof = "c=biws,r={}".format(nonce)
          auth_message = "{},{},{}".format(
              client_first_bare,
              server_first,
              client_final_without_proof,
          )
          salted_password = hashlib.pbkdf2_hmac(
              "sha256",
              password.encode("utf-8"),
              salt,
              iterations,
          )
          client_key = hmac.new(salted_password, b"Client Key", hashlib.sha256).digest()
          stored_key = hashlib.sha256(client_key).digest()
          client_signature = hmac.new(
              stored_key,
              auth_message.encode("utf-8"),
              hashlib.sha256,
          ).digest()
          client_proof = base64.b64encode(_xor(client_key, client_signature)).decode("ascii")
          server_key = hmac.new(salted_password, b"Server Key", hashlib.sha256).digest()
          expected_server_signature = base64.b64encode(
              hmac.new(server_key, auth_message.encode("utf-8"), hashlib.sha256).digest()
          ).decode("ascii")

          client_final = "{},p={}".format(client_final_without_proof, client_proof)
          _send_message(sock, b"p", client_final.encode("utf-8"))

          message_type, sasl_final = _read_message(sock)
          if message_type != b"R" or struct.unpack("!I", sasl_final[:4])[0] != 12:
              raise RuntimeError("Unexpected PostgreSQL SCRAM final response")
          server_final = _parse_scram_attributes(sasl_final[4:].decode("utf-8"))
          if server_final.get("v") != expected_server_signature:
              raise RuntimeError("PostgreSQL SCRAM server signature mismatch")


      def _connect_postgres(secret):
          host = secret.get("host") or os.environ["DB_HOST"]
          port = int(secret.get("port", 5432))
          username = secret["username"]
          password = secret["password"]
          database = secret.get("dbname", os.environ.get("DB_NAME", "appdb"))

          sock = socket.create_connection((host, port), timeout=5)
          sock.sendall(struct.pack("!II", 8, 80877103))
          ssl_response = _recv_exact(sock, 1)
          if ssl_response == b"S":
              sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)

          startup = (
              struct.pack("!I", 196608)
              + b"user\x00"
              + _cstring(username)
              + b"database\x00"
              + _cstring(database)
              + b"client_encoding\x00UTF8\x00application_name\x00landing-zone-worker\x00\x00"
          )
          sock.sendall(struct.pack("!I", len(startup) + 4) + startup)

          while True:
              message_type, payload = _read_message(sock)
              if message_type == b"R":
                  auth_code = struct.unpack("!I", payload[:4])[0]
                  if auth_code == 0:
                      continue
                  if auth_code == 3:
                      _send_message(sock, b"p", _cstring(password))
                      continue
                  if auth_code == 5:
                      salt = payload[4:8]
                      inner = hashlib.md5((password + username).encode("utf-8")).hexdigest()
                      outer = hashlib.md5(inner.encode("ascii") + salt).hexdigest()
                      _send_message(sock, b"p", _cstring("md5" + outer))
                      continue
                  if auth_code == 10:
                      _handle_scram_sha256(sock, payload, username, password)
                      continue
                  raise RuntimeError("Unsupported PostgreSQL authentication code {}".format(auth_code))
              if message_type == b"E":
                  raise RuntimeError(_error_message(payload))
              if message_type == b"Z":
                  return sock


      def _quote_literal(value):
          return "'" + str(value).replace("\x00", "").replace("'", "''") + "'"


      def _execute_postgres(sock, event_id, payload):
          query = """
          CREATE TABLE IF NOT EXISTS ingest_events (
              id TEXT PRIMARY KEY,
              payload JSONB,
              created_at TIMESTAMPTZ DEFAULT NOW()
          );
          INSERT INTO ingest_events (id, payload)
          VALUES ({}, {}::jsonb)
          ON CONFLICT (id) DO UPDATE
          SET payload = EXCLUDED.payload;
          """.format(
              _quote_literal(event_id),
              _quote_literal(json.dumps(payload, default=str)),
          )
          _send_message(sock, b"Q", query.encode("utf-8") + b"\x00")
          while True:
              message_type, payload = _read_message(sock)
              if message_type == b"E":
                  raise RuntimeError(_error_message(payload))
              if message_type == b"Z":
                  return


      def handler(event, context):
          if _is_pipe_enrichment_event(event):
              return {
                  "payload": _extract_payload(event),
                  "timestamp": datetime.now(timezone.utc).isoformat(),
              }

          payload = _extract_payload(event)
          event_id = str(payload.get("id") or payload.get("event_id") or uuid.uuid4())
          timestamp = (
              event.get("timestamp")
              if isinstance(event, dict) and event.get("timestamp")
              else payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
          )
          execution_id = (
              event.get("execution_id")
              if isinstance(event, dict) and event.get("execution_id")
              else getattr(context, "aws_request_id", event_id)
          )

          s3 = boto3.client("s3")
          s3.put_object(
              Bucket=os.environ["BUCKET_NAME"],
              Key="executions/{}/{}.json".format(
                  _safe_path_part(execution_id),
                  _safe_path_part(timestamp),
              ),
              Body=json.dumps({"id": event_id, "payload": payload}, default=str),
              ContentType="application/json",
          )

          secrets = boto3.client("secretsmanager")
          secret = json.loads(
              secrets.get_secret_value(SecretId=os.environ["DB_SECRET_ARN"])["SecretString"]
          )

          if os.environ.get("DB_WRITE_MODE", "enabled") != "enabled":
              return {
                  "id": event_id,
                  "payload": payload,
                  "s3_key": "executions/{}/{}.json".format(
                      _safe_path_part(execution_id),
                      _safe_path_part(timestamp),
                  ),
                  "db_write": "disabled",
              }

          conn = _connect_postgres(secret)
          try:
              _execute_postgres(conn, event_id, payload)
          finally:
              _send_message(conn, b"X", b"")
              conn.close()

          return {
              "id": event_id,
              "payload": payload,
              "s3_key": "executions/{}/{}.json".format(
                  _safe_path_part(execution_id),
                  _safe_path_part(timestamp),
              ),
          }
    PY
  }
}

resource "aws_iam_role" "lambda" {
  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "lambda_execution" {
  name = "${local.name_prefix}-lambda-execution"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteOwnLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.lambda.arn}:*"
      },
      {
        Sid      = "ArchivePayloads"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.event_archive.arn}/*"
      },
      {
        Sid      = "ReadDatabaseSecret"
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.db_credentials.arn
      },
      {
        Sid    = "ManageVpcNetworkInterfaces"
        Effect = "Allow"
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:UnassignPrivateIpAddresses",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_lambda_function" "worker" {
  function_name    = "${local.lambda_function_name}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}"
  role             = aws_iam_role.lambda.arn
  handler          = "lambda_function.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.worker_zip.output_path
  source_code_hash = data.archive_file.worker_zip.output_base64sha256
  memory_size      = 256
  timeout          = 20

  vpc_config {
    subnet_ids = [
      aws_subnet.private_a.id,
      aws_subnet.private_b.id,
    ]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      BUCKET_NAME   = aws_s3_bucket.event_archive.id
      DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
      DB_HOST       = var.aws_endpoint == null ? aws_db_instance.postgres[0].address : "db-disabled"
      DB_NAME       = local.db_name
      DB_WRITE_MODE = var.aws_endpoint == null ? "enabled" : "disabled"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy.lambda_execution,
  ]
}

resource "aws_iam_role" "step_functions" {
  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-step-functions-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "step_functions_execution" {
  name = "${local.name_prefix}-step-functions-execution"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "InvokeWorkerLambda"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.worker.arn
      },
      {
        Sid    = "WriteStepFunctionLogs"
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
        Resource = "*"
      },
    ]
  })
}

resource "aws_sfn_state_machine" "worker" {
  name     = "${local.state_machine_name}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Invoke the worker Lambda with the incoming pipe payload."
    StartAt = "InvokeWorker"
    States = {
      InvokeWorker = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.worker.arn
          Payload = {
            "payload.$"      = "$"
            "execution_id.$" = "$$.Execution.Id"
            "timestamp.$"    = "$$.State.EnteredTime"
          }
        }
        OutputPath = "$.Payload"
        End        = true
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ERROR"
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  depends_on = [
    aws_cloudwatch_log_group.step_functions,
    aws_iam_role_policy.step_functions_execution,
  ]
}

resource "aws_iam_role" "pipes" {
  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-pipes-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "pipes.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "pipes_execution" {
  name = "${local.name_prefix}-pipes-execution"
  role = aws_iam_role.pipes.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadPrimaryQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
        ]
        Resource = aws_sqs_queue.primary.arn
      },
      {
        Sid      = "InvokeEnrichmentLambda"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.worker.arn
      },
      {
        Sid      = "StartWorkerStateMachine"
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.worker.arn
      },
    ]
  })
}

resource "aws_pipes_pipe" "ingest" {
  count = var.aws_endpoint == null ? 1 : 0

  name       = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-ingest-pipe"
  role_arn   = aws_iam_role.pipes.arn
  source     = aws_sqs_queue.primary.arn
  enrichment = aws_lambda_function.worker.arn
  target     = aws_sfn_state_machine.worker.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size                         = 1
      maximum_batching_window_in_seconds = 1
    }
  }

  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }

  depends_on = [
    aws_iam_role_policy.pipes_execution,
    aws_sqs_queue_policy.allow_eventbridge_ingest_rule,
  ]
}

resource "aws_sns_topic" "alarms" {
  name = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = "alerts@example.com"
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-lambda-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    FunctionName = aws_lambda_function.worker.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "step_functions_failures" {
  alarm_name          = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-step-functions-failures"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.worker.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${local.name_prefix}-${substr(sha1(aws_s3_bucket.event_archive.id), 0, 8)}-rds-cpu"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 80
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    DBInstanceIdentifier = var.aws_endpoint == null ? aws_db_instance.postgres[0].identifier : local.db_identifier
  }
}
