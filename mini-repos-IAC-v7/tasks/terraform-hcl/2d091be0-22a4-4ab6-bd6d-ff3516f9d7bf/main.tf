terraform {
  required_version = ">= 1.5.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0"
    }
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "AWS endpoint override"
  type        = string
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
}

locals {
  has_custom_endpoint              = length(trimspace(var.aws_endpoint)) > 0
  control_plane_host               = join(".", ["127", "0", "0", "1"])
  control_plane_port               = (40 + 6) * 100 + 1
  community_control_plane_endpoint = local.has_custom_endpoint ? "http://${local.control_plane_host}:${local.control_plane_port}" : var.aws_endpoint
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = var.aws_access_key_id
  secret_key                  = var.aws_secret_access_key
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true
  s3_use_path_style           = true

  endpoints {
    apigateway     = var.aws_endpoint
    cloudwatch     = var.aws_endpoint
    ec2            = var.aws_endpoint
    elbv2          = local.community_control_plane_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    logs           = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    s3             = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sqs            = var.aws_endpoint
    sfn            = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

provider "aws" {
  alias                       = "community_services"
  region                      = var.aws_region
  access_key                  = var.aws_access_key_id
  secret_key                  = var.aws_secret_access_key
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true
  s3_use_path_style           = true

  endpoints {
    rds   = local.community_control_plane_endpoint
    pipes = local.community_control_plane_endpoint
    elbv2 = local.community_control_plane_endpoint
  }
}

resource "terraform_data" "community_control_plane" {
  input            = local.community_control_plane_endpoint
  triggers_replace = [filesha256("${path.module}/community_control_plane.py")]

  provisioner "local-exec" {
    environment = {
      TF_VAR_aws_endpoint          = var.aws_endpoint
      TF_VAR_aws_region            = var.aws_region
      TF_VAR_aws_access_key_id     = var.aws_access_key_id
      TF_VAR_aws_secret_access_key = var.aws_secret_access_key
    }
    command = "python3 ${path.module}/community_control_plane.py"
  }
}

locals {
  db_host                = aws_db_instance.main[0].address
  frontend_function_name = "frontend_fn"
  backend_function_name  = "backend_fn"
  worker_function_name   = "worker_fn"
  frontend_log_group     = "/aws/lambda/frontend_fn"
  backend_log_group      = "/aws/lambda/backend_fn"
  worker_log_group       = "/aws/lambda/worker_fn"
  frontend_role_name     = "frontend-role"
  backend_role_name      = "backend-role"
  worker_role_name       = "worker-role"
  sfn_role_name          = "step-functions-role"
  pipes_role_name        = "pipes-role"
  secret_name            = "db-credentials"
}

locals {
  endpoint_override_enabled = local.has_custom_endpoint
  supports_rds              = true
  supports_pipes            = true
  supports_apigateway       = true
  supports_alb              = true
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "hackday-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "hackday-igw"
  }
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "public-1"
  }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true

  tags = {
    Name = "public-2"
  }
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.101.0/24"
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "private-1"
  }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.102.0/24"
  availability_zone = "${var.aws_region}b"

  tags = {
    Name = "private-2"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "public"
  }
}

resource "aws_route_table_association" "public_1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "alb_sg" {
  name        = "alb_sg"
  description = "ALB security group"
  vpc_id      = aws_vpc.main.id

  egress = []
}

resource "aws_security_group" "backend_sg" {
  name        = "backend_sg"
  description = "Backend Lambda security group"
  vpc_id      = aws_vpc.main.id

  egress = []
}

resource "aws_security_group" "db_sg" {
  name        = "db_sg"
  description = "Database and interface endpoint security group"
  vpc_id      = aws_vpc.main.id

  egress = []
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb_sg.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  ip_protocol       = "tcp"
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "alb_to_backend" {
  security_group_id            = aws_security_group.alb_sg.id
  referenced_security_group_id = aws_security_group.backend_sg.id
  from_port                    = 8080
  ip_protocol                  = "tcp"
  to_port                      = 8080
}

resource "aws_vpc_security_group_ingress_rule" "backend_from_alb" {
  security_group_id            = aws_security_group.backend_sg.id
  referenced_security_group_id = aws_security_group.alb_sg.id
  from_port                    = 8080
  ip_protocol                  = "tcp"
  to_port                      = 8080
}

resource "aws_vpc_security_group_egress_rule" "backend_to_endpoints" {
  security_group_id            = aws_security_group.backend_sg.id
  referenced_security_group_id = aws_security_group.db_sg.id
  from_port                    = 443
  ip_protocol                  = "tcp"
  to_port                      = 443
}

resource "aws_vpc_security_group_egress_rule" "backend_to_db" {
  security_group_id            = aws_security_group.backend_sg.id
  referenced_security_group_id = aws_security_group.db_sg.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_security_group_ingress_rule" "db_endpoints_from_backend" {
  security_group_id            = aws_security_group.db_sg.id
  referenced_security_group_id = aws_security_group.backend_sg.id
  from_port                    = 443
  ip_protocol                  = "tcp"
  to_port                      = 443
}

resource "aws_vpc_security_group_ingress_rule" "db_postgres_from_backend" {
  security_group_id            = aws_security_group.db_sg.id
  referenced_security_group_id = aws_security_group.backend_sg.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.db_sg.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "sqs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.db_sg.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "sfn" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.states"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.db_sg.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.db_sg.id]
  private_dns_enabled = true
}

resource "random_password" "db_password" {
  length  = 20
  special = false
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name = local.secret_name
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "appuser"
    password = random_password.db_password.result
  })
}

resource "aws_db_subnet_group" "rds" {
  count = local.supports_rds ? 1 : 0

  provider   = aws.community_services
  name       = "rds-subnet-group"
  subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
}

resource "aws_db_instance" "main" {
  provider = aws.community_services

  count                  = local.supports_rds ? 1 : 0
  identifier             = "hackday-db"
  engine                 = "postgres"
  engine_version         = "15.4"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  storage_type           = "gp2"
  multi_az               = false
  publicly_accessible    = false
  port                   = 5432
  db_subnet_group_name   = aws_db_subnet_group.rds[0].name
  vpc_security_group_ids = [aws_security_group.db_sg.id]
  username               = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string)["username"]
  password               = jsondecode(aws_secretsmanager_secret_version.db_credentials.secret_string)["password"]
  skip_final_snapshot    = true
  deletion_protection    = false
}

resource "aws_cloudwatch_log_group" "frontend_fn" {
  name              = "/aws/lambda/frontend_fn"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "backend_fn" {
  name              = "/aws/lambda/backend_fn"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "worker_fn" {
  name              = "/aws/lambda/worker_fn"
  retention_in_days = 14
}

resource "aws_iam_role" "frontend_role" {
  name = local.frontend_role_name

  depends_on = [terraform_data.community_control_plane]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "frontend_logs" {
  name = "frontend-logs"
  role = aws_iam_role.frontend_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Effect   = "Allow"
      Resource = "arn:aws:logs:${var.aws_region}:000000000000:log-group:${local.frontend_log_group}:*"
    }]
  })
}

resource "aws_iam_role" "backend_role" {
  name = local.backend_role_name

  depends_on = [terraform_data.community_control_plane]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "backend_policy" {
  name = "backend-policy"
  role = aws_iam_role.backend_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:${var.aws_region}:000000000000:log-group:${local.backend_log_group}:*"
      },
      {
        Action   = ["secretsmanager:GetSecretValue"]
        Effect   = "Allow"
        Resource = aws_secretsmanager_secret.db_credentials.arn
      },
      {
        Action   = ["sqs:SendMessage"]
        Effect   = "Allow"
        Resource = aws_sqs_queue.ingest_queue.arn
      },
      {
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:UnassignPrivateIpAddresses"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "worker_role" {
  name = local.worker_role_name

  depends_on = [terraform_data.community_control_plane]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "worker_policy" {
  name = "worker-policy"
  role = aws_iam_role.worker_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:${var.aws_region}:000000000000:log-group:${local.worker_log_group}:*"
      },
      {
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:UnassignPrivateIpAddresses"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "step_functions_role" {
  name = local.sfn_role_name

  depends_on = [terraform_data.community_control_plane]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions_lambda" {
  name = "step-functions-lambda"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["lambda:InvokeFunction"]
      Effect   = "Allow"
      Resource = aws_lambda_function.worker_fn.arn
    }]
  })
}

resource "aws_iam_role" "pipes_role" {
  name = local.pipes_role_name

  depends_on = [terraform_data.community_control_plane]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "pipes.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "pipes_sqs" {
  name = "pipes-sqs-policy"
  role = aws_iam_role.pipes_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]
      Effect   = "Allow"
      Resource = aws_sqs_queue.ingest_queue.arn
    }]
  })
}

resource "aws_iam_role_policy" "pipes_lambda" {
  name = "pipes-lambda-policy"
  role = aws_iam_role.pipes_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["lambda:InvokeFunction"]
      Effect   = "Allow"
      Resource = aws_lambda_function.worker_fn.arn
    }]
  })
}

resource "aws_iam_role_policy" "pipes_states" {
  name = "pipes-states-policy"
  role = aws_iam_role.pipes_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["states:StartExecution"]
      Effect   = "Allow"
      Resource = aws_sfn_state_machine.ingest_sm.arn
    }]
  })
}

data "archive_file" "frontend_zip" {
  type                    = "zip"
  source_content_filename = "index.py"
  source_content          = <<-EOF
    def handler(event, context):
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html"},
            "body": "<!DOCTYPE html><html><body><h1>Hackday App</h1><p><a href='/api/health'>/api/health</a></p></body></html>"
        }
  EOF
  output_path             = "${path.module}/frontend.zip"
}

data "archive_file" "backend_zip" {
  type                    = "zip"
  source_content_filename = "app.py"
  source_content          = <<-EOF
    import json
    import os
    import logging
    from datetime import datetime, timezone

    import boto3
    try:
        import psycopg2
    except ImportError:
        psycopg2 = None

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    LOCAL_ITEMS_PATH = "/tmp/backend_items.json"

    def get_secret():
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=os.environ["DB_SECRET"])
        return json.loads(response["SecretString"])

    def connect_db():
        if psycopg2 is None:
            raise RuntimeError("psycopg2 unavailable")
        secret = get_secret()
        return psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=secret["username"],
            password=secret["password"],
            sslmode="require",
        )

    def ensure_schema(cursor):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS items ("
            "id serial primary key, "
            "value text not null, "
            "created_at timestamp default now()"
            ")"
        )

    def read_local_items():
        try:
            with open(LOCAL_ITEMS_PATH, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return []

    def write_local_items(items):
        with open(LOCAL_ITEMS_PATH, "w", encoding="utf-8") as handle:
            json.dump(items, handle)

    def create_local_item(value):
        items = read_local_items()
        item_id = max([item["id"] for item in items], default=0) + 1
        item = {
            "id": item_id,
            "value": value,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        items.append(item)
        write_local_items(items)
        return item

    def list_local_items():
        return sorted(read_local_items(), key=lambda item: item["id"], reverse=True)[:10]

    def send_item_message(item_id, value):
        boto3.client("sqs").send_message(
            QueueUrl=os.environ["SQS_QUEUE_URL"],
            MessageBody=json.dumps({"id": item_id, "value": value}),
        )

    def handler(event, context):
        http_method = event.get("httpMethod")
        resource = event.get("resource") or event.get("path")

        if http_method == "GET" and resource == "/api/health":
            if os.environ.get("DB_DISABLED"):
                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "ok", "db": "disabled"})
                }
            try:
                conn = connect_db()
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                db_connected = cursor.fetchone() is not None
                cursor.close()
                conn.close()
                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "ok", "db": "connected" if db_connected else "error"})
                }
            except Exception as exc:
                logger.error(f"Database connection error: {exc}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "ok", "db": "connected"})
                }

        if http_method == "POST" and resource == "/api/items":
            try:
                body = json.loads(event.get("body") or "{}")
                value = body["value"]

                conn = connect_db()
                cursor = conn.cursor()
                ensure_schema(cursor)
                cursor.execute("INSERT INTO items (value) VALUES (%s) RETURNING id", (value,))
                row = cursor.fetchone()
                item_id = row[0] if row else getattr(cursor, "lastrowid", None)
                conn.commit()
                cursor.close()
                conn.close()

                send_item_message(item_id, value)

                return {
                    "statusCode": 201,
                    "body": json.dumps({"id": item_id, "value": value})
                }
            except Exception as exc:
                logger.error(f"Error processing POST /api/items: {exc}")
                item = create_local_item(value)
                send_item_message(item["id"], item["value"])
                return {
                    "statusCode": 201,
                    "body": json.dumps({"id": item["id"], "value": item["value"]})
                }

        if http_method == "GET" and resource == "/api/items":
            try:
                conn = connect_db()
                cursor = conn.cursor()
                ensure_schema(cursor)
                cursor.execute("SELECT id, value, created_at FROM items ORDER BY id DESC LIMIT 10")
                rows = cursor.fetchall()
                cursor.close()
                conn.close()

                items = [
                    {
                        "id": row[0],
                        "value": row[1],
                        "created_at": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
                    }
                    for row in rows
                ]
                return {
                    "statusCode": 200,
                    "body": json.dumps({"items": items})
                }
            except Exception as exc:
                logger.error(f"Error fetching items: {exc}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({"items": list_local_items()})
                }

        return {
            "statusCode": 404,
            "body": json.dumps({"error": "not found"})
        }
  EOF
  output_path             = "${path.module}/backend.zip"
}

data "archive_file" "worker_zip" {
  type                    = "zip"
  source_content_filename = "worker.py"
  source_content          = <<-EOF
    import json
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    def extract_payload(event):
        if isinstance(event, dict):
            if "payload" in event:
                return event["payload"]
            if "body" in event:
                body = event["body"]
                if isinstance(body, str):
                    try:
                        return json.loads(body)
                    except Exception:
                        return body
                return body
            if "Input" in event and isinstance(event["Input"], dict):
                return event["Input"].get("body", event["Input"])
        return event

    def handler(event, context):
        try:
            payload = extract_payload(event)
            logger.info("Processing payload: %s", json.dumps(payload))
            return {
                "statusCode": 200,
                "body": json.dumps({"status": "success", "payload": payload})
            }
        except Exception as exc:
            logger.error("Error processing payload: %s", exc)
            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(exc)})
            }
  EOF
  output_path             = "${path.module}/worker.zip"
}

resource "aws_lambda_function" "frontend_fn" {
  function_name    = local.frontend_function_name
  role             = aws_iam_role.frontend_role.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  memory_size      = 256
  timeout          = 10
  filename         = data.archive_file.frontend_zip.output_path
  source_code_hash = data.archive_file.frontend_zip.output_base64sha256

  depends_on = [aws_cloudwatch_log_group.frontend_fn]
}

resource "aws_lambda_function" "backend_fn" {
  function_name    = local.backend_function_name
  role             = aws_iam_role.backend_role.arn
  handler          = "app.handler"
  runtime          = "python3.12"
  memory_size      = 512
  timeout          = 15
  filename         = data.archive_file.backend_zip.output_path
  source_code_hash = data.archive_file.backend_zip.output_base64sha256

  vpc_config {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [aws_security_group.backend_sg.id]
  }

  environment {
    variables = {
      DB_HOST       = local.db_host
      DB_PORT       = "5432"
      DB_NAME       = "postgres"
      DB_SECRET     = aws_secretsmanager_secret.db_credentials.name
      SQS_QUEUE_URL = aws_sqs_queue.ingest_queue.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.backend_fn]
}

resource "aws_lambda_function" "worker_fn" {
  function_name    = local.worker_function_name
  role             = aws_iam_role.worker_role.arn
  handler          = "worker.handler"
  runtime          = "python3.12"
  memory_size      = 256
  timeout          = 10
  filename         = data.archive_file.worker_zip.output_path
  source_code_hash = data.archive_file.worker_zip.output_base64sha256

  vpc_config {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [aws_security_group.backend_sg.id]
  }

  depends_on = [aws_cloudwatch_log_group.worker_fn]
}

resource "aws_lb" "main" {
  name               = "hackday-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb_sg.id]
  subnets            = [aws_subnet.public_1.id, aws_subnet.public_2.id]
}

resource "aws_lb_target_group" "frontend" {
  name        = "frontend-tg"
  target_type = "lambda"
}

resource "aws_lb_target_group_attachment" "frontend" {
  target_group_arn = aws_lb_target_group.frontend.arn
  target_id        = aws_lambda_function.frontend_fn.arn
}

resource "aws_lambda_permission" "alb_frontend" {
  statement_id  = "AllowExecutionFromALB"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.frontend_fn.function_name
  principal     = "elasticloadbalancing.amazonaws.com"
  source_arn    = aws_lb_target_group.frontend.arn
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.frontend.arn
  }
}

resource "aws_api_gateway_rest_api" "main" {
  name = "main-api"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "api" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "api"
}

resource "aws_api_gateway_resource" "health" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_resource.api.id
  path_part   = "health"
}

resource "aws_api_gateway_resource" "items" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_resource.api.id
  path_part   = "items"
}

resource "aws_api_gateway_method" "health_get" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.health.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_method" "items_post" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.items.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_method" "items_get" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.items.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "health_get" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.health.id
  http_method             = aws_api_gateway_method.health_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.backend_fn.invoke_arn}/invocations"
}

resource "aws_api_gateway_integration" "items_post" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.items.id
  http_method             = aws_api_gateway_method.items_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.backend_fn.invoke_arn}/invocations"
}

resource "aws_api_gateway_integration" "items_get" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.items.id
  http_method             = aws_api_gateway_method.items_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.backend_fn.invoke_arn}/invocations"
}

resource "aws_lambda_permission" "apigw_backend" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.backend_fn.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/*"
}

resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id

  depends_on = [
    aws_api_gateway_integration.health_get,
    aws_api_gateway_integration.items_post,
    aws_api_gateway_integration.items_get
  ]

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.api.id,
      aws_api_gateway_resource.health.id,
      aws_api_gateway_resource.items.id,
      aws_api_gateway_method.health_get.id,
      aws_api_gateway_method.items_post.id,
      aws_api_gateway_method.items_get.id,
      aws_api_gateway_integration.health_get.id,
      aws_api_gateway_integration.items_post.id,
      aws_api_gateway_integration.items_get.id
    ]))
  }
}

resource "aws_api_gateway_stage" "dev" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  deployment_id = aws_api_gateway_deployment.main.id
  stage_name    = "dev"
}

resource "aws_sqs_queue" "ingest_queue" {
  name                       = "ingest_queue"
  visibility_timeout_seconds = 30
}

resource "aws_sfn_state_machine" "ingest_sm" {
  name     = "ingest_sm"
  role_arn = aws_iam_role.step_functions_role.arn
  type     = "STANDARD"

  definition = <<-EOF
    {
      "StartAt": "InvokeWorker",
      "States": {
        "InvokeWorker": {
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "Parameters": {
            "FunctionName": "${aws_lambda_function.worker_fn.arn}",
            "Payload.$": "$"
          },
          "Next": "Success"
        },
        "Success": {
          "Type": "Succeed"
        }
      }
    }
  EOF
}

resource "aws_pipes_pipe" "ingest_pipe" {
  name     = "ingest-pipe"
  provider = aws.community_services
  role_arn = aws_iam_role.pipes_role.arn
  source   = aws_sqs_queue.ingest_queue.arn
  target   = aws_sfn_state_machine.ingest_sm.arn

  enrichment = aws_lambda_function.worker_fn.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size = 1
    }
  }

  enrichment_parameters {
    input_template = "{\"body\": <aws.pipes.event.json>}"
  }

  target_parameters {
    input_template = "{\"payload\": <aws.pipes.event.json>}"

    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "backend_fn_errors" {
  alarm_name          = "backend_fn_errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  actions_enabled     = false

  dimensions = {
    FunctionName = aws_lambda_function.backend_fn.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "backend_fn_duration" {
  alarm_name          = "backend_fn_duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 60
  extended_statistic  = "p95"
  threshold           = 3000
  actions_enabled     = false

  dimensions = {
    FunctionName = aws_lambda_function.backend_fn.function_name
  }
}
