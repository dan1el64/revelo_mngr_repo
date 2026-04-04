terraform {
  required_version = ">= 1.5.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.5"
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
  description = "AWS region used by the provider."
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS access key ID used by the provider."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key used by the provider."
  type        = string
  sensitive   = true
}

variable "aws_endpoint" {
  description = "Optional AWS-compatible endpoint override for alternate environments."
  type        = string
  default     = null
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
    apigatewayv2   = var.aws_endpoint
    cloudwatchlogs = var.aws_endpoint
    ec2            = var.aws_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sqs            = var.aws_endpoint
    sfn            = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

locals {
  endpoint_override_enabled = var.aws_endpoint != null && var.aws_endpoint != ""
  supports_rds              = !local.endpoint_override_enabled
  supports_apigateway       = !local.endpoint_override_enabled
  supports_pipes            = !local.endpoint_override_enabled

  worker_source = <<-PY
    import json
    import os
    import socket

    import boto3

    def handler(event, context):
        secret_arn = os.environ["SECRET_ARN"]
        db_host = os.environ["DB_HOST"]
        client = boto3.client("secretsmanager")
        payload = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
        try:
            socket.create_connection((db_host, 5432), timeout=1).close()
        except Exception:
            pass
        print(json.dumps({
            "function": "worker",
            "request_id": context.aws_request_id,
            "username": payload["username"],
        }, separators=(",", ":")))
        return {"status": "processed"}
  PY

  health_source = <<-PY
    import json

    def handler(event, context):
        print(json.dumps({
            "function": "health",
            "request_id": context.aws_request_id,
        }, separators=(",", ":")))
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"status": "ok"}),
        }
  PY

  enrichment_source = <<-PY
    import json

    def handler(event, context):
        print(json.dumps({
            "function": "enrichment",
            "request_id": context.aws_request_id,
        }, separators=(",", ":")))
        return event
  PY

  worker_db_host        = local.supports_rds ? one(aws_db_instance.storage_layer[*].address) : "storage-layer-disabled"
  http_api_endpoint_url = local.supports_apigateway ? one(aws_apigatewayv2_stage.default[*].invoke_url) : "http-api-disabled"
  rds_endpoint_address  = local.supports_rds ? one(aws_db_instance.storage_layer[*].address) : "storage-layer-disabled"
}

resource "random_password" "database" {
  length  = 24
  special = true
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_vpc" "cloud_boundaries" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.cloud_boundaries.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = false
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.cloud_boundaries.id
  cidr_block              = "10.20.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = false
}

resource "aws_security_group" "processing_units" {
  name        = "processing-units"
  description = "Processing Units security boundary"
  vpc_id      = aws_vpc.cloud_boundaries.id
  ingress     = []
  egress      = []
}

resource "aws_security_group" "storage_layer" {
  name        = "storage-layer"
  description = "Storage Layer security boundary"
  vpc_id      = aws_vpc.cloud_boundaries.id
  ingress     = []
  egress      = []
}

resource "aws_vpc_security_group_egress_rule" "processing_https" {
  security_group_id = aws_security_group.processing_units.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "processing_postgres" {
  security_group_id            = aws_security_group.processing_units.id
  referenced_security_group_id = aws_security_group.storage_layer.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_security_group_ingress_rule" "storage_postgres" {
  security_group_id            = aws_security_group.storage_layer.id
  referenced_security_group_id = aws_security_group.processing_units.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_security_group_egress_rule" "storage_https" {
  security_group_id = aws_security_group.storage_layer.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_secretsmanager_secret" "database_credentials" {
  name_prefix             = "database-credentials-"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database_credentials" {
  secret_id = aws_secretsmanager_secret.database_credentials.id
  secret_string = jsonencode({
    username = "payments_app"
    password = random_password.database.result
  })
}

resource "aws_db_subnet_group" "storage_layer" {
  count      = local.supports_rds ? 1 : 0
  name       = "storage-layer-subnets-${random_id.suffix.hex}"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_db_instance" "storage_layer" {
  count                           = local.supports_rds ? 1 : 0
  allocated_storage               = 20
  db_name                         = "payments"
  db_subnet_group_name            = one(aws_db_subnet_group.storage_layer[*].name)
  delete_automated_backups        = true
  deletion_protection             = false
  enabled_cloudwatch_logs_exports = ["postgresql"]
  engine                          = "postgres"
  engine_version                  = "15.4"
  identifier                      = "storage-layer-postgres-${random_id.suffix.hex}"
  instance_class                  = "db.t3.micro"
  password                        = jsondecode(aws_secretsmanager_secret_version.database_credentials.secret_string)["password"]
  publicly_accessible             = false
  skip_final_snapshot             = true
  storage_type                    = "gp2"
  username                        = jsondecode(aws_secretsmanager_secret_version.database_credentials.secret_string)["username"]
  vpc_security_group_ids          = [aws_security_group.storage_layer.id]
}

resource "aws_cloudwatch_log_group" "application" {
  name              = "/application/payments-${random_id.suffix.hex}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/apigateway/payments-access-${random_id.suffix.hex}"
  retention_in_days = 14
}

data "archive_file" "worker" {
  type        = "zip"
  output_path = "${path.module}/worker.zip"

  source {
    content  = local.worker_source
    filename = "app.py"
  }
}

data "archive_file" "health" {
  type        = "zip"
  output_path = "${path.module}/health.zip"

  source {
    content  = local.health_source
    filename = "app.py"
  }
}

data "archive_file" "enrichment" {
  type        = "zip"
  output_path = "${path.module}/enrichment.zip"

  source {
    content  = local.enrichment_source
    filename = "app.py"
  }
}

resource "aws_iam_role" "worker" {
  name_prefix = "payments-worker-role-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role" "health" {
  name_prefix = "payments-health-role-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "worker_basic" {
  role       = aws_iam_role.worker.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "health_basic" {
  role       = aws_iam_role.health.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "worker" {
  name = "payments-worker-inline"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "QueueRead"
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage"
        ]
        Resource = aws_sqs_queue.intake.arn
      },
      {
        Sid      = "ReadDatabaseSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.database_credentials.arn
      },
      {
        Sid    = "ManageVpcEnis"
        Effect = "Allow"
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:UnassignPrivateIpAddresses"
        ]
        # EC2 network-interface APIs do not support resource-level scoping.
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "health" {
  name = "payments-health-inline"
  role = aws_iam_role.health.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteApplicationLogs"
        Effect   = "Allow"
        Action   = ["logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.application.arn}:*"
      }
    ]
  })
}

resource "aws_lambda_function" "worker" {
  function_name    = "payments-worker-${random_id.suffix.hex}"
  filename         = data.archive_file.worker.output_path
  handler          = "app.handler"
  memory_size      = 256
  role             = aws_iam_role.worker.arn
  runtime          = "python3.12"
  source_code_hash = data.archive_file.worker.output_base64sha256
  timeout          = 15

  environment {
    variables = {
      DB_HOST    = local.worker_db_host
      SECRET_ARN = aws_secretsmanager_secret.database_credentials.arn
    }
  }

  vpc_config {
    security_group_ids = [aws_security_group.processing_units.id]
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  }
}

resource "aws_lambda_function" "health" {
  function_name    = "payments-health-${random_id.suffix.hex}"
  filename         = data.archive_file.health.output_path
  handler          = "app.handler"
  memory_size      = 256
  role             = aws_iam_role.health.arn
  runtime          = "python3.12"
  source_code_hash = data.archive_file.health.output_base64sha256
  timeout          = 15
}

resource "aws_lambda_function" "enrichment" {
  function_name    = "payments-enrichment-${random_id.suffix.hex}"
  filename         = data.archive_file.enrichment.output_path
  handler          = "app.handler"
  memory_size      = 256
  role             = aws_iam_role.health.arn
  runtime          = "python3.12"
  source_code_hash = data.archive_file.enrichment.output_base64sha256
  timeout          = 15
}

resource "aws_sqs_queue" "intake" {
  message_retention_seconds  = 1209600
  visibility_timeout_seconds = 60
}

resource "aws_iam_role" "api_gateway_sqs" {
  name_prefix = "payments-api-gateway-sqs-role-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "api_gateway_sqs" {
  name = "payments-api-gateway-sqs-inline"
  role = aws_iam_role.api_gateway_sqs.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.intake.arn
      }
    ]
  })
}

resource "aws_sqs_queue_policy" "intake" {
  queue_url = aws_sqs_queue.intake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowApiGatewayRoleOnly"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.api_gateway_sqs.arn
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.intake.arn
      }
    ]
  })
}

resource "aws_apigatewayv2_api" "front_door" {
  count         = local.supports_apigateway ? 1 : 0
  name          = "payments-front-door-${random_id.suffix.hex}"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "submit" {
  count                  = local.supports_apigateway ? 1 : 0
  api_id                 = one(aws_apigatewayv2_api.front_door[*].id)
  credentials_arn        = aws_iam_role.api_gateway_sqs.arn
  integration_subtype    = "SQS-SendMessage"
  integration_type       = "AWS_PROXY"
  payload_format_version = "1.0"

  request_parameters = {
    MessageBody = "$request.body"
    QueueUrl    = aws_sqs_queue.intake.url
  }
}

resource "aws_apigatewayv2_integration" "health" {
  count                  = local.supports_apigateway ? 1 : 0
  api_id                 = one(aws_apigatewayv2_api.front_door[*].id)
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.health.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "submit" {
  count     = local.supports_apigateway ? 1 : 0
  api_id    = one(aws_apigatewayv2_api.front_door[*].id)
  route_key = "POST /submit"
  target    = "integrations/${one(aws_apigatewayv2_integration.submit[*].id)}"
}

resource "aws_apigatewayv2_route" "health" {
  count     = local.supports_apigateway ? 1 : 0
  api_id    = one(aws_apigatewayv2_api.front_door[*].id)
  route_key = "GET /health"
  target    = "integrations/${one(aws_apigatewayv2_integration.health[*].id)}"
}

resource "aws_apigatewayv2_stage" "default" {
  count       = local.supports_apigateway ? 1 : 0
  api_id      = one(aws_apigatewayv2_api.front_door[*].id)
  auto_deploy = true
  name        = "$default"

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId = "$context.requestId"
      routeKey  = "$context.routeKey"
      status    = "$context.status"
    })
  }
}

resource "aws_lambda_permission" "api_health" {
  count         = local.supports_apigateway ? 1 : 0
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.health.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${one(aws_apigatewayv2_api.front_door[*].execution_arn)}/*/GET/health"
}

resource "aws_lambda_event_source_mapping" "worker" {
  batch_size                         = 5
  event_source_arn                   = aws_sqs_queue.intake.arn
  function_name                      = aws_lambda_function.worker.arn
  maximum_batching_window_in_seconds = 5
}

resource "aws_iam_role" "state_machine" {
  name_prefix = "payments-state-machine-role-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_sfn_state_machine" "processing" {
  definition = jsonencode({
    StartAt = "Prepare"
    States = {
      Prepare = {
        Type = "Pass"
        Next = "Complete"
      }
      Complete = {
        Type = "Succeed"
      }
    }
  })
  name     = "payments-processing-${random_id.suffix.hex}"
  role_arn = aws_iam_role.state_machine.arn
  type     = "STANDARD"
}

resource "aws_iam_role" "pipe" {
  name_prefix = "payments-pipe-role-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "pipes.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipe" {
  name = "payments-pipe-inline"
  role = aws_iam_role.pipe.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage"
        ]
        Resource = aws_sqs_queue.intake.arn
      },
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          aws_lambda_function.enrichment.arn,
          "${aws_lambda_function.enrichment.arn}:*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = aws_sfn_state_machine.processing.arn
      }
    ]
  })
}

resource "aws_pipes_pipe" "processing" {
  count         = local.supports_pipes ? 1 : 0
  desired_state = "RUNNING"
  enrichment    = aws_lambda_function.enrichment.arn
  name          = "payments-processing-pipe-${random_id.suffix.hex}"
  role_arn      = aws_iam_role.pipe.arn
  source        = aws_sqs_queue.intake.arn
  target        = aws_sfn_state_machine.processing.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size = 1
    }
  }
}

output "http_api_endpoint_url" {
  value = local.http_api_endpoint_url
}

output "sqs_queue_url" {
  value = aws_sqs_queue.intake.url
}

output "rds_endpoint_address" {
  value = local.rds_endpoint_address
}

output "secrets_manager_secret_arn" {
  value = aws_secretsmanager_secret.database_credentials.arn
}
