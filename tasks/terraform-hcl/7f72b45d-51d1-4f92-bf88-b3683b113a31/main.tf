variable "aws_region" {
  description = "AWS region to deploy to"
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "AWS service endpoint base URL"
  type        = string
  default     = null
  nullable    = true
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  default     = null
  nullable    = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  default     = null
  nullable    = true
}

locals {
  is_local_mode = var.aws_endpoint != null && var.aws_endpoint != ""
  account_id    = local.is_local_mode ? "000000000000" : data.aws_caller_identity.current[0].account_id
}

resource "random_id" "bucket_prefix" {
  byte_length = 4
}

data "aws_caller_identity" "current" {
  count = local.is_local_mode ? 0 : 1
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/lambda"
  output_path = "${path.module}/lambda.zip"
}

# AWS Provider Configuration
provider "aws" {
  region                      = var.aws_region
  skip_credentials_validation = local.is_local_mode
  skip_metadata_api_check     = local.is_local_mode
  skip_requesting_account_id  = local.is_local_mode
  s3_use_path_style           = local.is_local_mode

  endpoints {
    acm            = var.aws_endpoint
    apigateway     = var.aws_endpoint
    cloudwatch     = var.aws_endpoint
    cloudwatchlogs = var.aws_endpoint
    dynamodb       = var.aws_endpoint
    ec2            = var.aws_endpoint
    events         = var.aws_endpoint
    iam            = var.aws_endpoint
    kms            = var.aws_endpoint
    lambda         = var.aws_endpoint
    rds            = var.aws_endpoint
    s3             = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sns            = var.aws_endpoint
    sqs            = var.aws_endpoint
    ssm            = var.aws_endpoint
    sfn            = var.aws_endpoint
    sts            = var.aws_endpoint
    xray           = var.aws_endpoint
  }
}

# KMS Key for Data-at-Rest Encryption
resource "aws_kms_key" "data_at_rest" {
  description         = "KMS key for data-at-rest encryption"
  enable_key_rotation = true
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Allow IAM admin access"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow service access"
        Effect = "Allow"
        Principal = {
          Service = "logs.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow RDS access"
        Effect = "Allow"
        Principal = {
          Service = "rds.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow S3 access"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow SQS access"
        Effect = "Allow"
        Principal = {
          Service = "sqs.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow DynamoDB access"
        Effect = "Allow"
        Principal = {
          Service = "dynamodb.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow Lambda access"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "data_at_rest_alias" {
  name          = "alias/data-at-rest"
  target_key_id = aws_kms_key.data_at_rest.key_id
}

# Secrets Manager for Database Credentials
resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "db-credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_credentials_version" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "admin"
    password = "securepassword123"
  })
}

# IAM Roles
resource "aws_iam_role" "backend_execution_role" {
  name = "backend-execution-role"
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

resource "aws_iam_role" "state_machine_role" {
  name = "state-machine-role"
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

# IAM Policies
resource "aws_iam_policy" "backend_policy" {
  name = "backend-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws/lambda/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.raw_events.id}",
          "arn:aws:s3:::${aws_s3_bucket.raw_events.id}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.event_queue.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.data_at_rest.arn
      },
      {
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "state_machine_policy" {
  name = "state-machine-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.backend.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.event_queue.arn
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
          "logs:DescribeLogGroups"
        ]
        Resource = aws_cloudwatch_log_group.state_machine.arn
      }
    ]
  })
}

# Attach Policies to Roles
resource "aws_iam_role_policy_attachment" "backend_policy_attachment" {
  role       = aws_iam_role.backend_execution_role.name
  policy_arn = aws_iam_policy.backend_policy.arn
}

resource "aws_iam_role_policy_attachment" "state_machine_policy_attachment" {
  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.state_machine_policy.arn
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.101.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.102.0/24"
  availability_zone       = data.aws_availability_zones.available.names[1]
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.gw.id
}

resource "aws_route_table_association" "public_1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_1.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route" "private_nat" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.nat.id
}

resource "aws_route_table_association" "private_1" {
  subnet_id      = aws_subnet.private_1.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_2" {
  subnet_id      = aws_subnet.private_2.id
  route_table_id = aws_route_table.private.id
}

# Security Groups
resource "aws_security_group" "execution_env" {
  name        = "execution-env-sg"
  description = "Security group for Execution Environment"
  vpc_id      = aws_vpc.main.id
}

resource "aws_security_group" "relational_backbone" {
  name        = "relational-backbone-sg"
  description = "Security group for Relational Backbone"
  vpc_id      = aws_vpc.main.id
}

resource "aws_security_group_rule" "db_ingress" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.relational_backbone.id
  source_security_group_id = aws_security_group.execution_env.id
}

resource "aws_security_group_rule" "db_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.relational_backbone.id
  cidr_blocks       = [aws_vpc.main.cidr_block]
}

resource "aws_security_group_rule" "execution_env_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.execution_env.id
  prefix_list_ids   = [aws_vpc_endpoint.s3_gateway.prefix_list_id]
}

# S3 Gateway VPC Endpoint
resource "aws_vpc_endpoint" "s3_gateway" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = [
    aws_route_table.public.id,
    aws_route_table.private.id
  ]
}

resource "aws_vpc_endpoint_policy" "s3_gateway_policy" {
  vpc_endpoint_id = aws_vpc_endpoint.s3_gateway.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.raw_events.id}",
          "arn:aws:s3:::${aws_s3_bucket.raw_events.id}/*"
        ]
      }
    ]
  })
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "relational_backbone" {
  identifier                 = "relational-backbone"
  engine                     = "postgres"
  engine_version             = "15"
  instance_class             = "db.t3.micro"
  allocated_storage          = 20
  storage_type               = "gp2"
  storage_encrypted          = true
  kms_key_id                 = aws_kms_key.data_at_rest.arn
  multi_az                   = false
  publicly_accessible        = false
  deletion_protection        = false
  backup_retention_period    = 7
  backup_window              = "07:00-09:00"
  auto_minor_version_upgrade = true
  username                   = jsondecode(aws_secretsmanager_secret_version.db_credentials_version.secret_string).username
  password                   = jsondecode(aws_secretsmanager_secret_version.db_credentials_version.secret_string).password
  skip_final_snapshot        = true
  db_subnet_group_name       = aws_db_subnet_group.main.id
  vpc_security_group_ids     = [aws_security_group.relational_backbone.id]
}

resource "aws_db_subnet_group" "main" {
  name       = "main"
  subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
}

# S3 Bucket for Raw Events
resource "aws_s3_bucket" "raw_events" {
  bucket = "raw-events-${random_id.bucket_prefix.hex}"
}

resource "aws_s3_bucket_versioning" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_at_rest.arn
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id
  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  rule {
    id     = "expire-current-objects"
    status = "Enabled"
    expiration {
      days = 365
    }
  }
}

# SQS Queue
resource "aws_sqs_queue" "event_queue" {
  name                       = "event-queue"
  message_retention_seconds  = 1209600
  visibility_timeout_seconds = 60
  kms_master_key_id          = aws_kms_key.data_at_rest.arn
}

# DynamoDB Table
resource "aws_dynamodb_table" "idempotency" {
  name         = "idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  attribute {
    name = "pk"
    type = "S"
  }
  point_in_time_recovery {
    enabled = true
  }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.data_at_rest.arn
  }
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "backend" {
  name              = "/aws/lambda/backend"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.data_at_rest.arn
}

resource "aws_cloudwatch_log_group" "state_machine" {
  name              = "/aws/vendedlogs/states/StateMachineLogGroup"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.data_at_rest.arn
}

# CloudWatch Alarm for Lambda Errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "lambda-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Lambda function has errors"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.backend.function_name
  }
  alarm_actions = [aws_sns_topic.operational_notifications.arn]
}

# SNS Topic for Operational Notifications
resource "aws_sns_topic" "operational_notifications" {
  name = "operational-notifications"
}

resource "aws_sns_topic_policy" "operational_notifications_policy" {
  arn = aws_sns_topic.operational_notifications.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCloudWatchPublish"
        Effect = "Allow"
        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.operational_notifications.arn
      }
    ]
  })
}

# X-Ray Group for Lambda Tracing
resource "aws_xray_group" "lambda_tracing" {
  count = local.is_local_mode ? 0 : 1

  group_name        = "lambda-tracing"
  filter_expression = "service(Lambda)"
}

resource "aws_lambda_function" "backend" {
  function_name                  = "backend"
  runtime                        = "nodejs20.x"
  handler                        = "index.handler"
  filename                       = data.archive_file.lambda_package.output_path
  source_code_hash               = data.archive_file.lambda_package.output_base64sha256
  role                           = aws_iam_role.backend_execution_role.arn
  memory_size                    = 256
  timeout                        = 10
  reserved_concurrent_executions = 5
  vpc_config {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [aws_security_group.execution_env.id]
  }
  tracing_config {
    mode = "Active"
  }
  environment {
    variables = {
      QUEUE_URL   = aws_sqs_queue.event_queue.url
      BUCKET_NAME = aws_s3_bucket.raw_events.id
      TABLE_NAME  = aws_dynamodb_table.idempotency.name
    }
  }
}

# API Gateway
resource "aws_api_gateway_rest_api" "ingest_api" {
  name        = "ingest-api"
  description = "API for ingesting events"
}

resource "aws_api_gateway_resource" "ingest_resource" {
  rest_api_id = aws_api_gateway_rest_api.ingest_api.id
  parent_id   = aws_api_gateway_rest_api.ingest_api.root_resource_id
  path_part   = "ingest"
}


resource "aws_api_gateway_method" "ingest_post" {
  rest_api_id   = aws_api_gateway_rest_api.ingest_api.id
  resource_id   = aws_api_gateway_resource.ingest_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "ingest_integration" {
  rest_api_id             = aws_api_gateway_rest_api.ingest_api.id
  resource_id             = aws_api_gateway_resource.ingest_resource.id
  http_method             = aws_api_gateway_method.ingest_post.http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = aws_lambda_function.backend.invoke_arn
}

resource "aws_api_gateway_deployment" "ingest_api_deployment" {
  depends_on = [
    aws_api_gateway_method.ingest_post,
    aws_api_gateway_integration.ingest_integration
  ]
  rest_api_id = aws_api_gateway_rest_api.ingest_api.id
}

resource "aws_api_gateway_stage" "prod" {
  rest_api_id   = aws_api_gateway_rest_api.ingest_api.id
  deployment_id = aws_api_gateway_deployment.ingest_api_deployment.id
  stage_name    = "prod"
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.backend.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "arn:aws:execute-api:${var.aws_region}:${local.account_id}:${aws_api_gateway_rest_api.ingest_api.id}/*/POST/ingest"
}

# EventBridge Rule to Trigger State Machine
resource "aws_cloudwatch_event_rule" "scheduled_execution" {
  name                = "scheduled-execution"
  description         = "Scheduled execution of state machine"
  schedule_expression = "rate(5 minutes)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "state_machine_target" {
  rule      = aws_cloudwatch_event_rule.scheduled_execution.name
  target_id = "state-machine-target"
  arn       = aws_sfn_state_machine.orchestration.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
  input = jsonencode({
    requestId = "scheduled"
  })
}

resource "aws_iam_role" "eventbridge_role" {
  name = "eventbridge-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_policy_attachment" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.state_machine_policy.arn
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "orchestration" {
  name = "orchestration"
  definition = jsonencode({
    StartAt = "EnqueueEvents"
    States = {
      EnqueueEvents = {
        Type     = "Task"
        Resource = aws_lambda_function.backend.arn
        Parameters = {
          action = "enqueue"
        }
        Next = "ArchiveEvents"
      }
      ArchiveEvents = {
        Type     = "Task"
        Resource = aws_lambda_function.backend.arn
        Parameters = {
          action = "archive"
        }
        End = true
      }
    }
  })
  role_arn = aws_iam_role.state_machine_role.arn
  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
  }
}

output "api_invoke_url" {
  value = aws_api_gateway_stage.prod.invoke_url
}

output "s3_bucket_name" {
  value = aws_s3_bucket.raw_events.id
}

output "sqs_queue_url" {
  value = aws_sqs_queue.event_queue.url
}

output "rds_endpoint" {
  value = aws_db_instance.relational_backbone.endpoint
}
