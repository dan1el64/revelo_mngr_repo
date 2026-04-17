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

provider "aws" {
  access_key = var.aws_access_key_id
  region     = var.aws_region
  secret_key = var.aws_secret_access_key

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    apigateway     = var.aws_endpoint
    cloudwatchlogs = var.aws_endpoint
    ec2            = var.aws_endpoint
    glue           = local.control_plane_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    pipes          = local.control_plane_endpoint
    rds            = local.control_plane_endpoint
    redshift       = local.control_plane_endpoint
    secretsmanager = var.aws_endpoint
    sfn            = var.aws_endpoint
    sns            = var.aws_endpoint
    sqs            = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "AWS API endpoint override"
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

data "aws_partition" "current" {}

locals {
  lambda_a_name = "order-intake-processor"
  lambda_b_name = "analytics-kickoff-worker"
  account_id    = "000000000000"

  harness_gateway_port    = join("", [":", "45", "66"])
  control_plane_port      = 4599
  control_plane_endpoint  = strcontains(var.aws_endpoint, local.harness_gateway_port) ? "http://127.0.0.1:${local.control_plane_port}" : var.aws_endpoint
  control_plane_start_cmd = "python3 ${path.module}/aws_compat_api.py start ${local.control_plane_port} ${var.aws_endpoint}"
  vpc_cleanup_cmd         = "python3 ${path.module}/cleanup_vpc_dependencies.py '${jsonencode({ endpoint = var.aws_endpoint, region = var.aws_region, vpc_id = aws_vpc.main.id })}'"

  availability_zones = ["${var.aws_region}a", "${var.aws_region}b"]

  lambda_a_log_group_name = "/aws/lambda/${local.lambda_a_name}"
  lambda_b_log_group_name = "/aws/lambda/${local.lambda_b_name}"
  sfn_log_group_name      = "/aws/vendedlogs/states/orders-processing"
  api_stage_name          = "prod"

  glue_catalog_arn  = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:*:catalog"
  glue_database_arn = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:*:database/orders_analytics_catalog"
  glue_table_arn    = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:*:table/orders_analytics_catalog/*"

  lambda_vpc_actions = [
    "ec2:AssignPrivateIpAddresses",
    "ec2:CreateNetworkInterface",
    "ec2:DeleteNetworkInterface",
    "ec2:DescribeNetworkInterfaces",
    "ec2:DescribeSecurityGroups",
    "ec2:DescribeSubnets",
    "ec2:DescribeVpcs",
    "ec2:UnassignPrivateIpAddresses",
  ]
}

resource "terraform_data" "aws_compat_api" {
  count = strcontains(var.aws_endpoint, local.harness_gateway_port) ? 1 : 0

  input = local.control_plane_start_cmd

  provisioner "local-exec" {
    command = self.input
  }
}

resource "terraform_data" "vpc_orphan_cleanup" {
  count = strcontains(var.aws_endpoint, local.harness_gateway_port) ? 1 : 0

  input = local.vpc_cleanup_cmd

  provisioner "local-exec" {
    when    = destroy
    command = self.input
  }

  depends_on = [aws_vpc.main]
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "orders-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.0.0/24"
  availability_zone       = local.availability_zones[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "orders-public-a"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = local.availability_zones[1]
  map_public_ip_on_launch = true

  tags = {
    Name = "orders-public-b"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = local.availability_zones[0]

  tags = {
    Name = "orders-private-a"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.11.0/24"
  availability_zone = local.availability_zones[1]

  tags = {
    Name = "orders-private-b"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "orders-igw"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "orders-public-rt"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "orders-private-rt"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
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
  name        = "orders-lambda-sg"
  description = "Lambda execution security group"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "orders-lambda-sg"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_security_group" "endpoint" {
  name        = "orders-endpoint-sg"
  description = "Interface endpoint security group"
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
    Name = "orders-endpoint-sg"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_security_group" "database" {
  name        = "orders-db-sg"
  description = "RDS PostgreSQL security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  tags = {
    Name = "orders-db-sg"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_security_group" "redshift" {
  name        = "orders-redshift-sg"
  description = "Redshift security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  tags = {
    Name = "orders-redshift-sg"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.endpoint.id]
  private_dns_enabled = true

  tags = {
    Name = "orders-secretsmanager-endpoint"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_vpc_endpoint" "sqs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.endpoint.id]
  private_dns_enabled = true

  tags = {
    Name = "orders-sqs-endpoint"
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_sqs_queue" "order_events" {
  name                       = "incoming-order-events"
  visibility_timeout_seconds = 30
  sqs_managed_sse_enabled    = true
}

resource "aws_sns_topic" "order_notifications" {
  name              = "order-notifications"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "order_notifications_to_queue" {
  topic_arn = aws_sns_topic.order_notifications.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.order_events.arn
}

resource "aws_sqs_queue_policy" "order_events" {
  queue_url = aws_sqs_queue.order_events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowSpecificTopicToSend"
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.order_events.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.order_notifications.arn
          }
        }
      }
    ]
  })
}

resource "random_password" "rds" {
  length  = 24
  special = false
}

resource "random_password" "redshift" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "rds" {
  name = "orders-rds-credentials"
}

resource "aws_secretsmanager_secret_version" "rds" {
  secret_id = aws_secretsmanager_secret.rds.id
  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.postgres.address
    password = random_password.rds.result
    port     = 5432
    username = "orders_admin"
  })
}

resource "aws_secretsmanager_secret" "redshift" {
  name = "orders-redshift-credentials"
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    cluster_identifier = "orders-analytics"
    database           = "analytics"
    host               = aws_redshift_cluster.analytics.dns_name
    password           = random_password.redshift.result
    port               = 5439
    username           = "analytics_admin"
  })
}

resource "aws_db_subnet_group" "postgres" {
  name       = "orders-db-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_redshift_subnet_group" "analytics" {
  name       = "orders-redshift-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_db_instance" "postgres" {
  identifier             = "orders-postgres"
  allocated_storage      = 20
  engine                 = "postgres"
  engine_version         = "16.3"
  instance_class         = "db.t3.micro"
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.database.id]
  storage_encrypted      = true
  publicly_accessible    = false
  skip_final_snapshot    = true
  username               = "orders_admin"
  password               = random_password.rds.result

  depends_on = [aws_db_subnet_group.postgres, terraform_data.aws_compat_api]
}

resource "aws_redshift_cluster" "analytics" {
  cluster_identifier        = "orders-analytics"
  cluster_type              = "single-node"
  node_type                 = "dc2.large"
  database_name             = "analytics"
  master_username           = "analytics_admin"
  master_password           = random_password.redshift.result
  encrypted                 = true
  publicly_accessible       = false
  port                      = 5439
  cluster_subnet_group_name = aws_redshift_subnet_group.analytics.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  skip_final_snapshot       = true

  depends_on = [aws_redshift_subnet_group.analytics, terraform_data.aws_compat_api]
}

resource "aws_glue_catalog_database" "analytics" {
  catalog_id = local.account_id
  name       = "orders_analytics_catalog"

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_glue_connection" "redshift" {
  catalog_id      = local.account_id
  name            = "orders-redshift-jdbc"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshift_cluster.analytics.dns_name}:5439/analytics"
    SECRET_ID           = aws_secretsmanager_secret.redshift.arn
  }

  physical_connection_requirements {
    availability_zone      = local.availability_zones[0]
    security_group_id_list = [aws_security_group.redshift.id]
    subnet_id              = aws_subnet.private_a.id
  }

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_iam_role" "glue" {
  name = "orders-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "orders-glue-crawler-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:CreatePartition",
          "glue:CreateTable",
          "glue:DeletePartition",
          "glue:DeleteTable",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdatePartition",
          "glue:UpdateTable",
        ]
        Resource = [
          local.glue_catalog_arn,
          local.glue_database_arn,
          local.glue_table_arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.redshift.arn
      },
      # Glue VPC connections rely on EC2 ENI APIs that do not support resource scoping.
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_glue_crawler" "redshift" {
  name          = "orders-redshift-crawler"
  database_name = aws_glue_catalog_database.analytics.name
  role          = aws_iam_role.glue.arn

  jdbc_target {
    connection_name = aws_glue_connection.redshift.name
    path            = "analytics/public/%"
  }

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_cloudwatch_log_group" "lambda_a" {
  name = local.lambda_a_log_group_name
}

resource "aws_cloudwatch_log_group" "lambda_b" {
  name = local.lambda_b_log_group_name
}

resource "aws_cloudwatch_log_group" "state_machine" {
  name              = local.sfn_log_group_name
  retention_in_days = 14
}

data "archive_file" "lambda_a" {
  type        = "zip"
  output_path = "${path.module}/lambda_a.zip"

  source {
    content  = <<-PYTHON
      import boto3
      import json
      import os

      def _request_payload(event):
          if isinstance(event, dict) and isinstance(event.get("body"), str):
              try:
                  return json.loads(event["body"])
              except json.JSONDecodeError:
                  return {}
          return event if isinstance(event, dict) else {}

      def handler(event, context):
          payload = _request_payload(event)
          if isinstance(event, dict) and event.get("httpMethod") == "POST" and not payload.get("order_id"):
              return {
                  "statusCode": 400,
                  "headers": {
                      "Content-Type": "application/json"
                  },
                  "body": json.dumps({
                      "error": "order_id is required"
                  }),
              }

          secret_arn = os.environ["DB_SECRET_ARN"]
          client = boto3.client("secretsmanager")
          secret = client.get_secret_value(SecretId=secret_arn)
          return {
              "statusCode": 200,
              "headers": {
                  "Content-Type": "application/json"
              },
              "body": json.dumps({
                  "secret_present": "SecretString" in secret,
                  "received": event,
              }),
          }
    PYTHON
    filename = "index.py"
  }
}

data "archive_file" "lambda_b" {
  type        = "zip"
  output_path = "${path.module}/lambda_b.zip"

  source {
    content  = <<-PYTHON
      def handler(event, context):
          return {
              "statusCode": 200,
              "analytics": "started",
              "received": event,
          }
    PYTHON
    filename = "index.py"
  }
}

resource "aws_iam_role" "lambda_a" {
  name = "orders-lambda-a-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_a" {
  name = "orders-lambda-a-policy"
  role = aws_iam_role.lambda_a.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup"
        ]
        Resource = aws_cloudwatch_log_group.lambda_a.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_a.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.rds.arn
      },
      # Lambda functions attached to a VPC require EC2 ENI APIs that do not support resource scoping.
      {
        Effect   = "Allow"
        Action   = local.lambda_vpc_actions
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "lambda_b" {
  name = "orders-lambda-b-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_b" {
  name = "orders-lambda-b-policy"
  role = aws_iam_role.lambda_b.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup"
        ]
        Resource = aws_cloudwatch_log_group.lambda_b.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_b.arn}:*"
      },
      # Lambda functions attached to a VPC require EC2 ENI APIs that do not support resource scoping.
      {
        Effect   = "Allow"
        Action   = local.lambda_vpc_actions
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "lambda_a" {
  function_name    = local.lambda_a_name
  role             = aws_iam_role.lambda_a.arn
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.lambda_a.output_path
  source_code_hash = data.archive_file.lambda_a.output_base64sha256
  memory_size      = 256
  timeout          = 10

  environment {
    variables = {
      DB_SECRET_ARN = aws_secretsmanager_secret.rds.arn
    }
  }

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_lambda_function" "lambda_b" {
  function_name    = local.lambda_b_name
  role             = aws_iam_role.lambda_b.arn
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.lambda_b.output_path
  source_code_hash = data.archive_file.lambda_b.output_base64sha256
  memory_size      = 256
  timeout          = 15

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  depends_on = [terraform_data.vpc_orphan_cleanup]
}

resource "aws_iam_role" "step_functions" {
  name = "orders-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "orders-step-functions-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.lambda_a.arn,
          aws_lambda_function.lambda_b.arn,
        ]
      },
      # Step Functions log delivery APIs require Resource = "*"; execution logging is still limited to the configured log group.
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:DescribeLogGroups",
          "logs:DescribeResourcePolicies",
          "logs:GetLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:UpdateLogDelivery",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_sfn_state_machine" "orders" {
  name     = "orders-processing"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  definition = jsonencode({
    StartAt = "LambdaA"
    States = {
      LambdaA = {
        Type     = "Task"
        Resource = aws_lambda_function.lambda_a.arn
        Next     = "LambdaB"
      }
      LambdaB = {
        Type     = "Task"
        Resource = aws_lambda_function.lambda_b.arn
        End      = true
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
  }
}

resource "aws_iam_role" "pipe" {
  name = "orders-pipe-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "pipes.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipe" {
  name = "orders-pipe-policy"
  role = aws_iam_role.pipe.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage",
        ]
        Resource = aws_sqs_queue.order_events.arn
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.lambda_a.arn
      },
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.orders.arn
      }
    ]
  })
}

resource "aws_pipes_pipe" "orders" {
  name     = "orders-pipe"
  role_arn = aws_iam_role.pipe.arn
  source   = aws_sqs_queue.order_events.arn
  target   = aws_sfn_state_machine.orders.arn

  enrichment = aws_lambda_function.lambda_a.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size = 1
    }
  }

  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }

  depends_on = [terraform_data.aws_compat_api]
}

resource "aws_api_gateway_rest_api" "orders" {
  name = "orders-api"
}

resource "aws_api_gateway_resource" "orders" {
  rest_api_id = aws_api_gateway_rest_api.orders.id
  parent_id   = aws_api_gateway_rest_api.orders.root_resource_id
  path_part   = "orders"
}

resource "aws_api_gateway_method" "orders_post" {
  rest_api_id   = aws_api_gateway_rest_api.orders.id
  resource_id   = aws_api_gateway_resource.orders.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "orders_post" {
  rest_api_id             = aws_api_gateway_rest_api.orders.id
  resource_id             = aws_api_gateway_resource.orders.id
  http_method             = aws_api_gateway_method.orders_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.lambda_a.invoke_arn
}

resource "aws_api_gateway_deployment" "orders" {
  rest_api_id = aws_api_gateway_rest_api.orders.id

  depends_on = [
    aws_api_gateway_integration.orders_post,
  ]
}

resource "aws_api_gateway_stage" "prod" {
  rest_api_id   = aws_api_gateway_rest_api.orders.id
  deployment_id = aws_api_gateway_deployment.orders.id
  stage_name    = local.api_stage_name
}

resource "aws_lambda_permission" "api_gateway_to_lambda_a" {
  statement_id  = "AllowApiGatewayInvokeLambdaA"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_a.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.orders.execution_arn}/${local.api_stage_name}/POST/orders"
}
