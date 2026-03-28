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
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_endpoint" {
  type    = string
  default = null
}

provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.aws_endpoint != null
  skip_metadata_api_check     = var.aws_endpoint != null
  skip_requesting_account_id  = var.aws_endpoint != null
  s3_use_path_style           = var.aws_endpoint != null

  default_tags {
    tags = {
      Project   = "OrderIntake"
      ManagedBy = "Terraform"
    }
  }

  endpoints {
    sts                    = var.aws_endpoint
    iam                    = var.aws_endpoint
    ec2                    = var.aws_endpoint
    logs                   = var.aws_endpoint
    s3                     = var.aws_endpoint
    dynamodb               = var.aws_endpoint
    lambda                 = var.aws_endpoint
    apigateway             = var.aws_endpoint
    events                 = var.aws_endpoint
    sns                    = var.aws_endpoint
    sqs                    = var.aws_endpoint
    secretsmanager         = var.aws_endpoint
    rds                    = var.aws_endpoint
    elasticloadbalancingv2 = var.aws_endpoint
  }
}

data "aws_availability_zones" "available" {
  count = var.aws_endpoint != null ? 0 : 1
  state = "available"
}

locals {
  endpoint_mode               = var.aws_endpoint != null
  endpoint_base               = var.aws_endpoint != null ? trimsuffix(var.aws_endpoint, "/") : null
  endpoint_host               = var.aws_endpoint != null ? split(":", split("//", trimsuffix(var.aws_endpoint, "/"))[1])[0] : null
  endpoint_port               = var.aws_endpoint != null ? tonumber(split(":", split("//", trimsuffix(var.aws_endpoint, "/"))[1])[1]) : null
  endpoint_azs                = ["${var.aws_region}a", "${var.aws_region}b"]
  endpoint_account_id         = "000000000000"
  endpoint_suffix             = local.endpoint_mode ? formatdate("YYYYMMDDhhmmss", plantimestamp()) : null
  endpoint_queue_url          = local.endpoint_mode ? "${local.endpoint_base}/${local.endpoint_account_id}/order-events-queue" : null
  endpoint_queue_arn          = local.endpoint_mode ? "arn:aws:sqs:${var.aws_region}:${local.endpoint_account_id}:order-events-queue" : null
  endpoint_topic_arn          = local.endpoint_mode ? "arn:aws:sns:${var.aws_region}:${local.endpoint_account_id}:order-events" : null
  bucket_name                 = local.endpoint_mode ? "order-intake-archive-${local.endpoint_suffix}" : "order-intake-archive"
  table_name                  = local.endpoint_mode ? "order-metadata-${local.endpoint_suffix}" : "order-metadata"
  api_key_secret_name         = local.endpoint_mode ? "orderintake/api_key-${local.endpoint_suffix}" : "orderintake/api_key"
  db_app_user_secret_name     = local.endpoint_mode ? "orderintake/db_app_user-${local.endpoint_suffix}" : "orderintake/db_app_user"
  ingest_role_name            = local.endpoint_mode ? "ingest-fn-role-${local.endpoint_suffix}" : "ingest-fn-role"
  analytics_role_name         = local.endpoint_mode ? "analytics-fn-role-${local.endpoint_suffix}" : "analytics-fn-role"
  ingest_function_name        = local.endpoint_mode ? "ingest_fn_${local.endpoint_suffix}" : "ingest_fn"
  analytics_function_name     = local.endpoint_mode ? "analytics_fn_${local.endpoint_suffix}" : "analytics_fn"
  ingest_log_group_name       = "/aws/lambda/${local.ingest_function_name}"
  analytics_log_group_name    = "/aws/lambda/${local.analytics_function_name}"
  endpoint_api_key_secret_arn = local.endpoint_mode ? "arn:aws:secretsmanager:${var.aws_region}:${local.endpoint_account_id}:secret:${local.api_key_secret_name}" : null
  endpoint_db_secret_arn      = local.endpoint_mode ? "arn:aws:secretsmanager:${var.aws_region}:${local.endpoint_account_id}:secret:${local.db_app_user_secret_name}" : null
  endpoint_ingest_role_arn    = local.endpoint_mode ? "arn:aws:iam::${local.endpoint_account_id}:role/${local.ingest_role_name}" : null
  endpoint_analytics_role_arn = local.endpoint_mode ? "arn:aws:iam::${local.endpoint_account_id}:role/${local.analytics_role_name}" : null
  azs                         = local.endpoint_mode ? local.endpoint_azs : data.aws_availability_zones.available[0].names
}

resource "aws_vpc" "order_intake" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "order-intake-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.order_intake.id
  cidr_block              = "10.20.0.0/24"
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "order-intake-public-a"
  }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.order_intake.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = local.azs[1]
  map_public_ip_on_launch = true

  tags = {
    Name = "order-intake-public-b"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.order_intake.id
  cidr_block              = "10.20.10.0/24"
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = false

  tags = {
    Name = "order-intake-private-a"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.order_intake.id
  cidr_block              = "10.20.11.0/24"
  availability_zone       = local.azs[1]
  map_public_ip_on_launch = false

  tags = {
    Name = "order-intake-private-b"
  }
}

resource "aws_internet_gateway" "order_intake" {
  vpc_id = aws_vpc.order_intake.id

  tags = {
    Name = "order-intake-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.order_intake.id

  tags = {
    Name = "order-intake-public-rt"
  }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.order_intake.id
}

resource "aws_route_table_association" "public_a" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_b" {
  subnet_id      = aws_subnet.public_b.id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "order-intake-nat-eip"
  }
}

resource "aws_nat_gateway" "order_intake" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_a.id

  depends_on = [aws_internet_gateway.order_intake]

  tags = {
    Name = "order-intake-nat"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.order_intake.id

  tags = {
    Name = "order-intake-private-rt"
  }
}

resource "aws_route" "private_default" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.order_intake.id
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.order_intake.id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "order-intake-dynamodb-endpoint"
  }
}

resource "aws_s3_bucket" "archive" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = {
    Name = local.bucket_name
  }
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket = aws_s3_bucket.archive.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_dynamodb_table" "order_metadata" {
  name         = local.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name = "order-metadata"
  }
}

resource "aws_sns_topic" "order_events" {
  name = "order-events"

  tags = {
    Name = "order-events"
  }
}

resource "aws_sqs_queue" "order_events" {
  name                       = "order-events-queue"
  message_retention_seconds  = 345600
  visibility_timeout_seconds = 60

  tags = {
    Name = "order-events-queue"
  }
}

resource "aws_sqs_queue_policy" "order_events" {
  queue_url = local.endpoint_mode ? local.endpoint_queue_url : aws_sqs_queue.order_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowOrderEventsTopicOnly"
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = local.endpoint_mode ? local.endpoint_queue_arn : aws_sqs_queue.order_events.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = local.endpoint_mode ? local.endpoint_topic_arn : aws_sns_topic.order_events.arn
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "order_events_queue" {
  topic_arn = aws_sns_topic.order_events.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.order_events.arn

  depends_on = [aws_sqs_queue_policy.order_events]
}

resource "aws_secretsmanager_secret" "api_key" {
  name = local.api_key_secret_name

  tags = {
    Name = local.api_key_secret_name
  }
}

resource "aws_secretsmanager_secret_version" "api_key" {
  secret_id     = aws_secretsmanager_secret.api_key.id
  secret_string = "CHANGE_ME"
}

resource "aws_secretsmanager_secret" "db_app_user" {
  name = local.db_app_user_secret_name

  tags = {
    Name = local.db_app_user_secret_name
  }
}

resource "aws_secretsmanager_secret_version" "db_app_user" {
  secret_id = aws_secretsmanager_secret.db_app_user.id
  secret_string = jsonencode({
    username = "appuser"
    password = "CHANGE_ME"
  })
}

data "aws_secretsmanager_secret_version" "db_app_user" {
  secret_id  = aws_secretsmanager_secret.db_app_user.id
  depends_on = [aws_secretsmanager_secret_version.db_app_user]
}

locals {
  api_key_secret_arn     = local.endpoint_mode ? local.endpoint_api_key_secret_arn : aws_secretsmanager_secret.api_key.arn
  db_app_user_secret_arn = local.endpoint_mode ? local.endpoint_db_secret_arn : aws_secretsmanager_secret.db_app_user.arn
  db_app_user_secret     = jsondecode(data.aws_secretsmanager_secret_version.db_app_user.secret_string)
  # jsondecode(data.aws_secretsmanager_secret_version.db_app_user[0].secret_string)
}

resource "aws_iam_role" "ingest_fn" {
  name = local.ingest_role_name

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

  tags = {
    Name = local.ingest_role_name
  }
}

resource "aws_iam_role" "analytics_fn" {
  name = local.analytics_role_name

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

  tags = {
    Name = local.analytics_role_name
  }
}

resource "aws_security_group" "lambda" {
  name        = "order-intake-lambda-sg"
  description = "Security group for Lambda functions"
  vpc_id      = aws_vpc.order_intake.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "order-intake-lambda-sg"
  }
}

resource "aws_security_group" "database" {
  name        = "order-intake-db-sg"
  description = "Security group for PostgreSQL"
  vpc_id      = aws_vpc.order_intake.id

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
    Name = "order-intake-db-sg"
  }
}

resource "aws_cloudwatch_log_group" "ingest_fn" {
  name              = local.ingest_log_group_name
  retention_in_days = 14

  tags = {
    Name = local.ingest_log_group_name
  }
}

resource "aws_cloudwatch_log_group" "analytics_fn" {
  name              = local.analytics_log_group_name
  retention_in_days = 14

  tags = {
    Name = local.analytics_log_group_name
  }
}

resource "aws_iam_role_policy" "ingest_fn" {
  name = "ingest-fn-inline-policy"
  role = aws_iam_role.ingest_fn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = aws_dynamodb_table.order_metadata.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.order_events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = local.api_key_secret_arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.ingest_fn.arn}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "analytics_fn" {
  name = "analytics-fn-inline-policy"
  role = aws_iam_role.analytics_fn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
        ]
        Resource = aws_sqs_queue.order_events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.archive.arn}/raw/*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = local.db_app_user_secret_arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.analytics_fn.arn}:*"
      }
    ]
  })
}

data "archive_file" "ingest_fn" {
  type        = "zip"
  output_path = "${path.module}/ingest_fn.zip"

  source {
    filename = "index.py"
    content  = <<-PY
      import json
      import os

      import boto3


      def lambda_handler(event, context):
          sns = boto3.client("sns")
          secretsmanager = boto3.client("secretsmanager")
          dynamodb = boto3.resource("dynamodb")

          secretsmanager.get_secret_value(
              SecretId=os.environ["API_SECRET_ARN"],
          )

          sns.publish(
              TopicArn=os.environ["SNS_TOPIC_ARN"],
              Message=json.dumps({"event": "order_received", "source": "api"}),
          )

          dynamodb.Table(os.environ["TABLE_NAME"]).put_item(
              Item={
                  "pk": "ORDER",
                  "sk": "STATIC",
                  "source": "api",
                  "ttl": 4102444800,
              }
          )

          return {
              "statusCode": 200,
              "body": json.dumps({"ok": True}),
          }
    PY
  }
}

data "archive_file" "analytics_fn" {
  type        = "zip"
  output_path = "${path.module}/analytics_fn.zip"

  source {
    filename = "index.py"
    content  = <<-PY
      import json
      import os

      import boto3


      def lambda_handler(event, context):
          sqs = boto3.client("sqs")
          s3 = boto3.client("s3")
          secretsmanager = boto3.client("secretsmanager")

          secretsmanager.get_secret_value(
              SecretId=os.environ["DB_SECRET_ARN"],
          )

          response = sqs.receive_message(
              QueueUrl=os.environ["QUEUE_URL"],
              MaxNumberOfMessages=1,
              WaitTimeSeconds=5,
          )

          messages = response.get("Messages", [])
          processed = bool(messages)

          if processed:
              sqs.delete_message(
                  QueueUrl=os.environ["QUEUE_URL"],
                  ReceiptHandle=messages[0]["ReceiptHandle"],
              )
              s3.put_object(
                  Bucket=os.environ["BUCKET_NAME"],
                  Key="raw/analytics-marker.txt",
                  Body=b"processed",
              )

          return {
              "statusCode": 200,
              "body": json.dumps({"processed": processed}),
          }
    PY
  }
}

resource "aws_lambda_function" "ingest_fn" {
  function_name    = local.ingest_function_name
  role             = aws_iam_role.ingest_fn.arn
  runtime          = "python3.11"
  architectures    = ["x86_64"]
  handler          = "index.lambda_handler"
  filename         = data.archive_file.ingest_fn.output_path
  source_code_hash = data.archive_file.ingest_fn.output_base64sha256
  timeout          = 10
  memory_size      = 256

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      TABLE_NAME     = aws_dynamodb_table.order_metadata.name
      SNS_TOPIC_ARN  = aws_sns_topic.order_events.arn
      API_SECRET_ARN = local.api_key_secret_arn
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.ingest_fn,
    aws_iam_role_policy.ingest_fn,
  ]

  tags = {
    Name = local.ingest_function_name
  }
}

resource "aws_lambda_function" "analytics_fn" {
  function_name    = local.analytics_function_name
  role             = aws_iam_role.analytics_fn.arn
  runtime          = "python3.11"
  architectures    = ["x86_64"]
  handler          = "index.lambda_handler"
  filename         = data.archive_file.analytics_fn.output_path
  source_code_hash = data.archive_file.analytics_fn.output_base64sha256
  timeout          = 10
  memory_size      = 256

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      QUEUE_URL     = local.endpoint_mode ? local.endpoint_queue_url : aws_sqs_queue.order_events.id
      BUCKET_NAME   = aws_s3_bucket.archive.bucket
      DB_SECRET_ARN = local.db_app_user_secret_arn
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.analytics_fn,
    aws_iam_role_policy.analytics_fn,
  ]

  tags = {
    Name = local.analytics_function_name
  }
}

resource "aws_s3_bucket_policy" "archive" {
  bucket = aws_s3_bucket.archive.bucket

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.archive.arn,
          "${aws_s3_bucket.archive.arn}/*",
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid    = "AllowAnalyticsRead"
        Effect = "Allow"
        Principal = {
          AWS = local.endpoint_mode ? local.endpoint_analytics_role_arn : aws_iam_role.analytics_fn.arn
        }
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.archive.arn
      },
      {
        Sid    = "AllowAnalyticsGetObject"
        Effect = "Allow"
        Principal = {
          AWS = local.endpoint_mode ? local.endpoint_analytics_role_arn : aws_iam_role.analytics_fn.arn
        }
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.archive.arn}/*"
      },
      {
        Sid    = "AllowIngestRawWrites"
        Effect = "Allow"
        Principal = {
          AWS = local.endpoint_mode ? local.endpoint_ingest_role_arn : aws_iam_role.ingest_fn.arn
        }
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.archive.arn}/raw/*"
      },
    ]
  })
}

resource "aws_api_gateway_rest_api" "order_intake" {
  name = "order-intake-api"

  tags = {
    Name = "order-intake-api"
  }
}

resource "aws_api_gateway_resource" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.order_intake.id
  parent_id   = aws_api_gateway_rest_api.order_intake.root_resource_id
  path_part   = "ingest"
}

resource "aws_api_gateway_method" "ingest_post" {
  rest_api_id   = aws_api_gateway_rest_api.order_intake.id
  resource_id   = aws_api_gateway_resource.ingest.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "ingest_post" {
  rest_api_id             = aws_api_gateway_rest_api.order_intake.id
  resource_id             = aws_api_gateway_resource.ingest.id
  http_method             = aws_api_gateway_method.ingest_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.ingest_fn.invoke_arn}/invocations"
}

resource "aws_api_gateway_deployment" "order_intake" {
  rest_api_id = aws_api_gateway_rest_api.order_intake.id

  triggers = {
    redeploy = sha1(jsonencode({
      resource    = aws_api_gateway_resource.ingest.id
      method      = aws_api_gateway_method.ingest_post.id
      integration = aws_api_gateway_integration.ingest_post.id
    }))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.order_intake.id
  deployment_id = aws_api_gateway_deployment.order_intake.id
  stage_name    = "v1"

  tags = {
    Name = "v1"
  }
}

resource "aws_lambda_permission" "api_gateway_ingest" {
  statement_id  = "AllowApiGatewayInvokeIngest"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_fn.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.order_intake.execution_arn}/${aws_api_gateway_stage.v1.stage_name}/POST/ingest"
}

resource "aws_cloudwatch_event_rule" "analytics" {
  name                = "analytics-every-5-min"
  schedule_expression = "rate(5 minutes)"

  tags = {
    Name = "analytics-every-5-min"
  }
}

resource "aws_lambda_permission" "eventbridge_analytics" {
  statement_id  = "AllowEventBridgeInvokeAnalytics"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analytics_fn.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.analytics.arn
}

resource "aws_cloudwatch_event_target" "analytics" {
  rule = aws_cloudwatch_event_rule.analytics.name
  arn  = aws_lambda_function.analytics_fn.arn

  depends_on = [aws_lambda_permission.eventbridge_analytics]
}

resource "aws_db_subnet_group" "orders" {
  count      = local.endpoint_mode ? 0 : 1
  name       = "order-intake-db-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = {
    Name = "order-intake-db-subnet-group"
  }
}

resource "aws_db_instance" "orders" {
  count                   = local.endpoint_mode ? 0 : 1
  identifier              = "order-intake-db"
  engine                  = "postgres"
  engine_version          = "15.4"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  storage_type            = "gp2"
  storage_encrypted       = true
  publicly_accessible     = false
  backup_retention_period = 1
  deletion_protection     = false
  skip_final_snapshot     = true
  db_name                 = "orders"
  username                = local.db_app_user_secret.username
  password                = local.db_app_user_secret.password
  db_subnet_group_name    = aws_db_subnet_group.orders[0].name
  vpc_security_group_ids  = [aws_security_group.database.id]

  tags = {
    Name = "order-intake-db"
  }
}

output "api_invoke_base_url_v1" {
  value = local.endpoint_mode ? "${local.endpoint_base}/restapis/${aws_api_gateway_rest_api.order_intake.id}/${aws_api_gateway_stage.v1.stage_name}/_user_request_" : "https://${aws_api_gateway_rest_api.order_intake.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.v1.stage_name}"
}

output "sns_topic_arn" {
  value = local.endpoint_mode ? local.endpoint_topic_arn : aws_sns_topic.order_events.arn
}

output "sqs_queue_url" {
  value = local.endpoint_mode ? local.endpoint_queue_url : aws_sqs_queue.order_events.url
}

output "dynamodb_table_name" {
  value = local.table_name
}

output "s3_bucket_name" {
  value = local.bucket_name
}

output "rds_endpoint_address" {
  value = local.endpoint_mode ? local.endpoint_host : aws_db_instance.orders[0].address
}

output "rds_endpoint_port" {
  value = local.endpoint_mode ? local.endpoint_port : aws_db_instance.orders[0].port
}

output "api_key_secret_arn" {
  value = local.api_key_secret_arn
}

output "db_app_user_secret_arn" {
  value = local.db_app_user_secret_arn
}
