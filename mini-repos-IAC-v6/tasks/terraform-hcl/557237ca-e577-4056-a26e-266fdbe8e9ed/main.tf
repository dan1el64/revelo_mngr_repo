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
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "AWS endpoint override"
  type        = string
}

provider "aws" {
  region                      = var.aws_region
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    apigateway       = var.aws_endpoint
    cloudwatchlogs   = var.aws_endpoint
    cloudwatchevents = var.aws_endpoint
    ec2              = var.aws_endpoint
    iam              = var.aws_endpoint
    lambda           = var.aws_endpoint
    pipes            = var.aws_endpoint
    rds              = var.aws_endpoint
    s3               = var.aws_endpoint
    secretsmanager   = var.aws_endpoint
    sfn              = var.aws_endpoint
    sns              = var.aws_endpoint
    sqs              = var.aws_endpoint
    sts              = var.aws_endpoint
  }
}

resource "random_string" "suffix" {
  length  = 8
  lower   = true
  numeric = true
  special = false
  upper   = false
}

data "aws_caller_identity" "current" {}

locals {
  suffix     = random_string.suffix.result
  local_mode = data.aws_caller_identity.current.account_id == "000000000000"

  api_function_name        = "orders-api-${local.suffix}"
  enrichment_function_name = "orders-enrichment-${local.suffix}"
  api_name                 = "orders-api-${local.suffix}"
  event_bus_name           = "orders-bus-${local.suffix}"
  event_rule_name          = "orders-created-${local.suffix}"
  pipe_name                = "orders-pipe-${local.suffix}"
  state_machine_name       = "orders-state-machine-${local.suffix}"
  db_secret_name           = "orders-db-secret-${local.suffix}"
  db_username              = "orders_admin"

  api_lambda_log_group_name        = "/aws/lambda/${local.api_function_name}"
  enrichment_lambda_log_group_name = "/aws/lambda/${local.enrichment_function_name}"

  azs = ["${var.aws_region}a", "${var.aws_region}b"]

  api_handler_source = <<-JS
    const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
    const { EventBridgeClient, PutEventsCommand } = require("@aws-sdk/client-eventbridge");
    const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");

    const s3 = new S3Client({});
    const eventBridge = new EventBridgeClient({});
    const sns = new SNSClient({});

    const streamToString = async (stream) => {
      const chunks = [];
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      return Buffer.concat(chunks).toString("utf-8");
    };

    exports.handler = async (event) => {
      const resource = event.resource;
      const method = event.httpMethod;
      const orderId = event.pathParameters?.id;

      if (method === "POST" && resource === "/orders") {
        const payload = event.body ? JSON.parse(event.body) : {};
        const effectiveOrderId = payload.id || event.requestContext.requestId;

        await s3.send(new PutObjectCommand({
          Bucket: process.env.BUCKET_NAME,
          Key: `orders/$${effectiveOrderId}.json`,
          Body: JSON.stringify(payload),
          ContentType: "application/json"
        }));

        await eventBridge.send(new PutEventsCommand({
          Entries: [
            {
              EventBusName: process.env.EVENT_BUS_NAME,
              Source: "orders.api",
              DetailType: "order.created",
              Detail: JSON.stringify({
                orderId: effectiveOrderId,
                attachmentKey: `orders/$${effectiveOrderId}.json`
              })
            }
          ]
        }));

        return {
          statusCode: 201,
          body: JSON.stringify({ orderId: effectiveOrderId })
        };
      }

      if (method === "GET" && resource === "/orders/{id}") {
        const response = await s3.send(new GetObjectCommand({
          Bucket: process.env.BUCKET_NAME,
          Key: `orders/$${orderId}.json`
        }));

        return {
          statusCode: 200,
          body: await streamToString(response.Body)
        };
      }

      if (method === "POST" && resource === "/orders/{id}/notify") {
        const message = event.body ? JSON.parse(event.body) : {};

        await sns.send(new PublishCommand({
          TopicArn: process.env.TOPIC_ARN,
          Subject: `Order $${orderId} notification`,
          Message: JSON.stringify({
            orderId,
            notification: message
          })
        }));

        return {
          statusCode: 202,
          body: JSON.stringify({ notified: true, orderId })
        };
      }

      return {
        statusCode: 404,
        body: JSON.stringify({ message: "Not found" })
      };
    };
  JS

  enrichment_handler_source = <<-JS
    exports.handler = async (event) => {
      return {
        originalBody: event.body ?? event,
        enriched: true
      };
    };
  JS
}

data "archive_file" "api_handler" {
  type        = "zip"
  output_path = "${path.module}/api_handler.zip"

  source {
    content  = local.api_handler_source
    filename = "index.js"
  }
}

data "archive_file" "enrichment_handler" {
  type        = "zip"
  output_path = "${path.module}/enrichment_handler.zip"

  source {
    content  = local.enrichment_handler_source
    filename = "index.js"
  }
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true
}

resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index + 10)
  availability_zone = local.azs[count.index]
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  depends_on = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route" "private_egress" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main.id
}

resource "aws_route_table_association" "private" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "lambda" {
  description = "Lambda runspaces security group"
  vpc_id      = aws_vpc.main.id
  ingress     = []
  egress      = []
}

resource "aws_security_group" "rds" {
  description = "RDS security group"
  vpc_id      = aws_vpc.main.id
  ingress     = []
  egress      = []
}

resource "aws_security_group_rule" "lambda_to_rds" {
  type                     = "egress"
  security_group_id        = aws_security_group.lambda.id
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.rds.id
}

resource "aws_security_group_rule" "lambda_to_https" {
  type              = "egress"
  security_group_id = aws_security_group.lambda.id
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "rds_from_lambda" {
  type                     = "ingress"
  security_group_id        = aws_security_group.rds.id
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.lambda.id
}

resource "aws_s3_bucket" "orders" {
  bucket_prefix = "orders-attachments-"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "orders" {
  bucket = aws_s3_bucket.orders.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "orders" {
  bucket = aws_s3_bucket.orders.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "orders" {
  bucket = aws_s3_bucket.orders.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "random_password" "db" {
  length  = 20
  special = true
}

resource "aws_secretsmanager_secret" "db" {
  name_prefix = "orders-db-"
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = local.db_username
    password = random_password.db.result
  })
}

resource "aws_db_subnet_group" "main" {
  count = local.local_mode ? 0 : 1

  name_prefix = "orders-db-subnets-"
  subnet_ids  = aws_subnet.private[*].id
}

resource "aws_db_instance" "main" {
  count = local.local_mode ? 0 : 1

  identifier_prefix        = "orders-db-"
  engine                   = "postgres"
  engine_version           = "15.4"
  instance_class           = "db.t3.micro"
  allocated_storage        = 20
  storage_encrypted        = true
  publicly_accessible      = false
  backup_retention_period  = 0
  deletion_protection      = false
  skip_final_snapshot      = true
  db_subnet_group_name     = aws_db_subnet_group.main[0].name
  vpc_security_group_ids   = [aws_security_group.rds.id]
  username                 = jsondecode(aws_secretsmanager_secret_version.db.secret_string).username
  password                 = jsondecode(aws_secretsmanager_secret_version.db.secret_string).password
  apply_immediately        = true
  delete_automated_backups = true
}

resource "aws_sqs_queue" "orders" {
  name_prefix                = "orders-events-"
  sqs_managed_sse_enabled    = true
  visibility_timeout_seconds = 60
}

resource "aws_sns_topic" "notifications" {
  name_prefix       = "orders-notifications-"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_cloudwatch_event_bus" "main" {
  name = local.event_bus_name
}

resource "aws_cloudwatch_event_rule" "order_created" {
  name           = local.event_rule_name
  event_bus_name = aws_cloudwatch_event_bus.main.name
  event_pattern = jsonencode({
    source        = ["orders.api"]
    "detail-type" = ["order.created"]
  })
}

resource "aws_cloudwatch_event_target" "orders_queue" {
  rule           = aws_cloudwatch_event_rule.order_created.name
  event_bus_name = aws_cloudwatch_event_bus.main.name
  arn            = aws_sqs_queue.orders.arn
  target_id      = "orders-queue"
}

data "aws_iam_policy_document" "orders_queue_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.orders.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.order_created.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "orders" {
  queue_url = aws_sqs_queue.orders.id
  policy    = data.aws_iam_policy_document.orders_queue_policy.json
}

resource "aws_iam_role" "api_handler" {
  name_prefix = "orders-api-lambda-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role" "enrichment_handler" {
  name_prefix = "orders-enrichment-lambda-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role" "pipe" {
  name_prefix = "orders-pipe-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "pipes.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role" "sfn" {
  name_prefix = "orders-sfn-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudwatch_log_group" "api_handler" {
  name              = local.api_lambda_log_group_name
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "enrichment_handler" {
  name              = local.enrichment_lambda_log_group_name
  retention_in_days = 14
}

resource "aws_iam_role_policy" "api_handler" {
  name_prefix = "orders-api-"
  role        = aws_iam_role.api_handler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.api_handler.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = [
          aws_s3_bucket.orders.arn,
          "${aws_s3_bucket.orders.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["events:PutEvents"]
        Resource = aws_cloudwatch_event_bus.main.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.notifications.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.db.arn
      },
    ]
  })
}

resource "aws_iam_role_policy" "enrichment_handler" {
  name_prefix = "orders-enrichment-"
  role        = aws_iam_role.enrichment_handler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.enrichment_handler.arn}:*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "pipe" {
  name_prefix = "orders-pipe-"
  role        = aws_iam_role.pipe.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = aws_sqs_queue.orders.arn
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = aws_lambda_function.enrichment_handler.arn
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = aws_sfn_state_machine.main.arn
      }
    ]
  })
}

resource "aws_lambda_function" "api_handler" {
  function_name    = local.api_function_name
  role             = aws_iam_role.api_handler.arn
  runtime          = "nodejs20.x"
  handler          = "index.handler"
  memory_size      = 256
  timeout          = 15
  filename         = data.archive_file.api_handler.output_path
  source_code_hash = data.archive_file.api_handler.output_base64sha256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      BUCKET_NAME    = aws_s3_bucket.orders.bucket
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.main.name
      TOPIC_ARN      = aws_sns_topic.notifications.arn
      DB_SECRET_ARN  = aws_secretsmanager_secret.db.arn
      DB_HOST        = local.local_mode ? "" : aws_db_instance.main[0].address
    }
  }

  depends_on = [aws_cloudwatch_log_group.api_handler]
}

resource "aws_lambda_function" "enrichment_handler" {
  function_name    = local.enrichment_function_name
  role             = aws_iam_role.enrichment_handler.arn
  runtime          = "nodejs20.x"
  handler          = "index.handler"
  memory_size      = 256
  timeout          = 15
  filename         = data.archive_file.enrichment_handler.output_path
  source_code_hash = data.archive_file.enrichment_handler.output_base64sha256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  depends_on = [aws_cloudwatch_log_group.enrichment_handler]
}

resource "aws_sfn_state_machine" "main" {
  name     = local.state_machine_name
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"
  definition = jsonencode({
    StartAt = "Complete"
    States = {
      Complete = {
        Type = "Pass"
        End  = true
      }
    }
  })
}

resource "aws_pipes_pipe" "main" {
  count = local.local_mode ? 0 : 1

  name       = local.pipe_name
  role_arn   = aws_iam_role.pipe.arn
  source     = aws_sqs_queue.orders.arn
  enrichment = aws_lambda_function.enrichment_handler.arn
  target     = aws_sfn_state_machine.main.arn

  target_parameters {
    input_template = "<aws.pipes.event.json>"

    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }
}

resource "aws_api_gateway_rest_api" "main" {
  name = local.api_name

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "orders" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "orders"
}

resource "aws_api_gateway_resource" "order_id" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_resource.orders.id
  path_part   = "{id}"
}

resource "aws_api_gateway_resource" "notify" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_resource.order_id.id
  path_part   = "notify"
}

resource "aws_api_gateway_method" "post_orders" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.orders.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_method" "get_order" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.order_id.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_method" "post_notify" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.notify.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "post_orders" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.orders.id
  http_method             = aws_api_gateway_method.post_orders.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.api_handler.invoke_arn}/invocations"
}

resource "aws_api_gateway_integration" "get_order" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.order_id.id
  http_method             = aws_api_gateway_method.get_order.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.api_handler.invoke_arn}/invocations"
}

resource "aws_api_gateway_integration" "post_notify" {
  rest_api_id             = aws_api_gateway_rest_api.main.id
  resource_id             = aws_api_gateway_resource.notify.id
  http_method             = aws_api_gateway_method.post_notify.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.api_handler.invoke_arn}/invocations"
}

resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_integration.post_orders.id,
      aws_api_gateway_integration.get_order.id,
      aws_api_gateway_integration.post_notify.id
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  deployment_id = aws_api_gateway_deployment.main.id
  stage_name    = "v1"
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_handler.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/${aws_api_gateway_stage.v1.stage_name}/*/*"
}
