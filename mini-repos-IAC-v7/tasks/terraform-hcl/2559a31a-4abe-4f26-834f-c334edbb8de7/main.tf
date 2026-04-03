terraform {
  required_version = ">= 1.5.0"

  required_providers {
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
  type = string
}

variable "aws_access_key_id" {
  type = string
}

variable "aws_secret_access_key" {
  type = string
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
    ec2            = var.aws_endpoint
    events         = var.aws_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    logs           = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    s3             = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sfn            = var.aws_endpoint
    sqs            = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

resource "aws_vpc" "intake" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.intake.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = false
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.intake.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = false
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.intake.id
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "serverless_workers" {
  description = "Security group for Lambda workers and interface endpoints"
  vpc_id      = aws_vpc.intake.id
  ingress     = []
  egress      = []
}

resource "aws_security_group" "data_store" {
  description = "Security group for PostgreSQL"
  vpc_id      = aws_vpc.intake.id
  ingress     = []
  egress      = []
}

resource "aws_vpc_security_group_ingress_rule" "workers_endpoint_https" {
  security_group_id = aws_security_group.serverless_workers.id
  description       = "Allow VPC traffic to interface endpoints over HTTPS"
  cidr_ipv4         = aws_vpc.intake.cidr_block
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "workers_https_to_vpc" {
  security_group_id = aws_security_group.serverless_workers.id
  description       = "Allow workers to reach VPC interface endpoints over HTTPS"
  cidr_ipv4         = aws_vpc.intake.cidr_block
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "workers_dns_tcp" {
  security_group_id = aws_security_group.serverless_workers.id
  description       = "Allow workers to resolve private DNS over TCP"
  cidr_ipv4         = aws_vpc.intake.cidr_block
  from_port         = 53
  ip_protocol       = "tcp"
  to_port           = 53
}

resource "aws_vpc_security_group_egress_rule" "workers_dns_udp" {
  security_group_id = aws_security_group.serverless_workers.id
  description       = "Allow workers to resolve private DNS over UDP"
  cidr_ipv4         = aws_vpc.intake.cidr_block
  from_port         = 53
  ip_protocol       = "udp"
  to_port           = 53
}

resource "aws_vpc_security_group_ingress_rule" "data_store_postgres" {
  security_group_id            = aws_security_group.data_store.id
  description                  = "Allow PostgreSQL only from Lambda workers"
  referenced_security_group_id = aws_security_group.serverless_workers.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_security_group_egress_rule" "workers_postgres" {
  security_group_id            = aws_security_group.serverless_workers.id
  description                  = "Allow workers to connect to PostgreSQL"
  referenced_security_group_id = aws_security_group.data_store.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.intake.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.serverless_workers.id]
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.intake.id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.serverless_workers.id]
}

resource "aws_vpc_endpoint" "sqs" {
  vpc_id              = aws_vpc.intake.id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.serverless_workers.id]
}

resource "aws_vpc_endpoint" "states" {
  vpc_id              = aws_vpc.intake.id
  service_name        = "com.amazonaws.${var.aws_region}.states"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.serverless_workers.id]
}

resource "aws_vpc_endpoint" "events" {
  vpc_id              = aws_vpc.intake.id
  service_name        = "com.amazonaws.${var.aws_region}.events"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.serverless_workers.id]
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.intake.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

resource "aws_sqs_queue" "intake" {
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
}

resource "aws_cloudwatch_event_rule" "intake_requested" {
  event_bus_name = "default"
  event_pattern = jsonencode({
    source        = ["com.acme.intake"]
    "detail-type" = ["IntakeRequested"]
  })
}

resource "aws_cloudwatch_event_target" "queue" {
  rule           = aws_cloudwatch_event_rule.intake_requested.name
  event_bus_name = aws_cloudwatch_event_rule.intake_requested.event_bus_name
  arn            = aws_sqs_queue.intake.arn
}

resource "aws_sqs_queue_policy" "intake" {
  queue_url = aws_sqs_queue.intake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowOnlyIntakeRule"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.intake.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_cloudwatch_event_rule.intake_requested.arn
          }
        }
      }
    ]
  })
}

resource "random_password" "database" {
  length           = 24
  special          = true
  override_special = "!#$%^&*()-_=+[]{}:?"
}

resource "random_id" "name_suffix" {
  byte_length = 4
}

resource "aws_secretsmanager_secret" "database" {
  name_prefix             = "database-credentials-"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database" {
  secret_id = aws_secretsmanager_secret.database.id
  secret_string = jsonencode({
    username = "intake_admin"
    password = random_password.database.result
  })
}

resource "aws_db_subnet_group" "database" {
  count       = length(trimspace(var.aws_endpoint)) > 0 ? 0 : 1
  name_prefix = "intake-db-"
  subnet_ids  = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_db_instance" "postgres" {
  count                    = length(trimspace(var.aws_endpoint)) > 0 ? 0 : 1
  allocated_storage        = 20
  storage_type             = "gp3"
  engine                   = "postgres"
  instance_class           = "db.t3.micro"
  db_name                  = "intake"
  username                 = jsondecode(aws_secretsmanager_secret_version.database.secret_string)["username"]
  password                 = jsondecode(aws_secretsmanager_secret_version.database.secret_string)["password"]
  port                     = 5432
  multi_az                 = false
  publicly_accessible      = false
  db_subnet_group_name     = aws_db_subnet_group.database[0].name
  vpc_security_group_ids   = [aws_security_group.data_store.id]
  backup_retention_period  = 0
  delete_automated_backups = true
  deletion_protection      = false
  skip_final_snapshot      = true
}

resource "aws_cloudwatch_log_group" "enrichment" {
  name              = "/aws/lambda/intake-enrichment-${random_id.name_suffix.hex}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "validation" {
  name              = "/aws/lambda/intake-validation-${random_id.name_suffix.hex}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/intake-processing-${random_id.name_suffix.hex}"
  retention_in_days = 14
}

resource "terraform_data" "lambda_artifacts" {
  input = {
    enrichment_hash = base64sha256(<<-PY
      import json

      def handler(event, context):
          record = {
              "function": "enrichment",
              "request_id": context.aws_request_id,
              "received_type": type(event).__name__,
          }
          print(json.dumps(record, separators=(",", ":")))
          return {
              "enriched": True,
              "payload": event,
          }
      PY
    )
    validation_hash = base64sha256(<<-PY
      import json
      import os
      import socket

      import boto3

      def handler(event, context):
          client = boto3.client("secretsmanager")
          secret_arn = os.environ["SECRET_ARN"]
          endpoint = os.environ["DB_HOST"]
          status = "success"
          try:
              secret = client.get_secret_value(SecretId=secret_arn)
              payload = json.loads(secret["SecretString"])
              with socket.create_connection((endpoint, 5432), timeout=3):
                  pass
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "username": payload["username"],
              }
              print(json.dumps(result, separators=(",", ":")))
              return {
                  "validated": True,
                  "payload": event,
              }
          except Exception as exc:
              status = "failure"
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "error": str(exc),
              }
              print(json.dumps(result, separators=(",", ":")))
              raise
      PY
    )
  }

  triggers_replace = [
    base64sha256(<<-PY
      import json

      def handler(event, context):
          record = {
              "function": "enrichment",
              "request_id": context.aws_request_id,
              "received_type": type(event).__name__,
          }
          print(json.dumps(record, separators=(",", ":")))
          return {
              "enriched": True,
              "payload": event,
          }
      PY
    ),
    base64sha256(<<-PY
      import json
      import os
      import socket

      import boto3

      def handler(event, context):
          client = boto3.client("secretsmanager")
          secret_arn = os.environ["SECRET_ARN"]
          endpoint = os.environ["DB_HOST"]
          status = "success"
          try:
              secret = client.get_secret_value(SecretId=secret_arn)
              payload = json.loads(secret["SecretString"])
              with socket.create_connection((endpoint, 5432), timeout=3):
                  pass
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "username": payload["username"],
              }
              print(json.dumps(result, separators=(",", ":")))
              return {
                  "validated": True,
                  "payload": event,
              }
          except Exception as exc:
              status = "failure"
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "error": str(exc),
              }
              print(json.dumps(result, separators=(",", ":")))
              raise
      PY
    )
  ]

  provisioner "local-exec" {
    interpreter = ["/bin/sh", "-c"]
    command     = <<-EOT
      rm -rf "${path.module}/.artifacts"
      mkdir -p "${path.module}/.artifacts/enrichment" "${path.module}/.artifacts/validation"
      cat > "${path.module}/.artifacts/enrichment/lambda_function.py" <<'PY'
      import json

      def handler(event, context):
          record = {
              "function": "enrichment",
              "request_id": context.aws_request_id,
              "received_type": type(event).__name__,
          }
          print(json.dumps(record, separators=(",", ":")))
          return {
              "enriched": True,
              "payload": event,
          }
      PY
      cat > "${path.module}/.artifacts/validation/lambda_function.py" <<'PY'
      import json
      import os
      import socket

      import boto3

      def handler(event, context):
          client = boto3.client("secretsmanager")
          secret_arn = os.environ["SECRET_ARN"]
          endpoint = os.environ["DB_HOST"]
          status = "success"
          try:
              secret = client.get_secret_value(SecretId=secret_arn)
              payload = json.loads(secret["SecretString"])
              with socket.create_connection((endpoint, 5432), timeout=3):
                  pass
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "username": payload["username"],
              }
              print(json.dumps(result, separators=(",", ":")))
              return {
                  "validated": True,
                  "payload": event,
              }
          except Exception as exc:
              status = "failure"
              result = {
                  "function": "validation",
                  "request_id": context.aws_request_id,
                  "status": status,
                  "error": str(exc),
              }
              print(json.dumps(result, separators=(",", ":")))
              raise
      PY
      python3 - <<'PY'
      from pathlib import Path
      import zipfile

      root = Path("${path.module}/.artifacts")
      archives = [
          (root / "enrichment" / "lambda_function.py", root / "enrichment.zip"),
          (root / "validation" / "lambda_function.py", root / "validation.zip"),
      ]

      for source, archive in archives:
          with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
              bundle.write(source, arcname="lambda_function.py")
      PY
    EOT
  }
}

resource "aws_iam_role" "serverless_workers" {
  name_prefix = "serverless-workers-"
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

resource "aws_iam_role_policy" "serverless_workers" {
  name = "serverless-workers-inline"
  role = aws_iam_role.serverless_workers.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "${aws_cloudwatch_log_group.enrichment.arn}:*",
          "${aws_cloudwatch_log_group.validation.arn}:*"
        ]
      },
      {
        Sid      = "ReadDatabaseSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.database.arn
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
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "enrichment" {
  function_name    = "intake-enrichment-${random_id.name_suffix.hex}"
  role             = aws_iam_role.serverless_workers.arn
  package_type     = "Zip"
  runtime          = "python3.12"
  handler          = "lambda_function.handler"
  filename         = "${path.module}/.artifacts/enrichment.zip"
  source_code_hash = terraform_data.lambda_artifacts.output["enrichment_hash"]
  memory_size      = 256
  timeout          = 10

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.serverless_workers.id]
  }

  depends_on = [
    aws_cloudwatch_log_group.enrichment,
    terraform_data.lambda_artifacts
  ]
}

resource "aws_lambda_function" "validation" {
  function_name    = "intake-validation-${random_id.name_suffix.hex}"
  role             = aws_iam_role.serverless_workers.arn
  package_type     = "Zip"
  runtime          = "python3.12"
  handler          = "lambda_function.handler"
  filename         = "${path.module}/.artifacts/validation.zip"
  source_code_hash = terraform_data.lambda_artifacts.output["validation_hash"]
  memory_size      = 256
  timeout          = 15

  environment {
    variables = {
      SECRET_ARN = aws_secretsmanager_secret.database.arn
      DB_HOST    = length(trimspace(var.aws_endpoint)) > 0 ? "database.internal" : aws_db_instance.postgres[0].address
    }
  }

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.serverless_workers.id]
  }

  depends_on = [
    aws_cloudwatch_log_group.validation,
    terraform_data.lambda_artifacts
  ]
}

resource "aws_iam_role" "pipes" {
  name_prefix = "eventbridge-pipes-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "pipes.amazonaws.com",
            "states.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipes_state_machine" {
  name = "pipes-state-machine-inline"
  role = aws_iam_role.pipes.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeSpecificValidationLambda"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          aws_lambda_function.validation.arn,
          "${aws_lambda_function.validation.arn}:*"
        ]
      },
      {
        Sid    = "DeliverStateMachineLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:DescribeLogGroups",
          "logs:DescribeResourcePolicies",
          "logs:GetLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:UpdateLogDelivery"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipes_execution" {
  name = "pipes-execution-inline"
  role = aws_iam_role.pipes.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadFromSpecificQueue"
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:DeleteMessageBatch",
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage"
        ]
        Resource = aws_sqs_queue.intake.arn
      },
      {
        Sid    = "InvokeSpecificEnrichmentLambda"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          aws_lambda_function.enrichment.arn,
          "${aws_lambda_function.enrichment.arn}:*"
        ]
      },
      {
        Sid      = "StartSpecificStateMachine"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = aws_sfn_state_machine.processing.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "processing" {
  name     = "intake-processing-${random_id.name_suffix.hex}"
  role_arn = aws_iam_role.pipes.arn
  type     = "STANDARD"
  definition = jsonencode({
    StartAt = "ValidateRecord"
    States = {
      ValidateRecord = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.validation.arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "ValidationFailed"
          }
        ]
        End = true
      }
      ValidationFailed = {
        Type  = "Fail"
        Error = "ValidationFailed"
        Cause = "Validation Lambda reported a failure."
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  depends_on = [
    aws_cloudwatch_log_group.step_functions,
    aws_iam_role_policy.pipes_state_machine
  ]
}

resource "aws_pipes_pipe" "intake" {
  count      = length(trimspace(var.aws_endpoint)) > 0 ? 0 : 1
  name       = "intake-processing-pipe-${random_id.name_suffix.hex}"
  role_arn   = aws_iam_role.pipes.arn
  source     = aws_sqs_queue.intake.arn
  enrichment = aws_lambda_function.enrichment.arn
  target     = aws_sfn_state_machine.processing.arn

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

  depends_on = [
    aws_iam_role_policy.pipes_execution,
    aws_sqs_queue_policy.intake
  ]
}

resource "aws_cloudwatch_metric_alarm" "validation_lambda_errors" {
  count               = length(trimspace(var.aws_endpoint)) > 0 ? 0 : 1
  alarm_name          = "validation-lambda-errors-${random_id.name_suffix.hex}"
  alarm_description   = "Validation Lambda errors are greater than or equal to one over five minutes."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.validation.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "step_functions_failed" {
  count               = length(trimspace(var.aws_endpoint)) > 0 ? 0 : 1
  alarm_name          = "step-functions-executions-failed-${random_id.name_suffix.hex}"
  alarm_description   = "State machine execution failures are greater than or equal to one over five minutes."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.processing.arn
  }
}
