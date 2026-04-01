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
    apigateway     = var.aws_endpoint
    cloudwatchlogs = var.aws_endpoint
    ec2            = var.aws_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    s3             = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sfn            = var.aws_endpoint
    sqs            = var.aws_endpoint
    sts            = var.aws_endpoint
  }
}

data "archive_file" "lambda" {
  type        = "zip"
  output_path = "${path.module}/ingest-function.zip"

  source {
    filename = "index.py"
    content  = <<-PYTHON
      import json
      import os

      import boto3

      s3 = boto3.client("s3")
      sqs = boto3.client("sqs")


      def handler(event, context):
          if isinstance(event, dict) and event.get("httpMethod") == "POST" and event.get("resource") == "/ingest":
              body = event.get("body") or "{}"
              payload = json.loads(body) if isinstance(body, str) else body
              request_id = event.get("requestContext", {}).get("requestId", "ingest-request")
              object_key = f"ingest/{request_id}.json"

              s3.put_object(
                  Bucket=os.environ["ARCHIVE_BUCKET"],
                  Key=object_key,
                  Body=json.dumps(payload).encode("utf-8"),
                  ContentType="application/json",
              )
              sqs.send_message(
                  QueueUrl=os.environ["QUEUE_URL"],
                  MessageBody=json.dumps(
                      {
                          "archive_key": object_key,
                          "payload": payload,
                      }
                  ),
              )

              return {
                  "statusCode": 202,
                  "body": json.dumps(
                      {
                          "message": "accepted",
                          "archive_key": object_key,
                      }
                  ),
              }

          if isinstance(event, dict) and "Records" in event:
              return {
                  "processed_records": len(event["Records"]),
              }

          return {
              "processed": True,
              "event": event,
          }
    PYTHON
  }
}

resource "terraform_data" "preclean_named_resources" {
  triggers_replace = {
    aws_endpoint = var.aws_endpoint
    aws_region   = var.aws_region
  }

  provisioner "local-exec" {
    when = create

    environment = {
      AWS_ENDPOINT = var.aws_endpoint
      AWS_REGION   = var.aws_region
    }

    command = <<-EOT
      python3 - <<'PY'
      import os
      import time

      import boto3
      from botocore.config import Config
      from botocore.exceptions import ClientError

      REGION = os.environ["AWS_REGION"]
      ENDPOINT = os.environ["AWS_ENDPOINT"]
      ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "test")
      SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")


      def client(service_name):
          kwargs = {
              "region_name": REGION,
              "endpoint_url": ENDPOINT,
              "aws_access_key_id": ACCESS_KEY,
              "aws_secret_access_key": SECRET_KEY,
          }
          if service_name == "s3":
              kwargs["config"] = Config(s3={"addressing_style": "path"})
          return boto3.client(service_name, **kwargs)


      def ignore_not_found(func, *args, **kwargs):
          try:
              return func(*args, **kwargs)
          except ClientError as error:
              code = error.response.get("Error", {}).get("Code", "")
              if code in {
                  "ResourceNotFoundException",
                  "ResourceNotFoundFault",
                  "NoSuchBucket",
                  "NoSuchEntity",
                  "QueueDoesNotExist",
                  "DBInstanceNotFound",
                  "DBSubnetGroupNotFoundFault",
                  "InvalidParameterValue",
                  "NotFoundException",
              }:
                  return None
              raise


      logs = client("logs")
      s3 = client("s3")
      secretsmanager = client("secretsmanager")
      rds = client("rds")
      sqs = client("sqs")
      iam = client("iam")
      lambda_client = client("lambda")
      apigateway = client("apigateway")
      stepfunctions = client("stepfunctions")
      pipes = client("pipes")

      ignore_not_found(pipes.delete_pipe, Name="ingest-pipe")

      for _ in range(30):
          pipe_names = {pipe["Name"] for pipe in pipes.list_pipes().get("Pipes", [])}
          if "ingest-pipe" not in pipe_names:
              break
          time.sleep(1)

      for machine in stepfunctions.list_state_machines().get("stateMachines", []):
          if machine["name"] == "ingest-state-machine":
              ignore_not_found(stepfunctions.delete_state_machine, stateMachineArn=machine["stateMachineArn"])

      apis = apigateway.get_rest_apis().get("items", [])
      for api in apis:
          if api.get("name") == "ingest-api":
              ignore_not_found(apigateway.delete_rest_api, restApiId=api["id"])

      try:
          mappings = lambda_client.list_event_source_mappings(FunctionName="ingest-function").get("EventSourceMappings", [])
      except ClientError:
          mappings = []
      for mapping in mappings:
          ignore_not_found(lambda_client.delete_event_source_mapping, UUID=mapping["UUID"])

      ignore_not_found(lambda_client.delete_function, FunctionName="ingest-function")

      queue_url = None
      try:
          queue_url = sqs.get_queue_url(QueueName="ingest-queue")["QueueUrl"]
      except ClientError:
          queue_url = None
      if queue_url:
          ignore_not_found(sqs.delete_queue, QueueUrl=queue_url)

      try:
          ignore_not_found(rds.delete_db_instance, DBInstanceIdentifier="payments-db", SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
      except ClientError as error:
          if error.response.get("Error", {}).get("Code") != "InvalidDBInstanceState":
              raise

      for _ in range(60):
          try:
              rds.describe_db_instances(DBInstanceIdentifier="payments-db")
              time.sleep(2)
          except ClientError as error:
              if error.response.get("Error", {}).get("Code") == "DBInstanceNotFound":
                  break
              raise

      ignore_not_found(rds.delete_db_subnet_group, DBSubnetGroupName="payments-db-subnet-group")

      for role_name, policy_name in [
          ("lambda-execution-role", "lambda-inline-policy"),
          ("step-functions-role", "step-functions-inline-policy"),
          ("eventbridge-pipes-role", "eventbridge-pipes-inline-policy"),
      ]:
          try:
              attached_policies = iam.list_attached_role_policies(RoleName=role_name).get("AttachedPolicies", [])
          except ClientError:
              attached_policies = []
          for attached in attached_policies:
              ignore_not_found(iam.detach_role_policy, RoleName=role_name, PolicyArn=attached["PolicyArn"])
          try:
              inline_policies = iam.list_role_policies(RoleName=role_name).get("PolicyNames", [])
          except ClientError:
              inline_policies = []
          for inline_name in inline_policies:
              ignore_not_found(iam.delete_role_policy, RoleName=role_name, PolicyName=inline_name)
          ignore_not_found(iam.delete_role, RoleName=role_name)

      ignore_not_found(secretsmanager.delete_secret, SecretId="db-credentials", ForceDeleteWithoutRecovery=True)

      for log_group_name in ["/aws/lambda/ingest-function", "/aws/vendedlogs/states/ingest-state-machine"]:
          ignore_not_found(logs.delete_log_group, logGroupName=log_group_name)

      try:
          paginator = s3.get_paginator("list_object_versions")
          for page in paginator.paginate(Bucket="payments-ingest-bucket"):
              objects = []
              for version in page.get("Versions", []):
                  objects.append({"Key": version["Key"], "VersionId": version["VersionId"]})
              for marker in page.get("DeleteMarkers", []):
                  objects.append({"Key": marker["Key"], "VersionId": marker["VersionId"]})
              if objects:
                  s3.delete_objects(Bucket="payments-ingest-bucket", Delete={"Objects": objects})
      except ClientError:
          pass

      try:
          paginator = s3.get_paginator("list_objects_v2")
          for page in paginator.paginate(Bucket="payments-ingest-bucket"):
              objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
              if objects:
                  s3.delete_objects(Bucket="payments-ingest-bucket", Delete={"Objects": objects})
      except ClientError:
          pass

      ignore_not_found(s3.delete_bucket_policy, Bucket="payments-ingest-bucket")
      ignore_not_found(s3.delete_bucket, Bucket="payments-ingest-bucket")
      PY
    EOT
  }
}

resource "random_password" "db" {
  length           = 20
  special          = true
  min_numeric      = 1
  min_special      = 1
  override_special = "!#$%^&*()-_=+[]{}:"
}

resource "aws_vpc" "payments" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "payments-ingestion-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.payments.id
  cidr_block              = "10.20.0.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "payments-public-a"
  }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.payments.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true

  tags = {
    Name = "payments-public-b"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.payments.id
  cidr_block              = "10.20.10.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = false

  tags = {
    Name = "payments-private-a"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.payments.id
  cidr_block              = "10.20.11.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = false

  tags = {
    Name = "payments-private-b"
  }
}

resource "aws_internet_gateway" "payments" {
  vpc_id = aws_vpc.payments.id

  tags = {
    Name = "payments-ingestion-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.payments.id

  tags = {
    Name = "payments-public-rt"
  }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.payments.id
}

resource "aws_route_table_association" "public_a" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_b" {
  subnet_id      = aws_subnet.public_b.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.payments.id

  tags = {
    Name = "payments-private-rt"
  }
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
  vpc_id            = aws_vpc.payments.id
  vpc_endpoint_type = "Gateway"
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "payments-s3-endpoint"
  }
}

resource "aws_security_group" "api" {
  name        = "api-security-group"
  description = "API layer security group"
  vpc_id      = aws_vpc.payments.id

  ingress {
    description = "HTTPS from anywhere"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "worker" {
  name        = "worker-security-group"
  description = "Lambda worker security group"
  vpc_id      = aws_vpc.payments.id

  egress {
    description = "Allow all egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "database" {
  name        = "database-security-group"
  description = "Database security group"
  vpc_id      = aws_vpc.payments.id

  ingress {
    description     = "PostgreSQL from worker security group"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.worker.id]
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/ingest-function"
  retention_in_days = 14
  depends_on        = [terraform_data.preclean_named_resources]
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/ingest-state-machine"
  retention_in_days = 7
  depends_on        = [terraform_data.preclean_named_resources]
}

resource "aws_s3_bucket" "archive" {
  bucket        = "payments-ingest-bucket"
  force_destroy = true
  depends_on    = [terraform_data.preclean_named_resources]
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

resource "aws_s3_bucket_policy" "archive" {
  bucket = aws_s3_bucket.archive.id

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
      }
    ]
  })
}

resource "aws_secretsmanager_secret" "db" {
  name       = "db-credentials"
  depends_on = [terraform_data.preclean_named_resources]
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = "appuser"
    password = random_password.db.result
  })
}

resource "aws_db_subnet_group" "payments" {
  name       = "payments-db-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  depends_on = [terraform_data.preclean_named_resources]
}

resource "aws_db_instance" "payments" {
  identifier             = "payments-db"
  engine                 = "postgres"
  engine_version         = "16.3"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  storage_type           = "gp2"
  db_subnet_group_name   = aws_db_subnet_group.payments.name
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false
  skip_final_snapshot    = true
  storage_encrypted      = true
  username               = jsondecode(aws_secretsmanager_secret_version.db.secret_string).username
  password               = jsondecode(aws_secretsmanager_secret_version.db.secret_string).password
  depends_on             = [terraform_data.preclean_named_resources]
}

resource "aws_sqs_queue" "ingest" {
  name                       = "ingest-queue"
  visibility_timeout_seconds = 30
  sqs_managed_sse_enabled    = true
  depends_on                 = [terraform_data.preclean_named_resources]
}

resource "aws_iam_role" "lambda" {
  name       = "lambda-execution-role"
  depends_on = [terraform_data.preclean_named_resources]

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

resource "aws_iam_role_policy" "lambda" {
  name = "lambda-inline-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SendToQueue"
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
        ]
        Resource = aws_sqs_queue.ingest.arn
      },
      {
        Sid    = "ArchivePayloads"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.archive.arn}/*"
      },
      {
        Sid    = "ReadDatabaseSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = aws_secretsmanager_secret.db.arn
      },
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          aws_cloudwatch_log_group.lambda.arn,
          "${aws_cloudwatch_log_group.lambda.arn}:*",
        ]
      },
    ]
  })
}

resource "aws_lambda_function" "ingest" {
  function_name    = "ingest-function"
  role             = aws_iam_role.lambda.arn
  package_type     = "Zip"
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  memory_size      = 256
  timeout          = 10

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.worker.id]
  }

  environment {
    variables = {
      ARCHIVE_BUCKET = aws_s3_bucket.archive.bucket
      QUEUE_URL      = aws_sqs_queue.ingest.id
      SECRET_ARN     = aws_secretsmanager_secret.db.arn
    }
  }

  depends_on = [
    terraform_data.preclean_named_resources,
    aws_cloudwatch_log_group.lambda,
  ]
}

resource "aws_lambda_event_source_mapping" "ingest" {
  event_source_arn = aws_sqs_queue.ingest.arn
  function_name    = aws_lambda_function.ingest.arn
  batch_size       = 10
}

resource "aws_api_gateway_rest_api" "ingest" {
  name       = "ingest-api"
  depends_on = [terraform_data.preclean_named_resources]
}

resource "aws_api_gateway_resource" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  parent_id   = aws_api_gateway_rest_api.ingest.root_resource_id
  path_part   = "ingest"
}

resource "aws_api_gateway_method" "post" {
  rest_api_id   = aws_api_gateway_rest_api.ingest.id
  resource_id   = aws_api_gateway_resource.ingest.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "ingest" {
  rest_api_id             = aws_api_gateway_rest_api.ingest.id
  resource_id             = aws_api_gateway_resource.ingest.id
  http_method             = aws_api_gateway_method.post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.ingest.invoke_arn
}

resource "aws_api_gateway_deployment" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id

  depends_on = [
    aws_api_gateway_integration.ingest,
  ]
}

resource "aws_api_gateway_stage" "ingest" {
  rest_api_id   = aws_api_gateway_rest_api.ingest.id
  deployment_id = aws_api_gateway_deployment.ingest.id
  stage_name    = "prod"
}

resource "aws_lambda_permission" "apigateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.ingest.execution_arn}/*/POST/ingest"
}

resource "aws_iam_role" "step_functions" {
  name       = "step-functions-role"
  depends_on = [terraform_data.preclean_named_resources]

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

resource "aws_iam_role_policy" "step_functions" {
  name = "step-functions-inline-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeIngestLambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
        ]
        Resource = aws_lambda_function.ingest.arn
      },
      {
        Sid    = "WriteStateMachineLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          aws_cloudwatch_log_group.step_functions.arn,
          "${aws_cloudwatch_log_group.step_functions.arn}:*",
        ]
      },
      {
        Sid    = "ConfigureStateMachineLogDelivery"
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

resource "aws_sfn_state_machine" "ingest" {
  name     = "ingest-state-machine"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  definition = jsonencode({
    StartAt = "InvokeIngestFunction"
    States = {
      InvokeIngestFunction = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ingest.arn
          "Payload.$"  = "$"
        }
        End = true
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  depends_on = [
    terraform_data.preclean_named_resources,
    aws_cloudwatch_log_group.step_functions,
  ]
}

resource "aws_iam_role" "pipes" {
  name       = "eventbridge-pipes-role"
  depends_on = [terraform_data.preclean_named_resources]

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

resource "aws_iam_role_policy" "pipes" {
  name = "eventbridge-pipes-inline-policy"
  role = aws_iam_role.pipes.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ConsumeIngestQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = aws_sqs_queue.ingest.arn
      },
      {
        Sid    = "InvokeEnrichmentLambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
        ]
        Resource = aws_lambda_function.ingest.arn
      },
      {
        Sid    = "StartStateMachineExecution"
        Effect = "Allow"
        Action = [
          "states:StartExecution",
        ]
        Resource = aws_sfn_state_machine.ingest.arn
      },
    ]
  })
}

resource "aws_pipes_pipe" "ingest" {
  name       = "ingest-pipe"
  role_arn   = aws_iam_role.pipes.arn
  source     = aws_sqs_queue.ingest.arn
  target     = aws_sfn_state_machine.ingest.arn
  depends_on = [terraform_data.preclean_named_resources]

  enrichment = aws_lambda_function.ingest.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size = 10
    }
  }

  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }

  desired_state = "RUNNING"
}
