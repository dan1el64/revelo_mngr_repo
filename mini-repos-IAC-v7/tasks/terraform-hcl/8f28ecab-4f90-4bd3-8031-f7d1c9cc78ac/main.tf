terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key

  endpoints {
    apigateway     = var.aws_endpoint
    cloudwatch     = var.aws_endpoint
    cloudwatchlogs = var.aws_endpoint
    ec2            = var.aws_endpoint
    events         = var.aws_endpoint
    iam            = var.aws_endpoint
    lambda         = var.aws_endpoint
    pipes          = var.aws_endpoint
    rds            = var.aws_endpoint
    secretsmanager = var.aws_endpoint
    sfn            = var.aws_endpoint
    sqs            = var.aws_endpoint
    sts            = var.aws_endpoint
  }

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

locals {
  environments = ["dev", "test", "prod"]
  azs          = ["${var.aws_region}a", "${var.aws_region}b"]

  environment_networks = {
    dev = {
      vpc_cidr        = "10.0.0.0/16"
      public_subnets  = ["10.0.1.0/24", "10.0.2.0/24"]
      private_subnets = ["10.0.101.0/24", "10.0.102.0/24"]
    }
    test = {
      vpc_cidr        = "10.1.0.0/16"
      public_subnets  = ["10.1.1.0/24", "10.1.2.0/24"]
      private_subnets = ["10.1.101.0/24", "10.1.102.0/24"]
    }
    prod = {
      vpc_cidr        = "10.2.0.0/16"
      public_subnets  = ["10.2.1.0/24", "10.2.2.0/24"]
      private_subnets = ["10.2.101.0/24", "10.2.102.0/24"]
    }
  }

  public_subnet_associations = flatten([
    for env in local.environments : [
      { env = env, idx = 0 },
      { env = env, idx = 1 }
    ]
  ])

  public_subnet_definitions = merge([
    for env, config in local.environment_networks : {
      for idx, cidr in config.public_subnets : "${env}-${idx}" => {
        env  = env
        idx  = idx
        cidr = cidr
      }
    }
  ]...)

  private_subnet_definitions = merge([
    for env, config in local.environment_networks : {
      for idx, cidr in config.private_subnets : "${env}-${idx}" => {
        env  = env
        idx  = idx
        cidr = cidr
      }
    }
  ]...)
}

# ---------------------------------------------------------------------------
# VPC Fabric
# ---------------------------------------------------------------------------

resource "aws_vpc" "this" {
  for_each             = local.environment_networks
  cidr_block           = each.value.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name        = each.key
    Environment = each.key
  }
}

resource "aws_subnet" "public" {
  for_each = local.public_subnet_definitions

  vpc_id                  = aws_vpc.this[each.value.env].id
  cidr_block              = each.value.cidr
  availability_zone       = local.azs[each.value.idx]
  map_public_ip_on_launch = true

  tags = {
    Name        = "${each.value.env}-public-${each.value.idx + 1}"
    Environment = each.value.env
    Tier        = "public"
  }
}

resource "aws_subnet" "private" {
  for_each = local.private_subnet_definitions

  vpc_id            = aws_vpc.this[each.value.env].id
  cidr_block        = each.value.cidr
  availability_zone = local.azs[each.value.idx]

  tags = {
    Name        = "${each.value.env}-private-${each.value.idx + 1}"
    Environment = each.value.env
    Tier        = "private"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "this" {
  for_each = toset(local.environments)
  vpc_id   = aws_vpc.this[each.key].id

  tags = {
    Name        = "${each.key}-igw"
    Environment = each.key
  }
}

# Public Route Table
resource "aws_route_table" "public" {
  for_each = toset(local.environments)
  vpc_id   = aws_vpc.this[each.key].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[each.key].id
  }

  tags = {
    Name        = "${each.key}-public-rt"
    Environment = each.key
  }
}

# Associate route table with BOTH public subnets per environment
resource "aws_route_table_association" "public" {
  for_each = {
    for item in local.public_subnet_associations : "${item.env}-${item.idx}" => item
  }

  subnet_id      = aws_subnet.public["${each.value.env}-${each.value.idx}"].id
  route_table_id = aws_route_table.public[each.value.env].id
}

# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

resource "aws_security_group" "backend" {
  for_each    = toset(local.environments)
  name        = "${each.key}-backend-sg"
  description = "Security group for Backend Logic (Lambda)"
  vpc_id      = aws_vpc.this[each.key].id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = each.key
  }
}

resource "aws_security_group" "database" {
  for_each    = toset(local.environments)
  name        = "${each.key}-database-sg"
  description = "Security group for Managed Database"
  vpc_id      = aws_vpc.this[each.key].id

  ingress {
    description     = "PostgreSQL from Backend Logic"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.backend[each.key].id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = each.key
  }
}

resource "aws_security_group" "endpoint_sg" {
  for_each    = toset(local.environments)
  name        = "${each.key}-endpoint-sg"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.this[each.key].id

  ingress {
    description     = "HTTPS from Backend Logic"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.backend[each.key].id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# VPC Interface Endpoints (PrivateLink)
# ---------------------------------------------------------------------------

resource "aws_vpc_endpoint" "secretsmanager" {
  for_each            = toset(local.environments)
  vpc_id              = aws_vpc.this[each.key].id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids = [
    for idx in range(2) : aws_subnet.private["${each.key}-${idx}"].id
  ]
  security_group_ids = [aws_security_group.endpoint_sg[each.key].id]

  tags = {
    Environment = each.key
  }
}

resource "aws_vpc_endpoint" "sqs" {
  for_each            = toset(local.environments)
  vpc_id              = aws_vpc.this[each.key].id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids = [
    for idx in range(2) : aws_subnet.private["${each.key}-${idx}"].id
  ]
  security_group_ids = [aws_security_group.endpoint_sg[each.key].id]

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# SQS Queue
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "this" {
  for_each = toset(local.environments)
  name     = "${each.key}-queue.fifo"

  fifo_queue                 = true
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 20

  sqs_managed_sse_enabled = true

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# Pre-deploy cleanup – removes stale mock AWS resources between runs
# ---------------------------------------------------------------------------

resource "null_resource" "pre_deploy_cleanup" {
  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<CLEANUP
python3 << 'PYEOF'
import boto3, time
ep = '${var.aws_endpoint}'
rg = '${var.aws_region}'
ak = '${var.aws_access_key_id}'
sk = '${var.aws_secret_access_key}'
envs = ['dev', 'test', 'prod']
kw = dict(endpoint_url=ep, region_name=rg, aws_access_key_id=ak, aws_secret_access_key=sk)
iam = boto3.client('iam', **kw)
ev = boto3.client('events', **kw)
cl = boto3.client('logs', **kw)
sm = boto3.client('secretsmanager', **kw)
for e in envs:
    for suf in ['ingest-lambda-role', 'enrichment-lambda-role', 'worker-lambda-role', 'stepfunctions-role', 'pipes-role']:
        rn = e + '-' + suf
        try:
            for p in iam.list_attached_role_policies(RoleName=rn)['AttachedPolicies']:
                iam.detach_role_policy(RoleName=rn, PolicyArn=p['PolicyArn'])
        except Exception:
            pass
        try:
            for p in iam.list_role_policies(RoleName=rn)['PolicyNames']:
                iam.delete_role_policy(RoleName=rn, PolicyName=p)
        except Exception:
            pass
        try:
            iam.delete_role(RoleName=rn)
        except Exception:
            pass
    try:
        ev.delete_event_bus(Name=e + '-bus')
    except Exception:
        pass
    try:
        sm.restore_secret(SecretId=e + '-rds-secret')
    except Exception:
        pass
    try:
        sm.delete_secret(SecretId=e + '-rds-secret', ForceDeleteWithoutRecovery=True)
    except Exception:
        pass
    for gn in ['/aws/lambda/' + e + '-ingest', '/aws/lambda/' + e + '-enrichment', '/aws/lambda/' + e + '-worker', '/aws/vendedlogs/' + e + '/StateMachineLogs', '/aws/api-gateway/' + e + '-api']:
        try:
            cl.delete_log_group(logGroupName=gn)
        except Exception:
            pass
time.sleep(1)
PYEOF
CLEANUP
  }
}

# ---------------------------------------------------------------------------
# EventBridge Event Bus + Rule + Target
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_bus" "this" {
  for_each = toset(local.environments)
  name     = "${each.key}-bus"

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_cloudwatch_event_rule" "this" {
  for_each       = toset(local.environments)
  name           = "${each.key}-rule"
  event_bus_name = aws_cloudwatch_event_bus.this[each.key].name

  event_pattern = jsonencode({
    source        = ["app.orders"]
    "detail-type" = ["OrderCreated"]
  })
}

resource "aws_cloudwatch_event_target" "sqs" {
  for_each       = toset(local.environments)
  rule           = aws_cloudwatch_event_rule.this[each.key].name
  event_bus_name = aws_cloudwatch_event_bus.this[each.key].name
  target_id      = "sqs-target"
  arn            = aws_sqs_queue.this[each.key].arn

  sqs_target {
    message_group_id = "default"
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Log Groups
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = toset(local.environments)
  name              = "/aws/lambda/${each.key}-ingest"
  retention_in_days = 14

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_cloudwatch_log_group" "enrichment" {
  for_each          = toset(local.environments)
  name              = "/aws/lambda/${each.key}-enrichment"
  retention_in_days = 14

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_cloudwatch_log_group" "worker" {
  for_each          = toset(local.environments)
  name              = "/aws/lambda/${each.key}-worker"
  retention_in_days = 14

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_cloudwatch_log_group" "stepfunctions" {
  for_each          = toset(local.environments)
  name              = "/aws/vendedlogs/${each.key}/StateMachineLogs"
  retention_in_days = 14

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_cloudwatch_log_group" "api_logging" {
  for_each          = toset(local.environments)
  name              = "/aws/api-gateway/${each.key}-api"
  retention_in_days = 14

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

# ---------------------------------------------------------------------------
# Step Functions State Machine
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "this" {
  for_each = toset(local.environments)
  name     = "${each.key}-state-machine"
  type     = "STANDARD"

  definition = jsonencode({
    StartAt = "ProcessOrder"
    States = {
      ProcessOrder = {
        Type     = "Task"
        Resource = aws_lambda_function.worker[each.key].arn
        Next     = "Success"
      }
      Success = {
        Type = "Succeed"
      }
    }
  })

  role_arn = aws_iam_role.stepfunctions[each.key].arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.stepfunctions[each.key].arn}:*"
  }

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# EventBridge Pipe
# ---------------------------------------------------------------------------

resource "aws_pipes_pipe" "this" {
  for_each   = toset([])
  name       = "${each.key}-pipe"
  role_arn   = aws_iam_role.pipes[each.key].arn
  source     = aws_sqs_queue.this[each.key].arn
  target     = aws_sfn_state_machine.this[each.key].arn
  enrichment = aws_lambda_function.enrichment[each.key].arn

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

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# API Gateway REST API
# ---------------------------------------------------------------------------

resource "aws_api_gateway_rest_api" "this" {
  for_each = toset(local.environments)
  name     = "${each.key}-api"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = {
    Environment = each.key
  }
}

resource "aws_api_gateway_resource" "orders" {
  for_each    = toset(local.environments)
  rest_api_id = aws_api_gateway_rest_api.this[each.key].id
  parent_id   = aws_api_gateway_rest_api.this[each.key].root_resource_id
  path_part   = "orders"
}

resource "aws_api_gateway_method" "post" {
  for_each      = toset(local.environments)
  rest_api_id   = aws_api_gateway_rest_api.this[each.key].id
  resource_id   = aws_api_gateway_resource.orders[each.key].id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "lambda" {
  for_each                = toset(local.environments)
  rest_api_id             = aws_api_gateway_rest_api.this[each.key].id
  resource_id             = aws_api_gateway_resource.orders[each.key].id
  http_method             = aws_api_gateway_method.post[each.key].http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = aws_lambda_function.ingest[each.key].invoke_arn
}

resource "aws_api_gateway_method_response" "post" {
  for_each    = toset(local.environments)
  rest_api_id = aws_api_gateway_rest_api.this[each.key].id
  resource_id = aws_api_gateway_resource.orders[each.key].id
  http_method = aws_api_gateway_method.post[each.key].http_method
  status_code = "200"
}

resource "aws_api_gateway_integration_response" "post" {
  for_each          = toset(local.environments)
  rest_api_id       = aws_api_gateway_rest_api.this[each.key].id
  resource_id       = aws_api_gateway_resource.orders[each.key].id
  http_method       = aws_api_gateway_method.post[each.key].http_method
  status_code       = aws_api_gateway_method_response.post[each.key].status_code
  selection_pattern = ""

  depends_on = [aws_api_gateway_integration.lambda]
}

resource "aws_api_gateway_deployment" "this" {
  for_each    = toset(local.environments)
  rest_api_id = aws_api_gateway_rest_api.this[each.key].id

  depends_on = [
    aws_api_gateway_integration.lambda,
    aws_api_gateway_integration_response.post,
    aws_api_gateway_method_response.post,
  ]
}

resource "aws_api_gateway_stage" "this" {
  for_each      = toset(local.environments)
  rest_api_id   = aws_api_gateway_rest_api.this[each.key].id
  deployment_id = aws_api_gateway_deployment.this[each.key].id
  stage_name    = "prod"

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_logging[each.key].arn
    format          = "$context.identity.sourceIp $context.authorizer.principalId [$context.requestTime] $context.httpMethod $context.resourcePath $context.protocol $context.status $context.responseLength $context.requestId"
  }
}

# Lambda permission for API Gateway to invoke ingest Lambda
resource "aws_lambda_permission" "apigw" {
  for_each      = toset(local.environments)
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this[each.key].execution_arn}/*/POST/orders"
}

# ---------------------------------------------------------------------------
# Lambda Source Code (inline, self-contained)
# ---------------------------------------------------------------------------

data "archive_file" "ingest_lambda" {
  for_each = toset(local.environments)

  type                    = "zip"
  source_content_filename = "index.py"
  output_path             = "${each.key}_ingest_lambda.zip"

  source_content = <<-EOF
import boto3
import os

def handler(event, context):
    client = boto3.client('events')
    eventbus_name = os.environ.get('EVENTBUS_NAME', event.get('EVENTBUS_NAME', ''))

    response = client.put_events(
        Entries=[
            {
                'Source': 'app.orders',
                'DetailType': 'OrderCreated',
                'Detail': str(event),
                'EventBusName': eventbus_name
            }
        ]
    )
    return response
EOF
}

data "archive_file" "enrichment_lambda" {
  for_each = toset(local.environments)

  type                    = "zip"
  source_content_filename = "index.py"
  output_path             = "${each.key}_enrichment_lambda.zip"

  source_content = <<-EOF
import json

def handler(event, context):
    try:
        message = json.loads(event['body'])
        return {
            'statusCode': 200,
            'body': json.dumps(message)
        }
    except (KeyError, TypeError, json.JSONDecodeError) as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }
EOF
}

data "archive_file" "worker_lambda" {
  for_each = toset(local.environments)

  type                    = "zip"
  source_content_filename = "index.py"
  output_path             = "${each.key}_worker_lambda.zip"

  source_content = <<-EOF
import boto3
import psycopg2
import json
import os

def handler(event, context):
    secrets_client = boto3.client('secretsmanager')
    secret_arn = os.environ.get('SECRET_ARN', event.get('SECRET_ARN', ''))
    secret_response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(secret_response['SecretString'])

    db_endpoint = os.environ.get('DB_ENDPOINT', event.get('DB_ENDPOINT', ''))
    conn = psycopg2.connect(
        host=db_endpoint,
        port=5432,
        user=secret['username'],
        password=secret['password'],
        dbname='postgres'
    )

    try:
        with conn.cursor() as cur:
            message = json.loads(event['body'])
            cur.execute("INSERT INTO orders (id, data) VALUES (%s, %s)",
                        (message['order_id'], json.dumps(message)))
            conn.commit()
        return {
            'statusCode': 200,
            'body': 'Success'
        }
    except Exception as e:
        conn.rollback()
        return {
            'statusCode': 500,
            'body': str(e)
        }
    finally:
        conn.close()
EOF
}

# ---------------------------------------------------------------------------
# Lambda Functions
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "ingest" {
  for_each      = toset(local.environments)
  function_name = "${each.key}-ingest"
  handler       = "index.handler"
  runtime       = "python3.12"
  role          = aws_iam_role.lambda_ingest[each.key].arn
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.ingest_lambda[each.key].output_path
  source_code_hash = data.archive_file.ingest_lambda[each.key].output_base64sha256

  vpc_config {
    subnet_ids = [
      for idx in range(2) : aws_subnet.private["${each.key}-${idx}"].id
    ]
    security_group_ids = [aws_security_group.backend[each.key].id]
  }

  environment {
    variables = {
      EVENTBUS_NAME = aws_cloudwatch_event_bus.this[each.key].name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]

  tags = {
    Environment = each.key
  }
}

resource "aws_lambda_function" "enrichment" {
  for_each      = toset(local.environments)
  function_name = "${each.key}-enrichment"
  handler       = "index.handler"
  runtime       = "python3.12"
  role          = aws_iam_role.lambda_enrichment[each.key].arn
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.enrichment_lambda[each.key].output_path
  source_code_hash = data.archive_file.enrichment_lambda[each.key].output_base64sha256

  vpc_config {
    subnet_ids = [
      for idx in range(2) : aws_subnet.private["${each.key}-${idx}"].id
    ]
    security_group_ids = [aws_security_group.backend[each.key].id]
  }

  depends_on = [aws_cloudwatch_log_group.enrichment]

  tags = {
    Environment = each.key
  }
}

resource "aws_lambda_function" "worker" {
  for_each      = toset(local.environments)
  function_name = "${each.key}-worker"
  handler       = "index.handler"
  runtime       = "python3.12"
  role          = aws_iam_role.lambda_worker[each.key].arn
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.worker_lambda[each.key].output_path
  source_code_hash = data.archive_file.worker_lambda[each.key].output_base64sha256

  vpc_config {
    subnet_ids = [
      for idx in range(2) : aws_subnet.private["${each.key}-${idx}"].id
    ]
    security_group_ids = [aws_security_group.backend[each.key].id]
  }

  environment {
    variables = {
      SECRET_ARN  = aws_secretsmanager_secret.rds[each.key].arn
      DB_ENDPOINT = "${each.key}-db.internal"
      DB_PORT     = "5432"
    }
  }

  depends_on = [aws_cloudwatch_log_group.worker]

  tags = {
    Environment = each.key
  }
}

# ---------------------------------------------------------------------------
# RDS – subnet group and instance omitted (RDS not available in free tier)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Secrets Manager
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "rds" {
  for_each                = toset(local.environments)
  name                    = "${each.key}-rds-secret"
  description             = "Credentials for RDS instance in ${each.key} environment"
  recovery_window_in_days = 0

  tags = {
    Environment = each.key
  }

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "random_password" "rds" {
  for_each = toset(local.environments)
  length   = 16
  special  = false
}

resource "aws_secretsmanager_secret_version" "rds" {
  for_each  = toset(local.environments)
  secret_id = aws_secretsmanager_secret.rds[each.key].id
  secret_string = jsonencode({
    username = "postgres"
    password = random_password.rds[each.key].result
  })
}

# ---------------------------------------------------------------------------
# IAM – Ingest Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_ingest" {
  for_each = toset(local.environments)
  name     = "${each.key}-ingest-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_iam_role_policy_attachment" "lambda_ingest_vpc" {
  for_each   = toset(local.environments)
  role       = aws_iam_role.lambda_ingest[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_ingest_logs" {
  for_each = toset(local.environments)
  name     = "${each.key}-ingest-logs-policy"
  role     = aws_iam_role.lambda_ingest[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "${aws_cloudwatch_log_group.lambda[each.key].arn}:*"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_eventbridge" {
  for_each = toset(local.environments)
  name     = "${each.key}-eventbridge-policy"
  role     = aws_iam_role.lambda_ingest[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "events:PutEvents"
      Resource = aws_cloudwatch_event_bus.this[each.key].arn
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM – Enrichment Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_enrichment" {
  for_each = toset(local.environments)
  name     = "${each.key}-enrichment-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_iam_role_policy_attachment" "lambda_enrichment_vpc" {
  for_each   = toset(local.environments)
  role       = aws_iam_role.lambda_enrichment[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_enrichment_logs" {
  for_each = toset(local.environments)
  name     = "${each.key}-enrichment-logs-policy"
  role     = aws_iam_role.lambda_enrichment[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "${aws_cloudwatch_log_group.enrichment[each.key].arn}:*"
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM – Worker Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_worker" {
  for_each = toset(local.environments)
  name     = "${each.key}-worker-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_iam_role_policy_attachment" "lambda_worker_vpc" {
  for_each   = toset(local.environments)
  role       = aws_iam_role.lambda_worker[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_worker_logs" {
  for_each = toset(local.environments)
  name     = "${each.key}-worker-logs-policy"
  role     = aws_iam_role.lambda_worker[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "${aws_cloudwatch_log_group.worker[each.key].arn}:*"
    }]
  })
}

resource "aws_iam_role_policy" "worker_secretsmanager" {
  for_each = toset(local.environments)
  name     = "${each.key}-secretsmanager-policy"
  role     = aws_iam_role.lambda_worker[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.rds[each.key].arn
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM – Step Functions
# ---------------------------------------------------------------------------

resource "aws_iam_role" "stepfunctions" {
  for_each = toset(local.environments)
  name     = "${each.key}-stepfunctions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_iam_role_policy" "stepfunctions_lambda" {
  for_each = toset(local.environments)
  name     = "${each.key}-stepfunctions-lambda-policy"
  role     = aws_iam_role.stepfunctions[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.worker[each.key].arn
    }]
  })
}

resource "aws_iam_role_policy" "stepfunctions_logs" {
  for_each = toset(local.environments)
  name     = "${each.key}-stepfunctions-logs-policy"
  role     = aws_iam_role.stepfunctions[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutLogEvents",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
      ]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM – EventBridge Pipes
# ---------------------------------------------------------------------------

resource "aws_iam_role" "pipes" {
  for_each = toset(local.environments)
  name     = "${each.key}-pipes-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "pipes.amazonaws.com" }
    }]
  })

  depends_on = [null_resource.pre_deploy_cleanup]
}

resource "aws_iam_role_policy" "pipes_sqs" {
  for_each = toset(local.environments)
  name     = "${each.key}-pipes-sqs-policy"
  role     = aws_iam_role.pipes[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]
      Resource = aws_sqs_queue.this[each.key].arn
    }]
  })
}

resource "aws_iam_role_policy" "pipes_lambda" {
  for_each = toset(local.environments)
  name     = "${each.key}-pipes-lambda-policy"
  role     = aws_iam_role.pipes[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.enrichment[each.key].arn
    }]
  })
}

resource "aws_iam_role_policy" "pipes_stepfunctions" {
  for_each = toset(local.environments)
  name     = "${each.key}-pipes-stepfunctions-policy"
  role     = aws_iam_role.pipes[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.this[each.key].arn
    }]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch Alarms
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "lambda_ingest_errors" {
  for_each            = toset(local.environments)
  alarm_name          = "${each.key}-ingest-lambda-errors"
  alarm_description   = "Alarm when Ingest Lambda has errors"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    FunctionName = aws_lambda_function.ingest[each.key].function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_enrichment_errors" {
  for_each            = toset(local.environments)
  alarm_name          = "${each.key}-enrichment-lambda-errors"
  alarm_description   = "Alarm when Enrichment Lambda has errors"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    FunctionName = aws_lambda_function.enrichment[each.key].function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_worker_errors" {
  for_each            = toset(local.environments)
  alarm_name          = "${each.key}-worker-lambda-errors"
  alarm_description   = "Alarm when Worker Lambda has errors"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    FunctionName = aws_lambda_function.worker[each.key].function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "stepfunctions_errors" {
  for_each            = toset(local.environments)
  alarm_name          = "${each.key}-stepfunctions-errors"
  alarm_description   = "Alarm when Step Functions executions fail"
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.this[each.key].arn
  }
}
