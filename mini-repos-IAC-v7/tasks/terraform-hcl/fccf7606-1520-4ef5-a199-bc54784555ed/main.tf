terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_endpoint" {
  type    = string
  default = ""
}

variable "aws_access_key_id" {
  type      = string
  sensitive = true
  default   = ""
}

variable "aws_secret_access_key" {
  type      = string
  sensitive = true
  default   = ""
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key

  skip_credentials_validation = var.aws_endpoint != ""
  skip_metadata_api_check     = var.aws_endpoint != ""
  skip_requesting_account_id  = var.aws_endpoint != ""
  s3_use_path_style           = var.aws_endpoint != ""

  dynamic "endpoints" {
    for_each = var.aws_endpoint != "" ? [1] : []
    content {
      apigateway     = var.aws_endpoint
      apigatewayv2   = var.aws_endpoint
      cloudfront     = var.aws_endpoint
      cloudwatch     = var.aws_endpoint
      ec2            = var.aws_endpoint
      elasticache    = var.aws_endpoint
      events         = var.aws_endpoint
      glue           = var.aws_endpoint
      iam            = var.aws_endpoint
      lambda         = var.aws_endpoint
      logs           = var.aws_endpoint
      pipes          = var.aws_endpoint
      rds            = var.aws_endpoint
      redshift       = var.aws_endpoint
      s3             = var.aws_endpoint
      secretsmanager = var.aws_endpoint
      sfn            = var.aws_endpoint
      sqs            = var.aws_endpoint
      sts            = var.aws_endpoint
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_secretsmanager_random_password" "rds" {
  password_length     = 24
  exclude_punctuation = true
}

data "aws_secretsmanager_random_password" "redshift" {
  password_length     = 24
  exclude_punctuation = true
}

locals {
  az_a = data.aws_availability_zones.available.names[0]
  az_b = data.aws_availability_zones.available.names[1]

  app_name   = "three-tier-orders"
  vpc_cidr   = "10.0.0.0/16"
  bucket_id  = "${local.app_name}-${var.aws_region}-frontend"
  local_mode = var.aws_endpoint != ""

  api_handler_name       = "api-handler"
  worker_processor_name  = "worker-processor"
  enrichment_lambda_name = "enrichment-lambda"
  order_queue_name       = "orders-queue"

  rds_username      = "dbadmin"
  redshift_username = "analyticsadmin"

  private_subnet_ids     = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  lambda_vpc_permissions = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface"]
  lambda_log_permissions = ["logs:CreateLogStream", "logs:PutLogEvents"]
  endpoint_services      = ["secretsmanager", "sqs", "logs"]

  rds_secret_payload      = jsondecode(aws_secretsmanager_secret_version.rds.secret_string)
  redshift_secret_payload = jsondecode(aws_secretsmanager_secret_version.redshift.secret_string)

  frontend_index_html = <<-HTML
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Orders Console</title>
      </head>
      <body>
        <main>
          <h1>Orders Console</h1>
          <button id="create-order">Create order</button>
          <pre id="result"></pre>
        </main>
        <script src="/app.js"></script>
      </body>
    </html>
  HTML

  frontend_app_js = <<-JS
    const button = document.getElementById("create-order");
    const result = document.getElementById("result");

    async function createOrder() {
      const response = await fetch("/api/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ item: "sample-order", quantity: 1 })
      });

      const payload = await response.json();
      result.textContent = JSON.stringify(
        {
          status: response.status,
          payload
        },
        null,
        2
      );
    }

    button.addEventListener("click", createOrder);
  JS

  api_handler_source = <<-JS
    const crypto = require("crypto");
    const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

    const sqs = new SQSClient({});

    function jsonResponse(statusCode, payload) {
      return {
        statusCode,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      };
    }

    exports.handler = async (event) => {
      let payload = {};

      if (event && event.body) {
        try {
          payload = JSON.parse(event.body);
        } catch (error) {
          return jsonResponse(400, { error: "invalid-json" });
        }
      }

      if (!payload || Array.isArray(payload) || typeof payload !== "object") {
        return jsonResponse(400, { error: "invalid-payload" });
      }

      const orderId = "order-" + crypto.randomUUID();
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: process.env.SQS_QUEUE_URL,
          MessageBody: JSON.stringify({
            orderId,
            payload,
            submittedAt: new Date().toISOString()
          })
        })
      );

      return jsonResponse(202, { orderId });
    };
  JS

  worker_processor_source = <<-JS
    const crypto = require("crypto");
    const net = require("net");
    const tls = require("tls");
    const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");

    const secretsManager = new SecretsManagerClient({});

    function int32(value) {
      const buffer = Buffer.alloc(4);
      buffer.writeInt32BE(value, 0);
      return buffer;
    }

    function cString(value) {
      return Buffer.concat([Buffer.from(value, "utf8"), Buffer.from([0])]);
    }

    function createReader(socket) {
      let buffer = Buffer.alloc(0);
      let error = null;
      const waiters = [];

      function flush() {
        while (waiters.length > 0 && buffer.length >= 5) {
          const length = buffer.readInt32BE(1);
          if (buffer.length < length + 1) {
            return;
          }

          const type = String.fromCharCode(buffer[0]);
          const body = buffer.subarray(5, length + 1);
          buffer = buffer.subarray(length + 1);
          waiters.shift().resolve({ type, body });
        }
      }

      socket.on("data", (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
        flush();
      });

      socket.on("error", (err) => {
        error = err;
        while (waiters.length > 0) {
          waiters.shift().reject(err);
        }
      });

      socket.on("close", () => {
        if (!error) {
          error = new Error("socket-closed");
        }
        while (waiters.length > 0) {
          waiters.shift().reject(error);
        }
      });

      return {
        read() {
          if (error) {
            return Promise.reject(error);
          }
          return new Promise((resolve, reject) => {
            waiters.push({ resolve, reject });
            flush();
          });
        }
      };
    }

    function buildStartupMessage(user, database) {
      const body = Buffer.concat([
        int32(196608),
        cString("user"),
        cString(user),
        cString("database"),
        cString(database),
        cString("client_encoding"),
        cString("UTF8"),
        Buffer.from([0])
      ]);
      return Buffer.concat([int32(body.length + 4), body]);
    }

    function buildPasswordMessage(user, password, authType, salt) {
      let encoded = password;

      if (authType === 5) {
        const inner = crypto.createHash("md5").update(password + user, "utf8").digest("hex");
        const outer = crypto
          .createHash("md5")
          .update(Buffer.concat([Buffer.from(inner, "utf8"), salt]))
          .digest("hex");
        encoded = "md5" + outer;
      }

      const body = Buffer.concat([Buffer.from(encoded, "utf8"), Buffer.from([0])]);
      return Buffer.concat([Buffer.from("p"), int32(body.length + 4), body]);
    }

    function buildQueryMessage(sql) {
      const body = Buffer.concat([Buffer.from(sql, "utf8"), Buffer.from([0])]);
      return Buffer.concat([Buffer.from("Q"), int32(body.length + 4), body]);
    }

    function buildTerminateMessage() {
      return Buffer.concat([Buffer.from("X"), int32(4)]);
    }

    function parsePgError(body) {
      return body.toString("utf8").replace(/\u0000/g, " ").trim();
    }

    async function connectPostgres(config) {
      const socket = await new Promise((resolve, reject) => {
        const candidate = net.createConnection(
          {
            host: config.host,
            port: 5432
          },
          () => resolve(candidate)
        );
        candidate.on("error", reject);
      });

      const reader = createReader(socket);
      socket.write(buildStartupMessage(config.user, config.database));

      while (true) {
        const message = await reader.read();

        if (message.type === "R") {
          const authType = message.body.readInt32BE(0);
          if (authType === 0) {
            continue;
          }
          if (authType === 3 || authType === 5) {
            const salt = authType === 5 ? message.body.subarray(4, 8) : Buffer.alloc(0);
            socket.write(buildPasswordMessage(config.user, config.password, authType, salt));
            continue;
          }
          throw new Error("unsupported-postgres-auth-" + authType);
        }

        if (message.type === "E") {
          throw new Error(parsePgError(message.body));
        }

        if (message.type === "Z") {
          return { socket, reader };
        }
      }
    }

    async function runQuery(client, sql) {
      client.socket.write(buildQueryMessage(sql));

      while (true) {
        const message = await client.reader.read();

        if (message.type === "E") {
          throw new Error(parsePgError(message.body));
        }

        if (message.type === "Z") {
          return;
        }
      }
    }

    async function closePostgres(client) {
      client.socket.end(buildTerminateMessage());
    }

    function createRedisCommand(parts) {
      const chunks = ["*" + parts.length + "\r\n"];
      for (const part of parts) {
        const text = String(part);
        chunks.push("$" + Buffer.byteLength(text, "utf8") + "\r\n" + text + "\r\n");
      }
      return Buffer.from(chunks.join(""), "utf8");
    }

    async function setRedisKey(host, key, value) {
      const socket = await new Promise((resolve, reject) => {
        const candidate = tls.connect(
          {
            host,
            port: 6379,
            rejectUnauthorized: false
          },
          () => resolve(candidate)
        );
        candidate.on("error", reject);
      });

      socket.write(createRedisCommand(["SET", key, value]));
      const writeReply = await new Promise((resolve, reject) => {
        let buffer = "";

        socket.on("data", (chunk) => {
          buffer += chunk.toString("utf8");
          if (buffer.includes("\r\n")) {
            resolve(buffer);
          }
        });
        socket.on("error", reject);
      });

      socket.end();

      if (writeReply.startsWith("-")) {
        throw new Error("redis-command-failed");
      }
    }

    async function loadCredentials() {
      const secretValue = await secretsManager.send(
        new GetSecretValueCommand({
          SecretId: process.env.RDS_SECRET_ARN
        })
      );

      return JSON.parse(secretValue.SecretString);
    }

    exports.handler = async (event) => {
      const credentials = await loadCredentials();
      const records = Array.isArray(event && event.Records) ? event.Records : [];

      for (const record of records) {
        let message;

        try {
          message = JSON.parse(record.body);
        } catch (error) {
          console.error("invalid-record-body", error);
          continue;
        }

        if (!message || !message.orderId) {
          console.error("missing-order-id");
          continue;
        }

        const client = await connectPostgres({
          host: process.env.RDS_ENDPOINT,
          database: process.env.RDS_DATABASE,
          user: credentials.username,
          password: credentials.password
        });

        try {
          await runQuery(client, "create table if not exists orders (order_id text primary key, payload jsonb not null)");
          const payload = JSON.stringify(message).replace(/'/g, "''");
          const sql = "insert into orders(order_id, payload) values ('" + message.orderId.replace(/'/g, "''") + "', '" + payload + "'::jsonb) on conflict (order_id) do nothing";
          await runQuery(client, sql);
        } finally {
          await closePostgres(client);
        }

        try {
          await setRedisKey(process.env.REDIS_ENDPOINT, "order:" + message.orderId, "processed");
        } catch (error) {
          console.error("redis-write-failed", error);
        }
      }
    };
  JS

  enrichment_source = <<-JS
    exports.handler = async (event) => ({
      ...(event || {}),
      enriched: true,
      timestamp: new Date().toISOString()
    });
  JS
}

resource "aws_vpc" "main" {
  cidr_block           = local.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${local.app_name}-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  availability_zone       = local.az_a
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.app_name}-public-a"
    Tier = "public"
  }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.main.id
  availability_zone       = local.az_b
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.app_name}-public-b"
    Tier = "public"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  availability_zone = local.az_a
  cidr_block        = "10.0.10.0/24"

  tags = {
    Name = "${local.app_name}-private-a"
    Tier = "private"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  availability_zone = local.az_b
  cidr_block        = "10.0.11.0/24"

  tags = {
    Name = "${local.app_name}-private-b"
    Tier = "private"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.app_name}-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.app_name}-public-rt"
  }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
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
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.app_name}-private-rt"
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

resource "aws_security_group" "lambda" {
  name        = "${local.app_name}-lambda-sg"
  description = "Lambda and interface endpoint security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS from Lambda security group"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [local.vpc_cidr]
  }

  tags = {
    Name = "${local.app_name}-lambda-sg"
  }
}

resource "aws_security_group" "rds" {
  name        = "${local.app_name}-rds-sg"
  description = "RDS security group"
  vpc_id      = aws_vpc.main.id

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
    cidr_blocks = [local.vpc_cidr]
  }

  tags = {
    Name = "${local.app_name}-rds-sg"
  }
}

resource "aws_security_group" "redis" {
  name        = "${local.app_name}-redis-sg"
  description = "Redis security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [local.vpc_cidr]
  }

  tags = {
    Name = "${local.app_name}-redis-sg"
  }
}

resource "aws_vpc_endpoint" "interface" {
  for_each = toset(local.endpoint_services)

  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.${each.value}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = local.private_subnet_ids
  security_group_ids  = [aws_security_group.lambda.id]

  tags = {
    Name = "${local.app_name}-${each.value}-endpoint"
  }
}

resource "aws_sqs_queue" "orders" {
  name                       = local.order_queue_name
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/${local.app_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "lambda_api" {
  name              = "/aws/lambda/${local.api_handler_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "lambda_worker" {
  name              = "/aws/lambda/${local.worker_processor_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "lambda_enrichment" {
  name              = "/aws/lambda/${local.enrichment_lambda_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "state_machine" {
  name              = "/aws/vendedlogs/states/${local.app_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws/glue/crawlers/${local.app_name}"
  retention_in_days = 14
}

resource "aws_secretsmanager_secret" "rds" {
  name = "${local.app_name}-rds-credentials"
}

resource "aws_secretsmanager_secret_version" "rds" {
  secret_id = aws_secretsmanager_secret.rds.id
  secret_string = jsonencode({
    username = local.rds_username
    password = data.aws_secretsmanager_random_password.rds.random_password
  })
}

resource "aws_secretsmanager_secret" "redshift" {
  name = "${local.app_name}-redshift-credentials"
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    username = local.redshift_username
    password = data.aws_secretsmanager_random_password.redshift.random_password
  })
}

resource "aws_db_subnet_group" "postgres" {
  count = local.local_mode ? 0 : 1

  name       = "${local.app_name}-db-subnets"
  subnet_ids = local.private_subnet_ids
}

resource "aws_db_instance" "postgres" {
  count = local.local_mode ? 0 : 1

  identifier                      = "${local.app_name}-postgres"
  engine                          = "postgres"
  engine_version                  = "15.4"
  instance_class                  = "db.t3.micro"
  allocated_storage               = 20
  storage_type                    = "gp3"
  db_name                         = "orders"
  username                        = local.rds_secret_payload.username
  password                        = local.rds_secret_payload.password
  db_subnet_group_name            = aws_db_subnet_group.postgres[0].name
  vpc_security_group_ids          = [aws_security_group.rds.id]
  publicly_accessible             = false
  backup_retention_period         = 1
  multi_az                        = false
  deletion_protection             = false
  skip_final_snapshot             = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
  apply_immediately               = true
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-rds-cpu"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 80
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres[0].id
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-rds-storage"
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  comparison_operator = "LessThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 2147483648
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres[0].id
  }
}

resource "aws_elasticache_subnet_group" "redis" {
  count = local.local_mode ? 0 : 1

  name       = "${local.app_name}-redis-subnets"
  subnet_ids = local.private_subnet_ids
}

resource "aws_elasticache_replication_group" "redis" {
  count = local.local_mode ? 0 : 1

  replication_group_id       = "${local.app_name}-redis"
  description                = "Redis replication group for order processing"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = "cache.t3.micro"
  num_cache_clusters         = 1
  automatic_failover_enabled = false
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  subnet_group_name          = aws_elasticache_subnet_group.redis[0].name
  security_group_ids         = [aws_security_group.redis.id]
  port                       = 6379
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-redis-cpu"
  namespace           = "AWS/ElastiCache"
  metric_name         = "CPUUtilization"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 80
  treat_missing_data  = "notBreaching"

  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis[0].id
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_memory" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-redis-memory"
  namespace           = "AWS/ElastiCache"
  metric_name         = "FreeableMemory"
  comparison_operator = "LessThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 52428800
  treat_missing_data  = "notBreaching"

  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis[0].id
  }
}

resource "aws_redshift_subnet_group" "analytics" {
  count = local.local_mode ? 0 : 1

  name       = "${local.app_name}-redshift-subnets"
  subnet_ids = local.private_subnet_ids
}

resource "aws_redshift_cluster" "analytics" {
  count = local.local_mode ? 0 : 1

  cluster_identifier        = "${local.app_name}-redshift"
  database_name             = "appanalytics"
  master_username           = local.redshift_secret_payload.username
  master_password           = local.redshift_secret_payload.password
  node_type                 = "dc2.large"
  cluster_type              = "single-node"
  encrypted                 = true
  publicly_accessible       = false
  skip_final_snapshot       = true
  cluster_subnet_group_name = aws_redshift_subnet_group.analytics[0].name
}

resource "aws_cloudwatch_metric_alarm" "redshift_cpu" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-redshift-cpu"
  namespace           = "AWS/Redshift"
  metric_name         = "CPUUtilization"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 80
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterIdentifier = aws_redshift_cluster.analytics[0].id
  }
}

resource "aws_cloudwatch_metric_alarm" "redshift_health" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-redshift-health"
  namespace           = "AWS/Redshift"
  metric_name         = "HealthStatus"
  comparison_operator = "LessThanThreshold"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterIdentifier = aws_redshift_cluster.analytics[0].id
  }
}

resource "aws_s3_bucket" "frontend" {
  bucket        = local.bucket_id
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  bucket                  = aws_s3_bucket.frontend.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_apigatewayv2_api" "orders" {
  count = local.local_mode ? 0 : 1

  name          = "${local.app_name}-http-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "orders" {
  count                  = local.local_mode ? 0 : 1
  api_id                 = aws_apigatewayv2_api.orders[0].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api_handler.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "orders_post" {
  count     = local.local_mode ? 0 : 1
  api_id    = aws_apigatewayv2_api.orders[0].id
  route_key = "POST /api/orders"
  target    = "integrations/${aws_apigatewayv2_integration.orders[0].id}"
}

resource "aws_apigatewayv2_stage" "default" {
  count       = local.local_mode ? 0 : 1
  api_id      = aws_apigatewayv2_api.orders[0].id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId               = "$context.requestId"
      routeKey                = "$context.routeKey"
      status                  = "$context.status"
      integrationErrorMessage = "$context.integrationErrorMessage"
      responseLatency         = "$context.responseLatency"
    })
  }
}

resource "aws_lambda_permission" "api_gateway" {
  count         = local.local_mode ? 0 : 1
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_handler.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.orders[0].execution_arn}/*/*"
}

resource "aws_cloudfront_origin_access_control" "frontend" {
  count = local.local_mode ? 0 : 1

  name                              = "${local.app_name}-oac"
  description                       = "Origin access control for frontend bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "frontend" {
  count               = local.local_mode ? 0 : 1
  enabled             = true
  default_root_object = "index.html"

  origin {
    domain_name              = aws_s3_bucket.frontend.bucket_regional_domain_name
    origin_id                = "frontend-s3-origin"
    origin_access_control_id = aws_cloudfront_origin_access_control.frontend[0].id

    s3_origin_config {
      origin_access_identity = ""
    }
  }

  origin {
    domain_name = trimprefix(aws_apigatewayv2_api.orders[0].api_endpoint, "https://")
    origin_id   = "http-api-origin"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "frontend-s3-origin"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = "658327ea-f89d-4fab-a63d-7e88639e58f6"
    compress               = true
  }

  ordered_cache_behavior {
    path_pattern           = "api/*"
    target_origin_id       = "http-api-origin"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD", "OPTIONS"]
    compress               = true
    min_ttl                = 0
    default_ttl            = 0
    max_ttl                = 0

    forwarded_values {
      query_string = true
      headers      = ["Accept", "Authorization", "Content-Type", "Origin"]

      cookies {
        forward = "all"
      }
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_s3_bucket_policy" "frontend" {
  count  = local.local_mode ? 0 : 1
  bucket = aws_s3_bucket.frontend.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCloudFrontServicePrincipalReadOnly"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.frontend.arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.frontend[0].arn
          }
        }
      }
    ]
  })
}

resource "aws_s3_object" "index_html" {
  bucket       = aws_s3_bucket.frontend.id
  key          = "index.html"
  content      = local.frontend_index_html
  content_type = "text/html"
}

resource "aws_s3_object" "app_js" {
  bucket       = aws_s3_bucket.frontend.id
  key          = "app.js"
  content      = local.frontend_app_js
  content_type = "application/javascript"
}

resource "aws_cloudwatch_metric_alarm" "cloudfront_5xx" {
  count = local.local_mode ? 0 : 1

  alarm_name          = "${local.app_name}-cloudfront-5xx"
  namespace           = "AWS/CloudFront"
  metric_name         = "5xxErrorRate"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    DistributionId = aws_cloudfront_distribution.frontend[0].id
    Region         = "Global"
  }
}

resource "aws_iam_role" "api_handler" {
  name = "${local.app_name}-api-role"
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

resource "aws_iam_role_policy" "api_handler" {
  name = "${local.app_name}-api-policy"
  role = aws_iam_role.api_handler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.orders.arn]
      },
      {
        Effect   = "Allow"
        Action   = local.lambda_log_permissions
        Resource = ["${aws_cloudwatch_log_group.lambda_api.arn}:*"]
      },
      {
        Effect   = "Allow"
        Action   = local.lambda_vpc_permissions
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role" "worker_processor" {
  name = "${local.app_name}-worker-role"
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

resource "aws_iam_role_policy" "worker_processor" {
  name = "${local.app_name}-worker-policy"
  role = aws_iam_role.worker_processor.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = [aws_sqs_queue.orders.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.rds.arn]
      },
      {
        Effect   = "Allow"
        Action   = local.lambda_log_permissions
        Resource = ["${aws_cloudwatch_log_group.lambda_worker.arn}:*"]
      },
      {
        Effect   = "Allow"
        Action   = local.lambda_vpc_permissions
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role" "enrichment" {
  name = "${local.app_name}-enrichment-role"
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

resource "aws_iam_role_policy" "enrichment" {
  name = "${local.app_name}-enrichment-policy"
  role = aws_iam_role.enrichment.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = local.lambda_log_permissions
        Resource = ["${aws_cloudwatch_log_group.lambda_enrichment.arn}:*"]
      },
      {
        Effect   = "Allow"
        Action   = local.lambda_vpc_permissions
        Resource = ["*"]
      }
    ]
  })
}

data "archive_file" "api_handler" {
  type                    = "zip"
  output_path             = "${path.module}/.terraform/api-handler.zip"
  source_content_filename = "index.js"
  source_content          = local.api_handler_source
}

resource "aws_lambda_function" "api_handler" {
  function_name    = local.api_handler_name
  runtime          = "nodejs20.x"
  handler          = "index.handler"
  filename         = data.archive_file.api_handler.output_path
  source_code_hash = data.archive_file.api_handler.output_base64sha256
  role             = aws_iam_role.api_handler.arn
  memory_size      = 256
  timeout          = 10

  vpc_config {
    security_group_ids = [aws_security_group.lambda.id]
    subnet_ids         = local.private_subnet_ids
  }

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.orders.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda_api]
}

data "archive_file" "worker_processor" {
  type                    = "zip"
  output_path             = "${path.module}/.terraform/worker-processor.zip"
  source_content_filename = "index.js"
  source_content          = local.worker_processor_source
}

resource "aws_lambda_function" "worker_processor" {
  function_name    = local.worker_processor_name
  runtime          = "nodejs20.x"
  handler          = "index.handler"
  filename         = data.archive_file.worker_processor.output_path
  source_code_hash = data.archive_file.worker_processor.output_base64sha256
  role             = aws_iam_role.worker_processor.arn
  memory_size      = 256
  timeout          = 20

  vpc_config {
    security_group_ids = [aws_security_group.lambda.id]
    subnet_ids         = local.private_subnet_ids
  }

  environment {
    variables = {
      RDS_SECRET_ARN = aws_secretsmanager_secret.rds.arn
      RDS_ENDPOINT   = local.local_mode ? "endpoint-disabled" : aws_db_instance.postgres[0].address
      RDS_DATABASE   = "orders"
      REDIS_ENDPOINT = local.local_mode ? "endpoint-disabled" : aws_elasticache_replication_group.redis[0].primary_endpoint_address
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda_worker]
}

resource "aws_lambda_event_source_mapping" "worker_orders" {
  event_source_arn = aws_sqs_queue.orders.arn
  function_name    = aws_lambda_function.worker_processor.arn
  batch_size       = 5
}

data "archive_file" "enrichment" {
  type                    = "zip"
  output_path             = "${path.module}/.terraform/enrichment.zip"
  source_content_filename = "index.js"
  source_content          = local.enrichment_source
}

resource "aws_lambda_function" "enrichment" {
  function_name    = local.enrichment_lambda_name
  runtime          = "nodejs20.x"
  handler          = "index.handler"
  filename         = data.archive_file.enrichment.output_path
  source_code_hash = data.archive_file.enrichment.output_base64sha256
  role             = aws_iam_role.enrichment.arn
  memory_size      = 256
  timeout          = 10

  vpc_config {
    security_group_ids = [aws_security_group.lambda.id]
    subnet_ids         = local.private_subnet_ids
  }

  depends_on = [aws_cloudwatch_log_group.lambda_enrichment]
}

resource "aws_cloudwatch_metric_alarm" "api_errors" {
  alarm_name          = "${local.app_name}-api-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.api_handler.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "api_duration" {
  alarm_name          = "${local.app_name}-api-duration"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  comparison_operator = "GreaterThanThreshold"
  extended_statistic  = "p95"
  period              = 300
  evaluation_periods  = 1
  threshold           = 2000
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.api_handler.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "worker_errors" {
  alarm_name          = "${local.app_name}-worker-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.worker_processor.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "worker_duration" {
  alarm_name          = "${local.app_name}-worker-duration"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  comparison_operator = "GreaterThanThreshold"
  extended_statistic  = "p95"
  period              = 300
  evaluation_periods  = 1
  threshold           = 5000
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.worker_processor.function_name
  }
}

resource "aws_iam_role" "step_functions" {
  name = "${local.app_name}-sfn-role"
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
  name = "${local.app_name}-sfn-policy"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.enrichment.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.orders.arn]
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
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.state_machine.arn}:*"]
      }
    ]
  })
}

resource "aws_sfn_state_machine" "orders" {
  name     = "${local.app_name}-workflow"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Order enrichment workflow"
    StartAt = "InvokeEnrichment"
    States = {
      InvokeEnrichment = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.enrichment.arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Next       = "SendToQueue"
      }
      SendToQueue = {
        Type     = "Task"
        Resource = "arn:aws:states:::sqs:sendMessage"
        Parameters = {
          QueueUrl = aws_sqs_queue.orders.url
          MessageBody = {
            "orderId.$"   = "$.orderId"
            "enriched.$"  = "$.enriched"
            "timestamp.$" = "$.timestamp"
          }
        }
        End = true
      }
    }
  })

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
  }
}

resource "aws_cloudwatch_metric_alarm" "sfn_failed" {
  alarm_name          = "${local.app_name}-sfn-failed"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  comparison_operator = "GreaterThanThreshold"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.orders.arn
  }
}

resource "aws_iam_role" "pipes" {
  name = "${local.app_name}-pipes-role"
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
  name = "${local.app_name}-pipes-policy"
  role = aws_iam_role.pipes.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = [aws_sqs_queue.orders.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.enrichment.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.orders.arn]
      }
    ]
  })
}

resource "aws_pipes_pipe" "orders" {
  count    = local.local_mode ? 0 : 1
  name     = "${local.app_name}-pipe"
  role_arn = aws_iam_role.pipes.arn
  source   = aws_sqs_queue.orders.arn
  target   = aws_sfn_state_machine.orders.arn

  source_parameters {
    sqs_queue_parameters {
      batch_size = 1
    }
  }

  enrichment = aws_lambda_function.enrichment.arn

  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }
}

resource "aws_iam_role" "glue" {
  name = "${local.app_name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_secret" {
  name = "${local.app_name}-glue-secret"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.redshift.arn]
      }
    ]
  })
}

resource "aws_glue_catalog_database" "analytics" {
  count = local.local_mode ? 0 : 1

  name = "analytics_catalog"
}

resource "aws_glue_connection" "redshift" {
  count           = local.local_mode ? 0 : 1
  name            = "${local.app_name}-redshift-jdbc"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshift_cluster.analytics[0].endpoint}/appanalytics"
    SECRET_ID           = aws_secretsmanager_secret.redshift.arn
  }

  physical_connection_requirements {
    availability_zone      = local.az_a
    subnet_id              = aws_subnet.private_a.id
    security_group_id_list = [aws_security_group.lambda.id]
  }
}

resource "aws_glue_crawler" "analytics" {
  count         = local.local_mode ? 0 : 1
  name          = "${local.app_name}-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.analytics[0].name

  jdbc_target {
    connection_name = aws_glue_connection.redshift[0].name
    path            = "appanalytics/%"
  }

  depends_on = [aws_cloudwatch_log_group.glue]
}

output "cloudfront_distribution_domain_name" {
  value = local.local_mode ? "endpoint-disabled" : aws_cloudfront_distribution.frontend[0].domain_name
}

output "http_api_endpoint_url" {
  value = local.local_mode ? "endpoint-disabled" : aws_apigatewayv2_api.orders[0].api_endpoint
}

output "rds_endpoint_address" {
  value = local.local_mode ? "endpoint-disabled" : aws_db_instance.postgres[0].address
}

output "redshift_endpoint_address" {
  value = local.local_mode ? "endpoint-disabled" : aws_redshift_cluster.analytics[0].endpoint
}

output "sqs_queue_url" {
  value = aws_sqs_queue.orders.url
}
