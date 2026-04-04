#!/usr/bin/env node
import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as cdk from 'aws-cdk-lib';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

const DATABASE_NAME = 'appdb';
const SERVICE_NAME = 'rapid-prototype-poc';

function writeLambdaAsset(name: string, source: string): string {
  const contentHash = crypto.createHash('sha256').update(source).digest('hex').slice(0, 16);
  const assetDir = path.join(os.tmpdir(), 'single-file-cdk-assets', `${name}-${contentHash}`);
  fs.mkdirSync(assetDir, { recursive: true });
  fs.writeFileSync(path.join(assetDir, 'index.js'), source, 'utf8');
  return assetDir;
}

function logWriteResources(logGroup: logs.ILogGroup): string[] {
  return [logGroup.logGroupArn, `${logGroup.logGroupArn}:*`];
}

function addLogWritePolicy(role: iam.Role, logGroup: logs.ILogGroup): void {
  role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: logWriteResources(logGroup),
    }),
  );
}

function addVpcAccessPolicies(
  scope: cdk.Stack,
  role: iam.Role,
  vpc: ec2.IVpc,
  securityGroup: ec2.ISecurityGroup,
): void {
  const subnetArns = vpc.privateSubnets.map((subnet) =>
    scope.formatArn({
      service: 'ec2',
      resource: 'subnet',
      resourceName: subnet.subnetId,
    }),
  );

  const securityGroupArn = scope.formatArn({
    service: 'ec2',
    resource: 'security-group',
    resourceName: securityGroup.securityGroupId,
  });

  role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['ec2:CreateNetworkInterface'],
      resources: ['*'],
      conditions: {
        'ForAnyValue:StringEquals': {
          'ec2:Subnet': subnetArns,
          'ec2:SecurityGroup': [securityGroupArn],
        },
      },
    }),
  );

  role.addToPolicy(
    new iam.PolicyStatement({
      actions: [
        'ec2:AssignPrivateIpAddresses',
        'ec2:DeleteNetworkInterface',
        'ec2:UnassignPrivateIpAddresses',
      ],
      resources: [
        scope.formatArn({
          service: 'ec2',
          resource: 'network-interface',
          resourceName: '*',
        }),
      ],
    }),
  );

  role.addToPolicy(
    new iam.PolicyStatement({
      actions: [
        'ec2:DescribeNetworkInterfaces',
        'ec2:DescribeSecurityGroups',
        'ec2:DescribeSubnets',
        'ec2:DescribeVpcs',
      ],
      resources: ['*'],
    }),
  );
}

const postgresClientSource = String.raw`
const crypto = require('node:crypto');
const net = require('node:net');
const tls = require('node:tls');

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cString(value) {
  return Buffer.from(String(value) + '\0', 'utf8');
}

function readCString(buffer, offset) {
  const end = buffer.indexOf(0, offset);
  const nextOffset = end === -1 ? buffer.length : end + 1;
  return {
    value: buffer.toString('utf8', offset, end === -1 ? buffer.length : end),
    nextOffset,
  };
}

function parseError(payload) {
  const pieces = [];
  let offset = 0;

  while (offset < payload.length && payload[offset] !== 0) {
    offset += 1;
    const parsed = readCString(payload, offset);
    pieces.push(parsed.value);
    offset = parsed.nextOffset;
  }

  return pieces.join(' | ') || 'PostgreSQL error';
}

function parseRowDescription(payload) {
  const count = payload.readInt16BE(0);
  const columns = [];
  let offset = 2;

  for (let index = 0; index < count; index += 1) {
    const parsed = readCString(payload, offset);
    columns.push(parsed.value);
    offset = parsed.nextOffset + 18;
  }

  return columns;
}

function parseDataRow(payload, columns) {
  const count = payload.readInt16BE(0);
  const row = {};
  let offset = 2;

  for (let index = 0; index < count; index += 1) {
    const valueLength = payload.readInt32BE(offset);
    offset += 4;

    if (valueLength === -1) {
      row[columns[index]] = null;
      continue;
    }

    row[columns[index]] = payload.toString('utf8', offset, offset + valueLength);
    offset += valueLength;
  }

  return row;
}

function sqlLiteral(value) {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  return '\'' + String(value).replace(/'/g, '\'\'') + '\'';
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest();
}

function hmac(key, message) {
  return crypto.createHmac('sha256', key).update(message).digest();
}

function xorBuffers(left, right) {
  const output = Buffer.alloc(left.length);

  for (let index = 0; index < left.length; index += 1) {
    output[index] = left[index] ^ right[index];
  }

  return output;
}

function toSaslName(value) {
  return String(value).replace(/=/g, '=3D').replace(/,/g, '=2C');
}

function parseScramFields(message) {
  return Object.fromEntries(
    message.split(',').map((entry) => {
      const separator = entry.indexOf('=');
      return [entry.slice(0, separator), entry.slice(separator + 1)];
    }),
  );
}

function buildMessage(type, payload) {
  const header = Buffer.alloc(5);
  header.write(type, 0, 1, 'utf8');
  header.writeInt32BE(payload.length + 4, 1);
  return Buffer.concat([header, payload]);
}

function buildStartupMessage(parameters) {
  const pairs = Object.entries(parameters).flatMap(([key, value]) => [cString(key), cString(value)]);
  const payload = Buffer.concat([
    Buffer.from([0x00, 0x03, 0x00, 0x00]),
    ...pairs,
    Buffer.from([0x00]),
  ]);

  const header = Buffer.alloc(4);
  header.writeInt32BE(payload.length + 4, 0);
  return Buffer.concat([header, payload]);
}

function buildSaslInitialResponse(mechanism, initialResponse) {
  const mechanismBuffer = cString(mechanism);
  const initialBuffer = Buffer.from(initialResponse, 'utf8');
  const lengthBuffer = Buffer.alloc(4);
  lengthBuffer.writeInt32BE(initialBuffer.length, 0);
  return buildMessage('p', Buffer.concat([mechanismBuffer, lengthBuffer, initialBuffer]));
}

function buildSaslResponse(message) {
  return buildMessage('p', Buffer.from(message, 'utf8'));
}

function buildQueryMessage(sql) {
  return buildMessage('Q', cString(sql));
}

function buildTerminateMessage() {
  const payload = Buffer.alloc(0);
  return buildMessage('X', payload);
}

function buildMd5Password(password, user, salt) {
  const first = crypto.createHash('md5').update(password + user, 'utf8').digest('hex');
  const second = crypto.createHash('md5').update(Buffer.concat([Buffer.from(first, 'utf8'), salt])).digest('hex');
  return 'md5' + second;
}

function readExact(socket, expectedLength) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let received = 0;

    function cleanup() {
      socket.off('data', onData);
      socket.off('error', onError);
      socket.off('close', onClose);
    }

    function onError(error) {
      cleanup();
      reject(error);
    }

    function onClose() {
      cleanup();
      reject(new Error('Socket closed before the expected bytes were received'));
    }

    function onData(chunk) {
      chunks.push(chunk);
      received += chunk.length;

      if (received < expectedLength) {
        return;
      }

      cleanup();
      resolve(Buffer.concat(chunks).subarray(0, expectedLength));
    }

    socket.on('data', onData);
    socket.on('error', onError);
    socket.on('close', onClose);
  });
}

class MessageReader {
  constructor(socket) {
    this.buffer = Buffer.alloc(0);
    this.socket = socket;
    this.waiters = [];
    this.pendingError = null;

    socket.on('data', (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.drain();
    });

    socket.on('error', (error) => {
      this.pendingError = error;
      this.drain();
    });

    socket.on('close', () => {
      if (!this.pendingError) {
        this.pendingError = new Error('Socket closed');
      }
      this.drain();
    });
  }

  drain() {
    while (this.waiters.length > 0) {
      if (this.pendingError) {
        const waiter = this.waiters.shift();
        waiter.reject(this.pendingError);
        continue;
      }

      const message = this.extract();
      if (!message) {
        return;
      }

      const waiter = this.waiters.shift();
      waiter.resolve(message);
    }
  }

  extract() {
    if (this.buffer.length < 5) {
      return undefined;
    }

    const messageLength = this.buffer.readInt32BE(1);
    const totalLength = messageLength + 1;
    if (this.buffer.length < totalLength) {
      return undefined;
    }

    const type = this.buffer.toString('utf8', 0, 1);
    const payload = this.buffer.subarray(5, totalLength);
    this.buffer = this.buffer.subarray(totalLength);
    return { type, payload };
  }

  nextMessage() {
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
      this.drain();
    });
  }
}

class PgClient {
  constructor(config) {
    this.config = config;
    this.socket = undefined;
    this.reader = undefined;
  }

  async connect() {
    const rawSocket = await new Promise((resolve, reject) => {
      const socket = net.createConnection({
        host: this.config.host,
        port: this.config.port,
      });

      socket.once('connect', () => resolve(socket));
      socket.once('error', reject);
    });

    const sslRequest = Buffer.alloc(8);
    sslRequest.writeInt32BE(8, 0);
    sslRequest.writeInt32BE(80877103, 4);
    rawSocket.write(sslRequest);

    const sslResponse = (await readExact(rawSocket, 1)).toString('utf8');

    let socket = rawSocket;
    if (sslResponse === 'S') {
      socket = await new Promise((resolve, reject) => {
        const secureSocket = tls.connect({
          socket: rawSocket,
          rejectUnauthorized: false,
          servername: this.config.host,
        });

        secureSocket.once('secureConnect', () => resolve(secureSocket));
        secureSocket.once('error', reject);
      });
    } else if (sslResponse !== 'N') {
      throw new Error('Unexpected SSL negotiation response: ' + sslResponse);
    }

    this.socket = socket;
    this.reader = new MessageReader(socket);

    socket.write(
      buildStartupMessage({
        user: this.config.user,
        database: this.config.database,
        client_encoding: 'UTF8',
      }),
    );

    await this.authenticate();
  }

  async authenticate() {
    let scramContext = undefined;

    while (true) {
      const message = await this.reader.nextMessage();

      if (message.type === 'R') {
        const authType = message.payload.readInt32BE(0);

        if (authType === 0) {
          continue;
        }

        if (authType === 5) {
          const salt = message.payload.subarray(4, 8);
          this.socket.write(buildMessage('p', cString(buildMd5Password(this.config.password, this.config.user, salt))));
          continue;
        }

        if (authType === 10) {
          const mechanism = 'SCRAM-SHA-256';
          const nonce = crypto.randomBytes(18).toString('base64');
          const clientFirstBare = 'n=' + toSaslName(this.config.user) + ',r=' + nonce;
          const clientFirstMessage = 'n,,' + clientFirstBare;
          scramContext = { clientFirstBare };
          this.socket.write(buildSaslInitialResponse(mechanism, clientFirstMessage));
          continue;
        }

        if (authType === 11) {
          if (!scramContext) {
            throw new Error('Received SCRAM continuation without SCRAM context');
          }

          const serverFirst = message.payload.toString('utf8');
          const fields = parseScramFields(serverFirst);
          const channelBinding = 'c=biws';
          const clientFinalWithoutProof = channelBinding + ',r=' + fields.r;
          const salt = Buffer.from(fields.s, 'base64');
          const iterations = Number(fields.i);
          const saltedPassword = crypto.pbkdf2Sync(this.config.password, salt, iterations, 32, 'sha256');
          const clientKey = hmac(saltedPassword, 'Client Key');
          const storedKey = sha256(clientKey);
          const authMessage = [
            scramContext.clientFirstBare,
            serverFirst,
            clientFinalWithoutProof,
          ].join(',');
          const clientSignature = hmac(storedKey, authMessage);
          const clientProof = xorBuffers(clientKey, clientSignature).toString('base64');
          const serverKey = hmac(saltedPassword, 'Server Key');

          scramContext = {
            ...scramContext,
            expectedServerSignature: hmac(serverKey, authMessage).toString('base64'),
          };

          this.socket.write(
            buildSaslResponse(clientFinalWithoutProof + ',p=' + clientProof),
          );
          continue;
        }

        if (authType === 12) {
          if (!scramContext) {
            throw new Error('Received SCRAM final message without SCRAM context');
          }

          const serverFinal = parseScramFields(message.payload.toString('utf8'));
          if (serverFinal.v !== scramContext.expectedServerSignature) {
            throw new Error('SCRAM server signature validation failed');
          }
          continue;
        }

        throw new Error('Unsupported PostgreSQL authentication type: ' + authType);
      }

      if (message.type === 'S' || message.type === 'K' || message.type === 'N') {
        continue;
      }

      if (message.type === 'E') {
        throw new Error(parseError(message.payload));
      }

      if (message.type === 'Z') {
        return;
      }
    }
  }

  async query(sql) {
    this.socket.write(buildQueryMessage(sql));
    const rows = [];
    let columns = [];
    let commandTag = '';

    while (true) {
      const message = await this.reader.nextMessage();

      if (message.type === 'T') {
        columns = parseRowDescription(message.payload);
        continue;
      }

      if (message.type === 'D') {
        rows.push(parseDataRow(message.payload, columns));
        continue;
      }

      if (message.type === 'C') {
        commandTag = readCString(message.payload, 0).value;
        continue;
      }

      if (message.type === 'S' || message.type === 'N' || message.type === 'K') {
        continue;
      }

      if (message.type === 'E') {
        throw new Error(parseError(message.payload));
      }

      if (message.type === 'Z') {
        return { rows, commandTag };
      }
    }
  }

  async close() {
    if (!this.socket) {
      return;
    }

    try {
      this.socket.write(buildTerminateMessage());
    } catch (_error) {
      // Best effort on shutdown.
    }

    await new Promise((resolve) => {
      this.socket.once('close', resolve);
      this.socket.end();
      setTimeout(resolve, 250);
    });
  }
}

async function loadDatabaseConfig() {
  const host = process.env.DB_HOST;
  if (!host || host === 'unknown') {
    return null;
  }

  const { GetSecretValueCommand, SecretsManagerClient } = require('@aws-sdk/client-secrets-manager');
  const client = new SecretsManagerClient({
    region: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
  });
  const response = await client.send(
    new GetSecretValueCommand({
      SecretId: process.env.DB_SECRET_ARN,
    }),
  );

  const secretString = response.SecretString || '{}';
  const secret = JSON.parse(secretString);

  return {
    host,
    port: 5432,
    database: process.env.DB_NAME,
    user: secret.username,
    password: secret.password,
  };
}

function isDatabaseUnavailableError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return [
    'ENOTFOUND',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'EAI_AGAIN',
    'Socket closed',
  ].some((marker) => message.includes(marker));
}

async function withDatabaseConnection(run, options) {
  const config = await loadDatabaseConfig();
  if (!config) {
    if (options && options.allowUnavailable) {
      return undefined;
    }
    throw new Error('Database endpoint is not available');
  }

  const client = new PgClient(config);
  try {
    await client.connect();
  } catch (error) {
    if (options && options.allowUnavailable && isDatabaseUnavailableError(error)) {
      console.warn('Database connection unavailable:', error instanceof Error ? error.message : String(error));
      return undefined;
    }
    throw error;
  }

  try {
    return await run(client);
  } finally {
    await client.close();
  }
}
`;

const frontendSource = String.raw`
exports.handler = async () => {
  const html = '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Rapid Prototype POC</title><style>body{font-family:system-ui,sans-serif;margin:2rem;background:#f5f7fb;color:#132033}main{max-width:48rem;margin:0 auto;background:#fff;border-radius:16px;padding:2rem;box-shadow:0 18px 50px rgba(19,32,51,.08)}pre{padding:1rem;background:#eef3fb;border-radius:12px;overflow:auto}</style></head><body><main><h1>Rapid Prototype POC</h1><p>This page calls <code>/api/health</code> through the HTTP API.</p><pre id="result">Loading...</pre></main><script>async function load(){const target=document.getElementById("result");try{const response=await fetch("/api/health");const data=await response.json();target.textContent=JSON.stringify(data,null,2);}catch(error){target.textContent=String(error);}}load();</script></body></html>';
  return {
    statusCode: 200,
    headers: {
      'content-type': 'text/html; charset=utf-8',
    },
    body: html,
  };
};
`;

const backendSource = String.raw`
${postgresClientSource}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  };
}

function buildS3Client() {
  const { S3Client } = require('@aws-sdk/client-s3');
  return new S3Client({
    region: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
  });
}

function buildSqsClient() {
  const { SQSClient } = require('@aws-sdk/client-sqs');
  return new SQSClient({
    region: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
  });
}

function getRawBody(event) {
  if (!event.body) {
    return '';
  }

  if (event.isBase64Encoded) {
    return Buffer.from(event.body, 'base64').toString('utf8');
  }

  return event.body;
}

function isHttpEvent(event) {
  return Boolean(event && event.requestContext && event.requestContext.http && event.version === '2.0');
}

function isSqsEvent(event) {
  return Boolean(
    event &&
      Array.isArray(event.Records) &&
      event.Records.length > 0 &&
      event.Records.every((record) => record.eventSource === 'aws:sqs'),
  );
}

function isValidationEvent(event) {
  return Boolean(event && event.mode === 'validate' && typeof event.s3Key === 'string');
}

function isPipeEnrichmentEvent(event) {
  return Boolean(event && !event.requestContext && !event.Records && Object.prototype.hasOwnProperty.call(event, 'body'));
}

function parseQueueMessage(value) {
  if (typeof value === 'string') {
    return JSON.parse(value);
  }

  return value;
}

async function handleHealth() {
  return jsonResponse(200, {
    service: '${SERVICE_NAME}',
    timestamp: new Date().toISOString(),
  });
}

async function handleCreateItem(event) {
  const rawBody = getRawBody(event);
  const id = crypto.randomUUID();
  const s3Key = 'items/' + id + '.json';
  const payloadSha256 = crypto.createHash('sha256').update(rawBody).digest('hex');

  const { PutObjectCommand } = require('@aws-sdk/client-s3');
  const { SendMessageCommand } = require('@aws-sdk/client-sqs');

  await buildS3Client().send(
    new PutObjectCommand({
      Bucket: process.env.ARCHIVE_BUCKET_NAME,
      Key: s3Key,
      Body: rawBody,
      ContentType: 'application/json',
    }),
  );

  await withDatabaseConnection(async (client) => {
    await client.query(
      'INSERT INTO items (id, created_at, s3_key, status, payload_sha256) VALUES (' +
        [
          sqlLiteral(id),
          'CURRENT_TIMESTAMP',
          sqlLiteral(s3Key),
          sqlLiteral('created'),
          sqlLiteral(payloadSha256),
        ].join(', ') +
        ')',
    );
  }, { allowUnavailable: true });

  await buildSqsClient().send(
    new SendMessageCommand({
      QueueUrl: process.env.INGESTION_QUEUE_URL,
      MessageBody: JSON.stringify({
        id,
        s3Key,
        payloadSha256,
      }),
    }),
  );

  return jsonResponse(201, {
    id,
    s3Key,
    status: 'created',
  });
}

async function handleApiEvent(event) {
  const method = event.requestContext.http.method;
  const rawPath = event.rawPath;

  if (method === 'GET' && rawPath === '/api/health') {
    return handleHealth();
  }

  if (method === 'POST' && rawPath === '/api/items') {
    return handleCreateItem(event);
  }

  return jsonResponse(404, { message: 'Not found' });
}

async function handleSqsBatch(event) {
  await withDatabaseConnection(async (client) => {
    for (const record of event.Records) {
      const message = parseQueueMessage(record.body);
      await client.query(
        'UPDATE items SET status = ' +
          sqlLiteral('processing') +
          ' WHERE id = ' +
          sqlLiteral(message.id),
      );
    }
  }, { allowUnavailable: true });

  return {
    batchItemFailures: [],
  };
}

async function handleValidation(event) {
  const { HeadObjectCommand } = require('@aws-sdk/client-s3');
  await buildS3Client().send(
    new HeadObjectCommand({
      Bucket: process.env.ARCHIVE_BUCKET_NAME,
      Key: event.s3Key,
    }),
  );

  const result = await withDatabaseConnection(
    async (client) =>
      client.query(
        'UPDATE items SET status = ' +
          sqlLiteral('validated') +
          ' WHERE s3_key = ' +
          sqlLiteral(event.s3Key),
      ),
    { allowUnavailable: true },
  );

  if (result && !result.commandTag.endsWith(' 1')) {
    throw new Error('Validation update did not match an item for key ' + event.s3Key);
  }

  return {
    validated: true,
    s3Key: event.s3Key,
  };
}

async function handlePipeEnrichment(event) {
  const body = parseQueueMessage(event.body);

  if (!body || typeof body.s3Key !== 'string') {
    throw new Error('Pipe enrichment expected an object containing s3Key');
  }

  return {
    s3Key: body.s3Key,
  };
}

exports.handler = async (event) => {
  if (isHttpEvent(event)) {
    return handleApiEvent(event);
  }

  if (isSqsEvent(event)) {
    return handleSqsBatch(event);
  }

  if (isValidationEvent(event)) {
    return handleValidation(event);
  }

  if (isPipeEnrichmentEvent(event)) {
    return handlePipeEnrichment(event);
  }

  throw new Error('Unsupported event shape');
};
`;

const schemaSource = String.raw`
const https = require('node:https');
${postgresClientSource}

function sendCloudFormationResponse(event, context, status, data, physicalResourceId, reason) {
  return new Promise((resolve, reject) => {
    const responseBody = JSON.stringify({
      Status: status,
      Reason: reason || ('See CloudWatch Logs: ' + context.logStreamName),
      PhysicalResourceId: physicalResourceId || context.logStreamName,
      StackId: event.StackId,
      RequestId: event.RequestId,
      LogicalResourceId: event.LogicalResourceId,
      Data: data,
    });

    const url = new URL(event.ResponseURL);
    const request = https.request(
      {
        method: 'PUT',
        hostname: url.hostname,
        path: url.pathname + url.search,
        headers: {
          'content-length': Buffer.byteLength(responseBody),
        },
      },
      (response) => {
        response.on('data', () => undefined);
        response.on('end', resolve);
      },
    );

    request.on('error', reject);
    request.write(responseBody);
    request.end();
  });
}

async function ensureSchema() {
  const config = await loadDatabaseConfig();
  if (!config) {
    return;
  }

  const sql = [
    'CREATE TABLE IF NOT EXISTS items (',
    'id TEXT PRIMARY KEY,',
    'created_at TIMESTAMP NOT NULL,',
    's3_key TEXT NOT NULL,',
    'status TEXT NOT NULL,',
    'payload_sha256 TEXT NOT NULL',
    ')',
  ].join(' ');

  for (let attempt = 1; attempt <= 8; attempt += 1) {
    try {
      await withDatabaseConnection(async (client) => {
        await client.query(sql);
      }, { allowUnavailable: true });
      return;
    } catch (error) {
      if (isDatabaseUnavailableError(error)) {
        return;
      }
      if (attempt === 8) {
        throw error;
      }
      await delay(5000);
    }
  }
}

exports.handler = async (event, context) => {
  const physicalResourceId = event.PhysicalResourceId || 'DatabaseSchemaInitializer';

  try {
    if (event.RequestType === 'Delete') {
      await sendCloudFormationResponse(event, context, 'SUCCESS', {}, physicalResourceId);
      return;
    }

    await ensureSchema();
    await sendCloudFormationResponse(
      event,
      context,
      'SUCCESS',
      { tableName: 'items' },
      physicalResourceId,
    );
  } catch (error) {
    await sendCloudFormationResponse(
      event,
      context,
      'FAILED',
      {},
      physicalResourceId,
      error instanceof Error ? error.message : String(error),
    );
  }
};
`;

class ThreeTierPocStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 1,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
      ],
    });

    const backendSecurityGroup = new ec2.SecurityGroup(this, 'BackendSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Backend and schema Lambda network controls',
    });

    const databaseSecurityGroup = new ec2.SecurityGroup(this, 'DatabaseSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'PostgreSQL ingress only from backend compute',
    });

    databaseSecurityGroup.addIngressRule(
      backendSecurityGroup,
      ec2.Port.tcp(5432),
      'Allow PostgreSQL only from backend security group',
    );

    backendSecurityGroup.addEgressRule(
      databaseSecurityGroup,
      ec2.Port.tcp(5432),
      'PostgreSQL to the database tier',
    );
    backendSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      'HTTPS to AWS services via NAT and the Secrets Manager endpoint',
    );
    backendSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.udp(53),
      'VPC DNS over UDP',
    );
    backendSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(53),
      'VPC DNS over TCP',
    );

    const defaultVpcSecurityGroup = ec2.SecurityGroup.fromSecurityGroupId(
      this,
      'DefaultVpcSecurityGroup',
      vpc.vpcDefaultSecurityGroup,
    );

    new ec2.CfnSecurityGroupIngress(this, 'SecretsManagerEndpointIngress', {
      groupId: vpc.vpcDefaultSecurityGroup,
      ipProtocol: 'tcp',
      fromPort: 443,
      toPort: 443,
      sourceSecurityGroupId: backendSecurityGroup.securityGroupId,
    });

    const secretsManagerEndpoint = vpc.addInterfaceEndpoint('SecretsManagerEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
      subnets: { subnets: vpc.privateSubnets },
      securityGroups: [defaultVpcSecurityGroup],
      open: false,
      privateDnsEnabled: true,
    });

    const archiveBucket = new s3.Bucket(this, 'ArchiveBucket', {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [
        {
          prefix: 'items/',
          expiration: cdk.Duration.days(90),
          noncurrentVersionExpiration: cdk.Duration.days(30),
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const ingestionQueue = new sqs.Queue(this, 'IngestionQueue', {
      visibilityTimeout: cdk.Duration.seconds(60),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const databaseSecret = new secretsmanager.Secret(this, 'DatabaseSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'appuser' }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const dbSubnetGroup = new rds.SubnetGroup(this, 'DatabaseSubnetGroup', {
      vpc,
      description: 'Private subnets for the PostgreSQL instance',
      vpcSubnets: { subnets: vpc.privateSubnets },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const database = new rds.DatabaseInstance(this, 'Database', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_16_4,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      allocatedStorage: 20,
      storageType: rds.StorageType.GP3,
      storageEncrypted: true,
      multiAz: false,
      publiclyAccessible: false,
      deletionProtection: false,
      deleteAutomatedBackups: true,
      backupRetention: cdk.Duration.days(7),
      vpc,
      subnetGroup: dbSubnetGroup,
      vpcSubnets: { subnets: vpc.privateSubnets },
      securityGroups: [databaseSecurityGroup],
      credentials: rds.Credentials.fromUsername('appuser', {
        password: databaseSecret.secretValueFromJson('password'),
      }),
      databaseName: DATABASE_NAME,
      port: 5432,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const frontendLogGroup = new logs.LogGroup(this, 'FrontendLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const backendLogGroup = new logs.LogGroup(this, 'BackendLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const schemaLogGroup = new logs.LogGroup(this, 'SchemaLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const frontendRole = new iam.Role(this, 'FrontendLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    addLogWritePolicy(frontendRole, frontendLogGroup);

    const backendRole = new iam.Role(this, 'BackendLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    addLogWritePolicy(backendRole, backendLogGroup);
    addVpcAccessPolicies(this, backendRole, vpc, backendSecurityGroup);
    backendRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [databaseSecret.secretArn],
      }),
    );
    backendRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:PutObject'],
        resources: [archiveBucket.arnForObjects('items/*')],
      }),
    );
    backendRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'sqs:ChangeMessageVisibility',
          'sqs:DeleteMessage',
          'sqs:GetQueueAttributes',
          'sqs:ReceiveMessage',
          'sqs:SendMessage',
        ],
        resources: [ingestionQueue.queueArn],
      }),
    );

    const schemaRole = new iam.Role(this, 'SchemaLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    addLogWritePolicy(schemaRole, schemaLogGroup);
    addVpcAccessPolicies(this, schemaRole, vpc, backendSecurityGroup);
    schemaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [databaseSecret.secretArn],
      }),
    );
    const frontendLambda = new lambda.Function(this, 'FrontendLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(writeLambdaAsset('frontend', frontendSource)),
      memorySize: 256,
      timeout: cdk.Duration.seconds(5),
      role: frontendRole,
      logGroup: frontendLogGroup,
    });

    const backendLambda = new lambda.Function(this, 'BackendLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(writeLambdaAsset('backend', backendSource)),
      memorySize: 512,
      timeout: cdk.Duration.seconds(15),
      role: backendRole,
      vpc,
      vpcSubnets: { subnets: vpc.privateSubnets },
      securityGroups: [backendSecurityGroup],
      logGroup: backendLogGroup,
      environment: {
        ARCHIVE_BUCKET_NAME: archiveBucket.bucketName,
        DB_HOST: database.instanceEndpoint.hostname,
        DB_NAME: DATABASE_NAME,
        DB_SECRET_ARN: databaseSecret.secretArn,
        INGESTION_QUEUE_URL: ingestionQueue.queueUrl,
      },
    });

    const schemaLambda = new lambda.Function(this, 'SchemaLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(writeLambdaAsset('schema', schemaSource)),
      memorySize: 512,
      timeout: cdk.Duration.seconds(60),
      role: schemaRole,
      vpc,
      vpcSubnets: { subnets: vpc.privateSubnets },
      securityGroups: [backendSecurityGroup],
      logGroup: schemaLogGroup,
      environment: {
        DB_HOST: database.instanceEndpoint.hostname,
        DB_NAME: DATABASE_NAME,
        DB_SECRET_ARN: databaseSecret.secretArn,
      },
    });

    new lambda.EventSourceMapping(this, 'IngestionEventSourceMapping', {
      target: backendLambda,
      eventSourceArn: ingestionQueue.queueArn,
      batchSize: 5,
    });

    const schemaVersion = crypto.createHash('sha256').update(schemaSource).digest('hex');
    const schemaResource = new cdk.CustomResource(this, 'DatabaseSchema', {
      serviceToken: schemaLambda.functionArn,
      properties: {
        SchemaVersion: schemaVersion,
      },
    });
    schemaResource.node.addDependency(database);
    schemaResource.node.addDependency(secretsManagerEndpoint);

    const stateMachineRole = new iam.Role(this, 'StateMachineRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });
    stateMachineRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [backendLambda.functionArn],
      }),
    );

    const validationStateMachine = new sfn.StateMachine(this, 'ValidationStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      role: stateMachineRole,
      definitionBody: sfn.DefinitionBody.fromChainable(
        new tasks.LambdaInvoke(this, 'ValidateArchivedObject', {
          lambdaFunction: backendLambda,
          payload: sfn.TaskInput.fromObject({
            mode: 'validate',
            s3Key: sfn.JsonPath.stringAt('$.s3Key'),
          }),
          payloadResponseOnly: true,
        }),
      ),
    });

    const pipeRole = new iam.Role(this, 'PipeRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
    });
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'sqs:ChangeMessageVisibility',
          'sqs:DeleteMessage',
          'sqs:GetQueueAttributes',
          'sqs:ReceiveMessage',
        ],
        resources: [ingestionQueue.queueArn],
      }),
    );
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [backendLambda.functionArn],
      }),
    );
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [validationStateMachine.stateMachineArn],
      }),
    );

    new pipes.CfnPipe(this, 'ValidationPipe', {
      roleArn: pipeRole.roleArn,
      source: ingestionQueue.queueArn,
      enrichment: backendLambda.functionArn,
      target: validationStateMachine.stateMachineArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 1,
          maximumBatchingWindowInSeconds: 5,
        },
      },
      enrichmentParameters: {
        inputTemplate: '{"body": <$.body>}',
      },
      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
      },
    });

    const httpApi = new apigwv2.HttpApi(this, 'HttpApi');
    httpApi.addRoutes({
      path: '/',
      methods: [apigwv2.HttpMethod.GET],
      integration: new HttpLambdaIntegration('FrontendIntegration', frontendLambda),
    });
    httpApi.addRoutes({
      path: '/api/{proxy+}',
      methods: [apigwv2.HttpMethod.ANY],
      integration: new HttpLambdaIntegration('BackendIntegration', backendLambda),
    });

    new cdk.CfnOutput(this, 'HttpApiEndpoint', {
      value: httpApi.apiEndpoint,
    });
  }
}

const awsRegion = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'us-east-1';

const app = new cdk.App({ outdir: 'cdk.out' });
new ThreeTierPocStack(app, 'ThreeTierPocStack', {
  env: {
    region: awsRegion,
  },
});
app.synth();
