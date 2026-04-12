#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as redshift from 'aws-cdk-lib/aws-redshift';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfnTasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export interface NormalizedEnvironment {
  readonly awsEndpoint?: string;
  readonly awsRegion: string;
  readonly awsAccessKeyId?: string;
  readonly awsSecretAccessKey?: string;
}

export function normalizeEnvironment(source: NodeJS.ProcessEnv = process.env): NormalizedEnvironment {
  const trimmedEndpoint = source.AWS_ENDPOINT?.trim();
  const trimmedRegion = source.AWS_REGION?.trim();

  return {
    awsEndpoint: trimmedEndpoint && trimmedEndpoint.length > 0 ? trimmedEndpoint : undefined,
    awsRegion: trimmedRegion && trimmedRegion.length > 0 ? trimmedRegion : 'us-east-1',
    awsAccessKeyId: source.AWS_ACCESS_KEY_ID,
    awsSecretAccessKey: source.AWS_SECRET_ACCESS_KEY,
  };
}

export function applyEndpointOverrides(environment: NormalizedEnvironment): void {
  process.env.AWS_REGION = environment.awsRegion;
  process.env.CDK_DEFAULT_REGION = environment.awsRegion;

  if (environment.awsEndpoint) {
    process.env.AWS_ENDPOINT = environment.awsEndpoint;
    process.env.AWS_ENDPOINT_URL = environment.awsEndpoint;
  }
}

export function sanitizeName(value: string, fallback: string): string {
  const sanitized = value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  return sanitized.length > 0 ? sanitized : fallback;
}

function glueArn(resourceType: string, suffix?: string): string {
  const base = `arn:${cdk.Aws.PARTITION}:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}`;
  return suffix ? `${base}:${resourceType}/${suffix}` : `${base}:${resourceType}`;
}

function buildLogGroupArn(scope: Construct, logGroupName: string): string {
  return cdk.Stack.of(scope).formatArn({
    service: 'logs',
    resource: 'log-group',
    resourceName: logGroupName,
    arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
  });
}

function createSharedLambdaLogPolicy(scope: Construct, logGroupName: string): iam.PolicyStatement {
  const logGroupArn = buildLogGroupArn(scope, logGroupName);

  return new iam.PolicyStatement({
    actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
    resources: [logGroupArn, `${logGroupArn}:*`],
  });
}

function buildApiLambdaCode(): string {
  return `
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");
const net = require("node:net");

const requireEnv = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error("Missing required environment variable: " + name);
  }
  return value;
};

const clientConfig = () => {
  const config = { region: process.env.AWS_REGION || "us-east-1" };
  if (process.env.AWS_ENDPOINT) {
    config.endpoint = process.env.AWS_ENDPOINT;
  }
  return config;
};

const parseBody = (event) => {
  if (!event || !event.body) {
    return {};
  }
  if (typeof event.body === "string") {
    try {
      return JSON.parse(event.body);
    } catch (_) {
      return {};
    }
  }
  return event.body;
};

const correlationMetadata = (source) => {
  if (!source || typeof source.correlationId !== "string" || source.correlationId.length === 0) {
    return {};
  }
  return { correlationId: source.correlationId };
};

const sendOrderEvent = async (kind, metadata = {}) => {
  const queue = new SQSClient(clientConfig());
  const payload = {
    kind,
    orderId: "order-" + Date.now(),
    timestamp: new Date().toISOString(),
    ...metadata,
  };
  await queue.send(new SendMessageCommand({
    QueueUrl: requireEnv("ORDER_QUEUE_URL"),
    MessageBody: JSON.stringify(payload),
  }));
  return payload;
};

const loadDbSecret = async () => {
  const secrets = new SecretsManagerClient(clientConfig());
  const response = await secrets.send(new GetSecretValueCommand({
    SecretId: requireEnv("DB_SECRET_ARN"),
  }));
  if (!response.SecretString) {
    throw new Error("Database secret is missing generated credentials");
  }
  return JSON.parse(response.SecretString);
};

const normalizePort = (...candidates) => {
  for (const candidate of candidates) {
    const port = Number(candidate);
    if (Number.isInteger(port) && port >= 0 && port < 65536) {
      return port;
    }
  }
  return 5432;
};

const checkDatabaseEndpoint = async (host, port) => {
  const normalizedPort = normalizePort(port);
  return await new Promise((resolve) => {
    const socket = net.createConnection({ host, port: normalizedPort }, () => {
      socket.end();
      resolve({ host, port: normalizedPort, reachable: true });
    });
    socket.setTimeout(1000, () => {
      socket.destroy();
      resolve({ host, port: normalizedPort, reachable: false });
    });
    socket.on("error", () => {
      resolve({ host, port: normalizedPort, reachable: false });
    });
  });
};

exports.handler = async (event) => {
  const method = event && event.httpMethod;
  const path = event && event.path;
  const isSchedulerInvocation = !method && event && event.source === "scheduler" && event.action === "heartbeat";

  if (isSchedulerInvocation) {
    const payload = await sendOrderEvent("heartbeat", correlationMetadata(event));
    return {
      statusCode: 202,
      body: JSON.stringify({ accepted: true, source: "scheduler", payload }),
    };
  }

  if (method === "POST" && path === "/orders") {
    const payload = await sendOrderEvent("order-created", correlationMetadata(parseBody(event)));
    return {
      statusCode: 202,
      body: JSON.stringify({ accepted: true, payload }),
    };
  }

  if (method === "GET" && path === "/orders") {
    const secret = await loadDbSecret();
    const dbHost = requireEnv("DB_HOST");
    const endpoint = await checkDatabaseEndpoint(dbHost, normalizePort(process.env.DB_PORT, secret.port, 5432));
    const credentialKeys = ["username", "password"].filter((key) => Boolean(secret[key]));
    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        database: {
          host: dbHost,
          databaseName: process.env.DB_NAME || secret.dbname || null,
          credentialsResolved: credentialKeys.length > 0,
          endpoint,
        },
      }),
    };
  }

  return {
    statusCode: 405,
    body: JSON.stringify({ message: "Method Not Allowed" }),
  };
};
`.trim();
}

function buildEnrichmentLambdaCode(): string {
  return `
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const requireEnv = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error("Missing required environment variable: " + name);
  }
  return value;
};

const clientConfig = () => {
  const config = { region: process.env.AWS_REGION || "us-east-1" };
  if (process.env.AWS_ENDPOINT) {
    config.endpoint = process.env.AWS_ENDPOINT;
  }
  return config;
};

exports.handler = async (event) => {
  const body = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
  const auditId = event.messageId || "unknown-message";
  const enriched = {
    auditId: event.messageId || "unknown-message",
    receivedAt: new Date().toISOString(),
    detail: body,
  };

  const record = {
    auditId,
    processedAt: new Date().toISOString(),
    stage: "enrichment",
    event: enriched,
  };

  const s3 = new S3Client(clientConfig());
  await s3.send(new PutObjectCommand({
    Bucket: requireEnv("ENRICHMENT_AUDIT_BUCKET_NAME"),
    Key: "audit/enrichment/" + auditId + ".json",
    ContentType: "application/json",
    Body: JSON.stringify(record),
  }));

  return enriched;
};
`.trim();
}

function buildProcessorLambdaCode(): string {
  return `
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const requireEnv = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error("Missing required environment variable: " + name);
  }
  return value;
};

exports.handler = async (event) => {
  const clientConfig = { region: process.env.AWS_REGION || "us-east-1" };
  if (process.env.AWS_ENDPOINT) {
    clientConfig.endpoint = process.env.AWS_ENDPOINT;
  }

  const s3 = new S3Client(clientConfig);
  const auditId = event.auditId || ("audit-" + Date.now());
  const record = {
    auditId,
    processedAt: new Date().toISOString(),
    event,
  };

  await s3.send(new PutObjectCommand({
    Bucket: requireEnv("AUDIT_BUCKET_NAME"),
    Key: "audit/" + auditId + ".json",
    ContentType: "application/json",
    Body: JSON.stringify(record),
  }));

  return record;
};
`.trim();
}

export class BackendLogicStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'VpcFabric', {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
      ],
    });

    const computeSecurityGroup = new ec2.SecurityGroup(this, 'ComputeSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Compute tier security group',
    });

    const databaseSecurityGroup = new ec2.SecurityGroup(this, 'DatabaseSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Managed database tier security group',
    });

    databaseSecurityGroup.addIngressRule(
      computeSecurityGroup,
      ec2.Port.tcp(5432),
      'Allow PostgreSQL access only from the compute tier',
    );

    const sharedLogGroup = new logs.LogGroup(this, 'SharedLambdaLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      logGroupName: `/backend-logic/${sanitizeName(this.stackName, 'backend-logic')}/lambda`,
    });

    const auditBucket = new s3.Bucket(this, 'OrdersAuditBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const orderQueue = new sqs.Queue(this, 'OrderEventsQueue', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(4),
    });

    const databaseSecret = new secretsmanager.Secret(this, 'OrdersDatabaseSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: 'orders_admin',
          dbname: 'ordersdb',
        }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
    });
    databaseSecret.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const databaseSubnetGroup = new rds.CfnDBSubnetGroup(this, 'OrdersDatabaseSubnetGroup', {
      dbSubnetGroupDescription: 'Private subnet group for the orders database',
      subnetIds: vpc.privateSubnets.map((subnet) => subnet.subnetId),
    });
    databaseSubnetGroup.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const database = new rds.CfnDBInstance(this, 'OrdersDatabase', {
      dbInstanceClass: 'db.t3.micro',
      engine: 'postgres',
      engineVersion: '15.10',
      allocatedStorage: '20',
      storageEncrypted: true,
      publiclyAccessible: false,
      dbSubnetGroupName: databaseSubnetGroup.ref,
      vpcSecurityGroups: [databaseSecurityGroup.securityGroupId],
      masterUsername: new cdk.CfnDynamicReference(
        cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
        `${databaseSecret.secretArn}:SecretString:username`,
      ).toString(),
      masterUserPassword: new cdk.CfnDynamicReference(
        cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
        `${databaseSecret.secretArn}:SecretString:password`,
      ).toString(),
      dbName: 'ordersdb',
      deletionProtection: false,
      deleteAutomatedBackups: true,
    });
    database.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    database.addDependency(databaseSubnetGroup);

    const ordersHandlerName = `${this.stackName}-orders-handler`;
    const enrichmentHandlerName = `${this.stackName}-pipe-enrichment`;
    const processorHandlerName = `${this.stackName}-audit-processor`;

    const lambdaBaseEnvironment: Record<string, string> = {};

    if (process.env.AWS_ENDPOINT) {
      lambdaBaseEnvironment.AWS_ENDPOINT = process.env.AWS_ENDPOINT;
    }

    const ordersLambdaRole = new iam.Role(this, 'OrdersLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    ordersLambdaRole.addToPolicy(createSharedLambdaLogPolicy(this, sharedLogGroup.logGroupName));
    ordersLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sqs:SendMessage'],
      resources: [orderQueue.queueArn],
    }));
    ordersLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:DescribeSecret', 'secretsmanager:GetSecretValue'],
      resources: [databaseSecret.secretArn],
    }));
    ordersLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'ec2:AssignPrivateIpAddresses',
        'ec2:CreateNetworkInterface',
        'ec2:DeleteNetworkInterface',
        'ec2:DescribeNetworkInterfaces',
        'ec2:DescribeSecurityGroups',
        'ec2:DescribeSubnets',
        'ec2:DescribeVpcs',
        'ec2:UnassignPrivateIpAddresses',
      ],
      // These EC2 VPC attachment actions do not support resource-level scoping.
      resources: ['*'],
    }));

    const ordersLambda = new lambda.Function(this, 'OrdersHandler', {
      functionName: ordersHandlerName,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(buildApiLambdaCode()),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      role: ordersLambdaRole,
      logGroup: sharedLogGroup,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [computeSecurityGroup],
      environment: {
        ...lambdaBaseEnvironment,
        ORDER_QUEUE_URL: orderQueue.queueUrl,
        DB_SECRET_ARN: databaseSecret.secretArn,
        DB_HOST: database.attrEndpointAddress,
        DB_PORT: database.attrEndpointPort,
        DB_NAME: 'ordersdb',
      },
    });

    const enrichmentLambdaRole = new iam.Role(this, 'EnrichmentLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    enrichmentLambdaRole.addToPolicy(createSharedLambdaLogPolicy(this, sharedLogGroup.logGroupName));
    enrichmentLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [auditBucket.arnForObjects('audit/enrichment/*')],
    }));

    const enrichmentLambda = new lambda.Function(this, 'PipeEnrichmentHandler', {
      functionName: enrichmentHandlerName,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(buildEnrichmentLambdaCode()),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      role: enrichmentLambdaRole,
      logGroup: sharedLogGroup,
      environment: {
        ...lambdaBaseEnvironment,
        ENRICHMENT_AUDIT_BUCKET_NAME: auditBucket.bucketName,
      },
    });

    const processorLambdaRole = new iam.Role(this, 'ProcessorLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    processorLambdaRole.addToPolicy(createSharedLambdaLogPolicy(this, sharedLogGroup.logGroupName));
    processorLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [auditBucket.arnForObjects('*')],
    }));

    const processorLambda = new lambda.Function(this, 'AuditProcessorHandler', {
      functionName: processorHandlerName,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(buildProcessorLambdaCode()),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      role: processorLambdaRole,
      logGroup: sharedLogGroup,
      environment: {
        ...lambdaBaseEnvironment,
        AUDIT_BUCKET_NAME: auditBucket.bucketName,
      },
    });

    const api = new apigateway.RestApi(this, 'OrdersApi', {
      cloudWatchRole: false,
      deployOptions: {
        tracingEnabled: false,
      },
    });

    const ordersResource = api.root.addResource('orders');
    const ordersIntegration = new apigateway.LambdaIntegration(ordersLambda);
    ordersResource.addMethod('POST', ordersIntegration);
    ordersResource.addMethod('GET', ordersIntegration);

    const workflowDefinition = sfn.Chain.start(
      new sfnTasks.LambdaInvoke(this, 'WriteAuditRecord', {
        lambdaFunction: processorLambda,
        payloadResponseOnly: true,
      }),
    ).next(new sfn.Succeed(this, 'ProcessingComplete'));

    const stateMachine = new sfn.StateMachine(this, 'OrderProcessingStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      definitionBody: sfn.DefinitionBody.fromChainable(workflowDefinition),
    });

    const pipeRole = new iam.Role(this, 'OrderPipeRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
    });
    pipeRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'sqs:ChangeMessageVisibility',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
        'sqs:ReceiveMessage',
      ],
      resources: [orderQueue.queueArn],
    }));
    pipeRole.addToPolicy(new iam.PolicyStatement({
      actions: ['states:StartExecution'],
      resources: [stateMachine.stateMachineArn],
    }));
    pipeRole.addToPolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [enrichmentLambda.functionArn],
    }));

    const orderPipe = new pipes.CfnPipe(this, 'QueueToStateMachinePipe', {
      roleArn: pipeRole.roleArn,
      source: orderQueue.queueArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 1,
        },
      },
      enrichment: enrichmentLambda.functionArn,
      enrichmentParameters: {
        inputTemplate: '{"messageId": <$.messageId>, "body": <$.body>}',
      },
      target: stateMachine.stateMachineArn,
      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
      },
    });

    enrichmentLambda.addPermission('AllowPipeInvocation', {
      principal: new iam.ServicePrincipal('pipes.amazonaws.com'),
      sourceArn: orderPipe.attrArn,
    });

    const schedulerRole = new iam.Role(this, 'HeartbeatSchedulerRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    schedulerRole.addToPolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [ordersLambda.functionArn],
    }));

    const heartbeatSchedule = new scheduler.CfnSchedule(this, 'HeartbeatSchedule', {
      flexibleTimeWindow: { mode: 'OFF' },
      scheduleExpression: 'rate(5 minutes)',
      target: {
        arn: ordersLambda.functionArn,
        roleArn: schedulerRole.roleArn,
        input: JSON.stringify({
          source: 'scheduler',
          action: 'heartbeat',
        }),
      },
    });

    ordersLambda.addPermission('AllowSchedulerInvocation', {
      principal: new iam.ServicePrincipal('scheduler.amazonaws.com'),
      sourceArn: heartbeatSchedule.attrArn,
    });

    const redshiftSubnetGroup = new redshift.CfnClusterSubnetGroup(this, 'RedshiftSubnetGroup', {
      description: 'Private subnet group for the Redshift cluster',
      subnetIds: vpc.privateSubnets.map((subnet) => subnet.subnetId),
    });
    redshiftSubnetGroup.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const redshiftAdminSecret = new secretsmanager.Secret(this, 'RedshiftAdminSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: 'clusteradmin',
        }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
    });
    redshiftAdminSecret.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const redshiftCluster = new redshift.CfnCluster(this, 'OrdersWarehouse', {
      clusterType: 'single-node',
      dbName: 'dev',
      encrypted: true,
      masterUsername: 'clusteradmin',
      masterUserPassword: new cdk.CfnDynamicReference(
        cdk.CfnDynamicReferenceService.SECRETS_MANAGER,
        `${redshiftAdminSecret.secretArn}:SecretString:password`,
      ).toString(),
      nodeType: 'dc2.large',
      numberOfNodes: 1,
      clusterSubnetGroupName: redshiftSubnetGroup.ref,
      publiclyAccessible: false,
      port: 5439,
      vpcSecurityGroupIds: [vpc.vpcDefaultSecurityGroup],
    });
    redshiftCluster.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    redshiftCluster.addDependency(redshiftSubnetGroup);

    const glueDatabaseName = sanitizeName(`${cdk.Names.uniqueId(this)}-catalog`, 'orders-catalog').replace(/-/g, '_');
    const glueConnectionName = sanitizeName(`${cdk.Names.uniqueId(this)}-redshift-connection`, 'redshift-connection');

    const catalogDatabase = new glue.CfnDatabase(this, 'OrdersCatalogDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: glueDatabaseName,
      },
    });

    const glueConnection = new glue.CfnConnection(this, 'RedshiftJdbcConnection', {
      catalogId: this.account,
      connectionInput: {
        name: glueConnectionName,
        connectionType: 'JDBC',
        connectionProperties: {
          JDBC_CONNECTION_URL: cdk.Fn.join('', [
            'jdbc:redshift://',
            redshiftCluster.attrEndpointAddress,
            ':',
            redshiftCluster.attrEndpointPort,
            '/dev',
          ]),
          JDBC_ENGINE: 'redshift',
        },
        authenticationConfiguration: {
          authenticationType: 'BASIC',
          secretArn: redshiftAdminSecret.secretArn,
        },
        physicalConnectionRequirements: {
          availabilityZone: vpc.privateSubnets[0].availabilityZone,
          subnetId: vpc.privateSubnets[0].subnetId,
          securityGroupIdList: [vpc.vpcDefaultSecurityGroup],
        },
      },
    });
    glueConnection.addDependency(redshiftCluster);

    const glueRole = new iam.Role(this, 'GlueCrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:DescribeSecret', 'secretsmanager:GetSecretValue'],
      resources: [redshiftAdminSecret.secretArn],
    }));
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: ['glue:GetConnection'],
      resources: [
        glueArn('connection', glueConnectionName),
      ],
    }));
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:BatchCreatePartition',
        'glue:BatchDeletePartition',
        'glue:BatchUpdatePartition',
        'glue:CreateTable',
        'glue:DeletePartition',
        'glue:DeleteTable',
        'glue:GetDatabase',
        'glue:GetDatabases',
        'glue:GetPartition',
        'glue:GetPartitions',
        'glue:GetTable',
        'glue:GetTables',
        'glue:UpdatePartition',
        'glue:UpdateTable',
      ],
      resources: [
        glueArn('catalog'),
        glueArn('database', glueDatabaseName),
        // Glue table ARNs require a wildcard suffix for tables created inside the single catalog database.
        glueArn('table', `${glueDatabaseName}/*`),
      ],
    }));
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      // Glue controls its own log group naming, so the narrowest stable scope is the Glue log namespace.
      resources: [
        this.formatArn({
          service: 'logs',
          resource: 'log-group',
          resourceName: '/aws-glue/*',
          arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
        }),
        this.formatArn({
          service: 'logs',
          resource: 'log-group',
          resourceName: '/aws-glue/*:*',
          arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
        }),
      ],
    }));
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'ec2:CreateNetworkInterface',
        'ec2:DeleteNetworkInterface',
        'ec2:DescribeNetworkInterfaces',
        'ec2:DescribeSecurityGroups',
        'ec2:DescribeSubnets',
        'ec2:DescribeVpcEndpoints',
        'ec2:DescribeVpcs',
      ],
      // Glue crawler VPC attachments also require wildcard resource permissions.
      resources: ['*'],
    }));

    const crawler = new glue.CfnCrawler(this, 'RedshiftCrawler', {
      databaseName: glueDatabaseName,
      role: glueRole.roleArn,
      targets: {
        jdbcTargets: [
          {
            connectionName: glueConnectionName,
            path: 'dev/public/%',
          },
        ],
      },
    });
    crawler.addDependency(catalogDatabase);
    crawler.addDependency(glueConnection);

  }
}

export function synthesizeApp(environment: NormalizedEnvironment = normalizeEnvironment()): cdk.App {
  applyEndpointOverrides(environment);

  const app = new cdk.App();
  new BackendLogicStack(app, 'BackendLogicStack', {
    env: {
      region: environment.awsRegion,
    },
  });
  return app;
}

/* istanbul ignore next */
if (require.main === module) {
  synthesizeApp().synth();
}
