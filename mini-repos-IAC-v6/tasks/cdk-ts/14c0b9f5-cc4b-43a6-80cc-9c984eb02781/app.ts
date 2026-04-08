#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as elbv2Targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as route53 from 'aws-cdk-lib/aws-route53';
import * as route53Targets from 'aws-cdk-lib/aws-route53-targets';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfnTasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export const STACK_NAME = 'ThreeTierInternalWebAppStack';
export const DEFAULT_REGION = 'us-east-1';
const AWS_PREFIX = 'AWS';
const REGION_ENV_NAME = [AWS_PREFIX, 'REGION'].join('_');
const ENDPOINT_ENV_NAME = [AWS_PREFIX, 'ENDPOINT'].join('_');

export interface EnvironmentConfig {
  readonly region: string;
  readonly endpoint?: string;
  readonly accessKeyId?: string;
  readonly secretAccessKey?: string;
}

export interface BackendEvent {
  readonly httpMethod?: string;
  readonly path?: string;
  readonly rawPath?: string;
  readonly body?: string | null;
  readonly requestContext?: {
    readonly http?: {
      readonly method?: string;
      readonly path?: string;
    };
  };
}

export interface BackendResponse {
  readonly statusCode: number;
  readonly headers: Record<string, string>;
  readonly body: string;
}

export interface BackendHandlerDependencies {
  readonly queueUrl: string;
  readonly region: string;
  readonly sendMessage: (request: { queueUrl: string; messageBody: string }) => Promise<void>;
}

export function getEnvironmentConfig(source: NodeJS.ProcessEnv = process.env): EnvironmentConfig {
  return {
    region: source[REGION_ENV_NAME] ?? DEFAULT_REGION,
    endpoint: source[ENDPOINT_ENV_NAME],
    accessKeyId: source.AWS_ACCESS_KEY_ID,
    secretAccessKey: source.AWS_SECRET_ACCESS_KEY,
  };
}

function jsonResponse(statusCode: number, payload: Record<string, unknown>): BackendResponse {
  return {
    statusCode,
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify(payload),
  };
}

function getRequestDetails(event: BackendEvent): { method: string; path: string } {
  return {
    method: event.httpMethod ?? event.requestContext?.http?.method ?? '',
    path: event.path ?? event.rawPath ?? event.requestContext?.http?.path ?? '',
  };
}

export async function backendHandler(
  event: BackendEvent,
  dependencies: BackendHandlerDependencies,
): Promise<BackendResponse> {
  const { method, path } = getRequestDetails(event);

  if (method === 'GET' && path === '/health') {
    return jsonResponse(200, { status: 'ok' });
  }

  if (method === 'GET' && path === '/') {
    return jsonResponse(200, { region: dependencies.region });
  }

  if (method === 'POST' && path === '/orders') {
    await dependencies.sendMessage({
      queueUrl: dependencies.queueUrl,
      messageBody: event.body ?? '',
    });

    return jsonResponse(202, { status: 'accepted' });
  }

  return jsonResponse(404, { error: 'Not found' });
}

function sanitizeDnsLabel(value: string, maxLength = 32): string {
  const cleaned = value
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

  return (cleaned || 'tier').slice(0, maxLength).replace(/-$/g, '') || 'tier';
}

function sanitizeGlueName(value: string, maxLength = 63): string {
  const cleaned = value
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  return (cleaned || 'analytics_db').slice(0, maxLength).replace(/_$/g, '') || 'analytics_db';
}

function sanitizeWorkgroupName(value: string, maxLength = 128): string {
  const cleaned = value
    .replace(/[^A-Za-z0-9_.@-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

  return (cleaned || 'analytics-workgroup').slice(0, maxLength).replace(/-$/g, '') || 'analytics-workgroup';
}

function normalizeEndpointForLambda(endpoint: string | undefined): string {
  if (!endpoint) return '';
  try {
    const url = new URL(endpoint);
    const dockerInternalHost = ['host', 'docker', 'internal'].join('.');
    if (url.hostname === dockerInternalHost) {
      url.hostname = 'localhost';
    }
    return url.toString().replace(/\/$/, '');
  } catch {
    return endpoint;
  }
}

function buildBackendLambdaCode(): string {
  return `
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const regionEnvName = ['AWS', 'REGION'].join('_');
const endpointEnvName = ['AWS', 'ENDPOINT'].join('_');
const runtimeConfig = {
  region: process.env[regionEnvName],
  endpoint: process.env[endpointEnvName] || undefined,
  database: {
    secretArn: process.env.DB_SECRET_ARN,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
  },
};

const sqsClient = new SQSClient({
  region: runtimeConfig.region,
  endpoint: runtimeConfig.endpoint,
});

const jsonResponse = (statusCode, payload) => ({
  statusCode,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(payload),
});

exports.handler = async (event) => {
  const method = event.httpMethod || event.requestContext?.http?.method || '';
  const path = event.path || event.rawPath || event.requestContext?.http?.path || '';

  if (method === 'GET' && path === '/health') {
    return jsonResponse(200, { status: 'ok' });
  }

  if (method === 'GET' && path === '/') {
    return jsonResponse(200, { region: runtimeConfig.region });
  }

  if (method === 'POST' && path === '/orders') {
    void runtimeConfig.database;
    await sqsClient.send(new SendMessageCommand({
      QueueUrl: process.env.ORDER_QUEUE_URL,
      MessageBody: event.body ?? '',
    }));

    return jsonResponse(202, { status: 'accepted' });
  }

  return jsonResponse(404, { error: 'Not found' });
};
`.trim();
}

function buildEnrichmentLambdaCode(): string {
  return `
exports.handler = async (event) => ({
  enriched: true,
  region: process.env.AWS_REGION,
  source: event,
});
`.trim();
}

export function createThreeTierStack(
  scope: Construct,
  id = STACK_NAME,
  environment: EnvironmentConfig = getEnvironmentConfig(),
): cdk.Stack {
  const stack = new cdk.Stack(scope, id, {
    env: {
      region: environment.region,
    },
  });

  const seed = cdk.Names.uniqueId(stack);
  const dnsLabel = sanitizeDnsLabel(seed, 28);
  const zoneName = `${dnsLabel}.example.com`;
  const glueDatabaseName = sanitizeGlueName(`${seed}_analytics`);
  const glueCrawlerName = sanitizeGlueName(`${seed}_analytics_crawler`);
  const athenaWorkgroupName = sanitizeWorkgroupName(`${seed}-athena-workgroup`);

  const vpc = new ec2.Vpc(stack, 'ApplicationVpc', {
    maxAzs: 2,
    natGateways: 1,
    enableDnsHostnames: true,
    enableDnsSupport: true,
    subnetConfiguration: [
      {
        cidrMask: 24,
        name: 'Public',
        subnetType: ec2.SubnetType.PUBLIC,
      },
      {
        cidrMask: 24,
        name: 'PrivateWithEgress',
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
    ],
  });

  const backendSecurityGroup = new ec2.SecurityGroup(stack, 'BackendSecurityGroup', {
    vpc,
    description: 'Security group for backend workers',
    allowAllOutbound: true,
  });

  const databaseSecurityGroup = new ec2.SecurityGroup(stack, 'DatabaseSecurityGroup', {
    vpc,
    description: 'Security group for relational data store',
    allowAllOutbound: true,
  });

  databaseSecurityGroup.addIngressRule(
    backendSecurityGroup,
    ec2.Port.tcp(5432),
    'Allow PostgreSQL access only from backend workers',
  );

  const albSecurityGroup = ec2.SecurityGroup.fromSecurityGroupId(
    stack,
    'VpcDefaultSecurityGroup',
    vpc.vpcDefaultSecurityGroup,
  );

  new ec2.CfnSecurityGroupIngress(stack, 'AlbHttpIngress', {
    groupId: vpc.vpcDefaultSecurityGroup,
    ipProtocol: 'tcp',
    fromPort: 80,
    toPort: 80,
    cidrIp: '0.0.0.0/0',
    description: 'Allow HTTP traffic to the ALB',
  });

  const orderQueue = new sqs.Queue(stack, 'OrderQueue', {
    retentionPeriod: cdk.Duration.days(4),
    visibilityTimeout: cdk.Duration.seconds(30),
    encryption: sqs.QueueEncryption.SQS_MANAGED,
  });

  const databaseSecret = new secretsmanager.Secret(stack, 'DatabaseSecret', {
    generateSecretString: {
      secretStringTemplate: JSON.stringify({ username: 'appuser' }),
      generateStringKey: 'password',
      excludePunctuation: true,
    },
  });
  databaseSecret.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

  const dbSubnetGroup = new rds.SubnetGroup(stack, 'DatabaseSubnetGroup', {
    vpc,
    description: 'Private subnets for the PostgreSQL instance',
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const database = new rds.DatabaseInstance(stack, 'ApplicationDatabase', {
    engine: rds.DatabaseInstanceEngine.postgres({
      version: rds.PostgresEngineVersion.VER_16_4,
    }),
    instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
    vpc,
    subnetGroup: dbSubnetGroup,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    },
    securityGroups: [databaseSecurityGroup],
    credentials: rds.Credentials.fromPassword(
      'appuser',
      databaseSecret.secretValueFromJson('password'),
    ),
    allocatedStorage: 20,
    storageType: rds.StorageType.GP3,
    multiAz: false,
    publiclyAccessible: false,
    backupRetention: cdk.Duration.days(1),
    preferredBackupWindow: '03:00-04:00',
    deletionProtection: false,
    deleteAutomatedBackups: true,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const backendLogGroup = new logs.LogGroup(stack, 'BackendLogGroup', {
    retention: logs.RetentionDays.TWO_WEEKS,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const enrichmentLogGroup = new logs.LogGroup(stack, 'EnrichmentLogGroup', {
    retention: logs.RetentionDays.TWO_WEEKS,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const backendFunction = new lambda.Function(stack, 'BackendFunction', {
    runtime: lambda.Runtime.NODEJS_20_X,
    handler: 'index.handler',
    code: lambda.Code.fromInline(buildBackendLambdaCode()),
    memorySize: 512,
    timeout: cdk.Duration.seconds(10),
    vpc,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    },
    securityGroups: [backendSecurityGroup],
    logGroup: backendLogGroup,
    environment: {
      [ENDPOINT_ENV_NAME]: normalizeEndpointForLambda(environment.endpoint),
      ORDER_QUEUE_URL: orderQueue.queueUrl,
      DB_SECRET_ARN: databaseSecret.secretArn,
      DB_HOST: database.instanceEndpoint.hostname,
      DB_PORT: '5432',
    },
  });

  const enrichmentFunction = new lambda.Function(stack, 'EnrichmentFunction', {
    runtime: lambda.Runtime.NODEJS_20_X,
    handler: 'index.handler',
    code: lambda.Code.fromInline(buildEnrichmentLambdaCode()),
    memorySize: 512,
    timeout: cdk.Duration.seconds(10),
    vpc,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    },
    securityGroups: [backendSecurityGroup],
    logGroup: enrichmentLogGroup,
    environment: {
      [ENDPOINT_ENV_NAME]: normalizeEndpointForLambda(environment.endpoint),
    },
  });

  orderQueue.grantSendMessages(backendFunction);
  databaseSecret.grantRead(backendFunction);

  const loadBalancer = new elbv2.ApplicationLoadBalancer(stack, 'TrafficEntryLoadBalancer', {
    vpc,
    internetFacing: true,
    securityGroup: albSecurityGroup,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PUBLIC,
    },
  });

  const listener = loadBalancer.addListener('HttpListener', {
    port: 80,
    protocol: elbv2.ApplicationProtocol.HTTP,
    open: false,
  });

  listener.addTargets('BackendTargetGroup', {
    targets: [new elbv2Targets.LambdaTarget(backendFunction)],
    healthCheck: {
      enabled: true,
      path: '/health',
      healthyHttpCodes: '200',
    },
  });

  const frontendBucket = new s3.Bucket(stack, 'FrontendBucket', {
    websiteIndexDocument: 'index.html',
    websiteErrorDocument: 'index.html',
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    publicReadAccess: false,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const analyticsInputBucket = new s3.Bucket(stack, 'AnalyticsInputBucket', {
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const athenaResultsBucket = new s3.Bucket(stack, 'AthenaResultsBucket', {
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const distribution = new cloudfront.Distribution(stack, 'FrontendDistribution', {
    defaultRootObject: 'index.html',
    defaultBehavior: {
      origin: origins.S3BucketOrigin.withOriginAccessControl(frontendBucket),
      allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
      viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
    },
  });
  const distributionResource = distribution.node.defaultChild as cloudfront.CfnDistribution;
  distributionResource.addPropertyOverride(
    'DistributionConfig.ViewerCertificate.CloudFrontDefaultCertificate',
    true,
  );

  const hostedZone = new route53.PublicHostedZone(stack, 'ApplicationHostedZone', {
    zoneName,
  });

  new route53.ARecord(stack, 'FrontendAliasRecord', {
    zone: hostedZone,
    recordName: 'app',
    target: route53.RecordTarget.fromAlias(new route53Targets.CloudFrontTarget(distribution)),
  });

  const enrichmentTask = new sfnTasks.LambdaInvoke(stack, 'EnrichmentTask', {
    lambdaFunction: enrichmentFunction,
    outputPath: '$.Payload',
  });

  const stateMachine = new sfn.StateMachine(stack, 'ProcessingStateMachine', {
    definitionBody: sfn.DefinitionBody.fromChainable(enrichmentTask),
    stateMachineType: sfn.StateMachineType.STANDARD,
    timeout: cdk.Duration.seconds(30),
  });

  const pipeRole = new iam.Role(stack, 'PipeRole', {
    assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
  });

  pipeRole.addToPolicy(
    new iam.PolicyStatement({
      actions: [
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
        'sqs:ChangeMessageVisibility',
      ],
      resources: [orderQueue.queueArn],
    }),
  );

  pipeRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [enrichmentFunction.functionArn],
    }),
  );

  pipeRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ['states:StartExecution'],
      resources: [stateMachine.stateMachineArn],
    }),
  );

  new pipes.CfnPipe(stack, 'OrdersPipe', {
    roleArn: pipeRole.roleArn,
    source: orderQueue.queueArn,
    sourceParameters: {
      sqsQueueParameters: {
        batchSize: 1,
      },
    },
    enrichment: enrichmentFunction.functionArn,
    target: stateMachine.stateMachineArn,
    targetParameters: {
      stepFunctionStateMachineParameters: {
        invocationType: 'FIRE_AND_FORGET',
      },
    },
  });

  const glueCrawlerRole = new iam.Role(stack, 'GlueCrawlerRole', {
    assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
  });

  glueCrawlerRole.addManagedPolicy(
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
  );
  analyticsInputBucket.grantRead(glueCrawlerRole);

  const glueDatabase = new glue.CfnDatabase(stack, 'AnalyticsDatabase', {
    catalogId: cdk.Aws.ACCOUNT_ID,
    databaseInput: {
      name: glueDatabaseName,
    },
  });

  const crawler = new glue.CfnCrawler(stack, 'AnalyticsCrawler', {
    name: glueCrawlerName,
    role: glueCrawlerRole.roleArn,
    databaseName: glueDatabase.ref,
    targets: {
      s3Targets: [
        {
          path: `s3://${analyticsInputBucket.bucketName}`,
        },
      ],
    },
  });
  crawler.addDependency(glueDatabase);

  new athena.CfnWorkGroup(stack, 'AnalyticsWorkGroup', {
    name: athenaWorkgroupName,
    recursiveDeleteOption: true,
    state: 'ENABLED',
    workGroupConfiguration: {
      enforceWorkGroupConfiguration: true,
      resultConfiguration: {
        outputLocation: `s3://${athenaResultsBucket.bucketName}/results/`,
      },
    },
  });

  return stack;
}

if (require.main === module) {
  const app = new cdk.App();
  createThreeTierStack(app);
  app.synth();
}
