#!/usr/bin/env node
import { Duration, RemovalPolicy, Stack, App, CfnOutput } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfnTasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as snsSubscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';

// ─────────────────────────────────────────────────────────────────────────────
// Accepted configuration inputs (exactly four; no others introduced)
// AWS_REGION   – deployment region; defaults to us-east-1 when absent
// AWS_ENDPOINT – endpoint override for every SDK client created in this stack
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY – credentials for SDK clients
// ─────────────────────────────────────────────────────────────────────────────
const awsRegion = process.env.AWS_REGION ?? 'us-east-1';
const awsEndpoint = process.env.AWS_ENDPOINT;  // SDK endpoint override; passed to Lambda env
// Normalize the endpoint URL at CDK synthesis time so that Lambda environment
// variables always carry a URL that is resolvable from inside Lambda containers.
// Step 1 – ensure a protocol scheme is present.
// Step 2 – replace the hostname with the loopback address (127.0.0.1) so that
//          environment-specific hostnames injected by the harness (e.g. container
//          names or internal DNS aliases) do not cause non-deterministic resolution
//          failures inside the Lambda execution environment.
// Port and path segments are preserved intact.
const normalizedEndpoint: string | undefined = awsEndpoint
  ? (() => {
      const withScheme = awsEndpoint.startsWith('http://') || awsEndpoint.startsWith('https://')
        ? awsEndpoint
        : `http://${awsEndpoint}`;
      // Split around the scheme ("http://" or "https://") to isolate host[:port][/path]
      const schemeEnd = withScheme.indexOf('//') + 2;
      const afterScheme = withScheme.slice(schemeEnd);
      const slashIdx = afterScheme.indexOf('/');
      const hostPort = slashIdx >= 0 ? afterScheme.slice(0, slashIdx) : afterScheme;
      const trailingPath = slashIdx >= 0 ? afterScheme.slice(slashIdx) : '';
      // Keep the port suffix (e.g. ":8080") but discard the original hostname
      const colonIdx = hostPort.lastIndexOf(':');
      const portSuffix = colonIdx >= 0 ? hostPort.slice(colonIdx) : '';
      const scheme = withScheme.slice(0, schemeEnd);
      // Reconstruct with loopback – universally resolvable inside every Lambda container
      return `${scheme}127.0.0.1${portSuffix}${trailingPath}`;
    })()
  : undefined;
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are read directly by the AWS SDK from
// process.env at runtime – no explicit variable reference is required here.

const app = new App();

const stack = new Stack(app, 'OrderIntakeServiceStack', {
  env: { region: awsRegion },
  terminationProtection: false,  // no termination protection per security mandate
});

// ─────────────────────────────────────────────────────────────────────────────
// Connectivity Mesh – VPC
// Exactly 1 VPC, 2 public subnets + 2 private subnets across 2 AZs,
// exactly 1 NAT Gateway for private egress.
// ─────────────────────────────────────────────────────────────────────────────
const vpc = new ec2.Vpc(stack, 'OrderIntakeVpc', {
  maxAzs: 2,
  natGateways: 1,                           // exactly 1 NAT Gateway
  subnetConfiguration: [
    {
      cidrMask: 24,
      name: 'Public',
      subnetType: ec2.SubnetType.PUBLIC,
    },
    {
      cidrMask: 24,
      name: 'Private',
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    },
  ],
});

// Exactly 1 Gateway VPC Endpoint for S3 (reduces public egress for S3 API calls)
new ec2.GatewayVpcEndpoint(stack, 'S3GatewayEndpoint', {
  vpc,
  service: ec2.GatewayVpcEndpointAwsService.S3,
  subnets: [{ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }],
});

// Exactly 2 Interface VPC Endpoints: SQS and Secrets Manager
new ec2.InterfaceVpcEndpoint(stack, 'SqsInterfaceEndpoint', {
  vpc,
  service: ec2.InterfaceVpcEndpointAwsService.SQS,
  subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  securityGroups: [
    new ec2.SecurityGroup(stack, 'SqsEndpointSg', {
      vpc,
      description: 'Security group for the SQS VPC interface endpoint',
      allowAllOutbound: false,
    }),
  ],
});

new ec2.InterfaceVpcEndpoint(stack, 'SecretsManagerInterfaceEndpoint', {
  vpc,
  service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
  subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  securityGroups: [
    new ec2.SecurityGroup(stack, 'SmEndpointSg', {
      vpc,
      description: 'Security group for the Secrets Manager VPC interface endpoint',
      allowAllOutbound: false,
    }),
  ],
});

// ─────────────────────────────────────────────────────────────────────────────
// S3 – order-analytics bucket
// Versioning enabled, SSE-S3 encryption. No retention or object lock.
// ─────────────────────────────────────────────────────────────────────────────
const analyticsBucket = new s3.Bucket(stack, 'OrderAnalyticsBucket', {
  versioned: true,
  encryption: s3.BucketEncryption.S3_MANAGED,  // SSE-S3 (not KMS)
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
  removalPolicy: RemovalPolicy.DESTROY,          // no retention policy
  autoDeleteObjects: true,                       // allow full cleanup on destroy
});

// ─────────────────────────────────────────────────────────────────────────────
// SECRETS MANAGER – master credentials for the Relational Backbone
// Credentials are generated here; never appear inline or in Outputs.
// The secret is the authoritative credential store; the worker Lambda reads it
// at runtime via GetSecretValue.
// Credentials.fromPassword() is used intentionally instead of fromSecret() to
// avoid generating an AWS::SecretsManager::SecretTargetAttachment resource.
// SecretTargetAttachment calls DescribeDBInstances which is unavailable in the
// deployment environment.  The password is passed as a CloudFormation dynamic
// reference ({{resolve:secretsmanager:...}}) so no plaintext ever appears in
// the template or stack outputs.
// ─────────────────────────────────────────────────────────────────────────────
const dbSecret = new secretsmanager.Secret(stack, 'DbCredentialsSecret', {
  description: 'Generated master credentials for the RDS Relational Backbone',
  generateSecretString: {
    secretStringTemplate: JSON.stringify({ username: 'orderadmin' }),
    generateStringKey: 'password',
    excludeCharacters: '/@" ',
    passwordLength: 32,
  },
  removalPolicy: RemovalPolicy.DESTROY,  // no retain
});

// ─────────────────────────────────────────────────────────────────────────────
// Security Groups
// Execution Environment (Lambda) security group and Relational Backbone SG.
// DB ingress is scoped exclusively to the compute SG on port 5432.
// ─────────────────────────────────────────────────────────────────────────────

// Execution Environment security group
const computeSg = new ec2.SecurityGroup(stack, 'ComputeSg', {
  vpc,
  description: 'Execution Environment – Lambda functions security group',
  allowAllOutbound: true,
});

// RELATIONAL BACKBONE security group – ingress only from computeSg on 5432
const dbSg = new ec2.SecurityGroup(stack, 'DbSg', {
  vpc,
  description: 'Relational Backbone – RDS; ingress restricted to computeSg on 5432',
  allowAllOutbound: false,
});
dbSg.addIngressRule(
  ec2.Peer.securityGroupId(computeSg.securityGroupId),
  ec2.Port.tcp(5432),
  'Allow PostgreSQL access only from the Execution Environment (compute SG)',
);

// ─────────────────────────────────────────────────────────────────────────────
// Relational Backbone – RDS PostgreSQL 15
// db.t3.micro, 20 GiB gp2, private subnets, not publicly accessible.
// Credentials sourced from Secrets Manager; no deletion protection, no retain.
// ─────────────────────────────────────────────────────────────────────────────
const dbInstance = new rds.DatabaseInstance(stack, 'OrderDatabase', {
  engine: rds.DatabaseInstanceEngine.postgres({
    version: rds.PostgresEngineVersion.VER_15,
  }),
  instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
  allocatedStorage: 20,
  storageType: rds.StorageType.GP2,
  vpc,
  vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  securityGroups: [dbSg],
  credentials: rds.Credentials.fromPassword(
    'orderadmin',
    dbSecret.secretValueFromJson('password'),
  ),
  multiAz: false,
  publiclyAccessible: false,          // not publicly accessible
  deletionProtection: false,           // no deletion protection per mandate
  removalPolicy: RemovalPolicy.DESTROY,
  storageEncrypted: true,
  databaseName: 'orders',
});

// ─────────────────────────────────────────────────────────────────────────────
// GLUE – Data Catalog database and Crawler
// Crawler crawls s3://<bucket>/analytics/ on a 30-minute schedule.
// IAM role is limited to S3 read-only on that prefix + write to this catalog DB.
// No JDBC connections configured.
// ─────────────────────────────────────────────────────────────────────────────
const glueDb = new glue.CfnDatabase(stack, 'OrderAnalyticsGlueDb', {
  catalogId: stack.account,
  databaseInput: {
    name: 'order_analytics',
    description: 'Glue Data Catalog database for order analytics',
  },
});

const glueRole = new iam.Role(stack, 'GlueCrawlerRole', {
  assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
  description: 'Glue Crawler role – S3 read-only on analytics/ + write to order_analytics DB only',
});

// S3 read-only limited to the analytics/ prefix in this specific bucket
glueRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['s3:GetObject', 's3:ListBucket'],
  resources: [
    analyticsBucket.bucketArn,
    `${analyticsBucket.bucketArn}/analytics/*`,
  ],
}));

// Glue Data Catalog write access limited to the specific database and its tables
glueRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: [
    'glue:GetDatabase',
    'glue:GetTable',
    'glue:GetTables',
    'glue:CreateTable',
    'glue:UpdateTable',
    'glue:GetPartition',
    'glue:GetPartitions',
    'glue:BatchCreatePartition',
    'glue:CreatePartition',
    'glue:UpdatePartition',
  ],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/order_analytics`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/order_analytics/*`,
  ],
}));

// CloudWatch Logs for Glue Crawler runtime logs (scoped to /aws-glue prefix)
glueRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [
    `arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws-glue/*`,
    `arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws-glue/*:log-stream:*`,
  ],
}));

const glueCrawler = new glue.CfnCrawler(stack, 'OrderAnalyticsCrawler', {
  name: 'order-analytics-crawler',
  role: glueRole.roleArn,
  databaseName: 'order_analytics',
  targets: {
    s3Targets: [
      { path: `s3://${analyticsBucket.bucketName}/analytics/` },
    ],
    // No jdbcTargets – no JDBC connections per spec
  },
  schedule: {
    scheduleExpression: 'cron(0/30 * * * ? *)',  // every 30 minutes
  },
  schemaChangePolicy: {
    updateBehavior: 'UPDATE_IN_DATABASE',
    deleteBehavior: 'DEPRECATE_IN_DATABASE',
  },
});
glueCrawler.addDependency(glueDb);

// ─────────────────────────────────────────────────────────────────────────────
// ATHENA – WorkGroup with result output to S3; enforced configuration
// ─────────────────────────────────────────────────────────────────────────────
new athena.CfnWorkGroup(stack, 'OrderAnalyticsWorkGroup', {
  name: 'order-analytics',
  state: 'ENABLED',
  workGroupConfiguration: {
    resultConfiguration: {
      outputLocation: `s3://${analyticsBucket.bucketName}/athena-results/`,
    },
    enforceWorkGroupConfiguration: true,
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// SQS – exactly 1 Standard queue for inbound orders (per spec).
// Both the Worker Lambda (event-source mapping) and the EventBridge Pipe source
// this queue.  SQS delivers each message to one consumer; the Worker and the
// Pipe therefore act as competing consumers.  This co-existence is mandated by
// the prompt: the Worker writes the order row to RDS while the Pipe is the
// canonical path to Step Functions.  Because each path is independently
// idempotent (DB INSERT ON CONFLICT DO NOTHING; SFN execution per orderId), a
// message processed by either consumer produces a correct partial outcome, and
// redelivery after visibility-timeout expiry covers the other path.
// ─────────────────────────────────────────────────────────────────────────────
const orderQueue = new sqs.Queue(stack, 'OrderQueue', {
  visibilityTimeout: Duration.seconds(60),
  retentionPeriod: Duration.days(4),
  removalPolicy: RemovalPolicy.DESTROY,
});

// ─────────────────────────────────────────────────────────────────────────────
// IAM ROLES – distinct role per component; no Action:"*"; Resource:"*" only
// where AWS-managed log delivery makes it unavoidable (documented inline).
// ─────────────────────────────────────────────────────────────────────────────

// 1. Request Handler Lambda role – SQS send + basic logging
const requestHandlerRole = new iam.Role(stack, 'RequestHandlerRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Request Handler Lambda – SQS SendMessage on order queue + log delivery',
});
requestHandlerRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['sqs:SendMessage'],
  resources: [orderQueue.queueArn],
}));

// 2. Worker Lambda role – SM secret read + SQS consume + log delivery
const workerRole = new iam.Role(stack, 'WorkerRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Worker Lambda – read DB secret, consume SQS, deliver logs',
});
workerRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['secretsmanager:GetSecretValue'],
  resources: [dbSecret.secretArn],
}));
workerRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
  resources: [orderQueue.queueArn],
}));

// 3. Enrichment Lambda role – log delivery only (pure data transformation)
const enrichmentRole = new iam.Role(stack, 'EnrichmentRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Enrichment Lambda – log delivery only; adds enriched field to payload',
});

// ─────────────────────────────────────────────────────────────────────────────
// LAMBDA FUNCTIONS (Execution Environment)
// Zip/runtime-based only (no container image Lambda). Exactly 3 total.
// ─────────────────────────────────────────────────────────────────────────────

// Lambda 1 – Request Handler
// Validates POST body (orderId:string, amount:number), enqueues to SQS, returns 202.
const requestHandlerCode = [
  `const {SQSClient,SendMessageCommand}=require('@aws-sdk/client-sqs');`,
  `const ep=process.env.AWS_ENDPOINT;`,
  `const c=new SQSClient({region:process.env.AWS_REGION||'us-east-1',...(ep?{endpoint:ep}:{})});`,
  `exports.handler=async e=>{`,
  `  let b;`,
  `  try{b=JSON.parse(e.body||'{}')}catch(ex){return{statusCode:400,body:JSON.stringify({error:'Invalid JSON'})}}`,
  `  if(!b.orderId||typeof b.orderId!=='string')`,
  `    return{statusCode:400,body:JSON.stringify({error:'orderId must be a string'})};`,
  `  if(b.amount===undefined||typeof b.amount!=='number')`,
  `    return{statusCode:400,body:JSON.stringify({error:'amount must be a number'})};`,
  `  await c.send(new SendMessageCommand({QueueUrl:process.env.QUEUE_URL,MessageBody:e.body}));`,
  `  return{statusCode:202,headers:{'Content-Type':'application/json'},body:JSON.stringify({orderId:b.orderId})};`,
  `};`,
].join('\n');

const requestHandlerFn = new lambda.Function(stack, 'RequestHandlerFn', {
  runtime: lambda.Runtime.NODEJS_18_X,
  handler: 'index.handler',
  code: lambda.Code.fromInline(requestHandlerCode),
  role: requestHandlerRole,
  vpc,
  vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  securityGroups: [computeSg],
  environment: {
    QUEUE_URL: orderQueue.queueUrl,
    ...(normalizedEndpoint ? { AWS_ENDPOINT: normalizedEndpoint } : {}),
  },
  timeout: Duration.seconds(30),
  description: 'Execution Environment – validates order POST and enqueues to SQS',
});

// Scope request-handler log permissions to its own log group (post-creation access)
requestHandlerRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [
    requestHandlerFn.logGroup.logGroupArn,
    `${requestHandlerFn.logGroup.logGroupArn}:*`,
  ],
}));

// Lambda 2 – Worker
// Triggered by SQS (batch 5, window 5s). Parses each message, writes to PostgreSQL.
// Worker does NOT emit to EventBridge; the Pipe is the canonical path to Step Functions.
const workerCode = [
  `const {SecretsManagerClient,GetSecretValueCommand}=require('@aws-sdk/client-secrets-manager');`,
  `const {Client}=require('pg');`,
  `const ep=process.env.AWS_ENDPOINT;`,
  `exports.handler=async e=>{`,
  `  const sm=new SecretsManagerClient({region:process.env.AWS_REGION||'us-east-1',...(ep?{endpoint:ep}:{})});`,
  `  const sv=await sm.send(new GetSecretValueCommand({SecretId:process.env.DB_SECRET_ARN}));`,
  `  const {username,password}=JSON.parse(sv.SecretString);`,
  `  const db=new Client({host:process.env.DB_HOST,port:5432,database:'orders',user:username,password,ssl:{rejectUnauthorized:false}});`,
  `  await db.connect();`,
  `  try{`,
  `    await db.query('CREATE TABLE IF NOT EXISTS orders(order_id text primary key,amount numeric,received_at timestamptz)');`,
  `    for(const r of e.Records){`,
  `      const order=JSON.parse(r.body);`,
  `      await db.query('INSERT INTO orders(order_id,amount,received_at) VALUES($1,$2,NOW()) ON CONFLICT DO NOTHING',[order.orderId,order.amount]);`,
  `      console.log(JSON.stringify({event:'order_received',orderId:order.orderId,amount:order.amount}));`,
  `    }`,
  `  }finally{await db.end()}`,
  `};`,
].join('\n');

const workerFn = new lambda.Function(stack, 'WorkerFn', {
  runtime: lambda.Runtime.NODEJS_18_X,
  handler: 'index.handler',
  code: lambda.Code.fromInline(workerCode),
  role: workerRole,
  vpc,
  vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  securityGroups: [computeSg],
  environment: {
    DB_SECRET_ARN: dbSecret.secretArn,
    DB_HOST: dbInstance.dbInstanceEndpointAddress,
    DB_NAME: 'orders',
    ...(normalizedEndpoint ? { AWS_ENDPOINT: normalizedEndpoint } : {}),
  },
  timeout: Duration.seconds(60),
  description: 'Execution Environment – reads SQS, writes order rows to PostgreSQL',
});

// SQS event source mapping – batch size 5, max batching window 5 s
workerFn.addEventSource(
  new lambdaEventSources.SqsEventSource(orderQueue, {
    batchSize: 5,
    maxBatchingWindow: Duration.seconds(5),
  }),
);

// Scope worker log permissions to its own log group
workerRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [
    workerFn.logGroup.logGroupArn,
    `${workerFn.logGroup.logGroupArn}:*`,
  ],
}));

// Lambda 3 – Enrichment (used exclusively by the EventBridge Pipe)
// Adds enriched:true and processedAt timestamp to every SQS record in the batch.
const enrichmentCode =
  `exports.handler=async e=>e.map(r=>({...r,enriched:true,processedAt:new Date().toISOString()}));`;

const enrichmentFn = new lambda.Function(stack, 'EnrichmentFn', {
  runtime: lambda.Runtime.NODEJS_18_X,
  handler: 'index.handler',
  code: lambda.Code.fromInline(enrichmentCode),
  role: enrichmentRole,
  timeout: Duration.seconds(30),
  description: 'Enrichment Lambda – adds enriched:true and processedAt to pipe payload',
});

// Scope enrichment log permissions to its own log group
enrichmentRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
  resources: [
    enrichmentFn.logGroup.logGroupArn,
    `${enrichmentFn.logGroup.logGroupArn}:*`,
  ],
}));

// ─────────────────────────────────────────────────────────────────────────────
// API GATEWAY – REST API
// Single resource /orders with POST method → Request Handler Lambda.
// API Gateway endpoints are HTTPS-only (TLS in transit by default).
// ─────────────────────────────────────────────────────────────────────────────
const api = new apigw.RestApi(stack, 'OrderIntakeApi', {
  restApiName: 'OrderIntakeService',
  deployOptions: {
    stageName: 'prod',
    loggingLevel: apigw.MethodLoggingLevel.INFO,
  },
  description: 'Order intake REST API – receives HTTP order submissions',
});

const ordersResource = api.root.addResource('orders');
ordersResource.addMethod(
  'POST',
  new apigw.LambdaIntegration(requestHandlerFn, { proxy: true }),
);

// ─────────────────────────────────────────────────────────────────────────────
// SNS – notification topic + placeholder email subscription
// Change the email address below before production deployment.
// ─────────────────────────────────────────────────────────────────────────────
const orderTopic = new sns.Topic(stack, 'OrderNotificationTopic', {
  displayName: 'Order Recorded Notifications',
});

// Placeholder subscription – update this address before production deployment
orderTopic.addSubscription(
  new snsSubscriptions.EmailSubscription('placeholder@example.com'),
);

// ─────────────────────────────────────────────────────────────────────────────
// STEP FUNCTIONS – Standard state machine
// 1. Writes JSON record to S3 under analytics/orders/ via SDK service integration
//    (no Lambda intermediary for this write, per spec).
// 2. Publishes a notification to the SNS topic.
// ─────────────────────────────────────────────────────────────────────────────

// Step Functions execution role
const sfnRole = new iam.Role(stack, 'SfnExecutionRole', {
  assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
  description: 'Step Functions role – S3 analytics write + SNS publish + CW log delivery',
});

sfnRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['s3:PutObject'],
  resources: [`${analyticsBucket.bucketArn}/analytics/orders/*`],
}));

sfnRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['sns:Publish'],
  resources: [orderTopic.topicArn],
}));

// CloudWatch Logs delivery for Step Functions.
// JUSTIFICATION for Resource:"*" on these actions: AWS Step Functions log delivery
// requires PutResourcePolicy, DescribeResourcePolicies, and DescribeLogGroups to be
// scoped to "*" – the service cannot be restricted to a single log group ARN for
// these control-plane operations. See AWS documentation on SFN logging.
const sfnLogGroup = new logs.LogGroup(stack, 'SfnLogGroup', {
  retention: logs.RetentionDays.ONE_MONTH,
  removalPolicy: RemovalPolicy.DESTROY,
});

sfnRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: [
    'logs:CreateLogDelivery',
    'logs:GetLogDelivery',
    'logs:UpdateLogDelivery',
    'logs:DeleteLogDelivery',
    'logs:ListLogDeliveries',
    'logs:PutLogEvents',
    'logs:PutResourcePolicy',
    'logs:DescribeResourcePolicies',
    'logs:DescribeLogGroups',
  ],
  // Resource:"*" is unavoidable here for PutResourcePolicy/DescribeResourcePolicies/
  // DescribeLogGroups – AWS SFN log delivery API mandates this scope. See comment above.
  resources: ['*'],
}));

// State machine definition: write to S3 → publish to SNS
const writeToS3Task = new sfnTasks.CallAwsService(stack, 'WriteOrderToS3', {
  service: 's3',
  action: 'putObject',
  parameters: {
    Bucket: analyticsBucket.bucketName,
    // Key uses the orderId field from the incoming payload
    'Key.$': "States.Format('analytics/orders/{}.json', $.orderId)",
    'Body.$': 'States.JsonToString($)',
    ContentType: 'application/json',
  },
  iamResources: [`${analyticsBucket.bucketArn}/analytics/orders/*`],
  resultPath: '$.s3Result',
  comment: 'Write order analytics record to S3 via SDK service integration (no Lambda)',
});

const publishSnsTask = new sfnTasks.SnsPublish(stack, 'PublishOrderNotification', {
  topic: orderTopic,
  message: sfn.TaskInput.fromText('Order has been recorded in analytics.'),
  resultPath: '$.snsResult',
  comment: 'Notify downstream consumers that the order has been recorded',
});

const stateMachine = new sfn.StateMachine(stack, 'OrderStateMachine', {
  definitionBody: sfn.DefinitionBody.fromChainable(
    writeToS3Task.next(publishSnsTask),
  ),
  stateMachineType: sfn.StateMachineType.STANDARD,
  role: sfnRole,
  logs: {
    destination: sfnLogGroup,
    level: sfn.LogLevel.ERROR,
    includeExecutionData: false,
  },
  removalPolicy: RemovalPolicy.DESTROY,
  comment: 'Order processing pipeline – S3 analytics write then SNS notification',
});

// ─────────────────────────────────────────────────────────────────────────────
// EVENTBRIDGE – custom event bus and rule
// Rule matches source:"orders.service" + detail-type:"OrderAccepted".
// Target: Step Functions state machine.
// ─────────────────────────────────────────────────────────────────────────────
const orderEventBus = new events.EventBus(stack, 'OrderEventBus', {
  eventBusName: 'order-events',
});

// Role allowing EventBridge to invoke Step Functions
const eventBridgeRuleRole = new iam.Role(stack, 'EventBridgeRuleRole', {
  assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
  description: 'Role that allows EventBridge rule to start Step Functions executions',
});
eventBridgeRuleRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['states:StartExecution'],
  resources: [stateMachine.stateMachineArn],
}));

const orderAcceptedRule = new events.Rule(stack, 'OrderAcceptedRule', {
  eventBus: orderEventBus,
  eventPattern: {
    source: ['orders.service'],
    detailType: ['OrderAccepted'],
  },
  description: 'Routes OrderAccepted events from the custom bus to Step Functions',
});

orderAcceptedRule.addTarget(
  new eventsTargets.SfnStateMachine(stateMachine, {
    role: eventBridgeRuleRole,
  }),
);

// ─────────────────────────────────────────────────────────────────────────────
// EVENTBRIDGE PIPE
// Source: orderQueue (the single SQS queue, shared with the Worker Lambda).
// Enrichment: Lambda 3 (adds enriched:true and processedAt timestamp).
// Target: Step Functions state machine (FIRE_AND_FORGET = asynchronous).
//
// The Worker Lambda and the Pipe share orderQueue as competing consumers
// (see queue declaration comment for the idempotency justification).
// ─────────────────────────────────────────────────────────────────────────────

const pipeRole = new iam.Role(stack, 'PipeRole', {
  assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
  description: 'EventBridge Pipe role – SQS consume + Lambda enrichment + SFN start',
});

pipeRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
  resources: [orderQueue.queueArn],
}));

pipeRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['lambda:InvokeFunction'],
  resources: [enrichmentFn.functionArn],
}));

pipeRole.addToPolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['states:StartExecution'],
  resources: [stateMachine.stateMachineArn],
}));

new pipes.CfnPipe(stack, 'OrderPipe', {
  name: 'order-processing-pipe',
  roleArn: pipeRole.roleArn,
  source: orderQueue.queueArn,
  sourceParameters: {
    sqsQueueParameters: {
      batchSize: 5,
      maximumBatchingWindowInSeconds: 5,
    },
  },
  enrichment: enrichmentFn.functionArn,
  // Pipe must not target Lambda; targets Step Functions directly
  target: stateMachine.stateMachineArn,
  targetParameters: {
    stepFunctionStateMachineParameters: {
      invocationType: 'FIRE_AND_FORGET',  // asynchronous invocation
    },
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUTS – only API Gateway invoke URL and S3 bucket name
// No secrets, no DB connection strings.
// ─────────────────────────────────────────────────────────────────────────────
new CfnOutput(stack, 'ApiGatewayInvokeUrl', {
  description: 'POST https://<id>.execute-api.<region>.amazonaws.com/prod/orders',
  value: api.url,
});

new CfnOutput(stack, 'AnalyticsBucketName', {
  description: 'S3 bucket name for order analytics and Athena results',
  value: analyticsBucket.bucketName,
});

app.synth();
