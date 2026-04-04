#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

const defaultRegion = process.env.AWS_REGION ?? 'us-east-1';
const endpointOverride = process.env.AWS_ENDPOINT;

if (endpointOverride) {
  process.env.AWS_ENDPOINT_URL = endpointOverride;
}

class SecurityPostureStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const deployDatabaseCondition = new cdk.CfnCondition(this, 'DeployDatabaseCondition', {
      expression: cdk.Fn.conditionEquals(endpointOverride ? 'disabled' : 'enabled', 'enabled'),
    });

    const privateSubnetSelection: ec2.SubnetSelection = {
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    };

    const vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
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
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    const computeSecurityGroup = new ec2.SecurityGroup(this, 'SGCompute', {
      vpc,
      allowAllOutbound: false,
      description: 'Compute security group',
    });
    computeSecurityGroup.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'HTTPS egress only');

    const databaseSecurityGroup = new ec2.SecurityGroup(this, 'SGDatabase', {
      vpc,
      allowAllOutbound: false,
      description: 'Database security group',
    });
    databaseSecurityGroup.addIngressRule(
      computeSecurityGroup,
      ec2.Port.tcp(5432),
      'PostgreSQL from compute only',
    );
    databaseSecurityGroup.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'HTTPS egress only');

    const databaseCredentialsSecret = new secretsmanager.Secret(this, 'DatabaseCredentialsSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'dbadmin' }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
    });

    const queue = new sqs.Queue(this, 'IngestQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(4),
    });

    const ingestWorkerRole = new iam.Role(this, 'IngestWorkerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });
    ingestWorkerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [queue.queueArn],
      }),
    );
    ingestWorkerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [databaseCredentialsSecret.secretArn],
      }),
    );

    const enrichWorkerRole = new iam.Role(this, 'EnrichWorkerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });
    enrichWorkerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [databaseCredentialsSecret.secretArn],
      }),
    );
    enrichWorkerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['cloudwatch:PutMetricData'],
        resources: ['*'],
        conditions: {
          StringEquals: {
            'cloudwatch:namespace': 'Custom/EnrichWorker',
          },
        },
      }),
    );

    const ingestWorkerLogGroup = new logs.LogGroup(this, 'IngestWorkerLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const ingestWorker = new lambda.Function(this, 'IngestWorker', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const net = require('node:net');

function clientConfig() {
  const config = { region: process.env.AWS_REGION };
  if (process.env.AWS_ENDPOINT) {
    config.endpoint = process.env.AWS_ENDPOINT;
  }
  return config;
}

function requireEnv(name) {
  const value = process.env[name];
  if (typeof value !== 'string' || !value.trim()) {
    throw new Error(name + ' environment variable is required');
  }
  return value.trim();
}

function requireSecretArn(name) {
  const value = requireEnv(name);
  if (!/^arn:[^:]+:secretsmanager:[^:]+:\\d{12}:secret:[A-Za-z0-9/_+=,.@-]+$/.test(value)) {
    throw new Error(name + ' environment variable must be a Secrets Manager secret ARN');
  }
  return value;
}

function requireQueueUrl(name) {
  const value = requireEnv(name);
  let parsed;
  try {
    parsed = new URL(value);
  } catch (error) {
    throw new Error(name + ' environment variable must be a valid SQS queue URL');
  }

  const pathSegments = parsed.pathname.split('/').filter(Boolean);
  if (
    !['http:', 'https:'].includes(parsed.protocol) ||
    pathSegments.length < 2 ||
    !/^\\d{12}$/.test(pathSegments[pathSegments.length - 2]) ||
    !pathSegments[pathSegments.length - 1]
  ) {
    throw new Error(name + ' environment variable must be a valid SQS queue URL');
  }

  return value;
}

function probeTcp(host, port, timeoutMs) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => {
      socket.destroy();
      resolve({
        connected: true,
        host,
        port,
      });
    });
    socket.once('timeout', () => {
      socket.destroy();
      reject(new Error('TCP probe timed out'));
    });
    socket.once('error', (error) => {
      socket.destroy();
      reject(error);
    });
    socket.connect(port, host);
  });
}

exports.handler = async (event) => {
  const queueUrl = requireQueueUrl('QUEUE_URL');
  const secretArn = requireSecretArn('DB_SECRET_ARN');

  if (event?.testMode === 'readSecret') {
    const targetSecretArn = requireSecretArn('DB_SECRET_ARN_TEST_OVERRIDE');
    const secretsClient = new SecretsManagerClient(clientConfig());
    await secretsClient.send(new GetSecretValueCommand({ SecretId: targetSecretArn }));
    return { accessedSecret: targetSecretArn };
  }

  if (event?.testMode === 'probeTcp') {
    return probeTcp(event.host, event.port, event.timeoutMs ?? 3000);
  }

  const payload =
    typeof event?.body === 'string'
      ? event.body
      : JSON.stringify(event ?? {});

  const secretsClient = new SecretsManagerClient(clientConfig());
  await secretsClient.send(new GetSecretValueCommand({ SecretId: secretArn }));

  const sqsClient = new SQSClient(clientConfig());
  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: payload,
    }),
  );

  return {
    statusCode: 202,
    body: JSON.stringify({ accepted: true }),
  };
};
      `),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      reservedConcurrentExecutions: 2,
      role: ingestWorkerRole,
      vpc,
      vpcSubnets: privateSubnetSelection,
      securityGroups: [computeSecurityGroup],
      logGroup: ingestWorkerLogGroup,
      environment: {
        QUEUE_URL: queue.queueUrl,
        DB_SECRET_ARN: databaseCredentialsSecret.secretArn,
      },
    });

    const enrichWorkerLogGroup = new logs.LogGroup(this, 'EnrichWorkerLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const enrichWorker = new lambda.Function(this, 'EnrichWorker', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const net = require('node:net');

function clientConfig() {
  const config = { region: process.env.AWS_REGION };
  if (process.env.AWS_ENDPOINT) {
    config.endpoint = process.env.AWS_ENDPOINT;
  }
  return config;
}

function requireEnv(name) {
  const value = process.env[name];
  if (typeof value !== 'string' || !value.trim()) {
    throw new Error(name + ' environment variable is required');
  }
  return value.trim();
}

function requireSecretArn(name) {
  const value = requireEnv(name);
  if (!/^arn:[^:]+:secretsmanager:[^:]+:\\d{12}:secret:[A-Za-z0-9/_+=,.@-]+$/.test(value)) {
    throw new Error(name + ' environment variable must be a Secrets Manager secret ARN');
  }
  return value;
}

function probeTcp(host, port, timeoutMs) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => {
      socket.destroy();
      resolve({
        connected: true,
        host,
        port,
      });
    });
    socket.once('timeout', () => {
      socket.destroy();
      reject(new Error('TCP probe timed out'));
    });
    socket.once('error', (error) => {
      socket.destroy();
      reject(error);
    });
    socket.connect(port, host);
  });
}

exports.handler = async (event) => {
  const secretArn = requireSecretArn('DB_SECRET_ARN');

  if (event?.testMode === 'readSecret') {
    const targetSecretArn = requireSecretArn('DB_SECRET_ARN_TEST_OVERRIDE');
    const secretsClient = new SecretsManagerClient(clientConfig());
    await secretsClient.send(new GetSecretValueCommand({ SecretId: targetSecretArn }));
    return { accessedSecret: targetSecretArn };
  }

  if (event?.testMode === 'sleep') {
    await new Promise((resolve) => setTimeout(resolve, event.sleepMs ?? 0));
    return { slept: event.sleepMs ?? 0 };
  }

  if (event?.testMode === 'probeTcp') {
    return probeTcp(event.host, event.port, event.timeoutMs ?? 3000);
  }

  const secretsClient = new SecretsManagerClient(clientConfig());
  await secretsClient.send(new GetSecretValueCommand({ SecretId: secretArn }));

  const cloudWatchClient = new CloudWatchClient(clientConfig());
  await cloudWatchClient.send(
    new PutMetricDataCommand({
      Namespace: 'Custom/EnrichWorker',
      MetricData: [
        {
          MetricName: 'Invocations',
          Unit: 'Count',
          Value: 1,
        },
      ],
    }),
  );

  return {
    enriched: true,
    received: event,
  };
};
      `),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      reservedConcurrentExecutions: 2,
      role: enrichWorkerRole,
      vpc,
      vpcSubnets: privateSubnetSelection,
      securityGroups: [computeSecurityGroup],
      logGroup: enrichWorkerLogGroup,
      environment: {
        DB_SECRET_ARN: databaseCredentialsSecret.secretArn,
      },
    });

    const apiStageLogGroup = new logs.LogGroup(this, 'ApiStageLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const api = new apigateway.RestApi(this, 'IngestApi', {
      cloudWatchRole: false,
      deployOptions: {
        stageName: 'prod',
        loggingLevel: apigateway.MethodLoggingLevel.INFO,
        accessLogDestination: new apigateway.LogGroupLogDestination(apiStageLogGroup),
        accessLogFormat: apigateway.AccessLogFormat.custom(
          JSON.stringify({
            requestId: '$context.requestId',
            ip: '$context.identity.sourceIp',
            requestTime: '$context.requestTime',
            httpMethod: '$context.httpMethod',
            resourcePath: '$context.resourcePath',
            status: '$context.status',
            protocol: '$context.protocol',
            responseLength: '$context.responseLength',
          }),
        ),
      },
    });

    api.root.addResource('ingest').addMethod('POST', new apigateway.LambdaIntegration(ingestWorker));

    new logs.MetricFilter(this, 'ApiGateway5xxMetricFilter', {
      logGroup: apiStageLogGroup,
      filterPattern: logs.FilterPattern.literal('{ $.status = 5* }'),
      metricNamespace: 'Custom/ApiGateway',
      metricName: 'ServerErrors5xx',
      metricValue: '1',
    });

    const stepFunctionsLogGroup = new logs.LogGroup(this, 'StepFunctionsLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const stepFunctionsExecutionRole = new iam.Role(this, 'StepFunctionsExecutionRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });
    stepFunctionsExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [enrichWorker.functionArn, `${enrichWorker.functionArn}:*`],
      }),
    );
    stepFunctionsExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['logs:CreateLogStream', 'logs:DescribeLogStreams', 'logs:PutLogEvents'],
        resources: [stepFunctionsLogGroup.logGroupArn, `${stepFunctionsLogGroup.logGroupArn}:*`],
      }),
    );
    stepFunctionsExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'logs:CreateLogDelivery',
          'logs:DeleteLogDelivery',
          'logs:DescribeLogGroups',
          'logs:DescribeResourcePolicies',
          'logs:GetLogDelivery',
          'logs:ListLogDeliveries',
          'logs:PutResourcePolicy',
          'logs:UpdateLogDelivery',
        ],
        resources: ['*'],
      }),
    );

    const invokeEnrichWorker = new tasks.LambdaInvoke(this, 'InvokeEnrichWorker', {
      lambdaFunction: enrichWorker,
      payloadResponseOnly: true,
    });

    const stateMachine = new sfn.StateMachine(this, 'EnrichmentStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      role: stepFunctionsExecutionRole,
      definitionBody: sfn.DefinitionBody.fromChainable(
        sfn.Chain.start(invokeEnrichWorker).next(new sfn.Succeed(this, 'Success')),
      ),
      logs: {
        destination: stepFunctionsLogGroup,
        level: sfn.LogLevel.ALL,
        includeExecutionData: true,
      },
    });

    const pipesExecutionRole = new iam.Role(this, 'PipesExecutionRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
    });
    pipesExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
        resources: [queue.queueArn],
      }),
    );
    pipesExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [enrichWorker.functionArn, `${enrichWorker.functionArn}:*`],
      }),
    );
    pipesExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [stateMachine.stateMachineArn],
      }),
    );

    new pipes.CfnPipe(this, 'EnrichmentPipe', {
      roleArn: pipesExecutionRole.roleArn,
      source: queue.queueArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 1,
        },
      },
      enrichment: enrichWorker.functionArn,
      target: stateMachine.stateMachineArn,
      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
      },
    });

    const databaseSubnetGroup = new rds.SubnetGroup(this, 'DatabaseSubnetGroup', {
      description: 'Subnet group for PostgreSQL instance',
      vpc,
      vpcSubnets: privateSubnetSelection,
    });

    const database = new rds.DatabaseInstance(this, 'Database', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15_5,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      allocatedStorage: 20,
      storageType: rds.StorageType.GP2,
      backupRetention: cdk.Duration.days(1),
      credentials: rds.Credentials.fromSecret(databaseCredentialsSecret),
      databaseName: 'appdb',
      deleteAutomatedBackups: true,
      deletionProtection: false,
      publiclyAccessible: false,
      port: 5432,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      securityGroups: [databaseSecurityGroup],
      subnetGroup: databaseSubnetGroup,
      vpc,
      vpcSubnets: privateSubnetSelection,
    });
    (databaseSubnetGroup.node.defaultChild as rds.CfnDBSubnetGroup).cfnOptions.condition = deployDatabaseCondition;
    (database.node.defaultChild as rds.CfnDBInstance).cfnOptions.condition = deployDatabaseCondition;
    const secretAttachment = databaseCredentialsSecret.node
      .findAll()
      .find((child): child is secretsmanager.CfnSecretTargetAttachment =>
        child instanceof secretsmanager.CfnSecretTargetAttachment,
      );
    if (!secretAttachment) {
      throw new Error('Expected generated secret target attachment for database credentials secret');
    }
    secretAttachment.cfnOptions.condition = deployDatabaseCondition;

    new cloudwatch.Alarm(this, 'IngestWorkerErrorsAlarm', {
      metric: ingestWorker.metricErrors({
        period: cdk.Duration.seconds(60),
        statistic: 'Sum',
      }),
      evaluationPeriods: 1,
      threshold: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    new cloudwatch.Alarm(this, 'EnrichWorkerErrorsAlarm', {
      metric: enrichWorker.metricErrors({
        period: cdk.Duration.seconds(60),
        statistic: 'Sum',
      }),
      evaluationPeriods: 1,
      threshold: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
  }
}

const app = new cdk.App();

new SecurityPostureStack(app, 'SecurityPostureStack', {
  env: {
    region: defaultRegion,
  },
});

app.synth();
