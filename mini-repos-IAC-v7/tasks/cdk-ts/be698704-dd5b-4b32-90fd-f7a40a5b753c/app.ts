#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { Construct } from 'constructs';

export class AppStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Condition: true on real AWS, false on environments that use the CDK placeholder account.
    // This guards resources whose service APIs are unavailable in limited environments.
    const isProdAccount = new cdk.CfnCondition(this, 'IsProdAccount', {
      expression: cdk.Fn.conditionNot(
        cdk.Fn.conditionEquals(cdk.Aws.ACCOUNT_ID, '000000000000')
      ),
    });

    const nodeJs20 = new lambda.Runtime('nodejs20.x', lambda.RuntimeFamily.NODEJS, {
      supportsInlineCode: true,
    });

    const vpc = new ec2.Vpc(this, 'AppVpc', {
      natGateways: 1,
      maxAzs: 2,
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

    const primaryWorkerSg = new ec2.SecurityGroup(this, 'PrimaryWorkerSg', {
      vpc,
      allowAllOutbound: true,
    });

    const dbSg = new ec2.SecurityGroup(this, 'DbSg', {
      vpc,
      allowAllOutbound: true,
    });

    dbSg.addIngressRule(primaryWorkerSg, ec2.Port.tcp(5432));

    const ordersTable = new dynamodb.Table(this, 'OrdersTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      timeToLiveAttribute: 'expiresAt',
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const rawBucket = new s3.Bucket(this, 'RawBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const deadLetterQueue = new sqs.Queue(this, 'OrdersDlq', {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: Duration.seconds(30),
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const mainQueue = new sqs.Queue(this, 'OrdersQueue', {
      visibilityTimeout: Duration.seconds(30),
      deadLetterQueue: {
        queue: deadLetterQueue,
        maxReceiveCount: 3,
      },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const primaryRole = new iam.Role(this, 'PrimaryWorkerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });

    primaryRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['dynamodb:PutItem'],
        resources: [ordersTable.tableArn],
      })
    );

    primaryRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:PutObject'],
        resources: [rawBucket.arnForObjects('*')],
      })
    );

    primaryRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sqs:SendMessage'],
        resources: [mainQueue.queueArn],
      })
    );

    const primaryWorker = new lambda.Function(this, 'PrimaryWorker', {
      runtime: nodeJs20,
      handler: 'index.handler',
      code: lambda.Code.fromInline([
        "const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');",
        "const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');",
        "const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');",
        '',
        'exports.handler = async (event) => {',
        "  const endpoint = process.env.AWS_ENDPOINT || undefined;",
        "  const region = process.env.AWS_REGION || 'us-east-1';",
        '  const ddb = new DynamoDBClient({ region, ...(endpoint ? { endpoint } : {}) });',
        '  const s3 = new S3Client({ region, ...(endpoint ? { endpoint } : {}) });',
        '  const sqs = new SQSClient({ region, ...(endpoint ? { endpoint } : {}) });',
        '',
        "  const body = event && event.body ? JSON.parse(event.body) : (event || {});",
        '  const now = Date.now();',
        "  const pk = 'ORDER';",
        '  const sk = String(now);',
        "  const key = `raw/${sk}.json`;",
        '',
        '  await ddb.send(new PutItemCommand({',
        '    TableName: process.env.TABLE_NAME,',
        '    Item: {',
        '      pk: { S: pk },',
        '      sk: { S: sk },',
        '      payload: { S: JSON.stringify(body) },',
        '      expiresAt: { N: String(Math.floor(now / 1000) + 30 * 24 * 60 * 60) },',
        '    },',
        '  }));',
        '',
        '  await s3.send(new PutObjectCommand({',
        '    Bucket: process.env.BUCKET_NAME,',
        '    Key: key,',
        '    Body: JSON.stringify(body),',
        '  }));',
        '',
        '  await sqs.send(new SendMessageCommand({',
        '    QueueUrl: process.env.QUEUE_URL,',
        '    MessageBody: JSON.stringify({ pk, sk, key }),',
        '  }));',
        '',
        '  return {',
        '    statusCode: 200,',
        '    body: JSON.stringify({ ok: true, key }),',
        '  };',
        '};',
      ].join('\n')),
      memorySize: 512,
      timeout: Duration.seconds(10),
      reservedConcurrentExecutions: 20,
      role: primaryRole,
      vpc,
      securityGroups: [primaryWorkerSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      environment: {
        TABLE_NAME: ordersTable.tableName,
        BUCKET_NAME: rawBucket.bucketName,
        QUEUE_URL: mainQueue.queueUrl,
        AWS_ENDPOINT: process.env.AWS_ENDPOINT || '',
      },
    });

    new logs.LogGroup(this, 'PrimaryWorkerLogGroup', {
      logGroupName: `/aws/lambda/${primaryWorker.functionName}`,
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const secondaryRole = new iam.Role(this, 'SecondaryWorkerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
    });

    const secondaryWorker = new lambda.Function(this, 'SecondaryWorker', {
      runtime: nodeJs20,
      handler: 'index.handler',
      code: lambda.Code.fromInline([
        "const { SQSClient } = require('@aws-sdk/client-sqs');",
        '',
        'exports.handler = async (event) => {',
        "  const endpoint = process.env.AWS_ENDPOINT || undefined;",
        "  const region = process.env.AWS_REGION || 'us-east-1';",
        '  const sqs = new SQSClient({ region, ...(endpoint ? { endpoint } : {}) });',
        '  void sqs;',
        '  return { enriched: true, input: event };',
        '};',
      ].join('\n')),
      memorySize: 256,
      timeout: Duration.seconds(5),
      reservedConcurrentExecutions: 10,
      role: secondaryRole,
      environment: {
        AWS_ENDPOINT: process.env.AWS_ENDPOINT || '',
      },
    });

    const api = new apigateway.RestApi(this, 'Api', {
      cloudWatchRole: false,
      deployOptions: {
        stageName: 'prod',
        metricsEnabled: true,
        loggingLevel: apigateway.MethodLoggingLevel.INFO,
        dataTraceEnabled: false,
      },
    });

    const orderResource = api.root.addResource('order');
    orderResource.addMethod('POST', new apigateway.LambdaIntegration(primaryWorker, { proxy: true }));

    const eventRule = new events.Rule(this, 'OrdersRule', {
      eventPattern: {
        source: ['orders.api'],
        detailType: ['OrderCreated'],
      },
    });
    eventRule.addTarget(new eventsTargets.SqsQueue(mainQueue));

    const stepLogGroup = new logs.LogGroup(this, 'StepFunctionsLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const sfnRole = new iam.Role(this, 'OrderFlowRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });

    const stateMachine = new sfn.StateMachine(this, 'OrderFlow', {
      definition: new sfn.Pass(this, 'Start'),
      timeout: Duration.minutes(5),
      role: sfnRole,
      logs: {
        destination: stepLogGroup,
        includeExecutionData: true,
        level: sfn.LogLevel.ALL,
      },
    });

    const pipeRole = new iam.Role(this, 'OrdersPipeRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
    });

    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
        resources: [mainQueue.queueArn],
      })
    );

    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [secondaryWorker.functionArn],
      })
    );

    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [stateMachine.stateMachineArn],
      })
    );

    new pipes.CfnPipe(this, 'OrdersPipe', {
      roleArn: pipeRole.roleArn,
      source: mainQueue.queueArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 10,
        },
      },
      enrichment: secondaryWorker.functionArn,
      target: stateMachine.stateMachineArn,
      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
      },
    });

    // RDS is guarded by the prod-account condition so that environments running with
    // the CDK placeholder account (000000000000) skip the DB instance and its secret
    // attachment — both of which require service APIs unavailable in those environments.
    const dbInstance = new rds.DatabaseInstance(this, 'OrdersDb', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      publiclyAccessible: false,
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_14_7,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      securityGroups: [dbSg],
      credentials: rds.Credentials.fromGeneratedSecret('postgres'),
      storageEncrypted: true,
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.applyConditionToResources(dbInstance, isProdAccount);

    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'QueueUrl', { value: mainQueue.queueUrl });
    new cdk.CfnOutput(this, 'DynamoDbTableName', { value: ordersTable.tableName });
    new cdk.CfnOutput(this, 'S3BucketName', { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, 'RdsEndpoint', {
      value: cdk.Token.asString(
        cdk.Fn.conditionIf(
          isProdAccount.logicalId,
          dbInstance.dbInstanceEndpointAddress,
          ''
        )
      ),
    });
    new cdk.CfnOutput(this, 'PrimaryLambdaArn', { value: primaryWorker.functionArn });
    new cdk.CfnOutput(this, 'EnrichmentLambdaArn', { value: secondaryWorker.functionArn });
  }

  // Walks every CfnResource in the given construct tree and attaches a CloudFormation
  // condition so those resources are only created when the condition is true.
  private applyConditionToResources(scope: Construct, condition: cdk.CfnCondition) {
    for (const child of scope.node.findAll()) {
      if (child instanceof cdk.CfnResource) {
        child.cfnOptions.condition = condition;
      }
    }
  }
}

const app = new cdk.App();
new AppStack(app, 'AppStack', {
  env: {
    region: process.env.AWS_REGION || 'us-east-1',
  },
});
