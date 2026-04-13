#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventTargets from 'aws-cdk-lib/aws-events-targets';
import * as glue from 'aws-cdk-lib/aws-glue';
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

export interface StandbyRecoveryStackProps extends cdk.StackProps {
  readonly namePrefix?: string;
}

function sanitizeNamePrefix(value: string): string {
  const sanitized = value
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 24);

  return sanitized || 'dev';
}

function fixedName(prefix: string, suffix: string, maxLength = 63): string {
  const name = `${prefix}-${suffix}`.replace(/-+/g, '-');
  return name.length <= maxLength ? name : name.slice(0, maxLength).replace(/-$/g, '');
}

function glueName(prefix: string, suffix: string): string {
  return fixedName(prefix, suffix, 80).replace(/-/g, '_');
}

export class StandbyRecoveryStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StandbyRecoveryStackProps = {}) {
    super(scope, id, props);

    const namePrefix = sanitizeNamePrefix(
      props.namePrefix ??
      process.env.NAME_PREFIX ??
      this.node.tryGetContext('namePrefix') ??
      'dev',
    );
    const endpointUrl = process.env.AWS_ENDPOINT ?? process.env.AWS_ENDPOINT_URL ?? '';

    const vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr('10.40.0.0/16'),
      maxAzs: 2,
      natGateways: 1,
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
    (vpc.node.defaultChild as ec2.CfnVPC).applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const lambdaRunspaceSg = new ec2.SecurityGroup(this, 'LambdaRunspaceSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Lambda Runspace Security Group',
    });

    const databaseSg = new ec2.SecurityGroup(this, 'DatabaseSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Database Security Group',
    });
    databaseSg.addIngressRule(lambdaRunspaceSg, ec2.Port.tcp(5432), 'PostgreSQL from Lambda runspace');

    const endpointSg = new ec2.SecurityGroup(this, 'VpcEndpointSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Interface endpoint Security Group',
    });
    endpointSg.addIngressRule(lambdaRunspaceSg, ec2.Port.tcp(443), 'HTTPS from Lambda runspace');

    new ec2.GatewayVpcEndpoint(this, 'S3GatewayEndpoint', {
      vpc,
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [{ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }],
    });

    new ec2.InterfaceVpcEndpoint(this, 'SecretsManagerInterfaceEndpoint', {
      vpc,
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
      privateDnsEnabled: true,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [endpointSg],
    });

    const orderDlq = new sqs.Queue(this, 'OrderDeadLetterQueue', {
      retentionPeriod: cdk.Duration.days(14),
    });

    const orderQueue = new sqs.Queue(this, 'OrderQueue', {
      retentionPeriod: cdk.Duration.days(4),
      deadLetterQueue: {
        queue: orderDlq,
        maxReceiveCount: 3,
      },
    });

    const pipeSourceQueue = new sqs.Queue(this, 'PipeSourceQueue', {
      visibilityTimeout: cdk.Duration.seconds(60),
    });

    const eventBus = new events.EventBus(this, 'OrderEventBus');

    const orderReceivedRule = new events.Rule(this, 'OrderReceivedRule', {
      eventBus,
      eventPattern: {
        source: ['orders.api'],
      },
      targets: [new eventTargets.SqsQueue(orderQueue)],
    });

    const dbSecret = new secretsmanager.Secret(this, 'DatabaseSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'app_user' }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
    });

    const dbSubnetGroup = new rds.SubnetGroup(this, 'DatabaseSubnetGroup', {
      description: 'Private subnets for standby PostgreSQL',
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    const dbInstance = new rds.DatabaseInstance(this, 'PostgresDatabase', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      allocatedStorage: 20,
      backupRetention: cdk.Duration.days(1),
      credentials: rds.Credentials.fromPassword(
        'app_user',
        cdk.SecretValue.secretsManager(dbSecret.secretArn, { jsonField: 'password' }),
      ),
      databaseName: 'appdb',
      deleteAutomatedBackups: true,
      deletionProtection: false,
      multiAz: false,
      publiclyAccessible: false,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      securityGroups: [databaseSg],
      storageEncrypted: true,
      storageType: rds.StorageType.GP2,
      subnetGroup: dbSubnetGroup,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    const apiLogGroup = new logs.LogGroup(this, 'ApiAccessLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const api = new apigw.RestApi(this, 'StandbyApi', {
      cloudWatchRole: true,
      deployOptions: {
        stageName: 'standby',
        loggingLevel: apigw.MethodLoggingLevel.INFO,
        dataTraceEnabled: false,
        metricsEnabled: true,
        accessLogDestination: new apigw.LogGroupLogDestination(apiLogGroup),
        accessLogFormat: apigw.AccessLogFormat.jsonWithStandardFields(),
      },
    });

    const commonLambdaEnvironment = {
      AWS_ENDPOINT: endpointUrl,
      NODE_OPTIONS: '--enable-source-maps',
    };

    const healthLogGroup = new logs.LogGroup(this, 'HealthHandlerLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const orderLogGroup = new logs.LogGroup(this, 'OrderHandlerLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const helperLogGroup = new logs.LogGroup(this, 'SecretsHelperLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const healthHandler = new lambda.Function(this, 'HealthHandler', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async () => ({
          statusCode: 200,
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ status: 'healthy' }),
        });
      `),
      environment: {
        ...commonLambdaEnvironment,
        DATABASE_ENDPOINT_ADDRESS: dbInstance.instanceEndpoint.hostname,
        DATABASE_PORT: dbInstance.instanceEndpoint.port.toString(),
        SECRET_ARN: dbSecret.secretArn,
      },
      logGroup: healthLogGroup,
      memorySize: 128,
      securityGroups: [lambdaRunspaceSg],
      timeout: cdk.Duration.seconds(5),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    const healthResource = api.root.addResource('health');
    healthResource.addMethod('GET', new apigw.LambdaIntegration(healthHandler));

    const orderHandler = new lambda.Function(this, 'OrderHandler', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');
        const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

        const clientConfig = process.env.AWS_ENDPOINT
          ? { endpoint: process.env.AWS_ENDPOINT }
          : {};
        const eventBridge = new EventBridgeClient(clientConfig);
        const sqs = new SQSClient(clientConfig);

        exports.handler = async (event) => {
          const body = event.body ? JSON.parse(event.body) : event;
          const detail = JSON.stringify({
            requestId: event.requestContext?.requestId,
            body,
          });

          await sqs.send(new SendMessageCommand({
            QueueUrl: process.env.ORDER_QUEUE_URL,
            MessageBody: detail,
          }));

          await sqs.send(new SendMessageCommand({
            QueueUrl: process.env.PIPE_SOURCE_QUEUE_URL,
            MessageBody: detail,
          }));

          await eventBridge.send(new PutEventsCommand({
            Entries: [{
              EventBusName: process.env.EVENT_BUS_NAME,
              Source: 'orders.api',
              DetailType: 'OrderReceived',
              Detail: detail,
            }],
          }));

          return {
            statusCode: 202,
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ accepted: true }),
          };
        };
      `),
      environment: {
        ...commonLambdaEnvironment,
        EVENT_BUS_NAME: eventBus.eventBusName,
        ORDER_QUEUE_URL: orderQueue.queueUrl,
        PIPE_SOURCE_QUEUE_URL: pipeSourceQueue.queueUrl,
      },
      logGroup: orderLogGroup,
      memorySize: 256,
      securityGroups: [lambdaRunspaceSg],
      timeout: cdk.Duration.seconds(10),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    const ordersResource = api.root.addResource('orders');
    ordersResource.addMethod('POST', new apigw.LambdaIntegration(orderHandler));

    orderHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sqs:SendMessage'],
      resources: [orderQueue.queueArn, pipeSourceQueue.queueArn],
    }));
    eventBus.grantPutEventsTo(orderHandler);

    const secretsHelper = new lambda.Function(this, 'SecretsHelper', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        const { GetSecretValueCommand, SecretsManagerClient } = require('@aws-sdk/client-secrets-manager');
        const { SFNClient, StartExecutionCommand } = require('@aws-sdk/client-sfn');

        const clientConfig = process.env.AWS_ENDPOINT
          ? { endpoint: process.env.AWS_ENDPOINT }
          : {};
        const secretsManager = new SecretsManagerClient(clientConfig);
        const sfn = new SFNClient(clientConfig);

        exports.handler = async (event = {}) => {
          try {
            await secretsManager.send(new GetSecretValueCommand({
              SecretId: process.env.SECRET_ARN,
            }));
          } catch (_err) {
            // Secret fetch failed; proceed without it
          }

          if (event.Records) {
            for (const record of event.Records) {
              let body = record.body ?? '{}';
              if (typeof body === 'string') {
                try { body = JSON.parse(body); } catch { body = { raw: body }; }
              }
              await sfn.send(new StartExecutionCommand({
                stateMachineArn: process.env.STATE_MACHINE_ARN,
                input: JSON.stringify(body),
              }));
            }
            return;
          }

          const record = Array.isArray(event) ? event[0] : event;
          let body = record?.body ?? record ?? {};
          if (typeof body === 'string') {
            try {
              body = JSON.parse(body);
            } catch {
              body = { raw: body };
            }
          }

          return { status: 'OK', body };
        };
      `),
      environment: {
        ...commonLambdaEnvironment,
        SECRET_ARN: dbSecret.secretArn,
      },
      logGroup: helperLogGroup,
      memorySize: 128,
      securityGroups: [lambdaRunspaceSg],
      timeout: cdk.Duration.seconds(10),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });
    secretsHelper.addToRolePolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue'],
      resources: [dbSecret.secretArn],
    }));

    const recoveryDefinition = sfn.Chain
      .start(new sfn.Pass(this, 'BeginRecovery'))
      .next(new tasks.LambdaInvoke(this, 'CheckSecrets', {
        lambdaFunction: secretsHelper,
        outputPath: '$.Payload',
      }))
      .next(new sfn.Succeed(this, 'RecoveryReady'));

    const stateMachine = new sfn.StateMachine(this, 'FailureRecoveryStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(recoveryDefinition),
      stateMachineType: sfn.StateMachineType.STANDARD,
    });

    secretsHelper.addEnvironment('STATE_MACHINE_ARN', stateMachine.stateMachineArn);
    secretsHelper.addToRolePolicy(new iam.PolicyStatement({
      actions: ['states:StartExecution'],
      resources: [stateMachine.stateMachineArn],
    }));

    new lambda.EventSourceMapping(this, 'PipeQueueTrigger', {
      target: secretsHelper,
      eventSourceArn: pipeSourceQueue.queueArn,
      batchSize: 1,
    });
    pipeSourceQueue.grantConsumeMessages(secretsHelper);

    const pipeRole = new iam.Role(this, 'PipeRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
      description: 'Allows the order recovery pipe to consume SQS, enrich, and start recovery workflows',
    });
    pipeSourceQueue.grantConsumeMessages(pipeRole);
    secretsHelper.grantInvoke(pipeRole);
    stateMachine.grantStartExecution(pipeRole);

    new pipes.CfnPipe(this, 'OrderRecoveryPipe', {
      desiredState: 'RUNNING',
      roleArn: pipeRole.roleArn,
      source: pipeSourceQueue.queueArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 1,
          maximumBatchingWindowInSeconds: 0,
        },
      },
      enrichment: secretsHelper.functionArn,
      target: stateMachine.stateMachineArn,
      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
      },
    });

    const logsBucket = new s3.Bucket(this, 'StandbyLogsBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
    });

    const glueDatabase = new glue.CfnDatabase(this, 'StandbyGlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        description: 'Catalog for standby recovery logs',
        name: glueName(namePrefix, 'standby_logs_db'),
      },
    });

    const glueRole = new iam.Role(this, 'GlueCrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      description: 'Allows the Glue crawler to catalog standby recovery logs',
    });
    logsBucket.grantReadWrite(glueRole);
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:BatchCreatePartition',
        'glue:BatchGetPartition',
        'glue:CreatePartition',
        'glue:GetDatabase',
        'glue:GetPartition',
        'glue:GetPartitions',
        'glue:GetTable',
        'glue:GetTables',
        'glue:UpdatePartition',
        'glue:UpdateTable',
      ],
      resources: ['*'],
    }));
    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
      ],
      resources: ['*'],
    }));

    const crawler = new glue.CfnCrawler(this, 'StandbyLogsCrawler', {
      databaseName: glueName(namePrefix, 'standby_logs_db'),
      role: glueRole.roleArn,
      schemaChangePolicy: {
        deleteBehavior: 'LOG',
        updateBehavior: 'UPDATE_IN_DATABASE',
      },
      targets: {
        s3Targets: [{
          path: `s3://${logsBucket.bucketName}/orders/`,
        }],
      },
    });
    crawler.addDependency(glueDatabase);

    const athenaWorkGroupName = fixedName(namePrefix, 'standby-workgroup', 128);
    new athena.CfnWorkGroup(this, 'StandbyAthenaWorkgroup', {
      name: athenaWorkGroupName,
      state: 'ENABLED',
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetricsEnabled: true,
        resultConfiguration: {
          encryptionConfiguration: {
            encryptionOption: 'SSE_S3',
          },
          outputLocation: `s3://${logsBucket.bucketName}/athena-results/`,
        },
      },
    });

    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'RestApiId', { value: api.restApiId });
    new cdk.CfnOutput(this, 'ApiAccessLogGroupName', { value: apiLogGroup.logGroupName });
    new cdk.CfnOutput(this, 'OrderQueueUrl', { value: orderQueue.queueUrl });
    new cdk.CfnOutput(this, 'PipeSourceQueueUrl', { value: pipeSourceQueue.queueUrl });
    new cdk.CfnOutput(this, 'EventBusName', { value: eventBus.eventBusName });
    new cdk.CfnOutput(this, 'OrderReceivedRuleName', {
      value: cdk.Fn.select(1, cdk.Fn.split('|', orderReceivedRule.ruleName)),
    });
    new cdk.CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
    new cdk.CfnOutput(this, 'LogsBucketName', { value: logsBucket.bucketName });
    new cdk.CfnOutput(this, 'DatabaseSecretArn', { value: dbSecret.secretArn });
    new cdk.CfnOutput(this, 'DatabaseInstanceIdentifier', { value: dbInstance.instanceIdentifier });
    new cdk.CfnOutput(this, 'DatabaseSecurityGroupId', { value: databaseSg.securityGroupId });
    new cdk.CfnOutput(this, 'GlueCrawlerName', { value: crawler.ref });
    new cdk.CfnOutput(this, 'AthenaWorkGroupName', { value: athenaWorkGroupName });
    new cdk.CfnOutput(this, 'HealthHandlerRoleName', { value: healthHandler.role!.roleName });
    new cdk.CfnOutput(this, 'OrderHandlerRoleName', { value: orderHandler.role!.roleName });
    new cdk.CfnOutput(this, 'SecretsHelperRoleName', { value: secretsHelper.role!.roleName });
    new cdk.CfnOutput(this, 'PipeRoleName', { value: pipeRole.roleName });
    new cdk.CfnOutput(this, 'SecretsHelperLogGroupName', { value: helperLogGroup.logGroupName });
    new cdk.CfnOutput(this, 'SecretsHelperFunctionName', { value: secretsHelper.functionName });
  }
}

if (require.main === module) {
  const app = new cdk.App();
  new StandbyRecoveryStack(app, 'StandbyRecoveryStack', {
    env: {
      account: process.env.CDK_DEFAULT_ACCOUNT,
      region: process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION ?? 'us-east-1',
    },
  });
}
