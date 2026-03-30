#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import { Construct } from 'constructs';

const awsRegion = process.env.AWS_REGION ?? 'us-east-1';
process.env.AWS_REGION = awsRegion;
process.env.AWS_DEFAULT_REGION = process.env.AWS_DEFAULT_REGION ?? awsRegion;
process.env.CDK_DEFAULT_REGION = process.env.CDK_DEFAULT_REGION ?? awsRegion;

const awsEndpoint = process.env.AWS_ENDPOINT;
if (awsEndpoint) {
  // CDK and the AWS SDKs look for AWS_ENDPOINT_URL; map the allowed input so
  // synth/deploy keeps the requested region but can still route to a custom endpoint.
  process.env.AWS_ENDPOINT_URL = awsEndpoint;
}

const apiLambdaCode = `
const { randomUUID } = require('node:crypto');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');

const region = process.env.AWS_REGION || 'us-east-1';
const s3 = new S3Client({ region });
const sqs = new SQSClient({ region });
const eventBridge = new EventBridgeClient({ region });

exports.handler = async (event) => {
  const rawBody = typeof event.body === 'string' && event.body.length > 0
    ? event.body
    : JSON.stringify({});
  const key = 'orders/' + Date.now() + '-' + randomUUID() + '.json';

  await Promise.all([
    sqs.send(new SendMessageCommand({
      QueueUrl: process.env.QUEUE_URL,
      MessageBody: rawBody,
    })),
    s3.send(new PutObjectCommand({
      Bucket: process.env.BUCKET_NAME,
      Key: key,
      Body: rawBody,
      ContentType: 'application/json',
    })),
    eventBridge.send(new PutEventsCommand({
      Entries: [
        {
          EventBusName: process.env.EVENT_BUS_NAME,
          Source: 'orders.api',
          DetailType: 'OrderReceived',
          Detail: JSON.stringify({
            archiveKey: key,
            receivedBody: rawBody,
          }),
        },
      ],
    })),
  ]);

  return {
    statusCode: 202,
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      status: 'accepted',
      archiveKey: key,
    }),
  };
};
`;

const workerLambdaCode = `
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const region = process.env.AWS_REGION || 'us-east-1';
const secretsManager = new SecretsManagerClient({ region });
const sns = new SNSClient({ region });

function parseBody(body) {
  if (typeof body !== 'string') {
    return body;
  }

  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

exports.handler = async (event) => {
  const secretResponse = await secretsManager.send(new GetSecretValueCommand({
    SecretId: process.env.DB_SECRET_ARN,
  }));
  const credentials = JSON.parse(secretResponse.SecretString || '{}');

  const records = Array.isArray(event && event.Records)
    ? event.Records.map((record) => ({
        messageId: record.messageId,
        payload: parseBody(record.body),
      }))
    : [parseBody(event)];

  const processedAt = new Date().toISOString();
  const dbConnection = {
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || '5432'),
    username: credentials.username,
    database: credentials.dbname || process.env.DB_NAME,
    status: 'stubbed-connection',
  };

  await sns.send(new PublishCommand({
    TopicArn: process.env.TOPIC_ARN,
    Subject: 'orders-worker-processed',
    Message: JSON.stringify({
      processedAt,
      records,
      dbConnection,
    }),
  }));

  return {
    processedAt,
    recordCount: records.length,
    dbConnection,
  };
};
`;

class OrdersIngestStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const isLocalEndpointMode = Boolean(awsEndpoint);
    const deploysManagedDataPlane = !isLocalEndpointMode;
    const deploysPipe = !isLocalEndpointMode;
    const attachesLambdasToVpc = !isLocalEndpointMode;
    const createsQueueWorkerTrigger = !isLocalEndpointMode;
    const createsEventQueuePolicy = !isLocalEndpointMode;
    const createsExplicitRouting = !isLocalEndpointMode;

    const availabilityZones = isLocalEndpointMode
      ? [`${awsRegion}a`, `${awsRegion}b`]
      : [cdk.Fn.select(0, cdk.Fn.getAzs()), cdk.Fn.select(1, cdk.Fn.getAzs())];

    const vpc = new ec2.Vpc(this, 'OrdersVpc', {
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
      subnetConfiguration: [],
      natGateways: 0,
      maxAzs: 2,
    });

    const publicSubnetA = new ec2.CfnSubnet(this, 'OrdersPublicSubnetA', {
      vpcId: vpc.vpcId,
      cidrBlock: '10.0.101.0/24',
      availabilityZone: availabilityZones[0],
      mapPublicIpOnLaunch: true,
    });

    const publicSubnetB = new ec2.CfnSubnet(this, 'OrdersPublicSubnetB', {
      vpcId: vpc.vpcId,
      cidrBlock: '10.0.102.0/24',
      availabilityZone: availabilityZones[1],
      mapPublicIpOnLaunch: true,
    });

    const privateSubnetA = new ec2.CfnSubnet(this, 'OrdersPrivateSubnetA', {
      vpcId: vpc.vpcId,
      cidrBlock: '10.0.1.0/24',
      availabilityZone: availabilityZones[0],
      mapPublicIpOnLaunch: false,
    });

    const privateSubnetB = new ec2.CfnSubnet(this, 'OrdersPrivateSubnetB', {
      vpcId: vpc.vpcId,
      cidrBlock: '10.0.2.0/24',
      availabilityZone: availabilityZones[1],
      mapPublicIpOnLaunch: false,
    });

    let importedPrivateSubnets: ec2.ISubnet[] = [];
    let privateDefaultRoute: ec2.CfnRoute | undefined;

    if (createsExplicitRouting) {
      const internetGateway = new ec2.CfnInternetGateway(this, 'OrdersInternetGateway', {});

      const gatewayAttachment = new ec2.CfnVPCGatewayAttachment(this, 'OrdersInternetGatewayAttachment', {
        vpcId: vpc.vpcId,
        internetGatewayId: internetGateway.ref,
      });

      const publicRouteTable = new ec2.CfnRouteTable(this, 'OrdersPublicRouteTable', {
        vpcId: vpc.vpcId,
      });

      const publicDefaultRoute = new ec2.CfnRoute(this, 'OrdersPublicDefaultRoute', {
        routeTableId: publicRouteTable.ref,
        destinationCidrBlock: '0.0.0.0/0',
        gatewayId: internetGateway.ref,
      });
      publicDefaultRoute.addDependency(gatewayAttachment);

      new ec2.CfnSubnetRouteTableAssociation(this, 'OrdersPublicSubnetAssociationA', {
        subnetId: publicSubnetA.ref,
        routeTableId: publicRouteTable.ref,
      });

      new ec2.CfnSubnetRouteTableAssociation(this, 'OrdersPublicSubnetAssociationB', {
        subnetId: publicSubnetB.ref,
        routeTableId: publicRouteTable.ref,
      });

      const natEip = new ec2.CfnEIP(this, 'OrdersNatEip', {
        domain: 'vpc',
      });
      natEip.addDependency(gatewayAttachment);

      const natGateway = new ec2.CfnNatGateway(this, 'OrdersNatGateway', {
        subnetId: publicSubnetA.ref,
        allocationId: natEip.attrAllocationId,
      });

      const privateRouteTable = new ec2.CfnRouteTable(this, 'OrdersPrivateRouteTable', {
        vpcId: vpc.vpcId,
      });

      privateDefaultRoute = new ec2.CfnRoute(this, 'OrdersPrivateDefaultRoute', {
        routeTableId: privateRouteTable.ref,
        destinationCidrBlock: '0.0.0.0/0',
        natGatewayId: natGateway.ref,
      });
      privateDefaultRoute.addDependency(natGateway);

      new ec2.CfnSubnetRouteTableAssociation(this, 'OrdersPrivateSubnetAssociationA', {
        subnetId: privateSubnetA.ref,
        routeTableId: privateRouteTable.ref,
      });

      new ec2.CfnSubnetRouteTableAssociation(this, 'OrdersPrivateSubnetAssociationB', {
        subnetId: privateSubnetB.ref,
        routeTableId: privateRouteTable.ref,
      });

      importedPrivateSubnets = [
        ec2.Subnet.fromSubnetAttributes(this, 'ImportedPrivateSubnetA', {
          subnetId: privateSubnetA.ref,
          availabilityZone: availabilityZones[0],
          routeTableId: privateRouteTable.ref,
        }),
        ec2.Subnet.fromSubnetAttributes(this, 'ImportedPrivateSubnetB', {
          subnetId: privateSubnetB.ref,
          availabilityZone: availabilityZones[1],
          routeTableId: privateRouteTable.ref,
        }),
      ];
    }

    const apiLambdaSecurityGroup = new ec2.SecurityGroup(this, 'OrdersApiSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Attached to the orders-api Lambda.',
    });

    const workerLambdaSecurityGroup = new ec2.SecurityGroup(this, 'OrdersWorkerSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Attached to the orders-worker Lambda.',
    });

    let dataPlaneSecurityGroup: ec2.SecurityGroup | undefined;
    if (deploysManagedDataPlane) {
      dataPlaneSecurityGroup = new ec2.SecurityGroup(this, 'OrdersDataPlaneSecurityGroup', {
        vpc,
        description: 'Attached to the RDS instance.',
      });
    }

    for (const sg of [apiLambdaSecurityGroup, workerLambdaSecurityGroup]) {
      sg.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'Public AWS service endpoints');
      if (deploysManagedDataPlane) {
        sg.addEgressRule(
          ec2.Peer.securityGroupId(vpc.vpcDefaultSecurityGroup),
          ec2.Port.tcp(443),
          'Secrets Manager interface endpoint',
        );
      }
    }

    if (dataPlaneSecurityGroup) {
      workerLambdaSecurityGroup.addEgressRule(
        ec2.Peer.securityGroupId(dataPlaneSecurityGroup.securityGroupId),
        ec2.Port.tcp(5432),
        'PostgreSQL to the RDS instance',
      );

      dataPlaneSecurityGroup.addIngressRule(
        ec2.Peer.securityGroupId(workerLambdaSecurityGroup.securityGroupId),
        ec2.Port.tcp(5432),
        'Allow PostgreSQL only from the worker Lambda security group',
      );
    }

    if (deploysManagedDataPlane) {
      new ec2.CfnSecurityGroupIngress(this, 'OrdersSecretsEndpointIngressFromWorker', {
        groupId: vpc.vpcDefaultSecurityGroup,
        ipProtocol: 'tcp',
        fromPort: 443,
        toPort: 443,
        sourceSecurityGroupId: workerLambdaSecurityGroup.securityGroupId,
        description: 'Allow Secrets Manager endpoint access only from the worker Lambda security group',
      });
    }

    const ordersArchiveBucket = new s3.Bucket(this, 'OrdersArchiveBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const ordersQueue = new sqs.Queue(this, 'OrdersQueue', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      visibilityTimeout: cdk.Duration.seconds(60),
    });

    const ordersEventBus = new events.EventBus(this, 'OrdersEventBus');

    const ordersNotificationsTopic = new sns.Topic(this, 'OrdersNotificationsTopic');

    const dbSecret = new rds.DatabaseSecret(this, 'OrdersDatabaseSecret', {
      username: 'orders_app',
    });

    let ordersDatabase: rds.DatabaseInstance | undefined;
    let secretsManagerEndpoint: ec2.InterfaceVpcEndpoint | undefined;

    if (deploysManagedDataPlane) {
      const dbSubnetGroup = new rds.SubnetGroup(this, 'OrdersDbSubnetGroup', {
        vpc,
        description: 'Dedicated private subnets for the orders database.',
        vpcSubnets: {
          subnets: importedPrivateSubnets,
        },
      });

      ordersDatabase = new rds.DatabaseInstance(this, 'OrdersDatabase', {
        engine: rds.DatabaseInstanceEngine.postgres({
          version: rds.PostgresEngineVersion.VER_15,
        }),
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
        allocatedStorage: 20,
        storageEncrypted: true,
        vpc,
        vpcSubnets: {
          subnets: importedPrivateSubnets,
        },
        subnetGroup: dbSubnetGroup,
        securityGroups: dataPlaneSecurityGroup ? [dataPlaneSecurityGroup] : [],
        publiclyAccessible: false,
        deletionProtection: false,
        credentials: rds.Credentials.fromSecret(dbSecret),
        databaseName: 'orders',
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      secretsManagerEndpoint = new ec2.InterfaceVpcEndpoint(this, 'OrdersSecretsManagerEndpoint', {
        vpc,
        service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
        securityGroups: [ec2.SecurityGroup.fromSecurityGroupId(this, 'OrdersSecretsEndpointSecurityGroup', vpc.vpcDefaultSecurityGroup)],
        open: false,
        subnets: {
          subnets: importedPrivateSubnets,
        },
        privateDnsEnabled: true,
      });
      if (privateDefaultRoute) {
        secretsManagerEndpoint.node.addDependency(privateDefaultRoute);
      }
    }

    const apiLambdaRole = new iam.Role(this, 'OrdersApiLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const workerLambdaRole = new iam.Role(this, 'OrdersWorkerLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    if (attachesLambdasToVpc) {
      // Lambda ENI APIs do not support resource-level scoping, so keep the single
      // wildcard statement isolated to the minimum EC2 actions needed for VPC access.
      const lambdaVpcAccessPolicy = new iam.PolicyStatement({
        actions: [
          'ec2:CreateNetworkInterface',
          'ec2:DescribeNetworkInterfaces',
          'ec2:DeleteNetworkInterface',
          'ec2:AssignPrivateIpAddresses',
          'ec2:UnassignPrivateIpAddresses',
          'ec2:DescribeSubnets',
          'ec2:DescribeSecurityGroups',
          'ec2:DescribeVpcs',
        ],
        resources: ['*'],
      });

      apiLambdaRole.addToPolicy(lambdaVpcAccessPolicy);
      workerLambdaRole.addToPolicy(lambdaVpcAccessPolicy);
    }

    const ordersApi = new lambda.Function(this, 'OrdersApiFunction', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(apiLambdaCode),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      role: apiLambdaRole,
      ...(attachesLambdasToVpc
        ? {
            vpc,
            vpcSubnets: {
              subnets: importedPrivateSubnets,
            },
            securityGroups: [apiLambdaSecurityGroup],
          }
        : {}),
      environment: {
        BUCKET_NAME: ordersArchiveBucket.bucketName,
        EVENT_BUS_NAME: ordersEventBus.eventBusName,
        QUEUE_URL: ordersQueue.queueUrl,
      },
    });
    if (privateDefaultRoute) {
      ordersApi.node.addDependency(privateDefaultRoute);
    }

    const ordersWorker = new lambda.Function(this, 'OrdersWorkerFunction', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(workerLambdaCode),
      memorySize: 256,
      timeout: cdk.Duration.seconds(20),
      role: workerLambdaRole,
      ...(attachesLambdasToVpc
        ? {
            vpc,
            vpcSubnets: {
              subnets: importedPrivateSubnets,
            },
            securityGroups: [workerLambdaSecurityGroup],
          }
        : {}),
      environment: {
        DB_HOST: ordersDatabase ? ordersDatabase.dbInstanceEndpointAddress : 'orders-db.local',
        DB_NAME: 'orders',
        DB_PORT: ordersDatabase ? ordersDatabase.dbInstanceEndpointPort : '5432',
        DB_SECRET_ARN: dbSecret.secretArn,
        TOPIC_ARN: ordersNotificationsTopic.topicArn,
      },
    });
    if (privateDefaultRoute) {
      ordersWorker.node.addDependency(privateDefaultRoute);
    }
    if (secretsManagerEndpoint) {
      ordersWorker.node.addDependency(secretsManagerEndpoint);
    }

    const ordersApiLogGroup = new logs.LogGroup(this, 'OrdersApiLogGroup', {
      logGroupName: `/aws/lambda/${ordersApi.functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const ordersWorkerLogGroup = new logs.LogGroup(this, 'OrdersWorkerLogGroup', {
      logGroupName: `/aws/lambda/${ordersWorker.functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    apiLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sqs:SendMessage'],
      resources: [ordersQueue.queueArn],
    }));

    apiLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [ordersArchiveBucket.arnForObjects('orders/*')],
    }));

    apiLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['events:PutEvents'],
      resources: [ordersEventBus.eventBusArn],
    }));

    apiLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: [ordersApiLogGroup.logGroupArn, `${ordersApiLogGroup.logGroupArn}:*`],
    }));

    workerLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
      resources: [ordersQueue.queueArn],
    }));

    workerLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue'],
      resources: [dbSecret.secretArn],
    }));

    workerLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sns:Publish'],
      resources: [ordersNotificationsTopic.topicArn],
    }));

    workerLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: [ordersWorkerLogGroup.logGroupArn, `${ordersWorkerLogGroup.logGroupArn}:*`],
    }));

    const api = new apigateway.RestApi(this, 'OrdersApiGateway', {
      cloudWatchRole: false,
      endpointConfiguration: {
        types: [apigateway.EndpointType.REGIONAL],
      },
      deployOptions: {
        stageName: 'prod',
      },
    });

    const ordersResource = api.root.addResource('orders');
    ordersResource.addMethod('POST', new apigateway.LambdaIntegration(ordersApi, { proxy: true }));

    if (createsEventQueuePolicy) {
      new sqs.CfnQueuePolicy(this, 'OrdersQueuePolicy', {
        queues: [ordersQueue.queueUrl],
        policyDocument: {
          Version: '2012-10-17',
          Statement: [
            {
              Effect: 'Allow',
              Principal: {
                Service: 'events.amazonaws.com',
              },
              Action: 'sqs:SendMessage',
              Resource: ordersQueue.queueArn,
            },
          ],
        },
      });
    }

    new events.CfnRule(this, 'OrdersEventRule', {
      eventBusName: ordersEventBus.eventBusName,
      eventPattern: {
        source: ['orders.api'],
      },
      state: 'ENABLED',
      targets: [
        {
          id: 'Target0',
          arn: ordersQueue.queueArn,
        },
      ],
    });

    if (createsQueueWorkerTrigger) {
      ordersWorker.addEventSource(new lambdaEventSources.SqsEventSource(ordersQueue, {
        batchSize: 1,
      }));
    }

    const stateMachineRole = new iam.Role(this, 'OrdersStateMachineRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
    });

    const definition = new stepfunctions.Pass(this, 'RecordTimestamp', {
      result: stepfunctions.Result.fromObject({
        recordedAt: '2026-03-29T00:00:00Z',
      }),
      resultPath: '$.ingestMetadata',
    }).next(new stepfunctions.Succeed(this, 'OrdersWorkflowSucceeded'));

    const stateMachine = new stepfunctions.StateMachine(this, 'OrdersStateMachine', {
      definitionBody: stepfunctions.DefinitionBody.fromChainable(definition),
      stateMachineType: stepfunctions.StateMachineType.STANDARD,
      role: stateMachineRole,
    });

    if (deploysPipe) {
      const pipeRole = new iam.Role(this, 'OrdersPipeRole', {
        assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
      });

      pipeRole.addToPolicy(new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
        resources: [ordersQueue.queueArn],
      }));

      pipeRole.addToPolicy(new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [ordersWorker.functionArn],
      }));

      pipeRole.addToPolicy(new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [stateMachine.stateMachineArn],
      }));

      new pipes.CfnPipe(this, 'OrdersPipe', {
        roleArn: pipeRole.roleArn,
        source: ordersQueue.queueArn,
        sourceParameters: {
          sqsQueueParameters: {
            batchSize: 1,
          },
        },
        enrichment: ordersWorker.functionArn,
        target: stateMachine.stateMachineArn,
        targetParameters: {
          stepFunctionStateMachineParameters: {
            invocationType: 'FIRE_AND_FORGET',
          },
        },
      });
    }

    new cdk.CfnOutput(this, 'OrdersApiUrl', {
      value: api.url,
    });

    new cdk.CfnOutput(this, 'OrdersArchiveBucketName', {
      value: ordersArchiveBucket.bucketName,
    });

    new cdk.CfnOutput(this, 'OrdersQueueUrl', {
      value: ordersQueue.queueUrl,
    });

    new cdk.CfnOutput(this, 'OrdersNotificationsTopicArn', {
      value: ordersNotificationsTopic.topicArn,
    });
  }
}

const app = new cdk.App({
  outdir: process.env.CDK_OUTDIR ?? 'cdk.out',
});

new OrdersIngestStack(app, 'OrdersIngestStack', {
  env: {
    region: awsRegion,
  },
});

app.synth();
