#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as route53 from 'aws-cdk-lib/aws-route53';
import * as route53Targets from 'aws-cdk-lib/aws-route53-targets';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

const endpoint = process.env.AWS_ENDPOINT ?? process.env.AWS_ENDPOINT_URL;
const region = process.env.AWS_REGION ?? 'us-east-1';
const account =
  process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID ?? '000000000000';
const accessKeyId = process.env.AWS_ACCESS_KEY_ID ?? 'test';
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY ?? 'test';

if (endpoint) {
  process.env.AWS_ENDPOINT_URL = endpoint;
}

void accessKeyId;
void secretAccessKey;

class ThreeTierWebAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 1,
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

    const albSecurityGroup = new ec2.SecurityGroup(this, 'AlbSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'alb security group',
    });
    albSecurityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'http ingress');

    const ecsSecurityGroup = new ec2.SecurityGroup(this, 'EcsSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'ecs security group',
    });

    const lambdaSecurityGroup = new ec2.SecurityGroup(this, 'LambdaSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'lambda security group',
    });

    const rdsSecurityGroup = new ec2.SecurityGroup(this, 'RdsSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'rds security group',
    });

    const redisSecurityGroup = new ec2.SecurityGroup(this, 'RedisSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'redis security group',
    });

    ecsSecurityGroup.addIngressRule(
      albSecurityGroup,
      ec2.Port.tcp(8080),
      'backend traffic from alb',
    );

    rdsSecurityGroup.addIngressRule(
      ecsSecurityGroup,
      ec2.Port.tcp(5432),
      'postgres from ecs',
    );
    rdsSecurityGroup.addIngressRule(
      lambdaSecurityGroup,
      ec2.Port.tcp(5432),
      'postgres from lambda',
    );

    redisSecurityGroup.addIngressRule(
      ecsSecurityGroup,
      ec2.Port.tcp(6379),
      'redis from ecs',
    );

    ecsSecurityGroup.addEgressRule(rdsSecurityGroup, ec2.Port.tcp(5432), 'postgres to rds');
    ecsSecurityGroup.addEgressRule(redisSecurityGroup, ec2.Port.tcp(6379), 'redis to cache');
    ecsSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      'aws endpoints for logs and secrets',
    );

    lambdaSecurityGroup.addEgressRule(
      rdsSecurityGroup,
      ec2.Port.tcp(5432),
      'postgres to rds',
    );
    lambdaSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      'aws endpoints for logs and secrets',
    );

    const backendRepository = new ecr.Repository(this, 'BackendRepository');

    const backendCluster = new ecs.Cluster(this, 'BackendCluster', {
      vpc,
    });

    const backendLogGroup = new logs.LogGroup(this, 'BackendLogGroup', {
      logGroupName: '/ecs/app',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const databaseSecret = new secretsmanager.Secret(this, 'DatabaseSecret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'appuser' }),
        generateStringKey: 'password',
        excludePunctuation: true,
      },
    });

    const databaseSubnetGroup = new rds.CfnDBSubnetGroup(this, 'DatabaseSubnetGroup', {
      dbSubnetGroupDescription: 'private database subnets',
      subnetIds: vpc.privateSubnets.map((subnet) => subnet.subnetId),
    });

    const database = new rds.CfnDBInstance(this, 'Database', {
      engine: 'postgres',
      engineVersion: '15.3',
      dbInstanceClass: 'db.t3.micro',
      allocatedStorage: '20',
      masterUsername: databaseSecret.secretValueFromJson('username').unsafeUnwrap(),
      masterUserPassword: databaseSecret.secretValueFromJson('password').unsafeUnwrap(),
      multiAz: false,
      publiclyAccessible: false,
      backupRetentionPeriod: 7,
      storageEncrypted: true,
      deletionProtection: false,
      dbSubnetGroupName: databaseSubnetGroup.ref,
      vpcSecurityGroups: [rdsSecurityGroup.securityGroupId],
    });
    database.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const dbEndpointParameter = new ssm.StringParameter(this, 'DbEndpointParameter', {
      parameterName: '/app/db/endpoint',
      stringValue: database.attrEndpointAddress,
    });

    const sessionTable = new dynamodb.Table(this, 'SessionTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const redisSubnetGroup = new elasticache.CfnSubnetGroup(this, 'RedisSubnetGroup', {
      description: 'redis subnet group',
      subnetIds: vpc.privateSubnets.map((subnet) => subnet.subnetId),
    });

    new elasticache.CfnReplicationGroup(this, 'RedisReplicationGroup', {
      replicationGroupDescription: 'backend redis replication group',
      engine: 'redis',
      cacheNodeType: 'cache.t3.micro',
      numCacheClusters: 2,
      automaticFailoverEnabled: true,
      multiAzEnabled: true,
      cacheSubnetGroupName: redisSubnetGroup.ref,
      securityGroupIds: [redisSecurityGroup.securityGroupId],
      atRestEncryptionEnabled: true,
      transitEncryptionEnabled: true,
    });

    const jobQueue = new sqs.Queue(this, 'JobQueue', {
      visibilityTimeout: cdk.Duration.seconds(60),
      retentionPeriod: cdk.Duration.days(4),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });

    const notificationsTopic = new sns.Topic(this, 'NotificationsTopic');

    const taskRole = new iam.Role(this, 'BackendTaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });

    databaseSecret.grantRead(taskRole);
    sessionTable.grantReadWriteData(taskRole);
    jobQueue.grantSendMessages(taskRole);
    notificationsTopic.grantPublish(taskRole);

    taskRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['ssm:GetParameter'],
        resources: [dbEndpointParameter.parameterArn],
      }),
    );

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'BackendTaskDefinition', {
      cpu: 512,
      memoryLimitMiB: 1024,
      taskRole,
    });

    if (taskDefinition.executionRole) {
      databaseSecret.grantRead(taskDefinition.executionRole);
    }

    const backendContainer = taskDefinition.addContainer('BackendContainer', {
      image: ecs.ContainerImage.fromRegistry(
        cdk.Fn.join('', [backendRepository.repositoryUri, ':latest']),
      ),
      logging: ecs.LogDrivers.awsLogs({
        logGroup: backendLogGroup,
        streamPrefix: 'app',
      }),
      secrets: {
        DB_USERNAME: ecs.Secret.fromSecretsManager(databaseSecret, 'username'),
        DB_PASSWORD: ecs.Secret.fromSecretsManager(databaseSecret, 'password'),
      },
      environment: {
        DB_ENDPOINT_PARAMETER: dbEndpointParameter.parameterName,
        DYNAMODB_TABLE_NAME: sessionTable.tableName,
        JOB_QUEUE_URL: jobQueue.queueUrl,
        NOTIFICATIONS_TOPIC_ARN: notificationsTopic.topicArn,
      },
    });
    backendContainer.addPortMappings({ containerPort: 8080 });

    const backendService = new ecs.FargateService(this, 'BackendService', {
      cluster: backendCluster,
      taskDefinition,
      desiredCount: 2,
      minHealthyPercent: 100,
      assignPublicIp: false,
      securityGroups: [ecsSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    });

    const scaling = backendService.autoScaleTaskCount({
      minCapacity: 2,
      maxCapacity: 6,
    });
    scaling.scaleOnCpuUtilization('BackendCpuScaling', {
      targetUtilizationPercent: 50,
    });

    const loadBalancer = new elbv2.ApplicationLoadBalancer(this, 'BackendAlb', {
      vpc,
      internetFacing: true,
      securityGroup: albSecurityGroup,
    });

    const targetGroup = new elbv2.ApplicationTargetGroup(this, 'BackendTargetGroup', {
      vpc,
      port: 8080,
      protocol: elbv2.ApplicationProtocol.HTTP,
      targetType: elbv2.TargetType.IP,
      healthCheck: {
        path: '/health',
      },
    });

    targetGroup.addTarget(
      backendService.loadBalancerTarget({
        containerName: 'BackendContainer',
        containerPort: 8080,
      }),
    );

    const listener = loadBalancer.addListener('HttpListener', {
      port: 80,
      protocol: elbv2.ApplicationProtocol.HTTP,
      open: false,
      defaultTargetGroups: [targetGroup],
    });

    listener.addTargetGroups('HealthRule', {
      priority: 10,
      targetGroups: [targetGroup],
      conditions: [elbv2.ListenerCondition.pathPatterns(['/health'])],
    });
    listener.addTargetGroups('ApiRule', {
      priority: 20,
      targetGroups: [targetGroup],
      conditions: [elbv2.ListenerCondition.pathPatterns(['/api/*'])],
    });

    new cloudwatch.Alarm(this, 'BackendHttp5xxAlarm', {
      metric: new cloudwatch.Metric({
        namespace: 'AWS/ApplicationELB',
        metricName: 'HTTPCode_Target_5XX_Count',
        period: cdk.Duration.minutes(1),
        statistic: 'Sum',
        dimensionsMap: {
          LoadBalancer: loadBalancer.loadBalancerFullName,
          TargetGroup: targetGroup.targetGroupFullName,
        },
      }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    const migrationFunctionName = cdk.Fn.join('', [cdk.Aws.STACK_NAME, '-migration']);
    const migrationLogGroup = new logs.LogGroup(this, 'MigrationLambdaLogGroup', {
      logGroupName: cdk.Fn.join('', ['/aws/lambda/', migrationFunctionName]),
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const migrationRole = new iam.Role(this, 'MigrationLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    migrationRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
        resources: [
          migrationLogGroup.logGroupArn,
          cdk.Fn.join('', [migrationLogGroup.logGroupArn, ':*']),
        ],
      }),
    );
    databaseSecret.grantRead(migrationRole);
    migrationRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['ssm:GetParameter'],
        resources: [dbEndpointParameter.parameterArn],
      }),
    );

    const migrationLambda = new lambda.Function(this, 'MigrationLambda', {
      functionName: migrationFunctionName,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
exports.handler = async (event) => {
  console.log(JSON.stringify({ action: 'migrate', event }));
  return { status: 'ok' };
};
      `.trim()),
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      role: migrationRole,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [lambdaSecurityGroup],
      environment: {
        DB_SECRET_ARN: databaseSecret.secretArn,
        DB_ENDPOINT_PARAMETER: dbEndpointParameter.parameterName,
      },
    });

    const stateMachineLogGroup = new logs.LogGroup(this, 'MigrationStateMachineLogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const stateMachineRole = new iam.Role(this, 'MigrationStateMachineRole', {
      assumedBy: new iam.ServicePrincipal(`states.${cdk.Stack.of(this).region}.amazonaws.com`),
    });
    migrationLambda.grantInvoke(stateMachineRole);
    stateMachineRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: [
          'logs:CreateLogDelivery',
          'logs:GetLogDelivery',
          'logs:UpdateLogDelivery',
          'logs:DeleteLogDelivery',
          'logs:ListLogDeliveries',
          'logs:PutResourcePolicy',
          'logs:DescribeResourcePolicies',
          'logs:DescribeLogGroups',
        ],
        resources: ['*'],
      }),
    );

    const migrationStep = new tasks.LambdaInvoke(this, 'RunMigration', {
      lambdaFunction: migrationLambda,
      payloadResponseOnly: false,
      retryOnServiceExceptions: false,
    });

    const stateMachine = new stepfunctions.StateMachine(this, 'MigrationStateMachine', {
      definitionBody: stepfunctions.DefinitionBody.fromChainable(migrationStep),
      role: stateMachineRole,
      logs: {
        destination: stateMachineLogGroup,
        level: stepfunctions.LogLevel.ALL,
      },
    });

    const eventBridgeRole = new iam.Role(this, 'MigrationScheduleRole', {
      assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
    });
    eventBridgeRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [stateMachine.stateMachineArn],
      }),
    );

    new events.Rule(this, 'MigrationScheduleRule', {
      schedule: events.Schedule.expression('rate(6 hours)'),
      targets: [new eventsTargets.SfnStateMachine(stateMachine, { role: eventBridgeRole })],
    });

    const frontendBucket = new s3.Bucket(this, 'FrontendBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const frontendDistribution = new cloudfront.Distribution(this, 'FrontendDistribution', {
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessControl(frontendBucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
      },
    });

    const hostedZone = new route53.HostedZone(this, 'HostedZone', {
      zoneName: 'example.com',
    });

    new route53.ARecord(this, 'FrontendAliasRecord', {
      zone: hostedZone,
      recordName: 'frontend',
      target: route53.RecordTarget.fromAlias(
        new route53Targets.CloudFrontTarget(frontendDistribution),
      ),
    });

    new route53.ARecord(this, 'ApiAliasRecord', {
      zone: hostedZone,
      recordName: 'api',
      target: route53.RecordTarget.fromAlias(
        new route53Targets.LoadBalancerTarget(loadBalancer),
      ),
    });

    new cdk.CfnOutput(this, 'CloudFrontDomainName', {
      value: frontendDistribution.distributionDomainName,
    });
    new cdk.CfnOutput(this, 'ALBDNSName', {
      value: loadBalancer.loadBalancerDnsName,
    });
    new cdk.CfnOutput(this, 'HostedZoneId', {
      value: hostedZone.hostedZoneId,
    });
    new cdk.CfnOutput(this, 'RDSEndpoint', {
      value: database.attrEndpointAddress,
    });
  }
}

const app = new cdk.App({
  outdir: 'cdk.out',
});

new ThreeTierWebAppStack(app, 'ThreeTierWebAppStack', {
  env: {
    account,
    region,
  },
});

app.synth();
