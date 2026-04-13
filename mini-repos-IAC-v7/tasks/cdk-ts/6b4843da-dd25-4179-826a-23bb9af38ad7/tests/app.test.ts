import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { StandbyRecoveryStack } from '../app';

describe('StandbyRecoveryStack', () => {
  let template: Template;

  beforeAll(() => {
    const app = new cdk.App();
    const stack = new StandbyRecoveryStack(app, 'TestStack', {
      env: {
        account: '000000000000',
        region: 'us-east-1',
      },
      namePrefix: 'test',
    });
    template = Template.fromStack(stack);
  });

  test('creates the network boundary and private connectivity', () => {
    template.resourceCountIs('AWS::EC2::VPC', 1);
    template.resourceCountIs('AWS::EC2::Subnet', 4);
    template.resourceCountIs('AWS::EC2::NatGateway', 1);
    template.resourceCountIs('AWS::EC2::SecurityGroup', 3);
    template.resourceCountIs('AWS::EC2::VPCEndpoint', 2);

    template.hasResourceProperties('AWS::EC2::VPC', {
      CidrBlock: '10.40.0.0/16',
    });

    template.hasResourceProperties('AWS::EC2::VPCEndpoint', {
      VpcEndpointType: 'Gateway',
    });

    template.hasResourceProperties('AWS::EC2::SecurityGroupIngress', {
      FromPort: 5432,
      IpProtocol: 'tcp',
      ToPort: 5432,
    });
  });

  test('creates API and Lambda compute resources', () => {
    template.resourceCountIs('AWS::ApiGateway::RestApi', 1);
    template.resourceCountIs('AWS::ApiGateway::Method', 2);
    template.resourceCountIs('AWS::Lambda::Function', 3);

    template.hasResourceProperties('AWS::ApiGateway::Stage', {
      StageName: 'standby',
      MethodSettings: Match.arrayWith([
        Match.objectLike({
          LoggingLevel: 'INFO',
          MetricsEnabled: true,
        }),
      ]),
    });

    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'nodejs20.x',
      Environment: {
        Variables: Match.objectLike({
          AWS_ENDPOINT: Match.anyValue(),
        }),
      },
      VpcConfig: Match.anyValue(),
    });
  });

  test('creates messaging, events, and recovery workflow resources', () => {
    template.resourceCountIs('AWS::SQS::Queue', 3);
    template.resourceCountIs('AWS::Events::EventBus', 1);
    template.resourceCountIs('AWS::Events::Rule', 1);
    template.resourceCountIs('AWS::StepFunctions::StateMachine', 1);
    template.resourceCountIs('AWS::Pipes::Pipe', 1);

    template.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: Match.objectLike({
        maxReceiveCount: 3,
      }),
    });

    template.hasResourceProperties('AWS::Events::Rule', {
      EventPattern: {
        source: ['orders.api'],
      },
    });

    template.hasResourceProperties('AWS::Pipes::Pipe', {
      DesiredState: 'RUNNING',
      SourceParameters: {
        SqsQueueParameters: {
          BatchSize: 1,
          MaximumBatchingWindowInSeconds: 0,
        },
      },
      TargetParameters: {
        StepFunctionStateMachineParameters: {
          InvocationType: 'FIRE_AND_FORGET',
        },
      },
    });
  });

  test('creates the database and secret in private subnets', () => {
    template.resourceCountIs('AWS::SecretsManager::Secret', 1);
    template.resourceCountIs('AWS::RDS::DBSubnetGroup', 1);
    template.resourceCountIs('AWS::RDS::DBInstance', 1);

    template.hasResourceProperties('AWS::RDS::DBInstance', {
      AllocatedStorage: '20',
      DBInstanceClass: 'db.t3.micro',
      Engine: 'postgres',
      PubliclyAccessible: false,
      StorageEncrypted: true,
    });
  });

  test('creates analytics resources for recovery logs', () => {
    template.resourceCountIs('AWS::S3::Bucket', 1);
    template.resourceCountIs('AWS::Glue::Database', 1);
    template.resourceCountIs('AWS::Glue::Crawler', 1);
    template.resourceCountIs('AWS::Athena::WorkGroup', 1);

    template.hasResourceProperties('AWS::S3::Bucket', {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
      VersioningConfiguration: {
        Status: 'Enabled',
      },
    });

    template.hasResourceProperties('AWS::Athena::WorkGroup', {
      State: 'ENABLED',
      WorkGroupConfiguration: {
        EnforceWorkGroupConfiguration: true,
      },
    });
  });
});
