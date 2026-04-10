import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as vm from 'node:vm';
import {
  BackendLogicStack,
  applyEndpointOverrides,
  normalizeEnvironment,
  sanitizeName,
  synthesizeApp,
} from '../app';

type ResourceRecord = Record<string, any>;
type Entry = [string, any];

const ROOT = path.resolve(__dirname, '..');
const APP_SOURCE = fs.readFileSync(path.join(ROOT, 'app.ts'), 'utf8');

function synthesizeTemplate(): { template: Template; resources: ResourceRecord; templateJson: any } {
  const app = new cdk.App();
  const stack = new BackendLogicStack(app, 'BackendLogicStack', {
    env: { region: 'us-east-1', account: '123456789012' },
  });
  const template = Template.fromStack(stack);
  const templateJson = template.toJSON();
  return {
    template,
    resources: templateJson.Resources as ResourceRecord,
    templateJson,
  };
}

function entriesOfType(resources: ResourceRecord, type: string): Entry[] {
  return Object.entries(resources).filter(([, resource]) => resource.Type === type);
}

function onlyResource(resources: ResourceRecord, type: string, predicate?: (entry: Entry) => boolean): Entry {
  const matches = entriesOfType(resources, type).filter((entry) => (predicate ? predicate(entry) : true));
  expect(matches).toHaveLength(1);
  return matches[0];
}

function attachedPolicies(resources: ResourceRecord, roleLogicalId: string): Entry[] {
  return entriesOfType(resources, 'AWS::IAM::Policy').filter(([, policy]) =>
    (policy.Properties.Roles ?? []).some((roleRef: any) => roleRef.Ref === roleLogicalId),
  );
}

function flattenActions(action: string | string[]): string[] {
  return Array.isArray(action) ? action : [action];
}

function flattenResources(resource: any): any[] {
  return Array.isArray(resource) ? resource : [resource];
}

function flattenCfnString(value: any): string {
  if (typeof value === 'string') {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(flattenCfnString).join('');
  }
  if (value?.['Fn::Join']) {
    return flattenCfnString(value['Fn::Join'][1]);
  }
  if (value?.Ref) {
    return `<Ref:${value.Ref}>`;
  }
  if (value?.['Fn::GetAtt']) {
    const [logicalId, attribute] = value['Fn::GetAtt'];
    return `<GetAtt:${logicalId}.${attribute}>`;
  }
  return JSON.stringify(value);
}

function loadInlineHandler(
  code: string,
  env: Record<string, string>,
  modules: Record<string, unknown>,
  extras: Record<string, unknown> = {},
): (...args: any[]) => Promise<any> {
  const module = { exports: {} as Record<string, unknown> };
  const sandbox = {
    module,
    exports: module.exports,
    require: (name: string) => {
      if (!(name in modules)) {
        throw new Error(`Unexpected require: ${name}`);
      }
      return modules[name];
    },
    process: { env },
    console,
    Buffer,
    setTimeout,
    clearTimeout,
    ...extras,
  };

  vm.runInNewContext(code, sandbox);
  return module.exports.handler as (...args: any[]) => Promise<any>;
}

describe('source contract', () => {
  test('keeps the implementation in the single root app.ts file and avoids context inputs', () => {
    const rootTsFiles = fs
      .readdirSync(ROOT)
      .filter((file) => file.endsWith('.ts'))
      .sort();

    expect(rootTsFiles).toEqual(['app.ts']);
    expect(APP_SOURCE).not.toContain('tryGetContext(');
    expect(APP_SOURCE).not.toContain('node.getContext(');
    expect(APP_SOURCE).not.toContain('cdk.json context');
  });

  test('normalizeEnvironment uses the process environment when no source is provided', () => {
    process.env.AWS_ENDPOINT = '';
    process.env.AWS_REGION = 'eu-west-1';
    process.env.AWS_ACCESS_KEY_ID = 'default-key';
    process.env.AWS_SECRET_ACCESS_KEY = 'default-secret';

    expect(normalizeEnvironment()).toEqual({
      awsEndpoint: undefined,
      awsRegion: 'eu-west-1',
      awsAccessKeyId: 'default-key',
      awsSecretAccessKey: 'default-secret',
    });
  });

  test('normalizeEnvironment reads only the allowed external AWS inputs and defaults region to us-east-1', () => {
    expect(
      normalizeEnvironment({
        AWS_ENDPOINT: '  https://endpoint.internal  ',
        AWS_REGION: '  ',
        AWS_ACCESS_KEY_ID: 'key',
        AWS_SECRET_ACCESS_KEY: 'secret',
      } as NodeJS.ProcessEnv),
    ).toEqual({
      awsEndpoint: 'https://endpoint.internal',
      awsRegion: 'us-east-1',
      awsAccessKeyId: 'key',
      awsSecretAccessKey: 'secret',
    });
  });

  test('applyEndpointOverrides wires the provider endpoint base url variables', () => {
    delete process.env.AWS_ENDPOINT;
    delete process.env.AWS_ENDPOINT_URL;
    delete process.env.AWS_REGION;
    delete process.env.CDK_DEFAULT_REGION;

    applyEndpointOverrides({
      awsEndpoint: 'https://scheduler.internal',
      awsRegion: 'us-west-2',
    });

    expect(process.env.AWS_ENDPOINT).toBe('https://scheduler.internal');
    expect(process.env.AWS_ENDPOINT_URL).toBe('https://scheduler.internal');
    expect(process.env.AWS_REGION).toBe('us-west-2');
    expect(process.env.CDK_DEFAULT_REGION).toBe('us-west-2');
  });

  test('sanitizeName collapses invalid input and uses the fallback when needed', () => {
    expect(sanitizeName('Backend Logic__Stack', 'fallback')).toBe('backend-logic-stack');
    expect(sanitizeName('!!!', 'fallback')).toBe('fallback');
  });

  test('synthesizeApp works with only the allowed AWS inputs present', () => {
    const app = synthesizeApp({
      awsRegion: 'us-east-1',
      awsEndpoint: 'https://provider.internal',
      awsAccessKeyId: 'test-key',
      awsSecretAccessKey: 'test-secret',
    });

    expect(app).toBeInstanceOf(cdk.App);
  });

  test('synthesizeApp also works when it relies on normalizeEnvironment by default', () => {
    process.env.AWS_REGION = 'us-east-1';
    delete process.env.AWS_ENDPOINT;

    expect(synthesizeApp()).toBeInstanceOf(cdk.App);
  });
});

describe('network and api topology', () => {
  test('creates exactly one VPC fabric with two public and two private subnets plus one NAT gateway', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::EC2::VPC', 1);
    template.resourceCountIs('AWS::EC2::Subnet', 4);
    template.resourceCountIs('AWS::EC2::NatGateway', 1);
    template.resourceCountIs('AWS::EC2::SecurityGroup', 2);

    const subnets = entriesOfType(resources, 'AWS::EC2::Subnet').map(([, subnet]) => subnet.Properties);
    const publicSubnets = subnets.filter((subnet) => subnet.MapPublicIpOnLaunch === true);
    const privateSubnets = subnets.filter((subnet) => !subnet.MapPublicIpOnLaunch);

    expect(publicSubnets).toHaveLength(2);
    expect(privateSubnets).toHaveLength(2);
  });

  test('restricts the database security group to port 5432 from only the compute security group and leaves compute inbound closed', () => {
    const { template, resources } = synthesizeTemplate();
    const securityGroups = entriesOfType(resources, 'AWS::EC2::SecurityGroup');
    const ingressRules = entriesOfType(resources, 'AWS::EC2::SecurityGroupIngress');

    expect(securityGroups).toHaveLength(2);
    expect(ingressRules).toHaveLength(1);

    const [computeSecurityGroupId] = onlyResource(resources, 'AWS::EC2::SecurityGroup', ([, resource]) =>
      resource.Properties.GroupDescription === 'Compute tier security group',
    );
    const [databaseSecurityGroupId, databaseSecurityGroup] = onlyResource(resources, 'AWS::EC2::SecurityGroup', ([, resource]) =>
      resource.Properties.GroupDescription === 'Managed database tier security group',
    );
    const [, ingressRule] = ingressRules[0];

    expect(databaseSecurityGroup.Properties.SecurityGroupIngress).toBeUndefined();
    expect(ingressRule.Properties).toMatchObject({
      GroupId: { 'Fn::GetAtt': [databaseSecurityGroupId, 'GroupId'] },
      SourceSecurityGroupId: { 'Fn::GetAtt': [computeSecurityGroupId, 'GroupId'] },
      FromPort: 5432,
      ToPort: 5432,
      IpProtocol: 'tcp',
    });

    template.resourceCountIs('AWS::EC2::SecurityGroupEgress', 0);
  });

  test('builds a single REST API with the /orders resource and exactly GET and POST methods on the same lambda integration', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::ApiGateway::RestApi', 1);
    template.resourceCountIs('AWS::ApiGateway::Resource', 1);
    template.resourceCountIs('AWS::ApiGateway::Method', 2);

    const [, ordersResource] = onlyResource(resources, 'AWS::ApiGateway::Resource');
    expect(ordersResource.Properties.PathPart).toBe('orders');

    const methods = entriesOfType(resources, 'AWS::ApiGateway::Method');
    const httpMethods = methods.map(([, resource]) => resource.Properties.HttpMethod).sort();
    const integrationUris = methods.map(([, resource]) => JSON.stringify(resource.Properties.Integration.Uri));

    expect(httpMethods).toEqual(['GET', 'POST']);
    expect(new Set(integrationUris).size).toBe(1);
  });

  test('configures the API lambda for zip deployment inside the private subnets with the compute security group', () => {
    const { resources } = synthesizeTemplate();
    const [computeSecurityGroupId] = onlyResource(resources, 'AWS::EC2::SecurityGroup', ([, resource]) =>
      resource.Properties.GroupDescription === 'Compute tier security group',
    );
    const privateSubnets = entriesOfType(resources, 'AWS::EC2::Subnet')
      .filter(([, subnet]) => !subnet.Properties.MapPublicIpOnLaunch)
      .map(([logicalId]) => logicalId);
    const [, ordersLambda] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      Object.prototype.hasOwnProperty.call(resource.Properties.Environment?.Variables ?? {}, 'ORDER_QUEUE_URL'),
    );

    expect(ordersLambda.Properties).toMatchObject({
      Runtime: 'nodejs20.x',
      MemorySize: 256,
      Timeout: 10,
      Handler: 'index.handler',
    });
    expect(ordersLambda.Properties.Code.ZipFile).toEqual(expect.any(String));
    expect(ordersLambda.Properties.Code.ImageUri).toBeUndefined();
    expect(ordersLambda.Properties.VpcConfig.SecurityGroupIds).toEqual([
      { 'Fn::GetAtt': [computeSecurityGroupId, 'GroupId'] },
    ]);
    expect(ordersLambda.Properties.VpcConfig.SubnetIds).toHaveLength(2);
    expect(JSON.stringify(ordersLambda.Properties.VpcConfig.SubnetIds)).toContain(privateSubnets[0]);
    expect(JSON.stringify(ordersLambda.Properties.VpcConfig.SubnetIds)).toContain(privateSubnets[1]);
  });

  test('creates exactly one lambda log group with seven day retention and no kms encryption', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::Logs::LogGroup', 1);
    template.resourceCountIs('AWS::KMS::Key', 0);
    template.resourceCountIs('AWS::KMS::Alias', 0);

    const [, logGroup] = onlyResource(resources, 'AWS::Logs::LogGroup');
    expect(logGroup.Properties.RetentionInDays).toBe(7);
    expect(logGroup.Properties.KmsKeyId).toBeUndefined();
  });
});

describe('lambda business logic', () => {
  async function invokeOrdersHandler(
    event: Record<string, unknown>,
    envOverrides: Record<string, string> = {},
  ): Promise<{
    response: any;
    sentMessages: any[];
    secretRequests: any[];
    clientConfigs: any[];
    connections: any[];
  }> {
    const { resources } = synthesizeTemplate();
    const [, ordersLambda] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      Object.prototype.hasOwnProperty.call(resource.Properties.Environment?.Variables ?? {}, 'ORDER_QUEUE_URL'),
    );
    const sentMessages: any[] = [];
    const secretRequests: any[] = [];
    const clientConfigs: any[] = [];
    const connections: any[] = [];

    class SQSClient {
      constructor(config: any) {
        clientConfigs.push(config);
      }

      async send(command: any) {
        sentMessages.push(command.input);
        return {};
      }
    }

    class SendMessageCommand {
      constructor(public readonly input: any) {}
    }

    class SecretsManagerClient {
      constructor(config: any) {
        clientConfigs.push(config);
      }

      async send(command: any) {
        secretRequests.push(command.input);
        return {
          SecretString: JSON.stringify({
            dbname: 'ordersdb',
            port: 5432,
          }),
        };
      }
    }

    class GetSecretValueCommand {
      constructor(public readonly input: any) {}
    }

    const handler = loadInlineHandler(
      ordersLambda.Properties.Code.ZipFile,
      {
        AWS_REGION: 'us-east-1',
        AWS_ENDPOINT: 'https://provider.internal',
        ORDER_QUEUE_URL: 'queue-url',
        DB_SECRET_ARN: 'db-secret-arn',
        DB_HOST: 'db.internal',
        DB_PORT: '5432',
        DB_NAME: 'ordersdb',
        ...envOverrides,
      },
      {
        '@aws-sdk/client-sqs': { SQSClient, SendMessageCommand },
        '@aws-sdk/client-secrets-manager': { SecretsManagerClient, GetSecretValueCommand },
        'node:net': {
          createConnection: ({ host, port }: any, onConnect: () => void) => {
            connections.push({ host, port });
            const socket = {
              end: () => undefined,
              setTimeout: (_timeout: number, _onTimeout: () => void) => undefined,
              on: (_event: string, _handler: () => void) => undefined,
              destroy: () => undefined,
            };
            setTimeout(onConnect, 0);
            return socket;
          },
        },
      },
    );

    const response = await handler(event);
    return { response, sentMessages, secretRequests, clientConfigs, connections };
  }

  test('POST /orders enqueues a json order event and returns 202', async () => {
    const { response, sentMessages, clientConfigs } = await invokeOrdersHandler({
      httpMethod: 'POST',
      path: '/orders',
    });

    expect(response.statusCode).toBe(202);
    expect(sentMessages).toHaveLength(1);
    expect(clientConfigs[0]).toMatchObject({
      region: 'us-east-1',
      endpoint: 'https://provider.internal',
    });

    const body = JSON.parse(sentMessages[0].MessageBody);
    expect(body.kind).toBe('order-created');
    expect(body.orderId).toEqual(expect.stringMatching(/^order-/));
    expect(body.timestamp).toEqual(expect.any(String));
  });

  test('GET /orders reads the database secret, checks the database endpoint, and returns 200', async () => {
    const { response, secretRequests, connections, sentMessages } = await invokeOrdersHandler({
      httpMethod: 'GET',
      path: '/orders',
    });

    expect(response.statusCode).toBe(200);
    expect(secretRequests).toEqual([{ SecretId: 'db-secret-arn' }]);
    expect(connections).toEqual([{ host: 'db.internal', port: 5432 }]);
    expect(sentMessages).toHaveLength(0);

    const body = JSON.parse(response.body);
    expect(body).toMatchObject({
      ok: true,
      database: {
        host: 'db.internal',
        databaseName: 'ordersdb',
      },
    });
  });

  test('scheduled heartbeat invocations are distinguished from api requests without extra inputs', async () => {
    const { response, sentMessages } = await invokeOrdersHandler({
      source: 'scheduler',
      action: 'heartbeat',
    });

    expect(response.statusCode).toBe(202);
    expect(JSON.parse(sentMessages[0].MessageBody)).toMatchObject({
      kind: 'heartbeat',
    });
  });

  test('unsupported invocations return 405', async () => {
    const { response, sentMessages, secretRequests } = await invokeOrdersHandler({
      httpMethod: 'DELETE',
      path: '/orders',
    });

    expect(response.statusCode).toBe(405);
    expect(sentMessages).toHaveLength(0);
    expect(secretRequests).toHaveLength(0);
  });
});

describe('async pipeline and scheduler wiring', () => {
  test('configures the order queue with managed encryption, 30 second visibility timeout, and four day retention', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::SQS::Queue', 1);
    const [, queue] = onlyResource(resources, 'AWS::SQS::Queue');

    expect(queue.Properties).toMatchObject({
      SqsManagedSseEnabled: true,
      VisibilityTimeout: 30,
      MessageRetentionPeriod: 345600,
    });
  });

  test('scopes the api lambda sqs and secret permissions to the specific queue and database secret', () => {
    const { resources } = synthesizeTemplate();
    const [queueId] = onlyResource(resources, 'AWS::SQS::Queue');
    const [, ordersLambda] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      Object.prototype.hasOwnProperty.call(resource.Properties.Environment?.Variables ?? {}, 'ORDER_QUEUE_URL'),
    );
    const ordersRoleId = ordersLambda.Properties.Role['Fn::GetAtt'][0];
    const dbSecretArnReference = ordersLambda.Properties.Environment.Variables.DB_SECRET_ARN;
    const policies = attachedPolicies(resources, ordersRoleId);
    const sqsStatement = policies
      .flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement)
      .find((statement: any) => flattenActions(statement.Action).includes('sqs:SendMessage'));
    const secretStatement = policies
      .flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement)
      .find((statement: any) => flattenActions(statement.Action).includes('secretsmanager:GetSecretValue'));

    expect(sqsStatement.Resource).toEqual({ 'Fn::GetAtt': [queueId, 'Arn'] });
    expect(flattenActions(secretStatement.Action).sort()).toEqual([
      'secretsmanager:DescribeSecret',
      'secretsmanager:GetSecretValue',
    ]);
    expect(secretStatement.Resource).toEqual(dbSecretArnReference);
  });

  test('builds a standard state machine with a lambda task followed by a succeed state', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::StepFunctions::StateMachine', 1);
    const [, stateMachine] = onlyResource(resources, 'AWS::StepFunctions::StateMachine');
    const definitionString = flattenCfnString(stateMachine.Properties.DefinitionString);

    expect(stateMachine.Properties.StateMachineType).toBe('STANDARD');
    expect(definitionString).toContain('"Type":"Task"');
    expect(definitionString).toContain('"Type":"Succeed"');
    expect(definitionString).toContain('"WriteAuditRecord"');
    expect(definitionString).toContain('"ProcessingComplete"');
  });

  test('wires exactly one pipe from the queue to the state machine with a lambda enrichment step and scoped iam', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::Pipes::Pipe', 1);
    const [queueId] = onlyResource(resources, 'AWS::SQS::Queue');
    const [stateMachineId] = onlyResource(resources, 'AWS::StepFunctions::StateMachine');
    const [enrichmentLambdaId] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      resource.Properties.FunctionName === 'BackendLogicStack-pipe-enrichment',
    );
    const [, pipe] = onlyResource(resources, 'AWS::Pipes::Pipe');

    expect(pipe.Properties.Source).toEqual({ 'Fn::GetAtt': [queueId, 'Arn'] });
    expect(pipe.Properties.Target).toEqual({ Ref: stateMachineId });
    expect(pipe.Properties.Enrichment).toEqual({ 'Fn::GetAtt': [enrichmentLambdaId, 'Arn'] });
    expect(pipe.Properties.SourceParameters.SqsQueueParameters.BatchSize).toBe(1);
    expect(pipe.Properties.TargetParameters.StepFunctionStateMachineParameters.InvocationType).toBe('FIRE_AND_FORGET');

    const [pipeRoleId] = onlyResource(resources, 'AWS::IAM::Role', ([, role]) =>
      JSON.stringify(role.Properties.AssumeRolePolicyDocument).includes('pipes.amazonaws.com'),
    );
    const statements = attachedPolicies(resources, pipeRoleId).flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement);

    expect(statements.find((statement: any) => flattenActions(statement.Action).includes('sqs:ReceiveMessage')).Resource)
      .toEqual({ 'Fn::GetAtt': [queueId, 'Arn'] });
    expect(statements.find((statement: any) => flattenActions(statement.Action).includes('states:StartExecution')).Resource)
      .toEqual({ Ref: stateMachineId });
    expect(statements.find((statement: any) => flattenActions(statement.Action).includes('lambda:InvokeFunction')).Resource)
      .toEqual({ 'Fn::GetAtt': [enrichmentLambdaId, 'Arn'] });
  });

  test('creates a dedicated five minute scheduler that can invoke only the api lambda with fixed heartbeat json', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::Scheduler::Schedule', 1);
    const [ordersLambdaId] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      resource.Properties.FunctionName === 'BackendLogicStack-orders-handler',
    );
    const [, schedule] = onlyResource(resources, 'AWS::Scheduler::Schedule');

    expect(schedule.Properties.ScheduleExpression).toBe('rate(5 minutes)');
    expect(schedule.Properties.Target.Arn).toEqual({ 'Fn::GetAtt': [ordersLambdaId, 'Arn'] });
    expect(JSON.parse(schedule.Properties.Target.Input)).toEqual({
      source: 'scheduler',
      action: 'heartbeat',
    });

    const [schedulerRoleId] = onlyResource(resources, 'AWS::IAM::Role', ([, role]) =>
      JSON.stringify(role.Properties.AssumeRolePolicyDocument).includes('scheduler.amazonaws.com'),
    );
    const schedulerStatements = attachedPolicies(resources, schedulerRoleId)
      .flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement);

    expect(schedulerStatements).toHaveLength(1);
    expect(flattenActions(schedulerStatements[0].Action)).toEqual(['lambda:InvokeFunction']);
    expect(schedulerStatements[0].Resource).toEqual({ 'Fn::GetAtt': [ordersLambdaId, 'Arn'] });
  });
});

describe('data, catalog, audit, and least privilege', () => {
  test('creates encrypted postgres rds with generated secrets manager credentials and no plaintext password', () => {
    const { template, resources, templateJson } = synthesizeTemplate();

    template.resourceCountIs('AWS::RDS::DBInstance', 1);
    template.resourceCountIs('AWS::SecretsManager::Secret', 2);

    const [dbSecurityGroupId] = onlyResource(resources, 'AWS::EC2::SecurityGroup', ([, resource]) =>
      resource.Properties.GroupDescription === 'Managed database tier security group',
    );
    const [, dbInstance] = onlyResource(resources, 'AWS::RDS::DBInstance');
    const secrets = entriesOfType(resources, 'AWS::SecretsManager::Secret');

    expect(dbInstance.Properties).toMatchObject({
      DBInstanceClass: 'db.t3.micro',
      Engine: 'postgres',
      EngineVersion: '15.10',
      AllocatedStorage: '20',
      StorageEncrypted: true,
      PubliclyAccessible: false,
      DeletionProtection: false,
      VPCSecurityGroups: [{ 'Fn::GetAtt': [dbSecurityGroupId, 'GroupId'] }],
    });
    expect(flattenCfnString(dbInstance.Properties.MasterUserPassword)).toContain('{{resolve:secretsmanager:');
    expect(secrets).toEqual(
      expect.arrayContaining([
        expect.arrayContaining([
          expect.any(String),
          expect.objectContaining({
            Properties: expect.objectContaining({
              GenerateSecretString: expect.any(Object),
            }),
          }),
        ]),
      ]),
    );
    expect(JSON.stringify(templateJson)).not.toContain('"password":"');
  });

  test('creates a non-serverless encrypted redshift cluster and a jdbc glue connection/crawler targeting only redshift', () => {
    const { template, resources } = synthesizeTemplate();

    template.resourceCountIs('AWS::Redshift::Cluster', 1);
    template.resourceCountIs('AWS::Redshift::ClusterSubnetGroup', 1);
    template.resourceCountIs('AWS::Glue::Database', 1);
    template.resourceCountIs('AWS::Glue::Connection', 1);
    template.resourceCountIs('AWS::Glue::Crawler', 1);

    const [redshiftSecretId] = onlyResource(resources, 'AWS::SecretsManager::Secret', ([, resource]) =>
      JSON.stringify(resource.Properties.GenerateSecretString ?? {}).includes('clusteradmin'),
    );
    const [, redshiftCluster] = onlyResource(resources, 'AWS::Redshift::Cluster');
    const [, glueConnection] = onlyResource(resources, 'AWS::Glue::Connection');
    const [, crawler] = onlyResource(resources, 'AWS::Glue::Crawler');

    expect(redshiftCluster.Properties).toMatchObject({
      ClusterType: 'single-node',
      NodeType: 'dc2.large',
      NumberOfNodes: 1,
      Encrypted: true,
      PubliclyAccessible: false,
    });
    expect(flattenCfnString(redshiftCluster.Properties.MasterUserPassword)).toContain('{{resolve:secretsmanager:');
    expect(glueConnection.Properties.ConnectionInput.ConnectionType).toBe('JDBC');
    expect(flattenCfnString(glueConnection.Properties.ConnectionInput.ConnectionProperties.JDBC_CONNECTION_URL))
      .toContain('jdbc:redshift://');
    expect(glueConnection.Properties.ConnectionInput.AuthenticationConfiguration.SecretArn).toEqual({ Ref: redshiftSecretId });
    expect(crawler.Properties.Targets).toEqual({
      JdbcTargets: [
        {
          ConnectionName: glueConnection.Properties.ConnectionInput.Name,
          Path: '/',
        },
      ],
    });
  });

  test('creates an encrypted audit bucket with public access blocked, ssl enforcement, and processor putobject-only access', () => {
    const { template, resources, templateJson } = synthesizeTemplate();

    template.resourceCountIs('AWS::S3::Bucket', 1);
    const [bucketId, bucket] = onlyResource(resources, 'AWS::S3::Bucket');
    const [, bucketPolicy] = onlyResource(resources, 'AWS::S3::BucketPolicy');
    const [, processorLambda] = onlyResource(resources, 'AWS::Lambda::Function', ([, resource]) =>
      Object.prototype.hasOwnProperty.call(resource.Properties.Environment?.Variables ?? {}, 'AUDIT_BUCKET_NAME'),
    );
    const processorRoleId = processorLambda.Properties.Role['Fn::GetAtt'][0];

    expect(bucket.Properties).toMatchObject({
      BucketEncryption: {
        ServerSideEncryptionConfiguration: [
          {
            ServerSideEncryptionByDefault: {
              SSEAlgorithm: 'AES256',
            },
          },
        ],
      },
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    });
    expect(JSON.stringify(bucketPolicy.Properties.PolicyDocument)).toContain('aws:SecureTransport');

    const processorPolicy = attachedPolicies(resources, processorRoleId)
      .flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement)
      .find((statement: any) => flattenActions(statement.Action).includes('s3:PutObject'));

    expect(processorPolicy.Resource).toEqual({ 'Fn::Join': ['', [{ 'Fn::GetAtt': [bucketId, 'Arn'] }, '/*']] });
    expect(JSON.stringify(templateJson)).not.toContain('s3:ListAllMyBuckets');
  });

  test('scopes glue to the redshift secret, connection, catalog database, tables, and log namespace only', () => {
    const { resources } = synthesizeTemplate();

    const [glueRoleId] = onlyResource(resources, 'AWS::IAM::Role', ([, role]) =>
      JSON.stringify(role.Properties.AssumeRolePolicyDocument).includes('glue.amazonaws.com'),
    );
    const statements = attachedPolicies(resources, glueRoleId)
      .flatMap(([, policy]) => policy.Properties.PolicyDocument.Statement);
    const actionSets = statements.map((statement: any) => flattenActions(statement.Action));

    expect(actionSets).toEqual(
      expect.arrayContaining([
        ['secretsmanager:DescribeSecret', 'secretsmanager:GetSecretValue'],
        ['glue:GetConnection'],
        expect.arrayContaining(['glue:GetDatabase', 'glue:GetTable', 'glue:CreateTable']),
        ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      ]),
    );
  });

  test('keeps iam least-privilege, avoids wildcard actions, and limits wildcard resources to unavoidable ec2 statements', () => {
    const { resources, templateJson } = synthesizeTemplate();
    const policyResources = entriesOfType(resources, 'AWS::IAM::Policy');

    for (const [, policy] of policyResources) {
      for (const statement of policy.Properties.PolicyDocument.Statement) {
        const actions = flattenActions(statement.Action);
        const resourcesInStatement = flattenResources(statement.Resource);

        for (const action of actions) {
          expect(action).not.toBe('*');
          expect(action).not.toMatch(/:\*$/);
        }

        if (resourcesInStatement.some((resource) => resource === '*')) {
          expect(actions.every((action) => action.startsWith('ec2:'))).toBe(true);
        }
      }
    }

    expect(JSON.stringify(templateJson)).not.toContain('"Action":"*"');
    expect(JSON.stringify(templateJson)).not.toContain('"Action":["*"]');
  });

  test('avoids retain policies, termination protection, deletion protection, and customer managed kms resources', () => {
    const { resources, templateJson } = synthesizeTemplate();

    for (const resource of Object.values(resources)) {
      expect(resource.DeletionPolicy).not.toBe('Retain');
      expect(resource.UpdateReplacePolicy).not.toBe('Retain');
      expect(resource.UpdateReplacePolicy).not.toBe('Snapshot');
      expect(resource.Properties?.DeletionProtection).not.toBe(true);
      expect(resource.Properties?.EnableTerminationProtection).not.toBe(true);
    }

    expect(JSON.stringify(templateJson)).not.toContain('Retain');
    expect(JSON.stringify(templateJson)).not.toContain('TerminationProtection');
    expect(entriesOfType(resources, 'AWS::KMS::Key')).toHaveLength(0);
    expect(entriesOfType(resources, 'AWS::KMS::Alias')).toHaveLength(0);
  });
});
