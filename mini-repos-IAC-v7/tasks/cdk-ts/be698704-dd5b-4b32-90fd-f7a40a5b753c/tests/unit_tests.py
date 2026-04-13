import json
import re
from pathlib import Path

import pytest


TEMPLATE_PATH = Path('template.json')


def _template():
    if not TEMPLATE_PATH.exists():
        pytest.fail('template.json not found; run synth before unit tests')
    return json.loads(TEMPLATE_PATH.read_text(encoding='utf-8'))


def _resources(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template.get('Resources', {}).items()
        if resource.get('Type') == resource_type
    }


def _find_one(resources, predicate, message):
    matches = [(rid, res) for rid, res in resources.items() if predicate(rid, res)]
    assert len(matches) == 1, message
    return matches[0]


def _zipfile_text(lambda_props):
    code = lambda_props.get('Code', {})
    zipfile = code.get('ZipFile')
    if isinstance(zipfile, str):
        return zipfile
    if isinstance(zipfile, dict) and 'Fn::Join' in zipfile:
        join_parts = zipfile['Fn::Join'][1]
        return ''.join([part for part in join_parts if isinstance(part, str)])
    return ''


def test_resource_counts():
    template = _template()
    assert len(_resources(template, 'AWS::Logs::LogGroup')) == 2
    assert len(_resources(template, 'AWS::Lambda::Function')) == 2
    assert len(_resources(template, 'AWS::ApiGateway::RestApi')) == 1
    assert len(_resources(template, 'AWS::Events::Rule')) == 1
    assert len(_resources(template, 'AWS::Pipes::Pipe')) == 1
    assert len(_resources(template, 'AWS::SQS::Queue')) == 2
    assert len(_resources(template, 'AWS::DynamoDB::Table')) == 1
    assert len(_resources(template, 'AWS::S3::Bucket')) == 1
    assert len(_resources(template, 'AWS::RDS::DBInstance')) == 1
    assert len(_resources(template, 'AWS::SNS::Topic')) == 0


def test_log_groups_association_and_retention_without_kms():
    template = _template()
    log_groups = _resources(template, 'AWS::Logs::LogGroup')

    for _, lg in log_groups.items():
        props = lg.get('Properties', {})
        assert props.get('RetentionInDays') == 14
        assert 'KmsKeyId' not in props

    lambdas = _resources(template, 'AWS::Lambda::Function')
    primary_id, _ = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected exactly one primary lambda with 512 MB',
    )

    _, primary_log_group = _find_one(
        log_groups,
        lambda _, r: r['Properties'].get('LogGroupName', {}).get('Fn::Join') is not None,
        'Expected exactly one explicit lambda log group',
    )

    joined = primary_log_group['Properties']['LogGroupName']['Fn::Join'][1]
    assert any(isinstance(p, dict) and p.get('Ref') == primary_id for p in joined)

    state_machines = _resources(template, 'AWS::StepFunctions::StateMachine')
    _, sfn = _find_one(state_machines, lambda *_: True, 'Expected one state machine')
    logging_cfg = sfn['Properties'].get('LoggingConfiguration', {})
    assert logging_cfg.get('Level') == 'ALL'
    destinations = logging_cfg.get('Destinations', [])
    assert len(destinations) == 1

    # Gap 9: SFN logging destination must reference an actual log group resource from this stack
    dest_log_grp_arn = destinations[0].get('CloudWatchLogsLogGroup', {}).get('LogGroupArn', {})
    assert isinstance(dest_log_grp_arn, dict) and 'Fn::GetAtt' in dest_log_grp_arn, (
        'SFN logging destination LogGroupArn must be an Fn::GetAtt intrinsic reference'
    )
    referenced_log_group_id = dest_log_grp_arn['Fn::GetAtt'][0]
    assert referenced_log_group_id in log_groups, (
        f'SFN logging destination must reference a log group resource in this stack, got: {referenced_log_group_id}'
    )


def test_api_stage_logging_metrics_without_access_log_group():
    template = _template()
    stages = _resources(template, 'AWS::ApiGateway::Stage')
    _, stage = _find_one(stages, lambda *_: True, 'Expected one api stage')
    props = stage['Properties']
    settings = props.get('MethodSettings', [])
    assert len(settings) >= 1
    # Must have a global stage-wide entry covering all resources and methods
    global_setting = next(
        (s for s in settings if s.get('ResourcePath') == '/*' and s.get('HttpMethod') == '*'),
        None,
    )
    assert global_setting is not None, (
        'API Gateway stage must have a global MethodSettings entry with ResourcePath=/* and HttpMethod=*'
    )
    assert global_setting.get('MetricsEnabled') is True
    logging_level = global_setting.get('LoggingLevel')
    assert logging_level and logging_level != 'OFF', (
        f'API Gateway stage must have execution logging enabled (LoggingLevel INFO or ERROR), got: {logging_level}'
    )
    assert 'AccessLogSetting' not in props


def test_primary_and_secondary_lambda_contract():
    template = _template()
    lambdas = _resources(template, 'AWS::Lambda::Function')

    _, primary = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected one primary lambda',
    )
    _, secondary = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 256,
        'Expected one secondary lambda',
    )

    p_props = primary['Properties']
    s_props = secondary['Properties']

    assert p_props.get('Runtime') == 'nodejs20.x'
    assert p_props.get('Timeout') == 10
    assert p_props.get('ReservedConcurrentExecutions') == 20

    assert s_props.get('Runtime') == 'nodejs20.x'
    assert s_props.get('Timeout') == 5
    assert s_props.get('ReservedConcurrentExecutions') == 10

    assert 'Layers' not in p_props
    assert 'Layers' not in s_props

    p_code = _zipfile_text(p_props)
    s_code = _zipfile_text(s_props)
    assert p_code
    assert s_code
    assert 'process.env.AWS_ENDPOINT' in p_code
    assert 'process.env.AWS_ENDPOINT' in s_code
    assert "|| 'us-east-1'" in p_code
    assert "|| 'us-east-1'" in s_code

    # Gap 2: AWS_ENDPOINT must appear in the Lambda Environment.Variables (not just inline code)
    p_env = p_props.get('Environment', {}).get('Variables', {})
    s_env = s_props.get('Environment', {}).get('Variables', {})
    assert 'AWS_ENDPOINT' in p_env, 'Primary Lambda must expose AWS_ENDPOINT in Environment.Variables'
    assert 'AWS_ENDPOINT' in s_env, 'Secondary Lambda must expose AWS_ENDPOINT in Environment.Variables'

    # Gap 3: No unexpected environment variable keys beyond what the implementation requires
    primary_expected_env = {'TABLE_NAME', 'BUCKET_NAME', 'QUEUE_URL', 'AWS_ENDPOINT'}
    secondary_expected_env = {'AWS_ENDPOINT'}
    assert set(p_env.keys()) == primary_expected_env, (
        f'Primary Lambda Environment.Variables must be exactly {primary_expected_env}, '
        f'got: {set(p_env.keys())}'
    )
    assert set(s_env.keys()) == secondary_expected_env, (
        f'Secondary Lambda Environment.Variables must be exactly {secondary_expected_env}, '
        f'got: {set(s_env.keys())}'
    )


def test_lambda_invocation_sources_are_constrained():
    template = _template()
    lambdas = _resources(template, 'AWS::Lambda::Function')
    primary_id, _ = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected one primary lambda',
    )
    secondary_id, _ = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 256,
        'Expected one secondary lambda',
    )

    permissions = _resources(template, 'AWS::Lambda::Permission')
    primary_permissions = [
        p for p in permissions.values() if p['Properties'].get('FunctionName', {}).get('Fn::GetAtt', [None])[0] == primary_id
    ]
    assert len(primary_permissions) >= 1
    for permission in primary_permissions:
        assert permission['Properties'].get('Principal') == 'apigateway.amazonaws.com'

    secondary_permissions = [
        p for p in permissions.values() if p['Properties'].get('FunctionName', {}).get('Fn::GetAtt', [None])[0] == secondary_id
    ]
    assert len(secondary_permissions) == 0


def test_connectivity_rule_queue_pipe_and_method():
    template = _template()

    methods = _resources(template, 'AWS::ApiGateway::Method')
    post_method = [m for m in methods.values() if m['Properties'].get('HttpMethod') == 'POST']
    assert len(post_method) == 1

    # Gap 1: POST /order integration must target the primary Lambda (512 MB)
    lambdas = _resources(template, 'AWS::Lambda::Function')
    primary_lambda_id, _ = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected one primary lambda with 512 MB',
    )
    integration = post_method[0]['Properties'].get('Integration', {})
    assert integration.get('Type') in ('AWS_PROXY', 'AWS'), (
        'POST /order must use a Lambda (AWS_PROXY or AWS) integration'
    )
    assert primary_lambda_id in json.dumps(integration.get('Uri', {})), (
        f'POST /order integration URI must reference the primary Lambda (logical ID: {primary_lambda_id})'
    )

    rules = _resources(template, 'AWS::Events::Rule')
    _, rule = _find_one(rules, lambda *_: True, 'Expected one events rule')
    pattern = rule['Properties'].get('EventPattern', {})
    assert pattern.get('source') == ['orders.api']
    assert pattern.get('detail-type') == ['OrderCreated']

    queues = _resources(template, 'AWS::SQS::Queue')
    main_queue_id, main_queue = _find_one(
        queues,
        lambda _, q: 'RedrivePolicy' in q.get('Properties', {}),
        'Expected one main queue with DLQ redrive',
    )
    _, dlq = _find_one(
        queues,
        lambda _, q: q.get('Properties', {}).get('MessageRetentionPeriod') == 1209600,
        'Expected one DLQ with 14-day retention',
    )

    assert main_queue['Properties'].get('VisibilityTimeout') == 30
    redrive = main_queue['Properties'].get('RedrivePolicy', {})
    assert redrive.get('maxReceiveCount') == 3
    assert redrive.get('deadLetterTargetArn') is not None

    rule_targets = rule['Properties'].get('Targets', [])
    assert len(rule_targets) == 1
    target_arn = rule_targets[0].get('Arn')
    assert target_arn.get('Fn::GetAtt', [None])[0] == main_queue_id

    pipes_res = _resources(template, 'AWS::Pipes::Pipe')
    _, pipe = _find_one(pipes_res, lambda *_: True, 'Expected one pipe')
    pipe_props = pipe['Properties']

    assert pipe_props.get('Source', {}).get('Fn::GetAtt', [None])[0] == main_queue_id
    assert pipe_props.get('Enrichment', {}).get('Fn::GetAtt', [None])[0] in _resources(template, 'AWS::Lambda::Function')
    assert pipe_props.get('Target', {}).get('Ref') in _resources(template, 'AWS::StepFunctions::StateMachine')

    target_params = pipe_props.get('TargetParameters', {})
    sfn_params = target_params.get('StepFunctionStateMachineParameters', {})
    assert sfn_params.get('InvocationType') == 'FIRE_AND_FORGET'


def test_dynamodb_s3_and_rds_requirements():
    template = _template()

    tables = _resources(template, 'AWS::DynamoDB::Table')
    _, table = _find_one(tables, lambda *_: True, 'Expected one dynamodb table')
    t_props = table['Properties']

    assert t_props.get('BillingMode') == 'PAY_PER_REQUEST'
    ttl = t_props.get('TimeToLiveSpecification', {})
    assert ttl.get('Enabled') is True
    assert ttl.get('AttributeName') == 'expiresAt'

    pitr = t_props.get('PointInTimeRecoverySpecification', {})
    assert pitr.get('PointInTimeRecoveryEnabled') is True

    # Gap 10: DynamoDB key schema must use pk (HASH) and sk (RANGE) as required by the write path
    key_schema = t_props.get('KeySchema', [])
    hash_key = next((k for k in key_schema if k.get('KeyType') == 'HASH'), None)
    range_key = next((k for k in key_schema if k.get('KeyType') == 'RANGE'), None)
    assert hash_key is not None and hash_key.get('AttributeName') == 'pk', (
        f'DynamoDB partition key must be "pk", got: {hash_key}'
    )
    assert range_key is not None and range_key.get('AttributeName') == 'sk', (
        f'DynamoDB sort key must be "sk", got: {range_key}'
    )

    buckets = _resources(template, 'AWS::S3::Bucket')
    _, bucket = _find_one(buckets, lambda *_: True, 'Expected one bucket')
    b_props = bucket['Properties']

    pab = b_props.get('PublicAccessBlockConfiguration', {})
    assert pab.get('BlockPublicAcls') is True
    assert pab.get('BlockPublicPolicy') is True
    assert pab.get('IgnorePublicAcls') is True
    assert pab.get('RestrictPublicBuckets') is True

    sse_cfg = b_props.get('BucketEncryption', {}).get('ServerSideEncryptionConfiguration', [])
    assert len(sse_cfg) == 1
    assert sse_cfg[0]['ServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256'

    policies = _resources(template, 'AWS::S3::BucketPolicy')
    _, policy = _find_one(policies, lambda *_: True, 'Expected one bucket policy')
    statements = policy['Properties']['PolicyDocument']['Statement']
    ssl_deny = [
        s
        for s in statements
        if s.get('Effect') == 'Deny'
        and s.get('Condition', {}).get('Bool', {}).get('aws:SecureTransport') == 'false'
    ]
    assert len(ssl_deny) >= 1

    dbs = _resources(template, 'AWS::RDS::DBInstance')
    _, db = _find_one(dbs, lambda *_: True, 'Expected one db instance')
    d_props = db['Properties']
    assert d_props.get('Engine') == 'postgres'
    assert str(d_props.get('EngineVersion', '')).startswith('14')
    assert d_props.get('DBInstanceClass') == 'db.t3.micro'
    assert d_props.get('PubliclyAccessible') is False
    assert d_props.get('StorageEncrypted') is True
    assert d_props.get('DeletionProtection') is False

    db_subnet_groups = _resources(template, 'AWS::RDS::DBSubnetGroup')
    assert len(db_subnet_groups) == 1

    # Gap 2: RDS credentials must be backed by Secrets Manager (Credentials.fromGeneratedSecret)
    secrets = _resources(template, 'AWS::SecretsManager::Secret')
    assert len(secrets) >= 1, (
        'RDS must use Credentials.fromGeneratedSecret, which creates an AWS::SecretsManager::Secret resource'
    )

    # Gap 3: DB subnet group must reference only private (non-public) subnets
    _, db_subnet_group = next(iter(db_subnet_groups.items()))
    subnet_refs = db_subnet_group['Properties'].get('SubnetIds', [])
    ec2_subnets = _resources(template, 'AWS::EC2::Subnet')
    for subnet_ref in subnet_refs:
        if isinstance(subnet_ref, dict) and 'Ref' in subnet_ref:
            subnet_logical_id = subnet_ref['Ref']
            if subnet_logical_id in ec2_subnets:
                subnet_props = ec2_subnets[subnet_logical_id]['Properties']
                assert subnet_props.get('MapPublicIpOnLaunch') is not True, (
                    f'DB subnet group must not reference public subnets; {subnet_logical_id} '
                    f'has MapPublicIpOnLaunch=true'
                )


def test_rds_security_group_allows_only_primary_lambda_on_5432():
    template = _template()
    ingress_rules = _resources(template, 'AWS::EC2::SecurityGroupIngress')

    lambda_source_5432 = [
        r
        for r in ingress_rules.values()
        if r['Properties'].get('FromPort') == 5432
        and r['Properties'].get('ToPort') == 5432
        and r['Properties'].get('IpProtocol') == 'tcp'
        and 'SourceSecurityGroupId' in r['Properties']
        and 'CidrIp' not in r['Properties']
        and 'CidrIpv6' not in r['Properties']
    ]
    assert len(lambda_source_5432) >= 1

    # Gap 5: SourceSecurityGroupId must reference the primary Lambda's own security group
    lambdas = _resources(template, 'AWS::Lambda::Function')
    _, primary_lambda = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected one primary lambda with 512 MB',
    )
    primary_sg_logical_ids = set()
    for sg_ref in primary_lambda['Properties'].get('VpcConfig', {}).get('SecurityGroupIds', []):
        if isinstance(sg_ref, dict):
            if 'Fn::GetAtt' in sg_ref:
                primary_sg_logical_ids.add(sg_ref['Fn::GetAtt'][0])
            elif 'Ref' in sg_ref:
                primary_sg_logical_ids.add(sg_ref['Ref'])

    for rule in lambda_source_5432:
        source_ref = rule['Properties'].get('SourceSecurityGroupId', {})
        if isinstance(source_ref, dict):
            if 'Fn::GetAtt' in source_ref:
                source_logical_id = source_ref['Fn::GetAtt'][0]
            elif 'Ref' in source_ref:
                source_logical_id = source_ref['Ref']
            else:
                source_logical_id = None
            assert source_logical_id in primary_sg_logical_ids, (
                f'DB ingress rule on port 5432 must reference the primary Lambda security group '
                f'(one of {primary_sg_logical_ids}), got: {source_logical_id}'
            )


def test_iam_scope_and_pipe_dedicated_role():
    template = _template()

    roles = _resources(template, 'AWS::IAM::Role')
    pipes_roles = [
        rid for rid, role in roles.items()
        if role['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Principal'].get('Service') == 'pipes.amazonaws.com'
    ]
    lambda_roles = [
        rid for rid, role in roles.items()
        if role['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Principal'].get('Service') == 'lambda.amazonaws.com'
    ]
    assert len(pipes_roles) >= 1
    for pr in pipes_roles:
        assert pr not in lambda_roles

    policies = _resources(template, 'AWS::IAM::Policy')
    assert len(policies) > 0

    for _, pol in policies.items():
        statements = pol['Properties']['PolicyDocument']['Statement']
        for stmt in statements:
            actions = stmt.get('Action', [])
            resources = stmt.get('Resource', [])
            if isinstance(actions, str):
                actions = [actions]
            if isinstance(resources, str):
                resources = [resources]
            elif isinstance(resources, dict):
                resources = [resources]

            for action in actions:
                # No policy may use the catch-all wildcard action '*'
                assert action != '*', f'Wildcard action (*) is not allowed, found in: {actions}'
            for resource in resources:
                if resource != '*':
                    # Non-wildcard resources must be CDK intrinsic references, not literal ARN strings
                    assert isinstance(resource, dict), (
                        f'IAM resource must be a CDK intrinsic reference (Fn::GetAtt, Ref, etc.), '
                        f'not a literal ARN string: {resource}'
                    )
                # resource == '*' is permitted for named (non-wildcard) service-level actions
                # such as Step Functions log-delivery actions that do not support resource scoping.


def test_secondary_role_has_no_dynamodb_or_s3_write_actions():
    template = _template()

    # Redundant Test 1 fix: find secondary role via Lambda intrinsic reference, not by logical ID fragment
    lambdas = _resources(template, 'AWS::Lambda::Function')
    _, secondary_lambda = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 256,
        'Expected one secondary lambda with 256 MB',
    )
    role_ref = secondary_lambda['Properties'].get('Role', {})
    assert isinstance(role_ref, dict) and 'Fn::GetAtt' in role_ref, (
        'Secondary Lambda Role property must be an intrinsic Fn::GetAtt reference'
    )
    secondary_role_id = role_ref['Fn::GetAtt'][0]

    policies = _resources(template, 'AWS::IAM::Policy')
    secondary_policies = [
        p
        for p in policies.values()
        if any(
            isinstance(r, dict) and r.get('Ref') == secondary_role_id
            for r in p['Properties'].get('Roles', [])
        )
    ]

    forbidden = {'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem', 's3:PutObject', 's3:DeleteObject'}

    for pol in secondary_policies:
        statements = pol['Properties']['PolicyDocument']['Statement']
        for stmt in statements:
            actions = stmt.get('Action', [])
            if isinstance(actions, str):
                actions = [actions]
            assert forbidden.isdisjoint(set(actions))


def test_no_retain_or_termination_protection_patterns():
    template = _template()

    for _, resource in template.get('Resources', {}).items():
        assert resource.get('DeletionPolicy') != 'Retain'
        assert resource.get('UpdateReplacePolicy') != 'Retain'


def test_output_contract():
    template = _template()
    outputs = set(template.get('Outputs', {}).keys())
    required = {
        'ApiUrl',
        'QueueUrl',
        'DynamoDbTableName',
        'S3BucketName',
        'RdsEndpoint',
        'PrimaryLambdaArn',
        'EnrichmentLambdaArn',
    }
    assert required.issubset(outputs)


def test_no_manual_resource_names():
    """Gap 5: Resource names must not be manually specified; rely on CDK-generated names."""
    template = _template()

    for rid, res in _resources(template, 'AWS::DynamoDB::Table').items():
        assert 'TableName' not in res.get('Properties', {}), (
            f'DynamoDB table {rid} must not have a manually specified TableName'
        )

    for rid, res in _resources(template, 'AWS::SQS::Queue').items():
        assert 'QueueName' not in res.get('Properties', {}), (
            f'SQS queue {rid} must not have a manually specified QueueName'
        )

    for rid, res in _resources(template, 'AWS::S3::Bucket').items():
        assert 'BucketName' not in res.get('Properties', {}), (
            f'S3 bucket {rid} must not have a manually specified BucketName'
        )

    for rid, res in _resources(template, 'AWS::Lambda::Function').items():
        assert 'FunctionName' not in res.get('Properties', {}), (
            f'Lambda function {rid} must not have a manually specified FunctionName'
        )

    for rid, res in _resources(template, 'AWS::StepFunctions::StateMachine').items():
        assert 'StateMachineName' not in res.get('Properties', {}), (
            f'State machine {rid} must not have a manually specified StateMachineName'
        )


def test_no_unauthorized_parameters_or_hardcoded_accounts():
    """Gap 4: No custom parameters, no hardcoded account IDs."""
    template = _template()

    # CDK adds BootstrapVersion; no user-introduced parameters are allowed
    for key in template.get('Parameters', {}):
        assert key.startswith('Bootstrap'), (
            f'Unexpected custom parameter: {key}. Only CDK-generated parameters (BootstrapVersion) are allowed.'
        )

    # No hardcoded 12-digit AWS account IDs (CDK placeholder 000000000000 is permitted)
    template_str = json.dumps(template)
    real_account_ids = re.findall(r'(?<!\d)(?!000000000000\b)\d{12}(?!\d)', template_str)
    assert len(real_account_ids) == 0, (
        f'Found hardcoded AWS account IDs in template: {real_account_ids}'
    )


def test_primary_role_scoped_to_api_write_path():
    """Gap 6: Primary Lambda role must be limited to API write path actions only."""
    template = _template()

    lambdas = _resources(template, 'AWS::Lambda::Function')
    _, primary_lambda = _find_one(
        lambdas,
        lambda _, r: r['Properties'].get('MemorySize') == 512,
        'Expected one primary lambda with 512 MB',
    )

    role_ref = primary_lambda['Properties'].get('Role', {})
    assert isinstance(role_ref, dict) and 'Fn::GetAtt' in role_ref, (
        'Primary Lambda Role property must be an intrinsic Fn::GetAtt reference'
    )
    primary_role_id = role_ref['Fn::GetAtt'][0]

    policies = _resources(template, 'AWS::IAM::Policy')
    primary_policies = [
        p
        for p in policies.values()
        if any(
            isinstance(r, dict) and r.get('Ref') == primary_role_id
            for r in p['Properties'].get('Roles', [])
        )
    ]

    # Primary Lambda role must not include SFN invocation or pipe/enrichment-related permissions
    forbidden_in_primary = {
        'states:StartExecution',
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'lambda:InvokeFunction',
    }
    for pol in primary_policies:
        for stmt in pol['Properties']['PolicyDocument']['Statement']:
            actions = stmt.get('Action', [])
            if isinstance(actions, str):
                actions = [actions]
            overlap = forbidden_in_primary.intersection(set(actions))
            assert not overlap, (
                f'Primary Lambda role must not include {overlap}; '
                f'those permissions belong to the pipe or secondary role'
            )
