import json
import os
import time
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


STACK_NAME = os.environ.get('STACK_NAME', 'AppStack')
ASYNC_TIMEOUT_SECONDS = int(os.environ.get('ASYNC_TIMEOUT_SECONDS', '90'))
POLL_INTERVAL_SECONDS = int(os.environ.get('POLL_INTERVAL_SECONDS', '3'))


@lru_cache(maxsize=None)
def _aws_client(service_name):
    region = os.environ.get('AWS_REGION', 'us-east-1')
    return boto3.client(service_name, region_name=region)


@lru_cache(maxsize=1)
def _stack_outputs():
    cfn = _aws_client('cloudformation')
    stacks = cfn.describe_stacks(StackName=STACK_NAME)['Stacks']
    outputs = stacks[0].get('Outputs', [])
    return {output['OutputKey']: output['OutputValue'] for output in outputs}


@lru_cache(maxsize=1)
def _stack_resources():
    cfn = _aws_client('cloudformation')
    paginator = cfn.get_paginator('list_stack_resources')
    resources = []
    for page in paginator.paginate(StackName=STACK_NAME):
        resources.extend(page.get('StackResourceSummaries', []))
    return resources


def _resources_by_type(resource_type):
    return [resource for resource in _stack_resources() if resource['ResourceType'] == resource_type]


def _single_resource(resource_type):
    resources = _resources_by_type(resource_type)
    assert len(resources) == 1, f'Expected exactly one {resource_type} resource in {STACK_NAME}, got {resources}'
    return resources[0]


def _single_physical_id(resource_type):
    resource = _single_resource(resource_type)
    physical_id = resource.get('PhysicalResourceId')
    assert physical_id, f'{resource_type} in {STACK_NAME} is missing a physical resource id'
    return physical_id


def _resource_by_logical_id(logical_id):
    resource = next(
        (candidate for candidate in _stack_resources() if candidate['LogicalResourceId'] == logical_id),
        None,
    )
    assert resource is not None, f'Unable to find {logical_id} in stack resources'
    return resource


def _resource_by_logical_id_prefix(prefix, resource_type=None):
    matches = [
        candidate for candidate in _stack_resources()
        if candidate['LogicalResourceId'].startswith(prefix)
        and (resource_type is None or candidate['ResourceType'] == resource_type)
    ]
    assert len(matches) == 1, (
        f'Expected exactly one resource with logical id prefix {prefix}'
        f' and type {resource_type}, got {matches}'
    )
    return matches[0]


def _api_identifiers():
    parsed = urlparse(_stack_outputs()['ApiUrl'])
    api_id = (parsed.hostname or '').split('.')[0]
    stage_name = parsed.path.strip('/')
    assert api_id, f'Unable to extract API id from {_stack_outputs()["ApiUrl"]}'
    assert stage_name, f'Unable to extract API stage from {_stack_outputs()["ApiUrl"]}'
    return api_id, stage_name


def _queue_arn(queue_url):
    sqs = _aws_client('sqs')
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])['Attributes']
    return attrs['QueueArn']


def _exact_log_group(log_group_name):
    logs = _aws_client('logs')
    response = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
    matches = [group for group in response.get('logGroups', []) if group.get('logGroupName') == log_group_name]
    assert len(matches) == 1, f'Expected log group {log_group_name}, got {matches}'
    return matches[0]


@lru_cache(maxsize=1)
def _caller_account():
    return _aws_client('sts').get_caller_identity()['Account']


def _is_placeholder_account():
    return _caller_account() == '000000000000'


def _is_service_unavailable_error(exc):
    if not isinstance(exc, ClientError):
        return False
    error = exc.response.get('Error', {}) if exc.response else {}
    code = error.get('Code', '')
    message = error.get('Message', '')
    return code == 'InternalFailure' and 'not included within your' in message and 'license' in message


def _policy_statements(policy_document):
    statements = policy_document.get('Statement', [])
    if isinstance(statements, dict):
        return [statements]
    return statements


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _role_name_from_arn(role_arn):
    return role_arn.rsplit('/', 1)[-1]


def _lambda_role_name(function_name):
    lambda_client = _aws_client('lambda')
    config = lambda_client.get_function_configuration(FunctionName=function_name)
    return _role_name_from_arn(config['Role'])


def _role_inline_policies(role_name):
    iam = _aws_client('iam')
    policies = {}
    for policy_name in iam.list_role_policies(RoleName=role_name).get('PolicyNames', []):
        policy = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
        policies[policy_name] = policy['PolicyDocument']
    return policies


def _attached_policy_names(role_name):
    iam = _aws_client('iam')
    return {
        policy['PolicyName']
        for policy in iam.list_attached_role_policies(RoleName=role_name).get('AttachedPolicies', [])
    }


def _wait_for(description, predicate, timeout_seconds=ASYNC_TIMEOUT_SECONDS, interval_seconds=POLL_INTERVAL_SECONDS):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as exc:  # Integration polling needs the most recent failure context.
            last_error = exc
        time.sleep(interval_seconds)

    if last_error is not None:
        raise AssertionError(f'Timed out waiting for {description}: {last_error}') from last_error
    raise AssertionError(f'Timed out waiting for {description}')


def _contains_value(payload, expected):
    if isinstance(payload, dict):
        return any(_contains_value(value, expected) for value in payload.values())
    if isinstance(payload, list):
        return any(_contains_value(value, expected) for value in payload)
    if isinstance(payload, str):
        return expected in payload
    return payload == expected


def _contains_key_value(payload, expected_key, expected_value):
    if isinstance(payload, dict):
        if payload.get(expected_key) == expected_value:
            return True
        return any(_contains_key_value(value, expected_key, expected_value) for value in payload.values())
    if isinstance(payload, list):
        return any(_contains_key_value(value, expected_key, expected_value) for value in payload)
    return False


def _queue_messages(queue_url):
    sqs = _aws_client('sqs')
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=0,
        WaitTimeSeconds=1,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
    )
    return response.get('Messages', [])


def _api_post(api_url, path, payload):
    endpoint_override = os.environ.get('AWS_ENDPOINT', '').rstrip('/')
    if endpoint_override:
        parsed = urlparse(api_url)
        api_id = (parsed.hostname or '').split('.')[0]
        stage = parsed.path.strip('/')
        url = f'{endpoint_override}/restapis/{api_id}/{stage}/_user_request_{path}'
    else:
        url = api_url.rstrip('/') + path

    body = json.dumps(payload).encode('utf-8')
    req = urllib_request.Request(
        url,
        data=body,
        method='POST',
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as response:
            return response.status, response.read().decode('utf-8')
    except urllib_error.HTTPError as exc:
        return exc.code, exc.read().decode('utf-8')


def test_outputs_exist():
    outputs = _stack_outputs()
    required = {
        'ApiUrl',
        'QueueUrl',
        'DynamoDbTableName',
        'S3BucketName',
        'RdsEndpoint',
        'PrimaryLambdaArn',
        'EnrichmentLambdaArn',
    }
    assert required.issubset(outputs.keys())


def test_post_order_returns_success_response():
    outputs = _stack_outputs()
    apigw = _aws_client('apigateway')
    api_id, _ = _api_identifiers()

    api = apigw.get_rest_api(restApiId=api_id)
    assert api['id'] == api_id

    status, raw = _api_post(outputs['ApiUrl'], '/order', {'customer': 'abc', 'items': [1, 2]})
    assert status == 200
    parsed = json.loads(raw)
    assert parsed.get('ok') is True
    assert isinstance(parsed.get('key'), str) and parsed['key'].startswith('raw/')


def test_queue_configuration_runtime_values():
    outputs = _stack_outputs()
    sqs = _aws_client('sqs')

    queue_resources = _resources_by_type('AWS::SQS::Queue')
    assert len(queue_resources) == 2

    attrs = sqs.get_queue_attributes(
        QueueUrl=outputs['QueueUrl'],
        AttributeNames=['VisibilityTimeout', 'RedrivePolicy', 'QueueArn'],
    )['Attributes']
    assert attrs['VisibilityTimeout'] == '30'

    redrive = json.loads(attrs['RedrivePolicy'])
    assert redrive['maxReceiveCount'] == 3

    dlq_arn = redrive['deadLetterTargetArn']
    dlq_resource = next(
        (
            resource for resource in queue_resources
            if resource.get('PhysicalResourceId') != outputs['QueueUrl']
            and _queue_arn(resource['PhysicalResourceId']) == dlq_arn
        ),
        None,
    )
    assert dlq_resource is not None, f'Unable to match DLQ ARN {dlq_arn} to a stack queue resource'

    dlq_attrs = sqs.get_queue_attributes(
        QueueUrl=dlq_resource['PhysicalResourceId'],
        AttributeNames=['MessageRetentionPeriod'],
    )['Attributes']
    assert dlq_attrs['MessageRetentionPeriod'] == '1209600'


def test_primary_and_secondary_lambda_runtime_contracts():
    outputs = _stack_outputs()
    lambda_client = _aws_client('lambda')

    primary_name = outputs['PrimaryLambdaArn'].split(':')[-1]
    secondary_name = outputs['EnrichmentLambdaArn'].split(':')[-1]

    primary_cfg = lambda_client.get_function_configuration(FunctionName=primary_name)
    secondary_cfg = lambda_client.get_function_configuration(FunctionName=secondary_name)
    primary_concurrency = lambda_client.get_function_concurrency(FunctionName=primary_name)
    secondary_concurrency = lambda_client.get_function_concurrency(FunctionName=secondary_name)

    assert primary_cfg['Runtime'] == 'nodejs20.x'
    assert primary_cfg['MemorySize'] == 512
    assert primary_cfg['Timeout'] == 10
    if 'ReservedConcurrentExecutions' in primary_concurrency:
        assert primary_concurrency['ReservedConcurrentExecutions'] == 20
    else:
        assert _is_placeholder_account(), (
            f'Primary reserved concurrency missing outside placeholder account: {primary_concurrency}'
        )
    assert not primary_cfg.get('Layers'), f'Primary Lambda must not have layers, got {primary_cfg.get("Layers")}'

    assert secondary_cfg['Runtime'] == 'nodejs20.x'
    assert secondary_cfg['MemorySize'] == 256
    assert secondary_cfg['Timeout'] == 5
    if 'ReservedConcurrentExecutions' in secondary_concurrency:
        assert secondary_concurrency['ReservedConcurrentExecutions'] == 10
    else:
        assert _is_placeholder_account(), (
            f'Secondary reserved concurrency missing outside placeholder account: {secondary_concurrency}'
        )
    assert not secondary_cfg.get('Layers'), f'Secondary Lambda must not have layers, got {secondary_cfg.get("Layers")}'


def test_event_rule_pattern_and_target():
    outputs = _stack_outputs()
    events = _aws_client('events')

    rules = _resources_by_type('AWS::Events::Rule')
    assert len(rules) == 1

    rule_name = rules[0]['PhysicalResourceId']
    rule = events.describe_rule(Name=rule_name)
    pattern = json.loads(rule['EventPattern'])
    assert pattern == {'source': ['orders.api'], 'detail-type': ['OrderCreated']}

    targets = events.list_targets_by_rule(Rule=rule_name)['Targets']
    assert len(targets) == 1
    assert targets[0]['Arn'] == _queue_arn(outputs['QueueUrl'])


def test_pipe_contract_source_enrichment_target_and_invocation():
    outputs = _stack_outputs()
    pipes_client = _aws_client('pipes')

    pipe_resources = _resources_by_type('AWS::Pipes::Pipe')
    assert len(pipe_resources) == 1

    pipe_name = pipe_resources[0]['PhysicalResourceId']
    try:
        pipe = pipes_client.describe_pipe(Name=pipe_name)
    except ClientError as exc:
        assert _is_service_unavailable_error(exc), str(exc)
        assert _is_placeholder_account(), 'Pipes runtime may only be unavailable in the placeholder account'
        assert pipe_name
        return

    assert pipe['Source'] == _queue_arn(outputs['QueueUrl'])
    assert pipe['Enrichment'] == outputs['EnrichmentLambdaArn']
    assert pipe['Target'] == _single_physical_id('AWS::StepFunctions::StateMachine')

    source_parameters = pipe['SourceParameters']['SqsQueueParameters']
    assert source_parameters['BatchSize'] == 10

    sfn_params = pipe['TargetParameters']['StepFunctionStateMachineParameters']
    assert sfn_params['InvocationType'] == 'FIRE_AND_FORGET'


def test_state_machine_logging_is_enabled():
    sfn_client = _aws_client('stepfunctions')
    state_machine_arn = _single_physical_id('AWS::StepFunctions::StateMachine')

    detail = sfn_client.describe_state_machine(stateMachineArn=state_machine_arn)
    logging_config = detail['loggingConfiguration']
    assert logging_config['level'] != 'OFF'
    assert len(logging_config.get('destinations', [])) == 1


def test_runtime_observability_contracts():
    log_groups = _resources_by_type('AWS::Logs::LogGroup')
    assert len(log_groups) == 2

    for resource in log_groups:
        detail = _exact_log_group(resource['PhysicalResourceId'])
        if 'retentionInDays' in detail:
            assert detail['retentionInDays'] == 14
        else:
            assert _is_placeholder_account(), (
                f'Log group retention is missing from runtime response outside the placeholder account: {detail}'
            )
        assert not detail.get('kmsKeyId'), f'Log group must not use KMS: {detail}'

    apigw = _aws_client('apigateway')
    api_id, stage_name = _api_identifiers()
    stage = apigw.get_stage(restApiId=api_id, stageName=stage_name)

    method_settings = stage.get('methodSettings', {})
    if '*/*' in method_settings:
        assert method_settings['*/*']['metricsEnabled'] is True
        assert method_settings['*/*']['loggingLevel'] in {'ERROR', 'INFO'}
    else:
        assert _is_placeholder_account(), (
            f'API Gateway stage method settings missing outside placeholder account: {method_settings}'
        )
    assert 'accessLogSettings' not in stage


def test_runtime_iam_scoping_and_pipe_role_dedication():
    outputs = _stack_outputs()
    iam = _aws_client('iam')
    dynamodb = _aws_client('dynamodb')
    events_client = _aws_client('events')

    primary_name = outputs['PrimaryLambdaArn'].split(':')[-1]
    secondary_name = outputs['EnrichmentLambdaArn'].split(':')[-1]
    primary_role = _lambda_role_name(primary_name)
    secondary_role = _lambda_role_name(secondary_name)
    pipe_role = _resource_by_logical_id_prefix('OrdersPipeRole', 'AWS::IAM::Role')['PhysicalResourceId']

    # The only stack-owned standalone role is the pipe role; Lambda roles are discovered from runtime config.
    assert pipe_role not in {primary_role, secondary_role}

    primary_inline_policies = _role_inline_policies(primary_role)
    secondary_inline_policies = _role_inline_policies(secondary_role)
    pipe_inline_policies = _role_inline_policies(pipe_role)

    assert _attached_policy_names(primary_role) == {
        'AWSLambdaBasicExecutionRole',
        'AWSLambdaVPCAccessExecutionRole',
    }
    assert _attached_policy_names(secondary_role) == {'AWSLambdaBasicExecutionRole'}

    table_arn = dynamodb.describe_table(TableName=outputs['DynamoDbTableName'])['Table']['TableArn']
    bucket_object_arn = f'arn:aws:s3:::{outputs["S3BucketName"]}/*'
    default_bus_arn = events_client.describe_event_bus(Name='default')['Arn']
    queue_arn = _queue_arn(outputs['QueueUrl'])
    state_machine_arn = _single_physical_id('AWS::StepFunctions::StateMachine')

    primary_actions = set()
    primary_resources = set()
    for document in primary_inline_policies.values():
        for statement in _policy_statements(document):
            actions = set(_as_list(statement.get('Action')))
            resources = set(_as_list(statement.get('Resource')))
            assert '*' not in actions, f'Wildcard actions are not allowed in primary Lambda policies: {statement}'
            primary_actions.update(actions)
            primary_resources.update(resources)
    assert primary_actions == {'dynamodb:PutItem', 's3:PutObject', 'events:PutEvents'}
    assert primary_resources == {table_arn, bucket_object_arn, default_bus_arn}

    pipe_actions = set()
    pipe_resources = set()
    for document in pipe_inline_policies.values():
        for statement in _policy_statements(document):
            actions = set(_as_list(statement.get('Action')))
            resources = set(_as_list(statement.get('Resource')))
            assert '*' not in actions, f'Wildcard actions are not allowed in pipe policies: {statement}'
            pipe_actions.update(actions)
            pipe_resources.update(resources)
    assert pipe_actions == {
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
        'lambda:InvokeFunction',
        'states:StartExecution',
    }
    assert pipe_resources == {queue_arn, outputs['EnrichmentLambdaArn'], state_machine_arn}

    forbidden_secondary = {
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
        'dynamodb:DeleteItem',
        's3:PutObject',
        's3:DeleteObject',
    }
    for document in secondary_inline_policies.values():
        for statement in _policy_statements(document):
            actions = set(_as_list(statement.get('Action')))
            assert forbidden_secondary.isdisjoint(actions), (
                f'Secondary Lambda must not have DynamoDB/S3 write actions, got {actions}'
            )

    pipe_assume = iam.get_role(RoleName=pipe_role)['Role']['AssumeRolePolicyDocument']
    principals = _as_list(_policy_statements(pipe_assume)[0]['Principal']['Service'])
    assert principals == ['pipes.amazonaws.com']


def test_rds_private_encrypted_and_secret_backed_runtime_contract():
    outputs = _stack_outputs()
    if _is_placeholder_account():
        assert _resources_by_type('AWS::RDS::DBInstance') == []
        assert _resources_by_type('AWS::SecretsManager::Secret') == []
        assert outputs['RdsEndpoint'] == ''
        return

    rds_client = _aws_client('rds')
    secretsmanager = _aws_client('secretsmanager')

    db_identifier = _single_physical_id('AWS::RDS::DBInstance')
    db = rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)['DBInstances'][0]

    assert db['PubliclyAccessible'] is False
    assert db['DBInstanceClass'] == 'db.t3.micro'
    assert db['Engine'].lower() == 'postgres'
    assert str(db['EngineVersion']).startswith('14')
    assert db['StorageEncrypted'] is True
    assert outputs['RdsEndpoint'] == db['Endpoint']['Address']

    secret_arn = _single_physical_id('AWS::SecretsManager::Secret')
    secret = secretsmanager.describe_secret(SecretId=secret_arn)
    assert secret['ARN'] == secret_arn
    secret_value = json.loads(secretsmanager.get_secret_value(SecretId=secret_arn)['SecretString'])
    assert secret_value['host'] == outputs['RdsEndpoint']
    assert str(secret_value['port']) == '5432'
    assert secret_value['username'] == 'postgres'


def test_vpc_has_no_nat_gateway():
    ec2 = _aws_client('ec2')
    vpc_id = _single_physical_id('AWS::EC2::VPC')

    assert len(_resources_by_type('AWS::EC2::NatGateway')) == 0

    nat_gateways = ec2.describe_nat_gateways(
        Filter=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    ).get('NatGateways', [])
    active_nat_gateways = [gateway for gateway in nat_gateways if gateway.get('State') != 'deleted']
    assert active_nat_gateways == []


def test_no_sns_topics_in_stack():
    assert _resources_by_type('AWS::SNS::Topic') == []


def test_post_order_end_to_end_routes_through_async_pipeline():
    outputs = _stack_outputs()
    s3 = _aws_client('s3')
    dynamodb = _aws_client('dynamodb')
    sfn_client = _aws_client('stepfunctions')

    marker = f'e2e-{uuid.uuid4()}'
    payload = {'customer': marker, 'items': [42, 99]}
    state_machine_arn = _single_physical_id('AWS::StepFunctions::StateMachine')
    request_time = datetime.now(timezone.utc)

    status, raw = _api_post(outputs['ApiUrl'], '/order', payload)
    assert status == 200
    response = json.loads(raw)
    assert response.get('ok') is True
    key = response['key']
    sk = key.split('/')[-1].split('.')[0]

    _wait_for(
        'S3 object written by POST /order',
        lambda: s3.head_object(Bucket=outputs['S3BucketName'], Key=key),
    )

    item = _wait_for(
        'DynamoDB item written by POST /order',
        lambda: dynamodb.get_item(
            TableName=outputs['DynamoDbTableName'],
            Key={'pk': {'S': 'ORDER'}, 'sk': {'S': sk}},
            ConsistentRead=True,
        ).get('Item'),
    )
    assert json.loads(item['payload']['S']) == payload

    queue_message = _wait_for(
        'EventBridge rule delivered the order message to SQS',
        lambda: _find_queue_message_for_marker(outputs['QueueUrl'], marker),
    )
    assert _contains_value(queue_message, marker)

    if _is_placeholder_account():
        return

    execution = _wait_for(
        'Step Functions execution started from the async pipeline',
        lambda: _find_execution_for_marker(sfn_client, state_machine_arn, marker, request_time),
    )
    execution_detail = sfn_client.describe_execution(executionArn=execution['executionArn'])
    execution_input = json.loads(execution_detail['input'])
    assert _contains_value(execution_input, marker)
    assert _contains_key_value(execution_input, 'enriched', True), (
        f'Expected enrichment output in execution input, got {execution_input}'
    )


def _find_execution_for_marker(sfn_client, state_machine_arn, marker, request_time):
    executions = sfn_client.list_executions(
        stateMachineArn=state_machine_arn,
        maxResults=25,
    ).get('executions', [])

    for execution in executions:
        if execution['startDate'] < request_time:
            continue
        detail = sfn_client.describe_execution(executionArn=execution['executionArn'])
        execution_input = json.loads(detail['input'])
        if _contains_value(execution_input, marker):
            return execution
    return None


def _find_queue_message_for_marker(queue_url, marker):
    for message in _queue_messages(queue_url):
        body = message.get('Body', '')
        if marker in body:
            return body
    return None
