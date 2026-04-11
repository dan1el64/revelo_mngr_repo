"""Cross-resource integration tests for the synthesized CDK template."""

import json
import pathlib
import sys

import aws_cdk as cdk
from aws_cdk import assertions

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app import ENRICHMENT_CODE, ThreeTierStack


def synth_template():
    app = cdk.App()
    stack = ThreeTierStack(
        app,
        "IntegrationTestStack",
        env=cdk.Environment(account="000000000000", region="us-east-1"),
    )
    return assertions.Template.from_stack(stack).to_json()


def resources_of(template, resource_type):
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == resource_type
    }


def count_ref(value, logical_id):
    if isinstance(value, dict):
        return sum(count_ref(v, logical_id) for v in value.values()) + (
            1 if value == {"Ref": logical_id} or value == {"Fn::GetAtt": [logical_id, "Arn"]} else 0
        )
    if isinstance(value, list):
        return sum(count_ref(item, logical_id) for item in value)
    return 0


def test_observability_config_and_cloudtrail_wiring():
    template = synth_template()

    recorders = resources_of(template, "AWS::Config::ConfigurationRecorder")
    channels = resources_of(template, "AWS::Config::DeliveryChannel")
    rules = resources_of(template, "AWS::Config::ConfigRule")
    trails = resources_of(template, "AWS::CloudTrail::Trail")
    log_groups = resources_of(template, "AWS::Logs::LogGroup")
    buckets = resources_of(template, "AWS::S3::Bucket")
    distributions = resources_of(template, "AWS::CloudFront::Distribution")
    distribution_config = next(iter(distributions.values()))["Properties"]["DistributionConfig"]
    frontend_bucket_id = next(
        origin["DomainName"]["Fn::GetAtt"][0]
        for origin in distribution_config["Origins"]
        if "S3OriginConfig" in origin
    )

    assert len(recorders) == 1
    recording_group = next(iter(recorders.values()))["Properties"]["RecordingGroup"]
    assert recording_group == {
        "AllSupported": True,
        "IncludeGlobalResourceTypes": True,
    }
    assert len(channels) == 1
    config_bucket_ref = next(iter(channels.values()))["Properties"]["S3BucketName"]
    assert config_bucket_ref in [{"Ref": bucket_id} for bucket_id in buckets]
    assert config_bucket_ref != {"Ref": frontend_bucket_id}

    assert len(rules) == 1
    rule = next(iter(rules.values()))
    assert rule["Properties"]["Source"] == {
        "Owner": "AWS",
        "SourceIdentifier": "CLOUD_TRAIL_ENABLED",
    }
    assert "ManagementTrail" in rule["DependsOn"]

    assert len(trails) == 1
    trail = next(iter(trails.values()))["Properties"]
    assert trail["IsMultiRegionTrail"] is False
    assert trail["IncludeGlobalServiceEvents"] is True
    assert trail["EventSelectors"] == [
        {"IncludeManagementEvents": True, "ReadWriteType": "WriteOnly"}
    ]
    assert "DataResources" not in trail["EventSelectors"][0]
    assert trail["S3BucketName"] in [{"Ref": bucket_id} for bucket_id in buckets]
    assert trail["S3BucketName"] != {"Ref": frontend_bucket_id}
    assert trail["S3BucketName"] != config_bucket_ref
    assert {frontend_bucket_id, config_bucket_ref["Ref"], trail["S3BucketName"]["Ref"]} == set(buckets)
    assert trail["CloudWatchLogsLogGroupArn"] == {"Fn::GetAtt": ["CloudTrailLogGroup", "Arn"]}
    assert trail["CloudWatchLogsRoleArn"] == {"Fn::GetAtt": ["CloudTrailLogsRole", "Arn"]}
    assert log_groups["CloudTrailLogGroup"]["Properties"]["RetentionInDays"] == 30
    assert "KmsKeyId" not in log_groups["CloudTrailLogGroup"]["Properties"]
    assert count_ref(template["Resources"], "ApiGatewayAccessLogGroup") == 1


def test_async_workflow_queue_pipe_and_state_machine_are_connected():
    template = synth_template()

    queues = resources_of(template, "AWS::SQS::Queue")
    pipes = resources_of(template, "AWS::Pipes::Pipe")
    state_machines = resources_of(template, "AWS::StepFunctions::StateMachine")
    log_groups = resources_of(template, "AWS::Logs::LogGroup")
    functions = resources_of(template, "AWS::Lambda::Function")

    assert len(queues) == 1
    queue = next(iter(queues.values()))["Properties"]
    assert queue["VisibilityTimeout"] == 30
    assert queue["SqsManagedSseEnabled"] is True
    assert "FifoQueue" not in queue

    enrichment = functions["EnrichmentLambda"]["Properties"]
    assert enrichment["Runtime"] == "python3.12"
    assert enrichment["Architectures"] == ["arm64"]
    assert enrichment["MemorySize"] == 256
    assert enrichment["Timeout"] == 5
    assert enrichment["LoggingConfig"]["LogGroup"] == {"Ref": "EnrichmentLambdaLogGroup"}
    assert "AWS_ENDPOINT" in enrichment["Environment"]["Variables"]
    assert '"name"' in ENRICHMENT_CODE
    assert 'os.environ.get("AWS_ENDPOINT")' in ENRICHMENT_CODE
    assert 'kwargs["endpoint_url"] = endpoint' in ENRICHMENT_CODE
    assert "INSERT INTO" not in ENRICHMENT_CODE
    assert "CREATE TABLE" not in ENRICHMENT_CODE
    assert "Connection(" not in ENRICHMENT_CODE
    assert "DB_ENDPOINT_ADDRESS" not in ENRICHMENT_CODE
    assert "DB_SECRET_ARN" not in ENRICHMENT_CODE

    assert len(state_machines) == 1
    sm_props = next(iter(state_machines.values()))["Properties"]
    assert sm_props["StateMachineType"] == "STANDARD"
    definition = json.loads(sm_props["DefinitionString"])
    substitutions = sm_props["DefinitionSubstitutions"]
    assert definition["StartAt"] == "EnrichMessage"
    assert definition["States"]["EnrichMessage"]["Resource"] == "arn:aws:states:::lambda:invoke"
    assert definition["States"]["EnrichMessage"]["Parameters"]["FunctionName"] == "${EnrichmentFunctionArn}"
    assert substitutions["EnrichmentFunctionArn"] == {"Fn::GetAtt": ["EnrichmentLambda", "Arn"]}
    assert definition["States"]["EnrichMessage"]["Next"] == "CreateItem"
    assert definition["States"]["CreateItem"]["Parameters"]["FunctionName"] == "${BackendFunctionArn}"
    assert substitutions["BackendFunctionArn"] == {"Fn::GetAtt": ["BackendApiLambda", "Arn"]}
    assert definition["States"]["CreateItem"]["Parameters"]["Payload"]["action"] == "create_item"
    assert sm_props["LoggingConfiguration"]["Level"] == "ALL"
    assert sm_props["LoggingConfiguration"]["Destinations"][0]["CloudWatchLogsLogGroup"]["LogGroupArn"] == {
        "Fn::GetAtt": ["WorkflowLogGroup", "Arn"]
    }
    assert log_groups["WorkflowLogGroup"]["Properties"]["RetentionInDays"] == 14
    assert "KmsKeyId" not in log_groups["WorkflowLogGroup"]["Properties"]

    assert len(pipes) == 1
    pipe = next(iter(pipes.values()))["Properties"]
    assert pipe["Source"] == {"Fn::GetAtt": ["ItemsQueue", "Arn"]}
    assert pipe["Enrichment"] == {"Fn::GetAtt": ["EnrichmentLambda", "Arn"]}
    assert pipe["Target"] == {"Fn::GetAtt": ["ProcessingStateMachine", "Arn"]}
    assert pipe["TargetParameters"]["StepFunctionStateMachineParameters"]["InvocationType"] == "FIRE_AND_FORGET"


def test_pipe_role_permissions_are_scoped_to_queue_enrichment_and_state_machine():
    template = synth_template()

    roles = resources_of(template, "AWS::IAM::Role")
    pipe_role = roles["EventPipeRole"]["Properties"]
    policy = pipe_role["Policies"][0]["PolicyDocument"]
    statements = policy["Statement"]
    sqs_statement = next(s for s in statements if s["Resource"] == {"Fn::GetAtt": ["ItemsQueue", "Arn"]})

    assert pipe_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "pipes.amazonaws.com"
    }
    assert sqs_statement["Effect"] == "Allow"
    assert sqs_statement["Resource"] == {"Fn::GetAtt": ["ItemsQueue", "Arn"]}
    assert set(sqs_statement["Action"]) == {
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ChangeMessageVisibility",
    }
    assert {
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": {"Fn::GetAtt": ["EnrichmentLambda", "Arn"]},
    } in statements
    assert {
        "Effect": "Allow",
        "Action": "states:StartExecution",
        "Resource": {"Fn::GetAtt": ["ProcessingStateMachine", "Arn"]},
    } in statements


def test_alb_http_listener_forwards_to_backend_lambda_target():
    template = synth_template()

    load_balancers = resources_of(template, "AWS::ElasticLoadBalancingV2::LoadBalancer")
    listeners = resources_of(template, "AWS::ElasticLoadBalancingV2::Listener")
    listener_rules = resources_of(template, "AWS::ElasticLoadBalancingV2::ListenerRule")
    target_groups = resources_of(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
    subnets = resources_of(template, "AWS::EC2::Subnet")
    public_subnet_refs = [
        {"Ref": logical_id}
        for logical_id, subnet in subnets.items()
        if subnet["Properties"].get("MapPublicIpOnLaunch") is True
    ]

    assert len(load_balancers) == 1
    lb = next(iter(load_balancers.values()))["Properties"]
    assert lb["Scheme"] == "internet-facing"
    assert lb["Type"] == "application"
    assert lb["Subnets"] == public_subnet_refs

    assert len([listener for listener in listeners.values() if listener["Properties"]["Port"] == 443]) == 0
    assert len([listener for listener in listeners.values() if listener["Properties"]["Port"] == 80]) == 1
    listener = next(iter(listeners.values()))["Properties"]
    assert listener["Port"] == 80
    assert listener["Protocol"] == "HTTP"
    assert listener["DefaultActions"][0]["Type"] == "forward"

    assert len(target_groups) == 1
    target_group = next(iter(target_groups.values()))["Properties"]
    target_group_ref = {"Ref": next(iter(target_groups))}
    assert listener["DefaultActions"][0]["TargetGroupArn"] == target_group_ref

    for listener_rule in listener_rules.values():
        assert listener_rule["Properties"]["Actions"][0]["TargetGroupArn"] == target_group_ref

    assert target_group["TargetType"] == "lambda"
    assert target_group["Targets"][0]["Id"] == {"Fn::GetAtt": ["BackendApiLambda", "Arn"]}


def test_no_vpc_link_or_alb_certificate_resources_are_defined():
    template = synth_template()

    assert resources_of(template, "AWS::ApiGateway::VpcLink") == {}
    assert resources_of(template, "AWS::ApiGatewayV2::VpcLink") == {}
    assert resources_of(template, "AWS::CertificateManager::Certificate") == {}


def test_api_gateway_and_alb_have_lambda_invoke_permissions():
    template = synth_template()

    permissions = resources_of(template, "AWS::Lambda::Permission")
    principals = {p["Properties"]["Principal"] for p in permissions.values()}
    assert principals == {"apigateway.amazonaws.com", "elasticloadbalancing.amazonaws.com"}
    for permission in permissions.values():
        assert permission["Properties"]["Action"] == "lambda:InvokeFunction"
        assert permission["Properties"]["FunctionName"] == {"Fn::GetAtt": ["BackendApiLambda", "Arn"]}


def test_database_initializer_role_is_dedicated_and_scoped():
    template = synth_template()

    roles = resources_of(template, "AWS::IAM::Role")
    init_role = roles["DatabaseInitLambdaRole"]["Properties"]
    functions = resources_of(template, "AWS::Lambda::Function")
    init_lambda = functions["DatabaseInitLambda"]["Properties"]
    statements = init_role["Policies"][0]["PolicyDocument"]["Statement"]

    assert init_role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "lambda.amazonaws.com"
    }
    assert init_lambda["Role"] == {"Fn::GetAtt": ["DatabaseInitLambdaRole", "Arn"]}
    for function_id, function in functions.items():
        if function_id != "DatabaseInitLambda":
            assert function["Properties"]["Role"] != {"Fn::GetAtt": ["DatabaseInitLambdaRole", "Arn"]}
    assert len(statements) == 2
    secret_statement = next(
        statement for statement in statements
        if statement["Action"] == "secretsmanager:GetSecretValue"
    )
    db_statement = next(
        statement for statement in statements
        if statement["Action"] == "rds:DescribeDBInstances"
    )
    assert secret_statement["Resource"] == {"Ref": "DatabaseCredentialsSecret"}
    assert db_statement["Resource"] != "*"
    assert db_statement["Resource"]["Fn::Sub"][0].endswith(":db:${DbInstanceId}")
    assert db_statement["Resource"]["Fn::Sub"][1]["DbInstanceId"] == {"Ref": "DatabaseInstance"}
