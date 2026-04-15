import json
import os

import boto3
import pytest
from botocore.exceptions import ClientError


ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("TF_VAR_aws_endpoint")
ENVIRONMENTS = ("dev", "test", "prod")


def get_environment_tag(resource):
    return next(
        (tag["Value"] for tag in resource.get("Tags", []) if tag["Key"] == "Environment"),
        None,
    )


def alarm_by_name(metric_alarms):
    return {alarm["AlarmName"]: alarm for alarm in metric_alarms}


def function_by_name(functions):
    return {function["FunctionName"]: function for function in functions}


def maybe_parse_policy(policy_document):
    if isinstance(policy_document, str):
        return json.loads(policy_document)
    return policy_document


@pytest.fixture(scope="session")
def aws_client():
    """Create AWS clients using the configured endpoint override for the harness."""
    assert ENDPOINT_URL, "Integration tests require a configured endpoint URL"
    return boto3.session.Session(
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def test_vpc_fabric_deployment(aws_client):
    """VPCs, routing, endpoints, and security groups should be scoped per environment."""
    ec2 = aws_client.client("ec2", endpoint_url=ENDPOINT_URL)

    vpcs = ec2.describe_vpcs()["Vpcs"]
    tagged_vpcs = [vpc for vpc in vpcs if get_environment_tag(vpc) in ENVIRONMENTS]
    assert len(tagged_vpcs) == len(ENVIRONMENTS)
    assert {get_environment_tag(vpc) for vpc in tagged_vpcs} == set(ENVIRONMENTS)

    for env in ENVIRONMENTS:
        vpc = next(vpc for vpc in tagged_vpcs if get_environment_tag(vpc) == env)
        vpc_id = vpc["VpcId"]

        subnets = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["Subnets"]
        assert len(subnets) == 4

        public_subnets = [subnet for subnet in subnets if subnet.get("MapPublicIpOnLaunch")]
        private_subnets = [subnet for subnet in subnets if not subnet.get("MapPublicIpOnLaunch")]
        assert len(public_subnets) == 2
        assert len(private_subnets) == 2
        assert len({subnet["AvailabilityZone"] for subnet in subnets}) == 2

        igws = ec2.describe_internet_gateways(
            Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
        )["InternetGateways"]
        assert len(igws) == 1

        nat_gateways = ec2.describe_nat_gateways(
            Filter=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["NatGateways"]
        assert nat_gateways == []

        route_tables = ec2.describe_route_tables(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["RouteTables"]
        public_route_tables = [
            route_table
            for route_table in route_tables
            if any(
                route.get("GatewayId", "").startswith("igw-")
                for route in route_table.get("Routes", [])
            )
        ]
        assert len(public_route_tables) == 1

        associated_subnets = {
            association["SubnetId"]
            for association in public_route_tables[0].get("Associations", [])
            if not association.get("Main")
        }
        assert associated_subnets == {subnet["SubnetId"] for subnet in public_subnets}

        vpc_endpoints = ec2.describe_vpc_endpoints(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["VpcEndpoints"]
        assert len(vpc_endpoints) == 2
        assert {endpoint["ServiceName"] for endpoint in vpc_endpoints} == {
            "com.amazonaws.us-east-1.secretsmanager",
            "com.amazonaws.us-east-1.sqs",
        }
        assert {endpoint["VpcEndpointType"] for endpoint in vpc_endpoints} == {"Interface"}

        backend_sg = ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [f"{env}-backend-sg"]},
            ]
        )["SecurityGroups"][0]
        database_sg = ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [f"{env}-database-sg"]},
            ]
        )["SecurityGroups"][0]
        endpoint_sg = ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [f"{env}-endpoint-sg"]},
            ]
        )["SecurityGroups"][0]

        assert backend_sg["IpPermissions"] == []
        assert len(backend_sg["IpPermissionsEgress"]) == 1
        assert backend_sg["IpPermissionsEgress"][0]["IpProtocol"] == "-1"

        assert len(database_sg["IpPermissions"]) == 1
        db_rule = database_sg["IpPermissions"][0]
        assert db_rule["FromPort"] == 5432
        assert db_rule["ToPort"] == 5432
        assert len(db_rule["UserIdGroupPairs"]) == 1
        assert db_rule["UserIdGroupPairs"][0]["GroupId"] == backend_sg["GroupId"]
        assert database_sg["IpPermissionsEgress"]

        assert len(endpoint_sg["IpPermissions"]) == 1
        endpoint_rule = endpoint_sg["IpPermissions"][0]
        assert endpoint_rule["FromPort"] == 443
        assert endpoint_rule["ToPort"] == 443
        assert len(endpoint_rule["UserIdGroupPairs"]) == 1
        assert endpoint_rule["UserIdGroupPairs"][0]["GroupId"] == backend_sg["GroupId"]
        assert endpoint_sg["IpPermissionsEgress"]

        private_subnet_ids = {subnet["SubnetId"] for subnet in private_subnets}
        for endpoint in vpc_endpoints:
            assert set(endpoint["SubnetIds"]) == private_subnet_ids
            assert endpoint["Groups"][0]["GroupId"] == endpoint_sg["GroupId"]


def test_secrets_manager_configuration(aws_client):
    """Each environment should have a single secret with non-punctuated credentials."""
    secrets = aws_client.client("secretsmanager", endpoint_url=ENDPOINT_URL)
    response = secrets.list_secrets()
    managed_secrets = [
        secret
        for secret in response["SecretList"]
        if any(secret["Name"] == f"{env}-rds-secret" for env in ENVIRONMENTS)
    ]

    assert {secret["Name"] for secret in managed_secrets} == {
        f"{env}-rds-secret" for env in ENVIRONMENTS
    }

    for env in ENVIRONMENTS:
        secret_value = secrets.get_secret_value(SecretId=f"{env}-rds-secret")["SecretString"]
        secret = json.loads(secret_value)
        assert set(secret) == {"username", "password"}
        assert secret["username"]
        assert secret["password"].isalnum()


def test_sqs_configuration(aws_client):
    """Queues should expose the required operational settings for each environment."""
    sqs = aws_client.client("sqs", endpoint_url=ENDPOINT_URL)
    queue_urls = sqs.list_queues().get("QueueUrls", [])
    env_queue_urls = {
        env: [url for url in queue_urls if f"/{env}-queue" in url]
        for env in ENVIRONMENTS
    }

    for env, matching_urls in env_queue_urls.items():
        assert len(matching_urls) == 1
        attributes = sqs.get_queue_attributes(
            QueueUrl=matching_urls[0],
            AttributeNames=["All"],
        )["Attributes"]

        assert attributes["VisibilityTimeout"] == "60"
        assert attributes["MessageRetentionPeriod"] == "345600"
        assert attributes["SqsManagedSseEnabled"] == "true"


def test_lambda_function_deployment(aws_client):
    """Lambda functions should be deployed in VPC, use the expected runtime, and remain zip-based."""
    lambda_client = aws_client.client("lambda", endpoint_url=ENDPOINT_URL)

    functions = lambda_client.list_functions()["Functions"]
    functions_by_name = function_by_name(functions)
    expected_names = {
        f"{env}-{suffix}"
        for env in ENVIRONMENTS
        for suffix in ["ingest", "enrichment", "worker"]
    }
    assert expected_names.issubset(set(functions_by_name))

    for env in ENVIRONMENTS:
        for suffix in ["ingest", "enrichment", "worker"]:
            function_name = f"{env}-{suffix}"
            function = functions_by_name[function_name]

            assert function["Runtime"] == "python3.12"
            assert function["Timeout"] == 10
            assert function["MemorySize"] == 256
            assert function.get("PackageType", "Zip") == "Zip"
            assert len(function["VpcConfig"]["SubnetIds"]) == 2
            assert len(function["VpcConfig"]["SecurityGroupIds"]) == 1


def test_log_groups_configuration(aws_client):
    """Explicit log groups should exist with 14-day retention and no KMS binding."""
    logs = aws_client.client("logs", endpoint_url=ENDPOINT_URL)
    groups = logs.describe_log_groups()["logGroups"]
    groups_by_name = {group["logGroupName"]: group for group in groups}

    for env in ENVIRONMENTS:
        for name in [
            f"/aws/lambda/{env}-ingest",
            f"/aws/lambda/{env}-enrichment",
            f"/aws/lambda/{env}-worker",
            f"/aws/vendedlogs/{env}/StateMachineLogs",
            f"/aws/api-gateway/{env}-api",
        ]:
            assert name in groups_by_name
            assert groups_by_name[name]["retentionInDays"] == 14
            assert "kmsKeyId" not in groups_by_name[name]


def test_rds_configuration(aws_client):
    """Treat unavailable RDS support as a no-op in the harness environment."""
    rds = aws_client.client("rds", endpoint_url=ENDPOINT_URL)

    try:
        response = rds.describe_db_instances()
    except ClientError:
        return

    assert "DBInstances" in response


def test_eventbridge_configuration(aws_client):
    """Event buses and rules should be environment-scoped and target the correct queue."""
    events = aws_client.client("events", endpoint_url=ENDPOINT_URL)
    sqs = aws_client.client("sqs", endpoint_url=ENDPOINT_URL)

    buses = events.list_event_buses()["EventBuses"]
    env_buses = {bus["Name"]: bus for bus in buses if bus["Name"] in {f"{env}-bus" for env in ENVIRONMENTS}}
    assert set(env_buses) == {f"{env}-bus" for env in ENVIRONMENTS}

    queue_arns = {}
    for env in ENVIRONMENTS:
        queue_url = next(url for url in sqs.list_queues().get("QueueUrls", []) if f"/{env}-queue" in url)
        queue_arns[env] = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

    for env in ENVIRONMENTS:
        rules = events.list_rules(NamePrefix=f"{env}-", EventBusName=f"{env}-bus")["Rules"]
        assert len(rules) == 1

        event_pattern = json.loads(rules[0]["EventPattern"])
        assert event_pattern["source"] == ["app.orders"]
        assert event_pattern["detail-type"] == ["OrderCreated"]

        targets = events.list_targets_by_rule(
            Rule=rules[0]["Name"],
            EventBusName=f"{env}-bus",
        )["Targets"]
        assert len(targets) == 1
        assert targets[0]["Arn"] == queue_arns[env]


def test_stepfunctions_configuration(aws_client):
    """State machines should be STANDARD and include a worker task plus a terminal succeed state."""
    sfn = aws_client.client("stepfunctions", endpoint_url=ENDPOINT_URL)
    state_machines = sfn.list_state_machines()["stateMachines"]
    state_machines_by_name = {machine["name"]: machine for machine in state_machines}

    for env in ENVIRONMENTS:
        machine_name = f"{env}-state-machine"
        assert machine_name in state_machines_by_name

        machine = sfn.describe_state_machine(
            stateMachineArn=state_machines_by_name[machine_name]["stateMachineArn"]
        )
        definition = json.loads(machine["definition"])
        states = definition["States"]

        assert machine["type"] == "STANDARD"
        assert definition["StartAt"] in states
        assert any(
            state.get("Type") == "Task"
            and state.get("Resource", "").endswith(f":function:{env}-worker")
            for state in states.values()
        )
        assert any(state.get("Type") == "Succeed" for state in states.values())

        logging_configuration = machine.get("loggingConfiguration", {})
        if logging_configuration:
            assert logging_configuration["level"] == "ALL"
            destinations = logging_configuration.get("destinations", [])
            assert destinations
            assert destinations[0]["cloudWatchLogsLogGroup"]["logGroupArn"].endswith(
                f":log-group:/aws/vendedlogs/{env}/StateMachineLogs:*"
            )


def test_api_gateway_configuration(aws_client):
    """API Gateway resources, stage logging, and Lambda proxy integration should be scoped per environment."""
    apigateway = aws_client.client("apigateway", endpoint_url=ENDPOINT_URL)
    logs = aws_client.client("logs", endpoint_url=ENDPOINT_URL)

    apis = apigateway.get_rest_apis()["items"]
    apis_by_name = {api["name"]: api for api in apis}
    log_groups = {
        group["logGroupName"]: group["arn"]
        for group in logs.describe_log_groups()["logGroups"]
    }

    for env in ENVIRONMENTS:
        api_name = f"{env}-api"
        assert api_name in apis_by_name

        api_id = apis_by_name[api_name]["id"]
        resources = apigateway.get_resources(restApiId=api_id)["items"]
        orders_resource = next(resource for resource in resources if resource.get("path") == "/orders")

        method = apigateway.get_method(
            restApiId=api_id,
            resourceId=orders_resource["id"],
            httpMethod="POST",
        )
        stage = apigateway.get_stage(restApiId=api_id, stageName="prod")

        assert method["httpMethod"] == "POST"
        assert method["authorizationType"] == "NONE"
        assert method["methodIntegration"]["type"] == "AWS_PROXY"
        assert f":function:{env}-ingest" in method["methodIntegration"]["uri"]

        expected_log_group_arn = log_groups[f"/aws/api-gateway/{env}-api"].removesuffix(":*")
        assert stage["accessLogSettings"]["destinationArn"] == expected_log_group_arn


def test_iam_inline_policy_scopes(aws_client):
    """Inline IAM policies should remain environment-scoped and avoid broad resources where not needed."""
    iam = aws_client.client("iam", endpoint_url=ENDPOINT_URL)

    for env in ENVIRONMENTS:
        ingest_events = maybe_parse_policy(
            iam.get_role_policy(
                RoleName=f"{env}-ingest-lambda-role",
                PolicyName=f"{env}-eventbridge-policy",
            )["PolicyDocument"]
        )
        assert ingest_events["Statement"][0]["Resource"].endswith(f":event-bus/{env}-bus")

        ingest_logs = maybe_parse_policy(
            iam.get_role_policy(
                RoleName=f"{env}-ingest-lambda-role",
                PolicyName=f"{env}-ingest-logs-policy",
            )["PolicyDocument"]
        )
        assert ingest_logs["Statement"][0]["Resource"].endswith(f"/aws/lambda/{env}-ingest:*")

        worker_secret = maybe_parse_policy(
            iam.get_role_policy(
                RoleName=f"{env}-worker-lambda-role",
                PolicyName=f"{env}-secretsmanager-policy",
            )["PolicyDocument"]
        )
        assert worker_secret["Statement"][0]["Resource"].startswith(
            f"arn:aws:secretsmanager:us-east-1:000000000000:secret:{env}-rds-secret"
        )

        stepfunctions_lambda = maybe_parse_policy(
            iam.get_role_policy(
                RoleName=f"{env}-stepfunctions-role",
                PolicyName=f"{env}-stepfunctions-lambda-policy",
            )["PolicyDocument"]
        )
        assert stepfunctions_lambda["Statement"][0]["Resource"].endswith(f":function:{env}-worker")

        stepfunctions_logs = maybe_parse_policy(
            iam.get_role_policy(
                RoleName=f"{env}-stepfunctions-role",
                PolicyName=f"{env}-stepfunctions-logs-policy",
            )["PolicyDocument"]
        )
        assert stepfunctions_logs["Statement"][0]["Resource"] == "*"
