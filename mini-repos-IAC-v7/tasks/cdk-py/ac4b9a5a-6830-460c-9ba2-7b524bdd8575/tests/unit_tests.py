import os

os.environ.setdefault("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/aws-jsii-package-cache")

from aws_cdk import App, assertions

from app import InventoryStack


def _template():
    app = App()
    stack = InventoryStack(app, "InventoryStackTest")
    return assertions.Template.from_stack(stack)


def test_data_store_resources():
    template = _template()

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::DynamoDB::Table", 1)
    template.resource_count_is("AWS::Logs::LogGroup", 2)

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "VersioningConfiguration": {"Status": "Enabled"},
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    }
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )

    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": True},
        },
    )


def test_serverless_and_api_resources():
    template = _template()

    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::IAM::Role", 3)
    template.resource_count_is("AWS::ApiGateway::RestApi", 1)
    template.resource_count_is("AWS::Scheduler::Schedule", 1)

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Runtime": "python3.12",
            "MemorySize": 256,
        },
    )

    template.has_resource_properties(
        "AWS::ApiGateway::Method",
        {
            "HttpMethod": "GET",
            "AuthorizationType": "NONE",
            "Integration": {
                "Type": "AWS_PROXY",
                "IntegrationHttpMethod": "POST",
            },
        },
    )

    template.has_resource_properties(
        "AWS::Scheduler::Schedule",
        {
            "ScheduleExpression": "rate(15 minutes)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
        },
    )


def test_permissions_and_logging_constraints():
    template = _template()

    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"RetentionInDays": 14},
    )

    template.has_resource_properties(
        "AWS::Lambda::Function",
        assertions.Match.object_like(
            {
                "Handler": "app.handler",
                "Environment": {
                    "Variables": assertions.Match.object_like(
                        {
                            "aws_region": {"Ref": "awsregion"},
                            "aws_endpoint": {"Ref": "awsendpoint"},
                        }
                    )
                },
            }
        ),
    )

    template.has_resource_properties(
        "AWS::ApiGateway::Stage",
        {
            "MethodSettings": [
                {
                    "DataTraceEnabled": False,
                    "HttpMethod": "*",
                    "LoggingLevel": "INFO",
                    "ResourcePath": "/*",
                }
            ]
        },
    )
