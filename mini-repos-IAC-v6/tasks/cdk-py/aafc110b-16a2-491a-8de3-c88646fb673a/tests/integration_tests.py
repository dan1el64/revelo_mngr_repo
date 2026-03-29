import os
import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import patch

from aws_cdk import App
from aws_cdk.assertions import Match, Template


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app.py"


def load_app_module():
    spec = spec_from_file_location("solution_app_integration", APP_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.dict(
            os.environ,
            {
                "AWS_REGION": "us-east-1",
                "AWS_ENDPOINT": "https://api.aws.amazon.com",
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
            },
            clear=False,
        ):
            module = load_app_module()
            cls.app = App()
            cls.stack = module.SecureNotificationStack(cls.app, "IntegrationTestStack")
            cls.template = Template.from_stack(cls.stack)

    def test_database_security_group_is_not_open_to_world(self):
        template_json = self.template.to_json()
        ingress_rules = [
            resource["Properties"]
            for resource in template_json["Resources"].values()
            if resource["Type"] == "AWS::EC2::SecurityGroupIngress"
        ]

        self.assertEqual(len(ingress_rules), 1)
        self.assertEqual(
            ingress_rules[0],
            {
                "Description": "Allow PostgreSQL access only from compute resources",
                "FromPort": 5432,
                "GroupId": {"Fn::GetAtt": ["DatabaseSecurityGroup7319C0F6", "GroupId"]},
                "IpProtocol": "tcp",
                "SourceSecurityGroupId": {
                    "Fn::GetAtt": ["ComputeSecurityGroupF0F5C976", "GroupId"]
                },
                "ToPort": 5432,
            },
        )

        for rule in ingress_rules:
            self.assertNotEqual(rule.get("CidrIp"), "0.0.0.0/0")

    def test_api_gateway_route_exists(self):
        self.template.has_resource_properties(
            "AWS::ApiGateway::Method",
            {
                "HttpMethod": "POST",
                "AuthorizationType": "NONE",
                "Integration": Match.object_like({"Type": "AWS_PROXY"}),
            },
        )


if __name__ == "__main__":
    unittest.main()
