import os

os.environ.setdefault("JSII_RUNTIME_PACKAGE_CACHE", "/tmp/aws-jsii-package-cache")

from aws_cdk import App

from app import InventoryStack


def test_stack_synthesizes():
    app = App()
    InventoryStack(app, "InventoryStackIntegrationTest")
    assembly = app.synth()

    assert len(assembly.stacks) == 1
    assert assembly.stacks[0].stack_name == "InventoryStackIntegrationTest"
