#!/usr/bin/env python3
"""CDK app: one stack with an S3 bucket. Environment-agnostic."""

import os
import aws_cdk as cdk
from app_stack import AppStack

def main():
    app = cdk.App()
    name_prefix = os.environ.get("NAME_PREFIX", "dev")
    AppStack(app, "AppStack", name_prefix=name_prefix)
    app.synth()


if __name__ == "__main__":
    main()
