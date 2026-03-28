#!/usr/bin/env python3
"""CDK app entrypoint. Define your stack(s) here and pass config from environment."""

import os
import aws_cdk as cdk

def main():
    app = cdk.App()
    name_prefix = os.environ.get("NAME_PREFIX", "dev")
    # Add your stack and resources here. Use name_prefix for resource names.
    stack = cdk.Stack(app, "ExampleStack")
    # e.g. bucket = s3.Bucket(stack, "Main", bucket_name=f"{name_prefix}-bucket", ...)
    app.synth()


if __name__ == "__main__":
    main()
