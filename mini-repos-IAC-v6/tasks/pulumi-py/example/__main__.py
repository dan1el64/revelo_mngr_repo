"""Pulumi Python program for AWS infrastructure."""

import os
import pulumi
import pulumi_aws as aws

# Get name prefix from environment variable
name_prefix = os.environ.get("NAME_PREFIX", "dev")

# Create an S3 bucket
bucket = aws.s3.Bucket(
    "main",
    bucket=f"{name_prefix}-bucket",
    tags={
        "Name": f"{name_prefix}-bucket",
        "Project": name_prefix,
    },
)

# Export the bucket name and ARN
pulumi.export("bucketName", bucket.bucket)
pulumi.export("bucketArn", bucket.arn)
