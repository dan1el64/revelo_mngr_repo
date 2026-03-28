"""Example stack: S3 bucket with tags. Uses NAME_PREFIX from environment for naming."""

import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
from constructs import Construct


class AppStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, name_prefix: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            "Main",
            bucket_name=f"{name_prefix}-bucket",
        )
        cdk.Tags.of(bucket).add("Name", f"{name_prefix}-bucket")
        cdk.Tags.of(bucket).add("Project", name_prefix)

        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name, description="S3 bucket name")
