package com.example.pulumi;

import com.pulumi.Pulumi;
import com.pulumi.aws.s3.Bucket;
import com.pulumi.aws.s3.BucketArgs;

import java.util.Map;

/**
 * Pulumi Java app: S3 bucket with tags. Uses NAME_PREFIX from environment (or config namePrefix).
 */
public class App {
    public static void main(String[] args) {
        Pulumi.run(ctx -> {
            String namePrefix = System.getenv("NAME_PREFIX");
            if (namePrefix == null || namePrefix.isEmpty()) {
                namePrefix = ctx.config().get("namePrefix").orElse("dev");
            }

            String bucketName = namePrefix + "-bucket";
            Map<String, String> tags = Map.of(
                "Name", bucketName,
                "Project", namePrefix
            );

            var bucket = new Bucket("main", BucketArgs.builder()
                .bucket(bucketName)
                .tags(tags)
                .build());

            ctx.export("bucketName", bucket.bucket());
            ctx.export("bucketArn", bucket.arn());
        });
    }
}
