package com.example;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.s3.Bucket;
import software.constructs.Construct;

public class AppStack extends Stack {
    public AppStack(final Construct scope, final String id, final String namePrefix) {
        this(scope, id, namePrefix, null);
    }

    public AppStack(final Construct scope, final String id, final String namePrefix, final StackProps props) {
        super(scope, id, props);

        Bucket bucket = Bucket.Builder.create(this, "Main")
                .bucketName(namePrefix + "-bucket")
                .build();

        Tags.of(bucket).add("Name", namePrefix + "-bucket");
        Tags.of(bucket).add("Project", namePrefix);

        CfnOutput.Builder.create(this, "BucketName")
                .value(bucket.getBucketName())
                .description("S3 bucket name")
                .build();
    }
}
