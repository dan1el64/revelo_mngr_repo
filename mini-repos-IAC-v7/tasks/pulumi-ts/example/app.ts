import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const config = new pulumi.Config();
const namePrefix = config.require("namePrefix");

const bucket = new aws.s3.Bucket("main", {
    bucket: `${namePrefix}-bucket`,
    tags: {
        Name: `${namePrefix}-bucket`,
        Project: namePrefix,
    },
});

export const bucketName = bucket.bucket;
export const bucketArn = bucket.arn;
