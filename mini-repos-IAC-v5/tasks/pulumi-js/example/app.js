"use strict";

const pulumi = require("@pulumi/pulumi");
const aws = require("@pulumi/aws");

const config = new pulumi.Config();
const namePrefix = config.require("namePrefix");

const bucket = new aws.s3.Bucket("main", {
    bucket: `${namePrefix}-bucket`,
    tags: {
        Name: `${namePrefix}-bucket`,
        Project: namePrefix,
    },
});

exports.bucketName = bucket.bucket;
exports.bucketArn = bucket.arn;
