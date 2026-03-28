#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';

const app = new cdk.App();
const namePrefix = process.env.NAME_PREFIX || 'dev';

// Add your stack and resources here. Use namePrefix for resource names.
const stack = new cdk.Stack(app, 'ExampleStack');
// e.g. const bucket = new s3.Bucket(stack, 'Main', {
//   bucketName: `${namePrefix}-bucket`,
// });

app.synth();
