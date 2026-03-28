#!/usr/bin/env node
const cdk = require('aws-cdk-lib');
const { AppStack } = require('./app-stack');

const app = new cdk.App();
const namePrefix = process.env.NAME_PREFIX || 'dev';

new AppStack(app, 'AppStack', { namePrefix });

app.synth();
