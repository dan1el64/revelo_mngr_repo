#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { AppStack } from './app-stack';

const app = new cdk.App();
const namePrefix = process.env.NAME_PREFIX || 'dev';

new AppStack(app, 'AppStack', { namePrefix });

app.synth();
