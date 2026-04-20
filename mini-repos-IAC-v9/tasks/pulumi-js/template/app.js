"use strict";

const pulumi = require("@pulumi/pulumi");

/**
 * Pulumi JavaScript entrypoint. Define your stack here.
 * Use namePrefix from Pulumi config (set from NAME_PREFIX env) for resource names.
 */
const config = new pulumi.Config();
const namePrefix = config.require("namePrefix");

// Add your stack and resources here (e.g. const aws = require("@pulumi/aws"); and create resources).
// For stack outputs, use: exports.outputName = value;
