import * as pulumi from "@pulumi/pulumi";

/**
 * Pulumi TypeScript entrypoint. Define your stack here.
 * Use namePrefix from Pulumi config (set from NAME_PREFIX env) for resource names.
 */
const config = new pulumi.Config();
const namePrefix = config.require("namePrefix");

// Add your stack and resources here (e.g. import * as aws from "@pulumi/aws" and create resources).
