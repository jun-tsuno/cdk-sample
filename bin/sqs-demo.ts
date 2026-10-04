#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { SqsDemoStack } from "../lib/sqs-demo-stack";

const app = new cdk.App();

new SqsDemoStack(app, "SqsDemoStack", {
	env: {
		account: process.env.CDK_DEFAULT_ACCOUNT,
		region: process.env.CDK_DEFAULT_REGION,
	},
});
