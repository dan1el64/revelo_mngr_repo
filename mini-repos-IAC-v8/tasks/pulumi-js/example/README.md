# Pulumi JavaScript (AWS or local mode)

This repository contains a Pulumi JavaScript program that should work against **AWS** or **local mode** without code changes. Use environment variables and Pulumi config for the target; do not hardcode endpoint URLs in application code.

---

## Configuration

The program uses these environment variables (and Pulumi config `namePrefix`):

| Variable | Description | For local mode |
|----------|-------------|----------------|
| `NAME_PREFIX` | Prefix for resource names (e.g. buckets) to avoid collisions | `dev` (or any value) |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT` | Override for AWS API endpoint | `http://host.docker.internal:4566` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |

Do not hardcode local mode or AWS URLs in JavaScript code; use config/env so the same code works in both environments.

**Note:** Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566` (or use `host.docker.internal:4566` when the harness uses host gateway).

---

## Testing your implementation locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. Tests are **Python** (pytest); the Pulumi program is **JavaScript**.

### Run in the test container

```bash
docker run --rm -it \
  -v "$PWD:/work-ro:ro" \
  --add-host=host.docker.internal:host-gateway \
  -e AWS_ENDPOINT="http://host.docker.internal:4566" \
  -e AWS_REGION="us-east-1" \
  -e AWS_ACCESS_KEY_ID="test" \
  -e AWS_SECRET_ACCESS_KEY="test" \
  -e NAME_PREFIX="dev" \
  -e PULUMI_CONFIG_PASSPHRASE="" \
  -e PULUMI_BACKEND_URL="file:///work/.pulumi" \
  revelotalentcorp/iac-pulumi-js \
  bash -lc "cp -r /work-ro /work && cd /work && exec bash"
```

This mounts your repo read-only and copies it to `/work` inside the container (edits in `/work` do not persist to your host).

### Inside the container: install, configure, and test

```bash
npm install
pulumi stack select dev 2>/dev/null || pulumi stack init dev

# Point Pulumi AWS provider at local mode (required for any AWS service you use)
pulumi config set namePrefix "$NAME_PREFIX"
pulumi config set aws:region "$AWS_REGION"
pulumi config set aws:skipCredentialsValidation true
pulumi config set aws:skipMetadataApiCheck true
pulumi config set aws:skipRequestingAccountId true
pulumi config set aws:s3UsePathStyle true

for svc in \
  acm apigateway apigatewayv2 applicationautoscaling appsync athena \
  autoscaling backup cloudformation cloudwatch logs dynamodb ec2 ecr ecs efs eks \
  elasticache elasticbeanstalk elb elbv2 emr events firehose glue iam iot kinesis kms \
  lambda mq msk neptune opensearch organizations qldb rds redshift redshiftdata route53 \
  route53resolver s3 s3control sagemaker secretsmanager servicediscovery servicequotas \
  ses sns sqs ssm sfn sts transfer waf wafregional wafv2 xray
do
  pulumi config set --plaintext --path "aws:endpoints[0].${svc}" "$AWS_ENDPOINT"
done

# Preview and unit tests (use preview.json)
pulumi preview --non-interactive --json > preview.json
pytest tests/unit_tests.py -v

# Deploy locally and run integration tests (use state.json)
pulumi up --yes --non-interactive
pulumi stack export > state.json
pytest tests/integration_tests.py -v

# Clean up
pulumi destroy --yes --non-interactive
```
