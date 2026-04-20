# CDK Python example (AWS or local mode)

This repository contains an AWS CDK Python app that works against **AWS** or **local mode** without code changes. Configuration is via environment variables; no endpoint URLs or environment-specific values are hardcoded.

---

## Configuration

| Variable | Description | For local mode |
|----------|-------------|----------------|
| `NAME_PREFIX` | Prefix for resource names (e.g. buckets) to avoid collisions | `dev` or any unique prefix |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT_URL` | Override for AWS API endpoint | `http://10.0.2.20:4566` (fixed IP in custom network) |
| `AWS_ENDPOINT_URL_S3` | Override for S3 API endpoint | `http://10.0.2.20:4566` (same as above) |
| `AWS_ENDPOINT` | Alternative endpoint override | `http://10.0.2.20:4566` (ensures compatibility) |
| `CDK_DEFAULT_ACCOUNT` | AWS account ID for CDK | `000000000000` (local mode default) |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |

**Note:** Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. The container must join this network to connect to local mode.

---

## Testing locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. Tests are **Python** (pytest); the CDK app is **Python**.

### Run in the test container

**Important:** The container must join the same network as local mode.

```bash
docker run --rm -it \
  -v "$PWD:/work-ro:ro" \
  --network iac-harness-network \
  -e AWS_REGION="us-east-1" \
  -e AWS_ACCESS_KEY_ID="test" \
  -e AWS_SECRET_ACCESS_KEY="test" \
  -e NAME_PREFIX="dev" \
  -e CDK_DEFAULT_ACCOUNT="000000000000" \
  -e AWS_ENDPOINT_URL="http://10.0.2.20:4566" \
  -e AWS_ENDPOINT_URL_S3="http://10.0.2.20:4566" \
  -e AWS_ENDPOINT="http://10.0.2.20:4566" \
  revelotalentcorp/iac-cdk-py \
  bash -c "cp -r /work-ro /work && cd /work && exec bash"
```

Inside the container:

```bash
cdklocal synth
cp cdk.out/*.template.json template.json
pytest tests/unit_tests.py -v
cdklocal bootstrap
cdklocal deploy --require-approval never --all
pytest tests/integration_tests.py -v
cdklocal destroy --force --all
```
