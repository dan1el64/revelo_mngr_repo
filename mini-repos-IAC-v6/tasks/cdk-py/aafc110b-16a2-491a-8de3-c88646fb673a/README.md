# CDK Python (AWS or local mode)

This repository contains an AWS CDK Python app that should work against **AWS** or **local mode** without code changes. Use environment variables for configuration (e.g. name prefix); do not hardcode endpoint URLs or environment-specific values in application code.

---

## Configuration

The app uses these environment variables:

| Variable | Description | For local mode |
|----------|-------------|----------------|
| `NAME_PREFIX` | Prefix for resource names (e.g. buckets) to avoid collisions | `dev` or any unique prefix |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT_URL` | Override for AWS API endpoint | `http://aws-endpoint.internal` |
| `AWS_ENDPOINT_URL_S3` | Override for S3 API endpoint | `http://aws-endpoint.internal` |
| `AWS_ENDPOINT` | Alternative endpoint override | `http://aws-endpoint.internal` |
| `CDK_DEFAULT_ACCOUNT` | AWS account ID for CDK | `000000000000` (local mode default) |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |

Do not hardcode local mode or AWS URLs in your stack code; use `cdklocal` when targeting local mode so the same code works in both environments.

**Note:** Assumes a local provider is already running on the `iac-harness-network`. The container must join this network to connect to local mode.

---

## Testing your implementation locally

Assumes a local provider is already running on the `iac-harness-network`. The harness runs the steps via `scripts/cdk-py/run.sh`. Tests are **Python** (pytest); the CDK app is **Python**.

## Submission packaging

When submitting, compress only this task directory. Do not upload the parent repository, harness scripts, or unrelated generated artifacts. A clean archive should contain this task's source files only.

### Run in the test container

From the repo root (use the image built from `scripts/cdk-py/` or `revelotalentcorp/iac-cdk-py`):

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
  -e AWS_ENDPOINT_URL="http://aws-endpoint.internal" \
  -e AWS_ENDPOINT_URL_S3="http://aws-endpoint.internal" \
  -e AWS_ENDPOINT="http://aws-endpoint.internal" \
  revelotalentcorp/iac-cdk-py \
  bash -c "cp -r /work-ro /work && cd /work && exec bash"
```

### Inside the container: install, synth, and test

```bash
pip install -r requirements.txt

# Synthesize and produce template for unit tests
cdklocal synth
cp cdk.out/*.template.json template.json

pytest tests/unit_tests.py -v

# Deploy to locally and run integration tests
cdklocal bootstrap
cdklocal deploy --require-approval never --all
pytest tests/integration_tests.py -v

# Clean up
cdklocal destroy --force --all
```
