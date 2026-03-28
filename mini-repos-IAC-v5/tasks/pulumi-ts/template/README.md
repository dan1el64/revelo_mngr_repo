# Pulumi TypeScript (AWS or local mode)

This repository contains a Pulumi TypeScript program that should work against **AWS** or **local mode** without code changes. Use environment variables and Pulumi config for the target; do not hardcode endpoint URLs in application code.

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

Do not hardcode local mode or AWS URLs in TypeScript code; use config/env so the same code works in both environments.

**Note:** Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566` (or use `host.docker.internal:4566` when the harness uses host gateway).

---

## Testing your implementation locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. The harness runs the steps (setup, preview, unit tests, up, integration tests, destroy) via `scripts/pulumi-ts/run.sh`. Tests are **Python** (pytest); the Pulumi program is **TypeScript**.

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
  revelotalentcorp/iac-pulumi-ts \
  bash -lc "cp -r /work-ro /work && cd /work && exec bash"
```

### Inside the container: install, configure, and test

```bash
npm install
pulumi stack select dev 2>/dev/null || pulumi stack init dev

pulumi config set namePrefix "$NAME_PREFIX"
pulumi config set aws:region "$AWS_REGION"
pulumi config set aws:skipCredentialsValidation true
pulumi config set aws:skipMetadataApiCheck true
pulumi config set aws:skipRequestingAccountId true
pulumi config set aws:s3UsePathStyle true
# Set per-service endpoints (see run.sh or scripts/pulumi-ts/run.sh)

pulumi preview --non-interactive --json > preview.json
pytest tests/unit_tests.py -v

pulumi up --yes --non-interactive
pulumi stack export > state.json
pytest tests/integration_tests.py -v

pulumi destroy --yes --non-interactive
```
