# Pulumi Java (AWS or local mode)

This repository contains a Pulumi Java program that works against **AWS** or **local mode** without code changes. Configuration is via environment variables and Pulumi config; no endpoint URLs are hardcoded.

---

## Configuration

| Variable | Description | For local mode |
|----------|-------------|----------------|
| `NAME_PREFIX` | Prefix for resource names (e.g. buckets) to avoid collisions | `dev` or any unique prefix |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT` | Override for AWS API endpoint | `http://host.docker.internal:4566` or `http://10.0.2.20:4566` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |

Do not hardcode local mode or AWS URLs in Java code; use config/env so the same code works in both environments.

---

## Testing locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. The harness runs the steps (setup, preview, unit tests, up, integration tests, destroy) via `scripts/pulumi-java/run.sh`. Tests are **Python** (pytest); the Pulumi program is **Java**.

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
  revelotalentcorp/iac-pulumi-java \
  bash -c "cp -r /work-ro /work && cd /work && exec bash"
```

Inside the container:

```bash
pulumi stack select dev 2>/dev/null || pulumi stack init dev
pulumi config set namePrefix "$NAME_PREFIX"
# ... (AWS/local mode config as in run.sh)
pulumi preview --non-interactive --json > preview.json
pytest tests/unit_tests.py -v
pulumi up --yes --non-interactive
pulumi stack export > state.json
pytest tests/integration_tests.py -v
pulumi destroy --yes --non-interactive
```
