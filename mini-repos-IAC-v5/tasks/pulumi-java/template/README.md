# Pulumi Java (AWS or local mode)

This repository contains a Pulumi Java program that should work against **AWS** or **local mode** without code changes. Use environment variables and Pulumi config for the target; do not hardcode endpoint URLs in application code.

---

## Configuration

The program uses these environment variables (and Pulumi config `namePrefix`):

| Variable | Description | For local mode |
|----------|-------------|----------------|
| `NAME_PREFIX` | Prefix for resource names (e.g. buckets) to avoid collisions | `dev` (or any value) |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT` | Override for AWS API endpoint | `http://10.0.2.20:4566` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |

Do not hardcode local mode or AWS URLs in Java code; use config/env so the same code works in both environments.

**Note:** Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566` (or use `host.docker.internal:4566` when the harness uses host gateway).

---

## Testing your implementation locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. The harness runs the steps via `scripts/pulumi-java/run.sh`. Tests are **Python** (pytest); the Pulumi program is **Java**. Run in the test container (see below). Then:

```bash
pulumi stack select dev 2>/dev/null || pulumi stack init dev
# Configure namePrefix and AWS/local mode (see run.sh)
pulumi preview --non-interactive --json > preview.json
pytest tests/unit_tests.py -v
pulumi up --yes --non-interactive
pulumi stack export > state.json
pytest tests/integration_tests.py -v
pulumi destroy --yes --non-interactive
```
