# CloudFormation example (YAML)

Example CloudFormation stack in YAML: one S3 bucket with tags and a `BucketName` output. Compatible with the harness and local mode.

---

## Contents

- **template.yaml** – Parameters (`NamePrefix`), Resources (S3 bucket with Name/Project tags), Outputs (`BucketName`).
- **tests/unit_tests.py** – Assert template structure (Parameters, S3 bucket, Outputs).
- **tests/integration_tests.py** – Assert stack deployed and bucket exists (and tags in example).

---

## Run with harness

From the repo root (directory containing `harness.sh`):

```bash
./harness.sh cfn-yaml/example
```

---

## Local testing

Tests are **Python** (pytest). Use the framework image and run validate, unit tests, create-stack, integration tests, delete-stack as in the template README. Ensure a local provider is on `iac-harness-network` at `10.0.2.20:4566`.
