# Terraform task

The deliverable is a single `main.tf` that provisions the full stack described in the prompt:

- Cloud Boundaries: 1 VPC, 2 private subnets, 2 security groups
- Entry gateways: 1 HTTP API with `POST /submit` and `GET /health`, plus 1 SQS queue
- Processing Units: worker Lambda, health Lambda, and enrichment Lambda
- Storage Layer: Secrets Manager secret + secret version + PostgreSQL RDS instance
- Orchestration: Step Functions + EventBridge Pipe from SQS to Step Functions

## Allowed input variables

Only these input variables are allowed:

- `aws_region` with default `us-east-1`
- `aws_access_key_id`
- `aws_secret_access_key`
- `aws_endpoint` for optional AWS-compatible endpoint overrides

The AWS provider must use these variables, including wiring `aws_endpoint` into the provider endpoint overrides.

## Tests

Unit tests validate the Terraform contract directly from the HCL.

```bash
pytest tests/unit_tests.py
```

Integration tests validate deployed resources through `boto3`. They use `state.json` only to discover deployed identifiers after `terraform apply`.

```bash
terraform show -json > state.json
pytest tests/integration_tests.py
```
