# Order Intake Terraform

This task provisions an event-driven AWS stack with these runtime flows:

- `POST /ingest` on API Gateway invokes `ingest_fn`
- `ingest_fn` reads an API key from Secrets Manager, writes an order to DynamoDB, and publishes an SNS event
- SNS delivers the event to SQS
- `analytics_fn` reads DB credentials from Secrets Manager, consumes SQS, and writes `raw/analytics-marker.txt` to S3
- EventBridge schedules `analytics_fn` every 5 minutes

## Inputs

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `aws_region` | `string` | `"us-east-1"` | AWS region for all resources |
| `aws_endpoint` | `string` | `null` | Optional endpoint override for local AWS-compatible environments |

## Local Test Workflow

Unit tests validate Terraform through the generated plan JSON, not by parsing `main.tf`.
Integration tests validate deployed behavior using real AWS APIs.

```bash
terraform init
terraform plan -input=false -out=.tfplan
terraform show -json .tfplan > plan.json
pytest tests/unit_tests.py

terraform apply -input=false -auto-approve
pytest tests/integration_tests.py

terraform destroy -input=false -auto-approve
```

## Test Files

- `tests/unit_tests.py` expects `plan.json`
- `tests/integration_tests.py` expects deployed AWS credentials

## Submission

Submit only this task folder as the zip payload. Do not include harness files, scripts, or the parent repo structure.

One safe way to package the deliverable from the parent directory is:

```bash
zip -r efdf05e0-b498-4fc4-ae0a-c4d21649ac18.zip efdf05e0-b498-4fc4-ae0a-c4d21649ac18 \
  -x "*/.terraform/*" "*/.pytest_cache/*" "*/__pycache__/*" "*.tfstate*" "*.tfplan" "*/plan.json" "*/.DS_Store"
```
