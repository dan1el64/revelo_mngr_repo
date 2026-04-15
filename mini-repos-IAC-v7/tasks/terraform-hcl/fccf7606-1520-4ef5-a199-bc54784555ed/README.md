# Terraform template (env-agnostic)

This Terraform configuration works against **AWS** or an AWS-compatible endpoint override. When `aws_endpoint` points to a LocalStack-style endpoint, the module keeps the deployable subset active and skips resources that are not implemented by the target runtime.

---

## Input Variables

| Variable | Type | Description | Default |
|----------|------|-------------|---------|
| `name_prefix` | string | Prefix for resource names (e.g., S3 buckets) to avoid collisions | *Required* |
| `bucket_name` | string | S3 bucket name suffix. Final name: `${name_prefix}-${bucket_name}` | `"bucket"` |
| `aws_region` | string | AWS region | `"us-east-1"` |
| `aws_endpoint` | string | AWS endpoint override. Set to `http://10.0.2.20:4566` when targeting a local AWS-compatible endpoint, leave empty for AWS | `""` |

When `aws_endpoint` is set, Terraform still provisions the supported workflow components used by the harness: VPC, subnets, security groups, VPC endpoints, SQS, Secrets Manager, Lambda functions, Step Functions, IAM, log groups, S3 assets, and the alarms supported by that runtime.

**Environment variables** (alternative to `TF_VAR_*`):
- `AWS_ACCESS_KEY_ID` - AWS access key (use `"test"` when targeting a local AWS-compatible endpoint)
- `AWS_SECRET_ACCESS_KEY` - AWS secret key (use `"test"` when targeting a local AWS-compatible endpoint)
- `AWS_DEFAULT_REGION` - AWS region

---

## Testing locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. Tests are **Python** (pytest).

### Run in the test container

Use the provided Docker image with all dependencies (Terraform, Python, pytest):

```bash
docker run --rm -it \
  -v "$PWD:/work-ro:ro" \
  --network iac-harness-network \
  -e TF_VAR_aws_endpoint="http://10.0.2.20:4566" \
  -e TF_VAR_aws_region="us-east-1" \
  -e TF_VAR_name_prefix="dev" \
  -e AWS_ACCESS_KEY_ID="test" \
  -e AWS_SECRET_ACCESS_KEY="test" \
  revelotalentcorp/iac-terraform-hcl \
  bash -c "cp -r /work-ro /work && cd /work && exec bash"
```

This copies your files to `/work` inside the container (changes won't affect your local files).

### Example workflow inside the container:

```bash
# Initialize Terraform
terraform init

# Run plan and generate plan.json for unit tests
terraform plan -input=false -out=.tfplan
terraform show -json .tfplan > plan.json

# Run unit tests (these use plan.json)
pytest tests/unit_tests.py

# Apply to the configured endpoint
terraform apply -input=false -auto-approve

# Generate state.json for integration tests
terraform show -json > state.json

# Run integration tests (these use state.json)
pytest tests/integration_tests.py

# Clean up
terraform destroy -input=false -auto-approve
```
