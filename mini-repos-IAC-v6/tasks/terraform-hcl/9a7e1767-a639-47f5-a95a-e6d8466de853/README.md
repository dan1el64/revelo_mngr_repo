# Payments Ingestion Terraform

This task defines a single-file Terraform stack in `main.tf`.

## Inputs

- `aws_region`: optional, defaults to `us-east-1`
- `aws_endpoint`: required
- `AWS_ACCESS_KEY_ID`: provided by the runtime
- `AWS_SECRET_ACCESS_KEY`: provided by the runtime

## Validation

Typical validation flow:

```bash
terraform fmt -check
terraform init -input=false
terraform validate
terraform plan -input=false -out=.tfplan -var "aws_endpoint=$TF_VAR_aws_endpoint"
terraform show -json .tfplan > plan.json
pytest tests/unit_tests.py -q
terraform apply -input=false -auto-approve -var "aws_endpoint=$TF_VAR_aws_endpoint"
terraform show -json > state.json
pytest tests/integration_tests.py -q
terraform destroy -input=false -auto-approve -var "aws_endpoint=$TF_VAR_aws_endpoint"
```
