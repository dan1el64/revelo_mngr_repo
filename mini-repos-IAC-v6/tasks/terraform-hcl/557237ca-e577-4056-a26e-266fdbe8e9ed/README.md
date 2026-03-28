# Terraform task

The Terraform configuration lives in `main.tf` and exposes exactly two input variables:

- `aws_region` with default `us-east-1`
- `aws_endpoint` without a default

## Test flow

Generate the plan artifact used by the unit tests:

```bash
terraform init
terraform plan -input=false -out=.tfplan -var "aws_region=us-east-1" -var "aws_endpoint=$TF_VAR_aws_endpoint"
terraform show -json .tfplan > plan.json
python3 -m pytest tests/unit_tests.py
```

Apply and generate the state artifact used by the boto3 integration tests:

```bash
terraform apply -input=false -auto-approve -var "aws_region=us-east-1" -var "aws_endpoint=$TF_VAR_aws_endpoint"
terraform show -json > state.json
python3 -m pytest tests/integration_tests.py
terraform destroy -input=false -auto-approve -var "aws_region=us-east-1" -var "aws_endpoint=$TF_VAR_aws_endpoint"
```
