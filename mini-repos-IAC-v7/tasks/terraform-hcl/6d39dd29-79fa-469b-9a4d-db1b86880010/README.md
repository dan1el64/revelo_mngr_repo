# Terraform landing zone

This task is implemented as a single Terraform configuration in `main.tf`.

## Inputs

Only these Terraform input variables are defined:

| Variable | Default |
| --- | --- |
| `aws_endpoint` | `null` |
| `aws_region` | `"us-east-1"` |
| `aws_access_key_id` | required |
| `aws_secret_access_key` | required |

## Validation

```bash
terraform fmt -check -diff
terraform init -backend=false
terraform validate
python3 -m pytest -q
```
