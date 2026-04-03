# Terraform Security Configuration

This task contains a single Terraform module in `main.tf`.

## Input Variables

| Variable | Type | Default |
|----------|------|---------|
| `aws_region` | string | `"us-east-1"` |
| `aws_access_key_id` | string | none |
| `aws_secret_access_key` | string | none |

## Validation

```bash
terraform validate
python3 -m pytest tests/unit_tests.py tests/integration_tests.py -q
```
