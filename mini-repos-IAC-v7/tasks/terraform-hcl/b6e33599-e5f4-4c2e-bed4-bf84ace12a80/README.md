# Terraform AWS baseline

La entrega de infraestructura está contenida completamente en `main.tf`.

## Variables admitidas

- `aws_region` con valor por defecto `us-east-1`
- `aws_endpoint`
- `aws_access_key_id`
- `aws_secret_access_key`

## Flujo sugerido

```bash
terraform init
terraform plan -input=false -out=.tfplan \
  -var="aws_access_key_id=YOUR_ACCESS_KEY" \
  -var="aws_secret_access_key=YOUR_SECRET_KEY"
terraform show -json .tfplan > plan.json
pytest tests/unit_tests.py

terraform apply -input=false -auto-approve \
  -var="aws_access_key_id=YOUR_ACCESS_KEY" \
  -var="aws_secret_access_key=YOUR_SECRET_KEY"
terraform show -json > state.json
pytest tests/integration_tests.py

terraform destroy -input=false -auto-approve \
  -var="aws_access_key_id=YOUR_ACCESS_KEY" \
  -var="aws_secret_access_key=YOUR_SECRET_KEY"
```
