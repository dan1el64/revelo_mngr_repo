# CloudFormation template (YAML)

This directory is a stub for a **CloudFormation (YAML)** stack. Use `template.yaml` (or `template.yml`) as the main template.

---

## Expected structure

- **Parameters**: At least `NamePrefix` (String) so the harness can pass a unique prefix.
- **Resources**: At least one resource (e.g. S3 bucket). The example uses an S3 bucket with tags.
- **Outputs**: At least `BucketName` (for the example flow) so integration tests can read the deployed bucket name.

---

## Testing with the harness

From the repo root (directory containing `harness.sh`):

```bash
./harness.sh cfn-yaml/template
```

Or for the filled example:

```bash
./harness.sh cfn-yaml/example
```

---

## Running tests locally

Assumes a local provider is already running on the `iac-harness-network` at `10.0.2.20:4566`. Tests are **Python** (pytest). Use the framework Docker image:

```bash
docker run --rm -it \
  -v "$PWD:/work-ro:ro" \
  --network iac-harness-network \
  -e NAME_PREFIX=dev \
  -e STACK_NAME=dev \
  -e AWS_ENDPOINT_URL=http://10.0.2.20:4566 \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  revelotalentcorp/iac-cfn-yaml \
  bash -c "cp -r /work-ro /work && cd /work && exec bash"
```

Inside the container:

```bash
# Validate template
aws cloudformation validate-template --template-body file://template.yaml

# Unit tests (template structure)
pytest tests/unit_tests.py

# Deploy locally
aws cloudformation create-stack --stack-name "$STACK_NAME" \
  --template-body file://template.yaml \
  --parameters ParameterKey=NamePrefix,ParameterValue="$NAME_PREFIX" \
  --endpoint-url "$AWS_ENDPOINT_URL" --region "$AWS_REGION"

# Wait and export outputs for integration tests
aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME" --endpoint-url "$AWS_ENDPOINT_URL" --region "$AWS_REGION"
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --endpoint-url "$AWS_ENDPOINT_URL" --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output json > stack_outputs.json

# Integration tests
pytest tests/integration_tests.py

# Destroy
aws cloudformation delete-stack --stack-name "$STACK_NAME" --endpoint-url "$AWS_ENDPOINT_URL" --region "$AWS_REGION"
```
