# CDK Python (AWS)

This repository contains a single-file AWS CDK Python app in [app.py](/Users/admin/Desktop/Documents_local/Revelo/revelo_mngr_repo/mini-repos-IAC-v7/tasks/cdk-py/c1ba8525-281a-4c44-a9ec-4301c6cd9aab/app.py) for deployment to AWS.

## Configuration

The app reads only one deployment input:

| Variable | Description | Default |
|----------|-------------|---------|
| `aws_region` | AWS region for the stack environment | `us-east-1` |

AWS credentials should come from the standard AWS credential chain available to the CDK process.

## Local verification

Install dependencies, synthesize, and run tests:

```bash
pip install -r requirements.txt
pytest -q
```
