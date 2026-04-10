# CDK Python

This repository contains a single-file AWS CDK Python stack in `app.py`.

## Supported Inputs

The implementation relies only on:

- `AWS_ENDPOINT`
- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

`AWS_REGION` defaults to `us-east-1` when it is not set.

## Tests

Install dependencies and run:

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```

The test suite enforces full coverage for `app.py`.
