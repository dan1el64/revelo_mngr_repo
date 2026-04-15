# CDK Python App

This repository contains a single-file AWS CDK for Python implementation in `app.py`.

## Allowed inputs

The app reads only these environment variables:

- `AWS_ENDPOINT`
- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

`AWS_REGION` defaults to `us-east-1` when it is not provided.

## Commands

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Run the unit tests:

```bash
python3 -m pytest tests/unit_tests.py -q
```
