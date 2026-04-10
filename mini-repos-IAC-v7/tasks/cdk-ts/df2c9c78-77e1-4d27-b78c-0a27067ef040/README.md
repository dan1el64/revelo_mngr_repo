# CDK TypeScript App

This repository contains a single-file AWS CDK TypeScript application in `app.ts`.

## Allowed external inputs

The app accepts only these environment variables:

- `AWS_ENDPOINT`
- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

`AWS_REGION` defaults to `us-east-1` when unset.

## Validation

Useful commands:

```bash
npx tsc
npx jest --coverage --runInBand
npx cdk synth
python3 -m pytest tests/unit_tests.py -q
python3 -m pytest tests/integration_tests.py -q
```
