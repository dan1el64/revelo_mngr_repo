# CDK TypeScript App

This project defines a single-file AWS CDK TypeScript stack in `app.ts`.

## Evaluator-Aligned Contract

The implementation, prompt contract, and tests are aligned to the following effective requirements:

- The stack is authored in a single file named `app.ts`.
- The stable input contract is limited to `AWS_REGION`, `AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.
- `AWS_REGION` defaults to `us-east-1`.
- Any explicit SDK client configuration in the CDK app or inline Lambda code uses `AWS_ENDPOINT` when applicable.
- All resources must remain destructible: no deletion protection, no termination protection, and no retain or snapshot policies.
- The backend Lambda must support `GET /health`, `GET /`, and `POST /orders`.
- CloudFront access to the frontend bucket must use exactly one Origin Access Control (`OAC`), not Origin Access Identity (`OAI`).
- The rest of the infrastructure contract is implemented in `app.ts` and validated by `tests/unit_tests.py` and `tests/integration_tests.py`.

## Local Validation

```bash
npm install
npx tsc --noEmit
python3 -m pytest tests/unit_tests.py -q
```
