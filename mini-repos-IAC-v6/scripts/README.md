# Harness scripts per framework

The harness runs one framework at a time. Each framework has a directory `scripts/<framework>/` containing a single entry script that the harness calls once per step.

## Contract

- **Entry script**: `scripts/<framework>/run.sh`
- **Arguments**: `run.sh <repo_dir> <step>`
  - `repo_dir` – absolute path to the task directory under `tasks/` (e.g. `tasks/terraform-hcl/example`)
  - `step` – one of: `setup` | `syntax_plan` | `unit_tests` | `deploy_localstack` | `integration_tests` | `destroy`
- **Exit codes**: Script must exit 0 on success, non‑zero on failure. Failures are recorded but do not stop later steps.
- **Output**: Step name in blue, command output in default color, success in green with ✅, failure in red with ❌.

## Steps (logical order)

| Step                | Purpose |
|---------------------|--------|
| `setup`             | Start worker container and install/set up the project. |
| `syntax_plan`       | Syntax/format check and plan (no apply). |
| `unit_tests`        | Run unit tests (e.g. Python/pytest). |
| `deploy_localstack` | Deploy to LocalStack (harness starts LocalStack beforehand). |
| `integration_tests` | Run integration tests. |
| `destroy`           | Tear down resources and worker container. |

## Supported frameworks

- **terraform-hcl** – `scripts/terraform-hcl/run.sh`; Terraform fmt/init/validate/plan/apply/destroy; pytest for unit/integration using `plan.json` and `state.json`.
- **cdk-java** – `scripts/cdk-java/run.sh`; CDK Java (Maven) with `cdklocal`; synth produces `template.json`; pytest for unit/integration.
- **cdk-js** – `scripts/cdk-js/run.sh`; CDK JavaScript with `cdklocal`; synth produces `template.json`; pytest for unit/integration.
- **cdk-py** – `scripts/cdk-py/run.sh`; CDK Python with `cdklocal`; same contract.
- **cdk-ts** – `scripts/cdk-ts/run.sh`; CDK TypeScript with `cdklocal`; synth produces `template.json`; pytest for unit/integration.
- **cfn-json** – `scripts/cfn-json/run.sh`; CloudFormation JSON template; `template.json`, validate, create-stack, pytest.
- **cfn-yaml** – `scripts/cfn-yaml/run.sh`; CloudFormation YAML template; same contract.
- **pulumi-java** – `scripts/pulumi-java/run.sh`; Pulumi Java (Maven) preview/up/destroy; pytest using `preview.json` and `state.json`.
- **pulumi-py** – `scripts/pulumi-py/run.sh`; Pulumi Python; same contract.
- **pulumi-ts** – `scripts/pulumi-ts/run.sh`; Pulumi TypeScript (Node.js); preview/up/destroy; pytest using `preview.json` and `state.json`.
- **pulumi-js** – `scripts/pulumi-js/run.sh`; Pulumi JavaScript (Node.js); preview/up/destroy; pytest using `preview.json` and `state.json`.

To add a new framework, create `scripts/<framework>/run.sh` that accepts `(repo_dir, step)` and implements the six steps above.
