# IaC harness (LocalStack)

This harness runs a standard pipeline for infrastructure-as-code tasks: **setup → syntax/plan → unit tests → deploy (LocalStack) → integration tests → destroy**. It supports multiple frameworks (Terraform, CDK, CloudFormation, Pulumi) and is intended for validation and testing against LocalStack.

## Prerequisites

- **Docker** and **Docker Compose** (LocalStack and per-framework worker containers)
- **Bash** (scripts use `set -euo pipefail`)
- For step timeouts: **GNU `timeout`** (common on Linux; optional on macOS)
- Set the environment variable with the auth token from LocalStack: `LOCALSTACK_AUTH_TOKEN`

### Apple Silicon (M1/M2/M3/M4) Macs

All worker containers run as `linux/amd64`. Docker Desktop uses emulation to run them on ARM hosts. For significantly faster performance, enable **Rosetta** emulation instead of the default QEMU:

1. Open **Docker Desktop → Settings → General**
2. Enable **"Use Rosetta for x86_64/amd64 emulation on Apple Silicon"**
3. Click **Apply & Restart**

Rosetta is Apple's native translation layer and is substantially faster than QEMU for amd64 workloads. Without it, deploy and test steps will be noticeably slower on Apple Silicon.

## Quick start

Run the harness for a single task (provide path relative to script root):

```bash
./harness.sh tasks/terraform-hcl/example
```

```bash
./harness.sh tasks/cdk-java/example
```

Run the harness for **all** tasks and see a summary:

```bash
./run_all.sh
```

## Layout

| Path | Purpose |
|------|--------|
| `harness.sh` | Entry point: starts LocalStack, runs one task through setup → destroy, prints step summary. |
| `run_all.sh` | Runs the harness for every task under `tasks/` (by default skips `template` and `example`), then prints pass/fail summary. |
| `tasks/<framework>/<task>/` | One task per directory (e.g. `terraform-hcl/example`, `cdk-java/template`). |
| `scripts/<framework>/run.sh` | Per-framework script that implements the six steps for a given task. |
| `scripts/docker-compose.yml` | LocalStack service used by the harness (single shared instance). |
| `runs/<date_time>/<framework>/<task_id>/log` | Per-task logs from `run_all.sh` (one directory per run, then per framework and task). |

## Running a single task

From this directory (the one containing `harness.sh`):

```bash
./harness.sh tasks/<framework>/<task>
```

Examples:

- `./harness.sh tasks/terraform-hcl/example`
- `./harness.sh tasks/cfn-json/template`
- `./harness.sh tasks/pulumi-py/example`

The harness will:

1. Start LocalStack (if `scripts/docker-compose.yml` is present).
2. Run, in order: **setup** → **syntax/plan** → **unit tests** → **deploy (LocalStack)** → **integration tests** → **destroy/cleanup**.
3. Print a summary (each step ✅ or ❌). Exit code is 0 only if all steps passed.
4. Stop LocalStack on exit (trap).

Steps that fail do not stop the run; the harness continues and reports the overall result at the end.

### Options

- `--step-timeout N` – Max seconds per step (default: 300). Use `0` to disable timeouts.
- `--lock-wait N` – Max seconds to wait for LocalStack lock (default: 900).
- `--output PATH` – Path to log file for output (default: `runs/<task_path>.log`).

By default, all output is logged to `runs/<task_path>.log` (e.g., `runs/tasks_terraform-hcl_example.log`).

Examples:

```bash
./harness.sh --step-timeout 600 tasks/terraform-hcl/example
./harness.sh --output ./logs/task.log tasks/cdk-py/example
./harness.sh --step-timeout 600 --lock-wait 1200 --output ./logs/task.log tasks/terraform-hcl/example
```

## Frameworks and tasks

Frameworks under `scripts/`: **terraform-hcl**, **cdk-java**, **cdk-js**, **cdk-py**, **cdk-ts**, **cfn-json**, **cfn-yaml**, **pulumi-java**, **pulumi-js**, **pulumi-py**, **pulumi-ts**.

Task directories under `tasks/<framework>/` typically include **example** (filled sample) and **template** (minimal stub). See `scripts/README.md` for the per-framework contract and step details.
