#!/usr/bin/env bash
#
# Single Terraform harness script: setup, syntax/plan, unit tests, deploy (LocalStack),
# integration tests, destroy. Invoked per step by the main harness.
#
# Usage: run.sh <repo_dir> <step>
#   repo_dir - path to repository
#   step     - setup | syntax_plan | unit_tests | deploy_localstack | integration_tests | destroy
#
# All terraform/pytest commands run inside a long-running container.
# LocalStack runs on host via docker-compose.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"
source "$COMMON_DIR/localstack-validation.sh"
ACM_SCRIPT="$COMMON_DIR/request-acm-certificate.sh"

DOCKER_IMAGE="${DOCKER_IMAGE:-revelotalentcorp/iac-terraform-hcl}"
# Use parent PID (harness.sh) so container name is consistent across all invocations
CONTAINER_NAME="harness-worker-${HARNESS_PID:-$PPID}"
# terraform uses --network host so it reaches LocalStack via localhost on the gateway port

# Colors and emojis
C_BLUE='\033[34m'
C_GREEN='\033[32m'
C_RED='\033[31m'
C_RESET='\033[0m'
EMOJI_OK='✅'
EMOJI_FAIL='❌'

print_step() {
  echo ""
  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
  echo -e "${C_BLUE}  $*${C_RESET}"
  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
}

print_success() {
  echo -e "${C_GREEN}${EMOJI_OK} $*${C_RESET}"
}

print_fail() {
  echo -e "${C_RED}${EMOJI_FAIL} $*${C_RESET}"
}

run_step() {
  local step_name="$1"
  shift
  print_step "Step: $step_name"
  set +e
  "$@"
  local r=$?
  set -e
  if [[ $r -eq 0 ]]; then
    print_success "Step passed: $step_name"
    return 0
  else
    print_fail "Step failed: $step_name"
    return 1
  fi
}

# Helper: execute command in the container
exec_in_container() {
  docker exec -w /work "$CONTAINER_NAME" bash -c "$*"
}

# Helper: cleanup container
cleanup_container() {
  if docker ps -a --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Cleaning up worker container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}

# Trap to ensure cleanup on unexpected exit (only for destroy step)
trap_cleanup_if_destroy() {
  if [[ "${2:-}" == "destroy" ]]; then
    trap cleanup_container EXIT
  fi
}

# --- Step implementations ---

do_setup() {
  local repo_dir="$1"

  # If container already exists (e.g. from a previous run that did not reach destroy),
  # remove it and start fresh so no stale state is reused.
  if docker ps --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing worker container to avoid stale state: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi

  # Generate random name prefix for collision avoidance (never saved to repo)
  local name_prefix="tf-$(openssl rand -hex 8 2>/dev/null || echo "$$$(date +%s)")"

  # Start worker container with all environment variables
  echo "Starting worker container: $CONTAINER_NAME"
  docker run -d \
    --platform=linux/amd64 \
    --name "$CONTAINER_NAME" \
    -v "$repo_dir:/work-ro:ro" \
    --network host \
    -e "TF_VAR_name_prefix=$name_prefix" \
    -e "TF_VAR_aws_endpoint=http://localhost:4566" \
    -e "TF_VAR_aws_region=us-east-1" \
    -e "TF_VAR_acm_certificate_arn=${AcmCertificateArn:-}" \
    -e "AWS_ACCESS_KEY_ID=test" \
    -e "AWS_SECRET_ACCESS_KEY=test" \
    -e "AWS_DEFAULT_REGION=us-east-1" \
    -e "AWS_EC2_METADATA_DISABLED=true" \
    "$DOCKER_IMAGE" \
    bash -c "mkdir -p /work && cp -a /work-ro/. /work/ && touch /work/.harness_ready && tail -f /dev/null"

  # Wait until copy is fully complete (not only /work directory creation).
  # Without this, syntax/plan may start while cp is still running and Terraform sees an empty directory.
  local wait_max=20
  local waited=0
  while [[ $waited -lt $wait_max ]]; do
    if docker exec "$CONTAINER_NAME" test -f /work/.harness_ready 2>/dev/null; then
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  if [[ $waited -ge $wait_max ]]; then
    echo "Error: /work not ready after ${wait_max}s"
    return 1
  fi

  # Request ACM certificate inside container
  docker cp "$ACM_SCRIPT" "$CONTAINER_NAME:/tmp/request-acm-certificate.sh"
  
  # Capture and export for TF_VAR
  export AcmCertificateArn=$(
    exec_in_container 'source /tmp/request-acm-certificate.sh && request_acm_certificate >/dev/null && echo "$ACM_CERTIFICATE_ARN"' | tail -n 1
  )
  echo "Captured ACM Certificate ARN: $AcmCertificateArn"
}

do_syntax_plan() {
  local failed=0

  echo "Checking for LocalStack references..."
  if ! exec_in_container "$(declare -f validate_no_localstack_refs); validate_no_localstack_refs '*.tf' 'tests/*.py'"; then
    failed=1
  fi

  echo "Running terraform fmt -check..."
  exec_in_container 'terraform fmt -check -diff' || failed=1

  echo "Running terraform init..."
  exec_in_container 'terraform init -input=false -backend=false' || failed=1

  echo "Running terraform validate..."
  exec_in_container 'terraform validate' || failed=1

  echo "Running terraform plan (no apply)..."
  if ! exec_in_container 'terraform plan -input=false -out=.tfplan; exit_code=$?; (test $exit_code -eq 0 || test $exit_code -eq 2)'; then
    failed=1
  fi

  # Try to generate plan.json for unit tests even if earlier checks failed.
  # This allows downstream tests to run with plan data when plan output exists.
  echo "Generating plan.json for unit tests..."
  if ! exec_in_container 'if [ -f .tfplan ]; then terraform show -json .tfplan > plan.json; else echo ".tfplan not found; cannot generate plan.json"; exit 1; fi'; then
    failed=1
  fi

  if [[ $failed -ne 0 ]]; then
    return 1
  fi
}

do_unit_tests() {
  echo "Running pytest unit tests..."
  exec_in_container 'pytest tests/unit_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_deploy_localstack() {
  echo "Running terraform apply..."
  if ! exec_in_container 'terraform apply -input=false -auto-approve'; then
    return 1
  fi

  # Generate state.json for integration tests (stays in container)
  echo "Generating state.json for integration tests..."
  exec_in_container 'terraform show -json > state.json'
}

do_integration_tests() {
  echo "Running pytest integration tests..."
  exec_in_container 'pytest tests/integration_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_destroy() {
  echo "Running terraform destroy..."
  local destroy_ok=0
  exec_in_container 'terraform destroy -input=false -auto-approve' || destroy_ok=1

  # Cleanup container even when destroy failed (e.g. snapshot error, stuck resource)
  cleanup_container

  [[ $destroy_ok -eq 0 ]] || return 1
}

# --- Main dispatch ---

REPO_DIR="${1:?Usage: $0 <repo_dir> <step>}"
STEP="${2:?Usage: $0 <repo_dir> <step>}"

# Set trap for destroy step to ensure cleanup
trap_cleanup_if_destroy "$REPO_DIR" "$STEP"

case "$STEP" in
  setup)
    run_step "Setup" do_setup "$REPO_DIR"
    ;;
  syntax_plan)
    run_step "Syntax / Plan" do_syntax_plan
    ;;
  unit_tests)
    run_step "Unit tests (pytest)" do_unit_tests
    ;;
  deploy_localstack)
    run_step "Deploy (LocalStack)" do_deploy_localstack
    ;;
  integration_tests)
    run_step "Integration tests" do_integration_tests
    ;;
  destroy)
    run_step "Destroy / Cleanup" do_destroy
    ;;
  *)
    print_fail "Unknown step: $STEP"
    exit 1
    ;;
esac
