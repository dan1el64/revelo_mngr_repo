#!/usr/bin/env bash
#
# Single CDK TypeScript harness script: setup, syntax/synth, unit tests, deploy (LocalStack),
# integration tests, destroy. Invoked per step by the main harness.
#
# Usage: run.sh <repo_dir> <step>
#   repo_dir - path to repository
#   step     - setup | syntax_plan | unit_tests | deploy_localstack | integration_tests | destroy
#
# Uses CDK CLI (cdklocal) with fixed IP network for LocalStack connectivity.
# All cdk/npm commands run inside a long-running container; LocalStack runs on host via docker-compose.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"
source "$COMMON_DIR/localstack-validation.sh"
ACM_SCRIPT="$COMMON_DIR/request-acm-certificate.sh"

DOCKER_IMAGE="${DOCKER_IMAGE:-revelotalentcorp/iac-cdk-ts}"
CONTAINER_NAME="harness-worker-${HARNESS_PID:-$PPID}"
DOCKER_NETWORK="${DOCKER_NETWORK:-iac-harness-network}"
LOCALSTACK_IP="${LOCALSTACK_IP:-10.0.2.20}"
LOCALSTACK_PORT="${LOCALSTACK_PORT:-4566}"
AWS_ENDPOINT_URL="http://${LOCALSTACK_IP}:${LOCALSTACK_PORT}"

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

exec_in_container() {
  docker exec -w /work "$CONTAINER_NAME" bash -c "$*"
}

cleanup_container() {
  if docker ps -a --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Cleaning up worker container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}

trap_cleanup_if_destroy() {
  if [[ "${2:-}" == "destroy" ]]; then
    trap cleanup_container EXIT
  fi
}

# --- Step implementations ---

do_setup() {
  local repo_dir="$1"

  if docker ps --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing worker container to avoid stale state: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi

  local name_prefix="cdk-$(openssl rand -hex 8 2>/dev/null || echo "$$$(date +%s)")"

  echo "Starting worker container: $CONTAINER_NAME"
  docker run -d \
    --platform=linux/amd64 \
    --name "$CONTAINER_NAME" \
    --network "$DOCKER_NETWORK" \
    -v "$repo_dir:/work-ro:ro" \
    -e "NAME_PREFIX=$name_prefix" \
    -e "AWS_REGION=us-east-1" \
    -e "AWS_ACCESS_KEY_ID=test" \
    -e "AWS_SECRET_ACCESS_KEY=test" \
    -e "CDK_DEFAULT_ACCOUNT=000000000000" \
    -e "CDK_DEFAULT_REGION=us-east-1" \
    -e "CDK_DISABLE_CLI_TELEMETRY=true" \
    -e "CDK_DISABLE_LEGACY_EXPORT_WARNING=1" \
    -e "CDK_NOTICES=false" \
    -e "AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL" \
    -e "AWS_ENDPOINT_URL_S3=$AWS_ENDPOINT_URL" \
    -e "AWS_ENDPOINT=$AWS_ENDPOINT_URL" \
    -e "AcmCertificateArn=${AcmCertificateArn:-}" \
    "$DOCKER_IMAGE" \
    bash -c "cp -r /work-ro /work && tail -f /dev/null"

  # Wait for entrypoint to create /work (avoids "chdir to cwd (/work) failed" on first exec)
  local wait_max=20
  local waited=0
  while [[ $waited -lt $wait_max ]]; do
    if docker exec "$CONTAINER_NAME" test -d /work 2>/dev/null; then
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  if [[ $waited -ge $wait_max ]]; then
    echo "Error: /work not ready after ${wait_max}s"
    return 1
  fi

  # Install dependencies
  echo "Installing dependencies (npm install)..."
  exec_in_container 'npm install'

  # Request ACM certificate inside container
  docker cp "$ACM_SCRIPT" "$CONTAINER_NAME:/tmp/request-acm-certificate.sh"
  
  # Capture and export for CDK context
  export AcmCertificateArn=$(
    exec_in_container 'source /tmp/request-acm-certificate.sh && request_acm_certificate >/dev/null && echo "$ACM_CERTIFICATE_ARN"' | tail -n 1
  )
  echo "Captured ACM Certificate ARN: $AcmCertificateArn"
}

do_syntax_plan() {
  local failed=0

  echo "Checking for LocalStack references..."
  if ! exec_in_container "$(declare -f validate_no_localstack_refs); validate_no_localstack_refs '*.ts' 'tests/*.py'"; then
    failed=1
  fi

  echo "Running cdklocal synth..."
  if ! exec_in_container 'cdklocal synth --quiet 2>/dev/null || cdklocal synth'; then
    failed=1
  fi

  # Copy first synthesized template to template.json for unit tests
  echo "Generating template.json for unit tests..."
  exec_in_container 'f=$(ls cdk.out/*.template.json 2>/dev/null | head -1); if [ -n "$f" ]; then cp "$f" template.json; else echo "{}" > template.json; fi' || failed=1

  if [[ $failed -ne 0 ]]; then
    return 1
  fi
}

do_unit_tests() {
  echo "Running pytest unit tests..."
  exec_in_container 'pytest tests/unit_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_deploy_localstack() {
  echo "Bootstrapping (if needed)..."
  exec_in_container 'cdklocal bootstrap 2>/dev/null || true'

  echo "Running cdklocal deploy..."
  exec_in_container 'cdklocal deploy --require-approval never --all --context AcmCertificateArn="$AcmCertificateArn"'
}

do_integration_tests() {
  echo "Running pytest integration tests..."
  exec_in_container 'pytest tests/integration_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_destroy() {
  echo "Running cdklocal destroy..."
  exec_in_container 'cdklocal destroy --force --all 2>/dev/null || true'

  cleanup_container
}

# --- Main dispatch ---

REPO_DIR="${1:?Usage: $0 <repo_dir> <step>}"
STEP="${2:?Usage: $0 <repo_dir> <step>}"

trap_cleanup_if_destroy "$REPO_DIR" "$STEP"

case "$STEP" in
  setup)
    run_step "Setup" do_setup "$REPO_DIR"
    ;;
  syntax_plan)
    run_step "Syntax / Synth" do_syntax_plan
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
