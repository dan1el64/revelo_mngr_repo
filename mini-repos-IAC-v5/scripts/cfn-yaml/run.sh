#!/usr/bin/env bash
#
# Single CloudFormation (YAML) harness script: setup, syntax/validate, unit tests,
# deploy (LocalStack), integration tests, destroy. Invoked per step by the main harness.
#
# Usage: run.sh <repo_dir> <step>
#   repo_dir - path to repository
#   step     - setup | syntax_plan | unit_tests | deploy_localstack | integration_tests | destroy
#
# Expects template.yaml (or template.yml) in repo root. Uses AWS CLI and fixed IP network for LocalStack.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"
source "$COMMON_DIR/localstack-validation.sh"
ACM_SCRIPT="$COMMON_DIR/request-acm-certificate.sh"

DOCKER_IMAGE="${DOCKER_IMAGE:-revelotalentcorp/iac-cfn-yaml}"
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

  local name_prefix="cfn-$(openssl rand -hex 8 2>/dev/null || echo "$$$(date +%s)")"

  echo "Starting worker container: $CONTAINER_NAME"
  docker run -d \
    --platform=linux/amd64 \
    --name "$CONTAINER_NAME" \
    --network "$DOCKER_NETWORK" \
    -v "$repo_dir:/work-ro:ro" \
    -e "NAME_PREFIX=$name_prefix" \
    -e "STACK_NAME=$name_prefix" \
    -e "AWS_REGION=us-east-1" \
    -e "AWS_ACCESS_KEY_ID=test" \
    -e "AWS_SECRET_ACCESS_KEY=test" \
    -e "AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL" \
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

  # Request ACM certificate inside container
  docker cp "$ACM_SCRIPT" "$CONTAINER_NAME:/tmp/request-acm-certificate.sh"

  # Capture the ARN from container and export on host for parameter passing
  export AcmCertificateArn=$(
    exec_in_container 'source /tmp/request-acm-certificate.sh && request_acm_certificate >/dev/null && echo "$ACM_CERTIFICATE_ARN"' | tail -n 1
  )
  echo "Captured ACM Certificate ARN: $AcmCertificateArn"
}

do_syntax_plan() {
  local failed=0

  echo "Checking for LocalStack references..."
  if ! exec_in_container "$(declare -f validate_no_localstack_refs); validate_no_localstack_refs '*.yaml' '*.yml' 'tests/*.py'"; then
    failed=1
  fi

  local tpl
  tpl="$(exec_in_container 'if [ -f template.yaml ]; then echo template.yaml; elif [ -f template.yml ]; then echo template.yml; else echo template.yaml; fi')"
  echo "Validating CloudFormation template ($tpl)..."
  if ! exec_in_container "aws cloudformation validate-template --template-body file://$tpl --endpoint-url \"\$AWS_ENDPOINT_URL\" 2>/dev/null || aws cloudformation validate-template --template-body file://$tpl"; then
    failed=1
  else
    echo "Template is valid."
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
  local tpl
  tpl="$(exec_in_container 'if [ -f template.yaml ]; then echo template.yaml; elif [ -f template.yml ]; then echo template.yml; else echo template.yaml; fi')"
  echo "Creating stack..."
  if ! exec_in_container "aws cloudformation create-stack \
    --stack-name \"\$STACK_NAME\" \
    --template-body file://$tpl \
    --parameters ParameterKey=NamePrefix,ParameterValue=\"\$NAME_PREFIX\" \
                 ParameterKey=AcmCertificateArn,ParameterValue=\"\$AcmCertificateArn\" \
    --endpoint-url \"\$AWS_ENDPOINT_URL\" \
    --region \"\$AWS_REGION\" \
    --no-cli-pager"; then
    return 1
  fi
  echo "Waiting for stack to complete..."
  if ! exec_in_container "aws cloudformation wait stack-create-complete --stack-name \"\$STACK_NAME\" --endpoint-url \"\$AWS_ENDPOINT_URL\" --region \"\$AWS_REGION\""; then
    return 1
  fi
  echo "Generating stack_outputs.json for integration tests..."
  exec_in_container "aws cloudformation describe-stacks --stack-name \"\$STACK_NAME\" --endpoint-url \"\$AWS_ENDPOINT_URL\" --region \"\$AWS_REGION\" --query 'Stacks[0].Outputs' --output json > stack_outputs.json || echo '[]' > stack_outputs.json"
  exec_in_container "aws cloudformation describe-stacks --stack-name \"\$STACK_NAME\" --endpoint-url \"\$AWS_ENDPOINT_URL\" --region \"\$AWS_REGION\" --output json > stack_description.json || echo '{}' > stack_description.json"
}

do_integration_tests() {
  echo "Running pytest integration tests..."
  exec_in_container 'pytest tests/integration_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_destroy() {
  echo "Deleting stack..."
  exec_in_container "aws cloudformation delete-stack --stack-name \"\$STACK_NAME\" --endpoint-url \"\$AWS_ENDPOINT_URL\" --region \"\$AWS_REGION\" 2>/dev/null || true"
  exec_in_container "aws cloudformation wait stack-delete-complete --stack-name \"\$STACK_NAME\" --endpoint-url \"\$AWS_ENDPOINT_URL\" --region \"\$AWS_REGION\" 2>/dev/null || true"
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
    run_step "Syntax / Validate" do_syntax_plan
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
