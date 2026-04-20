#!/usr/bin/env bash
#
# Single Pulumi Python harness script: setup, syntax/preview, unit tests, deploy (LocalStack),
# integration tests, destroy. Invoked per step by the main harness.
#
# Usage: run.sh <repo_dir> <step>
#   repo_dir - path to repository
#   step     - setup | syntax_plan | unit_tests | deploy_localstack | integration_tests | destroy
#
# All pulumi/pytest commands run inside a long-running container.
# LocalStack runs on host via docker-compose.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"
source "$COMMON_DIR/localstack-validation.sh"
ACM_SCRIPT="$COMMON_DIR/request-acm-certificate.sh"

DOCKER_IMAGE="${DOCKER_IMAGE:-revelotalentcorp/iac-pulumi-py}"
# Use parent PID (harness.sh) so container name is consistent across all invocations
CONTAINER_NAME="harness-worker-${HARNESS_PID:-$PPID}"
LOCALSTACK_HOST="${LOCALSTACK_HOST:-host.docker.internal}"
LOCALSTACK_PORT="${LOCALSTACK_PORT:-4566}"

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


configure_pulumi_localstack_endpoints() {
  # Keep this list broad so any task can target LocalStack without per-task harness edits.
  local services=(
    acm apigateway apigatewayv2 applicationautoscaling appsync athena
    autoscaling backup cloudformation cloudwatch logs dynamodb ec2 ecr ecs efs eks
    elasticache elasticbeanstalk elb elbv2 emr events firehose glue iam iot kinesis kms
    lambda mq msk neptune opensearch organizations qldb rds redshift redshiftdata route53
    route53resolver s3 s3control sagemaker secretsmanager servicediscovery servicequotas
    ses sns sqs ssm sfn sts transfer waf wafregional wafv2 xray
  )

  local service
  for service in "${services[@]}"; do
    exec_in_container "pulumi config set --plaintext --path aws:endpoints[0].${service} \"\$AWS_ENDPOINT\""
  done
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

  # Check if container already exists
  if docker ps --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing worker container to avoid stale state: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi

  # Generate random name prefix for collision avoidance (never saved to repo)
  local name_prefix="pulumi-$(openssl rand -hex 8 2>/dev/null || echo "$$$(date +%s)")"

  # Start worker container with all environment variables
  echo "Starting worker container: $CONTAINER_NAME"
  docker run -d \
    --pull "${DOCKER_PULL_POLICY:-always}" \
    --platform=linux/amd64 \
    --name "$CONTAINER_NAME" \
    -v "$repo_dir:/work-ro:ro" \
    --add-host=host.docker.internal:host-gateway \
    -e "NAME_PREFIX=$name_prefix" \
    -e "AWS_ENDPOINT=http://${LOCALSTACK_HOST}:${LOCALSTACK_PORT}" \
    -e "AWS_REGION=us-east-1" \
    -e "AWS_ACCESS_KEY_ID=test" \
    -e "AWS_SECRET_ACCESS_KEY=test" \
    -e "PULUMI_CONFIG_PASSPHRASE=" \
    -e "PULUMI_BACKEND_URL=file:///work/.pulumi" \
    -e "AcmCertificateArn=${AcmCertificateArn:-}" \
    "$DOCKER_IMAGE" \
    bash -c "cp -r /work-ro /work && touch /work/.harness_ready && tail -f /dev/null"

  # Wait for the copy to fully complete before running any exec commands.
  # Checking for .harness_ready (written after cp -r finishes) avoids a race where
  # /work exists but its contents are still being copied.
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
  
  # Capture and export for Pulumi config
  export AcmCertificateArn=$(
    exec_in_container 'source /tmp/request-acm-certificate.sh && request_acm_certificate >/dev/null && echo "$ACM_CERTIFICATE_ARN"' | tail -n 1
  )
  echo "Captured ACM Certificate ARN: $AcmCertificateArn"
}

do_syntax_plan() {
  local failed=0

  echo "Checking for LocalStack references..."
  if ! exec_in_container "$(declare -f validate_no_localstack_refs); validate_no_localstack_refs '*.py' '__main__.py' 'tests/*.py'"; then
    failed=1
  fi

  echo "Creating .pulumi directory..."
  exec_in_container 'mkdir -p .pulumi' || failed=1

  echo "Running pulumi stack init (if needed)..."
  exec_in_container 'pulumi stack select dev 2>/dev/null || pulumi stack init dev' || failed=1

  echo "Configuring Pulumi AWS provider for LocalStack..."
  exec_in_container 'pulumi config set namePrefix "$NAME_PREFIX"' || failed=1
  exec_in_container 'pulumi config set aws:region "$AWS_REGION"' || failed=1
  exec_in_container 'pulumi config set aws:skipCredentialsValidation true' || failed=1
  exec_in_container 'pulumi config set aws:skipMetadataApiCheck true' || failed=1
  exec_in_container 'pulumi config set aws:skipRequestingAccountId true' || failed=1
  exec_in_container 'pulumi config set aws:s3UsePathStyle true' || failed=1
  exec_in_container 'pulumi config set acmCertificateArn "$AcmCertificateArn"' || failed=1
  configure_pulumi_localstack_endpoints || failed=1

  echo "Running pulumi preview..."
  if ! exec_in_container 'pulumi preview --non-interactive'; then
    failed=1
  fi

  # Generate preview.json for unit tests (stays in container)
  echo "Generating preview.json for unit tests..."
  exec_in_container 'pulumi preview --non-interactive --json > preview.json || echo "{}" > preview.json' || failed=1

  if [[ $failed -ne 0 ]]; then
    return 1
  fi
}

do_unit_tests() {
  echo "Running pytest unit tests..."
  exec_in_container 'pytest tests/unit_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_deploy_localstack() {
  echo "Running pulumi up..."
  if ! exec_in_container 'pulumi up --yes --non-interactive'; then
    return 1
  fi

  # Generate state.json for integration tests (stays in container)
  echo "Generating state.json for integration tests..."
  exec_in_container 'pulumi stack export > state.json'
}

do_integration_tests() {
  echo "Running pytest integration tests..."
  exec_in_container 'pytest tests/integration_tests.py -v --tb=short 2>&1 | tee /tmp/pytest.out; r=${PIPESTATUS[0]}; if grep -qE "[0-9]+ skipped" /tmp/pytest.out; then echo ""; echo "ERROR: One or more tests were skipped. Skipped tests are not allowed and cause this step to fail."; exit 1; fi; exit $r'
}

do_destroy() {
  echo "Running pulumi destroy..."
  exec_in_container 'pulumi destroy --yes --non-interactive || true'

  # Cleanup container
  cleanup_container
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
    run_step "Syntax / Preview" do_syntax_plan
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
