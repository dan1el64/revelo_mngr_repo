#!/usr/bin/env bash
#
# Harness: runs syntax/plan, unit tests, deploy (LocalStack), integration tests, and destroy.
# Runs all steps even if one fails; final exit code is 0 only if every step succeeded.
#
# Usage: ./harness.sh [--step-timeout N] [--lock-wait N] [--output PATH] [--docker-build-image DIR] <repo_path>
#   --step-timeout N  Max seconds per step (default: 1200 = 20 min). Use 0 to disable.
#   --lock-wait N     Max seconds to wait for LocalStack lock (default: 1200 = 20 min). Only one task uses LocalStack at a time.
#   --output PATH     Path to log file for output (default: runs/<task_path>.log). Parent directory will be created if needed.
#   --docker-build-image DIR  Build Docker image from DIR (Dockerfile in DIR) and set DOCKER_IMAGE for run steps. Use for work_item runs so steps use the local image instead of a remote one.
#   repo_path        Path relative to tasks/ (e.g. terraform-hcl/template)
#
# Example: ./harness.sh terraform-hcl/template
# Example: ./harness.sh --step-timeout 600 --lock-wait 1200 terraform-hcl/example
# Example: ./harness.sh --output ./logs/run.log --docker-build-image /path/to/work_item tasks/terraform-hcl/uuid
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Stable ID for worker container name so all steps (run via timeout) use the same container
export HARNESS_PID=$$
HARNESS_COMPOSE="${SCRIPT_DIR}/scripts/docker-compose.yml"
STEP_NAMES=()    # Array of step names
STEP_RESULTS=()  # Array of results (0=pass, 1=fail) - parallel to STEP_NAMES
STEP_SKIPPED=()  # 1 if step was skipped (script missing/not executable), 0 otherwise
STEP_TIMEOUT=1200  # default 20 min; 0 = no timeout; overridden by --step-timeout
LOCK_WAIT=1200     # max seconds to wait for LocalStack lock (20 min); overridden by --lock-wait
OUTPUT_FILE=""     # output log file path; overridden by --output, defaults to runs/...
DOCKER_BUILD_IMAGE_DIR=""  # when set, build Docker image from this dir and export DOCKER_IMAGE (for work_item runs)


C_BLUE='\033[34m'
C_GREEN='\033[32m'
C_RED='\033[31m'
C_RESET='\033[0m'
EMOJI_OK='✅'
EMOJI_FAIL='❌'

# Trap to ensure LocalStack and worker cleanup on exit
# When HARNESS_SKIP_COMPOSE_CLEANUP is set (e.g. run_all), only remove our worker; run_all stops LocalStack.
# When multiple harness.sh run in parallel (e.g. two terminals), only the last to exit should stop LocalStack,
# so we try to acquire the lock non-blocking; only if we get it (nobody else using/waiting) do we run compose down.
cleanup_localstack() {
  local worker_name="harness-worker-${HARNESS_PID}"
  if docker ps -a -q --filter "name=^${worker_name}$" 2>/dev/null | grep -q .; then
    echo ""
    echo "Removing worker container: $worker_name"
    # Kill first so any dangling docker exec is terminated and rm -f does not block.
    docker kill "$worker_name" 2>/dev/null || true
    docker rm -f "$worker_name" 2>/dev/null || true
  fi
  if [[ -z "${HARNESS_SKIP_COMPOSE_CLEANUP:-}" ]] && [[ -f "$HARNESS_COMPOSE" ]]; then
    local do_stop=false
    if command -v flock >/dev/null 2>&1; then
      local lockfile="${SCRIPT_DIR}/.harness-localstack.lock"
      if exec 201>"$lockfile" 2>/dev/null && flock -n 201 2>/dev/null; then
        do_stop=true
      fi
      exec 201>&- 2>/dev/null || true
    else
      do_stop=true
    fi
    if [[ "$do_stop" == "true" ]]; then
      echo ""
      echo "Stopping LocalStack..."
      docker compose -f "$HARNESS_COMPOSE" down -v
    fi
  fi
}
trap cleanup_localstack EXIT

wait_for_localstack() {
  local endpoint="${LOCALSTACK_ENDPOINT:-http://localhost:4566/_localstack/health}"
  local timeout_seconds="${LOCALSTACK_READY_TIMEOUT:-45}"
  local waited=0

  echo "Waiting for LocalStack health endpoint: $endpoint"
  while [[ $waited -lt $timeout_seconds ]]; do
    if curl -fsS "$endpoint" >/dev/null 2>&1; then
      echo "LocalStack is healthy."
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done

  echo ""
  echo "ERROR: LocalStack is not available."
  echo "  Health check timed out after ${timeout_seconds}s at ${endpoint}"
  echo "  Ensure LocalStack is running, reachable, and LOCALSTACK_AUTH_TOKEN is correctly set."
  echo ""
  return 1
}

usage() {
  echo "Usage: $0 [--step-timeout N] [--lock-wait N] [--output PATH] [--docker-build-image DIR] <repo_path>"
  echo "  --step-timeout N  Max seconds per step (default: 1200 = 20 min). Use 0 to disable."
  echo "  --lock-wait N     Max seconds to wait for LocalStack lock (default: 1200 = 20 min)."
  echo "  --output PATH     Path to log file for output (default: runs/<task_path>.log)."
  echo "  --docker-build-image DIR  Build Docker image from DIR; set DOCKER_IMAGE for run steps (e.g. work_item root)."
  echo "  repo_path         Relative path from script root (e.g. tasks/terraform-hcl/template)"
  echo ""
  echo "Example: $0 tasks/terraform-hcl/template"
  echo "Example: $0 --step-timeout 600 --lock-wait 1200 tasks/terraform-hcl/example"
  echo "Example: $0 --output ./logs/run.log --docker-build-image /path/to/work_item tasks/terraform-hcl/uuid"
  exit 1
}

run_step() {
  local name="$1"
  local script="$2"
  local repo_dir="$3"
  local step_id="$4"

  STEP_NAMES+=("$name")

  if [[ -x "$script" ]] && [[ -f "$script" ]]; then
    local r=0
    if [[ -n "${STEP_TIMEOUT}" ]] && [[ "${STEP_TIMEOUT}" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
      # Run the step in a dedicated process group so that when timeout fires, we can kill
      # the entire group. Otherwise (e.g. with BusyBox timeout or when the step runs
      # "docker exec"), only the direct child (run.sh) is killed and children like
      # docker exec keep running, so the step never appears to finish.
      if command -v setsid >/dev/null 2>&1; then
        timeout "$STEP_TIMEOUT" bash -c '
          trap "kill -TERM -\$pgrp 2>/dev/null; exit 124" TERM
          setsid "$1" "$2" "$3" &
          pid=$!
          pgrp=$pid
          wait $pid
          exit $?
        ' _ "$script" "$repo_dir" "$step_id" && r=0 || r=$?
      else
        if timeout "$STEP_TIMEOUT" "$script" "$repo_dir" "$step_id"; then
          r=0
        else
          r=$?
        fi
      fi
      if [[ $r -eq 124 ]]; then
        echo ""
        echo -e "${C_RED}Step timed out after ${STEP_TIMEOUT}s.${C_RESET}"
        # Force-stop then remove the worker so a dangling docker exec (e.g. when setsid is
        # unavailable) does not leave a stuck container. docker kill stops the container and
        # terminates any exec sessions; then rm -f removes it without blocking.
        local worker_name="harness-worker-${HARNESS_PID}"
        docker kill "$worker_name" 2>/dev/null || true
        docker rm -f "$worker_name" 2>/dev/null || true
        r=1
      fi
    else
      if "$script" "$repo_dir" "$step_id"; then
        r=0
      else
        r=1
      fi
    fi
    if [[ $r -eq 0 ]]; then
      STEP_RESULTS+=(0)
      STEP_SKIPPED+=(0)
      return 0
    else
      STEP_RESULTS+=(1)
      STEP_SKIPPED+=(0)
      return 1
    fi
  else
    echo "[SKIP] $name (script not found or not executable: $script)"
    STEP_RESULTS+=(1)   # Skipped due to missing script = environment problem, treat as failure
    STEP_SKIPPED+=(1)
    return 1
  fi
}


main() {
  # Parse optional --step-timeout, --lock-wait, --output, --docker-build-image
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --step-timeout)
        shift
        if [[ $# -gt 0 ]] && [[ "$1" =~ ^[0-9]+$ ]]; then
          STEP_TIMEOUT="$1"
          shift
        else
          echo "Error: --step-timeout requires a number (seconds). Use 0 to disable."
          usage
        fi
        ;;
      --lock-wait)
        shift
        if [[ $# -gt 0 ]] && [[ "$1" =~ ^[0-9]+$ ]]; then
          LOCK_WAIT="$1"
          shift
        else
          echo "Error: --lock-wait requires a number (seconds)."
          usage
        fi
        ;;
      --output)
        shift
        if [[ $# -gt 0 ]]; then
          OUTPUT_FILE="$1"
          shift
        else
          echo "Error: --output requires a file path."
          usage
        fi
        ;;
      --docker-build-image)
        shift
        if [[ $# -gt 0 ]]; then
          DOCKER_BUILD_IMAGE_DIR="$1"
          shift
        else
          echo "Error: --docker-build-image requires a directory path."
          usage
        fi
        ;;
      *)
        break
        ;;
    esac
  done

  if [[ $# -lt 1 ]]; then
    usage
  fi

  # Path is relative to script directory (e.g. tasks/terraform-hcl/example)
  REPO_DIR="${SCRIPT_DIR}/$1"

  if [[ ! -d "$REPO_DIR" ]]; then
    echo "Error: repo folder does not exist: $REPO_DIR"
    exit 1
  fi

  # Extract framework from path after tasks/ (e.g. tasks/terraform-hcl/example -> terraform-hcl)
  # Remove leading "tasks/" if present, then extract first path component
  REPO_PATH="${1#tasks/}"
  FRAMEWORK="${REPO_PATH%%/*}"
  SCRIPTS_DIR="${SCRIPT_DIR}/scripts/${FRAMEWORK}"

  if [[ ! -d "$SCRIPTS_DIR" ]]; then
    echo "Error: no scripts for framework '$FRAMEWORK' at $SCRIPTS_DIR"
    exit 1
  fi

  # If output file not specified, generate default path: runs/<task_path>.log
  if [[ -z "$OUTPUT_FILE" ]]; then
    local task_path_safe
    task_path_safe="$(echo "$1" | tr '/' '_')"
    OUTPUT_FILE="${SCRIPT_DIR}/runs/${task_path_safe}.log"
  fi

  # Create output file parent directory if needed
  local output_dir
  output_dir="$(dirname "$OUTPUT_FILE")"
  if [[ ! -d "$output_dir" ]]; then
    mkdir -p "$output_dir"
  fi
  # When using incremental naming (harness_<N>.log), do not truncate; else truncate so one run replaces the file
  if [[ ! "$OUTPUT_FILE" =~ harness_[0-9]+\.log$ ]]; then
    : > "$OUTPUT_FILE"
  fi

  # Redirect all output to the log file (and still display to console via tee)
  echo "Logging output to: $OUTPUT_FILE"
  exec > >(tee "$OUTPUT_FILE") 2>&1

  echo "Harness: REPO_DIR=$REPO_DIR FRAMEWORK=$FRAMEWORK"
  echo ""

  # Register QEMU ARM64 emulation so LocalStack can run arm64 Lambda containers on x86_64 hosts.
  # This is a no-op on native ARM64 hosts or when already registered (Docker Desktop handles this
  # automatically; native WSL2 Docker does not).
  if [[ "$(uname -m)" == "x86_64" ]] && ! grep -q "^enabled" /proc/sys/fs/binfmt_misc/qemu-aarch64 2>/dev/null; then
    echo "Registering QEMU ARM64 emulation for arm64 Lambda support..."
    docker run --privileged --rm tonistiigi/binfmt --install arm64 >/dev/null 2>&1 || true
  fi

  # Optionally build Docker image from work_item root so run steps use local image instead of remote.
  # Tag: revelotalentcorp/iac-<framework>-<uuid> so each work_item has a unique image (work_item dir name = uuid).
  # Docker build output: when OUTPUT_FILE is harness_<N>.log, use docker_build_<N>.log for incremental numbering.
  if [[ -n "$DOCKER_BUILD_IMAGE_DIR" ]]; then
    if [[ -d "$DOCKER_BUILD_IMAGE_DIR" ]] && [[ -f "$DOCKER_BUILD_IMAGE_DIR/Dockerfile" ]]; then
      local docker_image_name="revelotalentcorp/iac-${FRAMEWORK}-$(basename "$DOCKER_BUILD_IMAGE_DIR")"
      local log_run_num=""
      if [[ "$OUTPUT_FILE" =~ harness_([0-9]+)\.log$ ]]; then
        log_run_num="${BASH_REMATCH[1]}"
      fi
      local docker_build_log="${DOCKER_BUILD_IMAGE_DIR}/execution_artifacts/logs/docker_build${log_run_num:+_$log_run_num}.log"
      mkdir -p "$(dirname "$docker_build_log")"
      if [[ -z "$log_run_num" ]]; then
        : > "$docker_build_log"   # non-incremental: truncate so one run replaces the file
      fi
      echo "Building Docker image from $DOCKER_BUILD_IMAGE_DIR -> $docker_image_name"
      echo "Docker build log: $docker_build_log"
      docker build -t "$docker_image_name" "$DOCKER_BUILD_IMAGE_DIR" 2>&1 | tee "$docker_build_log"
      export DOCKER_IMAGE="$docker_image_name"
      echo "DOCKER_IMAGE=$DOCKER_IMAGE"
      echo ""
    else
      echo "Error: --docker-build-image dir missing or has no Dockerfile: $DOCKER_BUILD_IMAGE_DIR"
      exit 1
    fi
  fi

  # Start LocalStack (shared by all frameworks)
  echo ""
  echo "Starting LocalStack (harness environment)..."
  if [[ -f "$HARNESS_COMPOSE" ]]; then
    docker compose -f "$HARNESS_COMPOSE" up -d
    wait_for_localstack
  else
    echo "Warning: docker-compose.yml not found at $HARNESS_COMPOSE"
  fi

  run_step "setup" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "setup" || true

  run_step "syntax/plan" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "syntax_plan" || true
  run_step "unit tests" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "unit_tests" || true

  # LocalStack steps (deploy, integration, destroy) run under a lock so only one task uses LocalStack at a time.
  # The lock is held for the entire sequence (deploy → integration_tests → destroy) and released only at "exec 200>&-".
  # If flock fails (e.g. timeout), we must not run these steps or another task could deploy and overwrite state before our integration_tests run.
  HARNESS_LOCK="${SCRIPT_DIR}/.harness-localstack.lock"
  if command -v flock >/dev/null 2>&1; then
    exec 200>"$HARNESS_LOCK"
    if ! flock -w "$LOCK_WAIT" 200; then
      echo ""
      echo -e "${C_RED}Failed to acquire LocalStack lock (${HARNESS_LOCK}). Another task may be using LocalStack. Exiting.${C_RESET}"
      exit 1
    fi
    run_step "deploy (localstack)" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "deploy_localstack" || true
    run_step "integration tests" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "integration_tests" || true
    run_step "destroy/cleanup" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "destroy" || true
    exec 200>&-
  else
    run_step "deploy (localstack)" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "deploy_localstack" || true
    run_step "integration tests" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "integration_tests" || true
    run_step "destroy/cleanup" "${SCRIPTS_DIR}/run.sh" "$REPO_DIR" "destroy" || true
  fi

  # LocalStack will be stopped by the EXIT trap

  echo ""
  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
  echo -e "${C_BLUE}  HARNESS SUMMARY${C_RESET}"
  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"

  local failed_count=0
  local i=0
  for step_name in "${STEP_NAMES[@]}"; do
    if [[ ${STEP_RESULTS[$i]} -eq 0 ]]; then
      echo -e "${C_GREEN}${EMOJI_OK} $step_name${C_RESET}"
    else
      if [[ ${STEP_SKIPPED[$i]:-0} -eq 1 ]]; then
        echo -e "${C_RED}${EMOJI_FAIL} $step_name (skipped - script not found or not executable)${C_RESET}"
      else
        echo -e "${C_RED}${EMOJI_FAIL} $step_name${C_RESET}"
      fi
      failed_count=$((failed_count + 1))
    fi
    i=$((i + 1))
  done

  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
  if [[ $failed_count -eq 0 ]]; then
    echo -e "${C_GREEN}${EMOJI_OK} All steps passed.${C_RESET}"
  else
    echo -e "${C_RED}${EMOJI_FAIL} $failed_count step(s) failed.${C_RESET}"
    if [[ " ${STEP_SKIPPED[*]} " == *" 1 "* ]]; then
      echo -e "${C_RED}  (Skipped steps indicate an environment problem: ensure scripts/<framework>/run.sh exists and is executable.)${C_RESET}"
    fi
  fi
  echo ""
  echo "Log saved to: $OUTPUT_FILE"
  echo -e "${C_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"

  if [[ $failed_count -eq 0 ]]; then
    exit 0
  else
    exit 1
  fi
}

main "$@"
