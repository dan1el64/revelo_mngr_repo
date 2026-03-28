#!/usr/bin/env bash
#
# Request ACM certificate from LocalStack and export the ARN.
# This script is sourced by framework run.sh scripts during setup.
#
# Must be run inside a container with AWS CLI and AWS_ENDPOINT_URL or AWS_ENDPOINT set.
# Exports: ACM_CERTIFICATE_ARN environment variable
# Returns: 0 on success, 0 on failure (never fails, just exports empty string)

request_acm_certificate() {
  local endpoint="${AWS_ENDPOINT_URL:-${AWS_ENDPOINT:-http://localhost:4566}}"
  local region="${AWS_REGION:-us-east-1}"
  
  echo "Requesting ACM certificate from LocalStack..."
  
  # Ensure AWS CLI is in PATH
  local aws_cmd="aws"
  if ! command -v aws >/dev/null 2>&1; then
    if command -v /usr/local/bin/aws >/dev/null 2>&1; then
      aws_cmd="/usr/local/bin/aws"
    elif command -v /usr/bin/aws >/dev/null 2>&1; then
      aws_cmd="/usr/bin/aws"
    else
      echo "Warning: AWS CLI not found in PATH"
      export ACM_CERTIFICATE_ARN=""
      return 0
    fi
  fi
  
  local acm_response
  acm_response=$($aws_cmd acm request-certificate \
    --domain-name local.test \
    --region "$region" \
    --endpoint-url "$endpoint" \
    --output json 2>&1) || true
  
  if echo "$acm_response" | grep -q '"CertificateArn"'; then
    export ACM_CERTIFICATE_ARN=$(echo "$acm_response" | grep -o '"CertificateArn": "[^"]*"' | cut -d'"' -f4)
    echo "ACM Certificate ARN: $ACM_CERTIFICATE_ARN"
    return 0
  else
    echo "Warning: Failed to request ACM certificate"
    echo "Response: $acm_response"
    export ACM_CERTIFICATE_ARN=""
    return 0
  fi
}
