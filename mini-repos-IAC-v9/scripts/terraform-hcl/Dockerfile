# Development/testing image for agents writing Terraform solutions.
# Includes Python + pytest for unit/integration tests and Terraform for plan/apply/destroy.
# Works on both Linux (amd64/arm64) and macOS hosts (Intel/Apple Silicon).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2 (required for ACM certificate requests)
ARG TARGETARCH
RUN case "${TARGETARCH}" in \
    amd64) ARCH="x86_64" ;; \
    arm64) ARCH="aarch64" ;; \
    *) ARCH="x86_64" ;; \
    esac \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/awscliv2.zip /tmp/aws \
    && aws --version

# Install Terraform for current architecture.
# TARGETARCH is automatically set by Docker (amd64, arm64, etc.)
ARG TF_VERSION=1.9.0
RUN curl -fsSL "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_${TARGETARCH}.zip" -o /tmp/tf.zip \
    && unzip -q /tmp/tf.zip -d /usr/local/bin \
    && rm /tmp/tf.zip \
    && terraform version

# Install pytest and boto3 for tests
RUN pip install --no-cache-dir "boto3==1.42.89" "pytest==9.0.3"

# Set default environment variables for LocalStack testing.
# Agents get a working LocalStack setup out of the box; harness can override as needed.
ENV TF_VAR_aws_endpoint=http://localhost:4566 \
    TF_VAR_aws_region=us-east-1 \
    TF_VAR_aws_access_key_id=test \
    TF_VAR_aws_secret_access_key=test \
    TF_VAR_name_prefix=dev \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_EC2_METADATA_DISABLED=true

WORKDIR /app
