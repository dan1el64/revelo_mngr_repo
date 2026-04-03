# Development/testing image for agents writing CloudFormation (YAML) solutions.
# Includes AWS CLI, Python, and pytest for validate/deploy to LocalStack.
# Works on both Linux (amd64/arm64) and macOS hosts (Intel/Apple Silicon).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2 (required for cloudformation validate-template, create-stack, etc.)
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

# Install Python dependencies for tests (boto3, pytest, PyYAML for template parsing)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set default environment for LocalStack testing; harness can override.
ENV AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    NAME_PREFIX=dev

WORKDIR /app
