# Development/testing image for agents writing CDK JavaScript solutions.
# Includes Node.js 20 LTS, Python, and cdklocal (LocalStack). Node 18 is EOL and triggers CLI warnings.
# Works on both Linux (amd64/arm64) and macOS hosts (Intel/Apple Silicon).
FROM node:20-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    python3 \
    python3-pip \
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

# Install cdklocal and aws-cdk; CLI must be >=2.1033.0 to read schema 48 from aws-cdk-lib 2.184.x
RUN npm install -g aws-cdk-local@2.19.2 aws-cdk@2.1033.0

# Install Python dependencies for tests
RUN pip3 install --no-cache-dir --break-system-packages boto3 pytest

# Set default environment for LocalStack testing; harness can override.
# Fixed IP network approach eliminates DNS resolution issues.
ENV AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    CDK_DEFAULT_ACCOUNT=000000000000 \
    CDK_DISABLE_CLI_TELEMETRY=true \
    CDK_NOTICES=false \
    NAME_PREFIX=dev

WORKDIR /app
