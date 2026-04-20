# Development/testing image for agents writing Pulumi TypeScript solutions.
# Includes Node.js 20, Pulumi CLI, and Python + pytest for unit/integration tests.
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

# Install Pulumi (pinned to avoid breaking changes from upstream updates)
RUN curl -fsSL https://get.pulumi.com | bash -s -- --version 3.225.1
ENV PATH="/root/.pulumi/bin:${PATH}"

# Install Python deps for tests (boto3, pytest)
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Set default environment variables for LocalStack testing.
ENV AWS_ENDPOINT=http://localhost:4566 \
    AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_EC2_METADATA_DISABLED=true \
    NAME_PREFIX=dev \
    PULUMI_CONFIG_PASSPHRASE="local-dev" \
    PULUMI_BACKEND_URL=file:///work/.pulumi

WORKDIR /app
