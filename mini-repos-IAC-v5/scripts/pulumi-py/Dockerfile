# Development/testing image for agents writing Pulumi Python solutions.
# Includes Python + pytest for unit/integration tests and Pulumi for preview/up/destroy.
# Works on both Linux (amd64/arm64) and macOS hosts (Intel/Apple Silicon).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

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

# Install Pulumi
RUN curl -fsSL https://get.pulumi.com | bash
ENV PATH="/root/.pulumi/bin:${PATH}"

# Install Python deps (Pulumi AWS, boto3, pytest) so the base image is ready
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set default environment variables for LocalStack testing.
# Agents get a working LocalStack setup out of the box; harness can override as needed.
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
