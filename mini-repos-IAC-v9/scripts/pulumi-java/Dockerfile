# Pulumi Java + Python tests. Java 17, Maven, Pulumi CLI, pytest + boto3 for unit/integration tests.
FROM eclipse-temurin:17-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    maven \
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

# Install Pulumi CLI (pinned to avoid breaking changes from upstream updates)
RUN curl -fsSL https://get.pulumi.com | bash -s -- --version 3.225.1
ENV PATH="/root/.pulumi/bin:${PATH}"

# Install Python deps for tests (pytest, boto3)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Pre-download Maven dependencies for faster runs (same pattern as cdk-java)
WORKDIR /cache
COPY pom.xml ./
RUN mvn dependency:resolve dependency:resolve-plugins -B

ENV AWS_ENDPOINT=http://localhost:4566 \
    AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_EC2_METADATA_DISABLED=true \
    NAME_PREFIX=dev \
    PULUMI_CONFIG_PASSPHRASE= \
    PULUMI_BACKEND_URL=file:///work/.pulumi

WORKDIR /app
