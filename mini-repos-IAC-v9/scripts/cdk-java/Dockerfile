# Development/testing image for agents writing CDK Java solutions.
# Includes Java 17, Maven, Python, Node/npm for cdklocal (LocalStack).
# Works on both Linux (amd64/arm64) and macOS hosts (Intel/Apple Silicon).
FROM eclipse-temurin:17-jdk-jammy

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

# Node 20 LTS (Node 18 EOL causes cdklocal to print warnings); Jammy's nodejs is too old
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install cdklocal and aws-cdk; CLI must be >=2.1033.0 to read schema 48 from aws-cdk-lib 2.184.x
RUN npm install -g aws-cdk-local@2.19.2 aws-cdk@2.1033.0

# Install Python CDK dependencies for tests
RUN pip3 install --no-cache-dir "boto3==1.42.89" "pytest==9.0.3"

# Pre-download Maven dependencies for faster builds
# Copy pom.xml and download dependencies in a separate layer
WORKDIR /cache
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Create a sample pom.xml with common CDK dependencies
# This layer will be cached unless pom.xml structure changes
COPY pom.xml ./
RUN mvn dependency:resolve dependency:resolve-plugins -B

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
