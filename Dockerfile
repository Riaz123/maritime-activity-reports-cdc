# Maritime Activity Reports CDC/CDF - Dockerfile
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install PDM
RUN pip install pdm

# Copy dependency files
COPY pyproject.toml pdm.lock* ./

# Install dependencies
RUN pdm install --prod --no-lock --no-editable

# Copy source code
COPY src/ src/
COPY config/ config/
COPY scripts/ scripts/

# Install the package
RUN pdm install --prod --no-lock --no-editable

# Create non-root user
RUN useradd --create-home --shell /bin/bash maritime
RUN chown -R maritime:maritime /app
USER maritime

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import maritime_activity_reports; print('OK')" || exit 1

# Default command
CMD ["pdm", "run", "maritime-reports", "health-check"]

# Labels
LABEL maintainer="Data Engineering Team <data-team@your-company.com>"
LABEL version="1.0.0"
LABEL description="Maritime Activity Reports with CDC/CDF processing"
