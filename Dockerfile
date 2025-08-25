FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy project files for dependency installation
COPY --chown=airflow:root pyproject.toml README.md* /tmp/project/

# Install Airflow providers first
RUN pip install --no-cache-dir --user \
    apache-airflow-providers-celery \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    apache-airflow-providers-http \
    apache-airflow-providers-docker \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    apache-airflow-providers-microsoft-azure

# Install project dependencies from pyproject.toml
# Using pip install -e to install in editable mode reads from pyproject.toml
RUN cd /tmp/project && \
    pip install --no-cache-dir --user --no-deps \
    aiohttp>=3.12.15 \
    azure-storage-blob>=12.26.0 \
    boto3>=1.40.16 \
    faker>=37.5.3 \
    google-cloud-storage>=3.3.0 \
    great-expectations>=0.18.8 \
    httpx>=0.28.1 \
    numpy>=2.3.2 \
    pandas>=2.3.2 \
    psycopg2-binary>=2.9.10 \
    pyarrow>=21.0.0 \
    pydantic>=2.11.7 \
    pymongo>=4.14.1 \
    pytest>=8.4.1 \
    pytest-cov>=6.2.1 \
    python-dotenv>=1.1.1 \
    redis>=6.4.0 \
    requests>=2.32.5 \
    sentry-sdk>=2.35.0 \
    sqlalchemy>=2.0.43

# Clean up
RUN rm -rf /tmp/project

# Set working directory
WORKDIR /opt/airflow
