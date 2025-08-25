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

# Copy and install Python requirements if exists
COPY --chown=airflow:root requirements.txt* /tmp/

RUN if [ -f /tmp/requirements.txt ]; then \
    pip install --no-cache-dir --user -r /tmp/requirements.txt; \
    fi

# Install common Airflow providers
RUN pip install --no-cache-dir --user \
    apache-airflow-providers-celery \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    apache-airflow-providers-http \
    apache-airflow-providers-docker \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    apache-airflow-providers-microsoft-azure \
    pandas \
    numpy \
    requests \
    sqlalchemy

# Set working directory
WORKDIR /opt/airflow
