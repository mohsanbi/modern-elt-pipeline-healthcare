# Start from official Airflow image
FROM apache/airflow:2.8.1

# Switch to root to install system dependencies
USER root

# Install Docker CLI for DockerOperator
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    rm -rf /var/lib/apt/lists/*

# Copy your requirements.txt
COPY requirements.txt /

# Switch to airflow user BEFORE installing Python dependencies
USER airflow

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir -r /requirements.txt

