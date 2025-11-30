FROM python:3.10-slim

# Install OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Copy dbt-specific requirements
COPY requirements.txt /requirements.txt

# Install dbt + bigquery
RUN pip install --no-cache-dir -r /requirements.txt

# Create working directory
WORKDIR /usr/app

# Keep container running
CMD ["sleep", "infinity"]
