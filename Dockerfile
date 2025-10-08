FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/app

# Install DBT with PostgreSQL adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4 \
    sqlfluff==3.0.0 \
    sqlfluff-templater-dbt==3.0.0

# Create necessary directories
RUN mkdir -p /usr/app/dbt /usr/app/logs

# Set environment variables
ENV DBT_PROFILES_DIR=/usr/app/dbt
ENV PYTHONPATH=/usr/app

# Create non-root user for security
RUN groupadd -r dbt && useradd -r -g dbt dbt
RUN chown -R dbt:dbt /usr/app
USER dbt

# Default command
CMD ["tail", "-f", "/dev/null"]