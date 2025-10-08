FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app

# --- OPTIMIZATION START ---
# Copy only the requirements file first
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
# --- OPTIMIZATION END ---

# Copy the rest of the dbt project
COPY ./dbt /usr/app/dbt

# Set environment variable for dbt
ENV DBT_PROFILES_DIR=/usr/app/dbt

# Create non-root user for security
RUN groupadd -r dbt && useradd -r -g dbt dbt
# Change ownership of the entire app directory AFTER copying files
RUN chown -R dbt:dbt /usr/app
USER dbt

# Default command to keep container running if needed
CMD ["tail", "-f", "/dev/null"]
