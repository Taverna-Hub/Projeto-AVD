FROM python:3.11-slim

WORKDIR /mlflow

# Install system dependencies
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  gcc \
  g++ \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*

# Install MLflow and PostgreSQL driver
RUN pip install --no-cache-dir \
  mlflow==2.9.2 \
  psycopg2-binary==2.9.9 \
  boto3==1.34.10

# Create directories for artifacts
RUN mkdir -p /mlflow/artifacts

# Expose MLflow port
EXPOSE 5000

# Default command (can be overridden in docker-compose)
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
