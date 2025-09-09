# CAST AI Node Manager Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies and kubectl
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config.py .
COPY logger_utils.py .
COPY cast_utils.py .
COPY node_utils.py .
COPY alerts_utils.py .
COPY batch_utils.py .
COPY kubernetes_utils.py .
COPY main.py .

# Create non-root user
RUN useradd --create-home --shell /bin/bash cast-manager
USER cast-manager

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "main.py"]