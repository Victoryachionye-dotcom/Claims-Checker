# =============================================================================
# Healthcare Claims RAG Engine — Dockerfile
# Python 3.11 slim — production-ready FastAPI container
# =============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required by some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/
COPY scripts/ ./scripts/
COPY data/mock_claims/ ./data/mock_claims/

# Create runtime directories (not committed to version control)
RUN mkdir -p data/policy_kb data/vector_store data/batch_input \
             data/batch_output data/hold_queue data/reports

# Non-root user for security
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Expose FastAPI port
EXPOSE 8000

# Start the API server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
