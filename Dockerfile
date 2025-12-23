FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y make libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Python Deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Code
COPY . .

# Environment
ENV PYTHONPATH=/app

# Default command (overridden by compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]