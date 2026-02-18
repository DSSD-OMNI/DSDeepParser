FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 curl && rm -rf /var/lib/apt/lists/*

# Python deps (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Data directory (Railway persistent volume mounts here)
RUN mkdir -p /app/data /app/cache /app/exports

# Healthcheck via Prometheus metrics endpoint
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s \
    CMD curl -f http://localhost:8000/ || exit 1

# Metrics port
EXPOSE 8000

CMD ["python", "main.py"]
