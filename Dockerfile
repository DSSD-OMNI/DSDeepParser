FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/cache /app/exports

CMD ["sh", "-c", "python main-wrapper.py 2>&1 | tee /tmp/startup.log; echo "Container will stay alive for 1 hour"; sleep 3600"]
