FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/cache /app/exports

CMD sh -c "python main.py 2>&1 | tee /tmp/app.log; echo 'App finished, sleeping for debug...'; sleep 3600"
