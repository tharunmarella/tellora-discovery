FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install dependencies first (layer-cached separately from code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# On-demand signal worker (Watch / enrich). Railway cron that still calls
# python __main__.py will no-op.
CMD ["arq", "worker.WorkerSettings"]
