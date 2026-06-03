FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install dependencies first (layer-cached separately from code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# DISCOVERY_MODE selects what this container runs:
#   scrape   → one-shot weekly discovery scrape (default; used by the cron service)
#   bulk     → persistent ARQ worker on arq:bulk (+ reconciler cron)
#   ondemand → persistent ARQ worker on arq:ondemand
# Defaulting to scrape preserves the existing cron service behavior.
CMD ["sh", "-c", "case \"$DISCOVERY_MODE\" in \
  bulk) exec arq worker.BulkWorkerSettings ;; \
  ondemand) exec arq worker.OnDemandWorkerSettings ;; \
  *) exec python __main__.py ;; \
esac"]
