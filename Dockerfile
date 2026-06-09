FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install dependencies first (layer-cached separately from code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# One-off full headcount backfill — revert to ["python", "__main__.py"] after it completes.
# The always-on worker service overrides this with a Custom Start Command:
#   arq worker.WorkerSettings
CMD ["python", "__main__.py", "--headcount-backfill-only"]
