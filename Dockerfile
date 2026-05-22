FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer-cached separately from code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command — runs the full scrape.
# Pass --dry-run to limit to 2 pages per profile for testing.
CMD ["python", "-m", "tellora_discovery"]
