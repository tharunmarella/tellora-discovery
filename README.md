# tellora-discovery

Standalone Railway cron service that builds Tellora's pre-populated company database.

## What it does

Queries Apollo's **free** People API Search (`mixed_people/api_search`) across 17 ICP profiles, enriches each company with Serper + Gemini, and stores results in the shared `discovery_company` PostgreSQL table. Zero Apollo credits consumed.

**17 profiles:** DevTools · Logistics · Healthcare · Financial Services · Sales & Marketing · Cybersecurity · Construction · HR Tech · Real Estate · EdTech · Legal · Manufacturing · Retail · GovTech · Hospitality · Media & Advertising · Nonprofit

**CEO filter trick:** All profiles search for `person_titles=CEO` — every company has one CEO, so each result is a unique company. Natural dedup, no client-side grouping needed.

## Enrichment pipeline (per company)

1. **Serper search** `"{company} CEO: {ceo_first_name}"` → Google SERP results
2. **Gemini** reads knowledgeGraph + organic results → extracts domain, description, industries, CEO name, HQ, founded year, funding, keywords, use_case
3. **logo_url** constructed from domain via Google Favicon API (no extra call)
4. **Embedding** of description + use_case + industry + keywords → 768-dim vector stored in pgvector for semantic search
5. DuckDuckGo fallback if Serper is unavailable

## Crash recovery

After every committed page the service writes a checkpoint to `discovery_progress`. If the process crashes on page 247 of "healthcare", the next run reads the checkpoint and resumes from page 248.

## Railway setup

1. Add a new service in your Railway project.
2. **Root directory:** `tellora-discovery/`
3. **Start command:** `python __main__.py`
4. **Cron schedule:** `0 3 * * 0` (every Sunday 3 AM UTC)
5. Set environment variables (see `.env.example`):
   - `DATABASE_URL` — same as the main backend
   - `TELLORA_APOLLO_API_KEY` — Apollo master API key
   - `GOOGLE_API_KEY` — Gemini extraction + embeddings
   - `SERPER_API_KEY` — primary web search (falls back to DDG without it)

## Local testing

```bash
cd tellora-discovery
pip install -r requirements.txt
cp .env.example .env  # fill in values
python __main__.py --dry-run  # 2 pages per profile, quick test
python __main__.py             # full run
python test_batch.py           # test enrichment on a batch of companies
```

## File structure

```
tellora-discovery/
├── __main__.py        # Entry point + cron schedule
├── models.py          # DiscoveryCompany + DiscoveryProgress SQLModel tables
├── profiles.py        # 17 ICP filter configs
├── apollo_client.py   # Apollo pagination + rate limiter
├── enrichment.py      # Serper search + Gemini extraction + embedding
├── service.py         # Orchestration with checkpoint/resume logic
├── database.py        # DB engine + session
├── settings.py        # Env var loading
├── config_logging.py  # Logging setup
├── test_batch.py      # Manual enrichment quality test
└── requirements.txt
```

## Rate limits

- **Apollo:** 200 req/min, 600 req/hour — paces at ~1 req/sec, pauses near ceiling
- **Serper:** 50 req/sec — no practical limit for this pipeline
- **Gemini:** 10 concurrent enrichments (`LOOKUP_CONCURRENCY=10`)

## Expected output

~12,000 unique companies across all 17 profiles per weekly run (first run). Subsequent runs only add new companies Apollo has indexed since the last run.
