# tellora-discovery

Standalone Railway cron service that builds Tellora's pre-populated company database.

## What it does

Queries Apollo's **free** People API Search (`mixed_people/api_search`) across 5 ICP profiles, resolves company domains via Jina, and stores results in the shared `discovery_company` PostgreSQL table. Zero Apollo credits consumed.

**5 profiles:** AI Infra / DevTools · Operations · Healthcare · Financial Services · GTM

**CEO filter trick:** All profiles search for `person_titles=CEO` — every company has one CEO, so each API result is a unique company. Natural dedup, no client-side grouping needed.

## Crash recovery

After every committed page, the service writes a checkpoint to `discovery_progress`. If the process crashes on page 247 of "healthcare", the next run reads the checkpoint and resumes from page 248 — profiles already completed are skipped entirely.

## Railway setup

1. Add a new service in your Railway project.
2. **Root directory:** `tellora-discovery/`
3. **Start command:** `python -m tellora_discovery`
4. **Cron schedule:** `0 3 * * 0` (every Sunday 3 AM UTC)
5. Set environment variables (see `.env.example`):
   - `DATABASE_URL` — same as the main backend
   - `TELLORA_APOLLO_API_KEY` — Apollo master API key
   - `JINA_API_KEY` — optional but recommended

## Local testing

```bash
cd tellora-discovery
pip install -r requirements.txt
cp .env.example .env  # fill in values
python -m tellora_discovery --dry-run  # 2 pages per profile, quick test
python -m tellora_discovery            # full run
```

## File structure

```
tellora-discovery/
├── __main__.py        # Entry point
├── models.py          # DiscoveryCompany + DiscoveryProgress SQLModel tables
├── profiles.py        # The 5 ICP filter configs
├── apollo_client.py   # Apollo pagination + rate limiter
├── jina_client.py     # Domain lookup via Jina Search
├── service.py         # Orchestration with checkpoint/resume logic
├── database.py        # DB engine + session
├── settings.py        # Env var loading
├── config_logging.py  # Logging setup
└── requirements.txt
```

## Rate limits

- **Apollo:** 200 req/min, 600 req/hour — service paces at ~1 req/sec, pauses if approaching ceiling
- **Jina:** ~2 req/sec with 30s pause on 429

## Expected output

~100,000–200,000 unique companies across all 5 profiles per weekly run (first run). Subsequent runs only add new companies Apollo has indexed since the last run.
