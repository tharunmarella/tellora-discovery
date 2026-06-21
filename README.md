# tellora-discovery

Standalone Railway service that builds Tellora's pre-populated company database and serves on-demand signal enrichment.

## What it does

Queries Apollo's **free** People API Search (`mixed_people/api_search`) across 17 ICP profiles, enriches each company with Serper + Gemini, and stores results in the shared `discovery_company` PostgreSQL table. Zero Apollo credits consumed.

**17 profiles:** DevTools · Logistics · Healthcare · Financial Services · Sales & Marketing · Cybersecurity · Construction · HR Tech · Real Estate · EdTech · Legal · Manufacturing · Retail · GovTech · Hospitality · Media & Advertising · Nonprofit

**CEO filter trick:** All profiles search for `person_titles=CEO` — every company has one CEO, so each result is a unique company. Natural dedup, no client-side grouping needed.

### Two disjoint enrichment paths

1. **Weekly cron** (`python __main__.py`) — scrapes Apollo, enriches new companies inline, runs headcount backfill cap.
2. **On-demand worker** (`arq worker.WorkerSettings`, queue `arq:ondemand`) — user-triggered enrichment from the app (single "Enrich" + bulk actions). Backend enqueues `enrich_company_task`.

## Enrichment pipeline (per company)

### Scrape-time (Apollo ingest)

1. **Serper search** `"{company} CEO: {ceo_first_name}"` → Google SERP results
2. **Gemini** reads knowledgeGraph + organic results → domain, description, industries, CEO, HQ, funding
3. **logo_url** from domain via Google Favicon API

### Signal-time (buying signals)

1. **Jina Reader** — homepage, /about, /careers, /pricing, /customers, /changelog
2. **Job boards** — Greenhouse, Lever, Ashby, SmartRecruiters, Workable
3. **Free sources** — GitHub, EDGAR Form D, Google News RSS, HN, USAspending, DNS, Wayback
4. **Gemini synthesis** → `buying_signals`, `signal_score`, embeddings
5. **Signal diff** → typed events (`funding_round`, `hiring_surge`, `product_launch`, …)

Statuses: `pending` · `processing` · `enriched` · `partial` · `failed` · `skipped`

## Crash recovery

After every committed page the service writes a checkpoint to `discovery_progress`. Signal enrichment uses a reconcile cron (~10 min) to re-queue stuck `processing` rows and retryable `failed`/`partial` companies.

## Railway setup

1. Add a new service in your Railway project.
2. **Root directory:** `tellora-discovery/`
3. **Start command:** `python __main__.py`
4. **Cron schedule:** `0 3 * * 0` (every Sunday 3 AM UTC)
5. Set environment variables (see `.env.example`)

For the always-on on-demand worker, deploy a second service with start command `arq worker.WorkerSettings`.

## Local testing

```bash
cd tellora-discovery
pip install -r requirements.txt
cp .env.example .env
python __main__.py --dry-run
python __main__.py
python -m signals.runner --limit 10
python scripts/test_batch.py
python scripts/test_search.py
python scripts/backfill_hq_normalize.py --dry-run
```

## Project structure

```
tellora-discovery/
├── __main__.py              # Weekly cron entry (scrape + inline enrich)
├── worker.py                # ARQ on-demand worker (arq:ondemand)
├── settings.py              # Env vars
├── database.py              # DB engine + migrations
├── models.py                # SQLModel tables
├── llm.py                   # Shared Gemini client
├── config_logging.py
│
├── scrape/                  # Apollo ingest + scrape-time domain lookup
│   ├── service.py           # Checkpointed Apollo scrape orchestration
│   ├── apollo_client.py     # Pagination + rate limiter
│   ├── profiles.py          # 17 ICP filter configs
│   ├── domain_lookup.py     # Serper + Gemini at scrape time
│   └── headcount_backfill.py
│
├── signals/                 # Buying-signal enrichment
│   ├── pipeline.py          # Per-company orchestration (Jina, Gemini, …)
│   ├── runner.py            # Batch backfill CLI + persist helper
│   ├── diff.py              # Snapshot diff → typed events
│   ├── edges.py             # discovery_edge graph
│   ├── job_posts.py         # ATS fetch + concept extraction
│   ├── monitoring.py        # Cron tasks + reconcile_pending
│   ├── name_match.py        # Slug variants + relevance guards
│   ├── constants.py         # Shared API URLs / timeouts
│   ├── cache.py             # TTL LRU for in-process caches
│   └── sources/
│       ├── edgar.py         # SEC Form D poller
│       ├── github.py
│       ├── gov.py           # USAspending contracts
│       ├── hn.py            # Hacker News
│       └── news.py          # Google News RSS + Product Hunt
│
├── infra/                   # Observability
│   ├── axiom_arq.py
│   ├── axiom_logger.py
│   ├── sentry_init.py
│   └── sentry_telemetry.py
│
└── scripts/                 # One-off POCs and backfills
```

## Rate limits

- **Apollo:** 200 req/min, 600 req/hour — paces at ~1 req/sec
- **Serper:** 50 req/sec
- **Jina (unauth):** ~20 RPM — use `JINA_API_KEY` in production
- **Signal worker:** `max_jobs=5`, `job_timeout=600s`, `max_tries=3`

## Expected output

~12,000 unique companies across all 17 profiles per weekly run (first run). Subsequent runs only add new companies Apollo has indexed since the last run.
