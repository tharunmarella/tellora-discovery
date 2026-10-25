# tellora-discovery

Standalone service for **on-demand company signal enrichment** (Watch / Enrich in the app). Company Search itself is an ICP-live feed in `tellora-backend` (LLM queries + web search). There is **no weekly Apollo census scrape**.

## What it does

The ARQ worker (`arq worker.WorkerSettings`, queue `arq:ondemand`) claims `discovery_company` rows and enriches them with Serper, Jina, ATS boards, and Gemini. The backend enqueues `enrich_company_task` when a rep Watches or Enrichs a company.

Helpers still in `scrape/` are **not** a census pipeline: domain lookup and Apollo people-count used during enrichment / headcount refresh.

### Signal enrichment (per company)

1. **Pages** — homepage, /about, /careers, /pricing, /customers, /changelog: direct fetch + trafilatura (Jina Reader only for JS-only homepages)
2. **Tech stack** — Wappalyzer-format fingerprints (HTTP Archive fork, ~4k technologies; GPL-3.0 data downloaded at runtime, not vendored) over headers, cookies, meta, scripts and DNS
3. **Job boards** — Greenhouse, Lever, Ashby, SmartRecruiters, Workable, iCIMS, JazzHR, Rippling
4. **First-party feed** — the company's own changelog / blog / newsroom RSS → launches, funding, exec, expansion
5. **News** — Google News RSS, verified per headline against the company's domain + description (drops same-name companies, places, people)
6. **Free sources** — GitHub/npm, HN, USAspending (matched by UEI once known), DNS
7. **Gemini synthesis** → `buying_signals`, `signal_score`, embeddings
8. **Signal diff** → typed events; adopt/drop tech only between full fingerprint runs; repeat reports of one event merge into it as extra sources

### Batch sources (cron)

| Source | Library | Events |
|--------|---------|--------|
| SEC Form D (every 6h) | edgartools (daily form index — every filing, not one search page) | `funding_round`, auto-created startups |
| SEC 8-K (every 6h) | edgartools; Item 5.02 / 2.01 | `exec_hire`, `exec_departure`, `acquisition` |
| WARN notices (weekly) | warn-scraper (Big Local News) | `layoffs` |
| Product Hunt (daily) | RSS | `product_launch` |

All batch matching goes through `signals/entity.py`: legal-suffix cleaning (cleanco), exact / space-free / descriptor-suffix keys, no edit-distance fuzzing, and single common-word names (wordfreq) only match when the source's state equals the company's HQ state.

Statuses: `pending` · `processing` · `enriched` · `partial` · `failed` · `skipped`

## Crash recovery

Signal enrichment uses a reconcile cron (~10 min) to re-queue stuck `processing` rows and retryable `failed`/`partial` companies.

## Scheduled jobs (freshness)

| Job | Schedule (UTC) | What it refreshes |
|-----|----------------|-------------------|
| **Watched re-enrich** (worker) | Daily 4:00 (via maintenance) | Active research accounts |
| **Index re-enrich** (worker) | Daily (via maintenance) | Stale index rows |
| **Job posts / headcount** | Daily (via maintenance) | ATS hiring + Apollo people-count |
| **EDGAR Form D / 8-K** | Every 6h | Funding, exec changes, acquisitions |
| **WARN notices** | Mon 3:10 | Layoffs (`WARN_STATES`, `WARN_LOOKBACK_DAYS`) |
| **Product Hunt** | Periodic | Launch discovery |
| **Reconcile** | Every 10 min | Stuck/failed enrichment retries |
| **Jobhive import** | Sun 2:30 | Warm `ats_board` slugs |

Tune via env: `REFRESH_STALE_DAYS`, `REFRESH_BATCH_CAP`, `WATCHED_*`, `HEADCOUNT_REFRESH_*`. See [`docs/DISCOVERY_SCHEDULERS.md`](../docs/DISCOVERY_SCHEDULERS.md).

**One-time index backfill** (scrape metadata → signal re-enrich):

```bash
./scripts/run_index_reenrich.sh             # scrape backfill + full re-enrich
./scripts/run_index_reenrich.sh --limit 50  # sample both steps
python scripts/backfill_scrape_fields.py --dry-run  # count eligible rows only
```

## Railway setup

1. Add a service in your Railway project.
2. **Root directory:** `tellora-discovery/`
3. **Start command:** `arq worker.WorkerSettings`
4. Set environment variables (see `.env.example`)

Do **not** run a second cron service with `python __main__.py` — that entrypoint is a no-op leftover. Delete the old Railway census cron if it still exists.

**Observability:** set `SENTRY_DSN` and `ENVIRONMENT=production` on the worker. Without `SENTRY_DSN`, Sentry is disabled.

## Local testing

```bash
cd tellora-discovery
pip install -r requirements.txt
cp .env.example .env
arq worker.WorkerSettings
python -m signals.runner --limit 10
python scripts/test_batch.py
python scripts/test_search.py
python scripts/backfill_hq_normalize.py --dry-run
```

## Testing

```bash
cd tellora-discovery
pip install -r requirements-dev.txt

# Fast: pure unit + HTTP-mocked parsing (no Docker, no API keys)
pytest

# Task integration: real Postgres via testcontainers (Docker required)
pytest -m integration

# Live smoke: hits Serper/Gemini/Jina (needs API keys in .env)
pytest -m live

# Coverage gate (same as pre-push step 1): non-live suite, 75% on scoped modules
pytest -m "not live and not e2e" --cov --cov-config=.coveragerc --cov-fail-under=75

# E2E pipeline quality gate (pre-push step 2; needs SERPER + GOOGLE keys in .env)
pytest -m e2e -v
```

**Pre-push hook** (requires Docker + API keys in `.env`):

```bash
./scripts/install-git-hooks.sh   # once per clone
```

Blocks `git push` if:
1. Non-live tests fail or scoped coverage drops below 75%
2. E2E pipeline quality judge scores below threshold (per-company floor 7, mean >= 8)

Judge model: `E2E_JUDGE_MODEL` (default `gemini-3.5-flash`) runs with **Google Search grounding**.
CI does **not** run e2e (no secrets in GitHub Actions).

Omitted from the coverage gate (live-only): `signals/pipeline.py`, `signals/runner.py`, `signals/sources/*`, etc. — see `.coveragerc`.

Test layout:

```
tests/
├── unit/           # diff, name_match, pipeline helpers, llm, source mappers
├── parsing/        # respx-mocked HTTP fetchers + Gemini synthesis
├── integration/    # worker tasks, DB persistence (testcontainers)
├── live/           # opt-in external API smoke tests
└── e2e/            # LLM-judged pipeline quality gate (pre-push)
```

## Project structure

```
tellora-discovery/
├── __main__.py              # No-op (old census cron); use arq worker
├── tests/                   # pytest suite (unit / parsing / integration / live)
├── worker.py                # ARQ on-demand worker (arq:ondemand)
├── settings.py              # Env vars
├── database.py              # DB engine + migrations
├── models.py                # SQLModel tables
├── llm.py                   # Shared Gemini client
├── config_logging.py
│
├── scrape/                  # Enrichment helpers (not a census scrape)
│   ├── apollo_client.py     # Apollo people-count / pagination
│   ├── domain_lookup.py     # Serper + Gemini domain resolve
│   └── scrape_fields_backfill.py
│
├── signals/                 # Buying-signal enrichment
│   ├── pipeline.py          # Per-company orchestration (Jina, Gemini, …)
│   ├── runner.py            # Batch backfill CLI + persist helper
│   ├── headcount_backfill.py
│   ├── diff.py              # Snapshot diff → typed events
│   ├── edges.py             # discovery_edge graph
│   ├── job_posts.py         # ATS fetch + concept extraction
│   ├── monitoring.py        # Cron tasks + reconcile_pending
│   └── sources/
│
├── infra/                   # Observability
└── scripts/                 # One-off POCs and backfills
```

## Rate limits

- **Apollo:** 200 req/min, 600 req/hour — paces at ~1 req/sec (headcount / people lookup)
- **Serper:** 50 req/sec
- **Jina (unauth):** ~20 RPM — use `JINA_API_KEY` in production
- **httpcloak (optional):** set `HTTPCLOAK_FALLBACK=true` + `pip install httpcloak` for JazzHR/iCIMS/tech-stack WAF bypass
- **Signal worker:** `max_jobs` from `SIGNAL_ENRICH_MAX_JOBS`, `job_timeout=600s`, `max_tries=3`
