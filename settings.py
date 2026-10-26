"""Environment variable loading for the discovery service."""

import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Required env var {key!r} is not set")
    return val


DATABASE_URL: str = _require("DATABASE_URL")
TELLORA_APOLLO_API_KEY: str = _require("TELLORA_APOLLO_API_KEY")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

# Embeddings — must match the backend's EMBEDDING_PROVIDER / OPENAI_EMBEDDING_MODEL:
# the backend embeds queries, this worker embeds companies, both into description_embedding.
# Changing either re-embeds every company (reembed_companies_task, queued at startup).
# Only "openai" is supported.
EMBEDDING_PROVIDER: str = os.getenv("EMBEDDING_PROVIDER", "openai").strip().lower()
OPENAI_EMBEDDING_MODEL: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large").strip()
EMBEDDING_DIM = 768  # vector(768) columns

# HQ backfill LLM: optional primary model in front of the MODEL_P* chain
HQ_NORMALIZE_MODEL: str = os.getenv("HQ_NORMALIZE_MODEL", "").strip()

# LiteLLM gateway — the MODEL_P* fallback chain lives in llm.py. Bare model ids are
# treated as openai/*.
E2E_JUDGE_MODEL: str = os.getenv("E2E_JUDGE_MODEL", "openai/gpt-6.1-sol")
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
# Per-model retries inside LiteLLM before advancing to the next fallback model.
LITELLM_NUM_RETRIES: int = int(os.getenv("LITELLM_NUM_RETRIES", "2"))

# Redis keys — backend ARQ worker listens for enriched company domains / instant alerts
SIGNALS_READY_KEY: str = "tellora:signals_ready"
SIGNALS_ALERT_KEY: str = "tellora:signals_alert"

# Staleness refresh caps (ARQ worker crons — daily)
REFRESH_BATCH_CAP: int = int(os.getenv("REFRESH_BATCH_CAP", "4500"))
REFRESH_STALE_DAYS: int = int(os.getenv("REFRESH_STALE_DAYS", "30"))
REFRESH_ICP_STALE_DAYS: int = int(os.getenv("REFRESH_ICP_STALE_DAYS", "14"))
REFRESH_ICP_CAP: int = int(os.getenv("REFRESH_ICP_CAP", "500"))
WATCHED_STALE_HOURS: int = int(os.getenv("WATCHED_STALE_HOURS", "20"))
WATCHED_ORG_DAILY_BUDGET: int = int(os.getenv("WATCHED_ORG_DAILY_BUDGET", "200"))
WATCHED_REFRESH_GLOBAL_CAP: int = int(os.getenv("WATCHED_REFRESH_GLOBAL_CAP", "2000"))
WATCHED_REFRESH_CONCURRENCY: int = int(os.getenv("WATCHED_REFRESH_CONCURRENCY", "5"))
JOB_POLL_ATS_CAP: int = int(os.getenv("JOB_POLL_ATS_CAP", "500"))
JOB_POLL_FULL_ENRICH_CAP: int = int(os.getenv("JOB_POLL_FULL_ENRICH_CAP", "100"))

# Apollo free people-count → headcount proxy (total_entries × factor).
APOLLO_HEADCOUNT_FACTOR: float = float(os.getenv("APOLLO_HEADCOUNT_FACTOR", "1.0") or "1.0")

# Scrape-field backfill: max rows per batch (linkedin_url, raw_meta, etc.).
SCRAPE_FIELDS_BACKFILL_LIMIT: int = int(os.getenv("SCRAPE_FIELDS_BACKFILL_LIMIT", "500"))

# Weekly backfill: max Apollo rows to headcount-fill per cron run (rate-limited).
HEADCOUNT_BACKFILL_LIMIT: int = int(os.getenv("HEADCOUNT_BACKFILL_LIMIT", "1000"))
# Daily refresh: re-fetch Apollo headcount for rows with an existing estimate gone stale.
HEADCOUNT_REFRESH_STALE_DAYS: int = int(os.getenv("HEADCOUNT_REFRESH_STALE_DAYS", "30"))
HEADCOUNT_REFRESH_LIMIT: int = int(os.getenv("HEADCOUNT_REFRESH_LIMIT", "500"))

# Serper.dev — primary web search. Get a key at https://serper.dev
# Falls back to DDG if not set.
SERPER_API_KEY: str = os.getenv("SERPER_API_KEY", "")

# Search-grounded profile backfill (funding/HQ only — never alertable events).
PROFILE_BACKFILL_ENABLED: bool = os.getenv("PROFILE_BACKFILL_ENABLED", "true").lower() in (
    "1", "true", "yes",
)

# When true, run site: ATS searches via Serper on job-board miss (1 query per ATS max).
ATS_SERP_FALLBACK: bool = os.getenv("ATS_SERP_FALLBACK", "false").lower() in ("1", "true", "yes")

# Retry blocked HTML fetches (JazzHR, iCIMS, tech-stack homepage) via httpcloak on 403/429.
HTTPCLOAK_FALLBACK: bool = os.getenv("HTTPCLOAK_FALLBACK", "false").lower() in ("1", "true", "yes")

# GitHub REST API — optional token raises rate limit 60/hr → 5k/hr.
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

# SEC EDGAR fair-access policy requires an identifying User-Agent.
EDGAR_USER_AGENT: str = os.getenv("EDGAR_USER_AGENT", "Tellora Research research@tellora.ai")

# Max new discovery_company rows auto-created per daily EDGAR poll.
EDGAR_AUTO_CREATE_CAP: int = int(os.getenv("EDGAR_AUTO_CREATE_CAP", "25"))

# WARN Act layoff notices (warn-scraper). States scraped by the weekly cron.
WARN_STATES: list[str] = [
    s.strip().lower()
    for s in os.getenv("WARN_STATES", "ca,tx,ny,wa,il,nj,co,ga,va,or,az,pa,oh,mi,md,fl").split(",")
    if s.strip()
]
WARN_LOOKBACK_DAYS: int = int(os.getenv("WARN_LOOKBACK_DAYS", "21"))

# Wappalyzer-format technology fingerprints (GPL-3.0 data, fetched at runtime and
# cached on disk — not vendored). HTTP Archive maintains the most active fork.
TECH_FINGERPRINTS_URL: str = os.getenv(
    "TECH_FINGERPRINTS_URL",
    "https://raw.githubusercontent.com/HTTPArchive/wappalyzer/main/src",
)
TECH_FINGERPRINTS_CACHE: str = os.getenv("TECH_FINGERPRINTS_CACHE", "/tmp/tellora-tech-fingerprints.json")
TECH_FINGERPRINTS_MAX_AGE_H: int = int(os.getenv("TECH_FINGERPRINTS_MAX_AGE_H", "168"))

# News / first-party feed lookback for events (older items are context, not news).
NEWS_EVENT_MAX_AGE_DAYS: int = int(os.getenv("NEWS_EVENT_MAX_AGE_DAYS", "45"))

# Max new discovery_company rows auto-created per daily Product Hunt poll.
PH_AUTO_CREATE_CAP: int = int(os.getenv("PH_AUTO_CREATE_CAP", "10"))

# ICP-targeted index fill (scrape/icp_fill.py): each new company costs one
# Serper search + one LLM call + an embedding, then signal enrichment.
ICP_FILL_DAILY_CAP: int = int(os.getenv("ICP_FILL_DAILY_CAP", "1000"))
ICP_FILL_MAX_PAGES: int = int(os.getenv("ICP_FILL_MAX_PAGES", "15"))
ICP_FILL_LOOKUP_CONCURRENCY: int = int(os.getenv("ICP_FILL_LOOKUP_CONCURRENCY", "5"))

# Redis — used to notify the backend worker after company signals are enriched
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379")

# Jina Reader API — homepage/careers page text extraction. Get a key at https://jina.ai/reader
# Falls back to unauthenticated requests (20 RPM) if not set.
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")

SIGNAL_ENRICH_MAX_JOBS: int = int(os.getenv("SIGNAL_ENRICH_MAX_JOBS", "8"))
# Jobhive slug import during scheduled maintenance (0 = no limit on scan).
JOBHIVE_IMPORT_LIMIT: int = int(os.getenv("JOBHIVE_IMPORT_LIMIT", "500"))
# Look up jobhive slugs before live ATS discovery during signal enrichment.
JOBHIVE_ENRICH_LOOKUP: bool = os.getenv("JOBHIVE_ENRICH_LOOKUP", "true").lower() in ("1", "true", "yes")
# Optional local CSV directory (greenhouse.csv, …) instead of GitHub download.
JOBHIVE_LOCAL_DIR: str = os.getenv("JOBHIVE_LOCAL_DIR", "").strip()

# Observability (same tokens/dataset as tellora-backend)
ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
AXIOM_TOKEN: str = os.getenv("AXIOM_TOKEN", "")
AXIOM_ORG_ID: str = os.getenv("AXIOM_ORG_ID", "")
AXIOM_DATASET: str = os.getenv("AXIOM_DATASET") or (
    "tellora" if ENVIRONMENT == "production" else "tellora-dev"
)
SENTRY_DSN: str = os.getenv("SENTRY_DSN", "")

# Admin HTTP API — Bearer token for maintenance job endpoints (api/admin.py).
DISCOVERY_ADMIN_SECRET: str = os.getenv("DISCOVERY_ADMIN_SECRET", "")

# Signal enrichment worker / reconcile
SIGNAL_ENRICH_MAX_TRIES: int = int(os.getenv("SIGNAL_ENRICH_MAX_TRIES", "3"))
SIGNAL_RECONCILE_MAX_ATTEMPTS: int = int(os.getenv("SIGNAL_RECONCILE_MAX_ATTEMPTS", "3"))
SIGNAL_RECONCILE_BATCH: int = int(os.getenv("SIGNAL_RECONCILE_BATCH", "50"))
SIGNAL_ENRICH_TIMEOUT_S: int = int(os.getenv("SIGNAL_ENRICH_TIMEOUT_S", "480"))
SIGNAL_PROCESSING_STALE_MINUTES: int = int(os.getenv("SIGNAL_PROCESSING_STALE_MINUTES", "15"))
DOMAIN_CACHE_MAXSIZE: int = int(os.getenv("DOMAIN_CACHE_MAXSIZE", "5000"))
DOMAIN_CACHE_TTL_S: int = int(os.getenv("DOMAIN_CACHE_TTL_S", "3600"))
