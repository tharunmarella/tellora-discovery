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
GEMINI_API_KEY: str = os.getenv("GOOGLE_API_KEY", os.getenv("GEMINI_API_KEY", ""))
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")

# HQ backfill LLM: "gemini" (default) or "groq"
HQ_NORMALIZE_PROVIDER: str = os.getenv("HQ_NORMALIZE_PROVIDER", "gemini").strip().lower()
HQ_NORMALIZE_MODEL: str = os.getenv("HQ_NORMALIZE_MODEL", "").strip()
HQ_GROQ_MODEL: str = os.getenv("HQ_GROQ_MODEL", "openai/gpt-oss-20b").strip()

# Gemini model IDs (intentionally different per pipeline stage)
ENRICHMENT_GEMINI_MODEL: str = os.getenv("ENRICHMENT_GEMINI_MODEL", "gemini-2.5-flash-lite")
ENRICHMENT_GEMINI_FALLBACK_MODEL: str = os.getenv("ENRICHMENT_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
SIGNAL_GEMINI_MODEL: str = os.getenv("SIGNAL_GEMINI_MODEL", "gemini-3.1-flash-lite")
E2E_JUDGE_MODEL: str = os.getenv("E2E_JUDGE_MODEL", "gemini-3.5-flash")

# LiteLLM gateway — multi-model fallback chains (Gemini primary → Gemini fallback →
# Anthropic when ANTHROPIC_API_KEY is set). Bare model ids are treated as gemini/*.
SIGNAL_GEMINI_FALLBACK_MODEL: str = os.getenv("SIGNAL_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
LLM_CROSS_PROVIDER_MODEL: str = os.getenv("LLM_CROSS_PROVIDER_MODEL", "anthropic/claude-haiku-4-5")
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
# Per-model retries inside LiteLLM before advancing to the next fallback model.
LITELLM_NUM_RETRIES: int = int(os.getenv("LITELLM_NUM_RETRIES", "2"))

# Redis keys — backend ARQ worker listens for enriched company domains / instant alerts
SIGNALS_READY_KEY: str = "tellora:signals_ready"
SIGNALS_ALERT_KEY: str = "tellora:signals_alert"

# Staleness refresh caps (ARQ worker crons — daily)
REFRESH_BATCH_CAP: int = int(os.getenv("REFRESH_BATCH_CAP", "2000"))
REFRESH_STALE_DAYS: int = int(os.getenv("REFRESH_STALE_DAYS", "30"))
REFRESH_ICP_STALE_DAYS: int = int(os.getenv("REFRESH_ICP_STALE_DAYS", "14"))
REFRESH_ICP_CAP: int = int(os.getenv("REFRESH_ICP_CAP", "500"))
WATCHED_STALE_DAYS: int = int(os.getenv("WATCHED_STALE_DAYS", "6"))
WATCHED_REFRESH_LIMIT: int = int(os.getenv("WATCHED_REFRESH_LIMIT", "300"))
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

# Max new discovery_company rows auto-created per daily Product Hunt poll.
PH_AUTO_CREATE_CAP: int = int(os.getenv("PH_AUTO_CREATE_CAP", "10"))

# Redis — used to notify the backend worker after company signals are enriched
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379")

# Jina Reader API — homepage/careers page text extraction. Get a key at https://jina.ai/reader
# Falls back to unauthenticated requests (20 RPM) if not set.
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")

# Max pages to scrape per profile per run (100 results/page → 50,000 max).
# Override in env for testing: MAX_PAGES_PER_PROFILE=5
MAX_PAGES_PER_PROFILE: int = int(os.getenv("MAX_PAGES_PER_PROFILE", "500"))

# Discovery scrape schedule — hard-coded fallback matches Railway cron (Sun + Wed 3 AM UTC).
DISCOVERY_SCRAPE_CRON: str = os.getenv("DISCOVERY_SCRAPE_CRON", "0 3 * * 0,3")
# Set to 1 to skip the in-process schedule guard (local dev / manual runs anytime).
DISCOVERY_SCRAPE_SCHEDULE_DISABLED: bool = os.getenv("DISCOVERY_SCRAPE_SCHEDULE_DISABLED", "").strip() == "1"
# Worker ARQ cron mirrors DISCOVERY_SCRAPE_CRON when Railway cron service is absent.
DISCOVERY_SCRAPE_WORKER_FALLBACK: bool = os.getenv("DISCOVERY_SCRAPE_WORKER_FALLBACK", "1").strip() == "1"
# Hours to treat an in-progress scrape as active (worker fallback dedup).
SCRAPE_ACTIVE_HOURS: int = int(os.getenv("SCRAPE_ACTIVE_HOURS", "6"))
# When false, weekly cron enqueues pending rows instead of inline signals.runner.run().
DISCOVERY_INLINE_ENRICH: bool = os.getenv("DISCOVERY_INLINE_ENRICH", "true").lower() in ("1", "true", "yes")
ENQUEUE_SCRAPE_PENDING_LIMIT: int = int(os.getenv("ENQUEUE_SCRAPE_PENDING_LIMIT", "5000"))
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
