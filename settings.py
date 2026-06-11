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
SIGNAL_GEMINI_MODEL: str = os.getenv("SIGNAL_GEMINI_MODEL", "gemini-3.1-flash-lite")

# Redis keys — backend ARQ worker listens for enriched company domains / instant alerts
SIGNALS_READY_KEY: str = "tellora:signals_ready"
SIGNALS_ALERT_KEY: str = "tellora:signals_alert"

# Staleness refresh cap per weekly sweep
REFRESH_BATCH_CAP: int = int(os.getenv("REFRESH_BATCH_CAP", "500"))

# Serper.dev — primary web search. Get a key at https://serper.dev
# Falls back to DDG if not set.
SERPER_API_KEY: str = os.getenv("SERPER_API_KEY", "")

# GitHub REST API — optional token raises rate limit 60/hr → 5k/hr.
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

# SEC EDGAR fair-access policy requires an identifying User-Agent.
EDGAR_USER_AGENT: str = os.getenv("EDGAR_USER_AGENT", "Tellora Research research@tellora.ai")

# Max new discovery_company rows auto-created per daily EDGAR poll.
EDGAR_AUTO_CREATE_CAP: int = int(os.getenv("EDGAR_AUTO_CREATE_CAP", "25"))

# Redis — used to notify the backend worker after company signals are enriched
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379")

# Jina Reader API — used for fetching company website content (homepage, about, careers)
# and funding news search. Get a free key with 10M tokens at https://jina.ai/reader
# Falls back to unauthenticated requests (20 RPM) if not set.
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")

# Max pages to scrape per profile per run (100 results/page → 50,000 max).
# Override in env for testing: MAX_PAGES_PER_PROFILE=5
MAX_PAGES_PER_PROFILE: int = int(os.getenv("MAX_PAGES_PER_PROFILE", "500"))

# Apollo free people-count → headcount proxy (total_entries × factor).
APOLLO_HEADCOUNT_FACTOR: float = float(os.getenv("APOLLO_HEADCOUNT_FACTOR", "1.0") or "1.0")

# Weekly backfill: max Apollo rows to headcount-fill per cron run (rate-limited).
HEADCOUNT_BACKFILL_LIMIT: int = int(os.getenv("HEADCOUNT_BACKFILL_LIMIT", "1000"))
