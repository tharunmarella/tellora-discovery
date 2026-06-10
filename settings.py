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

# Serper.dev — primary web search. Get a key at https://serper.dev
# Falls back to DDG if not set.
SERPER_API_KEY: str = os.getenv("SERPER_API_KEY", "")

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
