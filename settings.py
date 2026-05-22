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
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")
SERPER_API_KEY: str = os.getenv("SERPER_API_KEY", "")

# Max pages to scrape per profile per run (100 results/page → 50,000 max).
# Override in env for testing: MAX_PAGES_PER_PROFILE=5
MAX_PAGES_PER_PROFILE: int = int(os.getenv("MAX_PAGES_PER_PROFILE", "500"))
