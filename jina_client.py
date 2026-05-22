"""
Jina Search client for domain resolution.

Searches Jina for '"CompanyName" official website', skips aggregator domains,
and returns the company's domain, website URL, and a short description.
"""

import asyncio
import logging
from urllib.parse import urlparse

import httpx

import settings as cfg

logger = logging.getLogger("discovery.jina")

JINA_SEARCH_URL = "https://s.jina.ai/"

# Domains that are NOT the company's own website
_SKIP_DOMAINS = {
    "linkedin.com", "crunchbase.com", "zoominfo.com", "bloomberg.com",
    "wikipedia.org", "facebook.com", "twitter.com", "x.com",
    "glassdoor.com", "indeed.com", "ycombinator.com", "techcrunch.com",
    "forbes.com", "pitchbook.com", "owler.com", "dnb.com",
    "rocketreach.co", "apollo.io", "clearbit.com",
}


async def lookup_domain(company_name: str) -> dict[str, str]:
    """
    Returns a dict with any of: domain, website_url, description.
    Empty dict if Jina finds nothing useful.
    """
    query = f'"{company_name}" official website'
    headers: dict[str, str] = {"Accept": "application/json"}
    if cfg.JINA_API_KEY:
        headers["Authorization"] = f"Bearer {cfg.JINA_API_KEY}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(f"{JINA_SEARCH_URL}{query}", headers=headers)

        if resp.status_code == 401:
            logger.error("Jina API key required — set JINA_API_KEY in .env (free at jina.ai)")
            return {}
        if resp.status_code == 429:
            logger.warning(f"Jina 429 for '{company_name}' — pausing 30s")
            await asyncio.sleep(30)
            return {}

        resp.raise_for_status()
        hits = resp.json().get("data", [])

    except Exception as exc:
        logger.warning(f"Jina lookup failed for '{company_name}': {exc}")
        return {}

    for hit in hits[:4]:
        url: str = hit.get("url", "")
        if not url:
            continue

        try:
            host = urlparse(url).netloc.lower().lstrip("www.")
        except Exception:
            continue

        if any(host == d or host.endswith(f".{d}") for d in _SKIP_DOMAINS):
            continue

        desc = (hit.get("description") or hit.get("content") or "").strip()[:300]
        result: dict[str, str] = {"domain": host, "website_url": url}
        if desc:
            result["description"] = desc
        return result

    return {}
