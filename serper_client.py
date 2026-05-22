"""
Serper.dev Google Search client for domain resolution.

POST https://google.serper.dev/search with X-API-KEY header.
~200ms per call, Google-quality results. 2,500 free queries.
"""

import asyncio
import logging
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("discovery.serper")

SERPER_URL = "https://google.serper.dev/search"

_SKIP_DOMAINS = {
    "linkedin.com", "crunchbase.com", "zoominfo.com", "bloomberg.com",
    "wikipedia.org", "facebook.com", "twitter.com", "x.com",
    "glassdoor.com", "indeed.com", "ycombinator.com", "techcrunch.com",
    "forbes.com", "pitchbook.com", "owler.com", "dnb.com",
    "rocketreach.co", "apollo.io", "clearbit.com", "g2.com",
    "capterra.com", "trustpilot.com", "google.com", "bing.com",
    "yahoo.com", "reddit.com", "quora.com", "medium.com",
}


def _extract_domain(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        if any(host == d or host.endswith(f".{d}") for d in _SKIP_DOMAINS):
            return None
        return host if host else None
    except Exception:
        return None


async def lookup_domain(company_name: str, ceo_first_name: str = "", api_key: str = "") -> dict[str, str]:
    """
    Returns a dict with any of: domain, website_url, description.
    Empty dict if nothing useful found.
    """
    if not api_key:
        logger.error("Serper API key required — set SERPER_API_KEY in .env")
        return {}

    query = f'"{company_name}" official website'

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                SERPER_URL,
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 5, "gl": "us"},
            )

        if resp.status_code == 429:
            logger.warning("Serper 429 — pausing 5s")
            await asyncio.sleep(5)
            return {}

        if resp.status_code == 401:
            logger.error("Serper API key invalid")
            return {}

        resp.raise_for_status()
        results = resp.json().get("organic", [])

    except Exception as exc:
        logger.warning(f"Serper lookup failed for '{company_name}': {exc}")
        return {}

    for r in results[:5]:
        url = r.get("link", "")
        domain = _extract_domain(url)
        if not domain:
            continue
        desc = (r.get("snippet") or "").strip()[:300]
        return {"domain": domain, "website_url": url, "description": desc}

    return {}
