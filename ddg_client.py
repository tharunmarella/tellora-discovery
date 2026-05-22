"""
DuckDuckGo search client for domain resolution.

Replaces Jina Search — no API key required, ~1-2s per lookup vs ~13s for Jina.
Uses the duckduckgo-search package (DDGS) which wraps DDG's HTML API.

Rate limit: DDG is tolerant of moderate traffic but will 202/rate-limit
under heavy bursts. We run 5 concurrent max and add a small delay on error.
"""

import asyncio
import logging
from urllib.parse import urlparse

from duckduckgo_search import DDGS

logger = logging.getLogger("discovery.ddg")

_SKIP_DOMAINS = {
    "linkedin.com", "crunchbase.com", "zoominfo.com", "bloomberg.com",
    "wikipedia.org", "facebook.com", "twitter.com", "x.com",
    "glassdoor.com", "indeed.com", "ycombinator.com", "techcrunch.com",
    "forbes.com", "pitchbook.com", "owler.com", "dnb.com",
    "rocketreach.co", "apollo.io", "clearbit.com", "g2.com",
    "capterra.com", "trustpilot.com", "bloomberg.com",
}


def _extract_domain(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        if any(host == d or host.endswith(f".{d}") for d in _SKIP_DOMAINS):
            return None
        return host if host else None
    except Exception:
        return None


async def lookup_domain(company_name: str, ceo_first_name: str = "") -> dict[str, str]:
    """
    Returns a dict with any of: domain, website_url, description.
    Empty dict if DDG finds nothing useful.

    Runs DDGS in a thread pool so it doesn't block the event loop.
    """
    query = f'"{company_name}" official website'

    try:
        results = await asyncio.get_event_loop().run_in_executor(
            None, _ddg_search, query
        )
    except Exception as exc:
        logger.warning(f"DDG lookup failed for '{company_name}': {exc}")
        await asyncio.sleep(2)  # brief pause on error
        return {}

    # Build name tokens to validate domain relevance
    name_tokens = set(
        w.lower() for w in company_name.replace("-", " ").split()
        if len(w) > 2 and w.lower() not in {"the", "inc", "llc", "ltd", "corp", "and", "for"}
    )

    for r in results[:5]:
        url    = r.get("href") or r.get("url") or ""
        domain = _extract_domain(url)
        if not domain:
            continue

        # Require at least one name token to appear in the domain
        # e.g. "arize.com" ✓ for "Arize AI", "mail.google.com" ✗ for "Codvo.ai"
        domain_lower = domain.replace("-", "").replace(".", "")
        if not any(tok in domain_lower for tok in name_tokens):
            continue

        desc = (r.get("body") or r.get("description") or "").strip()[:300]
        return {"domain": domain, "website_url": url, "description": desc}

    return {}


def _ddg_search(query: str) -> list[dict]:
    """Synchronous DDG text search — called via run_in_executor."""
    with DDGS() as ddgs:
        return list(ddgs.text(query, max_results=5))
