"""
DuckDuckGo (Bing-backed) search client for domain resolution and company enrichment.

One DDG search per company → one Gemini 2.5 Flash-Lite call that returns:
  - domain         : official website domain
  - description    : one-sentence summary of what the company does
  - industry       : broad industry category (e.g. "Sales Technology")
  - keywords       : 3–5 tags describing the company's space
  - use_case       : one sentence describing the type of company that would buy from them

Requires: GOOGLE_API_KEY env var (shared with tellora-backend).
"""

import asyncio
import json
import logging
import time
from urllib.parse import urlparse

logger = logging.getLogger("discovery.ddg")

# Reuse a single Gemini client per process to avoid per-call instantiation overhead
_gemini_client = None


def _get_gemini_client(api_key: str):
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client

_SKIP_DOMAINS = {
    "linkedin.com", "crunchbase.com", "zoominfo.com", "bloomberg.com",
    "wikipedia.org", "facebook.com", "twitter.com", "x.com",
    "glassdoor.com", "indeed.com", "ycombinator.com", "techcrunch.com",
    "forbes.com", "pitchbook.com", "owler.com", "dnb.com",
    "rocketreach.co", "apollo.io", "clearbit.com", "g2.com",
    "capterra.com", "trustpilot.com", "everydev.ai",
    "github.com", "reddit.com", "quora.com", "medium.com",
    "youtube.com", "vimeo.com", "podbean.com", "buzzsprout.com",
    "spotify.com", "apple.com", "businesswire.com", "prnewswire.com",
    "globenewswire.com", "accesswire.com",
}


def _extract_domain(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        if any(host == d or host.endswith(f".{d}") for d in _SKIP_DOMAINS):
            return None
        return host if host else None
    except Exception:
        return None


def _build_prompt(company_name: str, ceo_first_name: str, results: list[dict]) -> str:
    snippets = []
    for i, r in enumerate(results[:5], 1):
        snippets.append(f"{i}. Title: {r.get('title', '')}")
        snippets.append(f"   URL: {r.get('href', '')}")
        snippets.append(f"   Snippet: {(r.get('body') or '')[:200]}")

    ceo_hint = f" (CEO first name: {ceo_first_name})" if ceo_first_name else ""
    return f"""Company: {company_name}{ceo_hint}

Search results:
{chr(10).join(snippets)}

Return a JSON object with exactly these fields (all based only on the snippets above):
- "domain": the company's official website domain (e.g. "example.com"), or null if none of these is their own site
- "description": one sentence (max 150 chars) describing what {company_name} does
- "industry": broad industry category (e.g. "Sales Technology", "Construction Tech", "Healthcare AI"). null if unclear.
- "keywords": array of 3–5 short tags describing the company's space (e.g. ["AI", "field sales", "coaching"]). Empty array if unclear.
- "use_case": one sentence describing what type of company would buy or benefit from {company_name}'s product (e.g. "Home services companies with door-to-door sales teams"). null if unclear.

JSON only, no explanation."""


def _retry_gemini(fn, max_retries: int = 3):
    """Retry a Gemini API call with exponential backoff on rate-limit or transient errors."""
    backoff = 5
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            exc_str = str(exc).lower()
            is_retryable = any(s in exc_str for s in ("429", "rate", "quota", "resource_exhausted", "503", "timeout"))
            if not is_retryable or attempt == max_retries - 1:
                raise
            logger.warning(f"Gemini retryable error (attempt {attempt+1}): {exc} — backing off {backoff}s")
            time.sleep(backoff)
            backoff *= 2


def _embed_text(text: str, api_key: str) -> list[float] | None:
    """
    Embed text using gemini-embedding-001 (768 dims).
    Uses RETRIEVAL_DOCUMENT task type since these are indexed company profiles.
    output_dimensionality must be set explicitly — default is 3072.
    """
    try:
        from google.genai import types

        client = _get_gemini_client(api_key)

        def _do():
            resp = client.models.embed_content(
                model="gemini-embedding-001",
                contents=text,
                config=types.EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768,
                ),
            )
            return resp.embeddings[0].values

        return _retry_gemini(_do)
    except Exception as exc:
        logger.warning(f"Embedding failed: {exc}")
        return None


def _call_gemini(prompt: str, api_key: str) -> dict:
    client = _get_gemini_client(api_key)

    def _do():
        resp = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
        )
        raw = resp.text.strip().strip("`").strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()
        return json.loads(raw)

    return _retry_gemini(_do)


def _run_lookup(company_name: str, ceo_first_name: str, api_key: str) -> dict:
    """
    Single DDG search → Gemini extraction.
    Returns enrichment dict with any of: domain, website_url, description,
    industry, keywords, use_case.
    """
    try:
        from ddgs import DDGS
        from ddgs.exceptions import RatelimitException
    except ImportError:
        from duckduckgo_search import DDGS
        try:
            from duckduckgo_search.exceptions import RatelimitException
        except Exception:
            RatelimitException = Exception  # type: ignore

    query = f"{company_name} company software"

    def _search(client) -> list[dict]:
        backoff = 30
        for attempt in range(3):
            try:
                return list(client.text(query, max_results=5))
            except RatelimitException:
                if attempt == 2:
                    logger.warning(f"DDG 202 Ratelimit after 3 attempts: {query!r}")
                    return []
                logger.warning(f"DDG 202 Ratelimit — backing off {backoff}s")
                time.sleep(backoff)
                backoff *= 2
            except Exception as exc:
                logger.warning(f"DDG error for {query!r}: {exc}")
                return []
        return []

    with DDGS() as ddgs:
        results = _search(ddgs)

    if not results:
        return {}

    prompt = _build_prompt(company_name, ceo_first_name, results)
    try:
        extracted = _call_gemini(prompt, api_key)
    except Exception as exc:
        logger.warning(f"Gemini extraction failed for {company_name!r}: {exc}")
        return {}

    enrichment: dict = {}

    # Populate non-domain fields regardless of whether domain resolves
    description = extracted.get("description") or ""
    use_case = extracted.get("use_case") or ""

    if description:
        enrichment["description"] = description
    if extracted.get("industry"):
        enrichment["industry"] = extracted["industry"]
    if extracted.get("keywords"):
        enrichment["keywords"] = extracted["keywords"]
    if use_case:
        enrichment["use_case"] = use_case

    # Embed description + use_case together for semantic prospect search
    embed_text = " ".join(filter(None, [description, use_case])).strip()
    if embed_text:
        embedding = _embed_text(embed_text, api_key)
        if embedding:
            enrichment["description_embedding"] = embedding

    raw_domain = extracted.get("domain")
    if not raw_domain:
        return enrichment

    # Validate the domain Gemini returned — strip www and check it's not a skip domain
    domain = _extract_domain(f"https://{raw_domain}")
    if not domain:
        logger.debug(f"Gemini returned skip-listed domain {raw_domain!r} for {company_name!r}")
        return enrichment

    enrichment["domain"] = domain

    # Find the matching URL from results to return as website_url
    for r in results:
        url = r.get("href") or ""
        if domain in url:
            enrichment["website_url"] = url
            break

    return enrichment


async def lookup_domain(company_name: str, ceo_first_name: str = "") -> dict:
    """
    One DDG search + one Gemini 2.5 Flash-Lite call.
    Returns enrichment dict with any of: domain, website_url, description,
    industry, keywords, use_case.
    """
    import settings  # lazy import to avoid circular deps at module level

    api_key = settings.GEMINI_API_KEY
    if not api_key:
        logger.warning("GEMINI_API_KEY not set — skipping enrichment")
        return {}

    return await asyncio.get_event_loop().run_in_executor(
        None, _run_lookup, company_name, ceo_first_name, api_key
    )
