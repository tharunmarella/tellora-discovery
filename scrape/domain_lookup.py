"""
Company enrichment pipeline for the discovery service.

Flow per company:
  1. Serper search "{company_name} CEO: {ceo_first_name}"
  2. Trim response to knowledgeGraph + top 7 organic results
  3. Single Gemini call: extract all DiscoveryCompany fields from the JSON
  4. Embed description + use_case → 768-dim vector for semantic search

Fallback: DuckDuckGo if Serper is not configured or returns nothing.
Requires: GOOGLE_API_KEY. Optional: SERPER_API_KEY.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time

import httpx

import settings as cfg
from llm import get_router, retry_llm, strip_json_fences

logger = logging.getLogger("discovery.enrichment")

INDUSTRY_ENUM = [
    "DevTools", "Logistics", "Healthcare", "Financial Services",
    "Sales & Marketing", "Cybersecurity", "Construction", "HR Tech",
    "Real Estate", "EdTech", "Legal", "Manufacturing", "Retail",
    "GovTech", "Hospitality", "Media & Advertising", "Nonprofit", "Other",
]

EXTRACT_PROMPT = """You are a company data extraction agent. Below is the full Google search result JSON for "{company_name}".

Extract as many fields as possible and return a single JSON object matching this schema:
{{
  "name": "official company name",
  "domain": "company website domain e.g. example.com (null if not found)",
  "website_url": "full URL to company homepage (null if not found)",
  "description": "one sentence (max 150 chars) describing what the company does",
  "industries": ["pick ALL that apply from: {industry_list}"],
  "ceo_name": "full name of the CEO (null if not found)",
  "headquarters": "city and state/country e.g. San Francisco, CA (null if not found)",
  "founded_year": "4-digit year as string e.g. 2018 (null if not found)",
  "funding": "funding stage and/or amount e.g. Series B · $25M (null if not found)",
  "linkedin_url": "full LinkedIn company page URL e.g. https://www.linkedin.com/company/acme (null if not found)",
  "keywords": ["3-5 short tags describing the company space"],
  "use_case": "one sentence: what type of company would buy or benefit from this company's product (null if unclear)"
}}

IMPORTANT: "industries" must ONLY contain values from this list: {industry_list}
A company can belong to multiple industries. Pick all that apply.

Search result JSON:
{serper_json}

Return only valid JSON. No explanation, no markdown fences."""

_LINKEDIN_COMPANY_RE = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


def _normalize_linkedin_company_url(url: str) -> str | None:
    """Return canonical https://www.linkedin.com/company/<slug> or None."""
    if not url:
        return None
    m = _LINKEDIN_COMPANY_RE.search(url.strip())
    if not m:
        return None
    slug = m.group(1).strip("/")
    if not slug:
        return None
    return f"https://www.linkedin.com/company/{slug}"


def extract_linkedin_url(serper_data: dict) -> str | None:
    """
    Deterministic LinkedIn company URL from Serper organic / knowledgeGraph links.
    Ignores personal /in/ profile URLs.
    """
    candidates: list[str] = []

    kg = serper_data.get("knowledgeGraph") or {}
    for key in ("linkedin", "linkedinUrl", "linkedin_url", "website"):
        val = kg.get(key)
        if isinstance(val, str):
            candidates.append(val)

    for item in serper_data.get("organic") or []:
        link = item.get("link") or item.get("url") or ""
        if isinstance(link, str) and "linkedin.com" in link.lower():
            candidates.append(link)

    for raw in candidates:
        normalized = _normalize_linkedin_company_url(raw)
        if normalized:
            return normalized
    return None


def _call_gemini(company_name: str, serper_data: dict) -> dict:
    trimmed = {
        "knowledgeGraph": serper_data.get("knowledgeGraph"),
        "organic": serper_data.get("organic", [])[:7],
    }
    prompt = EXTRACT_PROMPT.format(
        company_name=company_name,
        serper_json=json.dumps(trimmed, indent=2),
        industry_list=", ".join(INDUSTRY_ENUM),
    )
    def _do():
        raw = get_router().complete_text(
            prompt,
            models=get_router().enrichment_models,
            temperature=0.0,
            json_mode=True,
        )
        return json.loads(strip_json_fences(raw))

    return retry_llm(_do)


# ── Search providers ──────────────────────────────────────────────────────────

def _fetch_serper(query: str, serper_api_key: str) -> dict:
    try:
        resp = httpx.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": serper_api_key, "Content-Type": "application/json"},
            json={"q": query, "gl": "us", "hl": "en", "num": 10},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning(f"Serper error for {query!r}: {exc}")
        return {}


def _fetch_ddg(query: str) -> dict:
    """DuckDuckGo fallback — returns a fake Serper-shaped dict with organic list."""
    try:
        from ddgs import DDGS
        from ddgs.exceptions import RatelimitException
    except ImportError:
        from duckduckgo_search import DDGS
        try:
            from duckduckgo_search.exceptions import RatelimitException
        except Exception:
            RatelimitException = Exception  # type: ignore

    backoff = 30
    for attempt in range(3):
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=10))
            # Normalise to Serper organic shape
            organic = [
                {"title": r.get("title", ""), "link": r.get("href", ""), "snippet": r.get("body", "")}
                for r in results
            ]
            return {"organic": organic}
        except RatelimitException:
            if attempt == 2:
                logger.warning(f"DDG ratelimit after 3 attempts: {query!r}")
                return {}
            logger.warning(f"DDG ratelimit — backing off {backoff}s")
            time.sleep(backoff)
            backoff *= 2
        except Exception as exc:
            logger.warning(f"DDG error for {query!r}: {exc}")
            return {}
    return {}


# ── Main lookup ───────────────────────────────────────────────────────────────

def _run_lookup(company_name: str, ceo_first_name: str, serper_api_key: str) -> dict:
    """
    One search (Serper → DDG fallback) + one Gemini call.
    Returns enrichment dict matching DiscoveryCompany fields plus
    keywords, use_case (stored in raw_meta) and description_embedding.
    """
    query = f"{company_name} CEO: {ceo_first_name}" if ceo_first_name else f"{company_name} company"

    # Search
    serper_data: dict = {}
    if serper_api_key:
        serper_data = _fetch_serper(query, serper_api_key)

    if not serper_data.get("organic"):
        logger.warning(f"Serper empty for {query!r} — falling back to DDG")
        serper_data = _fetch_ddg(query)

    if not serper_data.get("organic"):
        logger.debug(f"No search results for {company_name!r}")
        return {}

    # Single Gemini call — extracts all fields from the full Serper JSON
    try:
        extracted = _call_gemini(company_name, serper_data)
    except Exception as exc:
        logger.warning(f"Gemini extraction failed for {company_name!r}: {exc}")
        return {}

    # Map Gemini output → enrichment dict (only include non-null values)
    enrichment: dict = {}
    for field in ("name", "domain", "website_url", "description",
                  "ceo_name", "headquarters", "founded_year", "funding", "linkedin_url"):
        val = extracted.get(field)
        if val:
            enrichment[field] = val

    # Prefer deterministic LinkedIn company URL from search results over Gemini guess.
    linkedin = extract_linkedin_url(serper_data) or enrichment.get("linkedin_url")
    if linkedin:
        enrichment["linkedin_url"] = linkedin
    elif "linkedin_url" in enrichment:
        normalized = _normalize_linkedin_company_url(enrichment["linkedin_url"])
        if normalized:
            enrichment["linkedin_url"] = normalized
        else:
            enrichment.pop("linkedin_url", None)

    # industries: array from Gemini → store as comma-joined string in `industry` column
    industries = extracted.get("industries") or []
    if isinstance(industries, list) and industries:
        enrichment["industry"] = ", ".join(industries)

    # logo_url: construct from domain, no API call needed
    if enrichment.get("domain"):
        enrichment["logo_url"] = f"https://www.google.com/s2/favicons?domain={enrichment['domain']}&sz=64"

    # keywords + use_case go into raw_meta
    if extracted.get("keywords"):
        enrichment["keywords"] = extracted["keywords"]
    if extracted.get("use_case"):
        enrichment["use_case"] = extracted["use_case"]

    # Scrape-time embedding so new rows are searchable before signal enrichment.
    from llm import embed_text

    embed_parts = [
        p for p in (enrichment.get("description"), enrichment.get("industry")) if p
    ]
    if embed_parts and cfg.GEMINI_API_KEY:
        vec = embed_text(" ".join(embed_parts))
        if vec:
            enrichment["description_embedding"] = vec

    return enrichment


async def lookup_domain(company_name: str, ceo_first_name: str = "") -> dict:
    """
    Async wrapper — runs in thread pool to avoid blocking the event loop.
    Returns enrichment dict with DiscoveryCompany fields ready to write to DB.
    """
    if not cfg.GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set — skipping enrichment")
        return {}

    serper_api_key = cfg.SERPER_API_KEY
    if not serper_api_key:
        logger.info("SERPER_API_KEY not set — using DDG only")

    return await asyncio.get_event_loop().run_in_executor(
        None, _run_lookup, company_name, ceo_first_name, serper_api_key
    )
