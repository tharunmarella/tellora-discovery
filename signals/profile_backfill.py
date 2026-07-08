"""
Search-grounded profile backfill — funding/HQ fields only (never alertable events).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Optional

import httpx

import settings as cfg
from llm import get_router, retry_llm, signal_model_chain, strip_json_fences

if TYPE_CHECKING:
    from signals.pipeline import CompanySignalResult

logger = logging.getLogger("discovery.profile_backfill")

_FUNDING_QUERY = '{name} funding round total raised'
_HQ_QUERY = '{name} headquarters location'


async def fetch_serper_web_snippets(query: str, *, num: int = 5) -> list[str]:
    """Organic search snippets for profile backfill (one query max per call)."""
    if not cfg.SERPER_API_KEY or not query.strip():
        return []
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": cfg.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": query.strip(), "num": num},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("Serper profile backfill failed for %r: %s", query, exc)
        return []

    snippets: list[str] = []
    for hit in data.get("organic", [])[:num]:
        title = (hit.get("title") or "").strip()
        snippet = (hit.get("snippet") or "").strip()
        link = (hit.get("link") or "").strip()
        if not title and not snippet:
            continue
        body = title
        if snippet:
            body = f"{body} — {snippet}" if body else snippet
        if link:
            body = f"{body} ({link})"
        snippets.append(body)
    return snippets


def extract_funding_profile(company_name: str, snippets: list[str]) -> dict:
    """Profile-only funding extraction from search snippets."""
    if not snippets or not cfg.GEMINI_API_KEY:
        return {}
    evidence = "\n".join(f"- {s}" for s in snippets[:5])
    prompt = f"""Extract funding profile facts for a CRM company record.

STRICT RULES:
- Profile fields only — never invent rounds not explicitly mentioned in snippets.
- Return null for any field not clearly supported by the snippets.
- Do NOT emit alerts or time-sensitive triggers.

Company: {company_name}

Search snippets:
{evidence}

Return ONLY valid JSON:
{{"funding_stage": "<e.g. Series C or null>", "total_raised": "<e.g. $863M or null>", "investors": ["<name>"]}}"""

    def _do() -> dict:
        raw = get_router().complete_text(
            prompt,
            models=signal_model_chain(),
            temperature=0.0,
            json_mode=True,
        )
        data = json.loads(strip_json_fences(raw))
        return {
            "funding_stage": data.get("funding_stage") or None,
            "total_raised": data.get("total_raised") or None,
            "investors": [str(i) for i in (data.get("investors") or []) if i],
        }

    try:
        return retry_llm(_do)
    except Exception as exc:
        logger.warning("Funding profile backfill LLM failed for %s: %s", company_name, exc)
        return {}


def extract_hq_raw(company_name: str, snippets: list[str]) -> Optional[str]:
    """Pick a headquarters string from search snippets for normalization."""
    if not snippets or not cfg.GEMINI_API_KEY:
        return None
    evidence = "\n".join(f"- {s}" for s in snippets[:5])
    prompt = f"""From the search snippets below, extract the company's headquarters location as a single raw string (city, region, country if available).

Company: {company_name}

Snippets:
{evidence}

Return ONLY valid JSON: {{"headquarters": "<raw HQ string or null>"}}
Return null if HQ is not clearly stated."""

    def _do() -> Optional[str]:
        raw = get_router().complete_text(
            prompt,
            models=signal_model_chain(),
            temperature=0.0,
            json_mode=True,
        )
        data = json.loads(strip_json_fences(raw))
        hq = (data.get("headquarters") or "").strip()
        return hq or None

    try:
        return retry_llm(_do)
    except Exception as exc:
        logger.warning("HQ profile backfill LLM failed for %s: %s", company_name, exc)
        return None


async def _fetch_rss_fallback_snippets(company_name: str) -> list[str]:
    """Free Google News RSS fallback when Serper is unavailable (funding only)."""
    try:
        from signals.sources.funding_news import _fetch_rss_funding_news
        return await _fetch_rss_funding_news(company_name)
    except Exception as exc:
        logger.warning("RSS fallback failed for %s: %s", company_name, exc)
        return []


async def apply_profile_backfill(
    result: "CompanySignalResult",
    *,
    company_name: str,
    serper_kg: Optional[dict] = None,
    existing_headquarters: Optional[str] = None,
) -> "CompanySignalResult":
    """
    One Serper + one small LLM call max for missing funding/HQ profile fields.
    Never creates discovery_signal_event rows.
    """
    from signals.pipeline import normalize_headquarters

    if not cfg.PROFILE_BACKFILL_ENABLED:
        return result

    updates: dict = {}
    kg = serper_kg or {}

    needs_funding = not result.funding_stage and not result.total_raised
    if needs_funding:
        query = _FUNDING_QUERY.format(name=company_name)
        snippets = await fetch_serper_web_snippets(query)
        if not snippets:
            snippets = await _fetch_rss_fallback_snippets(company_name)
        profile = extract_funding_profile(company_name, snippets)
        if profile.get("funding_stage"):
            updates["funding_stage"] = profile["funding_stage"]
        if profile.get("total_raised"):
            updates["total_raised"] = profile["total_raised"]
        if profile.get("investors") and not result.investors:
            updates["investors"] = profile["investors"]

    needs_hq = not result.hq_city
    has_hq_source = bool(
        (existing_headquarters or "").strip()
        or (kg.get("headquarters") or "").strip()
    )
    if needs_hq and not has_hq_source:
        query = _HQ_QUERY.format(name=company_name)
        snippets = await fetch_serper_web_snippets(query)
        raw_hq = extract_hq_raw(company_name, snippets)
        if raw_hq:
            normalized = normalize_headquarters(raw_hq)
            if normalized.get("hq_city"):
                updates["hq_city"] = normalized.get("hq_city")
                updates["hq_region"] = normalized.get("hq_region")
                updates["hq_country"] = normalized.get("hq_country")

    if updates:
        logger.info("[%s] Profile backfill applied: %s", company_name, list(updates.keys()))
        return result.model_copy(update=updates)
    return result
