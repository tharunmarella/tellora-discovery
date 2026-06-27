"""
Funding news snippets for Gemini synthesis.

Serper News when SERPER_API_KEY is set; Google News RSS fallback (free).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from urllib.parse import quote

import httpx

import settings as cfg
from signals.name_match import title_mentions_company

logger = logging.getLogger("discovery.funding_news")

_MAX_SNIPPETS = 4
_SERPER_POOL = 8
_RSS_POOL = 12
_RSS_DAYS = 30

_RSS_FUNDING_URL = (
    "https://news.google.com/rss/search?"
    'q="{name}"+(funding+OR+raised+OR+"Series"+OR+investment)+when:{days}d'
    "&hl=en-US&gl=US&ceid=US:en"
)


def _format_snippet(title: str, description: str, url: str) -> str:
    desc = (description or "").strip()[:300]
    parts = [title.strip()]
    if desc:
        parts.append(desc)
    body = " — ".join(parts)
    if url:
        body = f"{body} ({url})"
    return body


def _filter_snippets(
    hits: list[tuple[str, str, str]],
    company_name: str,
) -> list[str]:
    """Keep hits whose title mentions the company; return up to _MAX_SNIPPETS."""
    snippets: list[str] = []
    for title, desc, url in hits:
        if not title.strip():
            continue
        if not title_mentions_company(title, company_name):
            logger.debug(
                "Dropping off-company funding hit for '%s': %r",
                company_name,
                title,
            )
            continue
        snippets.append(_format_snippet(title, desc, url))
        if len(snippets) >= _MAX_SNIPPETS:
            break
    return snippets


async def _fetch_serper_funding_news(company_name: str) -> list[str]:
    if not cfg.SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "https://google.serper.dev/news",
                headers={
                    "X-API-KEY": cfg.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "q": (
                        f'"{company_name}" funding OR raised OR "Series" '
                        f'OR investment OR "seed round"'
                    ),
                    "num": _SERPER_POOL,
                    "tbs": "qdr:m6",
                },
            )
            resp.raise_for_status()
            hits = [
                (
                    (h.get("title") or "").strip(),
                    (h.get("snippet") or "").strip(),
                    (h.get("link") or "").strip(),
                )
                for h in resp.json().get("news", [])[:_SERPER_POOL]
            ]
            snippets = _filter_snippets(hits, company_name)
            if snippets:
                logger.info(
                    "Funding news (Serper) for '%s': %d snippets",
                    company_name,
                    len(snippets),
                )
            return snippets
    except Exception as exc:
        logger.warning("Serper funding news failed for '%s': %s", company_name, exc)
        return []


async def _fetch_rss_funding_news(company_name: str) -> list[str]:
    encoded_name = quote(company_name, safe="")
    url = _RSS_FUNDING_URL.format(name=encoded_name, days=_RSS_DAYS)
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception as exc:
        logger.warning("Google News RSS funding failed for '%s': %s", company_name, exc)
        return []

    hits: list[tuple[str, str, str]] = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        src_el = item.find("source")
        source = (src_el.text or "").strip() if src_el is not None else ""
        if title:
            hits.append((title, source, link))
        if len(hits) >= _RSS_POOL:
            break

    snippets = _filter_snippets(hits, company_name)
    if snippets:
        logger.info(
            "Funding news (RSS) for '%s': %d snippets",
            company_name,
            len(snippets),
        )
    return snippets


async def fetch_funding_news(company_name: str) -> list[str]:
    """
    Recent funding headlines as text snippets for Gemini synthesis.
    Tries Serper News first, then Google News RSS (no API key required).
    """
    if not company_name:
        return []

    snippets = await _fetch_serper_funding_news(company_name)
    if snippets:
        return snippets

    return await _fetch_rss_funding_news(company_name)
