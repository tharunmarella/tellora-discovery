"""
Google News RSS ingester — free per-company news monitoring (no API key).

  fetch_company_news(name)  → headlines from the last 7 days
  classify_news(name, items) → Gemini batch relevance filter → extra_events

Relevant headlines become news_mention events; confident funding / exec-hire
classifications are upgraded to funding_round / exec_hire (higher heat weight).
Dedupe is on the encoded Google News URL.
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET

import httpx

import settings as cfg
from llm import get_gemini_client, retry_llm, strip_json_fences

logger = logging.getLogger("discovery.news")

_RSS_URL = (
    "https://news.google.com/rss/search?"
    'q="{name}"+when:{days}d+-stock+-shares&hl=en-US&gl=US&ceid=US:en'
)

# classifier category → event type (anything else stays news_mention)
_CATEGORY_UPGRADES = {"funding": "funding_round", "exec_hire": "exec_hire"}


async def fetch_company_news(company_name: str, days: int = 7) -> list[dict]:
    """Fetch recent headlines from Google News RSS. Returns [{title, url, date, source}]."""
    if not company_name:
        return []
    url = _RSS_URL.format(name=httpx.QueryParams({"q": company_name})["q"], days=days)
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception as exc:
        logger.warning(f"Google News RSS failed for '{company_name}': {exc}")
        return []

    items = []
    for item in root.iter("item"):
        src_el = item.find("source")
        items.append({
            "title": (item.findtext("title") or "").strip(),
            "url": (item.findtext("link") or "").strip(),
            "date": (item.findtext("pubDate") or "").strip(),
            "source": (src_el.text or "").strip() if src_el is not None else "",
        })
    return [it for it in items if it["title"]][:12]


def classify_news(company_name: str, items: list[dict]) -> list[dict]:
    """
    Gemini batch classification of headlines. Synchronous — run in executor.
    Returns extra_events drafts for relevant headlines only.
    """
    if not items or not cfg.GEMINI_API_KEY:
        return []

    sample = [{"i": i, "title": it["title"], "source": it["source"]}
              for i, it in enumerate(items)]
    prompt = f"""Classify each news headline about the company "{company_name}".
category = funding | exec_hire | partnership | product | expansion | layoffs | irrelevant
Mark relevant=false for stock chatter, lawsuits, listicles, or headlines not
actually about {company_name} as a business.

HEADLINES:
{json.dumps(sample)}

Respond with ONLY valid JSON: {{"results": [{{"i": 0, "category": "...", "relevant": true}}]}}"""

    def _do():
        client = get_gemini_client()
        resp = client.models.generate_content(model=cfg.SIGNAL_GEMINI_MODEL, contents=prompt)
        return json.loads(strip_json_fences(resp.text))

    try:
        verdicts = retry_llm(_do)
    except Exception as exc:
        logger.warning(f"News classification failed for '{company_name}': {exc}")
        return []

    events = []
    for v in verdicts.get("results", []):
        try:
            it = items[int(v["i"])]
        except (KeyError, ValueError, IndexError):
            continue
        category = (v.get("category") or "irrelevant").strip().lower()
        if not v.get("relevant") or category == "irrelevant":
            continue
        event_type = _CATEGORY_UPGRADES.get(category, "news_mention")
        events.append({
            "event_type": event_type,
            "title": it["title"][:500],
            "payload": {
                "key": (it.get("url") or it["title"])[:120],
                "url": it.get("url"),
                "date": it.get("date"),
                "news_source": it.get("source"),
                "category": category,
            },
            "source": "google_news",
            "confidence": 0.75,
        })
    return events[:5]
