"""
First-party feeds — a company's own changelog / blog / newsroom RSS or Atom.

A dated entry published by the company itself is the strongest launch signal
we can get (no name collisions, no LLM guessing which page is a "launch"), and
newsroom feeds also carry the company's own funding / exec / expansion news.

  discover_feed_url(domain, homepage_html) → feed URL or None (cached in raw_meta)
  fetch_feed_entries(feed_url)             → recent entries
  classify_feed_entries(company, entries)  → typed extra_events
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

import settings as cfg
from signals.diff import parse_event_date

logger = logging.getLogger("discovery.feeds")

_UA = {"User-Agent": "Mozilla/5.0 (compatible; TelloraDiscovery/1.0)"}
_COMMON_PATHS = (
    "/changelog/rss.xml", "/changelog/feed", "/changelog.xml", "/changelog/rss",
    "/blog/rss.xml", "/blog/feed", "/blog/rss", "/feed", "/rss.xml", "/feed.xml",
    "/atom.xml", "/news/rss.xml", "/press/rss.xml", "/newsroom/rss.xml",
)
_MAX_ENTRIES = 15

# classifier category → event type; anything else is dropped
_CATEGORY_EVENTS = {
    "launch": "product_launch",
    "funding": "funding_round",
    "exec_hire": "exec_hire",
    "expansion": "expansion",
    "acquisition": "acquisition",
    "partnership": "partnership",
}


def _feed_links_in_html(html: str, base_url: str) -> list[str]:
    links = []
    for tag in re.findall(r"<link\b[^>]*>", html or "", re.IGNORECASE):
        if not re.search(r"type=[\"']application/(rss|atom)\+xml", tag, re.IGNORECASE):
            continue
        href = re.search(r"href=[\"']([^\"']+)", tag, re.IGNORECASE)
        if href:
            links.append(urljoin(base_url, href.group(1)))
    return links


def _same_site(url: str, domain: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    d = domain.lower().replace("www.", "")
    return host == d or host.endswith("." + d)


def _rank(url: str) -> int:
    u = url.lower()
    for i, word in enumerate(("changelog", "release", "product", "news", "press", "blog")):
        if word in u:
            return i
    return 9


async def _is_feed(client: httpx.AsyncClient, url: str) -> bool:
    try:
        resp = await client.get(url)
    except Exception:
        return False
    if resp.status_code != 200:
        return False
    head = resp.text[:600].lower()
    return "<rss" in head or "<feed" in head or "<rdf:rdf" in head


async def discover_feed_url(domain: str, homepage_html: str = "") -> Optional[str]:
    """Best first-party feed for a domain: <link rel=alternate> first, then common paths."""
    if not domain:
        return None
    base = f"https://{domain}"
    candidates = [u for u in _feed_links_in_html(homepage_html, base) if _same_site(u, domain)]
    candidates.sort(key=_rank)
    async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_UA) as client:
        for url in candidates[:3]:
            if await _is_feed(client, url):
                return url
        for path in _COMMON_PATHS:
            url = f"{base}{path}"
            if await _is_feed(client, url):
                return url
    return None


async def fetch_feed_entries(feed_url: str, max_age_days: Optional[int] = None) -> list[dict]:
    """Recent entries: [{title, url, date, summary}] newest first."""
    import feedparser

    if not feed_url:
        return []
    max_age = timedelta(days=max_age_days or cfg.NEWS_EVENT_MAX_AGE_DAYS)
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers=_UA) as client:
            resp = await client.get(feed_url)
            resp.raise_for_status()
        parsed = await asyncio.to_thread(feedparser.parse, resp.text)
    except Exception as exc:
        logger.warning(f"Feed fetch failed for {feed_url}: {exc}")
        return []

    cutoff = datetime.now(timezone.utc) - max_age
    entries = []
    for e in parsed.entries[:50]:
        date = parse_event_date(e.get("published") or e.get("updated"))
        if not date or date < cutoff:
            continue
        summary = re.sub(r"<[^>]+>", " ", e.get("summary") or "")
        entries.append({
            "title": (e.get("title") or "").strip(),
            "url": e.get("link") or feed_url,
            "date": date.isoformat(),
            "summary": re.sub(r"\s+", " ", summary).strip()[:400],
        })
    entries = [x for x in entries if x["title"]]
    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:_MAX_ENTRIES]


def classify_feed_entries(company_name: str, entries: list[dict]) -> list[dict]:
    """LLM-type first-party entries. Synchronous — run in a thread."""
    if not entries:
        return []
    from llm import get_router, retry_llm, strip_json_fences

    sample = [{"i": i, "title": e["title"], "summary": e["summary"][:240]} for i, e in enumerate(entries)]
    prompt = f"""These entries come from {company_name}'s own blog / changelog / newsroom feed.
Classify each:
  launch      — a new product, major feature, or new plan/tier shipped by {company_name}
  funding     — {company_name} raised money (must state an amount or round)
  exec_hire   — {company_name} hired or promoted a named executive (VP and above)
  expansion   — new office, new country/market, or a large headcount expansion
  acquisition — {company_name} acquired or was acquired by a named company
  partnership — a named strategic partnership or integration launch
  other       — thought leadership, tips, events, webinars, hiring posts, minor fixes, recaps

ENTRIES:
{json.dumps(sample)}

Respond with ONLY valid JSON: {{"results": [{{"i": 0, "category": "launch", "headline": "short factual headline"}}]}}"""

    def _do():
        raw = get_router().complete_text(prompt, models=get_router().signal_models, temperature=0.0, json_mode=True)
        return json.loads(strip_json_fences(raw))

    try:
        verdicts = retry_llm(_do)
    except Exception as exc:
        logger.warning(f"Feed classification failed for '{company_name}': {exc}")
        return []

    events = []
    for v in verdicts.get("results", []):
        try:
            entry = entries[int(v["i"])]
        except (KeyError, ValueError, IndexError, TypeError):
            continue
        event_type = _CATEGORY_EVENTS.get((v.get("category") or "").strip().lower())
        if not event_type:
            continue
        headline = (v.get("headline") or entry["title"]).strip()
        events.append({
            "event_type": event_type,
            "title": headline[:500],
            "payload": {
                "key": f"feed:{entry['url'][:120]}",
                "url": entry["url"], "date": entry["date"], "feed_title": entry["title"],
            },
            "source": "company_feed",
            "confidence": 0.9,
            "evidence_url": entry["url"],
            "event_date": entry["date"],
        })
    return events[:6]
