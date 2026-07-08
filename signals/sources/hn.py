"""
Hacker News signals via Algolia API (free, no key).

  - Mentions: company name in stories (7d), >=10 pts or >=5 comments → news_mention
  - Show HN: URL host matches company domain → product_launch
"""

from __future__ import annotations

import logging
import time
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("discovery.hn")

_HN_API = "https://hn.algolia.com/api/v1/search_by_date"
_MENTION_MIN_POINTS = 10
_MENTION_MIN_COMMENTS = 5


async def fetch_hn_signals(company_name: str, domain: str) -> dict:
    """
    Returns {mentions: [...], launches: [...]} raw HN hits for extra_events.
    """
    if not company_name:
        return {"mentions": [], "launches": []}

    cutoff = int(time.time()) - 7 * 86400
    mentions: list[dict] = []
    launches: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(_HN_API, params={
                "query": company_name,
                "tags": "story",
                "numericFilters": f"created_at_i>{cutoff}",
                "hitsPerPage": 15,
            })
            if r.status_code != 200:
                return {"mentions": [], "launches": []}
            hits = r.json().get("hits", [])

            domain_host = (domain or "").lower().replace("www.", "")
            for h in hits:
                title = (h.get("title") or "").strip()
                url = h.get("url") or ""
                points = int(h.get("points") or 0)
                comments = int(h.get("num_comments") or 0)
                obj_id = h.get("objectID", "")

                if domain_host and title.lower().startswith("show hn"):
                    try:
                        host = urlparse(url).netloc.lower().replace("www.", "")
                    except Exception:
                        host = ""
                    if domain_host in host or host.endswith(domain_host):
                        launches.append({
                            "title": title,
                            "url": url,
                            "object_id": obj_id,
                            "points": points,
                            "created_at": h.get("created_at"),
                        })
                        continue

                if points >= _MENTION_MIN_POINTS or comments >= _MENTION_MIN_COMMENTS:
                    mentions.append({
                        "title": title,
                        "url": url or f"https://news.ycombinator.com/item?id={h.get('story_id') or obj_id}",
                        "object_id": obj_id,
                        "points": points,
                        "comments": comments,
                        "created_at": h.get("created_at"),
                    })
    except Exception as exc:
        logger.warning(f"HN signals failed for '{company_name}': {exc}")

    return {"mentions": mentions[:5], "launches": launches[:3]}


def hn_extra_events(hn: dict) -> list[dict]:
    """Convert HN hits into extra_events drafts."""
    events: list[dict] = []
    for m in hn.get("mentions") or []:
        events.append({
            "event_type": "news_mention",
            "title": f"HN: {m['title'][:460]}",
            "payload": {
                "key": f"hn:{m['object_id']}",
                "url": m.get("url"),
                "points": m.get("points"),
                "comments": m.get("comments"),
                "category": "discussion",
            },
            "source": "hacker_news",
            "confidence": 0.7,
            "evidence_url": m.get("url"),
            "event_date": m.get("created_at"),
        })
    for l in hn.get("launches") or []:
        events.append({
            "event_type": "product_launch",
            "title": f"Show HN: {l['title'][:460]}",
            "payload": {
                "key": f"hn:show:{l['object_id']}",
                "url": l.get("url"),
                "points": l.get("points"),
            },
            "source": "hacker_news",
            "confidence": 0.85,
            "evidence_url": l.get("url"),
            "event_date": l.get("created_at"),
        })
    return events
