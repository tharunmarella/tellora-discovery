"""HTTP-mocked tests for first-party feed discovery and classification."""

import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
import respx

from signals.sources.feeds import classify_feed_entries, discover_feed_url, fetch_feed_entries


def _rss(*items: tuple[str, datetime]) -> str:
    body = "".join(
        f"<item><title>{t}</title><link>https://acme.com/changelog/{i}</link>"
        f"<pubDate>{format_datetime(d)}</pubDate><description>Details</description></item>"
        for i, (t, d) in enumerate(items)
    )
    return f'<?xml version="1.0"?><rss version="2.0"><channel><title>Acme</title>{body}</channel></rss>'


@pytest.mark.asyncio
@respx.mock
async def test_discover_feed_from_link_tag_prefers_changelog():
    respx.get("https://acme.com/changelog/rss.xml").mock(return_value=httpx.Response(200, text=_rss()))
    html = (
        '<link rel="alternate" type="application/rss+xml" href="/blog/rss.xml">'
        '<link rel="alternate" type="application/rss+xml" href="/changelog/rss.xml">'
    )
    assert await discover_feed_url("acme.com", html) == "https://acme.com/changelog/rss.xml"


@pytest.mark.asyncio
@respx.mock
async def test_discover_feed_ignores_third_party_and_falls_back_to_paths():
    respx.get("https://acme.com/blog/rss.xml").mock(return_value=httpx.Response(200, text=_rss()))
    respx.get(url__regex=r"https://acme\.com/.*").mock(return_value=httpx.Response(404))
    html = '<link rel="alternate" type="application/rss+xml" href="https://medium.com/feed/acme">'
    assert await discover_feed_url("acme.com", html) == "https://acme.com/blog/rss.xml"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_feed_entries_keeps_recent_only():
    now = datetime.now(timezone.utc)
    respx.get("https://acme.com/feed").mock(return_value=httpx.Response(200, text=_rss(
        ("Workflows GA", now - timedelta(days=3)),
        ("2019 recap", now - timedelta(days=900)),
    )))
    entries = await fetch_feed_entries("https://acme.com/feed")
    assert [e["title"] for e in entries] == ["Workflows GA"]


def test_classify_feed_entries_maps_categories(monkeypatch):
    from unittest.mock import MagicMock

    router = MagicMock()
    router.signal_models = ["test"]
    router.complete_text.return_value = json.dumps({"results": [
        {"i": 0, "category": "launch", "headline": "Acme ships Workflows GA"},
        {"i": 1, "category": "other"},
    ]})
    monkeypatch.setattr("llm.get_router", lambda: router)
    entries = [
        {"title": "Workflows GA", "url": "https://acme.com/c/1", "date": "2026-10-20T00:00:00+00:00", "summary": ""},
        {"title": "5 tips for onboarding", "url": "https://acme.com/c/2", "date": "2026-10-19T00:00:00+00:00", "summary": ""},
    ]
    events = classify_feed_entries("Acme", entries)
    assert [(e["event_type"], e["source"], e["evidence_url"]) for e in events] == [
        ("product_launch", "company_feed", "https://acme.com/c/1")
    ]
