"""HTTP-mocked tests for funding-news relevance filtering."""

import httpx
import pytest
import respx

from signals.sources.funding_news import fetch_funding_news


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_drops_same_name_other_company(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SERPER_API_KEY", "test-key")
    respx.post("https://google.serper.dev/news").mock(
        return_value=httpx.Response(200, json={
            "news": [
                {
                    "title": "Guardrails AI raises $7.5M seed round",
                    "snippet": "Guardrails AI, an LLM validation startup, raised $7.5M.",
                    "link": "https://techcrunch.com/guardrails-ai-seed",
                },
                {
                    "title": "Guardrail Technologies lands $3M to expand platform",
                    "snippet": "Guardrail Technologies raised $3M to grow its safety platform.",
                    "link": "https://example.com/guardrail-technologies",
                },
            ],
        })
    )

    out = await fetch_funding_news("Guardrail Technologies")

    assert len(out) == 1
    assert "Guardrail Technologies" in out[0]
    assert "Guardrails AI" not in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_keeps_relevant_hit(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SERPER_API_KEY", "test-key")
    respx.post("https://google.serper.dev/news").mock(
        return_value=httpx.Response(200, json={
            "news": [
                {
                    "title": "Acme Robotics closes $25M Series B",
                    "snippet": "Acme Robotics announced a $25M Series B round.",
                    "link": "https://news.example/acme-series-b",
                },
            ],
        })
    )

    out = await fetch_funding_news("Acme Robotics")
    assert len(out) == 1
    assert "Acme Robotics" in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_rss_fallback_when_no_serper(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SERPER_API_KEY", "")
    rss = """<?xml version="1.0"?>
    <rss><channel>
      <item>
        <title>Acme Robotics closes $25M Series B</title>
        <link>https://news.example/acme-series-b</link>
        <source>TechNews</source>
      </item>
    </channel></rss>"""
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=rss)
    )

    out = await fetch_funding_news("Acme Robotics")
    assert len(out) == 1
    assert "Acme Robotics" in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_empty_when_no_hits(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SERPER_API_KEY", "")
    rss = """<?xml version="1.0"?><rss><channel></channel></rss>"""
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=rss)
    )

    assert await fetch_funding_news("Acme Robotics") == []
