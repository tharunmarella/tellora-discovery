"""HTTP-mocked tests for funding-news relevance filtering (Google News RSS)."""

import httpx
import pytest
import respx

from signals.sources.funding_news import fetch_funding_news


def _rss(*titles: str) -> str:
    items = "".join(
        f"<item><title>{t}</title><link>https://news.example/{i}</link><source>TechNews</source></item>"
        for i, t in enumerate(titles)
    )
    return f'<?xml version="1.0"?><rss><channel>{items}</channel></rss>'


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_drops_same_name_other_company():
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=_rss(
            "Guardrails AI raises $7.5M seed round",
            "Guardrail Technologies lands $3M to expand platform",
        ))
    )

    out = await fetch_funding_news("Guardrail Technologies")

    assert len(out) == 1
    assert "Guardrail Technologies" in out[0]
    assert "Guardrails AI" not in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_keeps_relevant_hit():
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=_rss("Acme Robotics closes $25M Series B"))
    )

    out = await fetch_funding_news("Acme Robotics")
    assert len(out) == 1
    assert "Acme Robotics" in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_never_calls_serper(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SERPER_API_KEY", "test-key")
    serper = respx.post("https://google.serper.dev/news").mock(return_value=httpx.Response(200, json={}))
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=_rss("Acme Robotics closes $25M Series B"))
    )

    assert len(await fetch_funding_news("Acme Robotics")) == 1
    assert not serper.called


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_empty_when_no_hits():
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=_rss())
    )

    assert await fetch_funding_news("Acme Robotics") == []
