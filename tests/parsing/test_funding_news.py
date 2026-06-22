"""HTTP-mocked tests for funding-news relevance filtering."""

import httpx
import pytest
import respx

from signals.pipeline import fetch_funding_news


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_drops_same_name_other_company(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "JINA_API_KEY", "test-key")
    payload = {
        "data": [
            {
                "title": "Guardrails AI raises $7.5M seed round",
                "description": "Guardrails AI, an LLM validation startup, raised $7.5M.",
                "url": "https://techcrunch.com/guardrails-ai-seed",
            },
            {
                "title": "Guardrail Technologies lands $3M to expand platform",
                "description": "Guardrail Technologies raised $3M to grow its safety platform.",
                "url": "https://example.com/guardrail-technologies",
            },
        ]
    }
    respx.get(host="s.jina.ai").mock(return_value=httpx.Response(200, json=payload))

    out = await fetch_funding_news("Guardrail Technologies")

    assert len(out) == 1
    assert "Guardrail Technologies" in out[0]
    assert "Guardrails AI" not in out[0]


@pytest.mark.asyncio
@respx.mock
async def test_funding_news_keeps_relevant_hit(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "JINA_API_KEY", "test-key")
    payload = {
        "data": [
            {
                "title": "Acme Robotics closes $25M Series B",
                "description": "Acme Robotics announced a $25M Series B round.",
                "url": "https://news.example/acme-series-b",
            }
        ]
    }
    respx.get(host="s.jina.ai").mock(return_value=httpx.Response(200, json=payload))

    out = await fetch_funding_news("Acme Robotics")
    assert len(out) == 1
    assert "Acme Robotics" in out[0]


@pytest.mark.asyncio
async def test_funding_news_empty_without_key(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "JINA_API_KEY", "")
    assert await fetch_funding_news("Acme Robotics") == []
