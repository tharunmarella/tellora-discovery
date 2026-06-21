"""HTTP-mocked tests for Serper KG parsing."""

import httpx
import pytest
import respx

from signals.pipeline import fetch_serper_kg


@pytest.mark.asyncio
@respx.mock
async def test_fetch_serper_kg_parses_knowledge_graph(monkeypatch):
    monkeypatch.setattr("settings.SERPER_API_KEY", "test-key")
    respx.post("https://google.serper.dev/search").mock(
        return_value=httpx.Response(200, json={
            "knowledgeGraph": {
                "description": "Dev tools company",
                "attributes": {
                    "Founded": "2015",
                    "Headquarters": "San Francisco, CA",
                    "CEO": "Jane Doe",
                    "Number of employees": "201-500 employees",
                },
            },
            "organic": [
                {
                    "link": "https://linkedin.com/company/acme",
                    "snippet": "Acme · 51-200 employees",
                }
            ],
        })
    )
    result = await fetch_serper_kg("Acme")
    assert result.get("kg_description") == "Dev tools company"
    assert result.get("ceo") == "Jane Doe"
    assert result.get("headcount") == 500


@pytest.mark.asyncio
@respx.mock
async def test_fetch_serper_kg_failure_returns_empty(monkeypatch):
    monkeypatch.setattr("settings.SERPER_API_KEY", "test-key")
    respx.post("https://google.serper.dev/search").mock(return_value=httpx.Response(500))
    assert await fetch_serper_kg("Acme") == {}
