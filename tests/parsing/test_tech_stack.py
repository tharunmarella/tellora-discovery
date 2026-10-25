"""HTTP-mocked tests for tech stack detection (fingerprints + regex fallback)."""

import httpx
import pytest
import respx

from signals.techdetect import Fingerprints

_FP = Fingerprints(
    {
        "Stripe": {"cats": [1], "scriptSrc": "js\\.stripe\\.com"},
        "HubSpot": {"cats": [2], "dns": {"TXT": ["hubspot-developer-verification"]}},
    },
    {"1": {"name": "Payment processors"}, "2": {"name": "Marketing automation"}},
)


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    from signals import pipeline

    pipeline._PAGE_CACHE._data.clear()
    monkeypatch.setattr("signals.techdetect.resolve_dns_records", lambda d: {})
    monkeypatch.setattr("signals.techdetect.load_fingerprints", lambda **kw: _FP)
    yield
    pipeline._PAGE_CACHE._data.clear()


@pytest.mark.asyncio
@respx.mock
async def test_detect_tech_stack_finds_stripe():
    from signals.pipeline import detect_tech_stack

    respx.get(url__regex=r"https://acme\.com.*").mock(
        return_value=httpx.Response(200, text='<script src="https://js.stripe.com/v3/"></script>')
    )
    assert "stripe" in await detect_tech_stack("acme.com")


@pytest.mark.asyncio
@respx.mock
async def test_detect_technologies_reports_details_and_dns(monkeypatch):
    from signals.pipeline import detect_technologies

    monkeypatch.setattr(
        "signals.techdetect.resolve_dns_records", lambda d: {"TXT": ["hubspot-developer-verification=1"]}
    )
    respx.get(url__regex=r"https://acme\.com.*").mock(return_value=httpx.Response(200, text="<p>hi</p>"))
    info = await detect_technologies("acme.com")
    assert info["source"] == "wappalyzer"
    assert info["details"] == [
        {"slug": "hubspot", "name": "HubSpot", "categories": ["Marketing automation"], "evidence": "dns:TXT"}
    ]


@pytest.mark.asyncio
@respx.mock
async def test_detect_tech_stack_fetch_error_returns_empty():
    from signals.pipeline import detect_technologies

    respx.get(url__regex=r"https://acme\.com.*").mock(return_value=httpx.Response(500))
    info = await detect_technologies("acme.com")
    assert info == {"slugs": [], "details": [], "source": "none"}


@pytest.mark.asyncio
@respx.mock
async def test_regex_fallback_when_fingerprints_unavailable(monkeypatch):
    from signals.pipeline import detect_technologies

    monkeypatch.setattr("signals.techdetect.load_fingerprints", lambda **kw: None)
    respx.get(url__regex=r"https://acme\.com.*").mock(
        return_value=httpx.Response(200, text='<script src="https://js.stripe.com/v3/"></script>')
    )
    info = await detect_technologies("acme.com")
    assert info["source"] == "regex" and "stripe" in info["slugs"]
