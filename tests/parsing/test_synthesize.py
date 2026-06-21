"""Tests for Gemini synthesis with stubbed client."""

import json

from signals.pipeline import CompanySignalResult, synthesize_company_signals


def test_synthesize_company_signals_with_stub(gemini_stub, monkeypatch):
    monkeypatch.setattr("settings.GEMINI_API_KEY", "test-key")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    payload = {
        "company_summary": "Acme builds dev tools.",
        "buying_signals": ["Hiring 5 engineers"],
        "signal_score": 55,
        "funding_stage": None,
        "total_raised": None,
        "investors": [],
        "headcount": 120,
        "hiring_roles": ["Engineer"],
        "tech_stack": ["react"],
        "pricing_model": "self-serve",
        "known_customers": [],
        "recent_launches": [],
        "hq_city": "San Francisco",
        "hq_region": "CA",
        "hq_country": "US",
    }
    gemini_stub(payload)
    result = synthesize_company_signals(
        company_name="Acme",
        homepage_text="We build developer tools for teams.",
        about_text="Founded in 2015.",
        careers_text="We're hiring.",
        tech_stack=["react"],
        job_board={"count": 1, "roles": ["Engineer"], "source": "greenhouse"},
        funding_news=[],
    )
    assert isinstance(result, CompanySignalResult)
    assert result.signal_score == 55
    assert result.company_summary == "Acme builds dev tools."


def test_synthesize_missing_api_key_returns_empty(monkeypatch):
    monkeypatch.setattr("settings.GEMINI_API_KEY", "")
    result = synthesize_company_signals(
        company_name="Acme",
        homepage_text="x",
        about_text="",
        careers_text="",
        tech_stack=[],
        job_board={"count": 0, "roles": [], "source": "none"},
        funding_news=[],
    )
    assert result.signal_score == 0
    assert result.company_summary is None
