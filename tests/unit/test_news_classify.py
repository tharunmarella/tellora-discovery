"""Unit tests for entity-verified news classification (signals/sources/news.py)."""

import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import MagicMock

from signals.sources.news import classify_news


def _router(monkeypatch, results):
    router = MagicMock()
    router.signal_models = ["test"]
    router.complete_text.return_value = json.dumps({"results": results})
    monkeypatch.setattr("signals.sources.news.get_router", lambda: router)
    return router


def _item(title, days_ago=1):
    date = format_datetime(datetime.now(timezone.utc) - timedelta(days=days_ago))
    return {"title": title, "url": f"https://n.example/{abs(hash(title))}", "date": date, "source": "Wire"}


def test_prompt_carries_company_identity(monkeypatch):
    router = _router(monkeypatch, [])
    classify_news("Origin", [_item("Blue Origin wins NASA contract")], domain="origin.com",
                  description="Pet insurance marketplace", industry="Insurance")
    prompt = router.complete_text.call_args[0][0]
    assert "origin.com" in prompt and "Pet insurance marketplace" in prompt


def test_drops_headlines_not_about_company(monkeypatch):
    _router(monkeypatch, [{"i": 0, "about_company": False, "category": "business_news"}])
    assert classify_news("Origin", [_item("Blue Origin wins NASA contract")]) == []


def test_funding_needs_amount_or_round(monkeypatch):
    _router(monkeypatch, [
        {"i": 0, "about_company": True, "category": "funding", "amount": "$40M"},
        {"i": 1, "about_company": True, "category": "funding", "amount": None},
    ])
    events = classify_news("Reframe Systems", [
        _item("Reframe Systems raises $40M"), _item("Reframe Systems secures investment from partner"),
    ])
    assert [e["event_type"] for e in events] == ["funding_round", "news_mention"]
    assert events[0]["payload"]["amount"] == "$40M"


def test_new_event_types(monkeypatch):
    _router(monkeypatch, [
        {"i": 0, "about_company": True, "category": "layoffs"},
        {"i": 1, "about_company": True, "category": "exec_departure"},
    ])
    events = classify_news("Acme", [_item("Acme cuts 10% of staff"), _item("Acme CFO steps down")])
    assert [e["event_type"] for e in events] == ["layoffs", "exec_departure"]


def test_stale_headlines_never_reach_the_model(monkeypatch):
    router = _router(monkeypatch, [])
    assert classify_news("Acme", [_item("Acme raises seed", days_ago=400)]) == []
    router.complete_text.assert_not_called()


def test_same_story_from_several_outlets_is_one_event(monkeypatch):
    _router(monkeypatch, [
        {"i": 0, "about_company": True, "category": "product", "story": "krea 3 launch"},
        {"i": 1, "about_company": True, "category": "product", "story": "Krea 3 launch"},
        {"i": 2, "about_company": True, "category": "product", "story": "krea video model"},
    ])
    events = classify_news("Krea", [
        _item("Krea launched Krea 3 for fast image editing - TestingCatalog"),
        _item("Krea 3 Ultra ranks in the top five for AI image editing - Camera Jabber"),
        _item("Krea ships a new video model - Glitchwire"),
    ])
    assert len(events) == 2
    assert len(events[0]["payload"]["sources"]) == 2
