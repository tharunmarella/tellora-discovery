"""Unit tests for pure extra_events mappers in signal sources."""

from signals.sources.github import github_extra_events
from signals.sources.gov import gov_extra_events
from signals.sources.hn import hn_extra_events


def test_github_extra_events_from_repos():
    events = github_extra_events({
        "new_repos": [{"name": "sdk", "stars": 10, "created_at": "2026-01-01"}],
        "new_npm_releases": [],
    })
    assert len(events) == 1
    assert events[0]["event_type"] == "product_launch"
    assert "sdk" in events[0]["title"]


def test_github_extra_events_npm():
    events = github_extra_events({
        "new_repos": [],
        "new_npm_releases": [{"name": "@acme/cli", "version": "1.0.0", "date": "2026-01-01"}],
    })
    assert events[0]["source"] == "npm"


def test_hn_extra_events_mentions_and_launches():
    events = hn_extra_events({
        "mentions": [{"title": "Acme in the news", "object_id": "1", "url": "http://x", "points": 20}],
        "launches": [{"title": "Show HN: Acme", "object_id": "2", "url": "http://y", "points": 50}],
    })
    types = {e["event_type"] for e in events}
    assert "news_mention" in types
    assert "product_launch" in types


def test_gov_extra_events():
    events = gov_extra_events([
        {"award_id": "A1", "recipient": "Acme", "amount": 2_500_000, "agency": "NASA", "date": "2026-01-01"},
    ])
    assert len(events) == 1
    assert events[0]["event_type"] == "gov_contract"
    assert "$2.5M" in events[0]["title"]
