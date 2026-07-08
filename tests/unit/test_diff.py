"""Unit tests for signals/diff.py."""

from datetime import datetime, timezone

from signals.diff import (
    TIER1_ALERTABLE,
    TIER2_CONTEXT,
    SignalEventDraft,
    _role_family,
    diff_snapshots,
    draft_from_extra,
    insert_events,
    snapshot_from_result,
)


def test_role_family_engineering():
    assert _role_family("Senior Software Engineer") == "engineering"


def test_role_family_sales_ae():
    assert _role_family("Account Executive") == "sales_ae"


def test_snapshot_from_result_maps_fields():
    snap = snapshot_from_result({
        "hiring_count": 5,
        "hiring_roles": ["Engineer"],
        "tech_stack": ["stripe"],
        "funding_stage": "Series A",
        "pricing_model": "enterprise",
        "page_fingerprints": {"pricing": "abc"},
        "recent_launches": ["Launch 1"],
    })
    assert snap["hiring_count"] == 5
    assert snap["pricing_model"] == "enterprise"


def test_diff_snapshots_no_prev_returns_empty():
    assert diff_snapshots(None, {"hiring_count": 10}) == []


def test_diff_funding_stage_no_longer_emits():
    """LLM funding-stage diff was removed — EDGAR/news are the only funding sources."""
    prev = snapshot_from_result({"funding_stage": "Seed"})
    curr = snapshot_from_result({"funding_stage": "Series A", "total_raised": "$10M"})
    events = diff_snapshots(prev, curr)
    assert not any(e.event_type == "funding_round" for e in events)


def test_diff_headcount_no_longer_emits():
    prev = snapshot_from_result({"headcount": 100})
    curr = snapshot_from_result({"headcount": 130})
    events = diff_snapshots(prev, curr)
    assert not any(e.event_type == "headcount_jump" for e in events)


def test_diff_pricing_no_longer_emits():
    prev = snapshot_from_result({
        "page_fingerprints": {"pricing": "oldfp"},
        "pricing_model": "enterprise",
    })
    curr = snapshot_from_result({
        "page_fingerprints": {"pricing": "newfp"},
        "pricing_model": "self-serve",
    })
    events = diff_snapshots(prev, curr)
    assert not any(e.event_type == "pricing_change" for e in events)


def test_diff_hiring_surge():
    prev = snapshot_from_result({"hiring_count": 10})
    curr = snapshot_from_result({"hiring_count": 16})
    events = diff_snapshots(prev, curr, careers_url="https://acme.com/careers")
    surge = [e for e in events if e.event_type == "hiring_surge"]
    assert surge
    assert surge[0].evidence_url == "https://acme.com/careers"


def test_diff_role_first_seen():
    prev = snapshot_from_result({"hiring_roles": ["Software Engineer"]})
    curr = snapshot_from_result({"hiring_roles": ["Software Engineer", "Account Executive"]})
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "role_first_seen" for e in events)


def test_diff_tech_first_seen():
    prev = snapshot_from_result({"tech_stack": ["react"]})
    curr = snapshot_from_result({"tech_stack": ["react", "stripe"]})
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "tech_first_seen" and "stripe" in e.title for e in events)


def test_diff_product_launch():
    prev = snapshot_from_result({"recent_launches": ["Old launch"]})
    curr = snapshot_from_result({"recent_launches": ["Old launch", "New Feature GA"]})
    events = diff_snapshots(prev, curr, changelog_url="https://acme.com/changelog")
    launches = [e for e in events if e.event_type == "product_launch"]
    assert launches
    assert launches[0].evidence_url == "https://acme.com/changelog"


def test_dedupe_key():
    ev = SignalEventDraft("funding_round", "Raised Series A", {"key": "series-a"}, "jina_news")
    assert ev.dedupe_key("company-1") == "company-1:funding_round:series-a"


def test_draft_from_extra_picks_evidence():
    draft = draft_from_extra({
        "event_type": "gov_contract",
        "title": "Contract award",
        "payload": {"key": "award:1", "url": "https://www.usaspending.gov/award/abc"},
        "source": "usaspending",
        "event_date": "2025-06-01",
    })
    assert draft.evidence_url == "https://www.usaspending.gov/award/abc"
    assert draft.event_date == datetime(2025, 6, 1, tzinfo=timezone.utc)


def test_tier_sets_disjoint():
    assert not (TIER1_ALERTABLE & TIER2_CONTEXT)


def test_insert_events_persists_evidence(monkeypatch):
    captured = []

    class FakeSession:
        def execute(self, stmt, params):
            captured.append(params)

    ev = SignalEventDraft(
        "gov_contract",
        "Federal contract",
        {"key": "award:1"},
        "usaspending",
        evidence_url="https://www.usaspending.gov/award/abc",
        event_date=datetime(2025, 1, 15, tzinfo=timezone.utc),
    )
    insert_events(FakeSession(), "co-1", [ev])
    assert len(captured) == 1
    assert captured[0]["evidence_url"] == "https://www.usaspending.gov/award/abc"
    assert captured[0]["event_date"] == datetime(2025, 1, 15, tzinfo=timezone.utc)
