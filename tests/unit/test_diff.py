"""Unit tests for signals/diff.py."""

from signals.diff import (
    SignalEventDraft,
    _role_family,
    diff_snapshots,
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


def test_diff_funding_round():
    prev = snapshot_from_result({"funding_stage": "Seed"})
    curr = snapshot_from_result({"funding_stage": "Series A", "total_raised": "$10M"})
    events = diff_snapshots(prev, curr)
    assert len(events) == 1
    assert events[0].event_type == "funding_round"


def test_diff_hiring_surge():
    prev = snapshot_from_result({"hiring_count": 10})
    curr = snapshot_from_result({"hiring_count": 16})
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "hiring_surge" for e in events)


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


def test_diff_headcount_jump():
    prev = snapshot_from_result({"headcount": 100})
    curr = snapshot_from_result({"headcount": 130})
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "headcount_jump" for e in events)


def test_diff_pricing_change():
    prev = snapshot_from_result({
        "page_fingerprints": {"pricing": "oldfp"},
        "pricing_model": "enterprise",
    })
    curr = snapshot_from_result({
        "page_fingerprints": {"pricing": "newfp"},
        "pricing_model": "self-serve",
    })
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "pricing_change" for e in events)


def test_diff_product_launch():
    prev = snapshot_from_result({"recent_launches": ["Old launch"]})
    curr = snapshot_from_result({"recent_launches": ["Old launch", "New Feature GA"]})
    events = diff_snapshots(prev, curr)
    assert any(e.event_type == "product_launch" for e in events)


def test_dedupe_key():
    ev = SignalEventDraft("funding_round", "Raised Series A", {"key": "series-a"}, "jina_news")
    assert ev.dedupe_key("company-1") == "company-1:funding_round:series-a"
