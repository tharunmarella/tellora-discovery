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


def _fp_result(*techs: tuple[str, str, list[str]], source: str = "wappalyzer") -> dict:
    return {
        "detected_tech": [{"slug": s, "name": n, "categories": c, "evidence": "script"} for s, n, c in techs],
        "tech_source": source,
    }


def test_diff_tech_adopted_from_fingerprints():
    prev = snapshot_from_result(_fp_result(("react", "React", ["JavaScript frameworks"])))
    curr = snapshot_from_result(_fp_result(
        ("react", "React", ["JavaScript frameworks"]), ("hubspot", "HubSpot", ["Marketing automation"]),
    ))
    events = diff_snapshots(prev, curr)
    assert [e.title for e in events if e.event_type == "tech_first_seen"] == [
        "Started using HubSpot (Marketing automation)"
    ]


def test_diff_tech_dropped_from_fingerprints():
    prev = snapshot_from_result(_fp_result(("intercom", "Intercom", ["Live chat"])))
    curr = snapshot_from_result(_fp_result(("drift", "Drift", ["Live chat"])))
    events = diff_snapshots(prev, curr)
    types = sorted(e.event_type for e in events)
    assert types == ["tech_dropped", "tech_first_seen"]
    assert any(e.title == "Stopped using Intercom (Live chat)" for e in events)


def test_diff_tech_ignores_infra_categories():
    prev = snapshot_from_result(_fp_result(("react", "React", ["JavaScript frameworks"])))
    curr = snapshot_from_result(_fp_result(
        ("react", "React", ["JavaScript frameworks"]), ("cloudflare", "Cloudflare", ["CDN"]),
    ))
    assert diff_snapshots(prev, curr) == []


def test_diff_tech_skips_failed_or_legacy_runs():
    good = snapshot_from_result(_fp_result(("hubspot", "HubSpot", ["CRM"])))
    dns_only = snapshot_from_result(_fp_result(source="wappalyzer_dns"))
    legacy = snapshot_from_result({"tech_stack": ["react"]})
    assert diff_snapshots(good, dns_only) == []  # homepage fetch failed — no "stopped using"
    assert diff_snapshots(legacy, good) == []  # LLM-era snapshot — no mass "started using"


def test_diff_recent_launches_no_longer_emit():
    prev = snapshot_from_result({"recent_launches": ["Old launch"]})
    curr = snapshot_from_result({"recent_launches": ["Old launch", "Investor Overview - Q1 2025"]})
    events = diff_snapshots(prev, curr, changelog_url="https://acme.com/changelog")
    assert not [e for e in events if e.event_type == "product_launch"]


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


class _ClusterSession:
    """Fake session: one stored funding event; records UPDATEs and INSERTs."""

    def __init__(self, existing):
        self.existing = existing
        self.updates, self.inserts = [], []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}

        class _R:
            def __init__(self, rows=None, first=None):
                self._rows, self._first = rows or [], first

            def first(self):
                return self._first

            def mappings(self):
                return self

            def all(self):
                return self._rows

        if "SELECT 1 FROM discovery_signal_event" in sql:
            return _R(first=None)
        if "SELECT id, title, payload" in sql:
            return _R(rows=self.existing if params.get("et") == "funding_round" else [])
        if sql.strip().startswith("UPDATE"):
            self.updates.append(params)
        else:
            self.inserts.append(params)
        return _R()


def test_insert_events_merges_second_outlet_into_existing_event():
    session = _ClusterSession([{
        "id": "ev-1",
        "title": "Reframe Systems raises $40M to scale modular microfactories - HousingWire",
        "payload": {"amount": "$40M", "sources": []},
    }])
    ev = SignalEventDraft(
        "funding_round",
        "Reframe Systems Raises $40M to Industrialize Homebuilding - Business Wire",
        {"key": "https://bw.example/a", "amount": "$40M", "url": "https://bw.example/a"},
        "google_news",
        evidence_url="https://bw.example/a",
    )
    inserted = insert_events(session, "co-1", [ev])
    assert inserted == []
    assert len(session.updates) == 1 and not session.inserts


def test_insert_events_keeps_different_rounds_separate():
    session = _ClusterSession([{
        "id": "ev-1", "title": "Acme raises $10M Series A", "payload": {"amount": "$10M"},
    }])
    ev = SignalEventDraft(
        "funding_round", "Acme raises $40M Series B", {"key": "b", "amount": "$40M"}, "google_news",
    )
    assert insert_events(session, "co-1", [ev]) == ["funding_round"]
    assert not session.updates
