"""Unit tests for watched refresh fair scheduling."""

from __future__ import annotations

from signals.monitoring import select_watched_refresh_candidates


def _candidate(company_id: str, org_id: str, rank: int, name: str | None = None) -> dict:
    return {
        "id": company_id,
        "name": name or company_id,
        "domain": f"{company_id}.com",
        "description": None,
        "industry": None,
        "raw_meta": None,
        "headcount": None,
        "headquarters": None,
        "org_id": org_id,
        "org_rank": rank,
    }


def test_round_robin_interleaves_orgs_before_depth():
    candidates = [
        _candidate("a1", "org-big", 1),
        _candidate("a2", "org-big", 2),
        _candidate("b1", "org-small", 1),
        _candidate("b2", "org-small", 2),
        _candidate("b3", "org-small", 3),
    ]
    selected, org_stats = select_watched_refresh_candidates(candidates, global_cap=10)
    ids = [str(r["id"]) for r in selected]

    assert ids.index("b1") < ids.index("a2")
    assert ids.index("a1") < ids.index("a2")
    assert org_stats["org-small"] == 3
    assert org_stats["org-big"] == 2


def test_shared_company_deduped_once():
    candidates = [
        _candidate("shared", "org-a", 1),
        _candidate("shared", "org-b", 3),
        _candidate("only-b", "org-b", 1),
    ]
    selected, org_stats = select_watched_refresh_candidates(candidates, global_cap=10)
    ids = [str(r["id"]) for r in selected]

    assert ids.count("shared") == 1
    assert set(ids) == {"shared", "only-b"}
    assert org_stats["org-a"] == 1
    assert org_stats["org-b"] == 1


def test_global_cap_truncates_after_fair_interleave():
    candidates = [
        _candidate(f"big-{i}", "org-big", i) for i in range(1, 6)
    ] + [
        _candidate(f"small-{i}", "org-small", i) for i in range(1, 4)
    ]
    selected, org_stats = select_watched_refresh_candidates(candidates, global_cap=4)
    ids = [str(r["id"]) for r in selected]

    assert len(ids) == 4
    assert "big-1" in ids
    assert "small-1" in ids
    assert "big-2" in ids
    assert "small-2" in ids
    assert org_stats["org-small"] == 2
    assert org_stats["org-big"] == 2


def test_empty_candidates():
    selected, org_stats = select_watched_refresh_candidates([], global_cap=100)
    assert selected == []
    assert org_stats == {}
