"""Integration tests for watched refresh candidate selection."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

import settings as cfg
from signals.monitoring import (
    load_watched_stale_candidates,
    select_watched_refresh_candidates,
)

pytestmark = pytest.mark.integration


def _watch_company(
    db_session,
    *,
    org_id: str,
    name: str,
    domain: str,
    enriched_at: datetime | None,
) -> str:
    discovery_id = str(uuid.uuid4())
    db_session.execute(
        text("""
            INSERT INTO discovery_company (
                id, apollo_org_name, name, domain, domain_resolved,
                enrichment_status, signal_enrichment_status,
                signal_enriched_at, first_seen_at, last_seen_at, created_at, updated_at
            ) VALUES (
                :id, :name, :name, :domain, true,
                'enriched', 'enriched',
                :enriched_at, NOW(), NOW(), NOW(), NOW()
            )
        """),
        {"id": discovery_id, "name": name, "domain": domain, "enriched_at": enriched_at},
    )
    db_session.execute(
        text("""
            INSERT INTO company (
                id, org_id, name, domain, discovery_company_id, watch_source, watched_at
            ) VALUES (
                :id, :org_id, :name, :domain, :discovery_company_id, 'manual', NOW()
            )
        """),
        {
            "id": str(uuid.uuid4()),
            "org_id": org_id,
            "name": name,
            "domain": domain,
            "discovery_company_id": discovery_id,
        },
    )
    db_session.flush()
    return discovery_id


def test_stale_hours_boundary(db_session, monkeypatch):
    monkeypatch.setattr(cfg, "WATCHED_STALE_HOURS", 20)
    monkeypatch.setattr(cfg, "WATCHED_ORG_DAILY_BUDGET", 200)

    now = datetime.now(timezone.utc)
    fresh_id = _watch_company(
        db_session,
        org_id="org-a",
        name="Fresh Co",
        domain="fresh.com",
        enriched_at=now - timedelta(hours=19),
    )
    stale_id = _watch_company(
        db_session,
        org_id="org-a",
        name="Stale Co",
        domain="stale.com",
        enriched_at=now - timedelta(hours=21),
    )
    db_session.commit()

    candidates = load_watched_stale_candidates(db_session)
    ids = {str(c["id"]) for c in candidates}

    assert fresh_id not in ids
    assert stale_id in ids


def test_per_org_budget_and_round_robin(db_session, monkeypatch):
    monkeypatch.setattr(cfg, "WATCHED_STALE_HOURS", 20)
    monkeypatch.setattr(cfg, "WATCHED_ORG_DAILY_BUDGET", 5)
    monkeypatch.setattr(cfg, "WATCHED_REFRESH_GLOBAL_CAP", 100)

    now = datetime.now(timezone.utc)
    stale_at = now - timedelta(hours=30)

    for i in range(7):
        _watch_company(
            db_session,
            org_id="org-big",
            name=f"Big {i}",
            domain=f"big{i}.com",
            enriched_at=stale_at - timedelta(minutes=i),
        )

    small_ids = []
    for i in range(3):
        small_ids.append(
            _watch_company(
                db_session,
                org_id="org-small",
                name=f"Small {i}",
                domain=f"small{i}.com",
                enriched_at=stale_at - timedelta(minutes=i),
            )
        )

    shared_id = _watch_company(
        db_session,
        org_id="org-big",
        name="Shared Co",
        domain="shared.com",
        enriched_at=stale_at - timedelta(days=1),
    )
    db_session.execute(
        text("""
            INSERT INTO company (
                id, org_id, name, domain, discovery_company_id, watch_source, watched_at
            ) VALUES (
                :id, :org_id, :name, :domain, :discovery_company_id, 'manual', NOW()
            )
        """),
        {
            "id": str(uuid.uuid4()),
            "org_id": "org-small",
            "name": "Shared Co",
            "domain": "shared.com",
            "discovery_company_id": shared_id,
        },
    )
    db_session.commit()

    candidates = load_watched_stale_candidates(db_session)
    selected, org_stats = select_watched_refresh_candidates(
        candidates,
        global_cap=cfg.WATCHED_REFRESH_GLOBAL_CAP,
    )
    selected_ids = {str(r["id"]) for r in selected}

    assert org_stats.get("org-small", 0) == 3
    assert org_stats.get("org-big", 0) <= 5
    assert len(selected_ids) == org_stats.get("org-small", 0) + org_stats.get("org-big", 0)
    assert shared_id in selected_ids
    assert all(sid in selected_ids for sid in small_ids)
