"""Integration tests for diff persistence and job-post diffing."""

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from signals.diff import diff_job_posts, persist_snapshot_and_events, snapshot_from_result


pytestmark = pytest.mark.integration


def test_persist_snapshot_and_events_writes_snapshot_and_event(db_session, company_factory):
    company_id = company_factory(status="enriched")
    result = {
        "hiring_count": 5,
        "hiring_roles": ["Engineer"],
        "tech_stack": ["stripe"],
        "funding_stage": "Series A",
        "total_raised": "$10M",
        "headcount": 100,
        "buying_signals": ["Hiring surge"],
        "concepts": [],
        "pricing_model": "enterprise",
        "page_fingerprints": {"pricing": "fp1"},
        "recent_launches": [],
        "extra_events": [],
    }
    db_session.execute(
        text("""
            INSERT INTO discovery_company_snapshot
                (id, company_id, captured_at, hiring_count, hiring_roles, tech_stack,
                 funding_stage, total_raised, headcount, buying_signals, concepts,
                 pricing_model, page_fingerprints, recent_launches)
            VALUES
                (:id, :company_id, NOW() - INTERVAL '1 day', 2, '[]'::jsonb, '[]'::jsonb,
                 'Seed', NULL, 80, '[]'::jsonb, '[]'::jsonb,
                 'enterprise', '{"pricing":"old"}'::jsonb, '[]'::jsonb)
        """),
        {"id": str(uuid.uuid4()), "company_id": company_id},
    )
    db_session.flush()

    inserted = persist_snapshot_and_events(db_session, company_id, "acme.com", result)
    db_session.flush()

    snap_count = db_session.execute(
        text("SELECT COUNT(*) FROM discovery_company_snapshot WHERE company_id = :cid"),
        {"cid": company_id},
    ).scalar()
    assert snap_count == 2
    assert "hiring_surge" in inserted

    row = db_session.execute(
        text("""
            SELECT evidence_url FROM discovery_signal_event
            WHERE company_id = :cid AND event_type = 'hiring_surge'
            LIMIT 1
        """),
        {"cid": company_id},
    ).first()
    assert row is not None
    assert row[0] == "https://acme.com/careers"


def test_persist_snapshot_dedupes_events(db_session, company_factory):
    company_id = company_factory(status="enriched")
    result = {
        "hiring_count": 0,
        "hiring_roles": [],
        "tech_stack": [],
        "buying_signals": [],
        "concepts": [],
        "page_fingerprints": {},
        "recent_launches": ["Launch A"],
        "extra_events": [{
            "event_type": "product_launch",
            "title": "Launch A",
            "payload": {"key": "launch-a", "url": "https://acme.com/launch-a"},
            "source": "test",
            "confidence": 0.9,
            "evidence_url": "https://acme.com/launch-a",
            "event_date": "2025-06-01",
        }],
    }
    persist_snapshot_and_events(db_session, company_id, None, result)
    db_session.flush()
    persist_snapshot_and_events(db_session, company_id, None, result)
    db_session.flush()

    count = db_session.execute(
        text("SELECT COUNT(*) FROM discovery_signal_event WHERE company_id = :cid"),
        {"cid": company_id},
    ).scalar()
    assert count == 1


def test_diff_job_posts_role_spike(db_session, company_factory):
    company_id = company_factory(status="enriched")
    now = datetime.now(timezone.utc)
    for i in range(3):
        db_session.execute(
            text("""
                INSERT INTO discovery_job_post
                    (id, company_id, external_id, title, role_family, concepts, source,
                     first_seen_at, last_seen_at)
                VALUES
                    (:id, :cid, :ext, :title, 'engineering', '[]'::jsonb, 'greenhouse',
                     :ts, :ts)
            """),
            {
                "id": str(uuid.uuid4()),
                "cid": company_id,
                "ext": f"eng-{i}",
                "title": f"Engineer {i}",
                "ts": now,
            },
        )
    db_session.flush()
    events = diff_job_posts(db_session, company_id)
    assert any(e.event_type == "role_spike" for e in events)


def test_diff_job_posts_concept_spike(db_session, company_factory):
    company_id = company_factory(status="enriched")
    now = datetime.now(timezone.utc)
    for i in range(2):
        db_session.execute(
            text("""
                INSERT INTO discovery_job_post
                    (id, company_id, external_id, title, role_family, concepts, source,
                     first_seen_at, last_seen_at)
                VALUES
                    (:id, :cid, :ext, :title, 'engineering', :concepts, 'greenhouse',
                     :ts, :ts)
            """),
            {
                "id": str(uuid.uuid4()),
                "cid": company_id,
                "ext": f"c-{i}",
                "title": f"ML Engineer {i}",
                "concepts": json.dumps(["kubernetes"]),
                "ts": now - timedelta(days=5),
            },
        )
    db_session.flush()
    events = diff_job_posts(db_session, company_id)
    assert any(e.event_type == "concept_spike" for e in events)
