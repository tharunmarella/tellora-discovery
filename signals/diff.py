"""
Signal diff engine — compare enrichment snapshots and emit typed events.

Used by signal_runner.persist_result after each enrichment run.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("discovery.signal_diff")

# High-weight events that trigger instant alerts for watched accounts
INSTANT_ALERT_TYPES = frozenset({"funding_round", "exec_hire"})

EVENT_WEIGHTS = {
    "funding_round": 30,
    "role_first_seen": 18,
    "role_spike": 22,
    "concept_first_seen": 15,
    "concept_spike": 20,
    "tech_first_seen": 10,
    "tech_investment": 14,
    "headcount_jump": 16,
    "hiring_surge": 20,
    "social_post": 8,
    "job_change": 15,
    "exec_hire": 18,
    "engagement": 12,
    "competitor_touch": 14,
    "product_launch": 18,
    "pricing_change": 14,
    "news_mention": 10,
    "gov_contract": 20,
}


@dataclass
class SignalEventDraft:
    event_type: str
    title: str
    payload: dict
    source: str
    confidence: float = 1.0
    observed_at: Optional[datetime] = None

    def dedupe_key(self, company_id: str) -> str:
        key = self.payload.get("key", self.title[:80])
        return f"{company_id}:{self.event_type}:{key}"


def _role_family(title: str) -> str:
    t = (title or "").lower()
    rules = [
        ("account executive", "sales_ae"),
        ("sales", "sales"),
        ("sdr", "sales_sdr"),
        ("solutions engineer", "solutions_eng"),
        ("support", "support"),
        ("security", "security"),
        ("infrastructure", "infra"),
        ("sre", "infra"),
        ("devops", "infra"),
        ("data", "data"),
        ("machine learning", "ml"),
        (" ai", "ml"),
        ("product manager", "product"),
        ("product designer", "design"),
        ("designer", "design"),
        ("marketing", "marketing"),
        ("finance", "finance"),
        ("legal", "legal"),
        ("people", "people_ops"),
        ("recruit", "people_ops"),
        ("engineer", "engineering"),
    ]
    for needle, fam in rules:
        if needle in t:
            return fam
    return "other"


def snapshot_from_result(result: dict) -> dict:
    """Build a snapshot dict from an enrichment result."""
    return {
        "hiring_count": result.get("hiring_count") or 0,
        "hiring_roles": result.get("hiring_roles") or [],
        "tech_stack": result.get("tech_stack") or [],
        "funding_stage": result.get("funding_stage"),
        "total_raised": result.get("total_raised"),
        "headcount": result.get("headcount"),
        "buying_signals": result.get("buying_signals") or [],
        "concepts": result.get("concepts") or [],
        "pricing_model": result.get("pricing_model"),
        "page_fingerprints": result.get("page_fingerprints") or {},
        "recent_launches": result.get("recent_launches") or [],
    }


def diff_snapshots(prev: Optional[dict], curr: dict) -> list[SignalEventDraft]:
    """Compare two snapshot dicts and return event drafts."""
    if not prev:
        return []

    events: list[SignalEventDraft] = []
    now = datetime.now(timezone.utc)

    prev_stage = prev.get("funding_stage")
    curr_stage = curr.get("funding_stage")
    if curr_stage and curr_stage != prev_stage:
        events.append(SignalEventDraft(
            "funding_round",
            f"Funding stage moved {prev_stage or 'unknown'} → {curr_stage}"
            + (f" (total raised {curr['total_raised']})" if curr.get("total_raised") else ""),
            {"key": curr_stage, "prev": prev_stage, "curr": curr_stage},
            "jina_news",
            observed_at=now,
        ))

    pc, cc = prev.get("hiring_count") or 0, curr.get("hiring_count") or 0
    if pc > 0 and cc >= pc * 1.5:
        events.append(SignalEventDraft(
            "hiring_surge",
            f"Open roles jumped {pc} → {cc} (+{(cc - pc) / pc * 100:.0f}%)",
            {"key": "hiring_count", "prev": pc, "curr": cc},
            "job_boards",
            observed_at=now,
        ))

    prev_fams = {_role_family(r) for r in (prev.get("hiring_roles") or [])}
    curr_roles = curr.get("hiring_roles") or []
    curr_fams = {_role_family(r) for r in curr_roles}
    for fam in sorted(curr_fams - prev_fams):
        sample = next((r for r in curr_roles if _role_family(r) == fam), fam)
        events.append(SignalEventDraft(
            "role_first_seen",
            f"First posting in role family '{fam}' (e.g. \"{sample}\")",
            {"key": fam, "sample_title": sample},
            "job_boards",
            confidence=0.9,
            observed_at=now,
        ))

    prev_tech = set(prev.get("tech_stack") or [])
    for tech in sorted(set(curr.get("tech_stack") or []) - prev_tech):
        events.append(SignalEventDraft(
            "tech_first_seen",
            f"Started using {tech}",
            {"key": tech},
            "tech_detect",
            confidence=0.7,
            observed_at=now,
        ))

    ph, ch = prev.get("headcount") or 0, curr.get("headcount") or 0
    if ph > 0 and ch >= ph * 1.2:
        events.append(SignalEventDraft(
            "headcount_jump",
            f"Headcount grew ~{ph} → ~{ch} (+{(ch - ph) / ph * 100:.0f}%)",
            {"key": "headcount", "prev": ph, "curr": ch},
            "apollo_kg",
            observed_at=now,
        ))

    # Pricing page changed (fingerprint diff — both snapshots must have one)
    prev_fp = prev.get("page_fingerprints") or {}
    curr_fp = curr.get("page_fingerprints") or {}
    if prev_fp.get("pricing") and curr_fp.get("pricing") and prev_fp["pricing"] != curr_fp["pricing"]:
        title = "Pricing page changed"
        if curr.get("pricing_model") and curr.get("pricing_model") != prev.get("pricing_model"):
            title += f" (model: {prev.get('pricing_model') or 'unknown'} → {curr['pricing_model']})"
        events.append(SignalEventDraft(
            "pricing_change",
            title,
            {"key": curr_fp["pricing"], "prev_fp": prev_fp["pricing"], "curr_fp": curr_fp["pricing"],
             "pricing_model": curr.get("pricing_model")},
            "website_pages",
            confidence=0.8,
            observed_at=now,
        ))

    # New launches in changelog vs previous snapshot
    prev_launches = {l.strip().lower() for l in (prev.get("recent_launches") or [])}
    for launch in (curr.get("recent_launches") or []):
        if launch.strip().lower() not in prev_launches:
            events.append(SignalEventDraft(
                "product_launch",
                f"Product launch: {launch}",
                {"key": launch.strip().lower()[:80]},
                "website_pages",
                confidence=0.85,
                observed_at=now,
            ))

    return events


def diff_job_posts(session: Session, company_id: str) -> list[SignalEventDraft]:
    """Emit role/concept spike events from discovery_job_post history."""
    now = datetime.now(timezone.utc)
    events: list[SignalEventDraft] = []

    rows = session.execute(text("""
        SELECT role_family, COUNT(*) AS n
        FROM discovery_job_post
        WHERE company_id = :cid AND closed_at IS NULL AND role_family IS NOT NULL
        GROUP BY role_family
    """), {"cid": company_id}).mappings().all()

    for row in rows:
        fam, n = row["role_family"], row["n"]
        if n >= 3:
            events.append(SignalEventDraft(
                "role_spike",
                f"{n} open '{fam}' posts",
                {"key": f"spike:{fam}", "open_posts": n},
                "job_posts",
                confidence=0.85,
                observed_at=now,
            ))

    concept_rows = session.execute(text("""
        SELECT LOWER(c) AS concept, COUNT(*) AS n
        FROM discovery_job_post,
             LATERAL jsonb_array_elements_text(concepts) AS c
        WHERE company_id = :cid
          AND closed_at IS NULL
          AND first_seen_at > NOW() - INTERVAL '30 days'
        GROUP BY LOWER(c)
        HAVING COUNT(*) >= 2
    """), {"cid": company_id}).mappings().all()

    for row in concept_rows:
        events.append(SignalEventDraft(
            "concept_spike",
            f"\"{row['concept']}\" mentioned in {row['n']} job posts (30d)",
            {"key": f"cspike:{row['concept']}", "count": row["n"]},
            "job_posts",
            observed_at=now,
        ))

    first_concepts = session.execute(text("""
        SELECT DISTINCT LOWER(c) AS concept
        FROM discovery_job_post,
             LATERAL jsonb_array_elements_text(concepts) AS c
        WHERE company_id = :cid
          AND first_seen_at > NOW() - INTERVAL '7 days'
          AND closed_at IS NULL
          AND LOWER(c) NOT IN (
              SELECT DISTINCT LOWER(c2)
              FROM discovery_job_post p2,
                   LATERAL jsonb_array_elements_text(p2.concepts) AS c2
              WHERE p2.company_id = :cid
                AND p2.first_seen_at < NOW() - INTERVAL '7 days'
          )
    """), {"cid": company_id}).mappings().all()

    for row in first_concepts:
        events.append(SignalEventDraft(
            "concept_first_seen",
            f"New concept in job posts: \"{row['concept']}\"",
            {"key": f"concept:{row['concept']}"},
            "job_posts",
            confidence=0.8,
            observed_at=now,
        ))

    return events


def _load_latest_snapshot(session: Session, company_id: str) -> Optional[dict]:
    row = session.execute(text("""
        SELECT hiring_count, hiring_roles, tech_stack, funding_stage, total_raised,
               headcount, buying_signals, concepts, pricing_model, page_fingerprints,
               recent_launches
        FROM discovery_company_snapshot
        WHERE company_id = :cid
        ORDER BY captured_at DESC
        LIMIT 1
    """), {"cid": company_id}).mappings().first()
    if not row:
        return None
    return {
        "hiring_count": row["hiring_count"],
        "hiring_roles": row["hiring_roles"] or [],
        "tech_stack": row["tech_stack"] or [],
        "funding_stage": row["funding_stage"],
        "total_raised": row["total_raised"],
        "headcount": row["headcount"],
        "buying_signals": row["buying_signals"] or [],
        "concepts": row["concepts"] or [],
        "pricing_model": row["pricing_model"],
        "page_fingerprints": row["page_fingerprints"] or {},
        "recent_launches": row["recent_launches"] or [],
    }


def write_snapshot(session: Session, company_id: str, snap: dict) -> None:
    session.execute(text("""
        INSERT INTO discovery_company_snapshot
            (id, company_id, captured_at, hiring_count, hiring_roles, tech_stack,
             funding_stage, total_raised, headcount, buying_signals, concepts,
             pricing_model, page_fingerprints, recent_launches)
        VALUES
            (:id, :company_id, NOW(), :hiring_count, CAST(:hiring_roles AS jsonb),
             CAST(:tech_stack AS jsonb), :funding_stage, :total_raised, :headcount,
             CAST(:buying_signals AS jsonb), CAST(:concepts AS jsonb),
             :pricing_model, CAST(:page_fingerprints AS jsonb), CAST(:recent_launches AS jsonb))
    """), {
        "id": str(uuid.uuid4()),
        "company_id": company_id,
        "hiring_count": snap.get("hiring_count"),
        "hiring_roles": json.dumps(snap.get("hiring_roles") or []),
        "tech_stack": json.dumps(snap.get("tech_stack") or []),
        "funding_stage": snap.get("funding_stage"),
        "total_raised": snap.get("total_raised"),
        "headcount": snap.get("headcount"),
        "buying_signals": json.dumps(snap.get("buying_signals") or []),
        "concepts": json.dumps(snap.get("concepts") or []),
        "pricing_model": snap.get("pricing_model"),
        "page_fingerprints": json.dumps(snap.get("page_fingerprints") or {}),
        "recent_launches": json.dumps(snap.get("recent_launches") or []),
    })


def insert_events(
    session: Session,
    company_id: str,
    events: list[SignalEventDraft],
) -> list[str]:
    """Insert events, skip duplicates. Returns event_types inserted."""
    inserted_types: list[str] = []
    for ev in events:
        dedupe = ev.dedupe_key(company_id)
        observed = ev.observed_at or datetime.now(timezone.utc)
        try:
            session.execute(text("""
                INSERT INTO discovery_signal_event
                    (id, company_id, event_type, title, payload, source,
                     observed_at, confidence, dedupe_key, created_at)
                VALUES
                    (:id, :company_id, :event_type, :title, CAST(:payload AS jsonb),
                     :source, :observed_at, :confidence, :dedupe_key, NOW())
                ON CONFLICT (dedupe_key) DO NOTHING
            """), {
                "id": str(uuid.uuid4()),
                "company_id": company_id,
                "event_type": ev.event_type,
                "title": ev.title,
                "payload": json.dumps(ev.payload),
                "source": ev.source,
                "observed_at": observed,
                "confidence": ev.confidence,
                "dedupe_key": dedupe,
            })
            inserted_types.append(ev.event_type)
        except Exception as exc:
            logger.warning(f"Event insert failed for {dedupe}: {exc}")
    return inserted_types


def persist_snapshot_and_events(
    session: Session,
    company_id: str,
    domain: Optional[str],
    result: dict,
) -> list[str]:
    """
    Write snapshot, diff against previous, insert events.
    Returns list of newly inserted event types (for alert routing).
    """
    snap = snapshot_from_result(result)
    prev = _load_latest_snapshot(session, company_id)
    if prev is None:
        baseline = result.get("baseline_fingerprints") or {}
        if baseline.get("pricing"):
            prev = {
                "hiring_count": 0,
                "hiring_roles": [],
                "tech_stack": [],
                "funding_stage": None,
                "total_raised": None,
                "headcount": None,
                "buying_signals": [],
                "concepts": [],
                "pricing_model": None,
                "page_fingerprints": {"pricing": baseline["pricing"], "changelog": ""},
                "recent_launches": [],
            }
    events = diff_snapshots(prev, snap)
    write_snapshot(session, company_id, snap)

    events.extend(diff_job_posts(session, company_id))

    for raw in result.get("extra_events") or []:
        events.append(SignalEventDraft(
            raw.get("event_type", "social_post"),
            raw.get("title", ""),
            raw.get("payload") or {},
            raw.get("source", "enrichment"),
            confidence=float(raw.get("confidence", 0.85)),
        ))

    inserted = insert_events(session, company_id, events)

    if domain and any(t in INSTANT_ALERT_TYPES for t in inserted):
        try:
            import settings as cfg
            import redis as _redis
            r = _redis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
            alert_key = getattr(cfg, "SIGNALS_ALERT_KEY", "tellora:signals_alert")
            r.rpush(alert_key, domain)
        except Exception as exc:
            logger.warning(f"Could not push instant alert for {domain}: {exc}")

    return inserted
