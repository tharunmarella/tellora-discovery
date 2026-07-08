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
from email.utils import parsedate_to_datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("discovery.signal_diff")

# Tier 1 — alertable when grounded with evidence_url
TIER1_ALERTABLE = frozenset({
    "funding_round",
    "exec_hire",
    "job_change",
    "product_launch",
    "gov_contract",
})

# Tier 2 — context only; feeds heat score, never instant alerts
TIER2_CONTEXT = frozenset({
    "hiring_surge",
    "role_spike",
    "role_first_seen",
    "concept_spike",
    "concept_first_seen",
    "news_mention",
    "tech_first_seen",
    "social_post",
    "competitor_touch",
    "engagement",
})

# Only EDGAR-grounded funding triggers instant alerts (via edgar.push_instant_alerts)
INSTANT_ALERT_TYPES = frozenset({"funding_round"})

EVENT_WEIGHTS = {
    "funding_round": 30,
    "exec_hire": 18,
    "job_change": 15,
    "product_launch": 18,
    "gov_contract": 20,
    "role_spike": 14,
    "hiring_surge": 14,
    "concept_spike": 20,
    "role_first_seen": 18,
    "concept_first_seen": 15,
    "news_mention": 10,
    "tech_first_seen": 10,
    "social_post": 8,
    "engagement": 12,
    "competitor_touch": 14,
}


@dataclass
class SignalEventDraft:
    event_type: str
    title: str
    payload: dict
    source: str
    confidence: float = 1.0
    observed_at: Optional[datetime] = None
    evidence_url: Optional[str] = None
    event_date: Optional[datetime] = None

    def dedupe_key(self, company_id: str) -> str:
        key = self.payload.get("key", self.title[:80])
        return f"{company_id}:{self.event_type}:{key}"


def parse_event_date(value: Any) -> Optional[datetime]:
    """Best-effort parse of event dates from RSS/ISO/email strings."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        if "T" in s and len(s) >= 10:
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, IndexError):
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def draft_from_extra(raw: dict) -> SignalEventDraft:
    """Build a draft from pipeline extra_events dict."""
    payload = raw.get("payload") or {}
    evidence = raw.get("evidence_url") or payload.get("url")
    event_date = parse_event_date(raw.get("event_date") or payload.get("date") or payload.get("filed_at"))
    return SignalEventDraft(
        raw.get("event_type", "social_post"),
        raw.get("title", ""),
        payload,
        raw.get("source", "enrichment"),
        confidence=float(raw.get("confidence", 0.85)),
        evidence_url=evidence,
        event_date=event_date,
    )


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


def diff_snapshots(
    prev: Optional[dict],
    curr: dict,
    *,
    careers_url: Optional[str] = None,
    changelog_url: Optional[str] = None,
) -> list[SignalEventDraft]:
    """Compare two snapshot dicts and return event drafts."""
    if not prev:
        return []

    events: list[SignalEventDraft] = []
    now = datetime.now(timezone.utc)

    pc, cc = prev.get("hiring_count") or 0, curr.get("hiring_count") or 0
    if pc > 0 and cc >= pc * 1.5:
        events.append(SignalEventDraft(
            "hiring_surge",
            f"Open roles jumped {pc} → {cc} (+{(cc - pc) / pc * 100:.0f}%)",
            {"key": "hiring_count", "prev": pc, "curr": cc},
            "job_boards",
            observed_at=now,
            evidence_url=careers_url,
            event_date=now,
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
            evidence_url=careers_url,
            event_date=now,
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
            event_date=now,
        ))

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
                evidence_url=changelog_url,
                event_date=now,
            ))

    return events


def diff_job_posts(
    session: Session,
    company_id: str,
    *,
    careers_url: Optional[str] = None,
) -> list[SignalEventDraft]:
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
                evidence_url=careers_url,
                event_date=now,
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
            evidence_url=careers_url,
            event_date=now,
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
            evidence_url=careers_url,
            event_date=now,
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
        event_date = ev.event_date or observed
        try:
            session.execute(text("""
                INSERT INTO discovery_signal_event
                    (id, company_id, event_type, title, payload, source,
                     observed_at, confidence, dedupe_key, evidence_url, event_date, created_at)
                VALUES
                    (:id, :company_id, :event_type, :title, CAST(:payload AS jsonb),
                     :source, :observed_at, :confidence, :dedupe_key,
                     :evidence_url, :event_date, NOW())
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
                "evidence_url": ev.evidence_url,
                "event_date": event_date,
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
    careers_url = f"https://{domain}/careers" if domain else None
    changelog_url = f"https://{domain}/changelog" if domain else None

    events = diff_snapshots(prev, snap, careers_url=careers_url, changelog_url=changelog_url)
    write_snapshot(session, company_id, snap)

    events.extend(diff_job_posts(session, company_id, careers_url=careers_url))

    for raw in result.get("extra_events") or []:
        events.append(draft_from_extra(raw))

    return insert_events(session, company_id, events)
