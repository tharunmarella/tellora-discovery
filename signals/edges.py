"""
Relationship edges between discovery entities — multi-hop queries in Postgres.

Entity types:
  company  : discovery_company.id
  person   : normalized name slug (display name kept in payload)
  vendor   : tech key e.g. 'stripe', 'google_workspace'
  customer : normalized company name (matched discovery_company id in payload)

Edge types:
  uses_vendor  : company → vendor   (from tech_stack)
  has_customer : company → customer (from known_customers)
  employs_exec : company → person   (from Form D related_persons)
  works_at     : person  → company  (from LinkedIn extension job changes)
  left_company : person  → company  (previous employer on a job change)
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("discovery.edges")


def entity_slug(name: str) -> str:
    """Normalize a person/company display name to a stable entity key."""
    n = (name or "").lower().strip()
    n = re.sub(r"[^a-z0-9]+", "-", n).strip("-")
    return n


@dataclass
class EdgeDraft:
    src_type: str
    src_id: str
    edge_type: str
    dst_type: str
    dst_id: str
    source: str
    payload: dict = field(default_factory=dict)


def upsert_edges(session: Session, edges: list[EdgeDraft]) -> int:
    """Insert edges, refreshing last_seen_at on conflict. Returns count written."""
    written = 0
    for e in edges:
        if not (e.src_id and e.dst_id):
            continue
        try:
            session.execute(text("""
                INSERT INTO discovery_edge
                    (id, src_type, src_id, edge_type, dst_type, dst_id,
                     payload, source, observed_at, last_seen_at, created_at)
                VALUES
                    (:id, :src_type, :src_id, :edge_type, :dst_type, :dst_id,
                     CAST(:payload AS jsonb), :source, NOW(), NOW(), NOW())
                ON CONFLICT (src_type, src_id, edge_type, dst_type, dst_id)
                DO UPDATE SET last_seen_at = NOW()
            """), {
                "id": str(uuid.uuid4()),
                "src_type": e.src_type,
                "src_id": e.src_id,
                "edge_type": e.edge_type,
                "dst_type": e.dst_type,
                "dst_id": e.dst_id,
                "payload": json.dumps(e.payload or {}),
                "source": e.source,
            })
            written += 1
        except Exception as exc:
            logger.warning(f"Edge upsert failed {e.edge_type} {e.src_id}→{e.dst_id}: {exc}")
    return written


# ── Builders ────────────────────────────────────────────────────────────────

def edges_from_enrichment(company_id: str, result: dict) -> list[EdgeDraft]:
    """company→vendor and company→customer edges from an enrichment result."""
    edges: list[EdgeDraft] = []

    for vendor in (result.get("tech_stack") or [])[:40]:
        key = entity_slug(vendor)
        if key:
            edges.append(EdgeDraft(
                "company", company_id, "uses_vendor", "vendor", key,
                source="signal_enrichment", payload={"display": vendor},
            ))

    for customer in (result.get("known_customers") or [])[:25]:
        key = entity_slug(customer)
        if len(key) >= 3:
            edges.append(EdgeDraft(
                "company", company_id, "has_customer", "customer", key,
                source="signal_enrichment", payload={"display": customer},
            ))

    return edges


def edges_from_filing(company_id: str, filing: dict) -> list[EdgeDraft]:
    """company→person exec edges from a parsed Form D filing."""
    edges: list[EdgeDraft] = []
    for person in (filing.get("related_persons") or [])[:8]:
        name = person.get("name") or ""
        key = entity_slug(name)
        if len(key) < 3:
            continue
        edges.append(EdgeDraft(
            "company", company_id, "employs_exec", "person", key,
            source="sec_edgar",
            payload={
                "display": name,
                "relationship": person.get("relationship"),
                "accession_no": filing.get("accession_no"),
            },
        ))
    return edges


def edges_from_job_change(
    person_name: str,
    contact_id: Optional[str],
    new_company_id: Optional[str],
    prev_company_id: Optional[str],
    job_title: Optional[str] = None,
) -> list[EdgeDraft]:
    """person→company edges from a LinkedIn extension job change."""
    key = entity_slug(person_name)
    if len(key) < 3:
        return []
    payload = {"display": person_name, "contact_id": contact_id, "job_title": job_title}
    edges: list[EdgeDraft] = []
    if new_company_id:
        edges.append(EdgeDraft(
            "person", key, "works_at", "company", new_company_id,
            source="linkedin_extension", payload=payload,
        ))
    if prev_company_id and prev_company_id != new_company_id:
        edges.append(EdgeDraft(
            "person", key, "left_company", "company", prev_company_id,
            source="linkedin_extension", payload=payload,
        ))
    return edges
