"""Integration tests for has_customer resolution (signals.edges.resolve_customer_edges)."""

import json

import pytest
from sqlalchemy import text

from signals.edges import EdgeDraft, edges_from_enrichment, resolve_customer_edges, upsert_edges


pytestmark = pytest.mark.integration


def _dst_company(db_session, src_id: str, dst_id: str):
    return db_session.execute(
        text("""
            SELECT dst_company_id FROM discovery_edge
            WHERE src_id = :s AND edge_type = 'has_customer' AND dst_id = :d
        """),
        {"s": src_id, "d": dst_id},
    ).scalar()


def _customer_edge(vendor_id: str, slug: str, payload: dict) -> EdgeDraft:
    return EdgeDraft("company", vendor_id, "has_customer", "customer", slug,
                     source="customer_crawl", payload=payload)


def test_resolve_by_domain_then_name(db_session, company_factory):
    vendor = company_factory(name="Attio", domain="attio.com")
    granola = company_factory(name="Granola", domain="granola.so")
    zentrix = company_factory(name="Zentrix Robotics", domain="zentrixrobotics.com")

    upsert_edges(db_session, edges_from_enrichment(vendor, {
        "known_customers": ["Zentrix Robotics", "Totally Unknown Widgets"],
    }))
    upsert_edges(db_session, [
        # display name differs from the tracked name; the domain decides
        _customer_edge(vendor, "granola-labs", {"display": "Granola Labs", "domain": "Granola.so"}),
        # ignored edges are never resolved
        _customer_edge(vendor, "zentrix", {"display": "Zentrix Robotics", "ignored_reason": "broad_vendor"}),
        # a vendor listing itself is not a customer edge
        _customer_edge(vendor, "attio", {"display": "Attio", "domain": "attio.com"}),
    ])
    db_session.commit()

    stats = resolve_customer_edges(db_session)
    db_session.commit()

    assert stats == {"unresolved": 4, "by_domain": 1, "by_name": 1}
    assert _dst_company(db_session, vendor, "granola-labs") == granola
    assert _dst_company(db_session, vendor, "zentrix-robotics") == zentrix
    assert _dst_company(db_session, vendor, "totally-unknown-widgets") is None
    assert _dst_company(db_session, vendor, "zentrix") is None
    assert _dst_company(db_session, vendor, "attio") is None

    again = resolve_customer_edges(db_session)
    assert again == {"unresolved": 2, "by_domain": 0, "by_name": 0}


def test_reupsert_keeps_resolved_company(db_session, company_factory):
    vendor = company_factory(name="Attio", domain="attio.com")
    customer = company_factory(name="Granola", domain="granola.so")

    upsert_edges(db_session, [EdgeDraft(
        "company", vendor, "has_customer", "customer", "granola",
        source="customer_crawl", payload={"display": "Granola"}, dst_company_id=customer,
    )])
    # a later enrichment sighting carries no resolution
    upsert_edges(db_session, edges_from_enrichment(vendor, {"known_customers": ["Granola"]}))
    db_session.commit()

    assert _dst_company(db_session, vendor, "granola") == customer
    payload = db_session.execute(
        text("SELECT payload FROM discovery_edge WHERE src_id = :s AND dst_id = 'granola'"), {"s": vendor},
    ).scalar()
    assert (payload if isinstance(payload, dict) else json.loads(payload))["display"] == "Granola"

