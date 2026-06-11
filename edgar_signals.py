"""
SEC EDGAR Form D poller — funding rounds weeks before press coverage.

Daily cron pipeline:
  1. Fetch the Form D filing index from EDGAR's free full-text search API.
  2. Fetch + parse each filing's primary_doc.xml: offering amount, amount sold,
     industry group, revenue range, executive officers.
  3. Persist tech-relevant filings to discovery_filing (even unmatched ones).
  4. Matched filers → enriched funding_round events + instant alerts.
  5. Unmatched tech filers → auto-created discovery_company rows
     (source=sec_edgar) so newly funded startups enter the index the day
     they file, before any press exists.

SEC fair-access policy: identify via User-Agent, stay well under 10 req/s.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg

logger = logging.getLogger("discovery.edgar")

_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{doc}"

# Form D industryGroupType values we consider sellable-tech-relevant.
# Excludes the bulk of Form Ds: pooled investment funds, real estate, energy.
TECH_INDUSTRY_GROUPS = frozenset({
    "computers",
    "telecommunications",
    "other technology",
    "business services",
    "biotechnology",
    "pharmaceuticals",
    "other health care",
})

# Max new discovery_company rows auto-created per poll (caps Serper/Gemini cost)
AUTO_CREATE_CAP = int(getattr(cfg, "EDGAR_AUTO_CREATE_CAP", 25) or 25)

_CORP_SUFFIXES = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|company|"
    r"holdings|technologies|technology|labs|group)\b\.?",
    re.IGNORECASE,
)


def normalize_name(name: str) -> str:
    """Normalize a company name for conservative exact-ish matching."""
    n = (name or "").lower()
    n = _CORP_SUFFIXES.sub(" ", n)
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def _fmt_amount(n: Optional[float]) -> str:
    if not n:
        return ""
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"${n / 1_000:.0f}K"
    return f"${n:.0f}"


# ── Step 1: index fetch ─────────────────────────────────────────────────────

async def fetch_recent_form_d(days: int = 2) -> list[dict]:
    """Fetch recent Form D filings. Returns [{name, cik, filed_at, accession_no, doc}]."""
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days)).date().isoformat()
    headers = {"User-Agent": cfg.EDGAR_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=20, headers=headers) as client:
            resp = await client.get(_SEARCH_URL, params={
                "q": "",
                "forms": "D",
                "startdt": start,
                "enddt": now.date().isoformat(),
            })
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning(f"EDGAR Form D fetch failed: {exc}")
        return []

    filings = []
    for hit in (data.get("hits", {}) or {}).get("hits", []):
        src = hit.get("_source", {})
        names = src.get("display_names") or []
        if not names:
            continue
        # display_names look like "Acme Inc.  (CIK 0001234567)"
        raw = str(names[0])
        m = re.match(r"^(.*?)\s*\(CIK\s*(\d+)\)", raw)
        name = m.group(1).strip() if m else raw.strip()
        cik = m.group(2) if m else None
        # hit _id looks like "0001234567-26-000001:primary_doc.xml"
        hit_id = str(src.get("_id") or hit.get("_id") or "")
        acc, _, doc = hit_id.partition(":")
        filings.append({
            "name": name,
            "cik": cik,
            "filed_at": src.get("file_date"),
            "accession_no": acc or hit_id,
            "doc": doc or "primary_doc.xml",
        })
    logger.info(f"EDGAR: {len(filings)} Form D filings since {start}")
    return filings


# ── Step 2: document fetch + parse ──────────────────────────────────────────

def _grab(xml: str, tag: str) -> Optional[str]:
    m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.S)
    return m.group(1).strip() if m else None


def _grab_amount(xml: str, tag: str) -> Optional[float]:
    raw = _grab(xml, tag)
    if not raw or "indefinite" in raw.lower():
        return None
    try:
        return float(re.sub(r"[^\d.]", "", raw) or 0) or None
    except ValueError:
        return None


def parse_form_d_xml(xml: str) -> dict:
    """Extract structured fields from a Form D primary_doc.xml."""
    industry = _grab(xml, "industryGroupType")
    first_sale = _grab(xml, "dateOfFirstSale")
    if first_sale and "yetToOccur" in first_sale:
        first_sale = "yet_to_occur"

    persons = []
    for m in re.finditer(
        r"<firstName>(.*?)</firstName>.*?<lastName>(.*?)</lastName>.*?"
        r"<relationship>(.*?)</relationship>",
        xml, re.S,
    ):
        persons.append({
            "name": f"{m.group(1).strip()} {m.group(2).strip()}",
            "relationship": m.group(3).strip(),
        })

    return {
        "industry_group": industry,
        "is_tech": bool(industry and industry.strip().lower() in TECH_INDUSTRY_GROUPS),
        "offering_amount": _grab_amount(xml, "totalOfferingAmount"),
        "amount_sold": _grab_amount(xml, "totalAmountSold"),
        "revenue_range": _grab(xml, "revenueRange"),
        "date_of_first_sale": first_sale,
        "state": _grab(xml, "stateOrCountryDescription"),
        "related_persons": persons[:8],
    }


async def fetch_filing_details(filings: list[dict], concurrency: int = 4) -> list[dict]:
    """Fetch + parse primary_doc.xml for each filing. Mutates and returns filings."""
    headers = {"User-Agent": cfg.EDGAR_USER_AGENT}
    sem = asyncio.Semaphore(concurrency)

    async def _one(client: httpx.AsyncClient, f: dict) -> None:
        if not f.get("cik"):
            return
        url = _ARCHIVE_URL.format(
            cik=int(f["cik"]),
            acc_nodash=f["accession_no"].replace("-", ""),
            doc=f["doc"],
        )
        async with sem:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    f.update(parse_form_d_xml(resp.text))
                await asyncio.sleep(0.15)  # stay well under SEC's 10 req/s
            except Exception as exc:
                logger.debug(f"EDGAR doc fetch failed for {f['accession_no']}: {exc}")

    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        await asyncio.gather(*[_one(client, f) for f in filings])

    parsed = [f for f in filings if "industry_group" in f]
    logger.info(
        f"EDGAR: parsed {len(parsed)}/{len(filings)} documents, "
        f"{sum(1 for f in parsed if f.get('is_tech'))} tech-relevant"
    )
    return filings


# ── Step 3: persist filings ─────────────────────────────────────────────────

def persist_filings(session: Session, filings: list[dict]) -> int:
    """Upsert tech-relevant parsed filings into discovery_filing."""
    written = 0
    for f in filings:
        if not f.get("is_tech"):
            continue
        session.execute(text("""
            INSERT INTO discovery_filing
                (id, accession_no, cik, entity_name, normalized_name, form_type,
                 filed_at, industry_group, is_tech, offering_amount, amount_sold,
                 revenue_range, date_of_first_sale, state, related_persons, created_at)
            VALUES
                (:id, :accession_no, :cik, :entity_name, :normalized_name, 'D',
                 :filed_at, :industry_group, true, :offering_amount, :amount_sold,
                 :revenue_range, :date_of_first_sale, :state,
                 CAST(:related_persons AS jsonb), NOW())
            ON CONFLICT (accession_no) DO NOTHING
        """), {
            "id": str(uuid.uuid4()),
            "accession_no": f["accession_no"],
            "cik": f.get("cik"),
            "entity_name": f["name"],
            "normalized_name": normalize_name(f["name"]),
            "filed_at": f.get("filed_at"),
            "industry_group": f.get("industry_group"),
            "offering_amount": f.get("offering_amount"),
            "amount_sold": f.get("amount_sold"),
            "revenue_range": f.get("revenue_range"),
            "date_of_first_sale": f.get("date_of_first_sale"),
            "state": f.get("state"),
            "related_persons": json.dumps(f.get("related_persons") or []),
        })
        written += 1
    return written


# ── Step 4: match + enriched events ─────────────────────────────────────────

def _insert_funding_event(session: Session, company_id: str, filing: dict) -> None:
    amount = _fmt_amount(filing.get("offering_amount"))
    execs = ", ".join(p["name"] for p in (filing.get("related_persons") or [])[:3])
    title = f"SEC Form D filed by {filing['name']}"
    if amount:
        title += f" — {amount} offering"
    if filing.get("amount_sold"):
        title += f" ({_fmt_amount(filing['amount_sold'])} already sold)"
    title += f" (filed {filing.get('filed_at')})"

    session.execute(text("""
        INSERT INTO discovery_signal_event
            (id, company_id, event_type, title, payload, source,
             observed_at, confidence, dedupe_key, created_at)
        VALUES
            (:id, :company_id, 'funding_round', :title, CAST(:payload AS jsonb),
             'sec_edgar', :observed_at, 0.9, :dedupe_key, NOW())
        ON CONFLICT (dedupe_key) DO NOTHING
    """), {
        "id": str(uuid.uuid4()),
        "company_id": company_id,
        "title": title,
        "payload": json.dumps({
            "key": f"edgar:{filing['accession_no']}",
            "form": "D",
            "accession_no": filing["accession_no"],
            "cik": filing.get("cik"),
            "filed_at": filing.get("filed_at"),
            "offering_amount": filing.get("offering_amount"),
            "amount_sold": filing.get("amount_sold"),
            "industry_group": filing.get("industry_group"),
            "executives": execs or None,
        }),
        "observed_at": datetime.now(timezone.utc),
        "dedupe_key": f"{company_id}:funding_round:edgar:{filing['accession_no']}",
    })


def match_and_insert_events(session: Session, filings: list[dict]) -> list[str]:
    """
    Conservative name match against discovery_company; insert enriched
    funding_round events for matches and link the filing row.
    Returns matched domains (for instant alerts).
    """
    if not filings:
        return []

    by_norm: dict[str, dict] = {}
    for f in filings:
        norm = normalize_name(f["name"])
        if len(norm) >= 4:  # skip too-short/ambiguous names
            by_norm.setdefault(norm, f)

    if not by_norm:
        return []

    rows = session.execute(text("""
        SELECT id, name, domain
        FROM discovery_company
        WHERE LOWER(regexp_replace(name, '[^a-zA-Z0-9]+', ' ', 'g')) = ANY(:norms)
           OR LOWER(name) = ANY(:norms)
    """), {"norms": list(by_norm.keys())}).mappings().all()

    from edges import edges_from_filing, upsert_edges

    matched_domains: list[str] = []
    for row in rows:
        norm = normalize_name(row["name"])
        filing = by_norm.get(norm)
        if not filing:
            continue
        filing["_matched"] = True
        _insert_funding_event(session, row["id"], filing)
        upsert_edges(session, edges_from_filing(row["id"], filing))
        session.execute(text("""
            UPDATE discovery_filing SET matched_company_id = :cid
            WHERE accession_no = :acc
        """), {"cid": row["id"], "acc": filing["accession_no"]})
        logger.info(f"EDGAR match: {filing['name']} → {row['name']} ({row['domain']})")
        if row["domain"]:
            matched_domains.append(row["domain"])
    return matched_domains


# ── Step 5: auto-create newly funded startups ───────────────────────────────

async def create_companies_from_filings(session: Session, filings: list[dict]) -> int:
    """
    For unmatched tech filings: resolve domain via the standard discovery
    enrichment lookup and insert a discovery_company row (source=sec_edgar).
    The new row enters the normal signal-enrichment pipeline as 'pending'.
    """
    from enrichment import lookup_domain

    candidates = [
        f for f in filings
        if f.get("is_tech") and not f.get("_matched") and len(normalize_name(f["name"])) >= 4
    ][:AUTO_CREATE_CAP]

    created = 0
    for f in candidates:
        # Already created by a previous poll (e.g. a Form D/A amendment)?
        exists = session.execute(text("""
            SELECT 1 FROM discovery_company
            WHERE LOWER(regexp_replace(name, '[^a-zA-Z0-9]+', ' ', 'g')) = :norm
               OR LOWER(apollo_org_name) = :norm
            LIMIT 1
        """), {"norm": normalize_name(f["name"])}).first()
        if exists:
            continue

        try:
            enrichment = await lookup_domain(f["name"])
        except Exception as exc:
            logger.warning(f"EDGAR auto-create lookup failed for {f['name']}: {exc}")
            continue

        domain = enrichment.get("domain")
        company_id = str(uuid.uuid4())
        amount = _fmt_amount(f.get("offering_amount"))
        industries = enrichment.get("industries") or []

        row = session.execute(text("""
            INSERT INTO discovery_company
                (id, apollo_org_name, name, domain, website_url, description, industry,
                 headquarters, founded_year, funding, source, source_profiles,
                 domain_resolved, enrichment_status, signal_enrichment_status,
                 raw_meta, first_seen_at, last_seen_at, created_at, updated_at)
            VALUES
                (:id, :org_name, :name, :domain, :website_url, :description, :industry,
                 :headquarters, :founded_year, :funding, 'sec_edgar', NULL,
                 :domain_resolved, :enrichment_status, 'pending',
                 CAST(:raw_meta AS jsonb), NOW(), NOW(), NOW(), NOW())
            ON CONFLICT (domain) WHERE domain IS NOT NULL DO NOTHING
            RETURNING id
        """), {
            "id": company_id,
            "org_name": f["name"],
            "name": enrichment.get("name") or f["name"],
            "domain": domain,
            "website_url": enrichment.get("website_url"),
            "description": enrichment.get("description"),
            "industry": industries[0] if industries else None,
            "headquarters": enrichment.get("headquarters") or f.get("state"),
            "founded_year": enrichment.get("founded_year"),
            "funding": f"Form D · {amount}" if amount else "Form D filed",
            "domain_resolved": bool(domain),
            "enrichment_status": "enriched" if enrichment.get("description") else "pending",
            "raw_meta": json.dumps({
                "keywords": enrichment.get("keywords"),
                "use_case": enrichment.get("use_case"),
                "edgar_accession": f["accession_no"],
            }),
        }).first()

        if row is None:  # domain conflict — company already exists under this domain
            continue

        session.execute(text("""
            UPDATE discovery_filing SET matched_company_id = :cid
            WHERE accession_no = :acc
        """), {"cid": company_id, "acc": f["accession_no"]})
        _insert_funding_event(session, company_id, f)
        from edges import edges_from_filing, upsert_edges
        upsert_edges(session, edges_from_filing(company_id, f))
        created += 1
        logger.info(f"EDGAR auto-created: {f['name']} → {domain or 'no domain'} ({amount or 'n/a'})")

    return created


def push_instant_alerts(domains: list[str]) -> None:
    if not domains:
        return
    try:
        import redis as _redis
        r = _redis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
        for d in domains:
            r.rpush(cfg.SIGNALS_ALERT_KEY, d)
    except Exception as exc:
        logger.warning(f"EDGAR alert push failed: {exc}")
