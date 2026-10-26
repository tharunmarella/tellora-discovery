"""
SEC EDGAR pollers — funding rounds and exec changes from legal filings.

Form D (daily):
  1. List every Form D / D/A filed in the window from EDGAR's daily form index
     (edgartools). The old full-text-search call returned one page of hits, so
     most of a day's ~250 filings were never seen.
  2. Parse each filing with edgartools' FormD object: offering amount, amount
     sold, industry group, revenue range, related persons, issuer state.
  3. Persist tech-relevant filings to discovery_filing (even unmatched ones).
  4. Matched filers (signals.entity, name + issuer state) → funding_round events.
  5. Unmatched tech filers → auto-created discovery_company rows
     (source=sec_edgar) so newly funded startups enter the index the day
     they file, before any press exists.

8-K (every 6h): officer appointments/departures (Item 5.02) and completed
acquisitions (Item 2.01) for tracked public companies.

SEC fair-access policy: identify via User-Agent (EDGAR_USER_AGENT); edgartools
throttles to SEC's 10 req/s limit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from signals.entity import CompanyIndex

logger = logging.getLogger("discovery.edgar")

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

# Max new discovery_company rows auto-created per daily EDGAR poll (caps Serper/LLM cost)
AUTO_CREATE_CAP = int(getattr(cfg, "EDGAR_AUTO_CREATE_CAP", 25) or 25)

_CORP_SUFFIXES = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|company|"
    r"holdings|technologies|technology|labs|group)\b\.?",
    re.IGNORECASE,
)

_identity_set = False


def _edgar():
    """Import edgartools with the SEC identity set (once per process)."""
    global _identity_set
    import edgar
    if not _identity_set:
        edgar.set_identity(cfg.EDGAR_USER_AGENT)
        _identity_set = True
    return edgar


def normalize_name(name: str) -> str:
    """Normalize a company name for the discovery_filing.normalized_name column."""
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


def _amount(raw: Any) -> Optional[float]:
    s = str(raw or "").strip()
    if not s or "indefinite" in s.lower():
        return None
    try:
        return float(re.sub(r"[^\d.]", "", s) or 0) or None
    except ValueError:
        return None


def _window(days: int) -> str:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return f"{start.isoformat()}:{end.isoformat()}"


def _list_filings_sync(form: str, days: int) -> list[dict]:
    filings = _edgar().get_filings(form=form, filing_date=_window(days))
    out = []
    for f in filings or []:
        out.append({
            "name": f.company,
            "cik": str(f.cik),
            "filed_at": str(f.filing_date),
            "accession_no": f.accession_no,
            "form": f.form,
            "doc": "primary_doc.xml",
            "_filing": f,
        })
    return out


# ── Step 1: index fetch ─────────────────────────────────────────────────────

async def fetch_recent_form_d(days: int = 2) -> list[dict]:
    """Every Form D / D/A filed in the last N days. Returns [{name, cik, filed_at, accession_no, form}]."""
    try:
        filings = await asyncio.to_thread(_list_filings_sync, "D", days)
    except Exception as exc:
        logger.warning(f"EDGAR Form D index fetch failed: {exc}")
        return []
    logger.info(f"EDGAR: {len(filings)} Form D filings in the last {days}d")
    return filings


# ── Step 2: document fetch + parse ──────────────────────────────────────────

def parse_form_d(form_d: Any) -> dict:
    """Structured fields from an edgartools FormD object."""
    od = form_d.offering_data
    issuer = form_d.primary_issuer
    industry = od.industry_group.industry_group_type if od.industry_group else None
    first_sale = (od.date_of_first_sale or "").strip() or None
    amounts = od.offering_sales_amounts
    persons = [
        {
            "name": f"{(p.first_name or '').strip()} {(p.last_name or '').strip()}".strip(),
            "relationship": ", ".join(p.relationships or []),
        }
        for p in (form_d.related_persons or [])
    ]
    address = issuer.primary_address if issuer else None
    return {
        "industry_group": industry,
        "is_tech": bool(industry and industry.strip().lower() in TECH_INDUSTRY_GROUPS),
        "is_new": bool(od.is_new),
        "is_business_combination": bool(
            od.business_combination_transaction and od.business_combination_transaction.is_business_combination
        ),
        "offering_amount": _amount(amounts.total_offering_amount if amounts else None),
        "amount_sold": _amount(amounts.total_amount_sold if amounts else None),
        "revenue_range": od.revenue_range,
        "date_of_first_sale": first_sale,
        "state": address.state_or_country_description if address else None,
        "related_persons": [p for p in persons if p["name"]][:8],
    }


def _load_form_d(f: dict) -> Any:
    filing = f.get("_filing")
    if filing is None:
        filing = _edgar().find(f["accession_no"])
    return filing.obj()


async def fetch_filing_details(filings: list[dict], concurrency: int = 4) -> list[dict]:
    """Parse each Form D with edgartools. Mutates and returns filings."""
    sem = asyncio.Semaphore(concurrency)

    async def _one(f: dict) -> None:
        async with sem:
            try:
                form_d = await asyncio.to_thread(_load_form_d, f)
                f.update(parse_form_d(form_d))
            except Exception as exc:
                logger.debug(f"EDGAR parse failed for {f['accession_no']}: {exc}")

    await asyncio.gather(*[_one(f) for f in filings])

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

    evidence_url = None
    cik = filing.get("cik")
    acc = filing.get("accession_no")
    doc = filing.get("doc") or "primary_doc.xml"
    if cik and acc:
        evidence_url = _ARCHIVE_URL.format(
            cik=int(cik),
            acc_nodash=acc.replace("-", ""),
            doc=doc,
        )

    event_date = datetime.now(timezone.utc)
    filed_at = filing.get("filed_at")
    if filed_at:
        try:
            if isinstance(filed_at, str) and len(filed_at) >= 10:
                event_date = datetime.strptime(filed_at[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    session.execute(text("""
        INSERT INTO discovery_signal_event
            (id, company_id, event_type, title, payload, source,
             observed_at, confidence, dedupe_key, evidence_url, event_date, created_at)
        VALUES
            (:id, :company_id, 'funding_round', :title, CAST(:payload AS jsonb),
             'sec_edgar', :observed_at, 0.9, :dedupe_key, :evidence_url, :event_date, NOW())
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
            "url": evidence_url,
        }),
        "observed_at": datetime.now(timezone.utc),
        "dedupe_key": f"{company_id}:funding_round:edgar:{filing['accession_no']}",
        "evidence_url": evidence_url,
        "event_date": event_date,
    })


def match_and_insert_events(
    session: Session,
    filings: list[dict],
    index: Optional[CompanyIndex] = None,
) -> list[str]:
    """
    Match filers to tracked companies (name + issuer state, see signals.entity);
    insert funding_round events for original filings and link the filing row.
    Amendments (D/A) and business-combination filings are linked but don't
    create a new funding event. Returns matched domains (for instant alerts).
    """
    if not filings:
        return []
    index = index or CompanyIndex.load(session)

    from signals.edges import edges_from_filing, upsert_edges

    matched_domains: list[str] = []
    for filing in filings:
        if "industry_group" not in filing:
            continue
        hit = index.match(filing["name"], state=filing.get("state"))
        if not hit:
            continue
        company = hit.company
        filing["_matched"] = True
        session.execute(text("""
            UPDATE discovery_filing SET matched_company_id = :cid
            WHERE accession_no = :acc
        """), {"cid": company.id, "acc": filing["accession_no"]})
        upsert_edges(session, edges_from_filing(company.id, filing))
        if not filing.get("is_new", True) or filing.get("is_business_combination"):
            continue
        _insert_funding_event(session, company.id, filing)
        logger.info(f"EDGAR match ({hit.reason}): {filing['name']} → {company.name} ({company.domain})")
        if company.domain:
            matched_domains.append(company.domain)
    return matched_domains


# ── Step 5: auto-create newly funded startups ───────────────────────────────

async def create_companies_from_filings(session: Session, filings: list[dict]) -> int:
    """
    For unmatched tech filings: resolve domain via the standard discovery
    enrichment lookup and insert a discovery_company row (source=sec_edgar).
    The new row enters the normal signal-enrichment pipeline as 'pending'.
    """
    from scrape.domain_lookup import lookup_domain

    candidates = [
        f for f in filings
        if f.get("is_tech") and f.get("is_new", True) and not f.get("_matched")
        and not f.get("is_business_combination") and len(normalize_name(f["name"])) >= 4
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
        from signals.edges import edges_from_filing, upsert_edges
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


# ── 8-K: officer changes (Item 5.02) and completed acquisitions (Item 2.01) ──

_8K_ITEMS = ("Item 5.02", "Item 2.01")
_8K_ITEM_TEXT_MAX = 6000


def _extract_8k_events_prompt(company_name: str, item: str, body: str) -> str:
    if item == "Item 5.02":
        return f"""From this SEC 8-K Item 5.02 filed by {company_name}, list each change to an
OFFICER (CEO, CFO, COO, CTO, President, Chief/EVP/SVP/VP-level executive).
Ignore board-only elections, compensation-only amendments and director
resignations unless the person is also an officer.

change = "appointed" (new officer or promotion) | "departed" (resignation, retirement, termination)

TEXT:
{body[:_8K_ITEM_TEXT_MAX]}

Respond with ONLY valid JSON:
{{"changes": [{{"person": "Full Name", "title": "Chief Financial Officer", "change": "appointed", "effective_date": "YYYY-MM-DD or null"}}]}}"""
    return f"""From this SEC 8-K Item 2.01 filed by {company_name}, describe the completed
acquisition or disposition of assets.

kind = "acquired" ({company_name} bought something) | "disposed" ({company_name} sold something)

TEXT:
{body[:_8K_ITEM_TEXT_MAX]}

Respond with ONLY valid JSON:
{{"deals": [{{"kind": "acquired", "counterparty": "Target Co", "amount_usd": 120000000 or null, "closed_date": "YYYY-MM-DD or null"}}]}}"""


def _llm_json(prompt: str) -> dict:
    from llm import get_router, retry_llm, strip_json_fences

    def _do():
        raw = get_router().complete_text(
            prompt, models=get_router().signal_models, temperature=0.0, json_mode=True,
        )
        return json.loads(strip_json_fences(raw))

    return retry_llm(_do)


def events_from_8k_item(filing: dict, company_name: str, item: str, body: str) -> list[dict]:
    """LLM-extract typed events from one 8-K item. Synchronous — run in a thread."""
    try:
        data = _llm_json(_extract_8k_events_prompt(company_name, item, body))
    except Exception as exc:
        logger.warning(f"8-K extraction failed for {filing['accession_no']} {item}: {exc}")
        return []

    url = filing.get("url")
    acc = filing["accession_no"]
    events: list[dict] = []
    if item == "Item 5.02":
        for c in (data.get("changes") or [])[:5]:
            person, title = (c.get("person") or "").strip(), (c.get("title") or "").strip()
            change = (c.get("change") or "").strip().lower()
            if not person or not title or change not in ("appointed", "departed"):
                continue
            hired = change == "appointed"
            events.append({
                "event_type": "exec_hire" if hired else "exec_departure",
                "title": (
                    f"{person} appointed {title} at {company_name}" if hired
                    else f"{person} ({title}) is leaving {company_name}"
                )[:500],
                "payload": {
                    "key": f"8k:{acc}:{person.lower()}",
                    "person": person, "role_title": title, "change": change,
                    "effective_date": c.get("effective_date"),
                    "accession_no": acc, "form": filing.get("form"), "url": url,
                },
                "source": "sec_8k",
                "confidence": 0.95,
                "evidence_url": url,
                "event_date": c.get("effective_date") or filing.get("filed_at"),
            })
    else:
        for d in (data.get("deals") or [])[:3]:
            counterparty = (d.get("counterparty") or "").strip()
            if not counterparty:
                continue
            amount = _fmt_amount(d.get("amount_usd")) if isinstance(d.get("amount_usd"), (int, float)) else ""
            acquired = (d.get("kind") or "acquired").lower() == "acquired"
            events.append({
                "event_type": "acquisition",
                "title": (
                    f"{company_name} {'acquired' if acquired else 'sold'} {counterparty}"
                    + (f" ({amount})" if amount else "")
                )[:500],
                "payload": {
                    "key": f"8k:{acc}:{counterparty.lower()[:60]}",
                    "kind": "acquired" if acquired else "disposed",
                    "counterparty": counterparty, "amount_usd": d.get("amount_usd"),
                    "accession_no": acc, "url": url,
                },
                "source": "sec_8k",
                "confidence": 0.95,
                "evidence_url": url,
                "event_date": d.get("closed_date") or filing.get("filed_at"),
            })
    return events


def _load_8k_items_sync(f: dict) -> dict[str, str]:
    filing = f.get("_filing") or _edgar().find(f["accession_no"])
    f["url"] = filing.homepage_url
    report = filing.obj()
    items = {}
    for item in _8K_ITEMS:
        if item in (report.items or []):
            try:
                body = str(report[item] or "").strip()
            except Exception:
                body = ""
            if len(body) >= 80:
                items[item] = body
    return items


async def fetch_matched_8k_events(
    session: Session,
    days: int = 2,
    index: Optional[CompanyIndex] = None,
) -> dict[str, list[dict]]:
    """
    8-Ks from tracked public companies → {company_id: [extra_event dicts]}.
    Only filings whose filer matches a tracked company are downloaded.
    """
    try:
        filings = await asyncio.to_thread(_list_filings_sync, "8-K", days)
    except Exception as exc:
        logger.warning(f"EDGAR 8-K index fetch failed: {exc}")
        return {}

    index = index or CompanyIndex.load(session)
    matched = []
    for f in filings:
        hit = index.match(f["name"])
        if hit:
            matched.append((f, hit.company))
    logger.info(f"EDGAR: {len(filings)} 8-Ks in {days}d, {len(matched)} from tracked companies")

    out: dict[str, list[dict]] = {}
    for f, company in matched:
        try:
            items = await asyncio.to_thread(_load_8k_items_sync, f)
        except Exception as exc:
            logger.debug(f"8-K load failed for {f['accession_no']}: {exc}")
            continue
        for item, body in items.items():
            evs = await asyncio.to_thread(events_from_8k_item, f, company.name, item, body)
            if evs:
                out.setdefault(company.id, []).extend(evs)
    return out
