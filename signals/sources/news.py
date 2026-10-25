"""
Google News RSS + Product Hunt feed ingesters (free, no API key).

  fetch_company_news(name)  → headlines from the last 7 days
  classify_news(name, items, domain=…, description=…) → entity-verified, typed extra_events
  fetch_product_hunt_launches() → daily PH launches for cron matching
"""

from __future__ import annotations

import json
import logging
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from signals.diff import parse_event_date
from signals.sources.edgar import normalize_name
from llm import get_router, retry_llm, strip_json_fences

logger = logging.getLogger("discovery.news")

_RSS_URL = (
    "https://news.google.com/rss/search?"
    'q="{name}"+when:{days}d+-stock+-shares&hl=en-US&gl=US&ceid=US:en'
)

# classifier category → event type (anything else is dropped)
_CATEGORY_EVENTS = {
    "funding": "funding_round",
    "exec_hire": "exec_hire",
    "exec_departure": "exec_departure",
    "layoffs": "layoffs",
    "acquisition": "acquisition",
    "expansion": "expansion",
    "partnership": "partnership",
    "product": "product_launch",
    "business_news": "news_mention",
}


async def fetch_company_news(company_name: str, days: int = 7) -> list[dict]:
    """Fetch recent headlines from Google News RSS. Returns [{title, url, date, source}]."""
    if not company_name:
        return []
    url = _RSS_URL.format(name=httpx.QueryParams({"q": company_name})["q"], days=days)
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception as exc:
        logger.warning(f"Google News RSS failed for '{company_name}': {exc}")
        return []

    items = []
    for item in root.iter("item"):
        src_el = item.find("source")
        items.append({
            "title": (item.findtext("title") or "").strip(),
            "url": (item.findtext("link") or "").strip(),
            "date": (item.findtext("pubDate") or "").strip(),
            "source": (src_el.text or "").strip() if src_el is not None else "",
        })
    return [it for it in items if it["title"]][:12]


def _company_card(company_name: str, domain: Optional[str], description: Optional[str], industry: Optional[str]) -> str:
    parts = [f"Name: {company_name}"]
    if domain:
        parts.append(f"Website: {domain}")
    if industry:
        parts.append(f"Industry: {industry}")
    if description:
        parts.append(f"What they do: {description.strip()[:400]}")
    return "\n".join(parts)


def classify_news(
    company_name: str,
    items: list[dict],
    *,
    domain: Optional[str] = None,
    description: Optional[str] = None,
    industry: Optional[str] = None,
) -> list[dict]:
    """
    Verify + type headlines found by a name search. Synchronous — run in executor.

    Name searches return same-named companies, places and people ("Origin" →
    Blue Origin, "Redmond" → the city), so the model gets the company's website,
    industry and description and must confirm each headline is about *this*
    company. Funding only counts when the company itself receives investment —
    grants, donations, fundraisers and prizes are dropped.
    """
    if not items:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=cfg.NEWS_EVENT_MAX_AGE_DAYS)
    fresh = []
    for it in items:
        date = parse_event_date(it.get("date"))
        if date is None or date >= cutoff:
            fresh.append(it)
    if not fresh:
        return []

    sample = [{"i": i, "title": it["title"], "source": it["source"]} for i, it in enumerate(fresh)]
    prompt = f"""You verify news headlines for one specific company.

COMPANY
{_company_card(company_name, domain, description, industry)}

For each headline decide:
about_company: true ONLY if the headline is about this exact company (the one
  at the website above). false for a different company/product/place/person that
  shares the name, for listicles, stock-price chatter, and sponsored posts.
category:
  funding        — this company RECEIVED venture/equity/debt investment (not grants,
                   donations, charity fundraisers, prizes, or investing in others)
  exec_hire      — this company hired/promoted a named executive (VP and above)
  exec_departure — a named executive left this company
  layoffs        — this company cut jobs or closed a site
  acquisition    — this company acquired, or was acquired by, a named company
  expansion      — new office, new market/country, major new facility
  partnership    — a named strategic partnership or major customer win
  product        — this company launched a product or major feature
  business_news  — other substantive news about this company's business
  irrelevant     — anything else
amount: the funding amount as written (e.g. "$40M"), else null
story: 2-5 lowercase words naming the real-world event; headlines about the
  same event get the same story (e.g. "krea 3 launch", "series b 40m")

HEADLINES:
{json.dumps(sample)}

Respond with ONLY valid JSON: {{"results": [{{"i": 0, "about_company": true, "category": "funding", "amount": "$40M", "story": "series b 40m"}}]}}"""

    def _do():
        raw = get_router().complete_text(
            prompt,
            models=get_router().signal_models,
            temperature=0.0,
            json_mode=True,
        )
        return json.loads(strip_json_fences(raw))

    try:
        verdicts = retry_llm(_do)
    except Exception as exc:
        logger.warning(f"News classification failed for '{company_name}': {exc}")
        return []

    events: list[dict] = []
    by_story: dict[tuple[str, str], dict] = {}
    for v in verdicts.get("results", []):
        try:
            it = fresh[int(v["i"])]
        except (KeyError, ValueError, IndexError, TypeError):
            continue
        if v.get("about_company") is not True:
            continue
        category = (v.get("category") or "irrelevant").strip().lower()
        event_type = _CATEGORY_EVENTS.get(category)
        if not event_type:
            continue
        amount = (v.get("amount") or "").strip() or None
        if event_type == "funding_round" and not amount and not re.search(
            r"\b(series [a-h]|seed|pre-seed|round)\b", it["title"], re.IGNORECASE
        ):
            event_type = "news_mention"  # "secures investment" with no amount or round is not a round
        story = re.sub(r"\s+", " ", (v.get("story") or "").strip().lower())[:60]
        source_ref = {"url": it.get("url"), "title": it["title"][:200], "source": it.get("source")}
        if story and (event_type, story) in by_story:
            # Same real-world event from another outlet: one event, more sources
            ev = by_story[(event_type, story)]
            ev["payload"]["sources"].append(source_ref)
            ev["confidence"] = min(0.95, ev["confidence"] + 0.05)
            continue
        ev = {
            "event_type": event_type,
            "title": it["title"][:500],
            "payload": {
                "key": (it.get("url") or it["title"])[:120],
                "url": it.get("url"),
                "date": it.get("date"),
                "news_source": it.get("source"),
                "category": category,
                "amount": amount,
                "story": story or None,
                "sources": [source_ref],
            },
            "source": "google_news",
            "confidence": 0.8,
            "evidence_url": it.get("url"),
            "event_date": it.get("date"),
        }
        if story:
            by_story[(event_type, story)] = ev
        events.append(ev)
    return events[:6]


# ── Product Hunt daily poll ──────────────────────────────────────────────────

_PH_FEED = "https://www.producthunt.com/feed"
_ATOM = "http://www.w3.org/2005/Atom"
_PH_AUTO_CREATE_CAP = int(getattr(cfg, "PH_AUTO_CREATE_CAP", 10) or 10)


async def fetch_product_hunt_launches() -> list[dict]:
    """Fetch recent Product Hunt launches from the public Atom feed."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(_PH_FEED)
            resp.raise_for_status()
        root = ET.fromstring(resp.text.encode())
    except Exception as exc:
        logger.warning(f"Product Hunt feed failed: {exc}")
        return []

    launches = []
    for entry in root.findall(f"{{{_ATOM}}}entry"):
        title = (entry.findtext(f"{{{_ATOM}}}title") or "").strip()
        link_el = entry.find(f"{{{_ATOM}}}link")
        url = link_el.get("href", "") if link_el is not None else ""
        updated = (entry.findtext(f"{{{_ATOM}}}updated") or "")[:10]
        slug = ""
        if url:
            path = urlparse(url).path.strip("/")
            if path.startswith("posts/"):
                slug = path.split("/", 1)[1]
            else:
                slug = path.split("/")[-1] if path else ""
        if title:
            launches.append({
                "title": title,
                "slug": slug or re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-"),
                "url": url,
                "date": updated,
            })
    return launches[:30]


def _insert_ph_launch_event(session: Session, company_id: str, launch: dict) -> None:
    session.execute(text("""
        INSERT INTO discovery_signal_event
            (id, company_id, event_type, title, payload, source,
             observed_at, confidence, dedupe_key, evidence_url, event_date, created_at)
        VALUES
            (:id, :company_id, 'product_launch', :title, CAST(:payload AS jsonb),
             'product_hunt', :observed_at, 0.8, :dedupe_key, :evidence_url, :event_date, NOW())
        ON CONFLICT (dedupe_key) DO NOTHING
    """), {
        "id": str(uuid.uuid4()),
        "company_id": company_id,
        "title": f"Product Hunt launch: {launch['title']}"[:500],
        "payload": json.dumps({
            "key": f"ph:{launch['slug']}",
            "slug": launch["slug"],
            "url": launch.get("url"),
            "date": launch.get("date"),
        }),
        "observed_at": datetime.now(timezone.utc),
        "dedupe_key": f"{company_id}:product_launch:ph:{launch['slug']}",
        "evidence_url": launch.get("url"),
        "event_date": parse_event_date(launch.get("date")) if launch.get("date") else None,
    })


def match_ph_launches(session: Session, launches: list[dict]) -> list[str]:
    """Match PH launch names to tracked companies (signals.entity); insert product_launch events."""
    if not launches:
        return []
    from signals.entity import CompanyIndex

    index = CompanyIndex.load(session)
    matched_domains: list[str] = []
    for launch in launches:
        hit = index.match(launch["title"])
        if not hit:
            continue
        launch["_matched"] = True
        _insert_ph_launch_event(session, hit.company.id, launch)
        if hit.company.domain:
            matched_domains.append(hit.company.domain)
    return matched_domains


async def create_companies_from_launches(session: Session, launches: list[dict]) -> int:
    """Auto-create discovery_company rows for unmatched PH launches (capped)."""
    from scrape.domain_lookup import lookup_domain

    candidates = [
        l for l in launches
        if not l.get("_matched") and len(normalize_name(l["title"])) >= 3
    ][:_PH_AUTO_CREATE_CAP]

    created = 0
    for launch in candidates:
        norm = normalize_name(launch["title"])
        exists = session.execute(text("""
            SELECT 1 FROM discovery_company
            WHERE LOWER(regexp_replace(name, '[^a-zA-Z0-9]+', ' ', 'g')) = :norm
               OR LOWER(apollo_org_name) = :norm
            LIMIT 1
        """), {"norm": norm}).first()
        if exists:
            continue

        try:
            enrichment = await lookup_domain(launch["title"])
        except Exception as exc:
            logger.warning(f"PH auto-create lookup failed for {launch['title']}: {exc}")
            continue

        domain = enrichment.get("domain")
        company_id = str(uuid.uuid4())
        industries = enrichment.get("industries") or []

        row = session.execute(text("""
            INSERT INTO discovery_company
                (id, apollo_org_name, name, domain, website_url, description, industry,
                 headquarters, founded_year, funding, source, source_profiles,
                 domain_resolved, enrichment_status, signal_enrichment_status,
                 raw_meta, first_seen_at, last_seen_at, created_at, updated_at)
            VALUES
                (:id, :org_name, :name, :domain, :website_url, :description, :industry,
                 :headquarters, NULL, 'Product Hunt launch', 'product_hunt', NULL,
                 :domain_resolved, :enrichment_status, 'pending',
                 CAST(:raw_meta AS jsonb), NOW(), NOW(), NOW(), NOW())
            ON CONFLICT (domain) WHERE domain IS NOT NULL DO NOTHING
            RETURNING id
        """), {
            "id": company_id,
            "org_name": launch["title"],
            "name": enrichment.get("name") or launch["title"],
            "domain": domain,
            "website_url": enrichment.get("website_url"),
            "description": enrichment.get("description"),
            "industry": industries[0] if industries else None,
            "headquarters": enrichment.get("headquarters"),
            "domain_resolved": bool(domain),
            "enrichment_status": "enriched" if enrichment.get("description") else "pending",
            "raw_meta": json.dumps({
                "keywords": enrichment.get("keywords"),
                "use_case": enrichment.get("use_case"),
                "product_hunt_slug": launch["slug"],
            }),
        }).first()

        if row is None:
            continue

        _insert_ph_launch_event(session, company_id, launch)
        created += 1
        logger.info(f"PH auto-created: {launch['title']} → {domain or 'no domain'}")

    return created
