"""
ICP-targeted index fill — grow discovery_company where an org's Discover pool is thin.

The backend sends Apollo filters built from one org's ICP (company size,
locations, industry keyword tags, and job titles those companies hire for).
Each variant is paged through Apollo's free People Search with owner/CEO
titles (≈ one result per company), resuming from a per-org cursor in
discovery_progress so weekly top-ups go deeper instead of re-reading page 1.

New companies get the same scrape-time lookup as the old weekly scrape
(domain, description, description_embedding — searchable by Discover at
once), then signal enrichment is queued, spread out so user-triggered
enrichment on arq:ondemand isn't starved.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import func, text
from sqlmodel import Session, select

import settings as cfg
from models import DiscoveryCompany, DiscoveryProgress

logger = logging.getLogger("discovery.icp_fill")

SOURCE = "apollo_icp"
OWNER_TITLES = ["CEO", "Founder", "Owner", "President"]
OWNER_SENIORITIES = ["owner", "founder", "c_suite"]
PER_PAGE = 100
RESTART_AFTER_DAYS = 30      # an exhausted variant starts over from page 1 after this
ENRICH_SPACING_SECONDS = 15  # gap between queued enrichments
YIELD_PAGES = 5              # pages kept in cursor.stats["recent"] (the backend skips low-yield orgs)

_ORG_FILTER_KEYS = (
    "organization_locations",
    "organization_num_employees_ranges",
    "revenue_range_min",
    "revenue_range_max",
)


def search_variants(filters: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """
    One Apollo search per signal the ICP gives us, each with owner titles so a
    page of people is a page of distinct companies. Keyword tags and job titles
    are separate variants: Apollo ANDs them, and either alone may match nothing
    on the free tier.
    """
    base = {k: filters[k] for k in _ORG_FILTER_KEYS if filters.get(k) not in (None, [], "")}
    base.update({
        "person_titles": OWNER_TITLES,
        "person_seniorities": OWNER_SENIORITIES,
        "include_similar_titles": True,
    })
    variants = []
    if filters.get("q_organization_keyword_tags"):
        variants.append(("tags", {**base, "q_organization_keyword_tags": filters["q_organization_keyword_tags"][:8]}))
    if filters.get("q_organization_job_titles"):
        variants.append(("hiring", {**base, "q_organization_job_titles": filters["q_organization_job_titles"][:8]}))
    if not variants:
        variants.append(("base", base))
    return variants


def _filters_hash(filters: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(filters, sort_keys=True, default=str).encode()).hexdigest()[:12]


def _cursor(session: Session, run_id: str, filters_hash: str) -> DiscoveryProgress:
    row = session.exec(select(DiscoveryProgress).where(DiscoveryProgress.run_id == run_id)).first()
    now = datetime.now(timezone.utc)
    if row is None:
        row = DiscoveryProgress(run_id=run_id, status="running", current_page=0,
                                stats={"filters": filters_hash})
        session.add(row)
        session.commit()
        session.refresh(row)
    elif (row.stats or {}).get("filters") != filters_hash:
        # the ICP changed — a page number from the old search means nothing for the new one
        row.status, row.current_page, row.completed_at = "running", 0, None
        row.stats = {"filters": filters_hash}
        session.add(row)
        session.commit()
    elif row.status == "exhausted":
        done = row.completed_at or row.started_at
        if done and done.tzinfo is None:
            done = done.replace(tzinfo=timezone.utc)
        if not done or now - done > timedelta(days=RESTART_AFTER_DAYS):
            row.status, row.current_page, row.completed_at = "running", 0, None
            session.add(row)
            session.commit()
    return row


def _known_names(session: Session) -> set[str]:
    rows = session.execute(text("SELECT lower(apollo_org_name), lower(name) FROM discovery_company")).all()
    return {n for pair in rows for n in pair if n}


def _tag_existing(session: Session, *, name: Optional[str] = None, domain: Optional[str] = None,
                  profile: str) -> bool:
    """Mark an existing company as seen for this ICP. True when it existed."""
    q = select(DiscoveryCompany)
    if domain:
        q = q.where(DiscoveryCompany.domain == domain)
    else:
        q = q.where(func.lower(DiscoveryCompany.apollo_org_name) == (name or "").lower())
    row = session.exec(q).first()
    if not row:
        return False
    profiles = list(row.source_profiles or [])
    if profile not in profiles:
        row.source_profiles = profiles + [profile]
    row.last_seen_at = datetime.now(timezone.utc)
    session.add(row)
    return True


async def _daily_budget_left(cap: int) -> int:
    """New companies still allowed today across all orgs (Serper + LLM cost guard)."""
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
        used = int(await r.get(_budget_key()) or 0)
        await r.aclose()
        return max(0, cap - used)
    except Exception:
        return cap


async def _spend_budget(n: int) -> None:
    if n <= 0:
        return
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
        await r.incrby(_budget_key(), n)
        await r.expire(_budget_key(), 60 * 60 * 26)
        await r.aclose()
    except Exception:
        pass


def _budget_key() -> str:
    return f"discovery:icp_fill:{datetime.now(timezone.utc):%Y-%m-%d}"


async def _queue_enrichment(company_ids: list[str]) -> None:
    if not company_ids:
        return
    import arq
    from arq.connections import RedisSettings

    pool = await arq.create_pool(RedisSettings.from_dsn(cfg.REDIS_URL))
    try:
        for i, cid in enumerate(company_ids):
            await pool.enqueue_job(
                "enrich_company_task", company_id=cid,
                _queue_name="arq:ondemand", _defer_by=timedelta(seconds=i * ENRICH_SPACING_SECONDS),
            )
    finally:
        await pool.aclose()


async def fill_index_for_icp(
    session: Session,
    key: str,
    filters: dict[str, Any],
    *,
    max_new: int,
    max_pages: Optional[int] = None,
    api_key: Optional[str] = None,
) -> dict[str, Any]:
    """
    Add up to max_new companies matching one org's ICP. `key` names the ICP
    (the org id) — it goes into source_profiles and the cursor run_id.
    """
    from scrape.apollo_client import ApolloBudgetExhausted, ApolloRateLimiter, ApolloRateLimitError, search_page
    from scrape.domain_lookup import lookup_domain
    from signals.pipeline import fetch_apollo_headcount

    api_key = api_key or cfg.TELLORA_APOLLO_API_KEY
    max_pages = max_pages or cfg.ICP_FILL_MAX_PAGES
    budget = min(max_new, await _daily_budget_left(cfg.ICP_FILL_DAILY_CAP))
    stats: dict[str, Any] = {"added": 0, "already_known": 0, "no_domain": 0, "pages": 0,
                             "budget": budget, "variants": {}}
    if budget <= 0:
        stats["skipped"] = "daily_cap"
        return stats

    profile = f"icp:{key}"
    known = _known_names(session)
    limiter = ApolloRateLimiter()
    sem = asyncio.Semaphore(cfg.ICP_FILL_LOOKUP_CONCURRENCY)
    new_ids: list[str] = []

    async def _resolve(name: str, first_name: str) -> tuple[str, dict, dict]:
        async with sem:
            found = await lookup_domain(name, first_name)
            domain = found.get("domain")
            headcount = await fetch_apollo_headcount(domain) if domain else {}
            return name, found, headcount

    for variant, vfilters in search_variants(filters):
        if stats.get("stopped"):
            break
        cursor = _cursor(session, f"{profile}:{variant}", _filters_hash(vfilters))
        vstats = {"pages": 0, "added": 0, "total_entries": None}
        stats["variants"][variant] = vstats
        if cursor.status == "exhausted":
            vstats["skipped"] = "exhausted"
            continue
        page = cursor.current_page
        rate_limited = 0
        while stats["added"] < budget and vstats["pages"] < max_pages:
            page += 1
            await limiter.wait()
            try:
                data = await search_page(api_key, vfilters, page=page, per_page=PER_PAGE)
                limiter.reset_backoff()
                rate_limited = 0
            except ApolloBudgetExhausted as exc:
                stats["stopped"] = str(exc)
                break
            except ApolloRateLimitError:
                rate_limited += 1
                page -= 1
                if rate_limited >= 3:
                    stats["stopped"] = "rate_limited"
                    break
                await limiter.on_rate_limit()
                continue
            vstats["pages"] += 1
            stats["pages"] += 1
            vstats["total_entries"] = data.get("total_entries")
            people = data.get("people") or []

            # org name → the owner's first name (disambiguates the domain search)
            fresh: dict[str, tuple[str, str]] = {}
            seen_on_page: set[str] = set()
            known_on_page = 0
            for person in people:
                name = ((person.get("organization") or {}).get("name") or "").strip()
                if not name or name.lower() in seen_on_page:
                    continue
                seen_on_page.add(name.lower())
                if name.lower() in known:
                    _tag_existing(session, name=name, profile=profile)
                    stats["already_known"] += 1
                    known_on_page += 1
                    continue
                fresh[name.lower()] = (name, person.get("first_name") or "")

            room = budget - stats["added"]
            results = await asyncio.gather(*[_resolve(n, f) for n, f in list(fresh.values())[:room]])
            added_on_page = 0
            for name, found, headcount in results:
                known.add(name.lower())
                domain = (found.get("domain") or "").lower() or None
                if not domain:
                    stats["no_domain"] += 1
                    continue
                if _tag_existing(session, domain=domain, profile=profile):
                    stats["already_known"] += 1
                    continue
                company = DiscoveryCompany(
                    apollo_org_name=name,
                    name=found.get("name") or name,
                    domain=domain,
                    website_url=found.get("website_url"),
                    linkedin_url=found.get("linkedin_url"),
                    description=found.get("description"),
                    industry=found.get("industry"),
                    ceo_name=found.get("ceo_name"),
                    headquarters=found.get("headquarters"),
                    founded_year=found.get("founded_year"),
                    funding=found.get("funding"),
                    logo_url=found.get("logo_url"),
                    description_embedding=found.get("description_embedding"),
                    headcount=headcount.get("headcount_estimate"),
                    source=SOURCE,
                    source_profiles=[profile],
                    domain_resolved=True,
                    enrichment_status="enriched" if found.get("description") else "pending",
                    raw_meta={"keywords": found.get("keywords"), "use_case": found.get("use_case")},
                )
                session.add(company)
                session.flush()
                new_ids.append(str(company.id))
                stats["added"] += 1
                vstats["added"] += 1
                added_on_page += 1

            cursor.current_page = page
            cursor.last_heartbeat = datetime.now(timezone.utc)
            prev = cursor.stats or {}
            recent = (prev.get("recent") or []) + [[added_on_page, known_on_page + len(results)]]
            cursor.stats = {**prev, "added": prev.get("added", 0) + added_on_page,
                            "recent": recent[-YIELD_PAGES:]}
            if len(people) < PER_PAGE:
                cursor.status, cursor.completed_at = "exhausted", datetime.now(timezone.utc)
            session.add(cursor)
            session.commit()
            if cursor.status == "exhausted":
                break

    await _spend_budget(stats["added"])
    await _queue_enrichment(new_ids)
    logger.info(f"[icp_fill] {profile}: {stats}")
    return stats
