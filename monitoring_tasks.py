"""
Scheduled monitoring tasks for the discovery ARQ worker.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from arq import cron
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from signal_enrichment import enrich_company_signals
from signal_runner import persist_result

logger = logging.getLogger("discovery.monitoring")

_engine = make_engine(pool_recycle=300, pool_size=3, max_overflow=5)

_SELECT_WATCHED_STALE = text("""
    SELECT DISTINCT dc.id, dc.name, dc.domain, dc.description, dc.industry,
           dc.raw_meta, dc.headcount, dc.headquarters
    FROM discovery_company dc
    JOIN org_research_company orc ON orc.company_id = dc.id
    WHERE dc.domain IS NOT NULL
      AND dc.domain_resolved = true
      AND (
          dc.signal_enriched_at IS NULL
          OR dc.signal_enriched_at < NOW() - INTERVAL '6 days'
      )
    LIMIT 200
""")

_SELECT_STALE_INDEX = text("""
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
    FROM discovery_company
    WHERE domain IS NOT NULL
      AND domain_resolved = true
      AND (
          signal_enriched_at IS NULL
          OR signal_enriched_at < NOW() - INTERVAL '90 days'
      )
    ORDER BY signal_enriched_at NULLS FIRST
    LIMIT :cap
""")

_SELECT_WATCHED_FOR_JOBS = text("""
    SELECT DISTINCT dc.id, dc.name
    FROM discovery_company dc
    JOIN org_research_company orc ON orc.company_id = dc.id
    WHERE dc.domain IS NOT NULL
""")


async def _refresh_company(row: dict, sem: asyncio.Semaphore) -> None:
    async with sem:
        company_id = str(row["id"])
        try:
            result = await enrich_company_signals(
                company_id=company_id,
                company_name=row["name"],
                domain=row["domain"],
                description=row.get("description"),
                industry=row.get("industry"),
                raw_meta=row.get("raw_meta"),
                existing_headcount=row.get("headcount"),
                existing_headquarters=row.get("headquarters"),
            )
            result["domain"] = row.get("domain")
            with Session(_engine) as session:
                persist_result(session, company_id, result)
                session.commit()
        except Exception as exc:
            logger.error(f"Refresh failed for {row.get('name')}: {exc}")


async def refresh_watched_companies_task(ctx) -> dict:
    """Weekly: re-enrich watched accounts older than 6 days."""
    with Session(_engine) as session:
        rows = [dict(r) for r in session.execute(_SELECT_WATCHED_STALE).mappings().all()]
    if not rows:
        return {"refreshed": 0}
    sem = asyncio.Semaphore(5)
    await asyncio.gather(*[_refresh_company(r, sem) for r in rows])
    logger.info(f"Refreshed {len(rows)} watched companies")
    return {"refreshed": len(rows)}


async def refresh_stale_index_task(ctx) -> dict:
    """Weekly: refresh stale index companies (90d cap)."""
    cap = cfg.REFRESH_BATCH_CAP
    with Session(_engine) as session:
        rows = [dict(r) for r in session.execute(
            _SELECT_STALE_INDEX, {"cap": cap},
        ).mappings().all()]
    if not rows:
        return {"refreshed": 0}
    sem = asyncio.Semaphore(5)
    await asyncio.gather(*[_refresh_company(r, sem) for r in rows])
    return {"refreshed": len(rows)}


async def poll_edgar_form_d_task(ctx) -> dict:
    """
    Daily SEC EDGAR Form D pipeline:
    index → parse documents → store tech filings → match events → auto-create startups.
    """
    from edgar_signals import (
        create_companies_from_filings,
        fetch_filing_details,
        fetch_recent_form_d,
        match_and_insert_events,
        persist_filings,
        push_instant_alerts,
    )

    filings = await fetch_recent_form_d(days=2)
    if not filings:
        return {"filings": 0, "matched": 0, "created": 0}

    filings = await fetch_filing_details(filings)

    with Session(_engine) as session:
        stored = persist_filings(session, filings)
        matched_domains = match_and_insert_events(session, filings)
        session.commit()

    with Session(_engine) as session:
        created = await create_companies_from_filings(session, filings)
        session.commit()

    push_instant_alerts(matched_domains)
    logger.info(
        f"EDGAR poll: {len(filings)} filings, {stored} tech stored, "
        f"{len(matched_domains)} matched, {created} companies auto-created"
    )
    return {
        "filings": len(filings),
        "tech_stored": stored,
        "matched": len(matched_domains),
        "created": created,
    }


async def poll_product_hunt_task(ctx) -> dict:
    """Daily: match Product Hunt launches to companies; auto-create unmatched (capped)."""
    from news_signals import (
        create_companies_from_launches,
        fetch_product_hunt_launches,
        match_ph_launches,
    )

    launches = await fetch_product_hunt_launches()
    if not launches:
        return {"launches": 0, "matched": 0, "created": 0}

    with Session(_engine) as session:
        matched_domains = match_ph_launches(session, launches)
        session.commit()

    with Session(_engine) as session:
        created = await create_companies_from_launches(session, launches)
        session.commit()

    logger.info(
        f"Product Hunt poll: {len(launches)} launches, "
        f"{len(matched_domains)} matched, {created} auto-created"
    )
    return {"launches": len(launches), "matched": len(matched_domains), "created": created}


async def poll_job_posts_task(ctx) -> dict:
    """Daily: poll ATS for watched accounts only."""
    from job_posts import fetch_job_board_posts, extract_posts_with_gemini, persist_job_posts

    with Session(_engine) as session:
        rows = [dict(r) for r in session.execute(_SELECT_WATCHED_FOR_JOBS).mappings().all()]

    updated = 0
    for row in rows[:100]:
        posts, source = await fetch_job_board_posts(row["name"])
        if not posts:
            continue
        posts = extract_posts_with_gemini(posts)
        with Session(_engine) as session:
            persist_job_posts(session, row["id"], posts, source)
            session.commit()
        updated += 1
        await asyncio.sleep(0.5)

    return {"polled": updated}
