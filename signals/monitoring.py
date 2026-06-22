"""
Scheduled monitoring tasks for the discovery ARQ worker.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from infra.lifespan import SERVICE_WORKER
from infra.sentry_telemetry import capture_task_failure
from signals.pipeline import enrich_company_signals
from signals.runner import persist_result

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
          OR dc.signal_enriched_at < NOW() - make_interval(days => :stale_days)
      )
    LIMIT :lim
""")

_SELECT_STALE_INDEX = text("""
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
    FROM discovery_company
    WHERE domain IS NOT NULL
      AND domain_resolved = true
      AND (
          signal_enriched_at IS NULL
          OR signal_enriched_at < NOW() - make_interval(days => :stale_days)
      )
    ORDER BY signal_enriched_at NULLS FIRST
    LIMIT :lim
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
            logger.error(f"Refresh failed for {row.get('name')}: {exc}", exc_info=True)
            capture_task_failure(
                exc,
                service=SERVICE_WORKER,
                task_name="refresh_company",
                stats={"company_id": company_id, "name": row.get("name"), "domain": row.get("domain")},
            )


async def refresh_watched_companies_task(ctx) -> dict:
    """Daily: re-enrich watched accounts older than WATCHED_STALE_DAYS."""
    with Session(_engine) as session:
        rows = [
            dict(r)
            for r in session.execute(
                _SELECT_WATCHED_STALE,
                {
                    "stale_days": cfg.WATCHED_STALE_DAYS,
                    "lim": cfg.WATCHED_REFRESH_LIMIT,
                },
            ).mappings().all()
        ]
    if not rows:
        return {"refreshed": 0}
    sem = asyncio.Semaphore(5)
    await asyncio.gather(*[_refresh_company(r, sem) for r in rows])
    logger.info(f"Refreshed {len(rows)} watched companies")
    return {"refreshed": len(rows)}


async def refresh_stale_index_task(ctx) -> dict:
    """Daily: refresh index companies stale beyond REFRESH_STALE_DAYS."""
    with Session(_engine) as session:
        rows = [
            dict(r)
            for r in session.execute(
                _SELECT_STALE_INDEX,
                {
                    "stale_days": cfg.REFRESH_STALE_DAYS,
                    "lim": cfg.REFRESH_BATCH_CAP,
                },
            ).mappings().all()
        ]
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
    from signals.sources.edgar import (
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
    from signals.sources.news import (
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
    from signals.job_posts import fetch_job_board_posts, extract_posts_with_gemini, persist_job_posts

    with Session(_engine) as session:
        rows = [dict(r) for r in session.execute(_SELECT_WATCHED_FOR_JOBS).mappings().all()]

    updated = 0
    for row in rows[:100]:
        posts, source = await fetch_job_board_posts(row["name"], domain=row.get("domain"))
        if not posts:
            continue
        posts = extract_posts_with_gemini(posts)
        with Session(_engine) as session:
            persist_job_posts(session, row["id"], posts, source)
            session.commit()
        updated += 1
        await asyncio.sleep(0.5)

    return {"polled": updated}


async def refresh_headcounts_task(ctx) -> dict:
    """Daily: re-fetch Apollo headcount estimates for rows with stale values."""
    from scrape.headcount_backfill import refresh_stale_apollo_headcounts

    return await refresh_stale_apollo_headcounts()


_SELECT_RECONCILE = text("""
    SELECT id
    FROM discovery_company
    WHERE (
        signal_enrichment_status = 'processing'
        AND (
            signal_last_attempt_at IS NULL
            OR signal_last_attempt_at < NOW() - (:stale_minutes * INTERVAL '1 minute')
        )
    )
    OR (
        signal_enrichment_status IN ('failed', 'partial')
        AND COALESCE(signal_attempt_count, 0) < :max_attempts
        AND (
            signal_last_attempt_at IS NULL
            OR signal_last_attempt_at < NOW() - (
                INTERVAL '1 minute' * POWER(2, LEAST(COALESCE(signal_attempt_count, 0), 4))
            )
        )
    )
    ORDER BY signal_last_attempt_at NULLS FIRST
    LIMIT :batch
""")

_RESET_TO_PENDING = text("""
    UPDATE discovery_company
    SET signal_enrichment_status = 'pending', updated_at = NOW()
    WHERE id = :company_id
""")


async def reconcile_pending_task(ctx) -> dict:
    """
    Re-enqueue stuck processing orphans and retryable failed/partial rows.
    Runs every 10 minutes on the on-demand worker.
    """
    import arq
    from arq.connections import RedisSettings

    stale_minutes = cfg.SIGNAL_PROCESSING_STALE_MINUTES
    max_attempts = cfg.SIGNAL_RECONCILE_MAX_ATTEMPTS
    batch = cfg.SIGNAL_RECONCILE_BATCH

    with Session(_engine) as session:
        rows = session.execute(
            _SELECT_RECONCILE,
            {"stale_minutes": stale_minutes, "max_attempts": max_attempts, "batch": batch},
        ).mappings().all()
        company_ids = [str(r["id"]) for r in rows]

    if not company_ids:
        return {"requeued": 0}

    pool = await arq.create_pool(RedisSettings.from_dsn(cfg.REDIS_URL))
    requeued = 0
    try:
        with Session(_engine) as session:
            for company_id in company_ids:
                session.execute(_RESET_TO_PENDING, {"company_id": company_id})
            session.commit()

        for company_id in company_ids:
            await pool.enqueue_job(
                "enrich_company_task",
                company_id,
                _queue_name="arq:ondemand",
            )
            requeued += 1
    finally:
        await pool.aclose()

    logger.info(f"Reconcile re-queued {requeued} companies for signal enrichment")
    return {"requeued": requeued}
