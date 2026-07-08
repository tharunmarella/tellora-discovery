"""
Scheduled monitoring tasks for the discovery ARQ worker.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine, run_with_db_retry
from infra.lifespan import SERVICE_WORKER
from infra.sentry_telemetry import capture_task_failure
from signals.pipeline import enrich_company_signals
from signals.runner import persist_result

logger = logging.getLogger("discovery.monitoring")

_engine = make_engine(pool_recycle=300, pool_size=3, max_overflow=5)

_SELECT_WATCHED_STALE_CANDIDATES = text("""
    WITH candidates AS (
        SELECT dc.id, dc.name, dc.domain, dc.description, dc.industry,
               dc.raw_meta, dc.headcount, dc.headquarters, c.org_id,
               ROW_NUMBER() OVER (
                   PARTITION BY c.org_id
                   ORDER BY dc.signal_enriched_at NULLS FIRST
               ) AS org_rank
        FROM discovery_company dc
        JOIN company c ON c.discovery_company_id = dc.id AND c.deleted_at IS NULL
        WHERE dc.domain IS NOT NULL
          AND dc.domain_resolved = true
          AND (
              dc.signal_enriched_at IS NULL
              OR dc.signal_enriched_at < NOW() - make_interval(hours => :stale_hours)
          )
    )
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters,
           org_id, org_rank
    FROM candidates
    WHERE org_rank <= :org_budget
    ORDER BY org_id, org_rank
""")

_SELECT_STALE_INDEX = text("""
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
    FROM discovery_company dc
    WHERE domain IS NOT NULL
      AND domain_resolved = true
      AND (
          signal_enriched_at IS NULL
          OR signal_enriched_at < NOW() - make_interval(days => :stale_days)
      )
      AND NOT EXISTS (
          SELECT 1 FROM company c WHERE c.discovery_company_id = dc.id AND c.deleted_at IS NULL
      )
      AND NOT (
          (source_profiles IS NOT NULL AND cardinality(source_profiles) > 0)
          OR last_seen_at > NOW() - INTERVAL '90 days'
      )
    ORDER BY signal_enriched_at NULLS FIRST
    LIMIT :lim
""")

_SELECT_ICP_HOT_STALE = text("""
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
    FROM discovery_company dc
    WHERE domain IS NOT NULL
      AND domain_resolved = true
      AND (
          signal_enriched_at IS NULL
          OR signal_enriched_at < NOW() - make_interval(days => :stale_days)
      )
      AND NOT EXISTS (
          SELECT 1 FROM company c WHERE c.discovery_company_id = dc.id AND c.deleted_at IS NULL
      )
      AND (
          (source_profiles IS NOT NULL AND cardinality(source_profiles) > 0)
          OR last_seen_at > NOW() - INTERVAL '90 days'
      )
    ORDER BY last_seen_at DESC NULLS LAST
    LIMIT :lim
""")

_SELECT_ATS_CACHED = text("""
    SELECT id, name, domain, ats_board
    FROM discovery_company
    WHERE domain IS NOT NULL
      AND domain_resolved = true
      AND ats_board IS NOT NULL
    ORDER BY signal_enriched_at DESC NULLS LAST
    LIMIT :lim
""")

_SELECT_WATCHED_FOR_JOBS = text("""
    SELECT DISTINCT dc.id, dc.name, dc.domain, dc.ats_board
    FROM discovery_company dc
    JOIN company c ON c.discovery_company_id = dc.id AND c.deleted_at IS NULL
    WHERE dc.domain IS NOT NULL
""")


def select_watched_refresh_candidates(
    candidates: list[dict],
    *,
    global_cap: int,
) -> tuple[list[dict], dict[str, int]]:
    """
    Round-robin interleave per-org ranked candidates; dedupe shared discovery rows.
    Returns (rows to refresh, per-org slot consumption counts).
    """
    if not candidates or global_cap <= 0:
        return [], {}

    by_org_rank: dict[tuple[str, int], list[str]] = defaultdict(list)
    company_fields: dict[str, dict] = {}

    for row in candidates:
        cid = str(row["id"])
        org_id = str(row["org_id"])
        rank = int(row["org_rank"])
        by_org_rank[(org_id, rank)].append(cid)
        if cid not in company_fields:
            company_fields[cid] = {
                "id": row["id"],
                "name": row["name"],
                "domain": row["domain"],
                "description": row.get("description"),
                "industry": row.get("industry"),
                "raw_meta": row.get("raw_meta"),
                "headcount": row.get("headcount"),
                "headquarters": row.get("headquarters"),
            }

    org_ids = sorted({org for org, _ in by_org_rank.keys()})
    selected_ids: set[str] = set()
    selected: list[dict] = []
    org_stats: dict[str, int] = defaultdict(int)
    rank = 1
    max_rank = max(r for _, r in by_org_rank.keys()) if by_org_rank else 0

    while len(selected) < global_cap and rank <= max_rank:
        found_any = False
        for org_id in org_ids:
            if len(selected) >= global_cap:
                break
            for cid in by_org_rank.get((org_id, rank), []):
                if cid in selected_ids:
                    continue
                selected_ids.add(cid)
                selected.append(company_fields[cid])
                org_stats[org_id] += 1
                found_any = True
                if len(selected) >= global_cap:
                    break
        if not found_any:
            rank += 1
            continue
        rank += 1

    return selected, dict(org_stats)


def load_watched_stale_candidates(session: Session) -> list[dict]:
    rows = session.execute(
        _SELECT_WATCHED_STALE_CANDIDATES,
        {
            "stale_hours": cfg.WATCHED_STALE_HOURS,
            "org_budget": cfg.WATCHED_ORG_DAILY_BUDGET,
        },
    ).mappings().all()
    return [dict(r) for r in rows]


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


async def _refresh_rows(
    rows: list[dict],
    *,
    label: str,
    concurrency: int | None = None,
) -> dict:
    if not rows:
        return {"refreshed": 0}
    sem = asyncio.Semaphore(concurrency or cfg.WATCHED_REFRESH_CONCURRENCY)
    await asyncio.gather(*[_refresh_company(r, sem) for r in rows])
    logger.info("Refreshed %d %s companies", len(rows), label)
    return {"refreshed": len(rows)}


async def refresh_watched_companies_task(ctx) -> dict:
    """Daily: re-enrich watched accounts older than WATCHED_STALE_HOURS."""
    with Session(_engine) as session:
        candidates = load_watched_stale_candidates(session)
    rows, org_stats = select_watched_refresh_candidates(
        candidates,
        global_cap=cfg.WATCHED_REFRESH_GLOBAL_CAP,
    )
    result = await _refresh_rows(rows, label="watched")
    result["org_stats"] = org_stats
    result["candidates"] = len(candidates)
    result["selected"] = len(rows)
    return result


async def refresh_icp_hot_task(ctx) -> dict:
    """Daily: refresh ICP-hot index companies (source_profiles or recent scrape)."""
    with Session(_engine) as session:
        rows = [
            dict(r)
            for r in session.execute(
                _SELECT_ICP_HOT_STALE,
                {
                    "stale_days": cfg.REFRESH_ICP_STALE_DAYS,
                    "lim": cfg.REFRESH_ICP_CAP,
                },
            ).mappings().all()
        ]
    result = await _refresh_rows(rows, label="icp-hot")
    result["tier"] = "icp_hot"
    return result


async def refresh_cold_index_task(ctx) -> dict:
    """Daily: refresh cold index companies beyond REFRESH_STALE_DAYS."""
    cold_cap = max(
        0,
        cfg.REFRESH_BATCH_CAP - cfg.WATCHED_REFRESH_GLOBAL_CAP - cfg.REFRESH_ICP_CAP,
    )
    if cold_cap == 0:
        return {"refreshed": 0, "tier": "cold"}
    with Session(_engine) as session:
        rows = [
            dict(r)
            for r in session.execute(
                _SELECT_STALE_INDEX,
                {
                    "stale_days": cfg.REFRESH_STALE_DAYS,
                    "lim": cold_cap,
                },
            ).mappings().all()
        ]
    result = await _refresh_rows(rows, label="cold")
    result["tier"] = "cold"
    return result


async def refresh_stale_index_task(ctx) -> dict:
    """Backward-compatible alias — runs ICP-hot then cold tiers."""
    icp = await refresh_icp_hot_task(ctx)
    cold = await refresh_cold_index_task(ctx)
    return {
        "refreshed": icp.get("refreshed", 0) + cold.get("refreshed", 0),
        "icp_hot": icp.get("refreshed", 0),
        "cold": cold.get("refreshed", 0),
    }


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
    """
    Daily: cheap ATS-board poll (index) + full re-enrich path for watched accounts.
    """
    from signals.job_posts import (
        extract_posts_with_gemini,
        fetch_cached_ats_board_posts,
        fetch_job_board_posts,
        persist_job_posts,
    )

    ats_updated = 0
    with Session(_engine) as session:
        ats_rows = [
            dict(r)
            for r in session.execute(
                _SELECT_ATS_CACHED, {"lim": cfg.JOB_POLL_ATS_CAP}
            ).mappings().all()
        ]

    for row in ats_rows:
        ats_board = row.get("ats_board")
        if not isinstance(ats_board, dict):
            continue
        posts, source = await fetch_cached_ats_board_posts(ats_board)
        if not posts:
            continue

        with Session(_engine) as session:
            existing = session.execute(
                text("""
                    SELECT external_id, title
                    FROM discovery_job_post
                    WHERE company_id = :cid AND closed_at IS NULL
                """),
                {"cid": row["id"]},
            ).mappings().all()
            known = {str(r["external_id"]): r["title"] for r in existing}

        new_or_changed = [
            p for p in posts
            if str(p.get("external_id") or "") not in known
            or known.get(str(p.get("external_id") or "")) != p.get("title")
        ]
        if new_or_changed:
            enriched = extract_posts_with_gemini(new_or_changed)
            by_id = {str(p.get("external_id")): p for p in enriched}
            for p in posts:
                ext = str(p.get("external_id") or "")
                if ext in by_id:
                    p.update(by_id[ext])

        with Session(_engine) as session:
            persist_job_posts(session, row["id"], posts, source)
            session.commit()
        ats_updated += 1
        await asyncio.sleep(0.25)

    with Session(_engine) as session:
        watched_rows = [
            dict(r) for r in session.execute(_SELECT_WATCHED_FOR_JOBS).mappings().all()
        ]

    watched_updated = 0
    for row in watched_rows[: cfg.JOB_POLL_FULL_ENRICH_CAP]:
        posts, source, _ = await fetch_job_board_posts(
            row["name"],
            domain=row.get("domain"),
            ats_board=row.get("ats_board"),
        )
        if not posts:
            continue
        posts = extract_posts_with_gemini(posts)
        with Session(_engine) as session:
            persist_job_posts(session, row["id"], posts, source)
            session.commit()
        watched_updated += 1
        await asyncio.sleep(0.5)

    return {
        "ats_polled": ats_updated,
        "watched_polled": watched_updated,
        "polled": ats_updated + watched_updated,
    }


async def import_jobhive_slugs_task(ctx) -> dict:
    """Weekly: warm ats_board from jobhive CSVs before scrape enrich wave."""
    from signals.jobhive_import import apply_jobhive_import, get_jobhive_index

    limit = cfg.JOBHIVE_IMPORT_LIMIT or None
    index = get_jobhive_index(force_reload=True)
    if index is None:
        return {"scanned": 0, "matched": 0, "applied": 0, "error": "index_load_failed"}
    with Session(_engine) as session:
        stats = apply_jobhive_import(
            session,
            index=index,
            limit=limit,
            only_missing=True,
        )
    logger.info("[jobhive_import] %s", stats)
    return stats


async def daily_discovery_maintenance_task(ctx) -> dict:
    """Daily consolidated maintenance — ordered steps with per-step stats."""
    import time

    from signals.scheduler_metrics import log_scheduler_health

    steps: dict = {}
    for name, coro in (
        ("scheduler_health", log_scheduler_health()),
        ("watched", refresh_watched_companies_task(ctx)),
        ("icp_hot", refresh_icp_hot_task(ctx)),
        ("cold", refresh_cold_index_task(ctx)),
        ("job_posts", poll_job_posts_task(ctx)),
        ("headcounts", refresh_headcounts_task(ctx)),
        ("edgar", poll_edgar_form_d_task(ctx)),
        ("product_hunt", poll_product_hunt_task(ctx)),
    ):
        start = time.perf_counter()
        try:
            steps[name] = await coro
            steps[name]["duration_ms"] = (time.perf_counter() - start) * 1000
        except Exception as exc:
            steps[name] = {
                "error": str(exc),
                "duration_ms": (time.perf_counter() - start) * 1000,
            }
            logger.error("[daily_maintenance] step %s failed: %s", name, exc, exc_info=True)
            capture_task_failure(
                exc,
                service=SERVICE_WORKER,
                task_name=f"daily_maintenance_{name}",
            )
    logger.info("[daily_maintenance] complete — steps=%s", list(steps.keys()))
    return steps


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
    ORDER BY
        CASE WHEN signal_enrichment_status = 'processing' THEN 0 ELSE 1 END,
        signal_last_attempt_at NULLS FIRST
    LIMIT :batch
""")

_COUNT_STUCK_PROCESSING = text("""
    SELECT COUNT(*) AS cnt
    FROM discovery_company
    WHERE signal_enrichment_status = 'processing'
      AND (
          signal_last_attempt_at IS NULL
          OR signal_last_attempt_at < NOW() - (:stale_minutes * INTERVAL '1 minute')
      )
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

    from signals.scheduler_metrics import log_scheduler_health

    stale_minutes = cfg.SIGNAL_PROCESSING_STALE_MINUTES
    max_attempts = cfg.SIGNAL_RECONCILE_MAX_ATTEMPTS
    batch = cfg.SIGNAL_RECONCILE_BATCH

    health = await log_scheduler_health(extra={"phase": "pre_reconcile"})

    def _load_candidates() -> tuple[list[str], int]:
        with Session(_engine) as session:
            rows = session.execute(
                _SELECT_RECONCILE,
                {"stale_minutes": stale_minutes, "max_attempts": max_attempts, "batch": batch},
            ).mappings().all()
            company_ids = [str(r["id"]) for r in rows]
            still_stuck = session.execute(
                _COUNT_STUCK_PROCESSING,
                {"stale_minutes": stale_minutes},
            ).scalar() or 0
            return company_ids, int(still_stuck)

    company_ids, still_stuck = run_with_db_retry(_load_candidates)

    if not company_ids:
        return {
            "requeued": 0,
            "still_stuck_count": int(still_stuck),
            "pending_enrich": health.get("pending_enrich"),
        }

    pool = await arq.create_pool(RedisSettings.from_dsn(cfg.REDIS_URL))
    requeued = 0
    try:
        def _reset_to_pending() -> None:
            with Session(_engine) as session:
                for company_id in company_ids:
                    session.execute(_RESET_TO_PENDING, {"company_id": company_id})
                session.commit()

        run_with_db_retry(_reset_to_pending)

        for company_id in company_ids:
            await pool.enqueue_job(
                "enrich_company_task",
                company_id,
                _queue_name="arq:ondemand",
            )
            requeued += 1
    finally:
        await pool.aclose()

    def _count_stuck_after() -> int:
        with Session(_engine) as session:
            return int(
                session.execute(
                    _COUNT_STUCK_PROCESSING,
                    {"stale_minutes": stale_minutes},
                ).scalar() or 0
            )

    still_stuck_after = run_with_db_retry(_count_stuck_after)

    logger.info(
        "Reconcile re-queued %d companies (still_stuck=%d)",
        requeued,
        still_stuck_after,
    )
    return {
        "requeued": requeued,
        "reconciled_count": requeued,
        "still_stuck_count": int(still_stuck_after),
    }
