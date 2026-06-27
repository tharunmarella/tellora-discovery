"""
Tellora Discovery ARQ Worker — On-Demand Enrichment
===================================================

Always-on worker that serves user-triggered (on-demand) enrichment from the
app — both single "Enrich" clicks and bulk on-demand actions (select-many →
enrich). The backend enqueues enrich_company_task onto arq:ondemand.

  arq:ondemand  ← user-triggered enrichment (single + bulk on-demand)

This is intentionally disjoint from the weekly discovery job: the discovery
service (python __main__.py) scrapes AND enriches its own companies inline, so
nothing here ever blocks behind the weekly batch (and vice versa).

Running:
  arq worker.WorkerSettings
"""

import logging
import time

import redis.asyncio as aioredis
from arq import cron
from arq.connections import RedisSettings
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from infra.axiom_arq import after_job_end, on_job_failure
from infra.axiom_logger import axiom_logger
from infra.lifespan import SERVICE_WORKER, bootstrap, shutdown as lifespan_shutdown, startup as lifespan_startup
from cron.scrape_schedule import (
    DEFAULT_DISCOVERY_SCRAPE_CRON,
    discovery_scrape_recently_active,
    parse_scrape_cron,
    should_run_discovery_scrape,
)
from cron.weekly import WeeklyCronArgs, execute as run_weekly_cron, validate_args as validate_weekly_args
from signals.enqueue import enqueue_scrape_pending_task
from signals.monitoring import (
    daily_discovery_maintenance_task,
    import_jobhive_slugs_task,
    poll_edgar_form_d_task,
    poll_job_posts_task,
    poll_product_hunt_task,
    reconcile_pending_task,
    refresh_headcounts_task,
    refresh_stale_index_task,
    refresh_watched_companies_task,
)
from signals.scheduler_metrics import log_scheduler_health_task

bootstrap(server_name=SERVICE_WORKER)

logger = logging.getLogger("discovery.worker")

_engine = make_engine(
    pool_recycle=300,
    pool_size=3,
    max_overflow=5,
)

_ENRICH_MAX_TRIES = cfg.SIGNAL_ENRICH_MAX_TRIES

# ── Redis helpers ───────────────────────────────────────────────────────────

_CLAIM_AND_LOAD = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'processing',
           signal_last_attempt_at   = NOW(),
           signal_attempt_count     = COALESCE(signal_attempt_count, 0) + 1,
           updated_at               = NOW()
    WHERE  id = :company_id
    AND    signal_enrichment_status IN ('pending', 'processing')
    RETURNING id, name, domain, description, industry, raw_meta, headcount, headquarters, ats_board
""")

_MARK_FAILED = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'failed', updated_at = NOW()
    WHERE  id = :company_id
""")


# ── Core task ───────────────────────────────────────────────────────────────

async def enrich_company_task(ctx, company_id: str) -> dict:
    """
    Enrich a single company. Safe to enqueue multiple times — the atomic
    claim ensures only one worker does the work.

    Enqueued by the backend for user-triggered (on-demand) enrichment.
    """
    job_try = int(ctx.get("job_try") or 1)
    max_tries = int(ctx.get("max_tries") or _ENRICH_MAX_TRIES)
    logger.info(
        f"[enrich_company_task] Starting for company_id={company_id} "
        f"(try {job_try}/{max_tries})"
    )

    with Session(_engine) as session:
        row = session.execute(_CLAIM_AND_LOAD, {"company_id": company_id}).mappings().first()
        if row is None:
            logger.info(
                f"[enrich_company_task] Skipping {company_id} — "
                "not pending/processing (already claimed or enriched)"
            )
            return {"ok": False, "skipped": True, "company_id": company_id}
        row = dict(row)
        session.commit()

    company_name = row["name"]
    domain = row["domain"]

    try:
        from signals.pipeline import enrich_company_signals
        result = await enrich_company_signals(
            company_id=company_id,
            company_name=company_name,
            domain=domain or "",
            description=row.get("description"),
            industry=row.get("industry"),
            raw_meta=row.get("raw_meta"),
            existing_headcount=row.get("headcount"),
            existing_headquarters=row.get("headquarters"),
            existing_ats_board=row.get("ats_board"),
        )
    except Exception as exc:
        logger.error(
            f"[enrich_company_task] Enrichment failed for {company_name}: {exc}",
            exc_info=True,
        )
        if job_try >= max_tries:
            with Session(_engine) as session:
                session.execute(_MARK_FAILED, {"company_id": company_id})
                session.commit()
            logger.error(
                f"[enrich_company_task] Marked {company_id} failed after {job_try} tries"
            )
        else:
            logger.warning(
                f"[enrich_company_task] Will retry {company_id} "
                f"(try {job_try}/{max_tries}); leaving status=processing"
            )
        raise

    result["domain"] = domain

    with Session(_engine) as session:
        from signals.runner import persist_result
        persist_result(session, company_id, result)
        session.commit()

    status = result.get("signal_enrichment_status")
    if domain and status in ("enriched", "partial"):
        try:
            r = aioredis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
            await r.rpush(cfg.SIGNALS_READY_KEY, domain)
            await r.aclose()
            logger.info(f"[enrich_company_task] Pushed {domain} to {cfg.SIGNALS_READY_KEY}")
        except Exception as exc:
            logger.warning(f"[enrich_company_task] Redis notify failed: {exc}")

    logger.info(
        f"[enrich_company_task] Done for {company_name} ({domain}), "
        f"status={status}, score={result.get('signal_score')}"
    )
    return {
        "ok": True,
        "company_id": company_id,
        "signal_score": result.get("signal_score"),
        "status": status,
    }


async def run_discovery_scrape_task(ctx) -> dict:
    """
    Fallback scrape cron on the worker when the Railway cron service is absent
    or misconfigured. Skips if a scrape already ran recently (Railway path).
    """
    start = time.perf_counter()

    async def _skip(reason: str) -> dict:
        logger.info("[run_discovery_scrape_task] Skipping — %s", reason)
        await axiom_logger.log_task_run(
            task_name="discovery_scrape_fallback",
            success=True,
            duration_ms=(time.perf_counter() - start) * 1000,
            service=SERVICE_WORKER,
            stats={"skipped": True, "skipped_reason": reason},
        )
        return {"ok": True, "skipped": True, "reason": reason}

    if not cfg.DISCOVERY_SCRAPE_WORKER_FALLBACK:
        return await _skip("worker_fallback_disabled")

    if discovery_scrape_recently_active():
        return await _skip("recent_run")

    if not should_run_discovery_scrape(
        cron_expr=cfg.DISCOVERY_SCRAPE_CRON,
        schedule_disabled=cfg.DISCOVERY_SCRAPE_SCHEDULE_DISABLED,
    ):
        return await _skip("off_schedule")

    args = WeeklyCronArgs()
    validate_weekly_args(args)
    logger.info(
        "[run_discovery_scrape_task] Starting worker fallback scrape (%s)",
        cfg.DISCOVERY_SCRAPE_CRON,
    )
    await run_weekly_cron(args)
    await axiom_logger.log_task_run(
        task_name="discovery_scrape_fallback",
        success=True,
        duration_ms=(time.perf_counter() - start) * 1000,
        service=SERVICE_WORKER,
        stats={"skipped": False},
    )
    return {"ok": True, "skipped": False}


# ── ARQ lifecycle hooks ─────────────────────────────────────────────────────

async def startup(ctx):
    await lifespan_startup(
        server_name=SERVICE_WORKER,
        create_tables=True,
        axiom_background_flush=True,
    )


async def shutdown(ctx):
    await lifespan_shutdown(server_name=SERVICE_WORKER)


# ── Redis settings ──────────────────────────────────────────────────────────

_redis_settings = RedisSettings.from_dsn(cfg.REDIS_URL)


# ── Worker settings ─────────────────────────────────────────────────────────

class WorkerSettings:
    """
    On-demand enrichment worker: listens on arq:ondemand for user-triggered
    enrichment (single "Enrich" clicks + bulk on-demand actions) enqueued by
    the backend. Stays disjoint from the weekly discovery job, which scrapes
    and enriches its own companies inline.
    """
    functions = [
        enrich_company_task,
        run_discovery_scrape_task,
        refresh_watched_companies_task,
        refresh_stale_index_task,
        refresh_headcounts_task,
        poll_job_posts_task,
        poll_edgar_form_d_task,
        poll_product_hunt_task,
        reconcile_pending_task,
        log_scheduler_health_task,
        enqueue_scrape_pending_task,
        import_jobhive_slugs_task,
        daily_discovery_maintenance_task,
    ]
    queue_name = "arq:ondemand"
    redis_settings = _redis_settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_failure = on_job_failure
    after_job_end = after_job_end
    max_jobs = cfg.SIGNAL_ENRICH_MAX_JOBS
    max_tries = _ENRICH_MAX_TRIES
    job_timeout = 600
    _scrape_schedule = parse_scrape_cron(
        cfg.DISCOVERY_SCRAPE_CRON or DEFAULT_DISCOVERY_SCRAPE_CRON
    )
    cron_jobs = [
        # Jobhive slug warm-up — Sunday 2:30 UTC (before Sun 3 AM scrape).
        cron(import_jobhive_slugs_task, weekday={6}, hour=2, minute=30),
        # Safety net: enqueue pending rows from last 24h.
        cron(enqueue_scrape_pending_task, hour=3, minute=30),
        # Scheduler health snapshot before daily maintenance block.
        cron(log_scheduler_health_task, hour=3, minute=55),
        # Fallback discovery scrape — mirrors DISCOVERY_SCRAPE_CRON (default Sun+Wed 3 AM UTC).
        cron(
            run_discovery_scrape_task,
            weekday=set(_scrape_schedule.python_weekdays),
            hour=_scrape_schedule.hour,
            minute=_scrape_schedule.minute,
        ),
        # Consolidated daily maintenance (watched → tiered refresh → jobs → headcount → ingest).
        cron(daily_discovery_maintenance_task, hour=4, minute=0),
        cron(reconcile_pending_task, minute={0, 10, 20, 30, 40, 50}),
    ]
