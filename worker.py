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

import redis.asyncio as aioredis
from arq import cron
from arq.connections import RedisSettings
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from infra.axiom_arq import after_job_end, on_job_failure
from infra.lifespan import SERVICE_WORKER, bootstrap, shutdown as lifespan_shutdown, startup as lifespan_startup
from signals.monitoring import (
    poll_edgar_form_d_task,
    poll_job_posts_task,
    poll_product_hunt_task,
    reconcile_pending_task,
    refresh_stale_index_task,
    refresh_watched_companies_task,
)

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
    RETURNING id, name, domain, description, industry, raw_meta, headcount, headquarters
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
        refresh_watched_companies_task,
        refresh_stale_index_task,
        poll_job_posts_task,
        poll_edgar_form_d_task,
        poll_product_hunt_task,
        reconcile_pending_task,
    ]
    queue_name = "arq:ondemand"
    redis_settings = _redis_settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_failure = on_job_failure
    after_job_end = after_job_end
    max_jobs = 5
    max_tries = _ENRICH_MAX_TRIES
    job_timeout = 600
    cron_jobs = [
        cron(refresh_watched_companies_task, weekday=0, hour=4, minute=0),
        cron(refresh_stale_index_task, weekday=0, hour=5, minute=0),
        cron(poll_job_posts_task, hour=6, minute=0),
        cron(poll_edgar_form_d_task, hour=10, minute=0),
        cron(poll_product_hunt_task, hour=11, minute=0),
        cron(reconcile_pending_task, minute={0, 10, 20, 30, 40, 50}),
    ]
