"""
Tellora Discovery ARQ Worker
============================

A single worker pool listens on one queue and handles all enrichment —
both user-triggered ("Enrich" clicks) and batch/scrape-triggered — plus the
reconciler cron:

  arq:discovery  ← all enrichment jobs (enrich_company_task)

If bulk volume ever grows enough to delay user-triggered enrichment, split
this back into separate on-demand/bulk pools on dedicated queues (ARQ has no
in-queue priority, so isolation requires separate queues + processes).

Running:
  arq worker.WorkerSettings
"""

import logging

import redis.asyncio as aioredis
from arq import cron
from arq.connections import RedisSettings
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import settings as cfg

logger = logging.getLogger("discovery.worker")

# ── DB engine (module-level; workers are long-lived processes) ──────────────

_db_url = cfg.DATABASE_URL
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

_engine = create_engine(
    _db_url,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=3,
    max_overflow=5,
)

# ── Redis helpers ───────────────────────────────────────────────────────────

SIGNALS_READY_KEY = "tellora:signals_ready"

_CLAIM_AND_LOAD = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'processing',
           updated_at               = NOW()
    WHERE  id = :company_id
    AND    signal_enrichment_status = 'pending'
    RETURNING id, name, domain, description, industry, raw_meta
""")

_MARK_FAILED = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'failed', updated_at = NOW()
    WHERE  id = :company_id
""")

# Reconciler: find rows stuck at 'pending' or 'processing' for >15 min
_STALE_PENDING = text("""
    SELECT id FROM discovery_company
    WHERE  signal_enrichment_status IN ('pending', 'processing')
    AND    domain IS NOT NULL
    AND    domain_resolved = true
    AND    updated_at < NOW() - INTERVAL '15 minutes'
    LIMIT  100
""")


# ── Core task ───────────────────────────────────────────────────────────────

async def enrich_company_task(ctx, company_id: str) -> dict:
    """
    Enrich a single company. Safe to enqueue multiple times — the atomic
    'pending → processing' claim ensures only one worker does the work.

    Used by both the on-demand and bulk worker pools.
    """
    logger.info(f"[enrich_company_task] Starting for company_id={company_id}")

    with Session(_engine) as session:
        row = session.execute(_CLAIM_AND_LOAD, {"company_id": company_id}).mappings().first()
        if row is None:
            logger.info(f"[enrich_company_task] Skipping {company_id} — not pending (already claimed or enriched)")
            return {"ok": False, "skipped": True, "company_id": company_id}
        row = dict(row)

    company_name = row["name"]
    domain = row["domain"]

    try:
        from signal_enrichment import enrich_company_signals
        result = await enrich_company_signals(
            company_id=company_id,
            company_name=company_name,
            domain=domain,
            description=row.get("description"),
            industry=row.get("industry"),
            raw_meta=row.get("raw_meta"),
        )
    except Exception as exc:
        logger.error(f"[enrich_company_task] Enrichment failed for {company_name}: {exc}", exc_info=True)
        with Session(_engine) as session:
            session.execute(_MARK_FAILED, {"company_id": company_id})
            session.commit()
        raise  # let ARQ retry

    # Persist result via the shared helper (also used by the CLI backfill runner)
    with Session(_engine) as session:
        from signal_runner import persist_result
        persist_result(session, company_id, result)
        session.commit()

    # Notify backend so waiting contacts get re-queued
    if domain and result.get("signal_enrichment_status") != "failed":
        try:
            r = aioredis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
            await r.rpush(SIGNALS_READY_KEY, domain)
            await r.aclose()
            logger.info(f"[enrich_company_task] Pushed {domain} to {SIGNALS_READY_KEY}")
        except Exception as exc:
            logger.warning(f"[enrich_company_task] Redis notify failed: {exc}")

    logger.info(f"[enrich_company_task] Done for {company_name} ({domain}), score={result.get('signal_score')}")
    return {"ok": True, "company_id": company_id, "signal_score": result.get("signal_score")}


# ── Reconciler cron (runs in bulk worker only) ──────────────────────────────

async def reconcile_pending_task(ctx) -> dict:
    """
    Safety-net cron: find companies stuck at 'pending' or 'processing' for
    >15 min (missed/dropped enqueue, or worker crash mid-job) and re-enqueue
    them onto arq:discovery.

    Runs every 10 minutes in the worker.
    """
    with Session(_engine) as session:
        rows = session.execute(_STALE_PENDING).mappings().all()
        stale_ids = [str(r["id"]) for r in rows]

    if not stale_ids:
        return {"reconciled": 0}

    logger.info(f"[reconcile_pending_task] Re-enqueueing {len(stale_ids)} stale companies onto arq:discovery")

    pool = ctx.get("redis")
    if pool is None:
        logger.warning("[reconcile_pending_task] No Redis pool in ctx, skipping re-enqueue")
        return {"reconciled": 0}

    import arq
    discovery_pool = await arq.create_pool(
        RedisSettings.from_dsn(cfg.REDIS_URL),
        default_queue_name="arq:discovery",
    )
    for company_id in stale_ids:
        await discovery_pool.enqueue_job("enrich_company_task", company_id)
    await discovery_pool.aclose()

    logger.info(f"[reconcile_pending_task] Re-enqueued {len(stale_ids)} companies")
    return {"reconciled": len(stale_ids)}


# ── ARQ lifecycle hooks ─────────────────────────────────────────────────────

async def startup(ctx):
    logger.info("Discovery ARQ worker starting up")


async def shutdown(ctx):
    logger.info("Discovery ARQ worker shutting down")


# ── Redis settings ──────────────────────────────────────────────────────────

_redis_settings = RedisSettings.from_dsn(cfg.REDIS_URL)


# ── Worker settings ─────────────────────────────────────────────────────────

class WorkerSettings:
    """
    Single discovery worker pool: listens on arq:discovery and handles both
    user-triggered (on-demand) and batch/scrape-triggered enrichment, plus the
    reconciler cron. Split into separate pools later if bulk volume starts
    delaying user-triggered "Enrich" requests.
    """
    functions = [enrich_company_task, reconcile_pending_task]
    queue_name = "arq:discovery"
    redis_settings = _redis_settings
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 5
    job_timeout = 600
    cron_jobs = [
        # Reconciler: re-enqueue stale/dropped companies every 10 minutes
        cron(reconcile_pending_task, minute={0, 10, 20, 30, 40, 50}),
    ]
