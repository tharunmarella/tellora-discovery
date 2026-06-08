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
    RETURNING id, name, domain, description, industry, raw_meta, headcount
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
    'pending → processing' claim ensures only one worker does the work.

    Enqueued by the backend for user-triggered (on-demand) enrichment.
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
            existing_headcount=row.get("headcount"),
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
    On-demand enrichment worker: listens on arq:ondemand for user-triggered
    enrichment (single "Enrich" clicks + bulk on-demand actions) enqueued by
    the backend. Stays disjoint from the weekly discovery job, which scrapes
    and enriches its own companies inline.
    """
    functions = [enrich_company_task]
    queue_name = "arq:ondemand"
    redis_settings = _redis_settings
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 5
    job_timeout = 600
