"""
Enqueue pending discovery_company rows for async signal enrichment on arq:ondemand.
"""

from __future__ import annotations

import logging
from typing import Optional

import arq
from arq.connections import RedisSettings
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine

logger = logging.getLogger("discovery.enqueue")

_engine = make_engine(pool_recycle=300, pool_size=2, max_overflow=2)

_SELECT_PENDING = text("""
    SELECT id
    FROM discovery_company
    WHERE signal_enrichment_status = 'pending'
      AND domain IS NOT NULL
      AND domain_resolved = true
      AND (:since_hours IS NULL OR created_at >= NOW() - (:since_hours * INTERVAL '1 hour'))
    ORDER BY created_at ASC
    LIMIT :lim
""")


async def enqueue_pending_enrichment(
    *,
    limit: Optional[int] = None,
    created_since_hours: Optional[int] = None,
) -> dict:
    """
    Enqueue enrich_company_task for pending companies (does not process inline).
    """
    lim = limit if limit is not None else 10_000
    with Session(_engine) as session:
        rows = session.execute(
            _SELECT_PENDING,
            {"lim": lim, "since_hours": created_since_hours},
        ).mappings().all()
        company_ids = [str(r["id"]) for r in rows]

    if not company_ids:
        return {"enqueued": 0}

    pool = await arq.create_pool(RedisSettings.from_dsn(cfg.REDIS_URL))
    enqueued = 0
    try:
        for company_id in company_ids:
            await pool.enqueue_job(
                "enrich_company_task",
                company_id,
                _queue_name="arq:ondemand",
            )
            enqueued += 1
    finally:
        await pool.aclose()

    logger.info(
        "[enqueue_pending] enqueued %d companies (limit=%s since_hours=%s)",
        enqueued, limit, created_since_hours,
    )
    return {"enqueued": enqueued}


async def enqueue_scrape_pending_task(ctx) -> dict:
    """Daily safety net: enqueue pending rows from the last 24h."""
    return await enqueue_pending_enrichment(
        limit=cfg.ENQUEUE_SCRAPE_PENDING_LIMIT,
        created_since_hours=24,
    )
