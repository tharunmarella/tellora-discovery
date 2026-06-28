"""
Scheduler health snapshots for discovery enrichment and scrape pipelines.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from infra.axiom_logger import axiom_logger
from infra.lifespan import SERVICE_WORKER

logger = logging.getLogger("discovery.scheduler_metrics")

_engine = make_engine(pool_recycle=300, pool_size=2, max_overflow=2)

_SNAPSHOT_SQL = text("""
    SELECT
        (SELECT COUNT(*) FROM discovery_company
         WHERE signal_enrichment_status = 'pending'
           AND domain IS NOT NULL AND domain_resolved = true) AS pending_enrich,
        (SELECT COUNT(*) FROM discovery_company
         WHERE signal_enrichment_status = 'processing'
           AND (
               signal_last_attempt_at IS NULL
               OR signal_last_attempt_at < NOW() - (:stale_minutes * INTERVAL '1 minute')
           )) AS processing_stale,
        (SELECT COUNT(*) FROM discovery_company
         WHERE signal_enrichment_status IN ('failed', 'partial')
           AND COALESCE(signal_attempt_count, 0) < :max_attempts) AS failed_retryable,
        (SELECT COUNT(*) FROM discovery_company
         WHERE domain IS NOT NULL AND domain_resolved = true
           AND (
               signal_enriched_at IS NULL
               OR signal_enriched_at < NOW() - make_interval(days => :refresh_stale_days)
           )) AS stale_index,
        (SELECT COUNT(*) FROM discovery_company
         WHERE ats_board IS NOT NULL) AS ats_board_cached,
        (SELECT MAX(completed_at) FROM discovery_progress
         WHERE status = 'completed') AS last_scrape_completed_at,
        (SELECT MAX(started_at) FROM discovery_progress
         WHERE status = 'running') AS last_scrape_started_at
""")


def collect_scheduler_metrics(session: Session) -> dict[str, Any]:
    row = session.execute(
        _SNAPSHOT_SQL,
        {
            "stale_minutes": cfg.SIGNAL_PROCESSING_STALE_MINUTES,
            "max_attempts": cfg.SIGNAL_RECONCILE_MAX_ATTEMPTS,
            "refresh_stale_days": cfg.REFRESH_STALE_DAYS,
        },
    ).mappings().first()
    if not row:
        return {}

    metrics = dict(row)
    for key in ("last_scrape_completed_at", "last_scrape_started_at"):
        val = metrics.get(key)
        if isinstance(val, datetime):
            metrics[key] = val.isoformat()
    return metrics


async def log_scheduler_health(
    *,
    service: str = SERVICE_WORKER,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Collect metrics and emit to Axiom as task_run scheduler_health."""
    from database import run_with_db_retry

    start = time.perf_counter()

    def _collect() -> dict[str, Any]:
        with Session(_engine) as session:
            return collect_scheduler_metrics(session)

    metrics = run_with_db_retry(_collect)
    if extra:
        metrics.update(extra)

    logger.info("[scheduler_health] %s", metrics)
    await axiom_logger.log_task_run(
        task_name="scheduler_health",
        success=True,
        duration_ms=(time.perf_counter() - start) * 1000,
        service=service,
        stats=metrics,
    )
    return metrics


async def log_scheduler_health_task(ctx) -> dict:
    """ARQ cron: snapshot scheduler backlog metrics."""
    metrics = await log_scheduler_health()
    return metrics
