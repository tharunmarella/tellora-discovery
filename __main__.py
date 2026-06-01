"""
Tellora Discovery Service — entry point.

Usage:
  python __main__.py           # run the scrape
  python __main__.py --dry-run # 2 pages per profile, no writes

Railway cron schedule: 0 3 * * 0  (every Sunday 3 AM UTC)
"""

import asyncio
import logging
import sys

from config_logging import setup_logging

setup_logging()
logger = logging.getLogger("discovery")


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    # Validate env first — fail fast before doing any work
    import settings as cfg  # noqa — triggers _require() validation

    # Ensure tables exist (idempotent — CREATE TABLE IF NOT EXISTS)
    from database import create_tables
    create_tables()

    if dry_run:
        logger.info("DRY RUN — max 2 pages per profile, no Jina calls, no DB writes")
        import settings
        settings.MAX_PAGES_PER_PROFILE = 2

    from service import run_discovery_scrape
    stats = asyncio.run(run_discovery_scrape())

    total = stats.get("total", 0)
    logger.info(f"Done. {total} new companies added. Per-profile: {stats}")

    # Enqueue signal enrichment for all pending companies onto the bulk ARQ queue.
    # The bulk worker (arq worker.BulkWorkerSettings) will process them; the
    # reconciler cron provides a safety net for any that are dropped.
    if not dry_run and total > 0:
        logger.info("Enqueueing signal enrichment jobs for newly scraped companies...")
        from sqlalchemy import text as _text
        from database import engine as _engine
        from sqlalchemy.orm import Session as _Session
        import arq as _arq
        from arq.connections import RedisSettings as _RedisSettings
        import settings as _cfg
        import asyncio as _asyncio

        with _Session(_engine) as _db:
            pending_ids = [
                str(r[0]) for r in _db.execute(_text(
                    "SELECT id FROM discovery_company "
                    "WHERE signal_enrichment_status = 'pending' "
                    "AND domain IS NOT NULL AND domain_resolved = true"
                )).all()
            ]

        async def _enqueue_bulk():
            pool = await _arq.create_pool(
                _RedisSettings.from_dsn(_cfg.REDIS_URL),
                default_queue_name="arq:bulk",
            )
            for company_id in pending_ids:
                await pool.enqueue_job("enrich_company_task", company_id)
            await pool.aclose()
            logger.info(f"Enqueued {len(pending_ids)} companies onto arq:bulk")

        _asyncio.run(_enqueue_bulk())

    sys.exit(0)


if __name__ == "__main__":
    main()
