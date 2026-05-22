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
    sys.exit(0)


if __name__ == "__main__":
    main()
