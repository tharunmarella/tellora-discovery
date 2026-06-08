"""
Tellora Discovery Service — entry point.

Self-contained weekly job: scrapes Apollo AND enriches everything it scraped,
end-to-end, in one process. It does NOT depend on the signal-worker — the
signal-worker only serves user-triggered (on-demand) enrichment from the app.

Usage:
  python __main__.py           # scrape + enrich all newly scraped companies
  python __main__.py --dry-run # 2 pages per profile, no writes, no enrichment

Railway cron schedule: 0 3 * * 0  (every Sunday 3 AM UTC)
"""

import asyncio
import logging
import sys

from config_logging import setup_logging

setup_logging()
logger = logging.getLogger("discovery")


async def _scrape_and_enrich(dry_run: bool) -> None:
    """Run the Apollo scrape, then enrich the newly-pending companies inline."""
    from service import run_discovery_scrape
    stats = await run_discovery_scrape()
    total = stats.get("total", 0)
    logger.info(f"Scrape done. {total} new companies added. Per-profile: {stats}")

    if dry_run or total <= 0:
        return

    # Enrich inline — the discovery job owns its companies end-to-end rather than
    # handing them off to the always-on signal-worker. signal_runner.run() pulls
    # every pending row (batched + concurrency-limited) and persists the signals.
    logger.info("Enriching newly scraped companies inline...")
    from signal_runner import run as run_enrichment
    await run_enrichment(limit=None, concurrency=5, batch_size=50, reset_failed=False)

    # Backfill headcount on older Apollo rows still missing it (free people-search proxy).
    logger.info("Backfilling Apollo headcount estimates for rows missing headcount...")
    from signal_enrichment import backfill_apollo_headcounts
    stats_hc = await backfill_apollo_headcounts()
    logger.info(f"Headcount backfill: {stats_hc}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    # Validate env first — fail fast before doing any work
    import settings as cfg  # noqa — triggers _require() validation

    if not cfg.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY / GOOGLE_API_KEY is not set — cannot enrich signals")
        sys.exit(1)

    # Ensure tables exist (idempotent — CREATE TABLE IF NOT EXISTS)
    from database import create_tables
    create_tables()

    if dry_run:
        logger.info("DRY RUN — max 2 pages per profile, no Jina calls, no DB writes")
        import settings
        settings.MAX_PAGES_PER_PROFILE = 2

    asyncio.run(_scrape_and_enrich(dry_run))
    sys.exit(0)


if __name__ == "__main__":
    main()
