"""
Tellora Discovery Service — entry point.

Self-contained weekly job: scrapes Apollo AND enriches everything it scraped,
end-to-end, in one process. It does NOT depend on the signal-worker — the
signal-worker only serves user-triggered (on-demand) enrichment from the app.

Usage:
  python __main__.py                        # scrape + enrich + weekly headcount cap
  python __main__.py --headcount-backfill-only  # backfill ALL missing headcounts
  python __main__.py --dry-run              # 2 pages per profile, no writes

Railway cron schedule: 0 3 * * 0  (every Sunday 3 AM UTC)
"""

import asyncio
import logging
import os
import sys

from config_logging import setup_logging

setup_logging()
logger = logging.getLogger("discovery")


async def _run_headcount_backfill(*, run_all: bool) -> None:
    from headcount_backfill import backfill_apollo_headcounts

    if run_all:
        logger.info("Starting full headcount backfill (all eligible rows)...")
    else:
        logger.info("Backfilling Apollo headcount estimates for rows missing headcount...")
    stats_hc = await backfill_apollo_headcounts(run_all=run_all)
    logger.info(f"Headcount backfill: {stats_hc}")


async def _scrape_and_enrich(dry_run: bool) -> None:
    """Run the Apollo scrape, then enrich the newly-pending companies inline."""
    from service import run_discovery_scrape

    stats = await run_discovery_scrape()
    total = stats.get("total", 0)
    logger.info(f"Scrape done. {total} new companies added. Per-profile: {stats}")

    if dry_run:
        return

    if total > 0:
        # Enrich inline — the discovery job owns its companies end-to-end rather than
        # handing them off to the always-on signal-worker. signal_runner.run() pulls
        # every pending row (batched + concurrency-limited) and persists the signals.
        logger.info("Enriching newly scraped companies inline...")
        from signal_runner import run as run_enrichment

        await run_enrichment(limit=None, concurrency=5, batch_size=50, reset_failed=False)

    await _run_headcount_backfill(run_all=False)


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    headcount_only = (
        "--headcount-backfill-only" in sys.argv
        or os.getenv("HEADCOUNT_BACKFILL_ONLY", "").strip() == "1"
    )

    # Validate env first — fail fast before doing any work
    import settings as cfg  # noqa — triggers _require() validation

    from database import create_tables

    create_tables()

    if headcount_only:
        asyncio.run(_run_headcount_backfill(run_all=True))
        sys.exit(0)

    if not cfg.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY / GOOGLE_API_KEY is not set — cannot enrich signals")
        sys.exit(1)

    if dry_run:
        logger.info("DRY RUN — max 2 pages per profile, skips enrichment + headcount backfill")
        import settings

        settings.MAX_PAGES_PER_PROFILE = 2

    asyncio.run(_scrape_and_enrich(dry_run))
    sys.exit(0)


if __name__ == "__main__":
    main()
