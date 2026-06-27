"""
Weekly discovery cron — scrape Apollo, enrich (inline or enqueue), headcount backfill.

Invoked by `python __main__.py` (Railway cron: 0 3 * * 0,3 — Sun + Wed).
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass

from infra.axiom_logger import axiom_logger
from infra.lifespan import SERVICE_CRON
from infra.sentry_telemetry import capture_task_failure

logger = logging.getLogger("discovery.cron.weekly")


@dataclass(frozen=True)
class WeeklyCronArgs:
    dry_run: bool = False
    headcount_only: bool = False
    force: bool = False


def parse_args(argv: list[str] | None = None) -> WeeklyCronArgs:
    args = argv if argv is not None else sys.argv
    return WeeklyCronArgs(
        dry_run="--dry-run" in args,
        headcount_only=(
            "--headcount-backfill-only" in args
            or os.getenv("HEADCOUNT_BACKFILL_ONLY", "").strip() == "1"
        ),
        force="--force" in args or os.getenv("DISCOVERY_SCRAPE_FORCE", "").strip() == "1",
    )


def ensure_scheduled_or_skip(args: WeeklyCronArgs) -> bool:
    """
    Return True to proceed. False when outside DISCOVERY_SCRAPE_CRON (logged).

    Manual override: ``--force`` or ``DISCOVERY_SCRAPE_FORCE=1``.
    """
    if args.headcount_only:
        return True

    import settings as cfg
    from cron.scrape_schedule import should_run_discovery_scrape

    if should_run_discovery_scrape(
        cron_expr=cfg.DISCOVERY_SCRAPE_CRON,
        force=args.force,
        schedule_disabled=cfg.DISCOVERY_SCRAPE_SCHEDULE_DISABLED,
    ):
        return True

    logger.info(
        "Skipping discovery scrape — outside schedule %s "
        "(use --force or DISCOVERY_SCRAPE_FORCE=1)",
        cfg.DISCOVERY_SCRAPE_CRON,
    )
    return False


def validate_args(args: WeeklyCronArgs) -> None:
    import settings as cfg  # noqa — triggers _require() validation

    if not args.headcount_only and not cfg.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY / GOOGLE_API_KEY is not set — cannot enrich signals")
        sys.exit(1)


def apply_dry_run_settings(args: WeeklyCronArgs) -> None:
    if not args.dry_run:
        return
    logger.info("DRY RUN — max 2 pages per profile, skips enrichment + headcount backfill")
    import settings

    settings.MAX_PAGES_PER_PROFILE = 2


async def run_headcount_backfill(*, run_all: bool) -> None:
    from scrape.headcount_backfill import backfill_apollo_headcounts

    task = "headcount_backfill_all" if run_all else "headcount_backfill_weekly"
    start = time.perf_counter()
    try:
        if run_all:
            logger.info("Starting full headcount backfill (all eligible rows)...")
        else:
            logger.info("Backfilling Apollo headcount estimates for rows missing headcount...")
        stats_hc = await backfill_apollo_headcounts(run_all=run_all)
        logger.info(f"Headcount backfill: {stats_hc}")
        await axiom_logger.log_task_run(
            task_name=task,
            success=True,
            duration_ms=(time.perf_counter() - start) * 1000,
            stats=stats_hc if isinstance(stats_hc, dict) else {"result": str(stats_hc)},
        )
    except Exception as exc:
        await axiom_logger.log_task_run(
            task_name=task,
            success=False,
            duration_ms=(time.perf_counter() - start) * 1000,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )
        capture_task_failure(exc, service=SERVICE_CRON, task_name=task)
        raise


async def scrape_and_enrich(*, dry_run: bool) -> None:
    """Run the Apollo scrape, then enrich the newly-pending companies inline."""
    import settings as cfg
    from scrape.service import run_discovery_scrape

    start = time.perf_counter()
    stats: dict = {}
    try:
        stats = await run_discovery_scrape()
        total = stats.get("total", 0)
        logger.info(f"Scrape done. {total} new companies added. Per-profile: {stats}")
        await axiom_logger.log_task_run(
            task_name="discovery_scrape",
            success=True,
            duration_ms=(time.perf_counter() - start) * 1000,
            stats=stats,
        )

        if dry_run:
            return

        if total > 0:
            enrich_start = time.perf_counter()
            enrich_stats: dict = {}
            if cfg.DISCOVERY_INLINE_ENRICH:
                logger.info("Enriching newly scraped companies inline...")
                from signals.runner import run as run_enrichment

                await run_enrichment(limit=None, concurrency=5, batch_size=50, reset_failed=False)
                task_name = "discovery_enrich_inline"
            else:
                logger.info("Enqueueing newly scraped companies for async enrichment...")
                from signals.enqueue import enqueue_pending_enrichment

                enrich_stats = await enqueue_pending_enrichment(limit=None)
                task_name = "discovery_enrich_enqueue"
            await axiom_logger.log_task_run(
                task_name=task_name,
                success=True,
                duration_ms=(time.perf_counter() - enrich_start) * 1000,
                stats={"scraped_total": total, **enrich_stats},
            )

        await run_headcount_backfill(run_all=False)
    except Exception as exc:
        await axiom_logger.log_task_run(
            task_name="discovery_weekly",
            success=False,
            duration_ms=(time.perf_counter() - start) * 1000,
            stats=stats or None,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )
        capture_task_failure(exc, service=SERVICE_CRON, task_name="discovery_weekly", stats=stats or None)
        raise


async def execute(args: WeeklyCronArgs) -> None:
    if args.headcount_only:
        await run_headcount_backfill(run_all=True)
    else:
        await scrape_and_enrich(dry_run=args.dry_run)
