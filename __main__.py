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
import sys

from cron.weekly import apply_dry_run_settings, execute, parse_args, validate_args
from infra.lifespan import SERVICE_CRON, bootstrap, lifespan

bootstrap(server_name=SERVICE_CRON)


def main() -> None:
    args = parse_args()
    validate_args(args)
    apply_dry_run_settings(args)

    async def run() -> None:
        async with lifespan(server_name=SERVICE_CRON):
            await execute(args)

    asyncio.run(run())
    sys.exit(0)


if __name__ == "__main__":
    main()
