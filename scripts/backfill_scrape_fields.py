"""
Backfill scrape-time fields on existing discovery_company rows.

Re-runs the same Serper + Gemini lookup used at Apollo ingest to fill
linkedin_url, website_url, ceo_name, raw_meta (keywords/use_case), logo_url,
and other scrape metadata — without touching signal enrichment fields.

Run before a full signal re-enrich so downstream synthesis has richer context.

    PYTHONPATH=. python scripts/backfill_scrape_fields.py --dry-run
    PYTHONPATH=. python scripts/backfill_scrape_fields.py --limit 50
    PYTHONPATH=. python scripts/backfill_scrape_fields.py --run-all
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from infra.lifespan import bootstrap
from scrape.scrape_fields_backfill import backfill_scrape_fields, count_eligible_rows

bootstrap(server_name="discovery.scrape_backfill")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill scrape-time company metadata")
    parser.add_argument("--limit", type=int, default=None, help="Max rows per batch")
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Loop batches until no eligible rows remain",
    )
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count eligible rows only (no API calls)",
    )
    parser.add_argument(
        "--source",
        default="apollo",
        help="discovery_company.source filter (default: apollo)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    eligible = count_eligible_rows(source=args.source)
    print(f"Eligible rows (source={args.source}): {eligible}")

    stats = await backfill_scrape_fields(
        limit=args.limit,
        run_all=args.run_all,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        source=args.source,
    )
    print(f"Done: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
