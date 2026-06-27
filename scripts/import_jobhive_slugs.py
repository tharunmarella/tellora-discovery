#!/usr/bin/env python3
"""
Bootstrap discovery_company.ats_board from jobhive ats-companies CSVs.

Matches Tellora companies by domain stem or normalized name, optionally
validates with a live ATS fetch + validate_board_match, then updates JSONB.

Usage:
  python scripts/import_jobhive_slugs.py --dry-run
  python scripts/import_jobhive_slugs.py --apply --limit 50
  python scripts/import_jobhive_slugs.py --apply --local-dir ./jobhive-csv
  python scripts/import_jobhive_slugs.py --apply --no-validate   # trust CSV only

Requires DATABASE_URL in .env (same as discovery service).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from sqlalchemy.orm import Session

from database import make_engine
from signals.job_posts import verify_ats_slug
from signals.jobhive_import import (
    SUPPORTED_ATS,
    build_ats_board,
    find_jobhive_match,
    load_index_from_dir,
    load_index_from_github,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("import_jobhive")

_SELECT = """
SELECT id, name, domain, ats_board
FROM discovery_company
WHERE domain IS NOT NULL
  AND domain_resolved = true
  {ats_filter}
ORDER BY last_seen_at DESC
{limit_clause}
"""


async def _validate_match(
    name: str,
    domain: str | None,
    ats: str,
    slug: str,
    sem: asyncio.Semaphore,
) -> bool:
    async with sem:
        ok = await verify_ats_slug(name, domain, ats, slug)
        await asyncio.sleep(0.25)
        return ok


async def run(args: argparse.Namespace) -> int:
    if args.local_dir:
        index = load_index_from_dir(Path(args.local_dir))
    else:
        logger.info("Downloading jobhive CSVs from GitHub (%s)...", ", ".join(SUPPORTED_ATS))
        index = load_index_from_github()
    logger.info("Loaded %d slug rows across %d ATS types", index.total_rows, len(index.by_slug))

    ats_filter = ""
    if args.only_missing:
        ats_filter = "AND (ats_board IS NULL OR ats_board = 'null'::jsonb)"

    limit_clause = f"LIMIT {int(args.limit)}" if args.limit else ""
    query = _SELECT.format(ats_filter=ats_filter, limit_clause=limit_clause)

    engine = make_engine()
    rows: list[dict] = []
    with Session(engine) as session:
        for row in session.execute(text(query)).mappings():
            rows.append(dict(row))

    logger.info("Scanning %d discovery_company rows", len(rows))

    matched = 0
    validated = 0
    applied = 0
    skipped_existing = 0
    sem = asyncio.Semaphore(5)

    pending_writes: list[tuple[str, dict, str, str, str]] = []

    for row in rows:
        existing = row.get("ats_board")
        if existing and not args.force:
            skipped_existing += 1
            continue

        hit = find_jobhive_match(row["name"], row.get("domain"), index)
        if not hit:
            continue
        ats, slug = hit
        matched += 1

        if args.validate:
            ok = await _validate_match(row["name"], row.get("domain"), ats, slug, sem)
            if not ok:
                logger.info(
                    "Reject %s (%s) — %s/%s failed live validation",
                    row["name"], row.get("domain"), ats, slug,
                )
                continue
            validated += 1

        board = build_ats_board(ats, slug)
        pending_writes.append((row["id"], board, row["name"], ats, slug))

        if args.dry_run:
            logger.info(
                "[dry-run] %s (%s) → %s/%s",
                row["name"], row.get("domain"), ats, slug,
            )

    if not args.dry_run and pending_writes:
        with Session(engine) as session:
            for company_id, board, name, ats, slug in pending_writes:
                session.execute(
                    text(
                        "UPDATE discovery_company SET ats_board = CAST(:ats_board AS jsonb) WHERE id = :id"
                    ),
                    {"id": company_id, "ats_board": json.dumps(board)},
                )
                applied += 1
                logger.info("Updated %s → %s/%s", name, ats, slug)
            session.commit()

    logger.info(
        "Done — matched=%d validated=%d applied=%d skipped_existing=%d dry_run=%s",
        matched, validated, applied, skipped_existing, args.dry_run,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Import jobhive ATS slugs into ats_board")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write updates to DB (default is dry-run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print matches without writing (default unless --apply)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max companies to scan")
    parser.add_argument(
        "--local-dir",
        type=Path,
        default=None,
        help="Directory with greenhouse.csv, lever.csv, … (skip GitHub download)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip live ATS fetch validation (faster, less safe)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing ats_board entries",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        default=True,
        help="Only rows without ats_board (default)",
    )
    parser.add_argument(
        "--all",
        dest="only_missing",
        action="store_false",
        help="Include rows that already have ats_board (use with --force to replace)",
    )
    args = parser.parse_args()

    if args.apply:
        args.dry_run = False
    elif not args.dry_run:
        args.dry_run = True

    args.validate = not args.no_validate

    raise SystemExit(asyncio.run(run(args)) or 0)


if __name__ == "__main__":
    main()
