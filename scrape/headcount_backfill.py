"""Apollo people-count headcount backfill for discovery_company rows."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from signals.pipeline import fetch_apollo_headcount

logger = logging.getLogger("discovery.headcount_backfill")


async def backfill_apollo_headcounts(
    limit: int | None = None,
    *,
    run_all: bool = False,
) -> dict:
    """
    Fill headcount on discovery_company rows missing it, using the free
    people-search proxy. Does not re-run full signal enrichment.

    Weekly cron: one batch capped by HEADCOUNT_BACKFILL_LIMIT (default 1000).
    --headcount-backfill-only: run_all=True loops until no eligible rows remain.
    """
    batch_size = int(getattr(cfg, "HEADCOUNT_BACKFILL_LIMIT", 1000))
    if run_all:
        cap = batch_size
    else:
        cap = limit if limit is not None else batch_size

    engine = make_engine()

    _select = text("""
        SELECT id, domain, name
        FROM   discovery_company
        WHERE  domain IS NOT NULL
        AND    (headcount IS NULL OR headcount = 0)
        ORDER  BY updated_at DESC
        LIMIT  :lim
    """)
    _update = text("""
        UPDATE discovery_company
        SET    headcount = :hc, updated_at = NOW()
        WHERE  id = :id
    """)

    total_filled = total_skipped = total_processed = 0
    batch_num = 0

    while True:
        batch_num += 1
        filled = skipped = 0
        with Session(engine) as session:
            rows = session.execute(_select, {"lim": cap}).mappings().all()
            if not rows:
                if batch_num == 1:
                    logger.info("[headcount_backfill] no rows missing headcount")
                break

            mode = "run_all" if run_all else f"cap={cap}"
            logger.info(
                f"[headcount_backfill] batch {batch_num}: {len(rows)} rows "
                f"({mode}, total so far: filled={total_filled}, skipped={total_skipped})"
            )
            for row in rows:
                domain = row["domain"]
                if not domain:
                    skipped += 1
                    continue
                res = await fetch_apollo_headcount(domain)
                hc = res.get("headcount_estimate")
                if hc:
                    session.execute(_update, {"hc": hc, "id": row["id"]})
                    filled += 1
                    logger.info(f"[headcount_backfill] {row['name']} ({domain}) → {hc}")
                else:
                    skipped += 1
                await asyncio.sleep(1.1)  # pace with Apollo free-tier limits
            session.commit()

        total_filled += filled
        total_skipped += skipped
        total_processed += len(rows)

        if not run_all or len(rows) < cap:
            break

    logger.info(
        f"[headcount_backfill] done — filled={total_filled}, "
        f"skipped={total_skipped}, processed={total_processed}"
    )
    return {
        "filled": total_filled,
        "skipped": total_skipped,
        "processed": total_processed,
        "batches": batch_num,
    }
