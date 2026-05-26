"""
Signal Enrichment Runner

Batch-processes discovery_company rows that need signal enrichment.
Mirrors the checkpoint architecture from service.py.

Usage:
  python signal_runner.py              # process all pending
  python signal_runner.py --limit 100  # test run: only 100 companies
  python signal_runner.py --reset-failed  # retry previously failed

Architecture:
  - Query: signal_enrichment_status = 'pending' AND domain IS NOT NULL AND domain_resolved = true
  - Batch size: 50
  - Concurrency: 5 companies processed in parallel per batch (semaphore-limited)
  - After each batch: commit + log progress checkpoint to stdout
  - Rate limiting: 1s delay between companies within a batch
  - Crash recovery: re-query pending rows — committed rows have status='enriched'
  - Companies with no domain: mark 'skipped'
"""

import argparse
import asyncio
import logging
import sys
import time
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import settings as cfg
from signal_enrichment import enrich_company_signals

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("signal_runner")


# ── DB setup ───────────────────────────────────────────────────────────────

def _make_engine():
    return create_engine(cfg.DATABASE_URL, pool_pre_ping=True)


# ── Queries ────────────────────────────────────────────────────────────────

_SELECT_PENDING = text("""
    SELECT id, name, domain, description, industry, raw_meta
    FROM   discovery_company
    WHERE  signal_enrichment_status = 'pending'
    AND    domain IS NOT NULL
    AND    domain_resolved = true
    ORDER  BY created_at ASC
    LIMIT  :batch_size
    OFFSET :offset
""")

_SELECT_NO_DOMAIN = text("""
    SELECT id FROM discovery_company
    WHERE  signal_enrichment_status = 'pending'
    AND    (domain IS NULL OR domain_resolved = false)
""")

_COUNT_PENDING = text("""
    SELECT COUNT(*) FROM discovery_company
    WHERE  signal_enrichment_status = 'pending'
    AND    domain IS NOT NULL
    AND    domain_resolved = true
""")

_UPDATE_SIGNAL = text("""
    UPDATE discovery_company SET
        company_summary          = :company_summary,
        buying_signals           = CAST(:buying_signals AS jsonb),
        signal_score             = :signal_score,
        funding_stage            = :funding_stage,
        total_raised             = :total_raised,
        headcount                = :headcount,
        hiring_roles             = CAST(:hiring_roles AS jsonb),
        hiring_count             = :hiring_count,
        tech_stack               = CAST(:tech_stack AS jsonb),
        description_embedding    = CAST(:description_embedding AS vector),
        search_tsv               = to_tsvector('english', :tsv_text),
        signal_enriched_at       = :signal_enriched_at,
        signal_enrichment_status = :signal_enrichment_status,
        updated_at               = NOW()
    WHERE id = :id
""")

_SKIP_NO_DOMAIN = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'skipped', updated_at = NOW()
    WHERE  signal_enrichment_status = 'pending'
    AND    (domain IS NULL OR domain_resolved = false)
""")


# ── Batch worker ───────────────────────────────────────────────────────────

async def _process_company(row: dict, sem: asyncio.Semaphore) -> dict:
    """Enrich a single company under the semaphore. Returns result dict."""
    company_id   = row["id"]
    company_name = row["name"]
    domain       = row["domain"]

    async with sem:
        try:
            result = await enrich_company_signals(
                company_id=company_id,
                company_name=company_name,
                domain=domain,
                description=row.get("description"),
                industry=row.get("industry"),
                raw_meta=row.get("raw_meta"),
            )
        except Exception as exc:
            logger.error(f"[{company_name}] Unhandled error: {exc}", exc_info=True)
            result = {
                "signal_enrichment_status": "failed",
                "signal_score": 0,
                "buying_signals": [],
                "company_summary": None,
                "funding_stage": None,
                "total_raised": None,
                "headcount": None,
                "hiring_roles": [],
                "hiring_count": 0,
                "tech_stack": [],
                "description_embedding": None,
                "tsv_text": row.get("description", "") or "",
                "signal_enriched_at": None,
            }
        result["id"] = company_id
        # Brief pause after each company to be polite to external APIs
        await asyncio.sleep(1)
        return result


async def _process_batch(rows: list[dict], concurrency: int = 5) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)
    tasks = [_process_company(row, sem) for row in rows]
    return await asyncio.gather(*tasks)


# ── DB write ───────────────────────────────────────────────────────────────

import json as _json


def _write_batch(session: Session, results: list[dict]) -> tuple[int, int]:
    """Write enrichment results to DB. Returns (success_count, fail_count)."""
    ok = fail = 0
    for r in results:
        emb = r.get("description_embedding")
        try:
            session.execute(_UPDATE_SIGNAL, {
                "id":                       r["id"],
                "company_summary":          r.get("company_summary"),
                "buying_signals":           _json.dumps(r.get("buying_signals") or []),
                "signal_score":             r.get("signal_score") or 0,
                "funding_stage":            r.get("funding_stage"),
                "total_raised":             r.get("total_raised"),
                "headcount":                r.get("headcount"),
                "hiring_roles":             _json.dumps(r.get("hiring_roles") or []),
                "hiring_count":             r.get("hiring_count") or 0,
                "tech_stack":               _json.dumps(r.get("tech_stack") or []),
                "description_embedding":    _json.dumps(emb) if emb else None,
                "tsv_text":                 (r.get("tsv_text") or "").strip() or " ",
                "signal_enriched_at":       r.get("signal_enriched_at"),
                "signal_enrichment_status": r.get("signal_enrichment_status", "enriched"),
            })
            if r.get("signal_enrichment_status") != "failed":
                ok += 1
            else:
                fail += 1
        except Exception as exc:
            logger.error(f"DB write failed for {r['id']}: {exc}")
            fail += 1
    session.commit()
    return ok, fail


# ── Main runner ────────────────────────────────────────────────────────────

async def run(limit: Optional[int], concurrency: int, batch_size: int, reset_failed: bool) -> None:
    engine = _make_engine()

    with Session(engine) as session:
        # Optionally reset previously failed companies to pending
        if reset_failed:
            count = session.execute(text(
                "UPDATE discovery_company SET signal_enrichment_status = 'pending' "
                "WHERE signal_enrichment_status = 'failed'"
            )).rowcount
            session.commit()
            logger.info(f"Reset {count} failed companies to pending")

        # Mark companies without a domain as skipped
        skipped = session.execute(_SKIP_NO_DOMAIN).rowcount
        session.commit()
        if skipped:
            logger.info(f"Skipped {skipped} companies with no resolved domain")

        # Count total to process
        total_pending = session.execute(_COUNT_PENDING).scalar() or 0
        if limit:
            total_pending = min(total_pending, limit)

        if total_pending == 0:
            logger.info("No companies pending signal enrichment. Exiting.")
            return

        logger.info(f"Starting signal enrichment for {total_pending} companies "
                    f"(batch_size={batch_size}, concurrency={concurrency})")

    total_ok   = 0
    total_fail = 0
    processed  = 0
    offset     = 0
    start_time = time.time()

    while processed < total_pending:
        remaining = total_pending - processed
        this_batch = min(batch_size, remaining)

        with Session(engine) as session:
            rows_raw = session.execute(
                _SELECT_PENDING,
                {"batch_size": this_batch, "offset": 0}  # always offset=0 since processed rows get updated
            ).mappings().all()
            rows = [dict(r) for r in rows_raw]

        if not rows:
            logger.info("No more pending rows — done.")
            break

        batch_start = time.time()
        results = await _process_batch(rows, concurrency=concurrency)

        with Session(engine) as session:
            ok, fail = _write_batch(session, results)

        total_ok   += ok
        total_fail += fail
        processed  += len(rows)
        elapsed     = time.time() - start_time
        rate        = processed / elapsed if elapsed > 0 else 0

        logger.info(
            f"Batch done: {processed}/{total_pending} companies processed | "
            f"ok={ok} fail={fail} | "
            f"rate={rate:.1f}/s | "
            f"eta={((total_pending - processed) / rate / 60):.0f}min"
            if rate > 0 else
            f"Batch done: {processed}/{total_pending} | ok={ok} fail={fail}"
        )

        # 2s cooldown between batches to give APIs breathing room
        if processed < total_pending:
            await asyncio.sleep(2)

    total_elapsed = time.time() - start_time
    logger.info(
        f"\n{'='*60}\n"
        f"Signal enrichment complete!\n"
        f"  Total processed : {processed}\n"
        f"  Enriched (ok)   : {total_ok}\n"
        f"  Failed          : {total_fail}\n"
        f"  Elapsed         : {total_elapsed/60:.1f} minutes\n"
        f"{'='*60}"
    )


# ── CLI ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run signal enrichment on all pending discovery_company rows."
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process this many companies (useful for test runs)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=5,
        help="Max parallel companies per batch (default: 5)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Companies per batch before committing (default: 50)"
    )
    parser.add_argument(
        "--reset-failed", action="store_true",
        help="Reset companies with status=failed back to pending before running"
    )
    args = parser.parse_args()

    if not cfg.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY / GOOGLE_API_KEY is not set — cannot run signal enrichment")
        sys.exit(1)

    asyncio.run(run(
        limit=args.limit,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        reset_failed=args.reset_failed,
    ))


if __name__ == "__main__":
    main()
