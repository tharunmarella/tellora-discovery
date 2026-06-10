"""
Signal Enrichment Runner — Manual Backfill CLI
===============================================

One-shot batch tool for backfilling signal enrichment on pending
discovery_company rows. Use this for:

  - Initial data loads / migrations
  - Manually retrying a large batch of failed companies
  - Ad-hoc dev/test runs

In production, enrichment runs in two disjoint places:
  - Weekly discovery cron (__main__.py) → scrapes + enriches inline via run()
  - Signal worker (worker.py, arq:ondemand) → user-triggered on-demand enrich

Usage:
  python signal_runner.py                 # backfill all pending
  python signal_runner.py --limit 100     # test run: only 100 companies
  python signal_runner.py --reset-failed  # retry previously failed

Architecture:
  - Query: signal_enrichment_status = 'pending' AND domain IS NOT NULL AND domain_resolved = true
  - Batch size: 50
  - Concurrency: 5 companies processed in parallel per batch (semaphore-limited)
  - After each batch: commit + log progress checkpoint to stdout
  - Rate limiting: 1s delay between companies within a batch
  - Companies with no domain: mark 'skipped'
"""

import argparse
import asyncio
import json as _json
import logging
import sys
import time
from datetime import datetime, timezone
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
    url = cfg.DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True)


# ── SQL statements ──────────────────────────────────────────────────────────

_SELECT_PENDING = text("""
    SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
    FROM   discovery_company
    WHERE  signal_enrichment_status = 'pending'
    AND    domain IS NOT NULL
    AND    domain_resolved = true
    ORDER  BY created_at ASC
    LIMIT  :batch_size
    OFFSET 0
""")

_COUNT_PENDING = text("""
    SELECT COUNT(*) FROM discovery_company
    WHERE  signal_enrichment_status = 'pending'
    AND    domain IS NOT NULL
    AND    domain_resolved = true
""")

_SKIP_NO_DOMAIN = text("""
    UPDATE discovery_company
    SET    signal_enrichment_status = 'skipped', updated_at = NOW()
    WHERE  signal_enrichment_status = 'pending'
    AND    (domain IS NULL OR domain_resolved = false)
""")

_UPDATE_SIGNAL = text("""
    UPDATE discovery_company SET
        company_summary          = :company_summary,
        buying_signals           = CAST(:buying_signals AS jsonb),
        signal_score             = :signal_score,
        funding_stage            = :funding_stage,
        total_raised             = :total_raised,
        headcount                = COALESCE(NULLIF(headcount, 0), :headcount),
        hiring_roles             = CAST(:hiring_roles AS jsonb),
        hiring_count             = :hiring_count,
        tech_stack               = CAST(:tech_stack AS jsonb),
        description_embedding    = CAST(:description_embedding AS vector),
        search_tsv               = to_tsvector('english', :tsv_text),
        hq_city                  = :hq_city,
        hq_region                = :hq_region,
        hq_country               = :hq_country,
        signal_enriched_at       = :signal_enriched_at,
        signal_enrichment_status = :signal_enrichment_status,
        updated_at               = NOW()
    WHERE id = :id
""")


# ── Shared persist helper (used by both this runner and worker.py) ──────────

def persist_result(session: Session, company_id: str, result: dict) -> bool:
    """
    Write a single enrichment result dict to discovery_company.
    Returns True on success, False on DB error.

    Called by _write_batch (batch runner) and directly by worker.py (ARQ task).
    The result dict is the value returned by enrich_company_signals().

    Headcount is only written when the row has no value yet (NULL or 0) — scrape
    and backfill estimates are never overwritten by signal enrichment.
    """
    emb = result.get("description_embedding")
    try:
        session.execute(_UPDATE_SIGNAL, {
            "id":                       company_id,
            "company_summary":          result.get("company_summary"),
            "buying_signals":           _json.dumps(result.get("buying_signals") or []),
            "signal_score":             result.get("signal_score") or 0,
            "funding_stage":            result.get("funding_stage"),
            "total_raised":             result.get("total_raised"),
            "headcount":                result.get("headcount"),
            "hiring_roles":             _json.dumps(result.get("hiring_roles") or []),
            "hiring_count":             result.get("hiring_count") or 0,
            "tech_stack":               _json.dumps(result.get("tech_stack") or []),
            "description_embedding":    _json.dumps(emb) if emb else None,
            "tsv_text":                 (result.get("tsv_text") or "").strip() or " ",
            "hq_city":                  result.get("hq_city"),
            "hq_region":                result.get("hq_region"),
            "hq_country":               result.get("hq_country"),
            "signal_enriched_at":       result.get("signal_enriched_at") or datetime.now(timezone.utc),
            "signal_enrichment_status": result.get("signal_enrichment_status", "enriched"),
        })
        return True
    except Exception as exc:
        logger.error(f"DB write failed for {company_id}: {exc}")
        return False


SIGNALS_READY_KEY = "tellora:signals_ready"


def _notify_signals_ready(domains: list[str]) -> None:
    """Push enriched domains to Redis so the backend ARQ worker can re-queue waiting contacts."""
    if not domains:
        return
    try:
        import redis as _redis
        r = _redis.from_url(cfg.REDIS_URL, socket_connect_timeout=2)
        for domain in domains:
            r.rpush(SIGNALS_READY_KEY, domain)
        logger.info(f"Pushed {len(domains)} domain(s) to {SIGNALS_READY_KEY}")
    except Exception as exc:
        logger.warning(f"Could not notify Redis after signal enrichment: {exc}")


# ── Batch processing ────────────────────────────────────────────────────────

async def _process_company(row: dict, sem: asyncio.Semaphore) -> dict:
    """Enrich a single company under the semaphore. Returns result dict with 'id' set."""
    company_id   = str(row["id"])
    company_name = row["name"]

    async with sem:
        try:
            result = await enrich_company_signals(
                company_id=company_id,
                company_name=company_name,
                domain=row["domain"],
                description=row.get("description"),
                industry=row.get("industry"),
                raw_meta=row.get("raw_meta"),
                existing_headcount=row.get("headcount"),
                existing_headquarters=row.get("headquarters"),
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
        await asyncio.sleep(1)
        return result


async def _process_batch(rows: list[dict], concurrency: int = 5) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)
    return await asyncio.gather(*[_process_company(row, sem) for row in rows])


def _write_batch(session: Session, results: list[dict]) -> tuple[int, int]:
    """Write a batch of enrichment results. Returns (success_count, fail_count)."""
    ok = fail = 0
    for r in results:
        success = persist_result(session, r["id"], r)
        if success and r.get("signal_enrichment_status") != "failed":
            ok += 1
        else:
            fail += 1
    session.commit()
    return ok, fail


# ── Main runner (one-shot backfill) ────────────────────────────────────────

async def run(limit: Optional[int], concurrency: int, batch_size: int, reset_failed: bool) -> None:
    engine = _make_engine()

    with Session(engine) as session:
        if reset_failed:
            count = session.execute(text(
                "UPDATE discovery_company SET signal_enrichment_status = 'pending' "
                "WHERE signal_enrichment_status = 'failed'"
            )).rowcount
            session.commit()
            logger.info(f"Reset {count} failed companies to pending")

        skipped = session.execute(_SKIP_NO_DOMAIN).rowcount
        session.commit()
        if skipped:
            logger.info(f"Skipped {skipped} companies with no resolved domain")

        total_pending = session.execute(_COUNT_PENDING).scalar() or 0
        if limit:
            total_pending = min(total_pending, limit)

        if total_pending == 0:
            logger.info("No companies pending signal enrichment. Exiting.")
            return

        logger.info(
            f"Starting signal enrichment for {total_pending} companies "
            f"(batch_size={batch_size}, concurrency={concurrency})"
        )

    total_ok   = 0
    total_fail = 0
    processed  = 0
    start_time = time.time()

    while processed < total_pending:
        remaining  = total_pending - processed
        this_batch = min(batch_size, remaining)

        with Session(engine) as session:
            rows_raw = session.execute(
                _SELECT_PENDING,
                {"batch_size": this_batch},
            ).mappings().all()
            rows = [dict(r) for r in rows_raw]

        if not rows:
            logger.info("No more pending rows — done.")
            break

        results = await _process_batch(rows, concurrency=concurrency)

        with Session(engine) as session:
            ok, fail = _write_batch(session, results)

        enriched_domains = [
            rows[i]["domain"] for i, r in enumerate(results)
            if r.get("signal_enrichment_status") != "failed" and rows[i].get("domain")
        ]
        _notify_signals_ready(enriched_domains)

        total_ok   += ok
        total_fail += fail
        processed  += len(rows)
        elapsed     = time.time() - start_time
        rate        = processed / elapsed if elapsed > 0 else 0

        if rate > 0:
            eta_min = (total_pending - processed) / rate / 60
            logger.info(
                f"Batch done: {processed}/{total_pending} | "
                f"ok={ok} fail={fail} | rate={rate:.1f}/s | eta={eta_min:.0f}min"
            )
        else:
            logger.info(f"Batch done: {processed}/{total_pending} | ok={ok} fail={fail}")

        if processed < total_pending:
            await asyncio.sleep(2)

    total_elapsed = time.time() - start_time
    logger.info(
        f"\n{'='*60}\n"
        f"Signal enrichment backfill complete!\n"
        f"  Total processed : {processed}\n"
        f"  Enriched (ok)   : {total_ok}\n"
        f"  Failed          : {total_fail}\n"
        f"  Elapsed         : {total_elapsed/60:.1f} minutes\n"
        f"{'='*60}"
    )


# ── CLI ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "One-shot backfill: run signal enrichment on pending discovery_company rows. "
            "Also used inline by the weekly discovery job (__main__.py)."
        )
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
