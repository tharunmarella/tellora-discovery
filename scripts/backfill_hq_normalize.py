"""
Backfill normalized HQ fields on discovery_company.

Normalizes raw `headquarters` strings into:
  - hq_city    (canonical English city, GeoNames-style)
  - hq_region  (US: ISO 3166-2 state code; non-US: subdivision name)
  - hq_country (ISO 3166-1 alpha-2)

Only updates rows where headquarters is set and hq_city is still NULL.
Deduplicates by distinct headquarters string (~2k values → ~85 Gemini calls at batch=25).

Prerequisites:
  - GEMINI_API_KEY or GOOGLE_API_KEY set
  - DATABASE_URL set (same as discovery service)

Usage:
  python scripts/backfill_hq_normalize.py --dry-run
  python scripts/backfill_hq_normalize.py --limit 50
  python scripts/backfill_hq_normalize.py --batch-size 25
  python scripts/backfill_hq_normalize.py --provider groq --model openai/gpt-oss-20b
  python scripts/backfill_hq_normalize.py --force
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import settings as cfg
from database import make_engine
from signals.pipeline import normalize_headquarters, normalize_headquarters_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_hq")


_ENSURE_COLUMNS = """
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS hq_city VARCHAR;
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS hq_region VARCHAR;
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS hq_country VARCHAR;
CREATE INDEX IF NOT EXISTS idx_discovery_hq_city ON discovery_company (hq_city);
CREATE INDEX IF NOT EXISTS idx_discovery_hq_country ON discovery_company (hq_country);
"""

_COUNT_DISTINCT = text("""
    SELECT COUNT(DISTINCT headquarters) AS distinct_hq,
           COUNT(*) AS row_count
    FROM discovery_company
    WHERE headquarters IS NOT NULL
      AND TRIM(headquarters) <> ''
      AND (:force OR hq_city IS NULL)
""")

_SELECT_DISTINCT = text("""
    SELECT DISTINCT headquarters
    FROM discovery_company
    WHERE headquarters IS NOT NULL
      AND TRIM(headquarters) <> ''
      AND (:force OR hq_city IS NULL)
    ORDER BY headquarters
    LIMIT :limit
""")

_UPDATE_ROWS = text("""
    UPDATE discovery_company
    SET    hq_city    = :hq_city,
           hq_region  = :hq_region,
           hq_country = :hq_country,
           updated_at = NOW()
    WHERE  headquarters = :headquarters
      AND  (:force OR hq_city IS NULL)
""")


def _make_engine():
    return make_engine()


def ensure_schema(session: Session) -> None:
    for stmt in _ENSURE_COLUMNS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            session.execute(text(stmt))
    session.commit()
    logger.info("Schema verified (hq_city / hq_region / hq_country + indexes)")


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _normalize_batch(
    batch: list[str],
    *,
    provider: str,
    model: str | None,
) -> dict[str, dict]:
    """LLM batch call with per-item fallback if the batch fails."""
    results = normalize_headquarters_batch(batch, provider=provider, model=model)
    if len(results) >= len(batch) * 0.8:
        return results

    logger.warning(
        f"Batch returned {len(results)}/{len(batch)} — falling back to singles for missing items"
    )
    for raw in batch:
        if raw not in results:
            results[raw] = normalize_headquarters(raw, provider=provider, model=model)
    return results


def _provider_ready(provider: str) -> bool:
    if provider == "groq":
        return bool(cfg.GROQ_API_KEY)
    return bool(cfg.GEMINI_API_KEY)


def _apply_batch(
    session: Session,
    batch: list[str],
    normalized: dict[str, dict],
    *,
    force: bool,
) -> tuple[int, int, int]:
    """Write one batch of HQ updates. Returns (ok, skipped, failed)."""
    ok = skipped = failed = 0
    for raw in batch:
        norm = normalized.get(raw) or {}
        if not any(norm.values()):
            skipped += 1
            continue
        updated = session.execute(_UPDATE_ROWS, {
            "headquarters": raw,
            "hq_city": norm.get("hq_city"),
            "hq_region": norm.get("hq_region"),
            "hq_country": norm.get("hq_country"),
            "force": force,
        }).rowcount
        if updated:
            ok += 1
        else:
            failed += 1
    session.commit()
    return ok, skipped, failed


def run(
    *,
    limit: int | None,
    dry_run: bool,
    force: bool,
    batch_size: int,
    delay_s: float,
    provider: str,
    model: str | None,
) -> None:
    engine = _make_engine()

    with Session(engine) as session:
        ensure_schema(session)
        counts = session.execute(_COUNT_DISTINCT, {"force": force}).mappings().one()
        distinct_hq = counts["distinct_hq"]
        row_count = counts["row_count"]
        to_process = min(distinct_hq, limit) if limit else distinct_hq
        num_batches = (to_process + batch_size - 1) // batch_size if to_process else 0

        model_label = model or (cfg.HQ_GROQ_MODEL if provider == "groq" else cfg.SIGNAL_GEMINI_MODEL)
        logger.info(
            f"Rows to update: {row_count} | distinct headquarters: {distinct_hq} | "
            f"will normalize: {to_process} in {num_batches} batch(es) of {batch_size} | "
            f"provider={provider} model={model_label}"
        )
        if dry_run:
            logger.info("Dry run — exiting without changes")
            return

        rows = session.execute(
            _SELECT_DISTINCT,
            {"force": force, "limit": to_process if limit else 10_000_000},
        ).mappings().all()
        distinct_values = [r["headquarters"] for r in rows]

    if not distinct_values:
        logger.info("Nothing to backfill.")
        return

    ok = skipped = failed = 0
    start = time.time()
    batches = _chunks(distinct_values, batch_size)

    for batch_idx, batch in enumerate(batches, start=1):
        batch_start = time.time()
        normalized = _normalize_batch(batch, provider=provider, model=model)
        with Session(engine) as session:
            b_ok, b_skipped, b_failed = _apply_batch(
                session, batch, normalized, force=force,
            )

        ok += b_ok
        skipped += b_skipped
        failed += b_failed
        done = min(batch_idx * batch_size, len(distinct_values))
        elapsed = time.time() - start
        rate = done / elapsed if elapsed > 0 else 0
        eta_min = (len(distinct_values) - done) / rate / 60 if rate > 0 else 0

        logger.info(
            f"Batch {batch_idx}/{len(batches)} done ({done}/{len(distinct_values)}) | "
            f"ok={b_ok} skipped={b_skipped} fail={b_failed} | "
            f"batch={time.time() - batch_start:.1f}s | eta={eta_min:.0f}min"
        )

        if delay_s > 0 and batch_idx < len(batches):
            time.sleep(delay_s)

    elapsed = time.time() - start
    logger.info(
        f"\n{'=' * 60}\n"
        f"HQ backfill complete\n"
        f"  Distinct normalized : {ok}\n"
        f"  Skipped (no result) : {skipped}\n"
        f"  Failed (0 rows)     : {failed}\n"
        f"  Elapsed             : {elapsed / 60:.1f} min\n"
        f"{'=' * 60}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill hq_city/hq_region/hq_country from headquarters")
    parser.add_argument("--limit", type=int, default=None, help="Only normalize this many distinct HQ strings")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only, no Gemini or DB writes")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-normalize rows that already have hq_city (default: only NULL hq_city)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=25,
        help="Headquarters strings per Gemini call (default: 25)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.3,
        help="Seconds between LLM batch calls (default: 0.3)",
    )
    parser.add_argument(
        "--provider", choices=["gemini", "groq"], default=None,
        help="LLM provider (default: HQ_NORMALIZE_PROVIDER env or gemini)",
    )
    parser.add_argument(
        "--model", default=None,
        help="Model override (default: gemini-3.1-flash-lite or openai/gpt-oss-20b)",
    )
    args = parser.parse_args()

    provider = (args.provider or cfg.HQ_NORMALIZE_PROVIDER or "gemini").strip().lower()
    if not _provider_ready(provider):
        key = "GROQ_API_KEY" if provider == "groq" else "GEMINI_API_KEY / GOOGLE_API_KEY"
        logger.error(f"{key} is not set for provider={provider}")
        sys.exit(1)

    if args.batch_size < 1:
        logger.error("--batch-size must be >= 1")
        sys.exit(1)

    run(
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
        batch_size=args.batch_size,
        delay_s=args.delay,
        provider=provider,
        model=args.model,
    )


if __name__ == "__main__":
    main()
