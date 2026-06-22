"""
Sample re-enrichment quality probe.

Picks N random already-enriched companies, re-runs the CURRENT enrichment
pipeline on them, persists the result, and prints before/after fill-rate deltas
plus a few grounded-judge scores. Use to validate pipeline changes on real data
before committing to a full ~8.7K backfill.

DATABASE_URL must point at the target DB. Skips the prod Redis notify.

    PYTHONPATH=. DATABASE_URL=... python scripts/sample_reenrich_quality.py --n 50
"""

from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from database import make_engine
from signals.runner import _process_batch, _write_batch

SNAPSHOT_COLS = [
    "company_summary",
    "signal_score",
    "funding_stage",
    "total_raised",
    "headcount",
    "hq_city",
    "hq_region",
    "hq_country",
    "buying_signals",
    "tech_stack",
]


def _select_sample(session: Session, n: int) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT id, name, domain, description, industry, raw_meta, headcount, headquarters
            FROM   discovery_company
            WHERE  signal_enrichment_status = 'enriched'
            AND    domain IS NOT NULL
            AND    domain_resolved = true
            ORDER  BY random()
            LIMIT  :n
            """
        ),
        {"n": n},
    ).mappings().all()
    return [dict(r) for r in rows]


def _snapshot(session: Session, ids: list[str]) -> dict[str, dict]:
    rows = session.execute(
        text(
            f"SELECT id, {', '.join(SNAPSHOT_COLS)} FROM discovery_company "
            "WHERE id = ANY(:ids)"
        ),
        {"ids": ids},
    ).mappings().all()
    return {str(r["id"]): dict(r) for r in rows}


def _filled(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    if isinstance(value, (int, float)):
        return value > 0
    return bool(value)


def _fill_rates(snap: dict[str, dict]) -> dict[str, int]:
    counts = {c: 0 for c in SNAPSHOT_COLS}
    for row in snap.values():
        for c in SNAPSHOT_COLS:
            if _filled(row.get(c)):
                counts[c] += 1
    return counts


def _year_leaks(snap: dict[str, dict]) -> int:
    leaks = 0
    for row in snap.values():
        hc = row.get("headcount")
        if isinstance(hc, int) and 2023 <= hc <= 2028:
            leaks += 1
    return leaks


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--judge", type=int, default=3, help="How many to grade with the LLM judge")
    args = parser.parse_args()

    engine = make_engine()
    with Session(engine) as session:
        rows = _select_sample(session, args.n)
        ids = [str(r["id"]) for r in rows]
        before = _snapshot(session, ids)

    print(f"Selected {len(rows)} enriched companies. Re-enriching (concurrency={args.concurrency})...")
    results = await _process_batch(rows, concurrency=args.concurrency)

    with Session(engine) as session:
        ok, fail = _write_batch(session, results)
    print(f"Persisted: ok={ok} fail={fail}")

    with Session(engine) as session:
        after = _snapshot(session, ids)

    before_rates = _fill_rates(before)
    after_rates = _fill_rates(after)
    n = len(ids)

    print("\n=== FILL-RATE: before -> after (of %d) ===" % n)
    for c in SNAPSHOT_COLS:
        b, a = before_rates[c], after_rates[c]
        arrow = "" if a == b else ("  ↑" if a > b else "  ↓")
        print(f"  {c:<16} {b:>3} ({100*b//n:>3}%) -> {a:>3} ({100*a//n:>3}%){arrow}")

    print(f"\n=== headcount year-leaks: before={_year_leaks(before)} -> after={_year_leaks(after)} ===")

    # Status outcome of the re-run.
    statuses: dict[str, int] = {}
    for r in results:
        s = r.get("signal_enrichment_status", "unknown")
        statuses[s] = statuses.get(s, 0) + 1
    print(f"=== re-run statuses: {statuses} ===")

    if args.judge > 0:
        print(f"\n=== grounded judge on {min(args.judge, len(results))} samples ===")
        from tests.e2e.test_pipeline_quality import judge_pipeline_output

        graded = [r for r in results if r.get("signal_enrichment_status") in ("enriched", "partial")]
        for r in graded[: args.judge]:
            row = next((x for x in rows if str(x["id"]) == str(r["id"])), None)
            name = row["name"] if row else r["id"]
            domain = r.get("domain") or (row["domain"] if row else "")
            try:
                verdict = judge_pipeline_output(name, domain, r)
                print(f"  {name} ({domain}): score={verdict.get('score')} — {verdict.get('reasons')}")
                if verdict.get("field_issues"):
                    print(f"     issues: {verdict.get('field_issues')}")
            except Exception as exc:  # noqa: BLE001
                print(f"  {name}: judge failed: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
