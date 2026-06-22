"""Backfill scrape-time DiscoveryCompany fields via Serper + Gemini lookup."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from database import make_engine
from scrape.domain_lookup import lookup_domain

logger = logging.getLogger("discovery.scrape_fields_backfill")

SCALAR_FIELDS = (
    "linkedin_url",
    "website_url",
    "ceo_name",
    "headquarters",
    "founded_year",
    "funding",
    "description",
    "industry",
    "logo_url",
    "domain",
)

_SELECT_ELIGIBLE = text("""
    SELECT id, name, domain, website_url, linkedin_url, ceo_name, headquarters,
           founded_year, funding, description, industry, logo_url, raw_meta,
           domain_resolved
    FROM   discovery_company
    WHERE  source = :source
    AND    (
        linkedin_url IS NULL OR btrim(linkedin_url) = ''
        OR website_url IS NULL OR btrim(website_url) = ''
        OR ceo_name IS NULL OR btrim(ceo_name) = ''
        OR founded_year IS NULL OR btrim(founded_year) = ''
        OR logo_url IS NULL OR btrim(logo_url) = ''
        OR description IS NULL OR btrim(description) = ''
        OR industry IS NULL OR btrim(industry) = ''
        OR headquarters IS NULL OR btrim(headquarters) = ''
        OR funding IS NULL OR btrim(funding) = ''
        OR raw_meta IS NULL
        OR raw_meta->'keywords' IS NULL
        OR raw_meta->'use_case' IS NULL
    )
    ORDER  BY updated_at DESC
    LIMIT  :lim
""")


def _filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return bool(value)


def _parse_raw_meta(raw_meta: Any) -> dict:
    if raw_meta is None:
        return {}
    if isinstance(raw_meta, dict):
        return dict(raw_meta)
    if isinstance(raw_meta, str):
        try:
            parsed = json.loads(raw_meta)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _ceo_first_name(ceo_name: str | None) -> str:
    if not ceo_name or not str(ceo_name).strip():
        return ""
    return str(ceo_name).strip().split()[0]


def merge_scrape_fields(existing: dict, enrichment: dict) -> dict[str, Any]:
    """
    Non-destructive merge: only fill fields that are currently empty.
    Never overwrite an existing domain.
    """
    updates: dict[str, Any] = {}

    for field in SCALAR_FIELDS:
        if field == "domain":
            continue
        if not _filled(existing.get(field)) and enrichment.get(field):
            updates[field] = enrichment[field]

    if not _filled(existing.get("domain")) and enrichment.get("domain"):
        updates["domain"] = enrichment["domain"]
        updates["domain_resolved"] = True

    existing_meta = _parse_raw_meta(existing.get("raw_meta"))
    meta_updates: dict[str, Any] = {}
    for key in ("keywords", "use_case"):
        if not _filled(existing_meta.get(key)) and enrichment.get(key):
            meta_updates[key] = enrichment[key]
    if meta_updates:
        updates["raw_meta"] = {**existing_meta, **meta_updates}

    domain = updates.get("domain") or existing.get("domain")
    if not _filled(existing.get("logo_url")) and domain and "logo_url" not in updates:
        updates["logo_url"] = f"https://www.google.com/s2/favicons?domain={domain}&sz=64"

    return updates


def count_eligible_rows(*, source: str = "apollo") -> int:
    engine = make_engine()
    with Session(engine) as session:
        row = session.execute(
            text(
                """
                SELECT COUNT(*) AS n
                FROM   discovery_company
                WHERE  source = :source
                AND    (
                    linkedin_url IS NULL OR btrim(linkedin_url) = ''
                    OR website_url IS NULL OR btrim(website_url) = ''
                    OR ceo_name IS NULL OR btrim(ceo_name) = ''
                    OR founded_year IS NULL OR btrim(founded_year) = ''
                    OR logo_url IS NULL OR btrim(logo_url) = ''
                    OR description IS NULL OR btrim(description) = ''
                    OR industry IS NULL OR btrim(industry) = ''
                    OR headquarters IS NULL OR btrim(headquarters) = ''
                    OR funding IS NULL OR btrim(funding) = ''
                    OR raw_meta IS NULL
                    OR raw_meta->'keywords' IS NULL
                    OR raw_meta->'use_case' IS NULL
                )
                """
            ),
            {"source": source},
        ).mappings().first()
        return int(row["n"] if row else 0)


def _apply_updates(session: Session, company_id: str, updates: dict[str, Any]) -> None:
    if not updates:
        return

    sets: list[str] = []
    params: dict[str, Any] = {"id": company_id}
    for key, val in updates.items():
        if key == "raw_meta":
            sets.append("raw_meta = CAST(:raw_meta AS jsonb)")
            params["raw_meta"] = json.dumps(val)
        else:
            sets.append(f"{key} = :{key}")
            params[key] = val
    sets.append("updated_at = NOW()")
    session.execute(
        text(f"UPDATE discovery_company SET {', '.join(sets)} WHERE id = :id"),
        params,
    )


async def _lookup_one(row: dict, sem: asyncio.Semaphore) -> tuple[dict, dict]:
    async with sem:
        ceo_first = _ceo_first_name(row.get("ceo_name"))
        enrichment = await lookup_domain(row["name"], ceo_first)
        return row, enrichment


async def backfill_scrape_fields(
    limit: int | None = None,
    *,
    run_all: bool = False,
    concurrency: int = 8,
    dry_run: bool = False,
    source: str = "apollo",
) -> dict:
    """
    Re-run scrape-time Serper + Gemini lookup for rows missing linkedin_url
    and/or other scrape metadata. Only fills empty columns.
    """
    batch_size = int(getattr(cfg, "SCRAPE_FIELDS_BACKFILL_LIMIT", 500))
    cap = batch_size if run_all else (limit if limit is not None else batch_size)

    if dry_run:
        eligible = count_eligible_rows(source=source)
        logger.info("[scrape_fields_backfill] dry run — %d eligible rows (source=%s)", eligible, source)
        return {"eligible": eligible, "updated": 0, "skipped": 0, "processed": 0, "dry_run": True}

    engine = make_engine()
    sem = asyncio.Semaphore(max(1, concurrency))

    total_updated = total_skipped = total_processed = 0
    batch_num = 0
    field_counts: dict[str, int] = {}

    while True:
        batch_num += 1
        updated = skipped = 0
        with Session(engine) as session:
            rows = session.execute(
                _SELECT_ELIGIBLE, {"source": source, "lim": cap}
            ).mappings().all()
            if not rows:
                if batch_num == 1:
                    logger.info("[scrape_fields_backfill] no eligible rows")
                break

            mode = "run_all" if run_all else f"cap={cap}"
            logger.info(
                "[scrape_fields_backfill] batch %d: %d rows (%s)",
                batch_num,
                len(rows),
                mode,
            )

            lookup_results = await asyncio.gather(
                *[_lookup_one(dict(row), sem) for row in rows]
            )

            for row, enrichment in lookup_results:
                if not enrichment:
                    skipped += 1
                    continue
                updates = merge_scrape_fields(row, enrichment)
                if not updates:
                    skipped += 1
                    continue
                _apply_updates(session, str(row["id"]), updates)
                updated += 1
                for key in updates:
                    field_counts[key] = field_counts.get(key, 0) + 1
                logger.info(
                    "[scrape_fields_backfill] %s — filled %s",
                    row["name"],
                    ", ".join(sorted(updates)),
                )

            session.commit()

        total_updated += updated
        total_skipped += skipped
        total_processed += len(rows)

        if not run_all or len(rows) < cap:
            break

    logger.info(
        "[scrape_fields_backfill] done — updated=%d skipped=%d processed=%d fields=%s",
        total_updated,
        total_skipped,
        total_processed,
        field_counts,
    )
    return {
        "updated": total_updated,
        "skipped": total_skipped,
        "processed": total_processed,
        "batches": batch_num,
        "fields": field_counts,
    }
