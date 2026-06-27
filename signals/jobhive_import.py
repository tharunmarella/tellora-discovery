"""
Import ATS slugs from the jobhive / ats-scrapers open dataset.

CSV format (per ATS file): name,slug,url
https://github.com/kalil0321/ats-scrapers/tree/main/ats-companies
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from signals.job_posts import _ATS_ORDER

logger = logging.getLogger("discovery.jobhive")

JOBHIVE_CSV_BASE = (
    "https://raw.githubusercontent.com/kalil0321/ats-scrapers/main/ats-companies"
)
SUPPORTED_ATS = _ATS_ORDER

_index_cache: Optional["JobhiveIndex"] = None
_index_failed = False
_index_lock = threading.Lock()

_NAME_SUFFIXES = [
    " inc", " inc.", " llc", " ltd", " corp", " corporation",
    " co", " co.", " technologies", " technology", " tech",
    " solutions", " group", " labs", " ai", " io", ".io", ".ai",
]


def normalize_company_key(name: str) -> str:
    """Alphanumeric key for fuzzy name matching against jobhive rows."""
    base = (name or "").lower().strip()
    for suffix in _NAME_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)].strip()
    return re.sub(r"[^a-z0-9]+", "", base)


def domain_stem(domain: Optional[str]) -> Optional[str]:
    if not domain:
        return None
    stem = domain.lower().replace("www.", "").split(".")[0]
    return stem if stem and len(stem) >= 2 else None


@dataclass
class JobhiveIndex:
    """In-memory slug directory from jobhive ats-companies CSVs."""

    by_slug: dict[str, dict[str, tuple[str, str]]] = field(default_factory=dict)
    by_name: dict[str, list[tuple[str, str]]] = field(default_factory=dict)

    def add(self, ats: str, name: str, slug: str) -> None:
        slug = slug.strip()
        if not slug:
            return
        self.by_slug.setdefault(ats, {})[slug.lower()] = (name, slug)
        key = normalize_company_key(name)
        if key:
            self.by_name.setdefault(key, []).append((ats, slug))

    @property
    def total_rows(self) -> int:
        return sum(len(slugs) for slugs in self.by_slug.values())


def parse_jobhive_csv(ats: str, text: str) -> JobhiveIndex:
    index = JobhiveIndex()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        slug = (row.get("slug") or "").strip()
        name = (row.get("name") or "").strip()
        if slug and name:
            index.add(ats, name, slug)
    return index


def merge_index(into: JobhiveIndex, part: JobhiveIndex) -> None:
    for ats, slugs in part.by_slug.items():
        into.by_slug.setdefault(ats, {}).update(slugs)
    for key, hits in part.by_name.items():
        into.by_name.setdefault(key, []).extend(hits)


def load_index_from_dir(directory: Path) -> JobhiveIndex:
    index = JobhiveIndex()
    for ats in SUPPORTED_ATS:
        path = directory / f"{ats}.csv"
        if not path.is_file():
            continue
        merge_index(index, parse_jobhive_csv(ats, path.read_text(encoding="utf-8")))
    return index


def load_index_from_github(
    *,
    base_url: str = JOBHIVE_CSV_BASE,
    timeout: float = 60.0,
) -> JobhiveIndex:
    index = JobhiveIndex()
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for ats in SUPPORTED_ATS:
            resp = client.get(f"{base_url}/{ats}.csv")
            resp.raise_for_status()
            merge_index(index, parse_jobhive_csv(ats, resp.text))
    return index


def find_jobhive_match(
    company_name: str,
    domain: Optional[str],
    index: JobhiveIndex,
) -> Optional[tuple[str, str]]:
    """
    Match a discovery_company row to (ats, slug).

    Priority: domain stem slug lookup → normalized company name.
    """
    stem = domain_stem(domain)
    if stem:
        for ats in SUPPORTED_ATS:
            entry = index.by_slug.get(ats, {}).get(stem.lower())
            if entry:
                return ats, entry[1]

    key = normalize_company_key(company_name)
    hits = index.by_name.get(key, [])
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]
    for ats in SUPPORTED_ATS:
        for a, s in hits:
            if a == ats:
                return a, s
    return hits[0]


def build_ats_board(ats: str, slug: str) -> dict:
    return {
        "source": ats,
        "slug": slug,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "imported_from": "jobhive",
    }


def clear_jobhive_index_cache() -> None:
    """Reset cached index (tests)."""
    global _index_cache, _index_failed
    with _index_lock:
        _index_cache = None
        _index_failed = False


def get_jobhive_index(*, force_reload: bool = False) -> Optional[JobhiveIndex]:
    """
    Process-local cached jobhive index for inline enrich lookups.
    Returns None when disabled or load fails.
    """
    import settings as cfg

    if not cfg.JOBHIVE_ENRICH_LOOKUP:
        return None

    global _index_cache, _index_failed
    if not force_reload and _index_cache is not None:
        return _index_cache
    if not force_reload and _index_failed:
        return None

    with _index_lock:
        if not force_reload and _index_cache is not None:
            return _index_cache
        if not force_reload and _index_failed:
            return None
        try:
            if cfg.JOBHIVE_LOCAL_DIR:
                _index_cache = load_index_from_dir(Path(cfg.JOBHIVE_LOCAL_DIR))
            else:
                _index_cache = load_index_from_github()
            _index_failed = False
            logger.info(
                "[jobhive] loaded index — %d slugs across %d ATS types",
                _index_cache.total_rows,
                len(_index_cache.by_slug),
            )
        except Exception as exc:
            logger.warning("[jobhive] index load failed: %s", exc)
            _index_cache = None
            _index_failed = True
            return None
        return _index_cache


def lookup_ats_board_from_jobhive(
    company_name: str,
    domain: Optional[str],
    *,
    existing: Optional[dict] = None,
) -> Optional[dict]:
    """
    Return cached ats_board or a jobhive match before live ATS discovery.
    """
    if existing and existing.get("source") and existing.get("slug"):
        return existing

    index = get_jobhive_index()
    if index is None:
        return None

    hit = find_jobhive_match(company_name, domain, index)
    if not hit:
        return None

    ats, slug = hit
    logger.info(
        "[jobhive] pre-enrich match %s (%s) → %s/%s",
        company_name,
        domain,
        ats,
        slug,
    )
    return build_ats_board(ats, slug)


_SELECT_IMPORT_ROWS = """
SELECT id, name, domain, ats_board
FROM discovery_company
WHERE domain IS NOT NULL
  AND domain_resolved = true
  {ats_filter}
ORDER BY last_seen_at DESC
{limit_clause}
"""


def apply_jobhive_import(
    session: Session,
    *,
    index: JobhiveIndex,
    limit: Optional[int] = None,
    only_missing: bool = True,
) -> dict:
    """
    Match discovery companies to jobhive slugs and write ats_board (no live validation).
    """
    ats_filter = "AND ats_board IS NULL" if only_missing else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = _SELECT_IMPORT_ROWS.format(ats_filter=ats_filter, limit_clause=limit_clause)

    rows = [dict(r) for r in session.execute(text(query)).mappings().all()]
    matched = 0
    applied = 0
    skipped_existing = 0

    for row in rows:
        if row.get("ats_board") and only_missing:
            skipped_existing += 1
            continue
        hit = find_jobhive_match(row["name"], row.get("domain"), index)
        if not hit:
            continue
        ats, slug = hit
        matched += 1
        board = build_ats_board(ats, slug)
        session.execute(
            text("UPDATE discovery_company SET ats_board = CAST(:ats_board AS jsonb) WHERE id = :id"),
            {"id": row["id"], "ats_board": json.dumps(board)},
        )
        applied += 1

    if applied:
        session.commit()

    return {
        "scanned": len(rows),
        "matched": matched,
        "applied": applied,
        "skipped_existing": skipped_existing,
    }
