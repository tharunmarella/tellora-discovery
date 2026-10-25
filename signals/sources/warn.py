"""
WARN Act layoff notices — state labor-department filings via warn-scraper
(Big Local News, Stanford). Free, legally filed, and they name the employer,
head count, site and date — unlike layoff headlines.

Weekly cron: scrape each configured state, normalize the state-specific
columns, keep notices from the last WARN_LOOKBACK_DAYS, match employers to
tracked companies (signals.entity) and return layoffs events.
"""

from __future__ import annotations

import asyncio
import csv
import importlib
import logging
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import settings as cfg
from signals.diff import parse_event_date
from signals.entity import CompanyIndex

logger = logging.getLogger("discovery.warn")

_COMPANY_KEYS = ("company", "business legal name", "business name", "employer", "job_site_name", "organization")
_NOTICE_DATE_KEYS = ("notice_date", "date of warn notice", "notice date", "received date", "received_date",
                     "wfdd_received_date", "date received", "date posted", "effective date",
                     "effective_date", "layoff start date")
_EMPLOYEE_KEYS = ("num_employees", "number of affected workers", "# of workers", "total_layoff_number",
                  "employees affected", "number of employees", "workforce affected", "affected", "workers")
_CITY_KEYS = ("city", "city_name", "location", "impacted site address", "address")


def _col(row: dict, keys: tuple[str, ...]) -> str:
    lowered = {(k or "").strip().lower(): v for k, v in row.items()}
    for key in keys:
        if lowered.get(key):
            return str(lowered[key]).strip()
    for key in keys:
        for k, v in lowered.items():
            if key in k and v:
                return str(v).strip()
    return ""


def _parse_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value.strip()[:10], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return parse_event_date(value.strip()[:19].replace(" ", "T") if re.match(r"\d{4}-\d{2}-\d{2} ", value) else value)


def _int(value: str) -> Optional[int]:
    m = re.search(r"\d[\d,]*", value or "")
    return int(m.group(0).replace(",", "")) if m else None


def normalize_rows(state: str, rows: list[dict]) -> list[dict]:
    """State CSV rows → [{company, notice_date, employees, location, state}]."""
    out = []
    for row in rows:
        company = _col(row, _COMPANY_KEYS)
        notice = _parse_date(_col(row, _NOTICE_DATE_KEYS))
        if not company or not notice:
            continue
        out.append({
            "company": company,
            "notice_date": notice,
            "employees": _int(_col(row, _EMPLOYEE_KEYS)),
            "location": _col(row, _CITY_KEYS)[:120],
            "state": state,
        })
    return out


def _scrape_state_sync(state: str, data_dir: Path, cache_dir: Path) -> list[dict]:
    from warn.runner import Runner

    path = Runner(data_dir=data_dir, cache_dir=cache_dir).scrape(state)
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh))


def _source_url(state: str) -> Optional[str]:
    try:
        return importlib.import_module(f"warn.scrapers.{state}").__source__.get("url")
    except Exception:
        return None


async def fetch_recent_notices(states: Optional[list[str]] = None, days: Optional[int] = None) -> list[dict]:
    """Scrape states (sequentially — several share upstream portals) and keep recent notices."""
    states = states or cfg.WARN_STATES
    days = days or cfg.WARN_LOOKBACK_DAYS
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    notices: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="warn-") as tmp:
        data_dir, cache_dir = Path(tmp, "exports"), Path(tmp, "cache")
        for state in states:
            try:
                rows = await asyncio.to_thread(_scrape_state_sync, state, data_dir, cache_dir)
            except Exception as exc:
                logger.warning(f"WARN scrape failed for {state}: {exc!r}"[:300])
                continue
            recent = [n for n in normalize_rows(state, rows) if n["notice_date"] >= cutoff]
            logger.info(f"WARN {state}: {len(rows)} notices, {len(recent)} in the last {days}d")
            notices.extend(recent)
    return notices


def match_notices(notices: list[dict], index: CompanyIndex) -> dict[str, list[dict]]:
    """Matched notices → {company_id: [layoffs extra_event dicts]}."""
    out: dict[str, list[dict]] = {}
    for n in notices:
        hit = index.match(n["company"], state=n["state"])
        if not hit:
            continue
        employees = n.get("employees")
        where = ", ".join(p for p in (n.get("location"), n["state"].upper()) if p)
        title = f"WARN notice: {hit.company.name} is laying off"
        title += f" {employees} employees" if employees else " employees"
        title += f" in {where}" if where else ""
        date = n["notice_date"].date().isoformat()
        out.setdefault(hit.company.id, []).append({
            "event_type": "layoffs",
            "title": title[:500],
            "payload": {
                "key": f"warn:{n['state']}:{date}:{employees or 0}:{n['company'].lower()[:40]}",
                "employer": n["company"], "employees": employees, "location": n.get("location"),
                "state": n["state"], "notice_date": date, "match": hit.reason,
                "url": _source_url(n["state"]),
            },
            "source": "warn_notice",
            "confidence": 0.95 if hit.score >= 99 else 0.85,
            "evidence_url": _source_url(n["state"]),
            "event_date": date,
        })
    return out
