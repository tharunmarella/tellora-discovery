"""
USAspending.gov contract awards — free federal spending API.

Companies that recently won government contracts have budget and are scaling.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from signals.entity import name_matches_company

logger = logging.getLogger("discovery.gov")

_API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

_AWARD_CODES = ["A", "B", "C", "D"]


def _fmt_amount(n: float | int | None) -> str:
    if not n:
        return ""
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"${n / 1_000:.0f}K"
    return f"${n:.0f}"


async def fetch_gov_awards(
    company_name: str,
    days: int = 90,
    *,
    uei: Optional[str] = None,
) -> list[dict]:
    """
    Federal awards to a company in the last N days.

    With a known UEI (SAM.gov's unique entity ID, cached in raw_meta) the search
    is exact. Without one, a name search runs and only recipients whose name
    matches the company (signals.entity) are kept; their UEI is returned on each
    award so the caller can cache it.
    Returns [{award_id, recipient, uei, amount, agency, date}].
    """
    if not company_name and not uei:
        return []

    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(_API, json={
                "filters": {
                    "award_type_codes": _AWARD_CODES,
                    "time_period": [{"start_date": start, "end_date": end}],
                    "recipient_search_text": [uei or company_name],
                },
                "fields": [
                    "Award ID",
                    "Recipient Name",
                    "Recipient UEI",
                    "Award Amount",
                    "Awarding Agency",
                    "Start Date",
                    "Base Obligation Date",
                ],
                "limit": 10,
                "page": 1,
            })
            resp.raise_for_status()
            results = resp.json().get("results", [])
    except Exception as exc:
        logger.warning(f"USAspending lookup failed for '{company_name}': {exc}")
        return []

    awards = []
    for row in results:
        recipient = row.get("Recipient Name") or ""
        row_uei = row.get("Recipient UEI")
        if uei:
            if row_uei and row_uei != uei:
                continue
        elif not name_matches_company(recipient, company_name):
            continue
        amount = row.get("Award Amount")
        try:
            amount_f = float(amount) if amount is not None else 0
        except (TypeError, ValueError):
            amount_f = 0
        if amount_f <= 0:
            continue
        awards.append({
            "award_id": row.get("Award ID") or row.get("generated_internal_id", ""),
            "internal_id": row.get("generated_internal_id"),
            "recipient": recipient,
            "uei": row_uei,
            "amount": amount_f,
            "agency": row.get("Awarding Agency", ""),
            "date": row.get("Base Obligation Date") or row.get("Start Date", ""),
        })
    return awards[:3]


def _award_url(a: dict) -> Optional[str]:
    award = a.get("internal_id") or a.get("award_id")
    return f"https://www.usaspending.gov/award/{award}" if award else None


def gov_extra_events(awards: list[dict]) -> list[dict]:
    """Convert federal awards into gov_contract extra_events."""
    events = []
    for a in awards:
        amt = _fmt_amount(a.get("amount"))
        agency = a.get("agency") or "federal agency"
        events.append({
            "event_type": "gov_contract",
            "title": f"Federal contract: {amt} from {agency}"[:500],
            "payload": {
                "key": f"usaspending:{a['award_id']}",
                "award_id": a["award_id"],
                "amount": a.get("amount"),
                "agency": agency,
                "date": a.get("date"),
                "recipient": a.get("recipient"),
                "url": _award_url(a),
                "uei": a.get("uei"),
            },
            "source": "usaspending",
            "confidence": 0.9,
            "evidence_url": _award_url(a),
            "event_date": a.get("date"),
        })
    return events
