"""
Schedule guard for the discovery scrape job.

Railway should use ``0 3 * * 0,3`` (Sun + Wed 3 AM UTC). That expression is
hard-coded here as the fallback when ``DISCOVERY_SCRAPE_CRON`` is unset.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Standard 5-field cron — Sunday + Wednesday 03:00 UTC.
DEFAULT_DISCOVERY_SCRAPE_CRON = "0 3 * * 0,3"

# Standard cron DOW (0=Sunday) → datetime.weekday() (0=Monday).
_CRON_DOW_TO_PYTHON = {0: 6, 1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5}


class ScrapeSchedule:
    __slots__ = ("minute", "hour", "cron_weekdays")

    def __init__(self, *, minute: int, hour: int, cron_weekdays: frozenset[int]) -> None:
        self.minute = minute
        self.hour = hour
        self.cron_weekdays = cron_weekdays

    @property
    def python_weekdays(self) -> frozenset[int]:
        return frozenset(_CRON_DOW_TO_PYTHON[d] for d in self.cron_weekdays)


def parse_scrape_cron(expr: str) -> ScrapeSchedule:
    """Parse minute, hour, and day-of-week from a 5-field cron expression."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"expected 5 cron fields, got {len(parts)}: {expr!r}")

    minute_s, hour_s, _dom, _month, dow_s = parts
    if minute_s == "*" or hour_s == "*" or dow_s == "*":
        raise ValueError(f"wildcard fields not supported for scrape guard: {expr!r}")
    if any(ch in minute_s + hour_s + dow_s for ch in "-/"):
        raise ValueError(f"ranges/steps not supported for scrape guard: {expr!r}")

    cron_weekdays = frozenset(int(part.strip()) for part in dow_s.split(","))
    return ScrapeSchedule(
        minute=int(minute_s),
        hour=int(hour_s),
        cron_weekdays=cron_weekdays,
    )


def _as_utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def should_run_discovery_scrape(
    now: datetime | None = None,
    *,
    cron_expr: str = DEFAULT_DISCOVERY_SCRAPE_CRON,
    force: bool = False,
    schedule_disabled: bool = False,
) -> bool:
    """Return True when ``now`` matches the scrape cron (UTC)."""
    if force or schedule_disabled:
        return True

    when = _as_utc(now or datetime.now(timezone.utc))
    schedule = parse_scrape_cron(cron_expr)

    if when.weekday() not in schedule.python_weekdays:
        return False
    if when.hour != schedule.hour:
        return False
    return True


def discovery_scrape_recently_active(
    within_hours: int = 20,
    *,
    active_hours: int | None = None,
) -> bool:
    """
    True when a scrape run is in progress or finished within ``within_hours``.

    Used by the worker fallback cron so it does not duplicate a Railway run.
    """
    import settings as cfg
    from sqlmodel import select

    from database import get_session
    from models import DiscoveryProgress

    active_h = active_hours if active_hours is not None else cfg.SCRAPE_ACTIVE_HOURS
    cutoff_active = datetime.now(timezone.utc) - timedelta(hours=active_h)
    cutoff_completed = datetime.now(timezone.utc) - timedelta(hours=within_hours)
    with get_session() as db:
        running = db.exec(
            select(DiscoveryProgress)
            .where(DiscoveryProgress.status == "running")
            .where(DiscoveryProgress.started_at >= cutoff_active)
            .limit(1)
        ).first()
        if running:
            return True

        completed = db.exec(
            select(DiscoveryProgress)
            .where(DiscoveryProgress.status == "completed")
            .where(DiscoveryProgress.completed_at >= cutoff_completed)
            .limit(1)
        ).first()
        return completed is not None
