"""Unit tests for cron/scrape_schedule.py."""

from datetime import datetime, timezone

import pytest

from cron.scrape_schedule import (
    DEFAULT_DISCOVERY_SCRAPE_CRON,
    parse_scrape_cron,
    should_run_discovery_scrape,
)


def test_default_cron_is_sun_wed_3am():
    assert DEFAULT_DISCOVERY_SCRAPE_CRON == "0 3 * * 0,3"
    schedule = parse_scrape_cron(DEFAULT_DISCOVERY_SCRAPE_CRON)
    assert schedule.minute == 0
    assert schedule.hour == 3
    assert schedule.cron_weekdays == frozenset({0, 3})
    assert schedule.python_weekdays == frozenset({6, 2})


@pytest.mark.parametrize(
    ("when", "expected"),
    [
        (datetime(2026, 6, 21, 3, 0, tzinfo=timezone.utc), True),   # Sunday
        (datetime(2026, 6, 24, 3, 30, tzinfo=timezone.utc), True),  # Wednesday
        (datetime(2026, 6, 22, 3, 0, tzinfo=timezone.utc), False),  # Monday
        (datetime(2026, 6, 21, 4, 0, tzinfo=timezone.utc), False),  # Sunday wrong hour
    ],
)
def test_should_run_discovery_scrape(when, expected):
    assert should_run_discovery_scrape(when) is expected


def test_should_run_force_and_disabled():
    when = datetime(2026, 6, 22, 3, 0, tzinfo=timezone.utc)  # Monday
    assert should_run_discovery_scrape(when, force=True) is True
    assert should_run_discovery_scrape(when, schedule_disabled=True) is True
