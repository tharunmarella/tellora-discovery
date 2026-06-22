"""Unit tests for headcount year-leak sanitizer."""

from signals.pipeline import sanitize_headcount


def test_sanitize_headcount_keeps_real_value():
    assert sanitize_headcount(2000, now_year=2026) == 2000
    assert sanitize_headcount(150, now_year=2026) == 150


def test_sanitize_headcount_drops_recent_year_leak():
    assert sanitize_headcount(2025, now_year=2026) is None
    assert sanitize_headcount(2026, now_year=2026) is None
    assert sanitize_headcount(2027, now_year=2026) is None


def test_sanitize_headcount_drops_founded_year_match():
    assert sanitize_headcount(2015, founded_year=2015, now_year=2026) is None


def test_sanitize_headcount_year_floor_catches_older_year_leak():
    # LLM-synthesized values pass year_floor=1900 so an older year (e.g. 2019)
    # is rejected as a leak.
    assert sanitize_headcount(2019, now_year=2026, year_floor=1900) is None
    assert sanitize_headcount(2008, now_year=2026, year_floor=1900) is None


def test_sanitize_headcount_default_floor_keeps_older_year():
    # Structured sources (default floor) trust an older 4-digit count.
    assert sanitize_headcount(2019, now_year=2026) == 2019


def test_sanitize_headcount_zero_and_none():
    assert sanitize_headcount(0, now_year=2026) is None
    assert sanitize_headcount(None, now_year=2026) is None


def test_sanitize_headcount_keeps_value_near_but_not_year():
    assert sanitize_headcount(1800, now_year=2026) == 1800
