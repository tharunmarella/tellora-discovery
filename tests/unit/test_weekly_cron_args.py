"""Unit tests for cron/weekly.py CLI arg parsing and validation."""

import pytest

from cron.weekly import (
    WeeklyCronArgs,
    apply_dry_run_settings,
    ensure_scheduled_or_skip,
    parse_args,
    validate_args,
)


def test_parse_args_defaults():
    assert parse_args([]) == WeeklyCronArgs(dry_run=False, headcount_only=False, force=False)


def test_parse_args_force_flag():
    assert parse_args(["--force"]).force is True


def test_parse_args_force_env(monkeypatch):
    monkeypatch.setenv("DISCOVERY_SCRAPE_FORCE", "1")
    assert parse_args([]).force is True


def test_parse_args_dry_run():
    assert parse_args(["--dry-run"]).dry_run is True
    assert parse_args(["--dry-run"]).headcount_only is False


def test_parse_args_headcount_only_flag():
    assert parse_args(["--headcount-backfill-only"]).headcount_only is True


def test_parse_args_headcount_only_env(monkeypatch):
    monkeypatch.setenv("HEADCOUNT_BACKFILL_ONLY", "1")
    assert parse_args([]).headcount_only is True


def test_apply_dry_run_settings_sets_max_pages(monkeypatch):
    import settings as cfg

    original = cfg.MAX_PAGES_PER_PROFILE
    try:
        apply_dry_run_settings(WeeklyCronArgs(dry_run=True))
        assert cfg.MAX_PAGES_PER_PROFILE == 2
    finally:
        cfg.MAX_PAGES_PER_PROFILE = original


def test_apply_dry_run_settings_noop_when_false(monkeypatch):
    import settings as cfg

    original = cfg.MAX_PAGES_PER_PROFILE
    try:
        apply_dry_run_settings(WeeklyCronArgs(dry_run=False))
        assert cfg.MAX_PAGES_PER_PROFILE == original
    finally:
        cfg.MAX_PAGES_PER_PROFILE = original


def test_validate_args_exits_without_gemini_key(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "GEMINI_API_KEY", "")
    with pytest.raises(SystemExit) as exc:
        validate_args(WeeklyCronArgs(headcount_only=False))
    assert exc.value.code == 1


def test_validate_args_headcount_only_skips_gemini_check(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "GEMINI_API_KEY", "")
    validate_args(WeeklyCronArgs(headcount_only=True))


def test_ensure_scheduled_or_skip_headcount_only_always_runs():
    assert ensure_scheduled_or_skip(WeeklyCronArgs(headcount_only=True)) is True


def test_ensure_scheduled_or_skip_force(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "DISCOVERY_SCRAPE_CRON", "0 3 * * 0,3")
    monkeypatch.setattr(cfg, "DISCOVERY_SCRAPE_SCHEDULE_DISABLED", False)
    assert ensure_scheduled_or_skip(WeeklyCronArgs(force=True)) is True
