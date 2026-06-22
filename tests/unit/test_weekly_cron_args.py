"""Unit tests for cron/weekly.py CLI arg parsing and validation."""

import pytest

from cron.weekly import (
    WeeklyCronArgs,
    apply_dry_run_settings,
    parse_args,
    validate_args,
)


def test_parse_args_defaults():
    assert parse_args([]) == WeeklyCronArgs(dry_run=False, headcount_only=False)


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
