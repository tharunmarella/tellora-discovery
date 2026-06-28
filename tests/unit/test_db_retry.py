"""Unit tests for transient DB error detection and retry helper."""

import pytest
from sqlalchemy.exc import OperationalError

from database import is_transient_db_error, run_with_db_retry


def test_is_transient_db_error_starting_up():
    exc = OperationalError(
        "connection failed: FATAL: the database system is starting up",
        None,
        None,
    )
    assert is_transient_db_error(exc)


def test_is_transient_db_error_not_transient():
    exc = OperationalError("syntax error at or near", None, None)
    assert not is_transient_db_error(exc)


def test_run_with_db_retry_succeeds_after_transient_failure(monkeypatch):
    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        if calls["n"] < 2:
            raise OperationalError("FATAL: the database system is starting up", None, None)
        return "ok"

    monkeypatch.setattr("database.time.sleep", lambda _s: None)
    assert run_with_db_retry(_fn, max_attempts=3) == "ok"
    assert calls["n"] == 2


def test_run_with_db_retry_raises_non_transient_immediately():
    with pytest.raises(OperationalError, match="syntax error"):
        run_with_db_retry(
            lambda: (_ for _ in ()).throw(OperationalError("syntax error", None, None)),
            max_attempts=3,
        )
