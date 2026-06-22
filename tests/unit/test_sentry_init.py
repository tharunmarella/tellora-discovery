"""Unit tests for infra/sentry_init.py."""

import infra.sentry_init as sentry_init
import sentry_sdk


def test_init_sentry_noop_without_dsn(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SENTRY_DSN", "")
    sentry_init._INITIALIZED = False
    calls = []

    def fake_init(**_kwargs):
        calls.append(True)

    monkeypatch.setattr(sentry_sdk, "init", fake_init)
    sentry_init.init_sentry(server_name="test")
    assert not calls
    assert not sentry_sdk.is_initialized()


def test_sentry_before_send_drops_events_during_pytest(monkeypatch):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/unit/test_sentry_init.py::test")
    assert sentry_init._sentry_before_send({"event_id": "abc"}, {}) is None


def test_sentry_before_send_passes_events_outside_pytest(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    event = {"event_id": "abc"}
    assert sentry_init._sentry_before_send(event, {}) is event


def test_flush_sentry_noop_when_not_initialized(monkeypatch):
    monkeypatch.setattr(sentry_sdk, "is_initialized", lambda: False)
    calls = []
    monkeypatch.setattr(sentry_sdk, "flush", lambda **kw: calls.append(kw))
    sentry_init.flush_sentry()
    assert calls == []
