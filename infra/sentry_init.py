"""Sentry init for discovery worker + cron (no FastAPI)."""

from __future__ import annotations

import logging
import os
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

import settings as cfg

logger = logging.getLogger(__name__)

_INITIALIZED = False


def _is_test_run() -> bool:
    """True while pytest is running (read env directly — not mockable in tests)."""
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _sentry_before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if _is_test_run():
        return None
    return event


def init_sentry(*, server_name: str) -> None:
    global _INITIALIZED
    if _INITIALIZED or not cfg.SENTRY_DSN:
        if not cfg.SENTRY_DSN:
            logger.info("[sentry] disabled — SENTRY_DSN not set")
        return

    sentry_sdk.init(
        dsn=cfg.SENTRY_DSN,
        environment=cfg.ENVIRONMENT,
        server_name=server_name,
        traces_sample_rate=0,
        send_default_pii=False,
        before_send=_sentry_before_send,
        integrations=[
            SqlalchemyIntegration(),
            RedisIntegration(),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
    )
    sentry_sdk.set_tag("service", server_name)
    _INITIALIZED = True
    logger.info("[sentry] initialized — env=%s server=%s", cfg.ENVIRONMENT, server_name)


def flush_sentry(*, timeout: float = 5.0) -> None:
    """Drain the Sentry event queue before process exit (critical for short-lived cron)."""
    if sentry_sdk.is_initialized():
        sentry_sdk.flush(timeout=timeout)
