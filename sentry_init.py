"""Sentry init for discovery worker + cron (no FastAPI)."""

from __future__ import annotations

import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

import settings as cfg

logger = logging.getLogger(__name__)

_INITIALIZED = False


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
        integrations=[
            SqlalchemyIntegration(),
            RedisIntegration(),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
    )
    _INITIALIZED = True
    logger.info("[sentry] initialized — env=%s server=%s", cfg.ENVIRONMENT, server_name)
