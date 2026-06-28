"""Process lifecycle — bootstrap, startup, shutdown (FastAPI lifespan equivalent)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from config_logging import setup_logging
from infra.axiom_logger import axiom_logger
from infra.sentry_init import flush_sentry, init_sentry

logger = logging.getLogger(__name__)

SERVICE_CRON = "tellora-discovery"
SERVICE_WORKER = "tellora-discovery-worker"
SERVICE_ADMIN = "tellora-discovery-admin"


def bootstrap(*, server_name: str) -> None:
    """Sync init at import — logging + Sentry before any work runs."""
    setup_logging()
    init_sentry(server_name=server_name)


async def startup(
    *,
    server_name: str,
    create_tables: bool = True,
    axiom_background_flush: bool = False,
) -> None:
    logger.info("[%s] starting up", server_name)
    if create_tables:
        from database import create_tables as _create_tables

        _create_tables()
    if axiom_background_flush:
        axiom_logger.start_background_flush()


async def shutdown(*, server_name: str) -> None:
    logger.info("[%s] shutting down", server_name)
    from llm import drain_litellm

    await drain_litellm()
    await axiom_logger.stop()
    flush_sentry()


@asynccontextmanager
async def lifespan(
    *,
    server_name: str,
    create_tables: bool = True,
    axiom_background_flush: bool = False,
) -> AsyncIterator[None]:
    await startup(
        server_name=server_name,
        create_tables=create_tables,
        axiom_background_flush=axiom_background_flush,
    )
    try:
        yield
    finally:
        await shutdown(server_name=server_name)
