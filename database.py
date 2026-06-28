"""Synchronous + async DB engines for the discovery service."""

import logging
import time
from typing import Callable, TypeVar

from sqlmodel import create_engine, SQLModel, Session

import settings as cfg

logger = logging.getLogger("discovery.db")

T = TypeVar("T")

_TRANSIENT_DB_MARKERS = (
    "starting up",
    "connection refused",
    "could not connect",
    "server closed the connection",
    "connection reset",
    "too many connections",
    "timeout expired",
    "broken pipe",
    "terminating connection",
)


def is_transient_db_error(exc: BaseException) -> bool:
    """True when Postgres/Redis infra is briefly unavailable (safe to retry)."""
    msg = str(exc).lower()
    exc_name = type(exc).__name__.lower()
    if "operationalerror" in exc_name or "interfaceerror" in exc_name:
        return any(marker in msg for marker in _TRANSIENT_DB_MARKERS)
    return any(marker in msg for marker in ("starting up", "connection refused"))


def run_with_db_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
) -> T:
    """Run a DB operation with exponential backoff on transient connection errors."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as exc:
            if not is_transient_db_error(exc) or attempt == max_attempts - 1:
                raise
            last_exc = exc
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "Transient DB error (attempt %s/%s), retrying in %.1fs: %s",
                attempt + 1,
                max_attempts,
                delay,
                exc,
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


def make_engine(*, pool_pre_ping: bool = True, **kwargs):
    """Create a SQLAlchemy engine with postgres:// URL normalization."""
    url = cfg.DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=pool_pre_ping, **kwargs)


engine = make_engine(
    echo=False,
    pool_recycle=300,
    pool_size=3,
    max_overflow=5,
)


# Idempotent column additions for tables that pre-date newer model fields
# (create_all only creates missing tables, never alters existing ones).
_ENSURE_COLUMNS_SQL = """
ALTER TABLE discovery_company_snapshot ADD COLUMN IF NOT EXISTS pricing_model VARCHAR;
ALTER TABLE discovery_company_snapshot ADD COLUMN IF NOT EXISTS page_fingerprints JSONB;
ALTER TABLE discovery_company_snapshot ADD COLUMN IF NOT EXISTS recent_launches JSONB;
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS source VARCHAR NOT NULL DEFAULT 'apollo';
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS signal_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS signal_last_attempt_at TIMESTAMPTZ;
ALTER TABLE discovery_company ADD COLUMN IF NOT EXISTS ats_board JSONB;
ALTER TABLE discovery_company DROP COLUMN IF EXISTS location;
"""


def create_tables() -> None:
    """Create discovery_company and discovery_progress tables if they don't exist."""
    from sqlalchemy import text as _text
    import models  # noqa: F401 — registers SQLModel metadata
    SQLModel.metadata.create_all(engine, checkfirst=True)
    with engine.begin() as conn:
        for stmt in _ENSURE_COLUMNS_SQL.strip().split("\n"):
            if stmt.strip():
                conn.execute(_text(stmt))
    logger.info("Tables verified/created")


def get_session() -> Session:
    return Session(engine)
