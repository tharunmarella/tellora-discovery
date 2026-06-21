"""Synchronous + async DB engines for the discovery service."""

import logging
from sqlmodel import create_engine, SQLModel, Session

import settings as cfg

logger = logging.getLogger("discovery.db")


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
