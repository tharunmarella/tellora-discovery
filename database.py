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


def create_tables() -> None:
    """Create discovery_company and discovery_progress tables if they don't exist."""
    import models  # noqa: F401 — registers SQLModel metadata
    SQLModel.metadata.create_all(engine, checkfirst=True)
    logger.info("Tables verified/created")


def get_session() -> Session:
    return Session(engine)
