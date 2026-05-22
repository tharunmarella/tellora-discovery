"""Synchronous + async DB engines for the discovery service."""

import logging
from sqlmodel import create_engine, SQLModel, Session

import settings as cfg

logger = logging.getLogger("discovery.db")

_db_url = cfg.DATABASE_URL
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    _db_url,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=3,
    max_overflow=5,
)


def create_tables() -> None:
    """Create discovery_company and discovery_progress tables if they don't exist."""
    import models  # noqa: F401 — registers SQLModel metadata
    SQLModel.metadata.create_all(engine)
    logger.info("Tables verified/created")


def get_session() -> Session:
    return Session(engine)
