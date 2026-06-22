"""Shared pytest fixtures for tellora-discovery."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

# Required before any project import — settings.py calls _require() at import time.
os.environ.setdefault("DATABASE_URL", "postgresql://placeholder:placeholder@localhost:5432/placeholder")
os.environ.setdefault("TELLORA_APOLLO_API_KEY", "test-apollo-key")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def clear_domain_cache():
    """Prevent cross-test pollution from pipeline domain context cache."""
    from signals.pipeline import _DOMAIN_CACHE

    _DOMAIN_CACHE._data.clear()
    yield
    _DOMAIN_CACHE._data.clear()


@pytest.fixture
def gemini_stub(monkeypatch):
    """Stub Gemini client to return canned JSON from generate_content."""

    def _apply(response_json: dict | str):
        if isinstance(response_json, dict):
            import json

            text_body = json.dumps(response_json)
        else:
            text_body = response_json

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = text_body
        mock_client.models.generate_content.return_value = mock_resp
        monkeypatch.setattr("llm._gemini_client", None)
        monkeypatch.setattr("llm.get_gemini_client", lambda: mock_client)

        # LLM text paths route through the LiteLLM gateway — stub complete_text.
        # complete_text so tests never hit the network.
        mock_router = MagicMock()
        mock_router.complete_text.return_value = text_body
        mock_router.enrichment_models = ["gemini/test-enrichment"]
        mock_router.signal_models = ["gemini/test-signal"]
        mock_router.synthesis_models = ["gemini/test-signal"]
        for target in (
            "llm.get_router",
            "signals.pipeline.get_router",
            "scrape.domain_lookup.get_router",
            "signals.job_posts.get_router",
            "signals.sources.news.get_router",
        ):
            monkeypatch.setattr(target, lambda _r=mock_router: _r)
        return mock_client

    return _apply


@pytest.fixture
def embed_stub(monkeypatch):
    """Return a fixed 768-dim embedding vector."""
    fake = [0.1] * 768
    monkeypatch.setattr("llm.embed_text", lambda _text: fake)
    monkeypatch.setattr("signals.pipeline.embed_text", lambda _text: fake)
    return fake


@pytest.fixture
def fakeredis(monkeypatch):
    import fakeredis.aioredis

    server = fakeredis.FakeServer()
    fake = fakeredis.aioredis.FakeRedis(server=server)

    monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: fake)
    monkeypatch.setattr("redis.from_url", lambda *_a, **_k: fakeredis.FakeRedis(server=server))
    return fake


def _patch_engines(engine) -> None:
    import database
    import worker
    import signals.monitoring as monitoring

    database.engine = engine
    worker._engine = engine
    monitoring._engine = engine


def _ensure_org_research_table(conn) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS org_research_company (
            id VARCHAR PRIMARY KEY,
            company_id VARCHAR NOT NULL,
            org_id VARCHAR,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))


@pytest.fixture(scope="session")
def pg_engine():
    """Session-scoped pgvector Postgres via testcontainers."""
    pytest.importorskip("testcontainers")
    from testcontainers.postgres import PostgresContainer

    from database import make_engine
    import settings as cfg

    with PostgresContainer("pgvector/pgvector:pg15") as postgres:
        url = postgres.get_connection_url()
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)

        cfg.DATABASE_URL = url
        engine = make_engine(pool_pre_ping=True, pool_size=2, max_overflow=2)

        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

        _patch_engines(engine)

        import database
        import models  # noqa: F401
        from sqlmodel import SQLModel

        SQLModel.metadata.create_all(engine, checkfirst=True)

        with engine.begin() as conn:
            for stmt in database._ENSURE_COLUMNS_SQL.strip().split("\n"):
                if stmt.strip():
                    conn.execute(text(stmt))
            _ensure_org_research_table(conn)

        yield engine
        engine.dispose()


_TRUNCATE_ORDER = [
    "discovery_signal_event",
    "discovery_company_snapshot",
    "discovery_job_post",
    "discovery_edge",
    "discovery_filing",
    "org_research_company",
    "discovery_progress",
    "discovery_company",
]


@pytest.fixture
def db_session(pg_engine) -> Iterator[Session]:
    """Function-scoped session; truncates tables after each test."""
    session = Session(pg_engine)
    try:
        yield session
        session.commit()
    finally:
        session.close()
        with pg_engine.begin() as conn:
            for table in _TRUNCATE_ORDER:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))


@pytest.fixture
def company_factory(db_session):
    """Insert a minimal discovery_company row; returns company_id."""

    def _create(
        *,
        name: str = "Acme Corp",
        domain: str = "acme.com",
        status: str = "pending",
        domain_resolved: bool = True,
        signal_enriched_at=None,
        signal_attempt_count: int = 0,
        signal_last_attempt_at=None,
        headcount: int | None = None,
    ) -> str:
        company_id = str(uuid.uuid4())
        db_session.execute(
            text("""
                INSERT INTO discovery_company (
                    id, apollo_org_name, name, domain, domain_resolved,
                    enrichment_status, signal_enrichment_status,
                    signal_attempt_count, signal_last_attempt_at,
                    signal_enriched_at, headcount,
                    first_seen_at, last_seen_at, created_at, updated_at
                ) VALUES (
                    :id, :name, :name, :domain, :domain_resolved,
                    'enriched', :status,
                    :attempt_count, :last_attempt_at,
                    :enriched_at, :headcount,
                    NOW(), NOW(), NOW(), NOW()
                )
            """),
            {
                "id": company_id,
                "name": name,
                "domain": domain,
                "domain_resolved": domain_resolved,
                "status": status,
                "attempt_count": signal_attempt_count,
                "last_attempt_at": signal_last_attempt_at,
                "enriched_at": signal_enriched_at,
                "headcount": headcount,
            },
        )
        db_session.flush()
        db_session.commit()
        return company_id

    return _create


@pytest.fixture
def watched_company_factory(db_session, company_factory):
    """Create discovery_company + org_research_company link."""

    def _create(**kwargs) -> str:
        company_id = company_factory(**kwargs)
        db_session.execute(
            text("""
                INSERT INTO org_research_company (id, company_id, org_id)
                VALUES (:id, :company_id, :org_id)
            """),
            {"id": str(uuid.uuid4()), "company_id": company_id, "org_id": "org-test"},
        )
        db_session.flush()
        db_session.commit()
        return company_id

    return _create
