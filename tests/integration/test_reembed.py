"""Integration tests for signals/reembed.py (embedding API mocked)."""

from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlmodel import Session

import settings as cfg
from signals.reembed import reembed_companies

pytestmark = pytest.mark.integration


@pytest.fixture
def openai_large(monkeypatch):
    monkeypatch.setattr(cfg, "EMBEDDING_PROVIDER", "openai")
    monkeypatch.setattr(cfg, "OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")
    monkeypatch.setattr(cfg, "OPENAI_API_KEY", "sk-test")
    calls = []

    def fake(texts):
        calls.append(list(texts))
        return [[0.5] * 768 for _ in texts]

    with patch("llm.embed_texts", side_effect=fake):
        yield calls


def _set(db, cid, **cols):
    sets = ", ".join(f"{k} = :{k}" for k in cols)
    db.execute(text(f"UPDATE discovery_company SET {sets} WHERE id = :id"), {"id": cid, **cols})
    db.commit()


@pytest.mark.asyncio
async def test_reembeds_resumably_and_clears_rows_without_text(db_session, company_factory, pg_engine, openai_large):
    calls = openai_large
    old = "[" + ",".join(["0.1"] * 768) + "]"
    a = company_factory(name="A", domain="a.com")
    b = company_factory(name="B", domain="b.com")
    c = company_factory(name="C", domain="c.com")
    _set(db_session, a, company_summary="Estimating software for contractors", industry="Construction")
    _set(db_session, b, description="Dental billing")
    db_session.execute(text("UPDATE discovery_company SET description_embedding = CAST(:v AS vector)"), {"v": old})
    db_session.commit()

    with Session(pg_engine) as s:
        first = await reembed_companies(s, batch=2, max_batches=1)
    assert first["status"] == "running" and first["embedded"] + first["cleared"] == 2

    with Session(pg_engine) as s:
        rest = await reembed_companies(s, batch=2)
    assert rest["status"] == "completed" and rest["embedded"] == 2 and rest["cleared"] == 1
    assert sorted(t for batch in calls for t in batch) == ["Dental billing", "Estimating software for contractors Construction"]

    vecs = dict(db_session.execute(text(
        "SELECT id, description_embedding::text FROM discovery_company WHERE id = ANY(:ids)"
    ), {"ids": [a, b, c]}).all())
    assert vecs[a].startswith("[0.5") and vecs[b].startswith("[0.5") and vecs[c] is None

    made = len(calls)
    with Session(pg_engine) as s:
        again = await reembed_companies(s)
    assert again["skipped"] == "completed" and len(calls) == made


@pytest.mark.asyncio
async def test_new_model_starts_its_own_run(db_session, company_factory, pg_engine, openai_large, monkeypatch):
    _set(db_session, company_factory(), description="Roofing")
    with Session(pg_engine) as s:
        assert (await reembed_companies(s))["status"] == "completed"
    monkeypatch.setattr(cfg, "OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    with Session(pg_engine) as s:
        out = await reembed_companies(s)
    assert out["model"] == "openai/text-embedding-3-small" and out["embedded"] == 1


@pytest.mark.asyncio
async def test_no_key_does_nothing(pg_engine, monkeypatch):
    monkeypatch.setattr(cfg, "EMBEDDING_PROVIDER", "openai")
    monkeypatch.setattr(cfg, "OPENAI_API_KEY", "")
    with Session(pg_engine) as s:
        assert (await reembed_companies(s))["skipped"] == "no_api_key"


def test_enrichment_without_embedding_keeps_stored_vector(db_session, company_factory, pg_engine):
    from signals.runner import persist_result

    cid = company_factory()
    db_session.execute(text("UPDATE discovery_company SET description_embedding = CAST(:v AS vector) WHERE id = :id"),
                       {"v": "[" + ",".join(["0.3"] * 768) + "]", "id": cid})
    db_session.commit()
    with Session(pg_engine) as s:
        assert persist_result(s, cid, {"company_summary": "x", "description_embedding": None})
        s.commit()
    kept = db_session.execute(text("SELECT description_embedding IS NOT NULL FROM discovery_company WHERE id = :id"),
                              {"id": cid}).scalar()
    assert kept
