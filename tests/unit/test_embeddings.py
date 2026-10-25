"""llm.embed_texts / embed_text provider dispatch."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import llm


def test_openai_batch_keeps_input_order(monkeypatch):
    monkeypatch.setattr(llm.cfg, "EMBEDDING_PROVIDER", "openai")
    monkeypatch.setattr(llm.cfg, "OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")
    client = MagicMock()
    client.embeddings.create.return_value = SimpleNamespace(data=[
        SimpleNamespace(index=1, embedding=[2.0]), SimpleNamespace(index=0, embedding=[1.0]),
    ])
    monkeypatch.setattr(llm, "get_openai_client", lambda: client)
    assert llm.embed_texts(["a", "b"]) == [[1.0], [2.0]]
    kwargs = client.embeddings.create.call_args.kwargs
    assert kwargs["model"] == "text-embedding-3-large" and kwargs["dimensions"] == 768
    assert llm.embedding_model() == "openai/text-embedding-3-large"


def test_embed_text_without_key_returns_none(monkeypatch):
    monkeypatch.setattr(llm.cfg, "EMBEDDING_PROVIDER", "openai")
    monkeypatch.setattr(llm.cfg, "OPENAI_API_KEY", "")
    assert llm.embed_text("hello") is None


def test_embed_text_swallows_api_errors(monkeypatch):
    monkeypatch.setattr(llm.cfg, "EMBEDDING_PROVIDER", "openai")
    monkeypatch.setattr(llm.cfg, "OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(llm, "embed_texts", MagicMock(side_effect=RuntimeError("boom")))
    assert llm.embed_text("hello") is None
