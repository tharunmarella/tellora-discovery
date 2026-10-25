"""Unit tests for the LiteLLM gateway router (no network)."""

import asyncio

import pytest

from llm import (
    LLMRouter,
    MODEL_P1,
    MODEL_P2,
    default_model_chain,
    model_chain,
    signal_model_chain,
    to_litellm_model,
)


def test_to_litellm_model_adds_gemini_prefix():
    assert to_litellm_model("gemini-3.1-flash-lite") == "gemini/gemini-3.1-flash-lite"


def test_to_litellm_model_keeps_provider_prefix():
    assert to_litellm_model("groq/openai/gpt-oss-20b") == "groq/openai/gpt-oss-20b"


def test_to_litellm_model_requires_name():
    with pytest.raises(ValueError):
        to_litellm_model("")


def test_model_chain_dedupes_and_skips_blanks():
    chain = model_chain("gemini-3.1-flash-lite", None, "gemini-3.1-flash-lite", "groq/x")
    assert chain == ["gemini/gemini-3.1-flash-lite", "groq/x"]


def test_default_model_chain_starts_with_p1(monkeypatch):
    import llm as llm_mod

    monkeypatch.setattr(llm_mod, "MODEL_P1", "gemini-3.1-flash-lite")
    monkeypatch.setattr(llm_mod, "MODEL_P2", "gemini-2.5-flash")
    monkeypatch.setattr(llm_mod, "MODEL_P3", "gemini-2.5-flash-lite")
    monkeypatch.setattr(llm_mod, "MODEL_P4", "groq/openai/gpt-oss-20b")
    monkeypatch.setattr(llm_mod.cfg, "GEMINI_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(llm_mod.cfg, "GROQ_API_KEY", "", raising=False)

    chain = default_model_chain()
    assert chain[0] == "gemini/gemini-3.1-flash-lite"
    assert "gemini/gemini-2.5-flash" in chain


def test_signal_model_chain_primary_override(monkeypatch):
    import llm as llm_mod

    monkeypatch.setattr(llm_mod, "MODEL_P1", "gemini-3.1-flash-lite")
    monkeypatch.setattr(llm_mod, "MODEL_P2", "gemini-2.5-flash")
    monkeypatch.setattr(llm_mod, "MODEL_P3", "gemini-2.5-flash-lite")
    monkeypatch.setattr(llm_mod, "MODEL_P4", "groq/openai/gpt-oss-20b")
    monkeypatch.setattr(llm_mod.cfg, "GEMINI_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(llm_mod.cfg, "GROQ_API_KEY", "", raising=False)

    assert signal_model_chain(primary="gemini-custom")[0] == "gemini/gemini-custom"


def test_complete_text_tries_models_in_order(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "LITELLM_NUM_RETRIES", 1)

    calls: list[str] = []

    class _Msg:
        content = "  hello  "

    class _Choice:
        message = _Msg()

    class _Resp:
        model = "gemini/gemini-2.5-flash"
        choices = [_Choice()]

    import litellm

    def _fake_completion(**kwargs):
        calls.append(kwargs["model"])
        if kwargs["model"] == "gemini/a":
            raise RuntimeError("fail primary")
        return _Resp()

    monkeypatch.setattr(litellm, "completion", _fake_completion)

    router = LLMRouter()
    out = router.complete_text("prompt", models=["gemini/a", "gemini/b"], temperature=0.0)
    assert out == "hello"
    assert calls == ["gemini/a", "gemini/b"]


def test_configure_litellm_sets_drop_params():
    import litellm

    from llm import configure_litellm

    configure_litellm()
    assert litellm.drop_params is True


def test_drain_litellm_noop_without_clients():
    from llm import drain_litellm

    asyncio.run(drain_litellm())
