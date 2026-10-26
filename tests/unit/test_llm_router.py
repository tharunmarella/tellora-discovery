"""Unit tests for the LiteLLM gateway router (no network)."""

import asyncio

import pytest

from llm import (
    LLMRouter,
    default_model_chain,
    llm_available,
    model_chain,
    signal_model_chain,
    to_litellm_model,
)


def _openai_chain(monkeypatch, *, openai_key="test-key", groq_key=""):
    import llm as llm_mod

    monkeypatch.setattr(llm_mod, "MODEL_P1", "gpt-6-luna")
    monkeypatch.setattr(llm_mod, "MODEL_P2", "openai/gpt-5.6-luna")
    monkeypatch.setattr(llm_mod, "MODEL_P3", "")
    monkeypatch.setattr(llm_mod, "MODEL_P4", "groq/openai/gpt-oss-20b")
    monkeypatch.setattr(llm_mod.cfg, "OPENAI_API_KEY", openai_key)
    monkeypatch.setattr(llm_mod.cfg, "GROQ_API_KEY", groq_key, raising=False)


def test_to_litellm_model_adds_openai_prefix():
    assert to_litellm_model("gpt-6-luna") == "openai/gpt-6-luna"


def test_to_litellm_model_keeps_provider_prefix():
    assert to_litellm_model("groq/openai/gpt-oss-20b") == "groq/openai/gpt-oss-20b"


def test_to_litellm_model_requires_name():
    with pytest.raises(ValueError):
        to_litellm_model("")


def test_model_chain_dedupes_and_skips_blanks():
    chain = model_chain("gpt-6-luna", None, "openai/gpt-6-luna", "groq/x")
    assert chain == ["openai/gpt-6-luna", "groq/x"]


def test_default_model_chain_is_openai_and_skips_keyless_groq(monkeypatch):
    _openai_chain(monkeypatch)
    assert default_model_chain() == ["openai/gpt-6-luna", "openai/gpt-5.6-luna"]


def test_default_model_chain_without_keys_raises(monkeypatch):
    _openai_chain(monkeypatch, openai_key="")
    assert llm_available() is False
    with pytest.raises(RuntimeError):
        default_model_chain()


def test_llm_available_with_only_groq(monkeypatch):
    _openai_chain(monkeypatch, openai_key="", groq_key="g")
    assert llm_available() is True
    assert default_model_chain() == ["groq/openai/gpt-oss-20b"]


def test_signal_model_chain_primary_override(monkeypatch):
    _openai_chain(monkeypatch)
    assert signal_model_chain(primary="gpt-custom")[0] == "openai/gpt-custom"


def test_complete_text_tries_models_in_order(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "LITELLM_NUM_RETRIES", 1)

    calls: list[dict] = []

    class _Msg:
        content = "  hello  "

    class _Choice:
        message = _Msg()

    class _Resp:
        model = "openai/gpt-6-luna"
        choices = [_Choice()]

    import litellm

    def _fake_completion(**kwargs):
        calls.append(kwargs)
        if kwargs["model"] == "openai/a":
            raise RuntimeError("fail primary")
        return _Resp()

    monkeypatch.setattr(litellm, "completion", _fake_completion)

    router = LLMRouter()
    out = router.complete_text("prompt", models=["openai/a", "openai/gpt-6-luna"], temperature=0.0)
    assert out == "hello"
    assert [c["model"] for c in calls] == ["openai/a", "openai/gpt-6-luna"]
    # luna only accepts temperature=0 with reasoning off
    assert "reasoning_effort" not in calls[0]
    assert calls[1]["reasoning_effort"] == "none"


def test_configure_litellm_sets_drop_params():
    import litellm

    from llm import configure_litellm

    configure_litellm()
    assert litellm.drop_params is True


def test_drain_litellm_noop_without_clients():
    from llm import drain_litellm

    asyncio.run(drain_litellm())
