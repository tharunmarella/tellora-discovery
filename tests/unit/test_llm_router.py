"""Unit tests for the LiteLLM gateway router (no network)."""

import asyncio

import pytest

from llm import LLMRouter, model_chain, to_litellm_model


def test_to_litellm_model_adds_gemini_prefix():
    assert to_litellm_model("gemini-3.1-flash-lite") == "gemini/gemini-3.1-flash-lite"


def test_to_litellm_model_keeps_provider_prefix():
    assert to_litellm_model("anthropic/claude-haiku-4-5") == "anthropic/claude-haiku-4-5"


def test_to_litellm_model_requires_name():
    with pytest.raises(ValueError):
        to_litellm_model("")


def test_model_chain_dedupes_and_skips_blanks():
    chain = model_chain("gemini-3.1-flash-lite", None, "gemini-3.1-flash-lite", "anthropic/x")
    assert chain == ["gemini/gemini-3.1-flash-lite", "anthropic/x"]


def test_synthesis_models_excludes_anthropic_without_key(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SIGNAL_GEMINI_MODEL", "gemini-3.1-flash-lite")
    monkeypatch.setattr(cfg, "SIGNAL_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(cfg, "LLM_CROSS_PROVIDER_MODEL", "anthropic/claude-haiku-4-5")
    monkeypatch.setattr(cfg, "ANTHROPIC_API_KEY", "")

    chain = LLMRouter().synthesis_models
    assert chain == ["gemini/gemini-3.1-flash-lite", "gemini/gemini-2.5-flash"]
    assert not any("anthropic" in m for m in chain)


def test_synthesis_models_includes_anthropic_with_key(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SIGNAL_GEMINI_MODEL", "gemini-3.1-flash-lite")
    monkeypatch.setattr(cfg, "SIGNAL_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(cfg, "LLM_CROSS_PROVIDER_MODEL", "anthropic/claude-haiku-4-5")
    monkeypatch.setattr(cfg, "ANTHROPIC_API_KEY", "sk-ant-test")

    chain = LLMRouter().synthesis_models
    assert chain[-1] == "anthropic/claude-haiku-4-5"


def test_enrichment_models_chain(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "ENRICHMENT_GEMINI_MODEL", "gemini-2.5-flash-lite")
    monkeypatch.setattr(cfg, "ENRICHMENT_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(cfg, "ANTHROPIC_API_KEY", "")

    assert LLMRouter().enrichment_models == [
        "gemini/gemini-2.5-flash-lite",
        "gemini/gemini-2.5-flash",
    ]


def test_signal_model_chain_primary_override(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "SIGNAL_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(cfg, "ANTHROPIC_API_KEY", "")

    from llm import signal_model_chain

    assert signal_model_chain(primary="gemini-custom") == [
        "gemini/gemini-custom",
        "gemini/gemini-2.5-flash",
    ]


def test_complete_text_uses_primary_and_fallbacks(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "LITELLM_NUM_RETRIES", 3)

    captured = {}

    class _Msg:
        content = "  hello  "

    class _Choice:
        message = _Msg()

    class _Resp:
        model = "gemini/gemini-2.5-flash"
        choices = [_Choice()]

    import litellm

    def _fake_completion(**kwargs):
        captured.update(kwargs)
        return _Resp()

    monkeypatch.setattr(litellm, "completion", _fake_completion)

    router = LLMRouter()
    out = router.complete_text(
        "prompt",
        models=["gemini/a", "gemini/b", "anthropic/c"],
        temperature=0.0,
    )
    assert out == "hello"
    assert captured["model"] == "gemini/a"
    assert captured["fallbacks"] == ["gemini/b", "anthropic/c"]
    assert captured["num_retries"] == 3


def test_configure_litellm_sets_drop_params():
    import litellm

    from llm import configure_litellm

    configure_litellm()
    assert litellm.drop_params is True


def test_drain_litellm_noop_without_clients():
    from llm import drain_litellm

    asyncio.run(drain_litellm())
