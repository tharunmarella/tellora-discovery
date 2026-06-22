"""Shared LLM helpers for the discovery service."""

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache
from typing import List, Optional

import settings as cfg

logger = logging.getLogger("discovery.llm")

EMBED_MODEL = "gemini-embedding-001"

_gemini_client = None


def get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=cfg.GEMINI_API_KEY)
    return _gemini_client


# ── LiteLLM gateway (multi-provider fallback) ───────────────────────────────


def to_litellm_model(model: str) -> str:
    """Normalize a bare model id to a LiteLLM provider-qualified id (gemini/* default)."""
    model = (model or "").strip()
    if not model:
        raise ValueError("model name required")
    return model if "/" in model else f"gemini/{model}"


def model_chain(*models: Optional[str]) -> List[str]:
    """Dedupe + normalize an ordered list of models into a LiteLLM fallback chain."""
    seen: set[str] = set()
    chain: List[str] = []
    for model in models:
        if not model:
            continue
        normalized = to_litellm_model(model)
        if normalized not in seen:
            seen.add(normalized)
            chain.append(normalized)
    return chain


def _cross_provider_model() -> Optional[str]:
    return cfg.LLM_CROSS_PROVIDER_MODEL if getattr(cfg, "ANTHROPIC_API_KEY", "") else None


def enrichment_model_chain() -> List[str]:
    return model_chain(
        cfg.ENRICHMENT_GEMINI_MODEL,
        cfg.ENRICHMENT_GEMINI_FALLBACK_MODEL,
        _cross_provider_model(),
    )


def signal_model_chain(*, primary: Optional[str] = None) -> List[str]:
    """Signal-path chain; optional primary override (e.g. HQ_NORMALIZE_MODEL)."""
    return model_chain(
        primary or cfg.SIGNAL_GEMINI_MODEL,
        cfg.SIGNAL_GEMINI_FALLBACK_MODEL,
        _cross_provider_model(),
    )


class LLMRouter:
    """
    Thin LiteLLM wrapper that runs a completion against a primary model and
    automatically falls back across models/providers on failure.

    Mirrors the recall-backend gateway: each provider authenticates with its own
    env key (LiteLLM reads GEMINI_API_KEY for gemini/*, ANTHROPIC_API_KEY for
    anthropic/*), so a mixed chain "just works" once the keys are exported.
    """

    def __init__(self) -> None:
        self._export_provider_keys()

    def _export_provider_keys(self) -> None:
        if cfg.GEMINI_API_KEY:
            os.environ.setdefault("GEMINI_API_KEY", cfg.GEMINI_API_KEY)
        if getattr(cfg, "ANTHROPIC_API_KEY", ""):
            os.environ.setdefault("ANTHROPIC_API_KEY", cfg.ANTHROPIC_API_KEY)

    @property
    def enrichment_models(self) -> List[str]:
        return enrichment_model_chain()

    @property
    def signal_models(self) -> List[str]:
        return signal_model_chain()

    @property
    def synthesis_models(self) -> List[str]:
        """Alias for signal_models (main synthesis call)."""
        return self.signal_models

    def complete_text(
        self,
        prompt: str,
        *,
        models: Optional[List[str]] = None,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """Synchronous completion with fallbacks. Returns the message text."""
        import litellm

        litellm.suppress_debug_info = True

        chain = models or self.synthesis_models
        if not chain:
            raise RuntimeError("no models configured for LLMRouter")

        primary, *fallbacks = chain
        kwargs = {
            "model": primary,
            "messages": [{"role": "user", "content": prompt}],
            "fallbacks": fallbacks or None,
            "num_retries": cfg.LITELLM_NUM_RETRIES,
            "temperature": temperature,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = litellm.completion(**kwargs)
        model_used = getattr(response, "model", None) or primary
        logger.info("LiteLLM completion via %s", model_used)
        return (response.choices[0].message.content or "").strip()


@lru_cache(maxsize=1)
def get_router() -> LLMRouter:
    return LLMRouter()


def retry_llm(fn, max_retries: int = 3):
    backoff = 5
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            exc_str = str(exc).lower()
            is_retryable = any(
                s in exc_str for s in ("429", "rate", "quota", "resource_exhausted", "503", "timeout")
            )
            if not is_retryable or attempt == max_retries - 1:
                raise
            logger.warning(f"Gemini retryable error (attempt {attempt+1}): {exc} — backing off {backoff}s")
            time.sleep(backoff)
            backoff *= 2


def strip_json_fences(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip().rstrip("`").strip()
    return text


def embed_text(text: str) -> list[float] | None:
    """Embed text with gemini-embedding-001 (768-dim, RETRIEVAL_DOCUMENT)."""
    if not text or not cfg.GEMINI_API_KEY:
        return None
    try:
        from google.genai import types

        def _do():
            client = get_gemini_client()
            resp = client.models.embed_content(
                model=EMBED_MODEL,
                contents=text,
                config=types.EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768,
                ),
            )
            return resp.embeddings[0].values

        return retry_llm(_do)
    except Exception as exc:
        logger.warning(f"Embedding failed: {exc}")
        return None
