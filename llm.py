"""Shared LLM helpers for the discovery service."""

from __future__ import annotations

import asyncio
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


def configure_litellm() -> None:
    """One-time LiteLLM globals (idempotent)."""
    import litellm

    litellm.suppress_debug_info = True
    litellm.drop_params = True


async def drain_litellm(timeout: float = 5.0) -> None:
    """
    Drain LiteLLM logging and close cached aiohttp clients.

    Sync litellm.completion() run via run_in_executor can leave aiohttp sessions
    open on the worker event loop — asyncio then logs 'Unclosed connector'.
    """
    configure_litellm()
    try:
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

        await asyncio.wait_for(GLOBAL_LOGGING_WORKER.flush(), timeout=timeout)
    except Exception:
        logger.debug("LiteLLM logging drain skipped", exc_info=True)
    try:
        import litellm

        close_fn = getattr(litellm, "close_litellm_async_clients", None)
        if close_fn is not None:
            await asyncio.wait_for(close_fn(), timeout=timeout)
    except Exception:
        logger.debug("LiteLLM async client close skipped", exc_info=True)


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

        configure_litellm()

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


_TRANSIENT_LLM_MARKERS = (
    "429",
    "rate",
    "quota",
    "resource_exhausted",
    "503",
    "timeout",
    "unavailable",
    "serviceunavailable",
    "high demand",
    # Transient network / DNS (Railway container blips)
    "name or service not known",
    "temporary failure in name resolution",
    "connection refused",
    "connection reset",
    "network is unreachable",
    "connect error",
    "errno -2",
    "errno -3",
    "getaddrinfo failed",
)


def is_transient_llm_error(exc: BaseException) -> bool:
    """True when the provider or network is briefly unavailable (safe to retry)."""
    if isinstance(exc, TimeoutError):
        return True
    exc_str = str(exc).lower()
    exc_type = type(exc).__name__.lower()
    if "connecterror" in exc_type or "retryerror" in exc_type:
        return any(marker in exc_str for marker in _TRANSIENT_LLM_MARKERS)
    return any(marker in exc_str for marker in _TRANSIENT_LLM_MARKERS)


def retry_llm(fn, max_retries: int = 3):
    backoff = 5
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            if not is_transient_llm_error(exc) or attempt == max_retries - 1:
                raise
            logger.warning(
                "LLM retryable error (attempt %s/%s): %s — backing off %ss",
                attempt + 1,
                max_retries,
                exc,
                backoff,
            )
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
