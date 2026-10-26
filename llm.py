"""LiteLLM model ladder — P1 (default) → P2 → P3."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from functools import lru_cache
from typing import List, Optional

import settings as cfg

logger = logging.getLogger("discovery.llm")

MODEL_P1 = os.getenv("MODEL_P1", "openai/gpt-6-luna").strip()
MODEL_P2 = os.getenv("MODEL_P2", "openai/gpt-5.6-luna").strip()
MODEL_P3 = os.getenv("MODEL_P3", "").strip()


def configure_litellm() -> None:
    import litellm

    litellm.suppress_debug_info = True
    litellm.drop_params = True


async def drain_litellm(timeout: float = 5.0) -> None:
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
    model = (model or "").strip()
    if not model:
        raise ValueError("model name required")
    return model if "/" in model else f"openai/{model}"


def model_chain(*models: Optional[str]) -> List[str]:
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


def _model_available(model: str) -> bool:
    if model.startswith("openai/"):
        return bool(cfg.OPENAI_API_KEY)
    if model.startswith("anthropic/"):
        return bool(getattr(cfg, "ANTHROPIC_API_KEY", ""))
    return False


def default_model_chain() -> List[str]:
    chain = model_chain(MODEL_P1, MODEL_P2, MODEL_P3)
    available = [m for m in chain if _model_available(m)]
    if not available:
        raise RuntimeError("No LLM models available — set OPENAI_API_KEY")
    return available


def llm_available() -> bool:
    """True when at least one model in the default chain has its API key."""
    return any(_model_available(m) for m in model_chain(MODEL_P1, MODEL_P2, MODEL_P3))


def signal_model_chain(*, primary: Optional[str] = None) -> List[str]:
    """Signal-path chain; optional primary override (e.g. HQ_NORMALIZE_MODEL)."""
    if primary:
        lead = to_litellm_model(primary)
        rest = [m for m in default_model_chain() if m != lead]
        return [lead, *rest]
    return default_model_chain()


def enrichment_model_chain() -> List[str]:
    return default_model_chain()


class LLMRouter:
    def __init__(self) -> None:
        self._export_provider_keys()

    def _export_provider_keys(self) -> None:
        if cfg.OPENAI_API_KEY:
            os.environ.setdefault("OPENAI_API_KEY", cfg.OPENAI_API_KEY)
        if getattr(cfg, "ANTHROPIC_API_KEY", ""):
            os.environ.setdefault("ANTHROPIC_API_KEY", cfg.ANTHROPIC_API_KEY)

    @property
    def enrichment_models(self) -> List[str]:
        return enrichment_model_chain()

    @property
    def signal_models(self) -> List[str]:
        return default_model_chain()

    @property
    def synthesis_models(self) -> List[str]:
        return self.signal_models

    def complete_text(
        self,
        prompt: str,
        *,
        models: Optional[List[str]] = None,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        import litellm

        configure_litellm()
        chain = models or self.synthesis_models
        if not chain:
            raise RuntimeError("no models configured for LLMRouter")

        last_exc: Exception | None = None
        for model in chain:
            kwargs = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "num_retries": cfg.LITELLM_NUM_RETRIES,
                "temperature": temperature,
            }
            if json_mode:
                kwargs["response_format"] = {"type": "json_object"}
            if "luna" in model:
                # luna rejects temperature != 1 unless reasoning is off; extraction wants it off anyway
                kwargs["reasoning_effort"] = "none"
                # with drop_params, LiteLLM silently drops reasoning_effort for
                # models its param map doesn't list (gpt-6-luna) unless allowed
                kwargs["allowed_openai_params"] = ["reasoning_effort"]
            try:
                response = litellm.completion(**kwargs)
                text = (response.choices[0].message.content or "").strip()
                if text:
                    logger.info("LiteLLM completion via %s", getattr(response, "model", model))
                    return text
            except Exception as exc:
                last_exc = exc
                logger.warning("LiteLLM completion failed via %s: %s", model, exc)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("all models in chain returned empty completion")


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


_openai_client = None


def get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=cfg.OPENAI_API_KEY)
    return _openai_client


def embedding_model() -> str:
    """Names the vector space stored in description_embedding."""
    return f"openai/{cfg.OPENAI_EMBEDDING_MODEL}"


def embedding_available() -> bool:
    return cfg.EMBEDDING_PROVIDER == "openai" and bool(cfg.OPENAI_API_KEY)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Document embeddings, one per text, in one API call. Raises on failure."""
    def _do():
        resp = get_openai_client().embeddings.create(
            model=cfg.OPENAI_EMBEDDING_MODEL, input=texts, dimensions=cfg.EMBEDDING_DIM,
        )
        return [d.embedding for d in sorted(resp.data, key=lambda d: d.index)]

    return retry_llm(_do)


def embed_text(text: str) -> list[float] | None:
    if not text or not embedding_available():
        return None
    try:
        return embed_texts([text])[0]
    except Exception as exc:
        logger.warning(f"Embedding failed: {exc}")
        return None
