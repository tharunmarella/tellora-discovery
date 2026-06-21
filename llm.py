"""Shared LLM helpers for the discovery service."""

from __future__ import annotations

import logging
import time

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
