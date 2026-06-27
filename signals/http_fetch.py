"""
HTTP HTML fetch with optional httpcloak fallback on WAF blocks.

When HTTPCLOAK_FALLBACK=true and httpx gets 403/429 (or connection errors),
retries via httpcloak browser TLS fingerprinting (jobhive-style auto fallback).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx

import settings as cfg

logger = logging.getLogger("discovery.http_fetch")

_BROWSER_UA = {"User-Agent": "Mozilla/5.0 (compatible; TelloraDiscovery/1.0)"}
_DEFAULT_TIMEOUT = 30.0


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    text: str
    via: str  # httpx | httpcloak
    headers: dict[str, str] = field(default_factory=dict)


def httpcloak_enabled() -> bool:
    return bool(cfg.HTTPCLOAK_FALLBACK)


def _should_try_httpcloak(status_code: Optional[int]) -> bool:
    if not httpcloak_enabled():
        return False
    if status_code is None:
        return True
    return status_code in (403, 429)


def _httpcloak_get_sync(url: str, *, timeout: float = _DEFAULT_TIMEOUT) -> FetchResult:
    try:
        import httpcloak
    except ImportError as exc:
        raise RuntimeError(
            "HTTPCLOAK_FALLBACK is enabled but httpcloak is not installed — pip install httpcloak"
        ) from exc

    session = httpcloak.Session(preset="chrome-latest", timeout=max(1, int(timeout)))
    try:
        resp = session.get(url, headers=dict(_BROWSER_UA))
        hdrs = {str(k).lower(): str(v) for k, v in (resp.headers or {}).items()}
        return FetchResult(
            status_code=int(resp.status_code),
            text=resp.text or "",
            via="httpcloak",
            headers=hdrs,
        )
    finally:
        session.close()


async def _httpcloak_get(url: str, *, timeout: float = _DEFAULT_TIMEOUT) -> FetchResult:
    return await asyncio.to_thread(_httpcloak_get_sync, url, timeout=timeout)


async def fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout: Optional[float] = None,
    extra_headers: Optional[dict[str, str]] = None,
    allow_httpcloak: bool = True,
) -> FetchResult:
    """
    GET url as HTML. Tries httpx first; on 403/429 or transport error optionally
    falls back to httpcloak when HTTPCLOAK_FALLBACK is enabled.
    """
    headers = dict(_BROWSER_UA)
    if extra_headers:
        headers.update(extra_headers)

    req_timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT

    try:
        resp = await client.get(url, headers=headers, timeout=req_timeout)
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
        if resp.status_code == 200:
            return FetchResult(status_code=200, text=resp.text, via="httpx", headers=hdrs)
        if allow_httpcloak and _should_try_httpcloak(resp.status_code):
            logger.info("[http_fetch] %s → %s via httpx; trying httpcloak", url, resp.status_code)
            cloak = await _httpcloak_get(url, timeout=req_timeout)
            if cloak.status_code == 200:
                return cloak
            logger.debug(
                "[http_fetch] httpcloak for %s returned %s",
                url,
                cloak.status_code,
            )
        return FetchResult(
            status_code=resp.status_code, text=resp.text or "", via="httpx", headers=hdrs,
        )
    except httpx.HTTPError as exc:
        if allow_httpcloak and httpcloak_enabled():
            logger.info("[http_fetch] httpx error for %s (%s); trying httpcloak", url, exc)
            try:
                return await _httpcloak_get(url, timeout=req_timeout)
            except Exception as cloak_exc:
                logger.debug("[http_fetch] httpcloak failed for %s: %s", url, cloak_exc)
        return FetchResult(status_code=0, text="", via="httpx")
