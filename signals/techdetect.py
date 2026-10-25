"""
Technology detection with Wappalyzer-format fingerprints.

The fingerprint set (~4k technologies, maintained by HTTP Archive's fork of the
last open-source Wappalyzer) is downloaded at runtime and cached on disk — it
is GPL-3.0 data, so it is not vendored into this repo.

Matches the parts of a fingerprint that a plain HTTP fetch can see: response
headers, cookies, <meta> tags, <script src>, inline scripts, raw HTML, URL and
DNS records. `js` (runtime globals) and `dom` (CSS selectors) need a browser
and are skipped. `implies` / `requires` / `excludes` are honoured.

Every detection carries its evidence (which field matched), so tech_stack is
grounded — no LLM guessing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

import settings as cfg

logger = logging.getLogger("discovery.techdetect")

_FILES = ["_"] + [chr(c) for c in range(ord("a"), ord("z") + 1)]
_HTML_SCAN_MAX = 600_000
_INLINE_SCRIPT_MAX = 50_000
_load_lock = threading.Lock()
_FINGERPRINTS: Optional["Fingerprints"] = None


def tech_slug(name: str) -> str:
    """'Google Analytics' → 'google_analytics' (the key stored in tech_stack)."""
    return re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")


def _as_list(v: Any) -> list:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _compile(pattern: str) -> Optional[re.Pattern]:
    """Wappalyzer pattern 'regex\\;version:\\1\\;confidence:50' → compiled regex."""
    regex = (pattern or "").split("\\;")[0]
    # A leading .* / .+ never changes whether search() matches, but makes it
    # quadratic on long inline scripts (one Grafana pattern took 60s on a 180KB bundle).
    regex = re.sub(r"^\^?\.[*+]\??", "", regex)
    try:
        return re.compile(regex, re.IGNORECASE)
    except re.error:
        return None


@dataclass
class _Tech:
    name: str
    categories: list[str]
    patterns: dict[str, Any] = field(default_factory=dict)
    implies: list[str] = field(default_factory=list)
    requires: list[str] = field(default_factory=list)
    excludes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Detection:
    slug: str
    name: str
    categories: tuple[str, ...]
    evidence: str


class Fingerprints:
    def __init__(self, technologies: dict[str, dict], categories: dict[str, dict]):
        cat_names = {str(k): v.get("name", str(k)) for k, v in categories.items()}
        self.techs: dict[str, _Tech] = {}
        for name, spec in technologies.items():
            t = _Tech(
                name=name,
                categories=[cat_names.get(str(c), str(c)) for c in _as_list(spec.get("cats"))],
                implies=[i.split("\\;")[0] for i in _as_list(spec.get("implies"))],
                requires=[r.split("\\;")[0] for r in _as_list(spec.get("requires"))],
                excludes=[e.split("\\;")[0] for e in _as_list(spec.get("excludes"))],
            )
            for key in ("html", "scriptSrc", "scripts", "url", "text"):
                compiled = [c for c in (_compile(p) for p in _as_list(spec.get(key))) if c]
                if compiled:
                    t.patterns[key] = compiled
            for key in ("headers", "cookies", "meta"):
                mapping = spec.get(key) or {}
                if isinstance(mapping, dict) and mapping:
                    t.patterns[key] = {
                        k.lower(): [c for c in (_compile(p) for p in _as_list(v)) if c] or [re.compile("")]
                        for k, v in mapping.items()
                    }
            dns = spec.get("dns") or {}
            if isinstance(dns, dict) and dns:
                t.patterns["dns"] = {
                    k.upper(): [c for c in (_compile(p) for p in _as_list(v)) if c] for k, v in dns.items()
                }
            self.techs[name] = t

    def __len__(self) -> int:
        return len(self.techs)

    def detect(
        self,
        *,
        html: str = "",
        headers: Optional[dict[str, str]] = None,
        cookies: Optional[dict[str, str]] = None,
        url: str = "",
        dns: Optional[dict[str, list[str]]] = None,
    ) -> list[Detection]:
        html = (html or "")[:_HTML_SCAN_MAX]
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        cookies = {k.lower(): v for k, v in (cookies or {}).items()}
        dns = {k.upper(): v for k, v in (dns or {}).items()}
        script_srcs = re.findall(r"<script[^>]+src=[\"']([^\"']+)", html, re.IGNORECASE)
        inline_scripts = [
            s[:_INLINE_SCRIPT_MAX]
            for s in re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.IGNORECASE | re.S)
        ]
        metas: dict[str, list[str]] = {}
        for tag in re.findall(r"<meta\b[^>]*>", html, re.IGNORECASE):
            key = re.search(r"(?:name|property|http-equiv)=[\"']([^\"']+)", tag, re.IGNORECASE)
            content = re.search(r"content=[\"']([^\"']*)", tag, re.IGNORECASE)
            if key:
                metas.setdefault(key.group(1).lower(), []).append(content.group(1) if content else "")

        found: dict[str, str] = {}

        def _kv(patterns: dict, values: dict, label: str) -> Optional[str]:
            for k, regexes in patterns.items():
                vals = _as_list(values.get(k))
                if not vals:
                    continue
                for v in vals:
                    if any(r.search(str(v)) for r in regexes):
                        return f"{label}:{k}"
            return None

        for name, t in self.techs.items():
            p = t.patterns
            if not p:
                continue
            evidence = None
            if "scriptSrc" in p:
                evidence = next((f"script:{s[:120]}" for s in script_srcs if any(r.search(s) for r in p["scriptSrc"])), None)
            if not evidence and "headers" in p:
                evidence = _kv(p["headers"], headers, "header")
            if not evidence and "cookies" in p:
                evidence = _kv(p["cookies"], cookies, "cookie")
            if not evidence and "meta" in p:
                evidence = _kv(p["meta"], metas, "meta")
            if not evidence and "dns" in p:
                evidence = _kv(p["dns"], dns, "dns")
            if not evidence and "url" in p and url and any(r.search(url) for r in p["url"]):
                evidence = "url"
            if not evidence and "scripts" in p:
                if any(r.search(s) for s in inline_scripts for r in p["scripts"]):
                    evidence = "inline_script"
            if not evidence and "html" in p and any(r.search(html) for r in p["html"]):
                evidence = "html"
            if evidence:
                found[name] = evidence

        # implies (transitively), then requires / excludes
        queue = list(found)
        while queue:
            name = queue.pop()
            for implied in self.techs.get(name, _Tech(name, [])).implies:
                if implied in self.techs and implied not in found:
                    found[implied] = f"implied_by:{name}"
                    queue.append(implied)
        for name in list(found):
            t = self.techs[name]
            if t.requires and not any(r in found for r in t.requires):
                found.pop(name, None)
        for name in list(found):
            for ex in self.techs[name].excludes if name in found else []:
                found.pop(ex, None)

        return [
            Detection(tech_slug(n), n, tuple(self.techs[n].categories), ev)
            for n, ev in sorted(found.items())
        ]


def _download_sync() -> dict:
    base = cfg.TECH_FINGERPRINTS_URL.rstrip("/")
    technologies: dict[str, dict] = {}
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        categories = client.get(f"{base}/categories.json").raise_for_status().json()
        for f in _FILES:
            technologies.update(client.get(f"{base}/technologies/{f}.json").raise_for_status().json())
    return {"technologies": technologies, "categories": categories, "fetched_at": time.time()}


def load_fingerprints(*, allow_download: bool = True) -> Optional[Fingerprints]:
    """Cached Fingerprints, refreshed from TECH_FINGERPRINTS_URL when the disk copy is stale."""
    global _FINGERPRINTS
    if _FINGERPRINTS is not None:
        return _FINGERPRINTS
    with _load_lock:
        if _FINGERPRINTS is not None:
            return _FINGERPRINTS
        path = cfg.TECH_FINGERPRINTS_CACHE
        data = None
        try:
            if os.path.exists(path):
                with open(path) as fh:
                    data = json.load(fh)
        except Exception as exc:
            logger.warning(f"Tech fingerprint cache unreadable ({path}): {exc}")
        stale = not data or (time.time() - data.get("fetched_at", 0)) > cfg.TECH_FINGERPRINTS_MAX_AGE_H * 3600
        if stale and allow_download:
            try:
                fresh = _download_sync()
                tmp = f"{path}.tmp"
                with open(tmp, "w") as fh:
                    json.dump(fresh, fh)
                os.replace(tmp, path)
                data = fresh
            except Exception as exc:
                logger.warning(f"Tech fingerprint download failed: {exc}")
        if not data:
            return None
        _FINGERPRINTS = Fingerprints(data["technologies"], data["categories"])
        logger.info(f"Loaded {len(_FINGERPRINTS)} technology fingerprints")
        return _FINGERPRINTS


def resolve_dns_records(domain: str) -> dict[str, list[str]]:
    """TXT / MX / NS / CNAME strings for fingerprint DNS matching. Synchronous (dnspython)."""
    out: dict[str, list[str]] = {}
    if not domain:
        return out
    try:
        import dns.resolver
    except ImportError:
        return out
    resolver = dns.resolver.Resolver()
    resolver.timeout = 5
    resolver.lifetime = 8
    for rtype in ("TXT", "MX", "NS", "CNAME"):
        try:
            answers = resolver.resolve(domain, rtype)
        except Exception:
            continue
        values = []
        for rdata in answers:
            if rtype == "TXT":
                values.append(b"".join(rdata.strings).decode("utf-8", errors="ignore"))
            elif rtype == "MX":
                values.append(str(rdata.exchange))
            else:
                values.append(str(rdata.target))
        out[rtype] = values
    return out
