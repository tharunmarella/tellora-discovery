"""
ATS detection and response validation for job-board fetching.

Tier-2 slug resolution: parse careers/homepage HTML for known ATS URL patterns
before falling back to slug_variants guessing.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

# (ats_source, regex) — first capture group is the board slug
_ATS_URL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("greenhouse", re.compile(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.I)),
    ("greenhouse", re.compile(r"boards-api\.greenhouse\.io/v1/boards/([a-zA-Z0-9_-]+)", re.I)),
    ("lever", re.compile(r"jobs\.lever\.co/([a-zA-Z0-9_-]+)", re.I)),
    ("ashby", re.compile(r"jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)", re.I)),
    ("smartrecruiters", re.compile(r"jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)", re.I)),
    ("workable", re.compile(r"apply\.workable\.com/([a-zA-Z0-9_-]+)", re.I)),
    ("rippling", re.compile(r"ats\.rippling\.com/([a-zA-Z0-9_-]+)", re.I)),
    ("jazzhr", re.compile(r"([a-zA-Z0-9_-]+)\.applytojob\.com", re.I)),
    ("icims", re.compile(r"((?:us)?careers-[a-zA-Z0-9_-]+)\.icims\.com", re.I)),
]

# Branded Greenhouse embed: careers.example.com/{slug}?gh_jid=123
_GH_EMBED = re.compile(
    r"https?://[^/\s\"']+/([a-zA-Z0-9_-]+)\?[^\"'\s]*gh_jid=\d+",
    re.I,
)

_SERP_SITE_BY_ATS = {
    "greenhouse": "boards.greenhouse.io",
    "lever": "jobs.lever.co",
    "ashby": "jobs.ashbyhq.com",
    "smartrecruiters": "jobs.smartrecruiters.com",
    "workable": "apply.workable.com",
    "rippling": "ats.rippling.com",
    "jazzhr": "applytojob.com",
    "icims": "icims.com",
}

_KNOWN_ATS_ROOTS = frozenset({
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "smartrecruiters.com",
    "workable.com",
    "rippling.com",
    "applytojob.com",
    "icims.com",
})

_SERP_URL_PATTERNS: dict[str, re.Pattern[str]] = {
    "greenhouse": re.compile(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.I),
    "lever": re.compile(r"jobs\.lever\.co/([a-zA-Z0-9_-]+)", re.I),
    "ashby": re.compile(r"jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)", re.I),
    "smartrecruiters": re.compile(r"jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)", re.I),
    "workable": re.compile(r"apply\.workable\.com/([a-zA-Z0-9_-]+)", re.I),
    "rippling": re.compile(r"ats\.rippling\.com/([a-zA-Z0-9_-]+)", re.I),
    "jazzhr": re.compile(r"([a-zA-Z0-9_-]+)\.applytojob\.com", re.I),
    "icims": re.compile(r"((?:us)?careers-[a-zA-Z0-9_-]+)\.icims\.com", re.I),
}


def _normalize_slug(slug: str, ats: str) -> str:
    slug = slug.split("/")[0].split("?")[0].strip()
    if ats == "ashby":
        return slug.lower()
    return slug


def detect_ats_candidates(*html_chunks: str) -> list[tuple[str, str]]:
    """
    Extract (ats_source, slug) pairs from careers/homepage HTML.
    Order preserved; duplicates removed.
    """
    text = "\n".join(c for c in html_chunks if c)
    if not text:
        return []

    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []

    def _add(ats: str, slug: str) -> None:
        slug = _normalize_slug(slug, ats)
        if len(slug) < 2:
            return
        key = (ats, slug)
        if key not in seen:
            seen.add(key)
            out.append(key)

    for ats, pattern in _ATS_URL_PATTERNS:
        for m in pattern.finditer(text):
            _add(ats, m.group(1))

    for m in _GH_EMBED.finditer(text):
        _add("greenhouse", m.group(1))

    return out


def extract_slug_from_serp_url(ats: str, url: str) -> Optional[str]:
    pattern = _SERP_URL_PATTERNS.get(ats)
    if not pattern:
        return None
    m = pattern.search(url)
    if not m:
        return None
    return _normalize_slug(m.group(1), ats)


def serp_site_for_ats(ats: str) -> Optional[str]:
    return _SERP_SITE_BY_ATS.get(ats)


def _root_domain(host: str) -> str:
    host = (host or "").lower().lstrip("www.")
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def _name_tokens(name: str) -> set[str]:
    return {t for t in re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).split() if len(t) >= 3}


def validate_board_match(
    posts: list[dict],
    *,
    company_name: str,
    domain: Optional[str],
    source: str,
    slug_inferred: bool = False,
) -> bool:
    """
    Reject ATS hits that likely belong to a different company.
    When slug came from detection/cache, validation is lighter.
    """
    if not posts:
        return False

    target_root = _root_domain(domain or "")
    company_tokens = _name_tokens(company_name)

    if source == "greenhouse" and target_root and slug_inferred:
        for p in posts[:3]:
            url = p.get("absolute_url") or ""
            if not url:
                continue
            try:
                host_root = _root_domain(urlparse(url).netloc)
                if host_root in _KNOWN_ATS_ROOTS:
                    continue
                if host_root != target_root:
                    return False
            except Exception:
                pass
        return True

    if source == "greenhouse" and target_root:
        for p in posts[:5]:
            url = p.get("absolute_url") or ""
            if not url:
                continue
            try:
                host_root = _root_domain(urlparse(url).netloc)
                if host_root in _KNOWN_ATS_ROOTS:
                    continue
                if host_root != target_root:
                    return False
            except Exception:
                pass

    org_name = (posts[0].get("organization_name") or "").lower()
    if org_name and company_tokens:
        org_tokens = _name_tokens(org_name)
        if org_tokens and not (company_tokens & org_tokens):
            if not slug_inferred:
                return False

    return True
