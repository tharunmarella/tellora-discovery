"""
Name/slug matching helpers — reduce false ATS hits and noisy news/HN/gov events.
"""

from __future__ import annotations

import re
from typing import Optional

# Common short company tokens that collide with unrelated ATS boards / news
_GENERIC_TOKENS = frozenset({
    "scale", "ramp", "notion", "apollo", "stripe", "square", "circle",
    "linear", "beam", "flow", "wave", "spark", "pulse", "core", "base",
    "path", "bridge", "cloud", "data", "tech", "labs", "ai", "io",
})

_FUNDING_KEYWORDS = re.compile(
    r"\b(series\s*[a-e]|seed\s*round|raised\s+\$|funding\s+round|\$\d+[\d,.]*\s*[mbk]?)\b",
    re.IGNORECASE,
)


def slug_variants(company_name: str, domain: Optional[str] = None) -> list[str]:
    """
    ATS slug candidates. Prefer domain stem over bare first-word name fragments.
    """
    variants: list[str] = []

    if domain:
        stem = domain.lower().replace("www.", "").split(".")[0]
        if stem and len(stem) >= 2:
            variants.append(stem)

    base = company_name.lower().strip()
    for suffix in [
        " inc", " inc.", " llc", " ltd", " corp", " corporation",
        " co", " co.", " technologies", " technology", " tech",
        " solutions", " group", " labs", " ai", " io", ".io", ".ai",
    ]:
        if base.endswith(suffix):
            base = base[: -len(suffix)].strip()

    hyphen = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    nospace = re.sub(r"[^a-z0-9]+", "", base)
    first_word = hyphen.split("-")[0] if "-" in hyphen else ""

    for v in (hyphen, nospace):
        if v and v not in variants:
            variants.append(v)

    # Bare first_word is high collision risk — only allow long, non-generic tokens
    if (
        first_word
        and len(first_word) >= 6
        and first_word not in _GENERIC_TOKENS
        and first_word != hyphen
        and first_word not in variants
    ):
        variants.append(first_word)

    return list(dict.fromkeys(variants))


def company_name_tokens(company_name: str) -> list[str]:
    base = re.sub(r"[^a-z0-9]+", " ", (company_name or "").lower()).strip()
    return [t for t in base.split() if len(t) >= 2]


def name_relevance_multiplier(company_name: str) -> float:
    """Discount confidence for short or generic company names."""
    tokens = company_name_tokens(company_name)
    if not tokens:
        return 0.5
    if len(tokens) == 1 and len(tokens[0]) <= 4:
        return 0.5
    if any(t in _GENERIC_TOKENS for t in tokens):
        return 0.6
    return 1.0


def title_mentions_company(title: str, company_name: str) -> bool:
    """Require at least one company token as a whole word in the title."""
    if not title or not company_name:
        return False
    title_l = title.lower()
    tokens = company_name_tokens(company_name)
    if not tokens:
        return False
    # Multi-word names: require the longest token (usually distinctive)
    if len(tokens) >= 2:
        return any(re.search(rf"\b{re.escape(t)}\b", title_l) for t in tokens if len(t) >= 4)
    token = tokens[0]
    if len(token) <= 4:
        return token in title_l and len(title_l) < 80
    return bool(re.search(rf"\b{re.escape(token)}\b", title_l))


def adjust_events_confidence(events: list[dict], company_name: str) -> list[dict]:
    mult = name_relevance_multiplier(company_name)
    if mult >= 1.0:
        return events
    adjusted = []
    for ev in events:
        copy = dict(ev)
        copy["confidence"] = round(float(copy.get("confidence", 0.85)) * mult, 3)
        adjusted.append(copy)
    return adjusted


def filter_events_by_relevance(events: list[dict], company_name: str) -> list[dict]:
    """Drop name-only events whose title doesn't mention the company."""
    kept = []
    for ev in events:
        title = ev.get("title") or ""
        if title_mentions_company(title, company_name):
            kept.append(ev)
    return adjust_events_confidence(kept, company_name)


def apply_funding_grounding(
    *,
    buying_signals: list[str],
    signal_score: int,
    funding_stage: Optional[str],
    total_raised: Optional[str],
    funding_news: list[str],
    extra_events: list[dict],
) -> tuple[list[str], int, Optional[str], Optional[str]]:
    """
    Drop or downrank funding claims not corroborated by structured sources.
    """
    has_corroboration = bool(funding_news) or any(
        e.get("event_type") == "funding_round" for e in extra_events
    )
    if has_corroboration:
        return buying_signals, signal_score, funding_stage, total_raised

    funding_signals = [s for s in buying_signals if _FUNDING_KEYWORDS.search(s)]
    non_funding_signals = [s for s in buying_signals if s not in funding_signals]

    if funding_stage or total_raised or funding_signals:
        capped_score = min(signal_score, 35 if non_funding_signals else 25)
        return non_funding_signals, capped_score, None, None

    return buying_signals, signal_score, funding_stage, total_raised
