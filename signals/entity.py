"""
Company entity resolution — decide whether a name seen in a filing, WARN
notice, award or headline is one of the companies we track.

Name-only equality was the root of most noisy events ("Origin" ← Blue Origin,
"Redmond" ← the city). Matching here is:
  1. legal-suffix-aware cleaning (cleanco) so "Acme Robotics, Inc." == "acme robotics"
  2. exact matching on the cleaned name, its space-free form ("data dog" =
     "datadog"), or its distinctive tokens once descriptors are set aside
     ("Acme Robotics Systems" = "Acme Robotics"), never a lone shared word
     ("Acme Robotics" ≠ "Acme Analytics"). No edit-distance fuzzing:
     "Allyx Therapeutics" scored 93 against "Alloy Therapeutics".
  3. a hard ambiguity rule: if two tracked companies match, no match
  4. a stricter bar for short / single-word / generic names: they only match
     exactly, and only when the source's state agrees with the company's HQ

Batch jobs (Form D, 8-K, WARN) build one CompanyIndex per run; it is a few
thousand rows, so loading it is cheaper than one SQL query per filing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

from cleanco import basename
from sqlalchemy import text
from sqlalchemy.orm import Session

from signals.name_match import _GENERIC_TOKENS

# Industry descriptors that many unrelated companies share; the remaining
# "core" tokens are what identifies the company.
_DESCRIPTORS = frozenset({
    "therapeutics", "pharmaceuticals", "pharma", "bio", "biosciences", "biotherapeutics",
    "medical", "health", "healthcare", "systems", "technologies", "technology", "tech",
    "software", "solutions", "labs", "lab", "analytics", "ai", "data", "digital", "networks",
    "group", "holdings", "partners", "capital", "ventures", "global", "international",
    "services", "industries", "enterprises", "robotics", "energy", "sciences", "the", "and",
    "co", "company", "corp", "inc", "llc", "ltd", "hq", "io", "app", "platform", "cloud",
})

_US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar", "california": "ca",
    "colorado": "co", "connecticut": "ct", "delaware": "de", "district of columbia": "dc",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id", "illinois": "il",
    "indiana": "in", "iowa": "ia", "kansas": "ks", "kentucky": "ky", "louisiana": "la",
    "maine": "me", "maryland": "md", "massachusetts": "ma", "michigan": "mi", "minnesota": "mn",
    "mississippi": "ms", "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm", "new york": "ny",
    "north carolina": "nc", "north dakota": "nd", "ohio": "oh", "oklahoma": "ok", "oregon": "or",
    "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt", "virginia": "va",
    "washington": "wa", "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}


def clean_name(name: str) -> str:
    """'Acme Robotics, Inc.' → 'acme robotics'. Strips legal suffixes and punctuation."""
    n = (name or "").strip()
    if not n:
        return ""
    # cleanco strips one suffix per call ("Acme Holdings LLC" needs two passes)
    for _ in range(2):
        stripped = basename(n)
        if not stripped or stripped == n:
            break
        n = stripped
    n = n.lower().replace("&", " and ")
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def state_code(value: Optional[str]) -> Optional[str]:
    """'California' / 'CA' / 'San Francisco, CA' → 'ca'. None when unknown."""
    if not value:
        return None
    v = value.strip().lower()
    if v in _US_STATES:
        return _US_STATES[v]
    if len(v) == 2 and v in _US_STATES.values():
        return v
    for full, code in _US_STATES.items():
        if re.search(rf"\b{full}\b", v):
            return code
    m = re.search(r"\b([a-z]{2})\b\s*\d{0,5}\s*$", v)
    if m and m.group(1) in _US_STATES.values():
        return m.group(1)
    return None


def core_tokens(cleaned: str) -> frozenset[str]:
    """Distinctive tokens: 'alloy therapeutics' → {'alloy'}."""
    return frozenset(t for t in cleaned.split() if t not in _DESCRIPTORS)


def _keys(cleaned: str) -> list[str]:
    """
    Lookup keys for a cleaned name, strictest first:
      the name itself; its space-free form; the name with trailing descriptors
      stripped (never below two tokens: "acme robotics systems" → "acme robotics");
      its core tokens when at least two remain ("alloy" alone is too thin).
    """
    keys = [cleaned, "c:" + cleaned.replace(" ", "")]
    tokens = cleaned.split()
    while len(tokens) > 2 and tokens[-1] in _DESCRIPTORS:
        tokens = tokens[:-1]
    keys.append("t:" + " ".join(tokens))
    core = core_tokens(cleaned)
    if len(core) >= 2:
        keys.append("k:" + " ".join(sorted(core)))
    return keys


_COMMON_WORD_ZIPF = 2.5  # wordfreq: "origin" 4.6, "redmond" 3.2, "figma" 1.7, "hyliion" 0


def _is_common_word(token: str) -> bool:
    from wordfreq import zipf_frequency

    return zipf_frequency(token, "en") >= _COMMON_WORD_ZIPF


def _is_compound_of_common_words(token: str) -> bool:
    """'datahub' = 'data' + 'hub' — as collision-prone as the two words."""
    return any(
        _is_common_word(token[:i]) and _is_common_word(token[i:])
        for i in range(3, len(token) - 2)
    )


def is_weak_name(cleaned: str) -> bool:
    """
    Names that collide with other companies, places and people — unsafe on name
    alone: a single common word ("Origin", "Redmond", "Notion"), a compound of
    common words ("DataHub"), or all-generic tokens ("Data Labs"). A coined
    single word ("Hyliion", "Careismatic") is distinctive enough to match.
    """
    tokens = cleaned.split()
    if not tokens:
        return True
    if len(tokens) == 1:
        t = tokens[0]
        return (
            len(t) <= 3 or t in _GENERIC_TOKENS or t in _DESCRIPTORS
            or _is_common_word(t) or _is_compound_of_common_words(t)
        )
    return all(t in _GENERIC_TOKENS or t in _DESCRIPTORS or len(t) <= 2 for t in tokens)


@dataclass(frozen=True)
class TrackedCompany:
    id: str
    name: str
    domain: Optional[str]
    cleaned: str
    state: Optional[str]


@dataclass(frozen=True)
class EntityMatch:
    company: TrackedCompany
    score: float
    reason: str


class CompanyIndex:
    """In-memory index of tracked companies for one batch run."""

    def __init__(self, companies: Iterable[TrackedCompany]):
        self.companies = [c for c in companies if c.cleaned]
        self._by_key: dict[str, dict[str, TrackedCompany]] = {}
        for c in self.companies:
            for key in _keys(c.cleaned):
                self._by_key.setdefault(key, {})[c.id] = c

    @classmethod
    def load(cls, session: Session) -> "CompanyIndex":
        rows = session.execute(text("""
            SELECT id, name, apollo_org_name, domain, hq_region, headquarters
            FROM discovery_company
        """)).mappings().all()
        companies = []
        for r in rows:
            state = state_code(r["hq_region"]) or state_code(r["headquarters"])
            for raw in {r["name"], r["apollo_org_name"]}:
                cleaned = clean_name(raw or "")
                if cleaned:
                    companies.append(TrackedCompany(str(r["id"]), r["name"], r["domain"], cleaned, state))
        return cls(companies)

    def match(self, name: str, *, state: Optional[str] = None) -> Optional[EntityMatch]:
        """
        The tracked company an external name refers to, or None.

        Tries the cleaned name, then the space-free form, then the core tokens
        (strong names only). The first key with exactly one company wins; two
        candidates are disambiguated by the source's state or rejected. Weak
        names additionally need the source state to equal the company's HQ state.
        """
        cleaned = clean_name(name)
        if not cleaned:
            return None
        filer_state = state_code(state)
        weak = is_weak_name(cleaned)
        for key in _keys(cleaned):
            reason = {"c:": "compact_name", "t:": "descriptor_suffix", "k:": "core_tokens"}.get(key[:2], "exact_name")
            if weak and reason in ("descriptor_suffix", "core_tokens"):
                break
            candidates = list(self._by_key.get(key, {}).values())
            if not candidates:
                continue
            if len(candidates) > 1:
                same_state = [c for c in candidates if filer_state and c.state == filer_state]
                if len(same_state) != 1:
                    return None
                return EntityMatch(same_state[0], 100.0, f"{reason}+state_disambiguated")
            company = candidates[0]
            if weak:
                if filer_state and company.state == filer_state:
                    return EntityMatch(company, 100.0, f"{reason}+state")
                return None
            if filer_state and company.state and company.state != filer_state and reason != "exact_name":
                return None
            return EntityMatch(company, 100.0 if reason == "exact_name" else 95.0, reason)
        return None


def name_matches_company(external_name: str, company_name: str) -> bool:
    """One-off check: is an external name (award recipient, filer) this company?"""
    a, b = clean_name(external_name), clean_name(company_name)
    if not a or not b:
        return False
    if a == b or a.replace(" ", "") == b.replace(" ", ""):
        return True
    if is_weak_name(b):
        return False
    return bool(set(_keys(a)[2:]) & set(_keys(b)[2:]))
