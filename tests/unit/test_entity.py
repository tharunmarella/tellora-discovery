"""Unit tests for signals/entity.py (company entity resolution)."""

from signals.entity import (
    CompanyIndex,
    TrackedCompany,
    clean_name,
    name_matches_company,
    state_code,
)


def _co(id_, name, domain=None, state=None):
    return TrackedCompany(id_, name, domain, clean_name(name), state)


INDEX = CompanyIndex([
    _co("reframe", "Reframe Systems", "reframe.systems", "ma"),
    _co("origin", "Origin", "origin.com", "ca"),
    _co("tagup", "Tagup", "tagup.io", "ma"),
    _co("travere", "Travere Therapeutics", "travere.com", "ca"),
    _co("acme-ny", "Acme Analytics", "acme-ny.com", "ny"),
    _co("acme-tx", "Acme Analytics", "acme-tx.com", "tx"),
])


def test_clean_name_strips_legal_suffixes():
    assert clean_name("Reframe Systems, Inc.") == "reframe systems"
    assert clean_name("TAGUP, INC") == "tagup"
    assert clean_name("Smith & Wesson Brands LLC") == "smith and wesson brands"


def test_state_code_variants():
    assert state_code("California") == "ca"
    assert state_code("CA") == "ca"
    assert state_code("Boston, Massachusetts") == "ma"
    assert state_code("San Francisco, CA 94107") == "ca"
    assert state_code("Ontario") is None


def test_exact_strong_name_matches():
    hit = INDEX.match("REFRAME SYSTEMS INC")
    assert hit and hit.company.id == "reframe"


def test_fuzzy_strong_name_matches():
    hit = INDEX.match("Travere Therapeutics Inc.")
    assert hit and hit.company.id == "travere"


def test_weak_name_needs_corroboration():
    # "Origin" alone is a common word — Blue Origin filings must not land on it
    assert INDEX.match("Blue Origin LLC") is None
    assert INDEX.match("Origin") is None
    assert INDEX.match("Origin Inc", state="California").company.id == "origin"


def test_coined_single_word_matches_without_state():
    hit = INDEX.match("Tagup, Inc.")
    assert hit and hit.company.id == "tagup" and hit.reason == "exact_name"


def test_common_and_compound_words_are_weak():
    from signals.entity import is_weak_name

    assert is_weak_name("origin") and is_weak_name("redmond") and is_weak_name("datahub")
    assert not is_weak_name("hyliion") and not is_weak_name("careismatic")


def test_ambiguous_name_needs_state():
    assert INDEX.match("Acme Analytics Inc") is None
    assert INDEX.match("Acme Analytics Inc", state="TX").company.id == "acme-tx"


def test_unrelated_name_no_match():
    assert INDEX.match("Original Footwear Manufacturing BR Inc") is None


def test_name_matches_company():
    assert name_matches_company("TAGUP, INC", "Tagup")
    assert not name_matches_company("ORIGINAL FOOTWEAR MANUFACTURING BR INC", "Origin")


def test_one_letter_difference_is_not_a_match():
    index = CompanyIndex([_co("alloy", "Alloy Therapeutics, Inc.", "alloytx.com", "ma")])
    assert index.match("Allyx Therapeutics, Inc.") is None
    assert not name_matches_company("Allyx Therapeutics", "Alloy Therapeutics")


def test_compact_and_core_token_matches():
    index = CompanyIndex([
        _co("dd", "Datadog Cloud", "datadoghq.com", "ny"),
        _co("acme", "Acme Robotics", "acme.ai", "ca"),
    ])
    assert index.match("Data Dog Cloud Inc").company.id == "dd"
    assert index.match("Acme Robotics Systems Corp").company.id == "acme"
    assert index.match("Acme Robotics Systems Corp", state="TX") is None  # loose match, wrong state
    assert index.match("Acme Analytics Inc") is None  # one shared word is not enough
