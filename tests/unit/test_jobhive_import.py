"""Unit tests for jobhive slug import matching."""

from pathlib import Path

from signals.jobhive_import import (
    build_ats_board,
    find_jobhive_match,
    normalize_company_key,
    parse_jobhive_csv,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


def test_normalize_company_key_strips_suffixes():
    assert normalize_company_key("Acme Technologies Inc.") == "acme"
    assert normalize_company_key("15Five") == "15five"


def test_parse_jobhive_csv():
    text = (FIXTURES / "jobhive_sample.csv").read_text()
    index = parse_jobhive_csv("greenhouse", text)
    assert index.by_slug["greenhouse"]["vercel"][0] == "Vercel"
    assert index.by_slug["greenhouse"]["stripe"][0] == "Stripe"


def test_find_jobhive_match():
    index = parse_jobhive_csv("greenhouse", (FIXTURES / "jobhive_sample.csv").read_text())
    index.add("lever", "Netflix", "netflix")
    index.add("ashby", "Ramp", "ramp")

    assert find_jobhive_match("Vercel", "vercel.com", index) == ("greenhouse", "vercel")
    assert find_jobhive_match("Stripe Inc", "stripe.com", index) == ("greenhouse", "stripe")
    assert find_jobhive_match("Netflix", "nflx.com", index) == ("lever", "netflix")
    assert find_jobhive_match("Ramp", "ramp.com", index) == ("ashby", "ramp")
    assert find_jobhive_match("Unknown Co", "unknown.io", index) is None


def test_build_ats_board_shape():
    board = build_ats_board("greenhouse", "vercel")
    assert board["source"] == "greenhouse"
    assert board["slug"] == "vercel"
    assert board["imported_from"] == "jobhive"
    assert "verified_at" in board


def test_lookup_ats_board_from_jobhive_uses_existing():
    from signals.jobhive_import import lookup_ats_board_from_jobhive

    existing = {"source": "lever", "slug": "acme"}
    assert lookup_ats_board_from_jobhive("Acme", "acme.com", existing=existing) is existing


def test_lookup_ats_board_from_jobhive_matches_index(monkeypatch):
    from signals import jobhive_import as jhi

    index = parse_jobhive_csv("greenhouse", (FIXTURES / "jobhive_sample.csv").read_text())
    monkeypatch.setattr(jhi, "get_jobhive_index", lambda **_: index)

    board = jhi.lookup_ats_board_from_jobhive("Stripe Inc", "stripe.com")
    assert board is not None
    assert board["source"] == "greenhouse"
    assert board["slug"] == "stripe"

    assert jhi.lookup_ats_board_from_jobhive("Unknown", "unknown.io") is None
