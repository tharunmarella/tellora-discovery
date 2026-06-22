"""Unit tests for scrape/scrape_fields_backfill.py merge logic."""

from scrape.scrape_fields_backfill import merge_scrape_fields


def test_merge_fills_missing_linkedin_and_raw_meta():
    existing = {
        "name": "Acme",
        "domain": "acme.com",
        "linkedin_url": None,
        "raw_meta": None,
    }
    enrichment = {
        "linkedin_url": "https://www.linkedin.com/company/acme",
        "keywords": ["devtools"],
        "use_case": "Teams building web apps",
    }
    updates = merge_scrape_fields(existing, enrichment)
    assert updates["linkedin_url"] == "https://www.linkedin.com/company/acme"
    assert updates["raw_meta"] == {
        "keywords": ["devtools"],
        "use_case": "Teams building web apps",
    }
    assert updates["logo_url"] == "https://www.google.com/s2/favicons?domain=acme.com&sz=64"


def test_merge_does_not_overwrite_existing_values():
    existing = {
        "domain": "acme.com",
        "linkedin_url": "https://www.linkedin.com/company/acme-old",
        "ceo_name": "Jane Doe",
        "logo_url": "https://example.com/logo.png",
        "raw_meta": {"keywords": ["existing"], "use_case": "keep me"},
    }
    enrichment = {
        "linkedin_url": "https://www.linkedin.com/company/acme-new",
        "ceo_name": "John Smith",
        "keywords": ["new"],
        "use_case": "replace me",
    }
    assert merge_scrape_fields(existing, enrichment) == {}


def test_merge_fills_domain_when_missing():
    existing = {"domain": None, "linkedin_url": None}
    enrichment = {
        "domain": "newco.com",
        "linkedin_url": "https://www.linkedin.com/company/newco",
    }
    updates = merge_scrape_fields(existing, enrichment)
    assert updates["domain"] == "newco.com"
    assert updates["domain_resolved"] is True
    assert updates["linkedin_url"].endswith("/newco")


def test_merge_preserves_existing_raw_meta_keys():
    existing = {
        "domain": "acme.com",
        "raw_meta": {"keywords": ["keep"], "custom": "tag"},
    }
    enrichment = {"use_case": "buyers of X"}
    updates = merge_scrape_fields(existing, enrichment)
    assert updates["raw_meta"]["keywords"] == ["keep"]
    assert updates["raw_meta"]["custom"] == "tag"
    assert updates["raw_meta"]["use_case"] == "buyers of X"
