"""Unit tests for scrape/domain_lookup.py helpers."""

from scrape.domain_lookup import extract_linkedin_url


def test_extract_linkedin_url_from_organic():
    data = {
        "organic": [
            {"link": "https://www.linkedin.com/in/jane-doe"},
            {"link": "https://www.linkedin.com/company/acme-corp/?trk=foo"},
        ]
    }
    assert extract_linkedin_url(data) == "https://www.linkedin.com/company/acme-corp"


def test_extract_linkedin_url_prefers_company_over_personal():
    data = {
        "organic": [
            {"link": "https://linkedin.com/in/ceo-profile"},
            {"link": "https://uk.linkedin.com/company/stripe"},
        ]
    }
    assert extract_linkedin_url(data) == "https://www.linkedin.com/company/stripe"


def test_extract_linkedin_url_from_knowledge_graph():
    data = {
        "knowledgeGraph": {"linkedin": "https://www.linkedin.com/company/vercel/"},
        "organic": [],
    }
    assert extract_linkedin_url(data) == "https://www.linkedin.com/company/vercel"


def test_extract_linkedin_url_none_when_missing():
    assert extract_linkedin_url({"organic": [{"link": "https://acme.com"}]}) is None
