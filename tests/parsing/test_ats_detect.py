"""Tests for ATS URL detection and response validation."""

from signals.ats_detect import (
    detect_ats_candidates,
    extract_slug_from_serp_url,
    validate_board_match,
)


def test_detect_greenhouse_from_careers_html():
    html = 'Apply at https://boards.greenhouse.io/stripe/jobs/123'
    assert ("greenhouse", "stripe") in detect_ats_candidates(html)


def test_detect_lever_and_ashby():
    html = """
    https://jobs.lever.co/netflix
    https://jobs.ashbyhq.com/ramp
    """
    found = detect_ats_candidates(html)
    assert ("lever", "netflix") in found
    assert ("ashby", "ramp") in found


def test_detect_branded_greenhouse_gh_jid():
    html = 'https://careers.nebius.com/nebius?gh_jid=12345'
    assert ("greenhouse", "nebius") in detect_ats_candidates(html)


def test_extract_slug_from_serp_url():
    url = "https://boards.greenhouse.io/stripe/jobs/123"
    assert extract_slug_from_serp_url("greenhouse", url) == "stripe"


def test_validate_rejects_wrong_greenhouse_domain():
    posts = [{
        "title": "Engineer",
        "absolute_url": "https://careers.wrongco.com/jobs/1",
    }]
    assert not validate_board_match(
        posts,
        company_name="Stripe",
        domain="stripe.com",
        source="greenhouse",
        slug_inferred=False,
    )


def test_validate_accepts_matching_greenhouse_domain():
    posts = [{
        "title": "Engineer",
        "absolute_url": "https://boards.greenhouse.io/stripe/jobs/1",
    }]
    assert validate_board_match(
        posts,
        company_name="Stripe",
        domain="stripe.com",
        source="greenhouse",
        slug_inferred=False,
    )


def test_validate_inferred_slug_skips_strict_domain_when_no_urls():
    posts = [{"title": "Engineer"}]
    assert validate_board_match(
        posts,
        company_name="Stripe",
        domain="stripe.com",
        source="greenhouse",
        slug_inferred=True,
    )


def test_detect_rippling_jazzhr_icims():
    html = """
    https://ats.rippling.com/acme/jobs
    https://acme.applytojob.com/apply/jobs
    https://careers-peraton.icims.com/jobs/search
    https://uscareers-rws.icims.com/jobs
    """
    found = detect_ats_candidates(html)
    assert ("rippling", "acme") in found
    assert ("jazzhr", "acme") in found
    assert ("icims", "careers-peraton") in found
    assert ("icims", "uscareers-rws") in found


def test_extract_slug_from_extended_serp_urls():
    assert extract_slug_from_serp_url("rippling", "https://ats.rippling.com/foo/jobs") == "foo"
    assert extract_slug_from_serp_url("jazzhr", "https://bar.applytojob.com/apply/jobs") == "bar"
    assert (
        extract_slug_from_serp_url("icims", "https://careers-acme.icims.com/jobs/1")
        == "careers-acme"
    )
