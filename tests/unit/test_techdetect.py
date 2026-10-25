"""Unit tests for signals/techdetect.py (Wappalyzer-format fingerprint matching)."""

from signals.techdetect import Fingerprints, tech_slug

CATEGORIES = {"1": {"name": "CRM"}, "2": {"name": "Analytics"}, "3": {"name": "CDN"}, "4": {"name": "Live chat"}}
TECHS = {
    "HubSpot": {
        "cats": [1],
        "scriptSrc": ["js\\.hs-scripts\\.com"],
        "dns": {"TXT": ["hubspot-developer-verification"]},
    },
    "Google Analytics": {"cats": [2], "scriptSrc": "google-analytics\\.com/analytics\\.js"},
    "Cloudflare": {"cats": [3], "headers": {"server": "^cloudflare$"}},
    "Intercom": {"cats": [4], "scripts": "intercomSettings", "implies": r"Segment\;confidence:50"},
    "Segment": {"cats": [2], "html": "never-matches-xyz"},
    "Grafana": {"cats": [2], "scripts": r'.+latestVersion":"[\d\.\w\-]+"\,"version":"([\d\.]+)\;version:\1'},
    "Shopify Plus": {"cats": [2], "html": "shopify", "requires": "Shopify"},
}
FP = Fingerprints(TECHS, CATEGORIES)


def _names(dets):
    return {d.name for d in dets}


def test_tech_slug():
    assert tech_slug("Google Analytics") == "google_analytics"
    assert tech_slug("HubSpot") == "hubspot"


def test_detects_script_header_and_dns():
    html = '<html><script src="https://js.hs-scripts.com/123.js"></script></html>'
    dets = FP.detect(html=html, headers={"Server": "cloudflare"}, dns={"TXT": ["v=spf1", "hubspot-developer-verification=abc"]})
    assert _names(dets) == {"HubSpot", "Cloudflare"}
    hub = next(d for d in dets if d.name == "HubSpot")
    assert hub.categories == ("CRM",) and hub.evidence.startswith("script:")


def test_inline_script_and_implies():
    dets = FP.detect(html="<script>window.intercomSettings = {app_id: 'x'}</script>")
    assert {"Intercom", "Segment"} <= _names(dets)
    assert next(d for d in dets if d.name == "Segment").evidence == "implied_by:Intercom"


def test_requires_drops_unsatisfied():
    assert "Shopify Plus" not in _names(FP.detect(html="<p>shopify</p>"))


def test_leading_wildcard_pattern_is_fast_on_long_scripts():
    import time

    blob = "<script>" + ("a" * 180_000) + "</script>"
    started = time.time()
    FP.detect(html=blob)
    assert time.time() - started < 2


def test_no_false_positive_on_plain_link():
    # The old regex list matched any link to hubspot.com
    assert "HubSpot" not in _names(FP.detect(html='<a href="https://www.hubspot.com/blog">read</a>'))
