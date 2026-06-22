"""Unit tests for signals/name_match.py."""

from signals.name_match import (
    apply_funding_grounding,
    filter_events_by_relevance,
    name_relevance_multiplier,
    slug_variants,
    title_mentions_company,
)


def test_slug_variants_prefers_domain_stem():
    variants = slug_variants("Scale Inc", domain="scale.com")
    assert variants[0] == "scale"


def test_slug_variants_strips_suffix():
    variants = slug_variants("Acme Technologies LLC", domain=None)
    assert "acme" in variants or "acme-technologies" in variants


def test_slug_variants_generic_token_guard():
    variants = slug_variants("Notion", domain=None)
    assert "notion" in variants
    # hyphen form included; bare first_word blocked for generic short tokens


def test_name_relevance_multiplier_generic():
    assert name_relevance_multiplier("Ramp") < 1.0


def test_name_relevance_multiplier_distinctive():
    assert name_relevance_multiplier("Beacon Biosignals") == 1.0


def test_title_mentions_company_multi_word():
    assert title_mentions_company("Beacon Biosignals raises Series B", "Beacon Biosignals")


def test_filter_events_by_relevance_drops_irrelevant():
    events = [
        {"title": "Unrelated headline about markets", "confidence": 0.9},
        {"title": "Beacon Biosignals launches new product", "confidence": 0.9},
    ]
    kept = filter_events_by_relevance(events, "Beacon Biosignals")
    assert len(kept) == 1
    assert "Beacon" in kept[0]["title"]


def test_apply_funding_grounding_corroborated():
    signals, score, stage, raised = apply_funding_grounding(
        buying_signals=["Series B raised $25M"],
        signal_score=80,
        funding_stage="Series B",
        total_raised="$25M",
        funding_news=["Acme raised $25M Series B"],
        extra_events=[],
    )
    assert stage == "Series B"
    assert score == 80


def test_apply_funding_grounding_strips_uncorroborated():
    signals, score, stage, raised = apply_funding_grounding(
        buying_signals=["Series B raised $25M", "Hiring engineers"],
        signal_score=80,
        funding_stage="Series B",
        total_raised="$25M",
        funding_news=[],
        extra_events=[],
    )
    assert stage is None
    assert raised is None
    assert "Series B" not in " ".join(signals)
    assert score <= 35


def test_apply_funding_grounding_strips_fabricated_stage_despite_news():
    # News exists but mentions an older round; LLM fabricated "Series F".
    signals, score, stage, raised = apply_funding_grounding(
        buying_signals=["Series F raised $500M", "Launched new product"],
        signal_score=90,
        funding_stage="Series F",
        total_raised="$500M",
        funding_news=["Acme closed a Series C round in 2021"],
        extra_events=[],
    )
    assert stage is None
    assert raised is None
    assert "Series F" not in " ".join(signals)


def test_apply_funding_grounding_keeps_corroborated_via_event():
    signals, score, stage, raised = apply_funding_grounding(
        buying_signals=["Series D raised $100M"],
        signal_score=85,
        funding_stage="Series D",
        total_raised="$100M",
        funding_news=[],
        extra_events=[{"event_type": "funding_round", "title": "Acme raises Series D"}],
    )
    assert stage == "Series D"
    assert score == 85
