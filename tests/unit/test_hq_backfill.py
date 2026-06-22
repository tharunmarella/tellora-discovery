"""Unit tests for HQ backfill helper in signals/pipeline.py."""

from signals.pipeline import backfill_hq_fields


def test_backfill_hq_fields_fills_when_city_missing():
    def _fake_normalizer(raw: str) -> dict:
        assert raw == "San Francisco, CA"
        return {"hq_city": "San Francisco", "hq_region": "CA", "hq_country": "US"}

    city, region, country = backfill_hq_fields(
        None, None, None, "San Francisco, CA", normalizer=_fake_normalizer
    )
    assert city == "San Francisco"
    assert region == "CA"
    assert country == "US"


def test_backfill_hq_fields_noop_when_city_present():
    def _fake_normalizer(_raw: str) -> dict:
        raise AssertionError("should not call normalizer")

    city, region, country = backfill_hq_fields(
        "Boston", "MA", "US", "San Francisco, CA", normalizer=_fake_normalizer
    )
    assert (city, region, country) == ("Boston", "MA", "US")


def test_backfill_hq_fields_noop_without_headquarters():
    def _fake_normalizer(_raw: str) -> dict:
        raise AssertionError("should not call normalizer")

    city, region, country = backfill_hq_fields(None, None, None, None, normalizer=_fake_normalizer)
    assert (city, region, country) == (None, None, None)
