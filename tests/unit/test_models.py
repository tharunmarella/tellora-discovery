"""Unit tests for SQLModel schema."""

from models import DiscoveryCompany, DiscoveryJobPost


def test_discovery_company_has_no_location_field():
    assert "location" not in DiscoveryCompany.model_fields


def test_discovery_job_post_keeps_location_field():
    assert "location" in DiscoveryJobPost.model_fields
