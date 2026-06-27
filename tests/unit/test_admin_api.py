"""Unit tests for discovery admin API."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

import settings as cfg


@pytest.fixture
def admin_client(monkeypatch):
    monkeypatch.setattr(cfg, "DISCOVERY_ADMIN_SECRET", "test-admin-secret")
    from api.admin import app

    with TestClient(app) as client:
        yield client


def test_health_no_auth(admin_client):
    resp = admin_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_admin_requires_token(admin_client):
    resp = admin_client.get("/admin/jobs")
    assert resp.status_code == 401


def test_scrape_fields_eligible(admin_client, monkeypatch):
    monkeypatch.setattr(
        "scrape.scrape_fields_backfill.count_eligible_rows",
        lambda source="apollo": 42,
    )
    resp = admin_client.get(
        "/admin/scrape-fields/eligible",
        headers={"Authorization": "Bearer test-admin-secret"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"source": "apollo", "eligible": 42}


def test_start_scrape_backfill_returns_job(admin_client):
    with patch("api.admin.submit_job", new_callable=AsyncMock) as mock_submit:
        from api.job_runner import JobRecord

        job = JobRecord(id="job-1", job_type="scrape_fields_backfill", params={"dry_run": True})
        mock_submit.return_value = job
        resp = admin_client.post(
            "/admin/jobs/scrape-fields-backfill",
            headers={"Authorization": "Bearer test-admin-secret"},
            json={"dry_run": True, "limit": 10},
        )
    assert resp.status_code == 202
    assert resp.json()["job"]["id"] == "job-1"
    assert resp.json()["job"]["status"] == "queued"
