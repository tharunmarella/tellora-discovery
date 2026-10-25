"""Integration tests for scrape/icp_fill.py (Apollo + domain lookup mocked)."""

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text
from sqlmodel import Session

from scrape.icp_fill import SOURCE, fill_index_for_icp, search_variants


pytestmark = pytest.mark.integration

FILTERS = {
    "organization_locations": ["United States"],
    "organization_num_employees_ranges": ["21,50"],
    "q_organization_job_titles": ["estimator", "superintendent"],
}


def _people(names: list[str], full_page: bool = True) -> dict:
    people = [{"first_name": "Pat", "organization": {"name": n}} for n in names]
    if full_page:  # pad with repeats so the page counts as full (100 people)
        people += [{"first_name": "Pat", "organization": {"name": names[0]}}] * (100 - len(people))
    return {"people": people, "total_entries": 900}


def _lookup(name: str, _first: str = "") -> dict:
    slug = name.lower().replace(" ", "")
    return {"domain": f"{slug}.com", "website_url": f"https://{slug}.com",
            "description": f"{name} is a commercial general contractor.",
            "industry": "Construction", "description_embedding": [0.01] * 768}


@pytest.fixture
def mocks():
    with patch("scrape.apollo_client.search_page", new_callable=AsyncMock) as search, \
         patch("scrape.domain_lookup.lookup_domain", new=AsyncMock(side_effect=_lookup)), \
         patch("signals.pipeline.fetch_apollo_headcount", new=AsyncMock(return_value={"headcount_estimate": 40})), \
         patch("scrape.icp_fill._queue_enrichment", new_callable=AsyncMock) as enqueue, \
         patch("scrape.icp_fill._daily_budget_left", new=AsyncMock(return_value=1000)), \
         patch("scrape.icp_fill._spend_budget", new_callable=AsyncMock), \
         patch("scrape.apollo_client.ApolloRateLimiter.wait", new=AsyncMock()):
        yield search, enqueue


def test_variants_keep_tags_and_titles_apart():
    variants = dict(search_variants({**FILTERS, "q_organization_keyword_tags": ["construction"]}))
    assert set(variants) == {"tags", "hiring"}
    assert "q_organization_job_titles" not in variants["tags"]
    assert "q_organization_keyword_tags" not in variants["hiring"]
    assert variants["hiring"]["person_titles"][0] == "CEO"
    assert variants["hiring"]["organization_num_employees_ranges"] == ["21,50"]
    assert dict(search_variants({}))["base"]["person_seniorities"]


@pytest.mark.asyncio
async def test_fill_adds_new_tags_known_and_resumes(db_session, company_factory, mocks, pg_engine):
    search, enqueue = mocks
    known = company_factory(name="Known Builders", domain="knownbuilders.com")
    clash = company_factory(name="Clash Co Legacy", domain="clashco.com")  # same domain, other name
    search.side_effect = [
        _people(["Known Builders", "Acme Contractors", "Clash Co"]),
        _people(["Beta Build", "Gamma Group Construction"]),
    ]

    with Session(pg_engine) as s:
        stats = await fill_index_for_icp(s, "org_1", FILTERS, max_new=3, max_pages=1, api_key="k")

    assert stats["added"] == 1 and stats["already_known"] == 2
    rows = db_session.execute(text("""
        SELECT domain, source, source_profiles, description_embedding IS NOT NULL AS vec, headcount
        FROM discovery_company WHERE source = :s
    """), {"s": SOURCE}).mappings().all()
    assert [(r["domain"], r["vec"], r["headcount"]) for r in rows] == [("acmecontractors.com", True, 40)]
    assert rows[0]["source_profiles"] == ["icp:org_1"]
    tagged = db_session.execute(
        text("SELECT id, source_profiles FROM discovery_company WHERE id = ANY(:ids)"), {"ids": [known, clash]},
    ).all()
    assert all(p == ["icp:org_1"] for _, p in tagged)
    enqueue.assert_awaited_once()

    # next run continues at page 2 of the same variant
    with Session(pg_engine) as s:
        stats2 = await fill_index_for_icp(s, "org_1", FILTERS, max_new=1, max_pages=1, api_key="k")
    assert search.call_args.kwargs["page"] == 2
    assert stats2["added"] == 1  # capped by max_new


@pytest.mark.asyncio
async def test_short_page_exhausts_variant(db_session, mocks, pg_engine):
    search, _ = mocks
    search.side_effect = [_people(["Solo Roofing"], full_page=False)]
    with Session(pg_engine) as s:
        first = await fill_index_for_icp(s, "org_2", FILTERS, max_new=10, max_pages=5, api_key="k")
        second = await fill_index_for_icp(s, "org_2", FILTERS, max_new=10, max_pages=5, api_key="k")
    assert first["added"] == 1 and first["pages"] == 1
    assert second["variants"]["hiring"]["skipped"] == "exhausted"
    assert search.await_count == 1
    status = db_session.execute(
        text("SELECT status FROM discovery_progress WHERE run_id = 'icp:org_2:hiring'")
    ).scalar()
    assert status == "exhausted"


@pytest.mark.asyncio
async def test_daily_cap_stops_before_apollo(mocks, pg_engine):
    search, _ = mocks
    with patch("scrape.icp_fill._daily_budget_left", new=AsyncMock(return_value=0)), Session(pg_engine) as s:
        stats = await fill_index_for_icp(s, "org_3", FILTERS, max_new=10, api_key="k")
    assert stats["skipped"] == "daily_cap"
    search.assert_not_called()


@pytest.mark.asyncio
async def test_page_yield_recorded_and_new_icp_restarts_cursor(db_session, company_factory, mocks, pg_engine):
    search, _ = mocks
    company_factory(name="Known Builders", domain="knownbuilders.com")
    search.side_effect = [_people(["Known Builders", "Acme Contractors"]), _people(["Fresh Roofing"])]
    with Session(pg_engine) as s:
        await fill_index_for_icp(s, "org_4", FILTERS, max_new=10, max_pages=1, api_key="k")
    stats = db_session.execute(
        text("SELECT stats FROM discovery_progress WHERE run_id = 'icp:org_4:hiring'")
    ).scalar()
    assert stats["recent"] == [[1, 2]] and stats["filters"]

    changed = {**FILTERS, "q_organization_job_titles": ["dental hygienist"]}
    with Session(pg_engine) as s:
        await fill_index_for_icp(s, "org_4", changed, max_new=10, max_pages=1, api_key="k")
    assert search.call_args.kwargs["page"] == 1  # not page 2 of the old search
    row = db_session.execute(
        text("SELECT current_page, stats FROM discovery_progress WHERE run_id = 'icp:org_4:hiring'")
    ).one()
    assert row.current_page == 1 and row.stats["recent"] == [[1, 1]]


@pytest.mark.asyncio
async def test_task_releases_backend_claims_when_nothing_ran(monkeypatch):
    import signals.monitoring as mon

    released = []
    monkeypatch.setattr(mon, "_release_claims", AsyncMock(side_effect=lambda keys: released.extend(keys)))
    with patch("scrape.icp_fill.fill_index_for_icp", new=AsyncMock(return_value={"added": 0, "skipped": "daily_cap"})):
        await mon.fill_index_for_icp_task({}, "org_5", FILTERS, release_keys=["a", "b"])
    with patch("scrape.icp_fill.fill_index_for_icp", new=AsyncMock(return_value={"added": 0, "variants": {}})):
        await mon.fill_index_for_icp_task({}, "org_5", FILTERS, release_keys=["c"])  # ran, found nothing: keep
    assert released == ["a", "b"]
