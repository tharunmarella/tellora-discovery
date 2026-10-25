"""Unit tests for signals/sources/warn.py (WARN notice normalization + matching)."""

from datetime import datetime, timedelta, timezone

from signals.entity import CompanyIndex, TrackedCompany, clean_name
from signals.sources.warn import match_notices, normalize_rows


def test_normalize_california_columns():
    rows = [{"notice_date": "10/07/2026", "company": "Acme Analytics, Inc.", "num_employees": "79", "city": "San Jose"}]
    out = normalize_rows("ca", rows)
    assert out[0]["company"] == "Acme Analytics, Inc."
    assert out[0]["employees"] == 79
    assert out[0]["notice_date"].date().isoformat() == "2026-10-07"


def test_normalize_texas_columns():
    rows = [{"NOTICE_DATE": "2026-10-07 00:00:00", "JOB_SITE_NAME": "Acme Analytics", "TOTAL_LAYOFF_NUMBER": "1,204", "CITY_NAME": "Austin"}]
    out = normalize_rows("tx", rows)
    assert out[0]["employees"] == 1204 and out[0]["location"] == "Austin"


def test_normalize_new_york_columns():
    rows = [{"Business Legal Name": "Acme Analytics LLC", "Date of WARN Notice ": "2026-10-01", "Number of Affected Workers ": "40"}]
    out = normalize_rows("ny", rows)
    assert out[0]["company"] == "Acme Analytics LLC" and out[0]["employees"] == 40


def test_rows_without_company_or_date_are_dropped():
    assert normalize_rows("ca", [{"company": "", "notice_date": "10/07/2026"}, {"company": "X Co"}]) == []


def test_match_notices_builds_layoffs_event():
    index = CompanyIndex([TrackedCompany("acme", "Acme Analytics", "acme.com", clean_name("Acme Analytics"), "ca")])
    notice = {
        "company": "Acme Analytics, Inc.", "notice_date": datetime.now(timezone.utc) - timedelta(days=3),
        "employees": 79, "location": "San Jose", "state": "ca",
    }
    out = match_notices([notice, {**notice, "company": "Unrelated Bakery LLC"}], index)
    assert list(out) == ["acme"]
    ev = out["acme"][0]
    assert ev["event_type"] == "layoffs" and "79 employees" in ev["title"] and "San Jose, CA" in ev["title"]
    assert ev["evidence_url"]
