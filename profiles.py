"""
The 5 ICP filter profiles for Apollo People API Search.

All profiles use "CEO" as the sole person_titles filter.
Every company has exactly one CEO → ~1 result per company = natural dedup.

Differentiation strategy (free-tier compatible):
  - organization_num_employees_ranges  → target the right company size
  - organization_num_jobs_range_min    → open jobs signal active/funded companies
  - q_organization_job_titles          → what the company is hiring = their sector
  - organization_locations             → geography when relevant

Removed: q_keywords (only matches ~2 results on free tier)
         currently_using_any_of_technology_uids (requires paid plan)
"""

from typing import Any

ICP_PROFILES: list[dict[str, Any]] = [
    {
        "slug": "devtools",
        "label": "AI Infra / DevTools",
        "description": "Tech companies building software — buy dev tools, cloud infra, observability.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["software engineer", "machine learning engineer"],
            "organization_num_jobs_range_min": 3,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "operations",
        "label": "Operations / Workflow Automation",
        "description": "Mid-size US companies buying workflow, process automation, scheduling tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "healthcare",
        "label": "Healthcare",
        "description": "Healthcare orgs buying clinical AI, billing, patient engagement tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["nurse", "clinical", "physician", "medical"],
            "include_similar_titles": True,
        },
    },
    {
        "slug": "finserv",
        "label": "Financial Services / Insurance",
        "description": "Financial institutions buying risk, compliance, fintech tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["financial analyst", "compliance officer", "underwriter"],
            "include_similar_titles": True,
        },
    },
    {
        "slug": "gtm",
        "label": "Sales & Marketing / GTM",
        "description": "B2B companies buying sales tools, marketing automation, CRMs.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["account executive", "sales development representative"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
]

PROFILE_BY_SLUG: dict[str, dict[str, Any]] = {p["slug"]: p for p in ICP_PROFILES}
