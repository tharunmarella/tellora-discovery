"""
The 5 ICP filter profiles for Apollo People API Search.

All profiles use "CEO" as the sole person_titles filter.
Every company has exactly one CEO → ~1 result per company = natural dedup.
Differentiation comes from q_keywords, tech stack, employee ranges, and locations.
"""

from typing import Any

ICP_PROFILES: list[dict[str, Any]] = [
    {
        "slug": "devtools",
        "label": "AI Infra / DevTools",
        "description": "Companies buying developer tools, cloud infra, observability.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "q_keywords": "software SaaS technology",
            "currently_using_any_of_technology_uids": [
                "amazon_web_services",
                "google_cloud_platform",
                "kubernetes",
                "docker",
                "datadog",
            ],
            "organization_num_jobs_range_min": 3,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "operations",
        "label": "Operations / Workflow Automation",
        "description": "Companies buying workflow, process automation, scheduling tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "include_similar_titles": True,
        },
    },
    {
        "slug": "healthcare",
        "label": "Healthcare",
        "description": "Healthcare orgs buying clinical AI, billing, patient engagement.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500", "501,1000"],
            "q_keywords": "healthcare medical clinic hospital",
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
            "q_keywords": "finance banking insurance lending",
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
            "q_keywords": "B2B SaaS",
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
]

PROFILE_BY_SLUG: dict[str, dict[str, Any]] = {p["slug"]: p for p in ICP_PROFILES}
