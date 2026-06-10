"""
The 17 ICP filter profiles for Apollo People API Search.

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
            # "platform engineer" / "site reliability" = infra-heavy companies = buy devtools
            "q_organization_job_titles": ["platform engineer", "site reliability engineer", "devops engineer"],
            "organization_num_jobs_range_min": 3,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "operations",
        "label": "Logistics & Supply Chain",
        "description": "Logistics, warehousing, and field service companies buying dispatch, routing, and workflow tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["fleet manager", "dispatch coordinator", "warehouse manager", "logistics coordinator"],
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
            "q_organization_job_titles": ["clinical informatics", "health IT", "Epic analyst", "medical billing manager"],
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
            "q_organization_job_titles": ["loan officer", "actuary", "AML analyst", "risk analyst"],
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
            # Hiring SDR + demand gen = dedicated outbound team = buys CRM/sales tools
            "q_organization_job_titles": ["sales development representative", "demand generation manager"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "cybersecurity",
        "label": "Cybersecurity",
        "description": "Security companies and orgs buying threat detection, compliance, and security tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["SOC analyst", "penetration tester", "threat intelligence analyst"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "construction",
        "label": "Construction / Field Services",
        "description": "Construction and field services companies buying project management, estimating, and BIM tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["project superintendent", "estimator", "BIM coordinator"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "hrtech",
        "label": "HR Tech",
        "description": "Companies with dedicated HR ops buying talent, HRIS, and compensation platforms.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["talent acquisition specialist", "HRIS analyst", "compensation analyst"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "proptech",
        "label": "Real Estate / PropTech",
        "description": "Real estate companies buying property management, leasing, and PropTech tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["property manager", "leasing consultant", "real estate broker"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "edtech",
        "label": "EdTech / Education",
        "description": "Schools and ed-tech companies buying LMS, curriculum, and learning platforms.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["instructional designer", "LMS administrator", "curriculum developer"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "legal",
        "label": "Legal",
        "description": "Law firms and legal departments buying contract management, e-discovery, and legal ops tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["paralegal", "legal operations manager", "contract manager"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "manufacturing",
        "label": "Manufacturing",
        "description": "Manufacturers buying MES, ERP, quality management, and shop-floor tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["manufacturing engineer", "quality engineer", "plant manager"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "retail",
        "label": "Retail / E-commerce",
        "description": "Retailers and e-commerce companies buying POS, inventory, and merchandising platforms.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["merchandiser", "e-commerce manager", "category manager"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "govtech",
        "label": "GovTech",
        "description": "Government agencies and contractors buying grants, procurement, and program management tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["51,200", "201,500", "501,1000"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["contracting officer", "grants manager", "program analyst"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "hospitality",
        "label": "Travel / Hospitality",
        "description": "Hotels, travel, and hospitality companies buying revenue management, booking, and ops tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["revenue manager", "hotel general manager", "concierge"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "media",
        "label": "Media / Advertising",
        "description": "Media and advertising companies buying ad tech, publishing, and programmatic platforms.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["media buyer", "ad operations manager", "programmatic trader"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
    {
        "slug": "nonprofit",
        "label": "Nonprofit",
        "description": "Nonprofits buying donor management, CRM, fundraising, and grant tracking tools.",
        "filters": {
            "person_titles": ["CEO"],
            "person_seniorities": ["founder", "c_suite"],
            "organization_num_employees_ranges": ["21,50", "51,200", "201,500"],
            "organization_locations": ["United States"],
            "q_organization_job_titles": ["development director", "grant writer", "fundraising manager"],
            "organization_num_jobs_range_min": 2,
            "include_similar_titles": True,
        },
    },
]
