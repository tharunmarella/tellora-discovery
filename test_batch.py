"""
Batch enrichment test: Serper → Gemini → DiscoveryCompany-shaped JSON.

Flow:
  1. Search Serper: "{company_name} CEO: {ceo_name}"
  2. Dump the full Serper JSON to Gemini
  3. Ask Gemini to return a JSON matching the DiscoveryCompany model fields

Usage:
  cd tellora-discovery
  SERPER_API_KEY=xxx GOOGLE_API_KEY=xxx python test_batch.py
"""

import json
import os
import sys
import httpx
from google import genai

SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

if not SERPER_API_KEY or not GOOGLE_API_KEY:
    print("Set SERPER_API_KEY and GOOGLE_API_KEY env vars")
    sys.exit(1)

# Test companies: (company_name, ceo_first_name)
COMPANIES = [
    ("Pave", "Matt"),
    ("SentiLink", "Naftali"),
    ("Luminai", "Aditya"),
    ("Second Front", "Peter"),
    ("Beacon Biosignals", "Erik"),
    ("Later", "Scott"),
]

GEMINI_MODEL = "gemini-2.5-flash-lite"

PROMPT_TEMPLATE = """You are a company data extraction agent. Below is the full Google search result JSON for "{company_name}".

Extract as many fields as possible and return a single JSON object matching this schema:
{{
  "name": "official company name",
  "domain": "company website domain e.g. example.com (null if not found)",
  "website_url": "full URL to company homepage (null if not found)",
  "description": "one sentence (max 150 chars) describing what the company does",
  "industries": ["pick ALL that apply from: DevTools, Logistics, Healthcare, Financial Services, Sales & Marketing, Cybersecurity, Construction, HR Tech, Real Estate, EdTech, Legal, Manufacturing, Retail, GovTech, Hospitality, Media & Advertising, Nonprofit, Other"],
  "ceo_name": "full name of the CEO (null if not found)",
  "headquarters": "city and state/country e.g. San Francisco, CA (null if not found)",
  "founded_year": "4-digit year as string e.g. 2018 (null if not found)",
  "funding": "funding stage and/or amount e.g. Series B · $25M (null if not found)",
  "keywords": ["3-5 short tags describing the company space"],
  "use_case": "one sentence: what type of company would buy or benefit from this company's product (null if unclear)"
}}

IMPORTANT: "industries" must ONLY contain values from the list above.
A company can belong to multiple industries. Pick all that apply.

Search result JSON:
{serper_json}

Return only valid JSON. No explanation, no markdown fences."""


def search_serper(company_name: str, ceo_first_name: str) -> dict:
    query = f"{company_name} CEO: {ceo_first_name}" if ceo_first_name else f"{company_name} company"
    resp = httpx.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
        json={"q": query, "gl": "us", "hl": "en", "num": 10},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def ask_gemini(company_name: str, serper_data: dict) -> dict:
    client = genai.Client(api_key=GOOGLE_API_KEY)
    # Only pass knowledgeGraph + organic (drop credits, relatedSearches noise)
    trimmed = {
        "knowledgeGraph": serper_data.get("knowledgeGraph"),
        "organic": serper_data.get("organic", [])[:7],
    }
    prompt = PROMPT_TEMPLATE.format(
        company_name=company_name,
        serper_json=json.dumps(trimmed, indent=2),
    )
    resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    raw = resp.text.strip().strip("`").strip()
    if raw.startswith("json"):
        raw = raw[4:].strip()
    return json.loads(raw)


def main():
    for company_name, ceo_first_name in COMPANIES:
        print(f"\n{'='*60}")
        print(f"  {company_name}  (CEO hint: {ceo_first_name})")
        print(f"{'='*60}")

        try:
            serper_data = search_serper(company_name, ceo_first_name)
            kg = serper_data.get("knowledgeGraph")
            organic_count = len(serper_data.get("organic", []))
            print(f"  Serper: {organic_count} organic results, KG={'yes' if kg else 'no'}")
        except Exception as e:
            print(f"  Serper error: {e}")
            continue

        try:
            result = ask_gemini(company_name, serper_data)
        except Exception as e:
            print(f"  Gemini error: {e}")
            continue

        # Print all non-null fields
        for key, val in result.items():
            if val is not None and val != [] and val != "":
                if isinstance(val, list):
                    print(f"  {key:15}: {', '.join(val)}")
                else:
                    display = str(val)[:90]
                    print(f"  {key:15}: {display}")


if __name__ == "__main__":
    main()
