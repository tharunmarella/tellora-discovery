"""
Batch enrichment test: Serper → Gemini → DiscoveryCompany-shaped JSON.

Flow:
  1. Search Serper: "{company_name} CEO: {ceo_name}"
  2. Dump the full Serper JSON to Gemini
  3. Ask Gemini to return a JSON matching the DiscoveryCompany model fields

Usage:
  cd tellora-discovery
  SERPER_API_KEY=xxx GOOGLE_API_KEY=xxx python scripts/test_batch.py
"""

import json
import sys
from pathlib import Path

import httpx
from google import genai

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import settings as cfg
from enrichment import EXTRACT_PROMPT, INDUSTRY_ENUM

if not cfg.SERPER_API_KEY or not cfg.GEMINI_API_KEY:
    print("Set SERPER_API_KEY and GOOGLE_API_KEY / GEMINI_API_KEY env vars")
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


def search_serper(company_name: str, ceo_first_name: str) -> dict:
    query = f"{company_name} CEO: {ceo_first_name}" if ceo_first_name else f"{company_name} company"
    resp = httpx.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": cfg.SERPER_API_KEY, "Content-Type": "application/json"},
        json={"q": query, "gl": "us", "hl": "en", "num": 10},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def ask_gemini(company_name: str, serper_data: dict) -> dict:
    client = genai.Client(api_key=cfg.GEMINI_API_KEY)
    trimmed = {
        "knowledgeGraph": serper_data.get("knowledgeGraph"),
        "organic": serper_data.get("organic", [])[:7],
    }
    prompt = EXTRACT_PROMPT.format(
        company_name=company_name,
        serper_json=json.dumps(trimmed, indent=2),
        industry_list=", ".join(INDUSTRY_ENUM),
    )
    resp = client.models.generate_content(model=cfg.ENRICHMENT_GEMINI_MODEL, contents=prompt)
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

        for key, val in result.items():
            if val is not None and val != [] and val != "":
                if isinstance(val, list):
                    print(f"  {key:15}: {', '.join(val)}")
                else:
                    display = str(val)[:90]
                    print(f"  {key:15}: {display}")


if __name__ == "__main__":
    main()
