"""
Semantic search test — buyer-intent query matching.

Instead of generating ICP queries that describe NEEDS ("companies that need PM tools"),
generates queries that describe what buyer companies ARE ("a general contracting firm
specializing in commercial construction"). This matches company descriptions in the DB
instead of matching competitors' use_cases.

Also: searches per ICP query separately (no averaging), deduplicates by company ID.

Usage:
  cd tellora-discovery
  GOOGLE_API_KEY=xxx DATABASE_URL=postgresql://... python scripts/test_search.py
  or just: python scripts/test_search.py  (reads from .env)
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import settings as cfg

if not cfg.GEMINI_API_KEY:
    print("Set GOOGLE_API_KEY / GEMINI_API_KEY env var")
    sys.exit(1)
if not cfg.DATABASE_URL:
    print("Set DATABASE_URL env var")
    sys.exit(1)

SEARCHES = [
    "AI-powered sales coaching platform for B2B SaaS companies",
    "Cybersecurity compliance tool for financial institutions",
    "Construction project management software for general contractors",
    "HR onboarding automation for mid-size companies",
]

from google import genai
from google.genai import types
import psycopg2

client = genai.Client(api_key=cfg.GEMINI_API_KEY)


def generate_buyer_profiles(product_description: str) -> list[str]:
    """
    Generate descriptions of what ideal BUYER companies look like —
    written as if they were the company's own About page.
    """
    prompt = f"""You are a B2B sales expert. A company sells this product:

"{product_description}"

Describe 4 types of companies that would BUY this product. For each, write a short
paragraph (2-3 sentences) as if you are writing that company's own About page or
LinkedIn summary.

Focus on what the company DOES as its core business — its operations, its industry,
its employees, its customers. Do NOT mention what software or tools they need.

Example for "field service scheduling software":
- "A regional HVAC and plumbing company serving residential customers across three states. The company employs 80 field technicians who handle 200+ daily service calls, coordinated by a small dispatch team."

Return a JSON array of strings. No explanation."""

    resp = client.models.generate_content(model=cfg.ENRICHMENT_GEMINI_MODEL, contents=prompt)
    raw = resp.text.strip().strip("`").strip()
    if raw.startswith("json"):
        raw = raw[4:].strip()
    queries = json.loads(raw)
    return [q for q in queries if isinstance(q, str)][:5]


def embed(text: str) -> list[float]:
    resp = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text,
        config=types.EmbedContentConfig(
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=768,
        ),
    )
    return resp.embeddings[0].values


def search(vector: list[float], limit: int = 10, min_score: float = 0.40) -> list[dict]:
    conn = psycopg2.connect(cfg.DATABASE_URL)
    cur = conn.cursor()
    vec_str = "[" + ",".join(str(v) for v in vector) + "]"
    cur.execute("""
        SELECT
            id, name, domain, industry, ceo_name, headquarters, funding,
            raw_meta->>'use_case' AS use_case,
            LEFT(description, 120) AS description,
            ROUND((1 - (description_embedding <=> %s::vector))::numeric, 4) AS score
        FROM discovery_company
        WHERE description_embedding IS NOT NULL
          AND 1 - (description_embedding <=> %s::vector) >= %s
        ORDER BY description_embedding <=> %s::vector
        LIMIT %s
    """, (vec_str, vec_str, min_score, vec_str, limit))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def search_multi_query(queries: list[str], per_query: int = 10, min_score: float = 0.40) -> list[dict]:
    """
    Search each query separately, deduplicate by company ID, keep highest score.
    """
    seen: dict[str, dict] = {}
    for q in queries:
        vec = embed(q)
        results = search(vec, limit=per_query, min_score=min_score)
        for r in results:
            cid = r["id"]
            if cid not in seen or float(r["score"]) > float(seen[cid]["score"]):
                seen[cid] = r
    return sorted(seen.values(), key=lambda r: float(r["score"]), reverse=True)


def main():
    for product_desc in SEARCHES:
        print(f"\n{'='*70}")
        print(f"  SEARCH: {product_desc}")
        print(f"{'='*70}")

        print("  Generating buyer profiles...")
        buyer_profiles = generate_buyer_profiles(product_desc)
        for i, bp in enumerate(buyer_profiles, 1):
            print(f"    {i}. {bp[:120]}...")

        print("  Searching per query (no averaging)...")
        results = search_multi_query(buyer_profiles, per_query=10, min_score=0.40)

        if not results:
            print("  No results above threshold.")
            continue

        top = results[:10]
        print(f"\n  Top {len(top)} matches:\n")
        for r in top:
            print(f"  [{r['score']}] {r['name']}")
            print(f"    domain    : {r['domain'] or '—'}")
            print(f"    industry  : {r['industry'] or '—'}")
            print(f"    hq        : {r['headquarters'] or '—'}")
            print(f"    desc      : {r['description'] or '—'}")
            print()


if __name__ == "__main__":
    main()
