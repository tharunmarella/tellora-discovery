"""
Semantic search test — mirrors exactly what the backend /api/discovery/search does.

Steps:
  1. Generate 4-5 ICP company profiles from a product description (Gemini)
  2. Embed each query with RETRIEVAL_QUERY task type
  3. Average the embeddings into one search vector
  4. Cosine similarity search via pgvector
  5. Print top results

Usage:
  GOOGLE_API_KEY=xxx DATABASE_URL=postgresql://... python test_search.py
  or just: python test_search.py  (reads from .env)
"""

import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not GOOGLE_API_KEY:
    print("Set GOOGLE_API_KEY env var")
    sys.exit(1)
if not DATABASE_URL:
    print("Set DATABASE_URL env var")
    sys.exit(1)

# ── Test queries — change these to test different ICPs ────────────────────────

SEARCHES = [
    "AI-powered sales coaching platform for B2B SaaS companies",
    "Cybersecurity compliance tool for financial institutions",
    "Construction project management software for general contractors",
    "HR onboarding automation for mid-size companies",
]

# ─────────────────────────────────────────────────────────────────────────────

from google import genai
from google.genai import types
import psycopg2

client = genai.Client(api_key=GOOGLE_API_KEY)
GEMINI_MODEL = "gemini-2.5-flash-lite"


def generate_icp_queries(product_description: str) -> list[str]:
    prompt = f"""You are a B2B sales expert helping identify ideal customer profiles (ICPs).

Product description: {product_description}

Generate 4 short descriptions of the types of COMPANIES that would most benefit from buying this product.
Each should sound like a company profile, e.g.:
- "Home services companies with door-to-door sales teams managing field reps"
- "Construction contractors looking to reduce risk in large commercial projects"

Return a JSON array of strings only. No explanation."""

    resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
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


def avg_embeddings(embeddings: list[list[float]]) -> list[float]:
    dims = len(embeddings[0])
    return [sum(e[i] for e in embeddings) / len(embeddings) for i in range(dims)]


def search(vector: list[float], limit: int = 8, min_score: float = 0.45) -> list[dict]:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    vec_str = "[" + ",".join(str(v) for v in vector) + "]"
    cur.execute("""
        SELECT
            name, domain, industry, ceo_name, headquarters, funding,
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


def main():
    for product_desc in SEARCHES:
        print(f"\n{'='*70}")
        print(f"  SEARCH: {product_desc}")
        print(f"{'='*70}")

        print("  Generating ICP queries...")
        icp_queries = generate_icp_queries(product_desc)
        for q in icp_queries:
            print(f"    • {q}")

        print("  Embedding and searching...")
        embeddings = [embed(q) for q in icp_queries]
        vector = avg_embeddings(embeddings)
        results = search(vector, limit=8, min_score=0.45)

        if not results:
            print("  No results above threshold.")
            continue

        print(f"\n  Top {len(results)} matches:\n")
        for r in results:
            print(f"  [{r['score']}] {r['name']}")
            print(f"    domain    : {r['domain'] or '—'}")
            print(f"    industry  : {r['industry'] or '—'}")
            print(f"    hq        : {r['headquarters'] or '—'}")
            print(f"    ceo       : {r['ceo_name'] or '—'}")
            print(f"    funding   : {r['funding'] or '—'}")
            print(f"    desc      : {r['description'] or '—'}")
            print()


if __name__ == "__main__":
    main()
