"""
Small-batch test script — fetches 2 pages from one ICP profile and prints results.

Usage (inside docker compose):
  docker compose run --rm discovery python test_batch.py
  docker compose run --rm discovery python test_batch.py --profile gtm
  docker compose run --rm discovery python test_batch.py --profile devtools --write

Flags:
  --profile <slug>  Which ICP profile to test (default: devtools)
                    Options: devtools, operations, healthcare, finserv, gtm
  --write           Write discovered companies to the DB (default: dry-run, print only)
  --pages <n>       Number of pages to fetch (default: 2, max ~10 for a quick test)
"""

import asyncio
import sys
import time

# ── Bootstrap ─────────────────────────────────────────────────────────────────
from config_logging import setup_logging
setup_logging()

import settings as cfg
from profiles import PROFILE_BY_SLUG
from apollo_client import ApolloRateLimiter, search_page
from jina_client import lookup_domain

# ── CLI args ──────────────────────────────────────────────────────────────────
args = sys.argv[1:]
profile_slug = "devtools"
write_to_db  = False
num_pages    = 2

for i, arg in enumerate(args):
    if arg == "--profile" and i + 1 < len(args):
        profile_slug = args[i + 1]
    if arg == "--write":
        write_to_db = True
    if arg == "--pages" and i + 1 < len(args):
        num_pages = max(1, min(int(args[i + 1]), 20))

if profile_slug not in PROFILE_BY_SLUG:
    print(f"Unknown profile '{profile_slug}'. Available: {', '.join(PROFILE_BY_SLUG)}")
    sys.exit(1)

profile = PROFILE_BY_SLUG[profile_slug]

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{'='*60}")
    print(f"  Tellora Discovery — Batch Test")
    print(f"  Profile : {profile['label']} ({profile_slug})")
    print(f"  Pages   : {num_pages}  |  Per page: 100")
    print(f"  DB write: {'YES' if write_to_db else 'NO (dry-run)'}")
    print(f"{'='*60}\n")

    limiter = ApolloRateLimiter(min_interval=1.1)
    all_companies: list[dict] = []  # {name, domain, website_url, description, page}

    # ── Step 1: Fetch Apollo pages ─────────────────────────────────────────────
    print("[ 1 / 3 ]  Fetching Apollo pages …")
    for page in range(1, num_pages + 1):
        await limiter.wait()
        t0 = time.monotonic()
        data = await search_page(cfg.TELLORA_APOLLO_API_KEY, profile["filters"], page=page)
        elapsed = time.monotonic() - t0

        people = data.get("people", [])
        total  = data.get("total_entries", "?")

        seen_names: set[str] = {c["name"].lower() for c in all_companies}
        new_count = 0
        for p in people:
            org_name = (p.get("organization") or {}).get("name", "").strip()
            if org_name and org_name.lower() not in seen_names:
                seen_names.add(org_name.lower())
                all_companies.append({
                    "name": org_name,
                    "ceo_first_name": p.get("first_name") or "",
                    "page": page,
                    "domain": None, "website_url": None, "description": None,
                })
                new_count += 1

        print(f"  page {page:>3} → {new_count:>3} org names  "
              f"(total_entries={total}, {elapsed:.1f}s)")

        if len(people) < 100:
            print(f"  Last page reached at {page}")
            break

    if not all_companies:
        print("\nNo results from Apollo — check your API key and filters.")
        sys.exit(1)

    print(f"\n  Total unique org names: {len(all_companies)}\n")

    # ── Step 2: Jina domain lookup (5 concurrent) ─────────────────────────────
    print("[ 2 / 3 ]  Resolving domains via Jina (5 concurrent) …")
    sem = asyncio.Semaphore(5)

    async def _resolve(company: dict) -> None:
        async with sem:
            result = await lookup_domain(company["name"], company["ceo_first_name"])
            company.update({
                "domain":      result.get("domain"),
                "website_url": result.get("website_url"),
                "description": (result.get("description") or "")[:80],
            })

    t_jina = time.monotonic()
    await asyncio.gather(*[_resolve(c) for c in all_companies])
    jina_elapsed = time.monotonic() - t_jina

    for i, company in enumerate(all_companies):
        resolved = "✓" if company.get("domain") else "✗"
        print(f"  [{i+1:>3}/{len(all_companies)}] {resolved}  {company['name']:<40}  "
              f"{company.get('domain') or '—'}")

    resolved_count = sum(1 for c in all_companies if c.get("domain"))
    rate = len(all_companies) / jina_elapsed if jina_elapsed > 0 else 0
    print(f"\n  Domains resolved: {resolved_count} / {len(all_companies)}  "
          f"({jina_elapsed:.0f}s total, {rate:.1f} companies/sec)\n")

    # ── Step 3: Summary table ──────────────────────────────────────────────────
    print("[ 3 / 3 ]  Results\n")
    print(f"  {'#':<4}  {'Company':<38}  {'Domain':<30}  {'Description'}")
    print(f"  {'-'*4}  {'-'*38}  {'-'*30}  {'-'*40}")
    for i, c in enumerate(all_companies, 1):
        desc  = (c["description"] or "—")[:50]
        domain = (c["domain"] or "—")[:30]
        print(f"  {i:<4}  {c['name']:<38}  {domain:<30}  {desc}")

    # ── Optional DB write ──────────────────────────────────────────────────────
    if write_to_db:
        print(f"\n  Writing {len(all_companies)} companies to DB …")
        from database import create_tables, get_session
        from models import DiscoveryCompany
        from sqlmodel import select, func

        create_tables()

        added = 0
        skipped = 0
        with get_session() as db:
            for c in all_companies:
                norm = c["name"].strip().lower()
                exists = db.exec(
                    select(DiscoveryCompany).where(
                        func.lower(DiscoveryCompany.apollo_org_name) == norm
                    )
                ).first()
                if exists:
                    skipped += 1
                    continue
                db.add(DiscoveryCompany(
                    apollo_org_name = c["name"],
                    name            = c["name"],
                    domain          = c["domain"],
                    website_url     = c["website_url"],
                    description     = c["description"] or None,
                    source_profiles = [profile_slug],
                    domain_resolved = bool(c["domain"]),
                    enrichment_status = "pending",
                ))
                added += 1
            db.commit()

        print(f"  Added: {added}  |  Skipped (already in DB): {skipped}")

    print(f"\n{'='*60}")
    print(f"  Done.")
    print(f"{'='*60}\n")


asyncio.run(main())
