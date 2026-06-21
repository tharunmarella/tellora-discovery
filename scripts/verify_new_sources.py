"""One-off live verification of the new-source enrichment path. No DB writes."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from signals.diff import diff_snapshots, snapshot_from_result
from signals.pipeline import enrich_company_signals


async def main():
    result = await enrich_company_signals(
        company_id="poc-verify",
        company_name="Vercel",
        domain="vercel.com",
        description=None,
        industry="Developer Tools",
        raw_meta=None,
        existing_headcount=600,
    )

    print("\n=== NEW FIELDS IN ENRICHMENT RESULT ===")
    print(f"pricing_model     : {result.get('pricing_model')}")
    print(f"recent_launches   : {json.dumps(result.get('recent_launches'), indent=2)}")
    print(f"page_fingerprints : {result.get('page_fingerprints')}")
    print(f"github_org        : {result.get('github_org')}")
    print(f"tech_stack (merged): {result.get('tech_stack')}")
    print(f"\nextra_events ({len(result.get('extra_events') or [])}):")
    for ev in result.get("extra_events") or []:
        print(f"  [{ev['source']:<13}] {ev['event_type']:<15} {ev['title'][:70]}")

    tsv = result.get("tsv_text") or ""
    print(f"\nsearch_tsv length: {len(tsv)} chars")

    # Simulate a previous snapshot to prove the new diff logic fires
    curr = snapshot_from_result(result)
    prev = dict(curr)
    prev["page_fingerprints"] = {"pricing": "deadbeefdeadbeef", "changelog": ""}
    prev["recent_launches"] = []
    prev["pricing_model"] = "enterprise"
    events = diff_snapshots(prev, curr)
    print(f"\n=== DIFF EVENTS vs simulated old snapshot ({len(events)}) ===")
    for ev in events:
        print(f"  {ev.event_type:<15} conf={ev.confidence:.2f}  {ev.title[:75]}")


asyncio.run(main())
