"""
New Signal Sources Proof-of-Concept
===================================

Proves the output of the five new ingesters BEFORE building them into the
pipeline. No DB writes. Live APIs only:

  1. Extra website pages   (Jina: /pricing, /customers, /changelog)
  2. GitHub org activity   (free REST API, optional GITHUB_TOKEN)
  3. SEC EDGAR Form D      (free full-text search API)
  4. Google News RSS       (free, no key)
  5. DNS TXT/MX records    (dig, zero cost)

Run:  python scripts/source_poc.py            # all sources
      python scripts/source_poc.py 3 4        # only EDGAR + news
"""

import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

import settings as cfg
from llm import get_gemini_client, retry_llm, strip_json_fences

DEMO_COMPANY = "Vercel"
DEMO_DOMAIN = "vercel.com"
NOW = datetime.now(timezone.utc)


def header(n, title):
    print(f"\n{'=' * 78}\nSOURCE {n} — {title}\n{'=' * 78}")


def gemini_json(prompt):
    def _do():
        client = get_gemini_client()
        resp = client.models.generate_content(
            model=cfg.SIGNAL_GEMINI_MODEL, contents=prompt)
        return json.loads(strip_json_fences(resp.text))
    return retry_llm(_do)


# ════════════════════════════════════════════════════════════════════════════
# 1. Extra website pages → pricing_change / product_launch
# ════════════════════════════════════════════════════════════════════════════

async def source1():
    header(1, "Extra website pages (pricing / customers / changelog)")
    from signal_enrichment import _jina_read

    pages = {}
    for path in ["/pricing", "/customers", "/changelog"]:
        text = await _jina_read(f"https://{DEMO_DOMAIN}{path}", max_chars=2000)
        pages[path] = text
        fp = hashlib.sha1(re.sub(r"\s+", " ", text).encode()).hexdigest()[:12]
        print(f"  {path:<12} {len(text):>5} chars   fingerprint={fp}")

    if pages.get("/changelog"):
        extracted = gemini_json(f"""From this changelog page text, list product
launches/releases with approximate dates. Only items visible in the text.
TEXT: {pages['/changelog'][:1800]}
Return JSON: {{"launches": [{{"title": "...", "date": "YYYY-MM or null"}}]}}""")
        print("\n  OUTPUT: recent_launches extracted live:")
        for l in extracted.get("launches", [])[:5]:
            print(f"    • {l.get('title')}  ({l.get('date')})")
    print("\n  => new launch vs snapshot → product_launch event")
    print("  => pricing fingerprint change vs snapshot → pricing_change event")


# ════════════════════════════════════════════════════════════════════════════
# 2. GitHub org → tech_first_seen / product_launch
# ════════════════════════════════════════════════════════════════════════════

async def source2():
    header(2, "GitHub org activity")
    headers = {"Accept": "application/vnd.github+json"}
    if os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"

    stem = DEMO_DOMAIN.split(".")[0]
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        org = None
        # Strategy 1: domain stem as org name, verify via blog/website field
        r = await client.get(f"https://api.github.com/orgs/{stem}")
        if r.status_code == 200:
            detail = r.json()
            if DEMO_DOMAIN in (detail.get("blog") or ""):
                org = detail
        # Strategy 2: user search fallback
        if not org:
            r = await client.get("https://api.github.com/search/users",
                                 params={"q": f"{DEMO_DOMAIN} type:org", "per_page": 3})
            for cand in (r.json().get("items", []) if r.status_code == 200 else []):
                detail = (await client.get(cand["url"])).json()
                if DEMO_DOMAIN in (detail.get("blog") or ""):
                    org = detail
                    break
        if not org:
            print(f"  no verified org match for {DEMO_DOMAIN} — would cache negative result")
            return
        print(f"  resolved {DEMO_DOMAIN} → github.com/{org['login']} "
              f"(verified via blog field: {org.get('blog')})")

        repos = (await client.get(
            f"https://api.github.com/orgs/{org['login']}/repos",
            params={"sort": "pushed", "per_page": 30})).json()

    langs = {}
    new_repos = []
    cutoff = NOW - timedelta(days=30)
    for repo in repos:
        if not isinstance(repo, dict):
            continue
        if repo.get("language"):
            langs[repo["language"]] = langs.get(repo["language"], 0) + 1
        created = datetime.fromisoformat(repo["created_at"].replace("Z", "+00:00"))
        if created > cutoff:
            new_repos.append((repo["name"], repo["stargazers_count"]))

    print(f"  languages (top): {sorted(langs, key=langs.get, reverse=True)[:6]}")
    print(f"  repos created in last 30d: {new_repos or 'none'}")
    print("\n  => languages merged into tech_stack (better than homepage regex)")
    print("  => new repo vs snapshot → product_launch event")


# ════════════════════════════════════════════════════════════════════════════
# 3. SEC EDGAR Form D → funding_round (weeks before press)
# ════════════════════════════════════════════════════════════════════════════

async def source3():
    header(3, "SEC EDGAR Form D filings")
    headers = {"User-Agent": "Tellora Research research@tellora.ai"}
    start = (NOW - timedelta(days=3)).date().isoformat()
    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        r = await client.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "q": "",
                "forms": "D",
                "startdt": start,
                "enddt": NOW.date().isoformat(),
            },
        )
        data = {}
        if r.status_code == 200:
            data = r.json()
        else:
            print(f"  primary endpoint returned {r.status_code}, trying full-text search UI API…")
            r2 = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={"q": "\"Form D\"", "forms": "D"},
            )
            if r2.status_code == 200:
                data = r2.json()
            else:
                print(f"  fallback also {r2.status_code} — trying browse-edgar RSS…")
                r3 = await client.get(
                    "https://www.sec.gov/cgi-bin/browse-edgar",
                    params={"action": "getcompany", "type": "D", "dateb": "",
                            "owner": "include", "count": "10", "output": "atom"},
                )
                if r3.status_code == 200:
                    root = ET.fromstring(r3.text)
                    ns = {"a": "http://www.w3.org/2005/Atom"}
                    entries = root.findall("a:entry", ns)
                    print(f"  browse-edgar atom: {len(entries)} recent Form D entries")
                    for e in entries[:6]:
                        title = e.findtext("a:title", "", ns)
                        updated = e.findtext("a:updated", "", ns)
                        print(f"    • {title[:64]:<64} {updated[:10]}")
                    print("\n  => normalized-name match vs discovery_company → funding_round event")
                    return

    hits = (data.get("hits", {}) or {}).get("hits", [])[:8]
    print(f"  Form D filings since {start}: {len(hits)} sampled")
    for h in hits[:6]:
        src = h.get("_source", {})
        names = src.get("display_names") or [str(src.get("display_name", "?"))]
        print(f"    • {str(names[0])[:60]:<60} filed={src.get('file_date', '?')}")
    print("\n  => normalized-name match vs discovery_company → funding_round event")
    print("     payload: {form: D, accession_no, filed_at} — beats press by weeks")


# ════════════════════════════════════════════════════════════════════════════
# 4. Google News RSS → news_mention / funding_round / exec_hire
# ════════════════════════════════════════════════════════════════════════════

async def source4():
    header(4, "Google News RSS (free, no key)")
    url = ("https://news.google.com/rss/search?"
           f'q="{DEMO_COMPANY}"+when:7d+-stock+-shares&hl=en-US&gl=US&ceid=US:en')
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        r = await client.get(url)
    root = ET.fromstring(r.text)
    items = []
    for item in root.iter("item"):
        src_el = item.find("source")
        items.append({
            "title": item.findtext("title") or "",
            "url": item.findtext("link") or "",
            "date": item.findtext("pubDate") or "",
            "source": src_el.text if src_el is not None else "",
        })
    print(f"  {len(items)} headlines for \"{DEMO_COMPANY}\" in last 7d:")
    for it in items[:6]:
        print(f"    • [{(it['source'] or '?')[:18]:<18}] {it['title'][:80]}")

    if items:
        sample = [{"i": i, "title": it["title"]} for i, it in enumerate(items[:8])]
        verdicts = gemini_json(f"""Classify each headline about {DEMO_COMPANY}:
category = funding | exec_hire | partnership | product | expansion | layoffs | irrelevant
{json.dumps(sample)}
Return JSON: {{"results": [{{"i": 0, "category": "...", "relevant": true}}]}}""")
        print("\n  OUTPUT: Gemini classification → events:")
        for v in verdicts.get("results", []):
            it = items[v["i"]]
            mark = v.get("category", "?")
            keep = "KEEP" if v.get("relevant") and mark != "irrelevant" else "drop"
            print(f"    [{keep}] {mark:<12} {it['title'][:65]}")
        print("\n  => relevant headlines → news_mention (or funding_round/exec_hire)")
        print("     deduped by encoded URL, weekly per watched account, $0 cost")


# ════════════════════════════════════════════════════════════════════════════
# 5. DNS TXT/MX → tech_first_seen
# ════════════════════════════════════════════════════════════════════════════

_DNS_PATTERNS = {
    "google-site-verification": "google_workspace",
    "MS=": "microsoft365",
    "atlassian-domain-verification": "atlassian",
    "stripe-verification": "stripe",
    "hubspot": "hubspot",
    "zoom": "zoom",
    "docusign": "docusign",
    "slack-domain-verification": "slack",
    "notion": "notion",
    "miro-verification": "miro",
    "openai-domain-verification": "openai",
    "linear-domain-verification": "linear",
    "facebook-domain-verification": "meta_ads",
    "apple-domain-verification": "apple_business",
    "canva-site-verification": "canva",
    "krisp-domain-verification": "krisp",
    "loom-verification": "loom",
}


def _dig(domain, rtype):
    try:
        out = subprocess.run(["dig", "+short", rtype, domain],
                             capture_output=True, text=True, timeout=10)
        return [l.strip().strip('"') for l in out.stdout.splitlines() if l.strip()]
    except Exception:
        return []


async def source5():
    header(5, "DNS TXT/MX records")
    txt = _dig(DEMO_DOMAIN, "TXT")
    mx = _dig(DEMO_DOMAIN, "MX")
    print(f"  TXT records: {len(txt)}, MX records: {len(mx)}")

    detected = set()
    for rec in txt:
        for needle, vendor in _DNS_PATTERNS.items():
            if needle.lower() in rec.lower():
                detected.add(vendor)
    for rec in mx:
        low = rec.lower()
        if "google" in low:
            detected.add("google_workspace_email")
        elif "outlook" in low or "microsoft" in low:
            detected.add("microsoft365_email")

    print("\n  OUTPUT: verified vendor usage from DNS:")
    for v in sorted(detected):
        print(f"    • {v}")
    for rec in txt[:4]:
        print(f"    (raw: {rec[:70]})")
    print("\n  => merged into tech_stack; new vendor vs snapshot → tech_first_seen")
    print("     zero cost, can't be faked by marketing pages")


# ════════════════════════════════════════════════════════════════════════════

SOURCES = {1: source1, 2: source2, 3: source3, 4: source4, 5: source5}


async def main():
    wanted = [int(a) for a in sys.argv[1:] if a.isdigit()] or sorted(SOURCES)
    print(f"New Signal Sources PoC — live run {NOW.isoformat()}  sources={wanted}")
    for n in wanted:
        try:
            await SOURCES[n]()
        except Exception as exc:
            print(f"\n  SOURCE {n} FAILED: {exc!r}")
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
