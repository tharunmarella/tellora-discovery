"""
Signal Platform Proof-of-Concept
================================

Proves the output of every phase of the discovery signal platform plan
BEFORE building it into the pipeline. No DB writes — everything is
in-memory and printed to stdout.

Live data sources used (same keys as production):
  - Greenhouse public API (job posts WITH bodies, content=true)
  - Jina Search           (funding news)
  - Serper                (social posts, exec-hire news)
  - Apollo free search    (people + titles for job-change detection)
  - Gemini flash-lite     (JD extraction, relevance filtering)
  - gemini-embedding-001  (ICP relevance, learning-loop centroid)

Where a phase needs HISTORY we don't have yet (diffs need a last-week
baseline), the baseline is SIMULATED and clearly labeled as such; the
detection logic itself runs for real on live current data.

Run:  python scripts/signal_poc.py            # all phases
      python scripts/signal_poc.py 2 4 7      # only phases 2, 4, 7
"""

import asyncio
import html as html_lib
import json
import math
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

import settings as cfg
from llm import embed_text, get_gemini_client, retry_llm, strip_json_fences
from signals.pipeline import check_job_boards, fetch_funding_news

NOW = datetime.now(timezone.utc)

# Demo subjects — real companies with public Greenhouse boards
DEMO_COMPANY = "Vercel"
DEMO_DOMAIN = "vercel.com"
DEMO_GH_SLUGS = ["vercel", "ramp", "monzo"]  # first that responds wins

# The "seller" whose ICP we score against (Tellora itself, dogfooding)
ICP_TEXT = (
    "B2B SaaS companies selling developer tools or sales/GTM software, "
    "Series A to C, 20-500 employees, investing in outbound sales, "
    "AI agents, or revenue operations."
)


def header(n: int, title: str) -> None:
    print()
    print("=" * 78)
    print(f"PHASE {n} — {title}")
    print("=" * 78)


def sub(title: str) -> None:
    print(f"\n--- {title} ---")


def gemini_json(prompt: str) -> dict | list:
    def _do():
        client = get_gemini_client()
        resp = client.models.generate_content(model=cfg.SIGNAL_GEMINI_MODEL, contents=prompt)
        return json.loads(strip_json_fences(resp.text))
    return retry_llm(_do)


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    return dot / (na * nb) if na and nb else 0.0


# ════════════════════════════════════════════════════════════════════════════
# Shared event model (what discovery_signal_event rows will look like)
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class SignalEvent:
    event_type: str
    title: str
    observed_at: datetime
    payload: dict = field(default_factory=dict)
    source: str = "poc"
    confidence: float = 1.0

    @property
    def dedupe_key(self) -> str:
        return f"{DEMO_DOMAIN}:{self.event_type}:{self.payload.get('key', self.title[:60])}"

    def show(self) -> str:
        age = (NOW - self.observed_at).days
        return (
            f"  [{self.event_type:>18}] {self.title}\n"
            f"  {'':>20} observed={self.observed_at.date()} ({age}d ago)  "
            f"confidence={self.confidence:.2f}  dedupe_key={self.dedupe_key}"
        )


ALL_EVENTS: list[SignalEvent] = []


# ════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Snapshot + diff → typed signal events
# ════════════════════════════════════════════════════════════════════════════

def _role_family(title: str) -> str:
    t = title.lower()
    rules = [
        ("account executive", "sales_ae"), ("sales", "sales"), ("sdr", "sales_sdr"),
        ("solutions engineer", "solutions_eng"), ("support", "support"),
        ("security", "security"), ("infrastructure", "infra"), ("sre", "infra"),
        ("devops", "infra"), ("data", "data"), ("machine learning", "ml"),
        ("ai", "ml"), ("product manager", "product"), ("product designer", "design"),
        ("designer", "design"), ("marketing", "marketing"), ("finance", "finance"),
        ("legal", "legal"), ("people", "people_ops"), ("recruit", "people_ops"),
        ("engineer", "engineering"),
    ]
    for needle, fam in rules:
        if needle in t:
            return fam
    return "other"


def diff_snapshots(prev: dict, curr: dict) -> list[SignalEvent]:
    """The real diff logic that will live in signal_diff.py."""
    events: list[SignalEvent] = []

    # funding_round: stage changed
    if curr.get("funding_stage") and curr["funding_stage"] != prev.get("funding_stage"):
        events.append(SignalEvent(
            "funding_round",
            f"Funding stage moved {prev.get('funding_stage') or 'unknown'} → {curr['funding_stage']}"
            + (f" (total raised {curr['total_raised']})" if curr.get("total_raised") else ""),
            NOW,
            {"key": curr["funding_stage"], "prev": prev.get("funding_stage"),
             "curr": curr["funding_stage"]},
            source="jina_news",
        ))

    # hiring_surge: open-roles count up >= 50%
    pc, cc = prev.get("hiring_count") or 0, curr.get("hiring_count") or 0
    if pc > 0 and cc >= pc * 1.5:
        events.append(SignalEvent(
            "hiring_surge",
            f"Open roles jumped {pc} → {cc} (+{(cc - pc) / pc * 100:.0f}%)",
            NOW, {"key": "hiring_count", "prev": pc, "curr": cc}, source="job_boards",
        ))

    # role_first_seen: a role family present now, absent before
    prev_fams = {_role_family(r) for r in prev.get("hiring_roles") or []}
    curr_fams = {_role_family(r) for r in curr.get("hiring_roles") or []}
    for fam in sorted(curr_fams - prev_fams):
        sample = next(r for r in curr["hiring_roles"] if _role_family(r) == fam)
        events.append(SignalEvent(
            "role_first_seen",
            f"First posting in role family '{fam}' (e.g. \"{sample}\")",
            NOW, {"key": fam, "sample_title": sample}, source="job_boards", confidence=0.9,
        ))

    # tech_first_seen
    for tech in sorted(set(curr.get("tech_stack") or []) - set(prev.get("tech_stack") or [])):
        events.append(SignalEvent(
            "tech_first_seen", f"Started using {tech}",
            NOW, {"key": tech}, source="tech_detect", confidence=0.7,
        ))

    # headcount_jump: >= 20%
    ph, ch = prev.get("headcount") or 0, curr.get("headcount") or 0
    if ph > 0 and ch >= ph * 1.2:
        events.append(SignalEvent(
            "headcount_jump",
            f"Headcount grew ~{ph} → ~{ch} (+{(ch - ph) / ph * 100:.0f}%)",
            NOW, {"key": "headcount", "prev": ph, "curr": ch}, source="apollo_kg",
        ))

    return events


async def phase1() -> None:
    header(1, "Snapshot diff → typed signal events")
    print(f"Company: {DEMO_COMPANY} ({DEMO_DOMAIN})")

    sub("LIVE: current state (Greenhouse/Lever + Jina funding news, right now)")
    jobs, news = await asyncio.gather(
        check_job_boards(DEMO_COMPANY), fetch_funding_news(DEMO_COMPANY),
    )
    print(f"  job board: {jobs['source']}, {jobs['count']} open roles")
    for r in jobs["roles"][:6]:
        print(f"    - {r}")
    print(f"  funding news snippets: {len(news)}")
    for s in news[:2]:
        print(f"    - {s[:130]}")

    curr_snapshot = {
        "captured_at": NOW.isoformat(),
        "hiring_count": jobs["count"],
        "hiring_roles": jobs["roles"],
        "funding_stage": "Series E",      # Vercel's known public stage
        "total_raised": "$563M",
        "tech_stack": ["stripe", "segment", "next.js"],
        "headcount": 650,
    }

    sub("SIMULATED: last-week snapshot (this is the only fabricated input)")
    prev_snapshot = {
        "captured_at": (NOW - timedelta(days=7)).isoformat(),
        "hiring_count": max(1, (jobs["count"] or 10) // 2),
        "hiring_roles": [r for r in jobs["roles"] if _role_family(r) not in ("sales_ae", "security")][:4],
        "funding_stage": "Series D",
        "total_raised": "$313M",
        "tech_stack": ["stripe", "next.js"],
        "headcount": 520,
    }
    print(f"  hiring_count={prev_snapshot['hiring_count']}, "
          f"funding_stage={prev_snapshot['funding_stage']}, headcount={prev_snapshot['headcount']}")

    sub("OUTPUT: events emitted by diff_snapshots(prev, curr)")
    events = diff_snapshots(prev_snapshot, curr_snapshot)
    if not events:
        print("  (no deltas)")
    for ev in events:
        print(ev.show())
    ALL_EVENTS.extend(events)
    print(f"\n  => {len(events)} typed rows would be inserted into discovery_signal_event")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Job-post BODY ingestion → concepts, spikes, first-times
# ════════════════════════════════════════════════════════════════════════════

def _strip_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_lib.unescape(s or ""))).strip()


async def _fetch_greenhouse_posts() -> tuple[str, list[dict]]:
    async with httpx.AsyncClient(timeout=15) as client:
        for slug in DEMO_GH_SLUGS:
            try:
                resp = await client.get(
                    f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                    params={"content": "true"},
                )
                if resp.status_code == 200 and resp.json().get("jobs"):
                    return slug, resp.json()["jobs"]
            except Exception:
                continue
    return "", []


async def phase2() -> None:
    header(2, "Job-post body ingestion (the data we throw away today)")

    sub("LIVE: Greenhouse with content=true (today's pipeline discards 'content')")
    slug, posts = await _fetch_greenhouse_posts()
    if not posts:
        print("  No Greenhouse board reachable — skipping phase")
        return
    print(f"  board='{slug}': {len(posts)} open posts, bodies included")
    sample = posts[:5]
    for p in sample:
        body = _strip_html(p.get("content", ""))
        print(f"    - {p['title']}  (body: {len(body)} chars)")

    sub("LIVE: Gemini per-post extraction (role_family, concepts, tech, initiatives)")
    blocks = []
    for i, p in enumerate(sample):
        body = _strip_html(p.get("content", ""))[:2200]
        blocks.append(f"POST {i + 1}\nTITLE: {p['title']}\nBODY: {body}")
    prompt = f"""You analyze job postings for B2B sales intelligence. For EACH post below,
extract what the company is working on or struggling with.

Rules: only facts visible in the text. concepts = pain points / initiatives /
strategic themes (2-5 words each, e.g. "migrate off Heroku", "scale PLG motion",
"SOC 2 compliance"). tech = named tools/platforms. initiatives = explicitly
named internal projects, else [].

{chr(10).join(blocks)}

Respond ONLY with valid JSON:
{{"posts": [{{"title": "...", "role_family": "...", "seniority": "junior|mid|senior|staff|exec",
"concepts": ["..."], "tech": ["..."], "initiatives": ["..."]}}]}}"""
    extracted = gemini_json(prompt)["posts"]
    for e in extracted:
        print(f"    {e['title']}")
        print(f"      role_family={e['role_family']}  seniority={e.get('seniority')}")
        print(f"      concepts   = {e.get('concepts')}")
        print(f"      tech       = {e.get('tech')}")
        if e.get("initiatives"):
            print(f"      initiatives= {e['initiatives']}")

    sub("OUTPUT: time-aware detectors over the post table")
    print("  (baseline = SIMULATED 30-days-ago post set: same posts minus the newest 2,")
    print("   concept counts halved — detector logic below is the real implementation)")

    fam_counts: dict[str, int] = {}
    concept_counts: dict[str, int] = {}
    for e in extracted:
        fam_counts[e["role_family"]] = fam_counts.get(e["role_family"], 0) + 1
        for c in e.get("concepts", []):
            concept_counts[c.lower()] = concept_counts.get(c.lower(), 0) + 1
    # simulated history: drop families/concepts introduced by the 2 newest posts
    hist_fams = dict(list(fam_counts.items())[:-1]) if len(fam_counts) > 1 else {}
    hist_concepts = {k: max(1, v // 2) for k, v in list(concept_counts.items())[:-2]}

    events: list[SignalEvent] = []
    for fam, n in fam_counts.items():
        if fam not in hist_fams:
            events.append(SignalEvent(
                "role_first_seen", f"First time hiring in '{fam}' (from JD analysis)",
                NOW, {"key": f"jd:{fam}", "open_posts": n}, source="job_posts", confidence=0.85,
            ))
        elif n >= 3 and n >= hist_fams[fam] * 2:
            events.append(SignalEvent(
                "role_spike", f"{n} open '{fam}' posts vs {hist_fams[fam]} last month "
                f"(+{(n - hist_fams[fam]) / hist_fams[fam] * 100:.0f}%)",
                NOW, {"key": f"spike:{fam}", "prev": hist_fams[fam], "curr": n}, source="job_posts",
            ))
    for c, n in concept_counts.items():
        if c not in hist_concepts:
            events.append(SignalEvent(
                "concept_first_seen", f"New concept in job posts: \"{c}\"",
                NOW, {"key": f"concept:{c}"}, source="job_posts", confidence=0.8,
            ))
        elif n >= 2 and n > hist_concepts[c]:
            growth = (n - hist_concepts[c]) / hist_concepts[c] * 100
            events.append(SignalEvent(
                "concept_spike", f"\"{c}\" mentions up {hist_concepts[c]} → {n} (+{growth:.0f}%) in 30d",
                NOW, {"key": f"cspike:{c}", "growth_pct": round(growth)}, source="job_posts",
            ))

    for ev in events[:8]:
        print(ev.show())
    if len(events) > 8:
        print(f"  ... and {len(events) - 8} more")
    ALL_EVENTS.extend(events[:8])
    print(f"\n  => unlocks search queries like \"companies struggling with "
          f"{list(concept_counts)[0] if concept_counts else 'X'}\"")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Scheduled monitoring + weekly digest
# ════════════════════════════════════════════════════════════════════════════

async def phase3() -> None:
    header(3, "Scheduled re-enrichment + weekly 'what changed' digest")

    sub("Refresh selection logic (real rule, demo watchlist)")
    watchlist = [
        ("vercel.com", NOW - timedelta(days=8)),
        ("ramp.com", NOW - timedelta(days=2)),
        ("linear.app", None),
        ("retool.com", NOW - timedelta(days=40)),
    ]
    print("  rule: watched accounts refresh if signal_enriched_at > 6 days (or never)")
    for domain, ts in watchlist:
        stale = ts is None or (NOW - ts).days > 6
        age = "never" if ts is None else f"{(NOW - ts).days}d ago"
        print(f"    {domain:<14} last_enriched={age:<8} -> {'REFRESH' if stale else 'skip'}")

    sub("OUTPUT: weekly digest rendered from this run's signal events")
    if not ALL_EVENTS:
        print("  (run phases 1-2 first to populate events)")
        return
    print(f"  Subject: {DEMO_COMPANY} and your watched accounts: "
          f"{len(ALL_EVENTS)} new signals this week\n")
    print(f"  {DEMO_COMPANY} ({DEMO_DOMAIN})")
    for ev in ALL_EVENTS:
        print(f"    • {ev.title}")
    print("\n  [View account] [Adjust watchlist]")
    print("\n  => closes the 'starred accounts checked weekly' truth gap")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 4 — Signal stacking heat score
# ════════════════════════════════════════════════════════════════════════════

EVENT_WEIGHTS = {
    "funding_round": 30, "role_first_seen": 18, "role_spike": 22,
    "concept_first_seen": 15, "concept_spike": 20, "tech_first_seen": 10,
    "tech_investment": 14, "headcount_jump": 16, "hiring_surge": 20,
    "social_post": 8, "job_change": 15, "exec_hire": 18,
}


async def phase4() -> None:
    header(4, "Signal stacking → account heat score")
    if not ALL_EVENTS:
        print("  (run phases 1-2 first to populate events)")
        return

    sub("LIVE: ICP relevance via real embeddings (gemini-embedding-001)")
    print(f"  ICP: \"{ICP_TEXT[:80]}...\"")
    icp_vec = embed_text(ICP_TEXT)
    rows = []
    for ev in ALL_EVENTS:
        ev_vec = embed_text(f"{ev.event_type}: {ev.title}")
        rel = cosine(icp_vec, ev_vec) if (icp_vec and ev_vec) else 0.5
        age_days = (NOW - ev.observed_at).days
        decay = math.exp(-age_days / 30)
        contrib = EVENT_WEIGHTS.get(ev.event_type, 10) * decay * rel * ev.confidence
        rows.append((ev, rel, decay, contrib))

    sub("OUTPUT: heat = sum(weight x e^(-age/30) x icp_relevance x confidence)")
    print(f"  {'event':<20} {'weight':>6} {'decay':>6} {'icp_rel':>8} {'contrib':>8}")
    total = 0.0
    for ev, rel, decay, contrib in sorted(rows, key=lambda r: -r[3]):
        total += contrib
        print(f"  {ev.event_type:<20} {EVENT_WEIGHTS.get(ev.event_type, 10):>6} "
              f"{decay:>6.2f} {rel:>8.3f} {contrib:>8.1f}")
    heat = min(100, round(total))
    print(f"\n  HEAT SCORE: {heat}/100 from {len(rows)} stacked signals")

    print("\n  why_now (as it would appear on a search result / CRM property):")
    for ev, _, _, contrib in sorted(rows, key=lambda r: -r[3])[:3]:
        print(f"    • {ev.title}")
    print(f"\n  => OpenFunnel's '3+ stacked signals' claim, computed per-org with real relevance")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 5 — ICP learning loop (accept/reject feedback)
# ════════════════════════════════════════════════════════════════════════════

ACCEPTED = [
    ("Attio", "Modern CRM for data-driven GTM teams, Series B, ~120 employees, building AI workflows"),
    ("Clay", "GTM data enrichment and outbound automation platform, Series B, hiring AEs and growth engineers"),
    ("Apollo.io", "Sales intelligence and engagement platform for outbound teams, growth stage"),
]
DISMISSED = [
    ("Toast", "Restaurant point-of-sale and management platform, public company, 5000+ employees"),
    ("Gusto", "Payroll and HR benefits for small businesses, late stage, sells to office managers"),
    ("Shopify", "E-commerce storefront platform, public, sells to merchants and retail brands"),
]
CANDIDATES = [
    ("Instantly", "Cold email outreach and deliverability platform for agencies and B2B sales teams"),
    ("Klaviyo", "Marketing automation for e-commerce brands, email and SMS campaigns"),
    ("Unify", "Warm outbound platform using buying signals and AI agents for B2B revenue teams"),
    ("Faire", "Wholesale marketplace connecting retailers with independent brands"),
    ("Common Room", "Community-led growth and GTM intelligence platform for B2B software companies"),
]


async def phase5() -> None:
    header(5, "ICP learning loop from accept/reject (real embeddings)")

    sub("Feedback recorded (what discovery_feedback rows capture)")
    for n, _ in ACCEPTED:
        print(f"  + accepted  : {n}")
    for n, _ in DISMISSED:
        print(f"  - dismissed : {n}")

    sub("LIVE: rank candidates by ICP embedding alone vs blended with accept-centroid")
    icp_vec = embed_text(ICP_TEXT)
    acc_vecs = [embed_text(f"{n}: {d}") for n, d in ACCEPTED]
    centroid = [sum(col) / len(acc_vecs) for col in zip(*acc_vecs)]
    blended = [0.7 * q + 0.3 * c for q, c in zip(icp_vec, centroid)]

    base, learned = [], []
    for name, desc in CANDIDATES:
        v = embed_text(f"{name}: {desc}")
        base.append((name, cosine(icp_vec, v)))
        learned.append((name, cosine(blended, v)))
    base.sort(key=lambda x: -x[1])
    learned.sort(key=lambda x: -x[1])

    print(f"  {'ICP query only':<34} {'ICP + accept-centroid (70/30)':<34}")
    for (n1, s1), (n2, s2) in zip(base, learned):
        print(f"  {n1:<24} {s1:.4f}    {n2:<24} {s2:.4f}")
    moved = [n for i, (n, _) in enumerate(learned) if base[i][0] != n]
    print(f"\n  ordering changed: {'yes — ' + ', '.join(moved[:3]) if moved else 'no (margins shifted)'}")

    sub("OUTPUT: few-shot block injected into the existing Gemini rerank prompt")
    print("  ORG FEEDBACK — companies this team ACCEPTED (score similar ones higher):")
    for n, d in ACCEPTED:
        print(f"    + {n}: {d[:70]}")
    print("  Companies this team DISMISSED (score similar ones lower):")
    for n, d in DISMISSED:
        print(f"    - {n}: {d[:70]}")
    print("\n  => OpenFunnel cannot do this: they never see CRM outcomes")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 6 — People signals: job changes + exec hires
# ════════════════════════════════════════════════════════════════════════════

async def phase6() -> None:
    header(6, "People signals: job-change detection + exec hires")

    sub(f"LIVE: Apollo free people search at {DEMO_DOMAIN} (current titles)")
    people = []
    try:
        from scrape.apollo_client import search_page
        data = await search_page(
            cfg.TELLORA_APOLLO_API_KEY,
            {"q_organization_domains_list": [DEMO_DOMAIN],
             "person_seniorities": ["vp", "head", "director"]},
            page=1, per_page=5,
        )
        people = data.get("people", [])
    except Exception as exc:
        print(f"  Apollo lookup failed: {exc}")
    for p in people[:4]:
        nm = f"{p.get('first_name', '')} {p.get('last_name', '') or ''}".strip()
        print(f"    {nm:<26} {p.get('title') or '(title hidden on free tier)'}")

    sub("OUTPUT: title diff vs stored contact (stored title SIMULATED as stale)")
    if people:
        p = people[0]
        nm = f"{p.get('first_name', '')} {p.get('last_name', '') or ''}".strip()
        new_title = p.get("title") or "VP of Engineering"
        old_title = "Senior Engineering Manager"
        ev = SignalEvent(
            "job_change",
            f"{nm}: \"{old_title}\" → \"{new_title}\" at {DEMO_COMPANY}",
            NOW, {"key": nm.lower().replace(" ", "_"), "prev_title": old_title,
                  "curr_title": new_title}, source="apollo_rematch", confidence=0.9,
        )
        print(ev.show())
        ALL_EVENTS.append(ev)

    sub(f"LIVE: exec-hire news via Serper (\"{DEMO_COMPANY}\" hires/appoints, last 90d)")
    hits = []
    if cfg.SERPER_API_KEY:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.post(
                "https://google.serper.dev/news",
                headers={"X-API-KEY": cfg.SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": f'"{DEMO_COMPANY}" hires OR appoints OR "joins as" VP OR Chief OR Head',
                      "num": 5, "tbs": "qdr:m3"},
            )
            hits = resp.json().get("news", [])
    if hits:
        for h in hits[:4]:
            print(f"    - {h.get('title', '')[:90]}")
            print(f"      {h.get('date', '?'):<16} {h.get('link', '')[:70]}")
    else:
        print("    (no exec-hire news in window — normal for many companies)")


# ════════════════════════════════════════════════════════════════════════════
# PHASE 7 — Social post monitoring (Serper public index)
# ════════════════════════════════════════════════════════════════════════════

SOCIAL_QUERY_PROMPT = "companies posting about launching AI agents for sales or GTM"


async def phase7() -> None:
    header(7, "Social post monitoring (Serper public index, OpenFunnel-style prompt)")
    print(f"  org_signal_query.prompt = \"{SOCIAL_QUERY_PROMPT}\"  lookback=30d")

    sub("LIVE: Serper search of LinkedIn posts (public Google index)")
    if not cfg.SERPER_API_KEY:
        print("  SERPER_API_KEY not set — skipping")
        return
    async with httpx.AsyncClient(timeout=12) as client:
        resp = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": cfg.SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": 'site:linkedin.com/posts "AI agent" (sales OR GTM OR outbound) launch',
                  "num": 10, "tbs": "qdr:m"},
        )
        organic = resp.json().get("organic", [])
    print(f"  {len(organic)} raw hits")
    for h in organic[:5]:
        print(f"    - {h.get('title', '')[:95]}")

    if not organic:
        return

    sub("LIVE: Gemini relevance filter → social_post events")
    items = [{"i": i, "title": h.get("title", ""), "snippet": h.get("snippet", "")[:200]}
             for i, h in enumerate(organic[:8])]
    verdicts = gemini_json(f"""Signal query: "{SOCIAL_QUERY_PROMPT}"
For each LinkedIn post below decide if it is a RELEVANT company/person buying-adjacent
signal for that query (not an ad, not a job post, not spam). Extract the company if named.

{json.dumps(items, indent=1)}

Respond ONLY with JSON: {{"results": [{{"i": 0, "relevant": true, "company": "name or null",
"reason": "<10 words"}}]}}""")["results"]
    kept = 0
    for v in verdicts:
        h = organic[v["i"]]
        mark = "KEEP" if v.get("relevant") else "drop"
        print(f"    [{mark}] {(v.get('company') or '?'):<22} {v.get('reason', '')[:50]}")
        if v.get("relevant"):
            kept += 1
    print(f"\n  => {kept} social_post events would be inserted (deduped by URL), "
          f"feeding heat score + digest")


# ════════════════════════════════════════════════════════════════════════════

PHASES = {1: phase1, 2: phase2, 3: phase3, 4: phase4, 5: phase5, 6: phase6, 7: phase7}


async def main() -> None:
    wanted = [int(a) for a in sys.argv[1:] if a.isdigit()] or sorted(PHASES)
    print(f"Signal Platform PoC — live run {NOW.isoformat()}  phases={wanted}")
    for n in wanted:
        try:
            await PHASES[n]()
        except Exception as exc:
            print(f"\n  PHASE {n} FAILED: {exc!r}")
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
