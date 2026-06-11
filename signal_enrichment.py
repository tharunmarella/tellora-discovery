"""
Company Signal Enrichment Pipeline.

Gathers buying signals for a single discovery_company record:
  1. Jina Reader  — homepage + /about + /careers (clean text)
  2. Job boards   — Greenhouse then Lever (public, free)
  3. Funding news — Jina Search for recent rounds
  4. Tech stack   — regex on homepage HTML/headers (no API needed)
  5. Gemini synthesis — company-only prompt using gemini-3.1-flash-lite

Adapted from tellora-backend/services/enrichment_service.py.
Person-specific params (name, job title, LinkedIn) removed.
Model: gemini-3.1-flash-lite  ($0.25/$1.50 per 1M tokens, stable)
"""

import asyncio
import hashlib
import json
import logging
import re
import time as _time
from datetime import datetime, timezone
from typing import Optional

import httpx
from pydantic import BaseModel, Field

import settings as cfg
from llm import embed_text, get_gemini_client, retry_llm, strip_json_fences

logger = logging.getLogger("discovery.signal")

# ── Constants ──────────────────────────────────────────────────────────────

JINA_READER    = "https://r.jina.ai/"
JINA_SEARCH    = "https://s.jina.ai/"
GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
LEVER_API      = "https://api.lever.co/v0/postings/{slug}?mode=json"

_HTTP_TIMEOUT = 8.0
_DOMAIN_CACHE: dict[str, tuple[float, dict]] = {}
_DOMAIN_CACHE_TTL = 3600  # 1 hour

# Apollo's free people-search returns total_entries (contacts Apollo knows at a
# company). It is NOT headcount, but scales with company size, so we use it as a
# last-resort headcount proxy when Gemini + Serper KG give us nothing. The factor
# converts "contacts Apollo has" → "approx employees" (rough; tune via env).
APOLLO_HEADCOUNT_FACTOR = float(getattr(cfg, "APOLLO_HEADCOUNT_FACTOR", 1.0) or 1.0)


# ── Serper Knowledge Graph lookup ──────────────────────────────────────────

async def fetch_serper_kg(company_name: str) -> dict:
    """
    Single Serper call to get Google Knowledge Graph + LinkedIn snippet.
    Returns structured facts: {founded, headquarters, founder, headcount, kg_description}.
    Zero LLM cost — pure Google structured data.
    """
    if not company_name or not cfg.SERPER_API_KEY:
        return {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": cfg.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": f"{company_name} company", "num": 5},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning(f"Serper KG failed for '{company_name}': {exc}")
        return {}

    result: dict = {}

    kg = data.get("knowledgeGraph", {})
    if kg:
        attrs = kg.get("attributes", {})
        result["kg_description"] = kg.get("description", "")
        result["founded"] = attrs.get("Founded", "")
        result["headquarters"] = attrs.get("Headquarters", "")
        result["founder"] = attrs.get("Founder", "")
        result["ceo"] = attrs.get("CEO", "")
        emp = attrs.get("Number of employees", "")
        if emp:
            nums = re.findall(r"[\d,]+", emp.replace(",", ""))
            if nums:
                try:
                    result["headcount"] = int(nums[-1])
                except ValueError:
                    pass

    for hit in data.get("organic", []):
        snippet = hit.get("snippet", "")
        link = hit.get("link", "")
        if "linkedin.com/company" in link:
            size_match = re.search(r"(\d[\d,]*)\s*[-–]\s*([\d,]+)\s*employees", snippet, re.IGNORECASE)
            if size_match and "headcount" not in result:
                try:
                    result["headcount"] = int(size_match.group(2).replace(",", ""))
                except ValueError:
                    pass
            break

    if result:
        logger.info(f"Serper KG for {company_name}: {result}")
    return result


# ── Apollo free people-count headcount proxy ───────────────────────────────

def _round_headcount(n: int) -> int:
    """Round to a tidy magnitude so an estimate doesn't look falsely precise."""
    if n <= 0:
        return 0
    if n < 100:
        return max(10, round(n / 10) * 10)
    if n < 1000:
        return round(n / 50) * 50
    return round(n / 100) * 100


async def fetch_apollo_headcount(domain: str) -> dict:
    """
    Free Apollo people-search by domain → total_entries (contacts Apollo has at
    the company). Used only as a last-resort headcount proxy: estimate ≈
    total_entries × APOLLO_HEADCOUNT_FACTOR. Costs no Apollo credits.

    Returns {"apollo_people_count": int, "headcount_estimate": int} or {}.
    """
    if not domain or not getattr(cfg, "TELLORA_APOLLO_API_KEY", None):
        return {}
    try:
        from apollo_client import search_page
        data = await search_page(
            cfg.TELLORA_APOLLO_API_KEY,
            {"q_organization_domains_list": [domain]},
            page=1,
            per_page=1,
        )
    except Exception as exc:
        logger.warning(f"Apollo headcount lookup failed for '{domain}': {exc}")
        return {}

    total = data.get("total_entries") or data.get("pagination", {}).get("total_entries") or 0
    try:
        total = int(total)
    except (TypeError, ValueError):
        total = 0
    if total <= 0:
        return {}

    estimate = _round_headcount(int(total * APOLLO_HEADCOUNT_FACTOR))
    logger.info(f"Apollo headcount proxy for {domain}: people={total} → ~{estimate} employees")
    return {"apollo_people_count": total, "headcount_estimate": estimate}


# ── Tech stack patterns ────────────────────────────────────────────────────

_TECH_PATTERNS: dict[str, list[str]] = {
    "hubspot":      [r"hubspot\.com", r"hs-scripts", r"hbspt\."],
    "salesforce":   [r"salesforce\.com", r"force\.com", r"pardot\.com"],
    "intercom":     [r"intercom\.io", r"intercomSettings"],
    "drift":        [r"drift\.com"],
    "zendesk":      [r"zendesk\.com"],
    "crisp":        [r"crisp\.chat", r"client\.crisp\.chat"],
    "calendly":     [r"calendly\.com"],
    "stripe":       [r"js\.stripe\.com", r"stripe\.com/v3"],
    "braintree":    [r"braintreegateway\.com"],
    "paypal":       [r"paypalobjects\.com"],
    "segment":      [r"cdn\.segment\.com", r"analytics\.js"],
    "mixpanel":     [r"mixpanel\.com", r"mixpanel\.init"],
    "heap":         [r"heapanalytics\.com", r"heap\.io"],
    "hotjar":       [r"hotjar\.com"],
    "clarity":      [r"clarity\.ms"],
    "klaviyo":      [r"a\.klaviyo\.com", r"klaviyo\.com/onsite"],
    "mailchimp":    [r"list-manage\.com", r"mailchimp\.com"],
    "shopify":      [r"cdn\.shopify\.com", r"Shopify\.theme"],
    "react":        [r"_next/static", r"react\.production\.min", r"__NEXT_DATA__"],
    "next.js":      [r"_next/", r"__NEXT_DATA__"],
    "vue":          [r"vue\.runtime", r"__vue__"],
    "angular":      [r"ng-version="],
    "cloudflare":   [r"cloudflare\.com", r"__cf_bm", r"cf-ray"],
}


# ── Tech stack detection ───────────────────────────────────────────────────

async def detect_tech_stack(domain: str) -> list[str]:
    """
    Fetch homepage HTML + headers, regex-match known tech.
    Returns list of detected technology keys e.g. ["hubspot", "stripe"].
    Zero cost — pure HTTP + regex.
    """
    if not domain:
        return []
    url = f"https://{domain}" if not domain.startswith("http") else domain
    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TelloraSalesBot/1.0)"},
        ) as client:
            resp = await client.get(url)
            search_text = resp.text + " " + " ".join(
                f"{k}: {v}" for k, v in resp.headers.items()
            )
    except Exception as exc:
        logger.warning(f"Tech stack fetch failed for {domain}: {exc}")
        return []

    detected = []
    for tech, patterns in _TECH_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, search_text, re.IGNORECASE):
                detected.append(tech)
                break
    return detected


# ── DNS TXT/MX tech detection ──────────────────────────────────────────────

# Domain-verification TXT records prove actual vendor usage (unlike marketing pages)
_DNS_TXT_PATTERNS: dict[str, str] = {
    "google-site-verification": "google_workspace",
    "ms=": "microsoft365",
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
    "loom-verification": "loom",
    "webex": "webex",
    "dropbox-domain-verification": "dropbox",
}


def _detect_dns_tech_sync(domain: str) -> list[str]:
    """Resolve TXT + MX records and map them to vendors. Synchronous (dnspython)."""
    if not domain:
        return []
    try:
        import dns.resolver
    except ImportError:
        logger.warning("dnspython not installed — skipping DNS tech detection")
        return []

    resolver = dns.resolver.Resolver()
    resolver.timeout = 5
    resolver.lifetime = 8

    detected: set[str] = set()

    try:
        for rdata in resolver.resolve(domain, "TXT"):
            rec = b"".join(rdata.strings).decode("utf-8", errors="ignore").lower()
            for needle, vendor in _DNS_TXT_PATTERNS.items():
                if needle in rec:
                    detected.add(vendor)
    except Exception:
        pass

    try:
        for rdata in resolver.resolve(domain, "MX"):
            host = str(rdata.exchange).lower()
            if "google" in host:
                detected.add("google_workspace")
            elif "outlook" in host or "microsoft" in host:
                detected.add("microsoft365")
            elif "zoho" in host:
                detected.add("zoho")
            elif "proofpoint" in host:
                detected.add("proofpoint")
    except Exception:
        pass

    return sorted(detected)


async def detect_dns_tech(domain: str) -> list[str]:
    """Async wrapper — DNS resolution runs in a thread to keep the loop free."""
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, _detect_dns_tech_sync, domain)
    except Exception as exc:
        logger.warning(f"DNS tech detection failed for {domain}: {exc}")
        return []


# ── Jina helpers ───────────────────────────────────────────────────────────

async def _jina_read(url: str, max_chars: int = 3000) -> str:
    """Fetch clean text from a URL via Jina Reader. Returns '' on failure."""
    if not url:
        return ""
    if not url.startswith("http"):
        url = f"https://{url}"
    headers: dict[str, str] = {"Accept": "text/plain", "X-No-Cache": "true"}
    if cfg.JINA_API_KEY:
        headers["Authorization"] = f"Bearer {cfg.JINA_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(f"{JINA_READER}{url}", headers=headers)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("retry-after", "15"))
                logger.warning(f"Jina 429 for {url} — backing off {retry_after}s")
                await asyncio.sleep(min(retry_after, 30))
                resp = await client.get(f"{JINA_READER}{url}", headers=headers)
            resp.raise_for_status()
            return resp.text[:max_chars]
    except Exception as exc:
        logger.warning(f"Jina read failed for '{url}': {exc}")
        return ""


def page_fingerprint(text_content: str) -> str:
    """Stable SHA1 fingerprint of normalized page text. '' for empty pages."""
    if not text_content or len(text_content.strip()) < 100:
        return ""
    normalized = re.sub(r"\s+", " ", text_content).strip().lower()
    return hashlib.sha1(normalized.encode()).hexdigest()[:16]


async def fetch_wayback_fingerprint(domain: str, path: str = "/pricing") -> str:
    """
    Historical page fingerprint via Wayback CDX + Jina (cold-start for pricing diffs).
    Finds a snapshot 30-90 days old and fingerprints normalized text.
    """
    if not domain:
        return ""
    url = f"{domain}{path}" if path.startswith("/") else f"{domain}/{path}"
    if not url.startswith("http"):
        url = f"https://{url}"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "http://web.archive.org/cdx/search/cdx",
                params={
                    "url": url,
                    "output": "json",
                    "filter": "statuscode:200",
                    "limit": "-20",
                },
            )
            if resp.status_code != 200:
                return ""
            rows = resp.json()
            if len(rows) < 2:
                return ""

            now = datetime.now(timezone.utc)
            target_ts = None
            for row in reversed(rows[1:]):
                if len(row) < 2:
                    continue
                ts = row[1]
                if len(ts) < 8:
                    continue
                try:
                    snap_dt = datetime.strptime(ts[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                age_days = (now - snap_dt).days
                if 30 <= age_days <= 90:
                    target_ts = ts
                    break

            if not target_ts and len(rows) > 1:
                target_ts = rows[1][1]

            if not target_ts:
                return ""

            archived = f"https://web.archive.org/web/{target_ts}/{url}"
            text = await _jina_read(archived, max_chars=2000)
            return page_fingerprint(text)
    except Exception as exc:
        logger.warning(f"Wayback fingerprint failed for {domain}{path}: {exc}")
        return ""


async def fetch_company_context(domain: str) -> dict:
    """
    Fetch homepage + /about + /careers + /pricing + /customers + /changelog
    via Jina Reader. Cached per domain for 1 hour.
    Returns {homepage, about, careers, pricing, customers, changelog}.
    """
    empty = {"homepage": "", "about": "", "careers": "",
             "pricing": "", "customers": "", "changelog": ""}
    if not domain:
        return empty

    cached = _DOMAIN_CACHE.get(domain)
    if cached:
        ts, ctx = cached
        if _time.time() - ts < _DOMAIN_CACHE_TTL and "pricing" in ctx:
            return ctx
        del _DOMAIN_CACHE[domain]

    base = f"https://{domain}" if not domain.startswith("http") else domain

    homepage_text, about_text, careers_text, pricing_text, customers_text, changelog_text = (
        await asyncio.gather(
            _jina_read(base, max_chars=8000),
            _jina_read(f"{base}/about", max_chars=2000),
            _jina_read(f"{base}/careers", max_chars=2000),
            _jina_read(f"{base}/pricing", max_chars=2000),
            _jina_read(f"{base}/customers", max_chars=2000),
            _jina_read(f"{base}/changelog", max_chars=2000),
        )
    )

    if len(about_text) < 200:
        about_alt = await _jina_read(f"{base}/about-us", max_chars=2000)
        if len(about_alt) > len(about_text):
            about_text = about_alt

    if len(careers_text) < 200:
        for path in ["/jobs", "/join-us", "/work-with-us", "/open-positions"]:
            alt = await _jina_read(f"{base}{path}", max_chars=2000)
            if len(alt) > len(careers_text):
                careers_text = alt
            if len(careers_text) >= 200:
                break

    if len(customers_text) < 200:
        alt = await _jina_read(f"{base}/case-studies", max_chars=2000)
        if len(alt) > len(customers_text):
            customers_text = alt

    if len(changelog_text) < 200:
        for path in ["/blog", "/whats-new", "/releases"]:
            alt = await _jina_read(f"{base}{path}", max_chars=2000)
            if len(alt) > len(changelog_text):
                changelog_text = alt
            if len(changelog_text) >= 200:
                break

    ctx = {
        "homepage": homepage_text,
        "about": about_text,
        "careers": careers_text,
        "pricing": pricing_text,
        "customers": customers_text,
        "changelog": changelog_text,
    }
    _DOMAIN_CACHE[domain] = (_time.time(), ctx)
    logger.info(
        f"Company context for {domain}: "
        f"homepage={len(homepage_text)}c, about={len(about_text)}c, careers={len(careers_text)}c, "
        f"pricing={len(pricing_text)}c, customers={len(customers_text)}c, changelog={len(changelog_text)}c"
    )
    return ctx


async def fetch_exec_hire_news(company_name: str) -> list[dict]:
    """Serper news for executive hires at a company."""
    if not company_name or not cfg.SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post(
                "https://google.serper.dev/news",
                headers={
                    "X-API-KEY": cfg.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "q": f'"{company_name}" hires OR appoints OR "joins as" VP OR Chief OR Head',
                    "num": 4,
                    "tbs": "qdr:m3",
                },
            )
            resp.raise_for_status()
            return [
                {"title": h.get("title", ""), "url": h.get("link", ""), "date": h.get("date", "")}
                for h in resp.json().get("news", [])[:4]
                if h.get("title")
            ]
    except Exception as exc:
        logger.warning(f"Exec hire news failed for '{company_name}': {exc}")
        return []


async def fetch_funding_news(company_name: str) -> list[str]:
    """
    Jina Search for recent funding news. Returns up to 4 snippets.
    Requires JINA_API_KEY.
    """
    if not company_name or not cfg.JINA_API_KEY:
        return []
    query = f"{company_name} funding raised investment round"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {cfg.JINA_API_KEY}",
    }
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.get(f"{JINA_SEARCH}{query}", headers=headers)
            resp.raise_for_status()
            data = resp.json()
            snippets = []
            for hit in data.get("data", [])[:4]:
                title = hit.get("title", "").strip()
                desc  = hit.get("description", "").strip()[:300]
                url   = hit.get("url", "")
                if title and desc:
                    snippets.append(f"{title} — {desc} ({url})")
            return snippets
    except Exception as exc:
        logger.warning(f"Funding news search failed for '{company_name}': {exc}")
        return []


# ── Job board helpers ──────────────────────────────────────────────────────

def _slug_variants(company_name: str) -> list[str]:
    base = company_name.lower().strip()
    for suffix in [
        " inc", " inc.", " llc", " ltd", " corp", " corporation",
        " co", " co.", " technologies", " technology", " tech",
        " solutions", " group", " labs", " ai", " io", ".io", ".ai",
    ]:
        if base.endswith(suffix):
            base = base[: -len(suffix)].strip()
    hyphen     = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    nospace    = re.sub(r"[^a-z0-9]+", "", base)
    first_word = hyphen.split("-")[0] if "-" in hyphen else ""
    variants   = [hyphen]
    if nospace and nospace != hyphen:
        variants.append(nospace)
    if first_word and len(first_word) > 3 and first_word != hyphen:
        variants.append(first_word)
    return list(dict.fromkeys(variants))


async def check_job_boards(company_name: str) -> dict:
    """
    Check Greenhouse then Lever for open positions.
    Returns {count, roles, source}.
    """
    variants = _slug_variants(company_name)
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        for slug in variants:
            try:
                resp = await client.get(GREENHOUSE_API.format(slug=slug))
                if resp.status_code == 200:
                    jobs = resp.json().get("jobs", [])
                    if jobs:
                        roles = [j.get("title", "") for j in jobs[:10]]
                        return {"count": len(jobs), "roles": roles, "source": "greenhouse"}
            except Exception:
                pass

        for slug in variants:
            try:
                resp = await client.get(LEVER_API.format(slug=slug))
                if resp.status_code == 200:
                    jobs = resp.json()
                    if isinstance(jobs, list) and jobs:
                        roles = [j.get("text", "") for j in jobs[:10]]
                        return {"count": len(jobs), "roles": roles, "source": "lever"}
            except Exception:
                pass

    return {"count": 0, "roles": [], "source": "none"}


# ── Gemini synthesis ───────────────────────────────────────────────────────

class CompanySignalResult(BaseModel):
    """Structured company-level signal output from Gemini."""
    company_summary: Optional[str] = Field(
        None,
        description="2-3 sentences: what the company does, who they sell to, market position. "
                    "From homepage/about only. null if page text is too thin.",
    )
    buying_signals: list[str] = Field(
        default_factory=list,
        description="Concrete, time-sensitive triggers e.g. 'Series B raised $25M'. Empty if none.",
    )
    signal_score: int = Field(
        default=0,
        description="0-100 urgency. 0=dormant, 40=some signals, 70=strong, 90+=exceptional.",
    )
    funding_stage: Optional[str] = Field(None, description="e.g. 'Series A', 'Seed'. null if unknown.")
    total_raised: Optional[str] = Field(None, description="e.g. '$25M', '$1.2B'. null if unknown.")
    investors: list[str] = Field(default_factory=list)
    headcount: Optional[int] = Field(None, description="Approx employee count. null if unknown.")
    hiring_roles: list[str] = Field(
        default_factory=list,
        description="Open job titles from job board data.",
    )
    tech_stack: list[str] = Field(
        default_factory=list,
        description="Technologies detected on the company website.",
    )
    pricing_model: Optional[str] = Field(
        None,
        description="One of: enterprise, self-serve, freemium, usage-based. null if unclear.",
    )
    known_customers: list[str] = Field(default_factory=list)
    recent_launches: list[str] = Field(
        default_factory=list,
        description="Product launches/releases visible in changelog/blog text, "
                    "e.g. 'AI Gateway GA (2026-06)'. Empty if none.",
    )
    hq_city: Optional[str] = Field(
        None,
        description="Canonical English city name (GeoNames-style). null if unknown.",
    )
    hq_region: Optional[str] = Field(
        None,
        description="US: ISO 3166-2 two-letter state (CA, NY). Non-US: subdivision name. null if unknown.",
    )
    hq_country: Optional[str] = Field(
        None,
        description="ISO 3166-1 alpha-2 country code (US, GB). null if unknown.",
    )


_EMPTY_SIGNAL = CompanySignalResult(
    company_summary=None,
    buying_signals=[],
    signal_score=0,
)

_JSON_SCHEMA = """{
  "company_summary": "<2-3 sentences from homepage/about, null if insufficient>",
  "buying_signals": ["<signal1>", "<signal2>"],
  "signal_score": <0-100>,
  "funding_stage": "<stage or null>",
  "total_raised": "<amount or null>",
  "investors": ["<investor1>"],
  "headcount": <int or null>,
  "hiring_roles": ["<role1>", "<role2>"],
  "tech_stack": ["<tool1>", "<tool2>"],
  "pricing_model": "<enterprise|self-serve|freemium|usage-based or null>",
  "known_customers": ["<customer1>"],
  "recent_launches": ["<launch title (date if visible)>"],
  "hq_city": "<canonical English city or null>",
  "hq_region": "<ISO 3166-2 subdivision or null>",
  "hq_country": "<ISO 3166-1 alpha-2 or null>"
}"""

_HQ_NORMALIZATION_RULES = """HEADQUARTERS NORMALIZATION (hq_city, hq_region, hq_country):
- Normalize from KNOWN HEADQUARTERS and/or Google Knowledge Graph HQ evidence.
- Return null for any field you cannot determine confidently — never guess.
- hq_city: Canonical English city name (GeoNames-style). No abbreviations (not "SF"/"NYC"/"SFO"). For metro/region labels (e.g. "Bay Area", "Greater NYC Area"), use the anchor city ("San Francisco", "New York").
- hq_region: United States → ISO 3166-2 two-letter state code (CA, NY, TX). Other countries → full subdivision/province name, or null.
- hq_country: ISO 3166-1 alpha-2 uppercase (US, GB, IN, DE)."""

_HQ_BATCH_JSON_SCHEMA = """{
  "items": [
    {"raw": "<exact input headquarters string>", "hq_city": "<canonical English city or null>", "hq_region": "<ISO 3166-2 subdivision or null>", "hq_country": "<ISO 3166-1 alpha-2 or null>"}
  ]
}"""

_EMPTY_HQ = {"hq_city": None, "hq_region": None, "hq_country": None}


def _parse_hq_batch_response(raw: str) -> list[dict]:
    text = strip_json_fences(raw)
    start_obj = text.find("{")
    start_arr = text.find("[")
    if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
        end = text.rfind("}")
        payload = json.loads(text[start_obj:end + 1])
        if isinstance(payload, dict):
            items = payload.get("items") or payload.get("results") or []
            return items if isinstance(items, list) else []
    if start_arr != -1:
        end = text.rfind("]")
        return json.loads(text[start_arr:end + 1])
    return json.loads(text)


def _hq_normalize_model(provider: str, model: Optional[str] = None) -> str:
    if model:
        return model
    if provider == "groq":
        return cfg.HQ_GROQ_MODEL
    if cfg.HQ_NORMALIZE_MODEL:
        return cfg.HQ_NORMALIZE_MODEL
    return cfg.SIGNAL_GEMINI_MODEL


def _hq_provider_ready(provider: str) -> bool:
    if provider == "groq":
        return bool(cfg.GROQ_API_KEY)
    return bool(cfg.GEMINI_API_KEY)


def _call_hq_llm(prompt: str, *, provider: str, model: str) -> str:
    if provider == "groq":
        from groq import Groq
        client = Groq(api_key=cfg.GROQ_API_KEY)
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You normalize company headquarters strings to structured geographic fields. "
                        "Respond with valid JSON only."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content or ""

    client = get_gemini_client()
    resp = client.models.generate_content(model=model, contents=prompt)
    return resp.text


def _build_hq_batch_prompt(inputs: list[str]) -> str:
    lines = "\n".join(f"{i + 1}. {raw}" for i, raw in enumerate(inputs))
    return f"""You normalize messy company headquarters strings to standard geographic fields.

{_HQ_NORMALIZATION_RULES}

For EACH headquarters string below, output one object in the "items" array.
Include the exact raw string unchanged in "raw".
Return null for any field you cannot determine confidently — never guess.

HEADQUARTERS STRINGS:
{lines}

Respond with ONLY valid JSON matching this schema. No markdown:
{_HQ_BATCH_JSON_SCHEMA}"""


def _items_to_hq_map(items: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for item in items:
        raw = (item.get("raw") or "").strip()
        if not raw:
            continue
        out[raw] = {
            "hq_city": item.get("hq_city") or None,
            "hq_region": item.get("hq_region") or None,
            "hq_country": item.get("hq_country") or None,
        }
    return out


def normalize_headquarters_batch(
    raw_headquarters: list[str],
    *,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> dict[str, dict]:
    """
    Normalize many raw headquarters strings in one LLM call (Gemini or Groq).
    Returns {raw_string: {hq_city, hq_region, hq_country}} for each input.
    """
    inputs = [(r or "").strip() for r in raw_headquarters if (r or "").strip()]
    if not inputs:
        return {}

    provider = (provider or cfg.HQ_NORMALIZE_PROVIDER or "gemini").strip().lower()
    if not _hq_provider_ready(provider):
        logger.warning(f"HQ batch normalization skipped — {provider} API key not set")
        return {}

    prompt = _build_hq_batch_prompt(inputs)
    model_id = _hq_normalize_model(provider, model)

    def _do():
        raw = _call_hq_llm(prompt, provider=provider, model=model_id)
        return _items_to_hq_map(_parse_hq_batch_response(raw))

    try:
        return retry_llm(_do)
    except Exception as exc:
        logger.warning(
            f"HQ batch normalization failed ({len(inputs)} items, {provider}/{model_id}): {exc}"
        )
        return {}


def normalize_headquarters(
    raw_headquarters: str,
    *,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> dict:
    """
    Normalize a single raw headquarters string to hq_city / hq_region / hq_country.
    Used by the one-off backfill script; enrichment uses synthesize_company_signals.
    """
    raw = (raw_headquarters or "").strip()
    if not raw:
        return dict(_EMPTY_HQ)
    batch = normalize_headquarters_batch([raw], provider=provider, model=model)
    return batch.get(raw) or dict(_EMPTY_HQ)


def synthesize_company_signals(
    company_name: str,
    homepage_text: str,
    about_text: str,
    careers_text: str,
    tech_stack: list[str],
    job_board: dict,
    funding_news: list[str],
    serper_kg: Optional[dict] = None,
    existing_headquarters: Optional[str] = None,
    pricing_text: str = "",
    customers_text: str = "",
    changelog_text: str = "",
) -> CompanySignalResult:
    """
    Send all company evidence to Gemini and receive a structured CompanySignalResult.
    Uses gemini-3.1-flash-lite — stable, optimized for extraction at scale.
    Synchronous: runs in a thread pool via asyncio.get_event_loop().run_in_executor.
    """
    if not cfg.GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set — skipping synthesis")
        return _EMPTY_SIGNAL

    tech_line = (
        f"Detected tools on homepage: {', '.join(tech_stack)}"
        if tech_stack
        else "No tools detected on homepage."
    )

    jobs = job_board
    if jobs.get("count"):
        jobs_line = (
            f"Source: {jobs['source']} | "
            f"{jobs['count']} open roles: {', '.join(jobs['roles'][:8])}"
        )
    elif careers_text.strip():
        jobs_line = "Not found on Greenhouse/Lever — see CAREERS PAGE below for hiring signals."
    else:
        jobs_line = "No open roles found on Greenhouse, Lever, or careers page."

    news_block = (
        "\n".join(f"- {s}" for s in funding_news)
        if funding_news
        else "No recent funding news found."
    )

    kg = serper_kg or {}
    kg_lines = []
    if kg.get("kg_description"):
        kg_lines.append(f"Description: {kg['kg_description']}")
    if kg.get("founded"):
        kg_lines.append(f"Founded: {kg['founded']}")
    if kg.get("headquarters"):
        kg_lines.append(f"HQ: {kg['headquarters']}")
    if kg.get("ceo"):
        kg_lines.append(f"CEO: {kg['ceo']}")
    if kg.get("founder"):
        kg_lines.append(f"Founder: {kg['founder']}")
    if kg.get("headcount"):
        kg_lines.append(f"Employees: ~{kg['headcount']}")
    kg_block = "\n".join(kg_lines) if kg_lines else "No Knowledge Graph data available."

    prompt = f"""You are a B2B sales intelligence analyst. Extract factual signals from the evidence below.

STRICT RULES:
1. Only state facts visible in the evidence. Do NOT invent or infer.
2. If a field has no evidence, leave it null / empty list.
3. signal_score: 0=no evidence, 40=some signals, 70=strong buying triggers, 90+=exceptional.
4. company_summary: what the company does, who they sell to, market position. From homepage/about only. null if text is too thin.
5. buying_signals: concrete, time-sensitive triggers only (funding rounds, hiring sprees, product launches, expansions).
6. pricing_model: ground in the PRICING PAGE text when available.
7. known_customers: named customers from the CUSTOMERS PAGE / homepage logos.
8. recent_launches: product launches/releases visible in the CHANGELOG/BLOG text, with dates when shown.
9. {_HQ_NORMALIZATION_RULES}

COMPANY: {company_name}

KNOWN HEADQUARTERS (raw, from database):
{(existing_headquarters or "").strip() or "Unknown"}

TECH STACK:
{tech_line}

JOB BOARD:
{jobs_line}

RECENT FUNDING NEWS:
{news_block}

GOOGLE KNOWLEDGE GRAPH / LINKEDIN:
{kg_block}

COMPANY HOMEPAGE:
{homepage_text or "Unavailable"}

COMPANY ABOUT PAGE:
{about_text or "Unavailable"}

COMPANY CAREERS PAGE:
{careers_text or "Unavailable"}

COMPANY PRICING PAGE:
{pricing_text or "Unavailable"}

COMPANY CUSTOMERS PAGE:
{customers_text or "Unavailable"}

COMPANY CHANGELOG / BLOG:
{changelog_text or "Unavailable"}

Respond with ONLY valid JSON matching this exact schema. No markdown, no explanation:
{_JSON_SCHEMA}"""

    def _do():
        client = get_gemini_client()
        resp = client.models.generate_content(model=cfg.SIGNAL_GEMINI_MODEL, contents=prompt)
        data = json.loads(strip_json_fences(resp.text))
        return CompanySignalResult(**{
            k: data.get(k)
            for k in CompanySignalResult.model_fields
        })

    try:
        return retry_llm(_do)
    except Exception as exc:
        logger.warning(f"Gemini synthesis failed for {company_name}: {exc}")
        return _EMPTY_SIGNAL


# ── Embedding ──────────────────────────────────────────────────────────────

def embed_company(
    company_summary: Optional[str],
    description: Optional[str],
    industry: Optional[str],
    tech_stack: list[str],
    recent_launches: Optional[list[str]] = None,
) -> Optional[list[float]]:
    """
    Embed company text for semantic search using gemini-embedding-001.
    Uses company_summary if available (richer), falls back to description.
    Synchronous — run in executor.
    """
    if not cfg.GEMINI_API_KEY:
        return None

    parts = []
    if company_summary:
        parts.append(company_summary)
    elif description:
        parts.append(description)
    if industry:
        parts.append(industry)
    if tech_stack:
        parts.append(", ".join(tech_stack))
    if recent_launches:
        parts.append("Recent launches: " + "; ".join(recent_launches[:5]))

    text = " ".join(filter(None, parts)).strip()
    if not text:
        return None

    return embed_text(text)


def build_search_tsv(
    company_summary: Optional[str],
    description: Optional[str],
    industry: Optional[str],
    tech_stack: list[str],
    raw_meta: Optional[dict],
    recent_launches: Optional[list[str]] = None,
    known_customers: Optional[list[str]] = None,
) -> str:
    """
    Build a concatenated plain-text string to be converted to tsvector in Postgres.
    Used for BM25 hybrid search.
    """
    parts = []
    if company_summary:
        parts.append(company_summary)
    if description and description != company_summary:
        parts.append(description)
    if industry:
        parts.append(industry)
    if tech_stack:
        parts.append(" ".join(tech_stack))
    if recent_launches:
        parts.append(" ".join(recent_launches))
    if known_customers:
        parts.append(" ".join(known_customers))
    if raw_meta:
        if raw_meta.get("keywords"):
            kws = raw_meta["keywords"]
            if isinstance(kws, list):
                parts.append(" ".join(kws))
        if raw_meta.get("use_case"):
            parts.append(raw_meta["use_case"])
    return " ".join(parts)


# ── Main per-company enrichment entry point ────────────────────────────────

async def enrich_company_signals(
    company_id: str,
    company_name: str,
    domain: str,
    description: Optional[str],
    industry: Optional[str],
    raw_meta: Optional[dict],
    existing_headcount: Optional[int] = None,
    existing_headquarters: Optional[str] = None,
) -> dict:
    """
    Full signal enrichment for one company. Returns a dict ready to write
    back to the discovery_company table.

    Steps:
      1. Parallel: fetch_company_context + check_job_boards + fetch_funding_news + detect_tech_stack
      2. Gemini synthesis  (run in thread pool — synchronous Gemini SDK)
      3. Embed             (run in thread pool)
      4. Build search_tsv text
    """
    logger.info(f"[{company_name}] Starting signal enrichment")

    # Step 1 — parallel signal gathering
    from github_signals import fetch_github_signals, github_extra_events
    from gov_signals import fetch_gov_awards, gov_extra_events
    from hn_signals import fetch_hn_signals, hn_extra_events
    from job_posts import fetch_job_board_posts
    from news_signals import classify_news, fetch_company_news

    ctx_task     = asyncio.create_task(fetch_company_context(domain))
    jobs_task    = asyncio.create_task(fetch_job_board_posts(company_name))
    news_task    = asyncio.create_task(fetch_funding_news(company_name))
    tech_task    = asyncio.create_task(detect_tech_stack(domain))
    kg_task = asyncio.create_task(fetch_serper_kg(company_name))
    dns_task = asyncio.create_task(detect_dns_tech(domain))
    gh_task = asyncio.create_task(fetch_github_signals(domain))
    rss_task = asyncio.create_task(fetch_company_news(company_name))
    hn_task = asyncio.create_task(fetch_hn_signals(company_name, domain))
    gov_task = asyncio.create_task(fetch_gov_awards(company_name))
    wayback_task = asyncio.create_task(fetch_wayback_fingerprint(domain, "/pricing"))
    need_apollo_hc = not (existing_headcount and existing_headcount > 0)
    if need_apollo_hc:
        apollo_task = asyncio.create_task(fetch_apollo_headcount(domain))
        (ctx, job_posts_result, funding_news, tech_stack, serper_kg,
         dns_tech, github, rss_news, hn_data, gov_awards, baseline_pricing_fp,
         apollo_hc) = await asyncio.gather(
            ctx_task, jobs_task, news_task, tech_task, kg_task,
            dns_task, gh_task, rss_task, hn_task, gov_task, wayback_task, apollo_task,
        )
    else:
        (ctx, job_posts_result, funding_news, tech_stack, serper_kg,
         dns_tech, github, rss_news, hn_data, gov_awards, baseline_pricing_fp) = await asyncio.gather(
            ctx_task, jobs_task, news_task, tech_task, kg_task,
            dns_task, gh_task, rss_task, hn_task, gov_task, wayback_task,
        )
        apollo_hc = {}

    # DNS-verified vendors + GitHub languages beat homepage regex — merge all
    tech_stack = list(dict.fromkeys(
        list(tech_stack) + list(dns_tech) + list(github.get("languages") or [])
    ))

    job_posts, job_source = job_posts_result if isinstance(job_posts_result, tuple) else ([], "none")
    job_board = {
        "count": len(job_posts),
        "roles": [p.get("title", "") for p in job_posts[:10]],
        "source": job_source,
    }

    homepage_text = ctx.get("homepage", "")
    about_text    = ctx.get("about", "")
    careers_text  = ctx.get("careers", "")
    pricing_text  = ctx.get("pricing", "")
    customers_text = ctx.get("customers", "")
    changelog_text = ctx.get("changelog", "")

    # Step 2 — Gemini synthesis (run in executor to keep event loop free)
    loop = asyncio.get_event_loop()
    result: CompanySignalResult = await loop.run_in_executor(
        None,
        synthesize_company_signals,
        company_name,
        homepage_text,
        about_text,
        careers_text,
        tech_stack,
        job_board,
        funding_news,
        serper_kg,
        existing_headquarters,
        pricing_text,
        customers_text,
        changelog_text,
    )

    # News: Google News RSS + Gemini classifier (free); Serper exec-hire as fallback
    news_events: list[dict] = []
    if rss_news:
        news_events = await loop.run_in_executor(None, classify_news, company_name, rss_news)
        exec_news = []
    else:
        exec_news = await fetch_exec_hire_news(company_name)

    loop2 = asyncio.get_event_loop()
    if job_posts:
        from job_posts import extract_posts_with_gemini
        job_posts = await loop2.run_in_executor(None, extract_posts_with_gemini, job_posts)

    concepts: list[str] = []
    for p in job_posts:
        concepts.extend(p.get("concepts") or [])
    concepts = list(dict.fromkeys(c.lower() for c in concepts if c))

    merged_tech = list(dict.fromkeys((result.tech_stack or []) + tech_stack))

    # Step 3 — Re-embed with richer text
    embedding: Optional[list[float]] = await loop.run_in_executor(
        None,
        embed_company,
        result.company_summary,
        description,
        industry,
        merged_tech,
        result.recent_launches,
    )

    # Step 4 — build search_tsv text (Postgres will convert to tsvector via to_tsvector())
    tsv_text = build_search_tsv(
        company_summary=result.company_summary,
        description=description,
        industry=industry,
        tech_stack=merged_tech,
        raw_meta=raw_meta,
        recent_launches=result.recent_launches,
        known_customers=result.known_customers,
    )
    if concepts:
        tsv_text = f"{tsv_text} {' '.join(concepts)}".strip()

    logger.info(
        f"[{company_name}] Done — signal_score={result.signal_score}, "
        f"signals={len(result.buying_signals)}, hiring={job_board.get('count', 0)}"
    )

    # Headcount priority: Gemini extraction → Serper KG → Apollo people-count proxy.
    headcount = (
        result.headcount
        or serper_kg.get("headcount")
        or (existing_headcount if existing_headcount and existing_headcount > 0 else None)
        or apollo_hc.get("headcount_estimate")
    )

    extra_events = list(news_events)
    extra_events.extend(github_extra_events(github))
    extra_events.extend(hn_extra_events(hn_data))
    extra_events.extend(gov_extra_events(gov_awards))
    extra_events.extend([
        {
            "event_type": "exec_hire",
            "title": h["title"][:500],
            "payload": {"key": h.get("url", h["title"])[:120], "url": h.get("url"), "date": h.get("date")},
            "source": "serper_news",
            "confidence": 0.85,
        }
        for h in (exec_news or [])[:2]
    ])

    return {
        "company_summary":          result.company_summary,
        "buying_signals":           result.buying_signals,
        "signal_score":             result.signal_score,
        "funding_stage":            result.funding_stage,
        "total_raised":             result.total_raised,
        "headcount":                headcount,
        "hiring_roles":             result.hiring_roles or job_board.get("roles", []),
        "hiring_count":             job_board.get("count") or len(result.hiring_roles),
        "tech_stack":               merged_tech,
        "description_embedding":    embedding,
        "tsv_text":                 tsv_text,
        "hq_city":                  result.hq_city,
        "hq_region":                result.hq_region,
        "hq_country":               result.hq_country,
        "signal_enriched_at":       datetime.now(timezone.utc),
        "signal_enrichment_status": "enriched" if result.company_summary else "failed",
        "job_posts":                job_posts,
        "job_source":               job_source,
        "concepts":                 concepts,
        "pricing_model":            result.pricing_model,
        "recent_launches":          result.recent_launches,
        "page_fingerprints":        {
            "pricing": page_fingerprint(pricing_text),
            "changelog": page_fingerprint(changelog_text),
        },
        "baseline_fingerprints":    {
            "pricing": baseline_pricing_fp,
        } if baseline_pricing_fp else {},
        "github_org":               github.get("org"),
        "extra_events":             extra_events,
    }
