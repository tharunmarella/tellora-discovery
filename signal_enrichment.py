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
import json
import logging
import re
import time as _time
from datetime import datetime, timezone
from typing import Optional

import httpx
from pydantic import BaseModel, Field

import settings as cfg

logger = logging.getLogger("discovery.signal")

# ── Constants ──────────────────────────────────────────────────────────────

JINA_READER    = "https://r.jina.ai/"
JINA_SEARCH    = "https://s.jina.ai/"
GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
LEVER_API      = "https://api.lever.co/v0/postings/{slug}?mode=json"

GEMINI_MODEL = "gemini-3.1-flash-lite"
EMBED_MODEL  = "gemini-embedding-001"

_HTTP_TIMEOUT = 8.0
_DOMAIN_CACHE: dict[str, tuple[float, dict]] = {}
_DOMAIN_CACHE_TTL = 3600  # 1 hour

_gemini_client = None


def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=cfg.GEMINI_API_KEY)
    return _gemini_client


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


async def fetch_company_context(domain: str) -> dict:
    """
    Fetch homepage + /about + /careers via Jina Reader.
    Cached per domain for 1 hour. Returns {homepage, about, careers}.
    """
    if not domain:
        return {"homepage": "", "about": "", "careers": ""}

    cached = _DOMAIN_CACHE.get(domain)
    if cached:
        ts, ctx = cached
        if _time.time() - ts < _DOMAIN_CACHE_TTL:
            return ctx
        del _DOMAIN_CACHE[domain]

    base = f"https://{domain}" if not domain.startswith("http") else domain

    homepage_text, about_text, careers_text = await asyncio.gather(
        _jina_read(base, max_chars=3000),
        _jina_read(f"{base}/about", max_chars=2000),
        _jina_read(f"{base}/careers", max_chars=2000),
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

    ctx = {"homepage": homepage_text, "about": about_text, "careers": careers_text}
    _DOMAIN_CACHE[domain] = (_time.time(), ctx)
    logger.info(
        f"Company context for {domain}: "
        f"homepage={len(homepage_text)}c, about={len(about_text)}c, careers={len(careers_text)}c"
    )
    return ctx


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
  "known_customers": ["<customer1>"]
}"""


def _retry_gemini(fn, max_retries: int = 3):
    backoff = 5
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            exc_str = str(exc).lower()
            is_retryable = any(
                s in exc_str for s in ("429", "rate", "quota", "resource_exhausted", "503", "timeout")
            )
            if not is_retryable or attempt == max_retries - 1:
                raise
            logger.warning(f"Gemini retryable error (attempt {attempt+1}): {exc} — backing off {backoff}s")
            _time.sleep(backoff)
            backoff *= 2


def synthesize_company_signals(
    company_name: str,
    homepage_text: str,
    about_text: str,
    careers_text: str,
    tech_stack: list[str],
    job_board: dict,
    funding_news: list[str],
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

    prompt = f"""You are a B2B sales intelligence analyst. Extract factual signals from the evidence below.

STRICT RULES:
1. Only state facts visible in the evidence. Do NOT invent or infer.
2. If a field has no evidence, leave it null / empty list.
3. signal_score: 0=no evidence, 40=some signals, 70=strong buying triggers, 90+=exceptional.
4. company_summary: what the company does, who they sell to, market position. From homepage/about only. null if text is too thin.
5. buying_signals: concrete, time-sensitive triggers only (funding rounds, hiring sprees, product launches, expansions).

COMPANY: {company_name}

TECH STACK:
{tech_line}

JOB BOARD:
{jobs_line}

RECENT FUNDING NEWS:
{news_block}

COMPANY HOMEPAGE:
{homepage_text or "Unavailable"}

COMPANY ABOUT PAGE:
{about_text or "Unavailable"}

COMPANY CAREERS PAGE:
{careers_text or "Unavailable"}

Respond with ONLY valid JSON matching this exact schema. No markdown, no explanation:
{_JSON_SCHEMA}"""

    def _do():
        client = _get_gemini_client()
        resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        raw = resp.text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip().rstrip("`").strip()
        data = json.loads(raw)
        return CompanySignalResult(**{
            k: data.get(k)
            for k in CompanySignalResult.model_fields
        })

    try:
        return _retry_gemini(_do)
    except Exception as exc:
        logger.warning(f"Gemini synthesis failed for {company_name}: {exc}")
        return _EMPTY_SIGNAL


# ── Embedding ──────────────────────────────────────────────────────────────

def embed_company(
    company_summary: Optional[str],
    description: Optional[str],
    industry: Optional[str],
    tech_stack: list[str],
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

    text = " ".join(filter(None, parts)).strip()
    if not text:
        return None

    try:
        from google.genai import types

        def _do():
            client = _get_gemini_client()
            resp = client.models.embed_content(
                model=EMBED_MODEL,
                contents=text,
                config=types.EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768,
                ),
            )
            return resp.embeddings[0].values

        return _retry_gemini(_do)
    except Exception as exc:
        logger.warning(f"Embedding failed: {exc}")
        return None


def build_search_tsv(
    company_summary: Optional[str],
    description: Optional[str],
    industry: Optional[str],
    tech_stack: list[str],
    raw_meta: Optional[dict],
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
    ctx_task     = asyncio.create_task(fetch_company_context(domain))
    jobs_task    = asyncio.create_task(check_job_boards(company_name))
    news_task    = asyncio.create_task(fetch_funding_news(company_name))
    tech_task    = asyncio.create_task(detect_tech_stack(domain))

    ctx, job_board, funding_news, tech_stack = await asyncio.gather(
        ctx_task, jobs_task, news_task, tech_task
    )

    homepage_text = ctx.get("homepage", "")
    about_text    = ctx.get("about", "")
    careers_text  = ctx.get("careers", "")

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
    )

    # Step 3 — Re-embed with richer text
    embedding: Optional[list[float]] = await loop.run_in_executor(
        None,
        embed_company,
        result.company_summary,
        description,
        industry,
        result.tech_stack or tech_stack,
    )

    # Step 4 — build search_tsv text (Postgres will convert to tsvector via to_tsvector())
    tsv_text = build_search_tsv(
        company_summary=result.company_summary,
        description=description,
        industry=industry,
        tech_stack=result.tech_stack or tech_stack,
        raw_meta=raw_meta,
    )

    logger.info(
        f"[{company_name}] Done — signal_score={result.signal_score}, "
        f"signals={len(result.buying_signals)}, hiring={job_board.get('count', 0)}"
    )

    return {
        "company_summary":          result.company_summary,
        "buying_signals":           result.buying_signals,
        "signal_score":             result.signal_score,
        "funding_stage":            result.funding_stage,
        "total_raised":             result.total_raised,
        "headcount":                result.headcount,
        "hiring_roles":             result.hiring_roles or job_board.get("roles", []),
        "hiring_count":             job_board.get("count") or len(result.hiring_roles),
        "tech_stack":               result.tech_stack or tech_stack,
        "description_embedding":    embedding,
        "tsv_text":                 tsv_text,  # passed to runner for to_tsvector() update
        "signal_enriched_at":       datetime.now(timezone.utc),
        "signal_enrichment_status": "enriched" if result.company_summary else "failed",
    }
