"""
Job-post ingestion — fetch ATS bodies, Gemini-extract concepts, persist to DB.

Slug resolution tiers:
  0. Cached ats_board on discovery_company (or jobhive pre-enrich lookup)
  1. ATS URL detection from careers/homepage HTML
  2. Optional Serper site: search fallback
  3. slug_variants guess (last resort)
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

import settings as cfg
from llm import get_router, retry_llm, strip_json_fences
from signals.ats_detect import (
    detect_ats_candidates,
    extract_slug_from_serp_url,
    serp_site_for_ats,
    validate_board_match,
)
from signals.constants import GREENHOUSE_API, HTTP_TIMEOUT, LEVER_API
from signals.ats_extended import fetch_icims, fetch_jazzhr, fetch_rippling
from signals.name_match import slug_variants

logger = logging.getLogger("discovery.job_posts")

_MAX_BODY_CHARS = 4000
_MAX_POSTS_EXTRACT = 12

ASHBY_API = "https://api.ashbyhq.com/posting-api/job-board/{slug}"
SMARTRECRUITERS_LIST = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"
SMARTRECRUITERS_DETAIL = "https://api.smartrecruiters.com/v1/companies/{slug}/postings/{post_id}"
WORKABLE_WIDGET = "https://apply.workable.com/api/v1/widget/accounts/{slug}"

_ATS_PRIMARY_ORDER = ("greenhouse", "lever", "ashby", "smartrecruiters", "workable")
_ATS_EXTENDED_ORDER = ("rippling", "jazzhr", "icims")
_ATS_ORDER = _ATS_PRIMARY_ORDER + _ATS_EXTENDED_ORDER


def _strip_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_lib.unescape(s or ""))).strip()


def _log_fetch_miss(ats: str, slug: str, status: Optional[int] = None, exc: Optional[Exception] = None) -> None:
    if exc is not None:
        logger.debug("[job_posts] %s/%s failed: %s", ats, slug, exc)
    elif status is not None:
        logger.debug("[job_posts] %s/%s returned %s", ats, slug, status)


async def _fetch_greenhouse(client: httpx.AsyncClient, slug: str) -> list[dict]:
    resp = await client.get(GREENHOUSE_API.format(slug=slug), params={"content": "true"})
    if resp.status_code != 200:
        _log_fetch_miss("greenhouse", slug, status=resp.status_code)
        return []
    jobs = resp.json().get("jobs", [])
    if not jobs:
        return []
    posts = []
    for j in jobs:
        body = _strip_html(j.get("content", ""))[:_MAX_BODY_CHARS]
        loc = (j.get("location") or {}).get("name", "")
        posts.append({
            "external_id": str(j.get("id", j.get("internal_job_id", ""))),
            "title": j.get("title", ""),
            "location": loc,
            "body_text": body,
            "source": "greenhouse",
            "absolute_url": j.get("absolute_url", ""),
        })
    return posts


async def _fetch_lever(client: httpx.AsyncClient, slug: str) -> list[dict]:
    resp = await client.get(LEVER_API.format(slug=slug))
    if resp.status_code != 200:
        _log_fetch_miss("lever", slug, status=resp.status_code)
        return []
    jobs = resp.json()
    if not isinstance(jobs, list) or not jobs:
        return []
    org = (jobs[0].get("categories") or {}).get("team", "")
    posts = []
    for j in jobs:
        desc = _strip_html((j.get("descriptionPlain") or j.get("description") or ""))[:_MAX_BODY_CHARS]
        posts.append({
            "external_id": str(j.get("id", "")),
            "title": j.get("text", ""),
            "location": (j.get("categories") or {}).get("location", ""),
            "body_text": desc,
            "source": "lever",
            "organization_name": j.get("company") or org,
        })
    return posts


async def _fetch_ashby(client: httpx.AsyncClient, slug: str) -> list[dict]:
    resp = await client.get(ASHBY_API.format(slug=slug))
    if resp.status_code != 200:
        _log_fetch_miss("ashby", slug, status=resp.status_code)
        return []
    data = resp.json()
    jobs = data.get("jobs", [])
    if not jobs:
        return []
    org = (data.get("organization") or {}).get("name", "")
    posts = []
    for j in jobs:
        body = _strip_html(j.get("descriptionHtml", ""))[:_MAX_BODY_CHARS]
        loc = j.get("location") or ""
        if isinstance(loc, dict):
            loc = loc.get("name", "")
        posts.append({
            "external_id": str(j.get("id", "")),
            "title": j.get("title", ""),
            "location": loc,
            "body_text": body,
            "source": "ashby",
            "organization_name": org,
        })
    return posts


async def _fetch_smartrecruiters(client: httpx.AsyncClient, slug: str) -> list[dict]:
    resp = await client.get(
        SMARTRECRUITERS_LIST.format(slug=slug),
        params={"limit": _MAX_POSTS_EXTRACT},
    )
    if resp.status_code != 200:
        _log_fetch_miss("smartrecruiters", slug, status=resp.status_code)
        return []
    listings = resp.json().get("content", [])
    if not listings:
        return []
    posts = []
    org_name = ""
    for item in listings[:_MAX_POSTS_EXTRACT]:
        post_id = item.get("id")
        if not post_id:
            continue
        detail_resp = await client.get(
            SMARTRECRUITERS_DETAIL.format(slug=slug, post_id=post_id),
        )
        if detail_resp.status_code != 200:
            continue
        detail = detail_resp.json()
        if not org_name:
            org_name = (detail.get("company") or {}).get("name", "")
        body = _strip_html(
            detail.get("jobAd", {}).get("sections", {}).get("jobDescription", {}).get("text", "")
            or detail.get("jobAd", {}).get("jobDescription", "")
            or ""
        )[:_MAX_BODY_CHARS]
        loc = (detail.get("location") or {}).get("city", "")
        posts.append({
            "external_id": str(post_id),
            "title": detail.get("name") or item.get("name", ""),
            "location": loc,
            "body_text": body,
            "source": "smartrecruiters",
            "organization_name": org_name,
        })
    return posts


async def _fetch_workable(client: httpx.AsyncClient, slug: str) -> list[dict]:
    resp = await client.get(
        WORKABLE_WIDGET.format(slug=slug),
        params={"details": "true"},
    )
    if resp.status_code != 200:
        _log_fetch_miss("workable", slug, status=resp.status_code)
        return []
    data = resp.json()
    jobs = data.get("jobs", [])
    if not jobs:
        return []
    company = data.get("name") or ""
    posts = []
    for j in jobs[:_MAX_POSTS_EXTRACT]:
        body = _strip_html(j.get("description", "") or j.get("full_description", ""))[:_MAX_BODY_CHARS]
        loc = j.get("location", {})
        if isinstance(loc, dict):
            loc = loc.get("location_str", "") or loc.get("city", "")
        posts.append({
            "external_id": str(j.get("shortcode", j.get("id", ""))),
            "title": j.get("title", ""),
            "location": loc,
            "body_text": body,
            "source": "workable",
            "organization_name": company,
        })
    return posts


    return posts


async def _fetch_rippling(client: httpx.AsyncClient, slug: str) -> list[dict]:
    return await fetch_rippling(client, slug, max_posts=_MAX_POSTS_EXTRACT)


async def _fetch_jazzhr(client: httpx.AsyncClient, slug: str) -> list[dict]:
    return await fetch_jazzhr(client, slug, max_posts=_MAX_POSTS_EXTRACT)


async def _fetch_icims(client: httpx.AsyncClient, slug: str) -> list[dict]:
    return await fetch_icims(client, slug, max_posts=_MAX_POSTS_EXTRACT)


_FETCHERS = {
    "greenhouse": _fetch_greenhouse,
    "lever": _fetch_lever,
    "ashby": _fetch_ashby,
    "smartrecruiters": _fetch_smartrecruiters,
    "workable": _fetch_workable,
    "rippling": _fetch_rippling,
    "jazzhr": _fetch_jazzhr,
    "icims": _fetch_icims,
}


async def _fetch_single_ats(
    client: httpx.AsyncClient,
    ats: str,
    slug: str,
) -> list[dict]:
    fetcher = _FETCHERS.get(ats)
    if not fetcher:
        return []
    try:
        return await fetcher(client, slug)
    except Exception as exc:
        _log_fetch_miss(ats, slug, exc=exc)
        return []


async def verify_ats_slug(
    company_name: str,
    domain: Optional[str],
    ats: str,
    slug: str,
) -> bool:
    """Live-fetch an ATS board and run validate_board_match (for slug imports)."""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        posts = await _fetch_single_ats(client, ats, slug)
        if not posts:
            return False
        return validate_board_match(
            posts,
            company_name=company_name,
            domain=domain,
            source=ats,
            slug_inferred=True,
        )


def _build_candidate_list(
    company_name: str,
    domain: Optional[str],
    *,
    ats_board: Optional[dict],
    careers_html: str,
    serp_candidates: list[tuple[str, str]],
) -> list[tuple[str, str, bool]]:
    """Return (ats, slug, slug_inferred) in priority order."""
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, bool]] = []

    def _add(ats: str, slug: str, inferred: bool) -> None:
        key = (ats, slug)
        if key in seen:
            return
        seen.add(key)
        out.append((ats, slug, inferred))

    if ats_board and ats_board.get("source") and ats_board.get("slug"):
        _add(str(ats_board["source"]), str(ats_board["slug"]), True)

    for ats, slug in detect_ats_candidates(careers_html):
        _add(ats, slug, True)

    for ats, slug in serp_candidates:
        _add(ats, slug, True)

    for slug in slug_variants(company_name, domain):
        for ats in _ATS_PRIMARY_ORDER:
            _add(ats, slug, False)

    stem = (domain or "").lower().replace("www.", "").split(".")[0]
    if stem and len(stem) >= 2:
        for ats in _ATS_EXTENDED_ORDER:
            _add(ats, stem, False)

    return out


async def _discover_slugs_via_serper(company_name: str) -> list[tuple[str, str]]:
    if not cfg.ATS_SERP_FALLBACK or not cfg.SERPER_API_KEY or not company_name:
        return []

    found: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    headers = {"X-API-KEY": cfg.SERPER_API_KEY, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for ats in _ATS_ORDER:
            site = serp_site_for_ats(ats)
            if not site:
                continue
            query = f'site:{site} "{company_name}"'
            try:
                resp = await client.post(
                    "https://google.serper.dev/search",
                    headers=headers,
                    json={"q": query, "num": 5},
                )
                if resp.status_code != 200:
                    logger.debug("[job_posts] Serper fallback %s: HTTP %s", ats, resp.status_code)
                    continue
                for item in resp.json().get("organic", [])[:5]:
                    slug = extract_slug_from_serp_url(ats, item.get("link", ""))
                    if slug and (ats, slug) not in seen:
                        seen.add((ats, slug))
                        found.append((ats, slug))
                        break
            except Exception as exc:
                logger.debug("[job_posts] Serper fallback %s failed: %s", ats, exc)

    return found


async def fetch_cached_ats_board_posts(
    ats_board: dict,
) -> tuple[list[dict], str]:
    """Cheap ATS fetch using a cached ats_board slug only (no Serper / discovery)."""
    source = str(ats_board.get("source") or "")
    slug = str(ats_board.get("slug") or "")
    if not source or not slug:
        return [], "none"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        posts = await _fetch_single_ats(client, source, slug)
    return posts, source


async def fetch_job_board_posts(
    company_name: str,
    domain: Optional[str] = None,
    *,
    ats_board: Optional[dict] = None,
    careers_html: str = "",
) -> tuple[list[dict], str, Optional[dict]]:
    """
    Fetch open job posts with bodies from ATS public APIs.
    Returns (posts, source, resolved_ats_board for caching).
    """
    from signals.jobhive_import import lookup_ats_board_from_jobhive

    ats_board = lookup_ats_board_from_jobhive(
        company_name,
        domain,
        existing=ats_board,
    )

    serp_candidates = await _discover_slugs_via_serper(company_name)
    candidates = _build_candidate_list(
        company_name,
        domain,
        ats_board=ats_board,
        careers_html=careers_html,
        serp_candidates=serp_candidates,
    )

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        for ats, slug, inferred in candidates:
            posts = await _fetch_single_ats(client, ats, slug)
            if not posts:
                continue
            if not validate_board_match(
                posts,
                company_name=company_name,
                domain=domain,
                source=ats,
                slug_inferred=inferred,
            ):
                logger.info(
                    "[job_posts] Rejected %s/%s for %s — validation failed",
                    ats, slug, company_name,
                )
                continue
            resolved = {
                "source": ats,
                "slug": slug,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            }
            logger.info(
                "[job_posts] %s via %s/%s (%d posts, inferred=%s)",
                company_name, ats, slug, len(posts), inferred,
            )
            return posts, ats, resolved

    return [], "none", None


async def check_job_boards(
    company_name: str,
    domain: Optional[str] = None,
    *,
    ats_board: Optional[dict] = None,
    careers_html: str = "",
) -> dict:
    """Lightweight summary {count, roles, source} for POCs and tests."""
    posts, source, _ = await fetch_job_board_posts(
        company_name,
        domain,
        ats_board=ats_board,
        careers_html=careers_html,
    )
    return {
        "count": len(posts),
        "roles": [p.get("title", "") for p in posts[:10]],
        "source": source,
    }


def extract_posts_with_gemini(posts: list[dict]) -> list[dict]:
    """Batch Gemini extraction for role_family, concepts, tech, initiatives."""
    if not posts or not cfg.GEMINI_API_KEY:
        return posts

    sample = [p for p in posts if p.get("body_text")][: _MAX_POSTS_EXTRACT]
    if not sample:
        return posts

    blocks = []
    for i, p in enumerate(sample):
        blocks.append(
            f"POST {i + 1}\nTITLE: {p['title']}\nBODY: {p.get('body_text', '')[:2200]}"
        )
    prompt = f"""Analyze job postings for B2B sales intelligence. For EACH post extract facts visible in text only.

{chr(10).join(blocks)}

Respond ONLY with valid JSON:
{{"posts": [{{"idx": 1, "role_family": "...", "seniority": "junior|mid|senior|staff|exec",
"concepts": ["..."], "tech": ["..."], "initiatives": ["..."]}}]}}"""

    def _do():
        raw = get_router().complete_text(
            prompt,
            models=get_router().signal_models,
            temperature=0.0,
            json_mode=True,
        )
        data = json.loads(strip_json_fences(raw))
        return data.get("posts", [])

    try:
        extracted = retry_llm(_do)
        by_idx = {e.get("idx", i + 1): e for i, e in enumerate(extracted)}
        for i, p in enumerate(sample):
            ex = by_idx.get(i + 1, {})
            p["role_family"] = ex.get("role_family")
            p["seniority"] = ex.get("seniority")
            p["concepts"] = ex.get("concepts") or []
            p["tech"] = ex.get("tech") or []
            p["initiatives"] = ex.get("initiatives") or []
    except Exception as exc:
        logger.warning(f"Job post Gemini extraction failed: {exc}")

    return posts


def persist_job_posts(session: Session, company_id: str, posts: list[dict], source: str) -> list[str]:
    """
    Upsert job posts, mark closed posts, return aggregated concepts for snapshot.
    """
    if not posts:
        return []

    now = datetime.now(timezone.utc)
    seen_ids: set[str] = set()
    all_concepts: list[str] = []

    for p in posts:
        ext_id = str(p.get("external_id") or "")
        if not ext_id:
            continue
        seen_ids.add(ext_id)
        concepts = p.get("concepts") or []
        all_concepts.extend(concepts)

        session.execute(text("""
            INSERT INTO discovery_job_post
                (id, company_id, external_id, title, location, body_text,
                 role_family, seniority, concepts, tech, initiatives, source,
                 first_seen_at, last_seen_at, closed_at)
            VALUES
                (:id, :company_id, :external_id, :title, :location, :body_text,
                 :role_family, :seniority, CAST(:concepts AS jsonb), CAST(:tech AS jsonb),
                 CAST(:initiatives AS jsonb), :source, :now, :now, NULL)
            ON CONFLICT (company_id, external_id) DO UPDATE SET
                title = EXCLUDED.title,
                location = EXCLUDED.location,
                body_text = COALESCE(EXCLUDED.body_text, discovery_job_post.body_text),
                role_family = COALESCE(EXCLUDED.role_family, discovery_job_post.role_family),
                seniority = COALESCE(EXCLUDED.seniority, discovery_job_post.seniority),
                concepts = COALESCE(EXCLUDED.concepts, discovery_job_post.concepts),
                tech = COALESCE(EXCLUDED.tech, discovery_job_post.tech),
                initiatives = COALESCE(EXCLUDED.initiatives, discovery_job_post.initiatives),
                last_seen_at = :now,
                closed_at = NULL
        """), {
            "id": str(uuid.uuid4()),
            "company_id": company_id,
            "external_id": ext_id,
            "title": p.get("title", ""),
            "location": p.get("location"),
            "body_text": p.get("body_text"),
            "role_family": p.get("role_family"),
            "seniority": p.get("seniority"),
            "concepts": json.dumps(concepts),
            "tech": json.dumps(p.get("tech") or []),
            "initiatives": json.dumps(p.get("initiatives") or []),
            "source": p.get("source") or source,
            "now": now,
        })

    open_rows = session.execute(text("""
        SELECT external_id FROM discovery_job_post
        WHERE company_id = :cid AND closed_at IS NULL
    """), {"cid": company_id}).scalars().all()
    for ext in open_rows:
        if ext not in seen_ids:
            session.execute(text("""
                UPDATE discovery_job_post SET closed_at = :now
                WHERE company_id = :cid AND external_id = :ext
            """), {"cid": company_id, "ext": ext, "now": now})

    return list(dict.fromkeys(c.lower() for c in all_concepts if c))
