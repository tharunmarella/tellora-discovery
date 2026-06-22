"""
Job-post ingestion — fetch ATS bodies, Gemini-extract concepts, persist to DB.
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
from signals.constants import GREENHOUSE_API, HTTP_TIMEOUT, LEVER_API
from signals.name_match import slug_variants

logger = logging.getLogger("discovery.job_posts")

_MAX_BODY_CHARS = 4000
_MAX_POSTS_EXTRACT = 12

ASHBY_API = "https://api.ashbyhq.com/posting-api/job-board/{slug}"
SMARTRECRUITERS_LIST = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"
SMARTRECRUITERS_DETAIL = "https://api.smartrecruiters.com/v1/companies/{slug}/postings/{post_id}"
WORKABLE_WIDGET = "https://apply.workable.com/api/v1/widget/accounts/{slug}"
WORKABLE_JOB = "https://apply.workable.com/api/v1/jobs/{shortcode}"


def _strip_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_lib.unescape(s or ""))).strip()


async def fetch_job_board_posts(
    company_name: str,
    domain: Optional[str] = None,
) -> tuple[list[dict], str]:
    """
    Fetch open job posts with bodies from Greenhouse or Lever.
    Returns (posts, source).
    """
    variants = slug_variants(company_name, domain)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        for slug in variants:
            try:
                resp = await client.get(
                    GREENHOUSE_API.format(slug=slug),
                    params={"content": "true"},
                )
                if resp.status_code == 200:
                    jobs = resp.json().get("jobs", [])
                    if jobs:
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
                            })
                        return posts, "greenhouse"
            except Exception:
                pass

        for slug in variants:
            try:
                resp = await client.get(LEVER_API.format(slug=slug))
                if resp.status_code == 200:
                    jobs = resp.json()
                    if isinstance(jobs, list) and jobs:
                        posts = []
                        for j in jobs:
                            desc = _strip_html(
                                (j.get("descriptionPlain") or j.get("description") or "")
                            )[:_MAX_BODY_CHARS]
                            posts.append({
                                "external_id": str(j.get("id", "")),
                                "title": j.get("text", ""),
                                "location": (j.get("categories") or {}).get("location", ""),
                                "body_text": desc,
                                "source": "lever",
                            })
                        return posts, "lever"
            except Exception:
                pass

        for slug in variants:
            try:
                resp = await client.get(ASHBY_API.format(slug=slug))
                if resp.status_code == 200:
                    jobs = resp.json().get("jobs", [])
                    if jobs:
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
                            })
                        return posts, "ashby"
            except Exception:
                pass

        for slug in variants:
            try:
                resp = await client.get(
                    SMARTRECRUITERS_LIST.format(slug=slug),
                    params={"limit": _MAX_POSTS_EXTRACT},
                )
                if resp.status_code != 200:
                    continue
                listings = resp.json().get("content", [])
                if not listings:
                    continue
                posts = []
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
                    })
                if posts:
                    return posts, "smartrecruiters"
            except Exception:
                pass

        for slug in variants:
            try:
                resp = await client.get(WORKABLE_WIDGET.format(slug=slug))
                if resp.status_code != 200:
                    continue
                jobs = resp.json().get("jobs", [])
                if not jobs:
                    continue
                posts = []
                for j in jobs[:_MAX_POSTS_EXTRACT]:
                    shortcode = j.get("shortcode")
                    if not shortcode:
                        continue
                    detail_resp = await client.get(WORKABLE_JOB.format(shortcode=shortcode))
                    if detail_resp.status_code != 200:
                        continue
                    detail = detail_resp.json()
                    body = _strip_html(
                        detail.get("description", "") or detail.get("full_description", "")
                    )[:_MAX_BODY_CHARS]
                    posts.append({
                        "external_id": str(shortcode),
                        "title": detail.get("title") or j.get("title", ""),
                        "location": detail.get("location", {}).get("location_str", "")
                        if isinstance(detail.get("location"), dict)
                        else detail.get("location", ""),
                        "body_text": body,
                        "source": "workable",
                    })
                if posts:
                    return posts, "workable"
            except Exception:
                pass

    return [], "none"


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
