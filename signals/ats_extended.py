"""
Fetchers for iCIMS, JazzHR, and Rippling job boards.

Adapted from the jobhive / ats-scrapers project (MIT).
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import re
from typing import Optional
from urllib.parse import urlencode

import httpx

from signals.http_fetch import fetch_html

logger = logging.getLogger("discovery.ats_extended")

_MAX_ICIMS_PAGES = 4
_JAZZHR_DETAIL_LIMIT = 6
_RIPPLING_DETAIL_LIMIT = 12

# --- iCIMS -------------------------------------------------------------------

_ICIMS_JOB_CARD_RE = re.compile(
    r'<li[^>]+class="[^"]*iCIMS_JobCardItem[^"]*"[^>]*>(?P<body>.*?)</li>',
    re.DOTALL | re.IGNORECASE,
)
_ICIMS_JOB_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(?P<href>https?://[^"]*?/jobs/(?P<id>\d+)/[^"]*?/job[^"]*)"[^>]*'
    r'class="[^"]*iCIMS_Anchor[^"]*"[^>]*>(?P<inner>.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
_ICIMS_TITLE_RE = re.compile(r"<h3[^>]*>(?P<title>.*?)</h3>", re.DOTALL | re.IGNORECASE)
_ICIMS_LOCATION_RE = re.compile(
    r'<span[^>]+class="[^"]*sr-only[^"]*field-label[^"]*"[^>]*>\s*Job Locations\s*</span>'
    r"\s*<span[^>]*>\s*(?P<loc>[^<]*?)\s*</span>",
    re.DOTALL | re.IGNORECASE,
)
_ICIMS_DESC_RE = re.compile(
    r'<div[^>]+class="[^"]*col-xs-12[^"]*description[^"]*"[^>]*>(?P<desc>.*?)</div>',
    re.DOTALL | re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")


def icims_base_url(slug: str) -> str:
    slug = slug.strip().rstrip("/")
    if slug.startswith(("http://", "https://")):
        return slug.rstrip("/")
    if slug.startswith(("careers-", "uscareers-")):
        return f"https://{slug}.icims.com"
    return f"https://careers-{slug}.icims.com"


def _strip_tags(text: str) -> str:
    cleaned = _TAG_RE.sub(" ", text or "")
    cleaned = html_lib.unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _parse_icims_page(html_text: str, *, max_posts: int) -> list[dict]:
    posts: list[dict] = []
    seen: set[str] = set()
    for card in _ICIMS_JOB_CARD_RE.finditer(html_text):
        body = card.group("body")
        anchor = _ICIMS_JOB_ANCHOR_RE.search(body)
        if not anchor:
            continue
        job_id = anchor.group("id")
        if job_id in seen:
            continue
        seen.add(job_id)
        title_match = _ICIMS_TITLE_RE.search(anchor.group("inner"))
        if not title_match:
            continue
        title = _strip_tags(title_match.group("title"))
        if not title:
            continue
        loc_match = _ICIMS_LOCATION_RE.search(body)
        location = _strip_tags(loc_match.group("loc")) if loc_match else ""
        desc_match = _ICIMS_DESC_RE.search(body)
        body_text = _strip_tags(desc_match.group("desc")) if desc_match else ""
        posts.append({
            "external_id": job_id,
            "title": title,
            "location": location,
            "body_text": body_text,
            "source": "icims",
            "absolute_url": html_lib.unescape(anchor.group("href")),
        })
        if len(posts) >= max_posts:
            break
    return posts


async def fetch_icims(
    client: httpx.AsyncClient,
    slug: str,
    *,
    max_posts: int = 12,
) -> list[dict]:
    base = icims_base_url(slug)
    posts: list[dict] = []
    seen_ids: set[str] = set()
    for page in range(_MAX_ICIMS_PAGES):
        page_url = f"{base}/jobs/search?{urlencode({'ss': '1', 'pr': page, 'in_iframe': '1'})}"
        result = await fetch_html(
            client,
            page_url,
            timeout=30.0,
            extra_headers={"Accept": "text/html"},
        )
        if result.status_code == 404:
            break
        if result.status_code != 200:
            logger.debug(
                "[icims] %s page %s returned %s (via %s)",
                slug, page, result.status_code, result.via,
            )
            break
        page_posts = _parse_icims_page(result.text, max_posts=max_posts - len(posts))
        new_posts = [p for p in page_posts if p["external_id"] not in seen_ids]
        if not new_posts:
            break
        for p in new_posts:
            seen_ids.add(p["external_id"])
        posts.extend(new_posts)
        if len(posts) >= max_posts:
            break
    return posts[:max_posts]


# --- JazzHR ------------------------------------------------------------------

_JAZZHR_ROW_RE = re.compile(
    r'<tr\s+id="row_job_[^"]+"[^>]*>(?P<body>.*?)</tr>',
    re.DOTALL | re.IGNORECASE,
)
_JAZZHR_TITLE_RE = re.compile(
    r'<a[^>]+class="[^"]*job_title_link[^"]*"[^>]+'
    r'href="/apply/jobs/details/(?P<id>[A-Za-z0-9_-]+)[^"]*"[^>]*>'
    r"(?P<title>.*?)</a>",
    re.DOTALL | re.IGNORECASE,
)
_JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]+?)</script>',
    re.IGNORECASE,
)


def _jazzhr_listing_url(slug: str) -> str:
    return f"https://{slug}.applytojob.com/apply/jobs"


def _jazzhr_detail_url(slug: str, job_id: str) -> str:
    return f"https://{slug}.applytojob.com/apply/jobs/details/{job_id}"


def _parse_jazzhr_listing(html_text: str, slug: str) -> list[dict]:
    posts: list[dict] = []
    seen: set[str] = set()
    for row in _JAZZHR_ROW_RE.finditer(html_text):
        body = row.group("body")
        title_match = _JAZZHR_TITLE_RE.search(body)
        if not title_match:
            continue
        job_id = title_match.group("id")
        if job_id in seen:
            continue
        seen.add(job_id)
        title = _strip_tags(title_match.group("title"))
        if not title:
            continue
        tds = re.findall(r"<td[^>]*>(.*?)</td>", body, re.DOTALL | re.IGNORECASE)
        location = _strip_tags(tds[-1]) if len(tds) >= 2 else ""
        posts.append({
            "external_id": job_id,
            "title": title,
            "location": location,
            "body_text": "",
            "source": "jazzhr",
            "absolute_url": _jazzhr_detail_url(slug, job_id),
        })
    return posts


def _description_from_jsonld(html_text: str) -> str:
    for match in _JSON_LD_RE.finditer(html_text):
        try:
            data = json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("@type") != "JobPosting":
                continue
            desc = node.get("description")
            if isinstance(desc, str) and desc.strip():
                return _strip_tags(desc)
    return ""


async def _enrich_jazzhr_detail(
    client: httpx.AsyncClient,
    slug: str,
    post: dict,
) -> None:
    result = await fetch_html(client, post["absolute_url"], timeout=30.0)
    if result.status_code != 200:
        return
    post["body_text"] = _description_from_jsonld(result.text)


async def fetch_jazzhr(
    client: httpx.AsyncClient,
    slug: str,
    *,
    max_posts: int = 12,
) -> list[dict]:
    url = _jazzhr_listing_url(slug)
    result = await fetch_html(client, url, timeout=30.0)
    if result.status_code != 200:
        logger.debug("[jazzhr] %s returned %s (via %s)", slug, result.status_code, result.via)
        return []
    posts = _parse_jazzhr_listing(result.text, slug)[:max_posts]
    if not posts:
        return []
    for post in posts[:_JAZZHR_DETAIL_LIMIT]:
        await _enrich_jazzhr_detail(client, slug, post)
    return posts


# --- Rippling ----------------------------------------------------------------

_RIPPLING_LIST = "https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs"
_RIPPLING_DETAIL = "https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs/{job_id}"


def _rippling_location(item: dict) -> str:
    loc = item.get("workLocation") or item.get("location") or {}
    if isinstance(loc, str):
        return loc.strip()
    if isinstance(loc, dict):
        for key in ("displayName", "label", "city", "country"):
            val = loc.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return ""


def _rippling_description(detail: dict) -> str:
    desc_obj = detail.get("description")
    if not isinstance(desc_obj, dict):
        return ""
    parts: list[str] = []
    for key in ("role", "company"):
        html = desc_obj.get(key)
        if isinstance(html, str) and html.strip():
            parts.append(_strip_tags(html))
    return "\n\n".join(parts)


async def _rippling_list(client: httpx.AsyncClient, slug: str) -> list[dict]:
    try:
        resp = await client.get(
            _RIPPLING_LIST.format(slug=slug),
            headers={"Accept": "application/json"},
        )
    except httpx.HTTPError as exc:
        logger.debug("[rippling] %s list failed: %s", slug, exc)
        return []
    if resp.status_code != 200:
        logger.debug("[rippling] %s list returned %s", slug, resp.status_code)
        return []
    payload = resp.json()
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("jobs") or []
    elif isinstance(payload, list):
        items = payload
    else:
        items = []
    posts: list[dict] = []
    for item in items[:_RIPPLING_DETAIL_LIMIT]:
        if not isinstance(item, dict):
            continue
        job_id = str(item.get("uuid") or item.get("id") or "")
        if not job_id:
            continue
        url = (
            item.get("url")
            or item.get("hostedUrl")
            or f"https://ats.rippling.com/{slug}/jobs/{job_id}"
        )
        posts.append({
            "external_id": job_id,
            "title": item.get("name") or item.get("title") or "",
            "location": _rippling_location(item),
            "body_text": "",
            "source": "rippling",
            "absolute_url": url,
            "organization_name": slug,
        })
    return posts


async def fetch_rippling(
    client: httpx.AsyncClient,
    slug: str,
    *,
    max_posts: int = 12,
) -> list[dict]:
    posts = await _rippling_list(client, slug)
    if not posts:
        return []
    posts = posts[:max_posts]
    for post in posts:
        job_id = post["external_id"]
        try:
            resp = await client.get(
                _RIPPLING_DETAIL.format(slug=slug, job_id=job_id),
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError:
            continue
        if resp.status_code != 200:
            continue
        try:
            detail = resp.json()
        except ValueError:
            continue
        post["body_text"] = _rippling_description(detail)
        org = detail.get("companyName") or detail.get("company")
        if isinstance(org, str) and org.strip():
            post["organization_name"] = org.strip()
    return posts
