"""
GitHub org ingester — free technographics + launch signals.

Resolves a company domain to a GitHub org (verified via the org's blog/website
field), then reads public repos for languages and recently created repos.

  - languages       → merged into tech_stack (better than homepage regex)
  - new public repo → product_launch event (emitted via extra_events)

Auth: optional GITHUB_TOKEN env (5k req/hr). Unauthenticated 60/hr is enough
for watched-account refreshes.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime, timedelta, timezone

import httpx

import settings as cfg

logger = logging.getLogger("discovery.github")

_API = "https://api.github.com"

# domain → (ts, result dict). Negative resolutions cached too (org=None).
_GH_CACHE: dict[str, tuple[float, dict]] = {}
_GH_CACHE_TTL = 24 * 3600

_EMPTY = {"org": None, "languages": [], "new_repos": [], "repo_count": 0}


def _headers() -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if cfg.GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {cfg.GITHUB_TOKEN}"
    return h


async def _resolve_org(client: httpx.AsyncClient, domain: str) -> dict | None:
    """Resolve domain → GitHub org dict, verified via the org's blog field."""
    stem = domain.split(".")[0]

    # Strategy 1: domain stem as org login (covers most companies, 1 request)
    try:
        r = await client.get(f"{_API}/orgs/{stem}")
        if r.status_code == 200:
            detail = r.json()
            if domain in (detail.get("blog") or ""):
                return detail
    except Exception:
        pass

    # Strategy 2: user search fallback
    try:
        r = await client.get(f"{_API}/search/users",
                             params={"q": f"{domain} type:org", "per_page": 3})
        if r.status_code != 200:
            return None
        for cand in r.json().get("items", []):
            detail = (await client.get(cand["url"])).json()
            if domain in (detail.get("blog") or ""):
                return detail
    except Exception:
        pass
    return None


async def fetch_github_signals(domain: str) -> dict:
    """
    Returns {org, languages, new_repos, repo_count}.
      org        : GitHub login or None (no verified match)
      languages  : lowercase language names ordered by frequency
      new_repos  : [{name, stars, created_at}] created in the last 30 days
    Cached per domain for 24h (including negative resolutions).
    """
    if not domain:
        return dict(_EMPTY)

    cached = _GH_CACHE.get(domain)
    if cached and _time.time() - cached[0] < _GH_CACHE_TTL:
        return cached[1]

    result = dict(_EMPTY)
    try:
        async with httpx.AsyncClient(timeout=15, headers=_headers()) as client:
            org = await _resolve_org(client, domain)
            if org:
                result["org"] = org["login"]
                r = await client.get(
                    f"{_API}/orgs/{org['login']}/repos",
                    params={"sort": "pushed", "per_page": 30},
                )
                repos = r.json() if r.status_code == 200 else []

                langs: dict[str, int] = {}
                cutoff = datetime.now(timezone.utc) - timedelta(days=30)
                for repo in repos:
                    if not isinstance(repo, dict):
                        continue
                    if repo.get("language"):
                        lang = repo["language"].lower()
                        langs[lang] = langs.get(lang, 0) + 1
                    try:
                        created = datetime.fromisoformat(
                            repo["created_at"].replace("Z", "+00:00"))
                    except (KeyError, ValueError):
                        continue
                    if created > cutoff and not repo.get("fork"):
                        result["new_repos"].append({
                            "name": repo["name"],
                            "stars": repo.get("stargazers_count", 0),
                            "created_at": repo["created_at"],
                        })
                result["languages"] = sorted(langs, key=langs.get, reverse=True)
                result["repo_count"] = len(repos)
    except Exception as exc:
        logger.warning(f"GitHub signals failed for {domain}: {exc}")
        return dict(_EMPTY)  # don't cache transient failures

    _GH_CACHE[domain] = (_time.time(), result)
    if result["org"]:
        logger.info(
            f"GitHub {domain} → {result['org']}: "
            f"{len(result['languages'])} languages, {len(result['new_repos'])} new repos"
        )
    return result


def github_extra_events(gh: dict) -> list[dict]:
    """Convert new public repos into product_launch extra_events drafts."""
    events = []
    for repo in (gh.get("new_repos") or [])[:3]:
        events.append({
            "event_type": "product_launch",
            "title": f"New public GitHub repo: {repo['name']}"
                     + (f" ({repo['stars']}★)" if repo.get("stars") else ""),
            "payload": {"key": f"gh:{repo['name']}", "repo": repo["name"],
                        "stars": repo.get("stars", 0), "created_at": repo.get("created_at")},
            "source": "github",
            "confidence": 0.7,
        })
    return events
