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
from datetime import datetime, timedelta, timezone

import httpx

import settings as cfg
from signals.cache import TTLLRUCache

logger = logging.getLogger("discovery.github")

_API = "https://api.github.com"

# domain → result dict. Negative resolutions cached too (org=None).
_GH_CACHE: TTLLRUCache[dict] = TTLLRUCache(maxsize=cfg.DOMAIN_CACHE_MAXSIZE, ttl=86400.0)

_EMPTY = {"org": None, "languages": [], "new_repos": [], "repo_count": 0, "new_npm_releases": [], "npm_package_count": 0}


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
    if cached is not None:
        return cached

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

                # npm scope releases (devtools signal) — @{org} search
                # Use a plain client — GitHub Accept headers break the npm registry API.
                npm_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
                org_login = org["login"]
                try:
                    async with httpx.AsyncClient(timeout=15) as npm_client:
                        for query in (f"@{org_login}", f"scope:{org_login}"):
                            nr = await npm_client.get(
                                "https://registry.npmjs.org/-/v1/search",
                                params={"text": query, "size": 30},
                            )
                            if nr.status_code != 200:
                                continue
                            objects = nr.json().get("objects", [])
                            if not objects:
                                continue
                            result["npm_package_count"] = nr.json().get("total", len(objects))
                            seen_pkg: set[str] = set()
                            for obj in objects:
                                pkg = obj.get("package", {})
                                name = pkg.get("name", "")
                                if not name or name in seen_pkg:
                                    continue
                                if not (name.startswith(f"@{org_login}/") or name == org_login):
                                    continue
                                seen_pkg.add(name)
                                version = pkg.get("version", "")
                                date_str = pkg.get("date", "")
                                if not date_str:
                                    continue
                                try:
                                    pub = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                                except ValueError:
                                    continue
                                if pub > npm_cutoff:
                                    result["new_npm_releases"].append({
                                        "name": name,
                                        "version": version,
                                        "date": date_str,
                                    })
                            if result["new_npm_releases"]:
                                break
                except Exception as exc:
                    logger.debug(f"npm search failed for {org_login}: {exc}")
    except Exception as exc:
        logger.warning(f"GitHub signals failed for {domain}: {exc}")
        return dict(_EMPTY)  # don't cache transient failures

    _GH_CACHE.set(domain, result)
    if result["org"]:
        logger.info(
            f"GitHub {domain} → {result['org']}: "
            f"{len(result['languages'])} languages, {len(result['new_repos'])} new repos"
        )
    return result


def github_extra_events(gh: dict) -> list[dict]:
    """Convert new public repos and npm releases into product_launch extra_events."""
    events = []
    for repo in (gh.get("new_repos") or [])[:3]:
        repo_url = f"https://github.com/{gh.get('org')}/{repo['name']}" if gh.get("org") else None
        events.append({
            "event_type": "product_launch",
            "title": f"New public GitHub repo: {repo['name']}"
                     + (f" ({repo['stars']}★)" if repo.get("stars") else ""),
            "payload": {"key": f"gh:{repo['name']}", "repo": repo["name"],
                        "stars": repo.get("stars", 0), "created_at": repo.get("created_at"),
                        "url": repo_url},
            "source": "github",
            "confidence": 0.7,
            "evidence_url": repo_url,
            "event_date": repo.get("created_at"),
        })
    for pkg in (gh.get("new_npm_releases") or [])[:3]:
        pkg_url = f"https://www.npmjs.com/package/{pkg['name']}"
        events.append({
            "event_type": "product_launch",
            "title": f"npm release: {pkg['name']}@{pkg.get('version', '?')}",
            "payload": {
                "key": f"npm:{pkg['name']}@{pkg.get('version', '')}",
                "package": pkg["name"],
                "version": pkg.get("version"),
                "date": pkg.get("date"),
                "url": pkg_url,
            },
            "source": "npm",
            "confidence": 0.6,
            "evidence_url": pkg_url,
            "event_date": pkg.get("date"),
        })
    return events
