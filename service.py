"""
Discovery scrape orchestration with checkpoint-based crash recovery.

Flow per run:
  1. Find the latest incomplete run in discovery_progress, or create a new one.
  2. For each profile (resuming from checkpoint if available):
     a. Paginate Apollo (CEO filter → ~1 org per result).
     b. After every page: commit companies to DB + update checkpoint.
     c. On completion: mark profile done in checkpoint.
  3. Mark the run as completed.

Crash recovery:
  If the process dies on page 247 of "healthcare", the next invocation reads the
  checkpoint, skips "devtools" and "operations" (already completed), and resumes
  "healthcare" starting at page 248.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import select, func

import settings as cfg
from database import get_session
from models import DiscoveryCompany, DiscoveryProgress
from profiles import ICP_PROFILES
from apollo_client import ApolloRateLimiter, paginate_profile
from ddg_client import lookup_domain

logger = logging.getLogger("discovery.service")

LOOKUP_CONCURRENCY = 2   # parallel enrichment lookups per page (each = 2 Gemini calls)
_lookup_sem = asyncio.Semaphore(LOOKUP_CONCURRENCY)


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_or_create_run(db) -> DiscoveryProgress:
    """
    Return the most recent incomplete run, or create a fresh one.
    A run is "incomplete" if status == "running" and started within the last 7 days
    (guards against zombie runs from very old crashes).
    """
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    existing = db.exec(
        select(DiscoveryProgress)
        .where(DiscoveryProgress.status == "running")
        .where(DiscoveryProgress.started_at >= cutoff)
        .order_by(DiscoveryProgress.started_at.desc())
    ).first()

    if existing:
        logger.info(
            f"Resuming run {existing.run_id!r} — "
            f"completed profiles: {existing.profiles_completed}, "
            f"current profile: {existing.current_profile!r} at page {existing.current_page}"
        )
        return existing

    run = DiscoveryProgress(
        run_id=_run_id(),
        status="running",
        profiles_completed=[],
        profiles_failed=[],
        stats={},
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    logger.info(f"New run started: {run.run_id!r}")
    return run


def _save_checkpoint(db, run: DiscoveryProgress, profile_slug: str, page: int) -> None:
    run.current_profile = profile_slug
    run.current_page = page
    run.last_heartbeat = datetime.now(timezone.utc)
    db.add(run)
    db.commit()


def _mark_profile_done(db, run: DiscoveryProgress, slug: str, added: int) -> None:
    completed = list(run.profiles_completed or [])
    if slug not in completed:
        completed.append(slug)
    run.profiles_completed = completed
    run.current_profile = None
    run.current_page = 0
    stats = dict(run.stats or {})
    stats[slug] = added
    run.stats = stats
    run.last_heartbeat = datetime.now(timezone.utc)
    db.add(run)
    db.commit()
    logger.info(f"Profile '{slug}' marked complete — {added} companies added")


def _mark_profile_failed(db, run: DiscoveryProgress, slug: str) -> None:
    failed = list(run.profiles_failed or [])
    if slug not in failed:
        failed.append(slug)
    run.profiles_failed = failed
    run.current_profile = None
    run.current_page = 0
    run.last_heartbeat = datetime.now(timezone.utc)
    db.add(run)
    db.commit()
    logger.warning(f"Profile '{slug}' marked failed after retries exhausted")


def _mark_run_complete(db, run: DiscoveryProgress) -> None:
    run.status = "completed"
    run.completed_at = datetime.now(timezone.utc)
    db.add(run)
    db.commit()
    logger.info(f"Run {run.run_id!r} completed — stats: {run.stats}")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_known_names(db) -> set[str]:
    rows = db.exec(select(DiscoveryCompany.apollo_org_name)).all()
    return {r.strip().lower() for r in rows if r}


def _norm(name: str) -> str:
    return name.strip().lower()


def _add_profile_to_existing(db, name_lower: str, slug: str) -> bool:
    """
    If this company is already in the DB, append the profile slug to
    source_profiles and return True. Returns False if not found.
    """
    existing = db.exec(
        select(DiscoveryCompany).where(
            func.lower(DiscoveryCompany.apollo_org_name) == name_lower
        )
    ).first()
    if not existing:
        return False
    profiles = list(existing.source_profiles or [])
    if slug not in profiles:
        profiles.append(slug)
        existing.source_profiles = profiles
        existing.last_seen_at = datetime.now(timezone.utc)
        db.add(existing)
    return True


# ── Core scrape logic ─────────────────────────────────────────────────────────

async def _scrape_profile(
    run: DiscoveryProgress,
    profile: dict,
    limiter: ApolloRateLimiter,
    known_names: set[str],
) -> int:
    """
    Scrape one profile end-to-end with checkpoint saves after every page.
    Returns number of new companies added.
    """
    slug = profile["slug"]
    employee_range = ",".join(profile["filters"].get("organization_num_employees_ranges", []))

    # Determine resume page
    resume_from = 1
    if run.current_profile == slug and run.current_page > 0:
        resume_from = run.current_page + 1
        logger.info(f"[{slug}] Resuming from page {resume_from}")

    pages_data = await paginate_profile(
        api_key=cfg.TELLORA_APOLLO_API_KEY,
        profile=profile,
        limiter=limiter,
        start_page=resume_from,
        max_pages=cfg.MAX_PAGES_PER_PROFILE,
    )

    if not pages_data:
        logger.info(f"[{slug}] No pages returned")
        return 0

    total_added = 0

    with get_session() as db:
        # Reload run inside this session
        run_db = db.get(DiscoveryProgress, run.id)

        for page, org_names in pages_data:
            new_for_page: list[DiscoveryCompany] = []
            seen_in_page: set[str] = set()

            # Filter to only new companies before hitting Jina
            new_companies: list[tuple[str, str]] = []  # (org_name, ceo_first_name)
            for org_name, ceo_first_name in org_names:
                norm = _norm(org_name)
                if not norm or norm in known_names or norm in seen_in_page:
                    continue
                seen_in_page.add(norm)
                if _add_profile_to_existing(db, norm, slug):
                    known_names.add(norm)
                    continue
                new_companies.append((org_name, ceo_first_name))

            # Concurrent enrichment lookups (5 at a time)
            async def _resolve(org_name: str, ceo_first_name: str) -> tuple[str, dict]:
                async with _lookup_sem:
                    return org_name, await lookup_domain(org_name, ceo_first_name)

            lookup_results = await asyncio.gather(*[_resolve(n, c) for n, c in new_companies])

            for name, enrichment in lookup_results:
                norm = _norm(name)
                domain = enrichment.get("domain")

                # Check domain collision (same company found by different profile)
                if domain:
                    domain_clash = db.exec(
                        select(DiscoveryCompany).where(DiscoveryCompany.domain == domain)
                    ).first()
                    if domain_clash:
                        profiles = list(domain_clash.source_profiles or [])
                        if slug not in profiles:
                            profiles.append(slug)
                            domain_clash.source_profiles = profiles
                            db.add(domain_clash)
                        known_names.add(norm)
                        continue

                company = DiscoveryCompany(
                    apollo_org_name=name,
                    name=name,
                    domain=domain or None,
                    website_url=enrichment.get("website_url"),
                    description=enrichment.get("description"),
                    industry=enrichment.get("industry"),
                    description_embedding=enrichment.get("description_embedding"),
                    employee_range=employee_range or None,
                    source_profiles=[slug],
                    domain_resolved=bool(domain),
                    enrichment_status="pending",
                    raw_meta={
                        "keywords": enrichment.get("keywords"),
                        "use_case": enrichment.get("use_case"),
                    } if enrichment else None,
                )
                new_for_page.append(company)
                known_names.add(norm)

            for company in new_for_page:
                db.add(company)

            db.commit()
            total_added += len(new_for_page)

            # Save checkpoint after every page
            _save_checkpoint(db, run_db, slug, page)

            if total_added % 500 == 0 and total_added > 0:
                logger.info(f"[{slug}] Running total: {total_added} new companies")

        _mark_profile_done(db, run_db, slug, total_added)

    return total_added


# ── Entry point ───────────────────────────────────────────────────────────────

async def run_discovery_scrape() -> dict[str, int]:
    """
    Full discovery pipeline with checkpoint-based crash recovery.
    Safe to call multiple times — will resume from last checkpoint.
    Returns stats dict per profile plus a "total" key.
    """
    limiter = ApolloRateLimiter(min_interval=1.1)

    with get_session() as db:
        run = _load_or_create_run(db)
        run_id = run.id
        completed = set(run.profiles_completed or [])
        failed = set(run.profiles_failed or [])

    # Load all known company names once upfront (avoids per-company SELECT)
    with get_session() as db:
        known_names = _load_known_names(db)
    logger.info(f"Known companies in DB: {len(known_names)}")

    stats: dict[str, int] = {}

    for profile in ICP_PROFILES:
        slug = profile["slug"]

        if slug in completed:
            logger.info(f"[{slug}] Already completed this run — skipping")
            continue

        logger.info(f"[{slug}] Starting profile: {profile['label']}")
        try:
            added = await _scrape_profile(
                run=_get_run(run_id),
                profile=profile,
                limiter=limiter,
                known_names=known_names,
            )
            stats[slug] = added
        except Exception as exc:
            logger.error(f"[{slug}] Profile failed: {exc}", exc_info=True)
            with get_session() as db:
                run_db = db.get(DiscoveryProgress, run_id)
                _mark_profile_failed(db, run_db, slug)
            stats[slug] = 0

        # Brief pause between profiles to spread Apollo hourly usage
        await asyncio.sleep(10)

    with get_session() as db:
        run_db = db.get(DiscoveryProgress, run_id)
        _mark_run_complete(db, run_db)

    stats["total"] = sum(v for k, v in stats.items() if k != "total")
    logger.info(f"Scrape complete — {stats}")
    return stats


def _get_run(run_id: str) -> DiscoveryProgress:
    """Load the progress row by PK (needed to pass into sub-functions with fresh sessions)."""
    with get_session() as db:
        run = db.get(DiscoveryProgress, run_id)
        if not run:
            raise RuntimeError(f"Run {run_id!r} not found in DB")
        # Detach from session so it can be passed around
        db.expunge(run)
        return run
