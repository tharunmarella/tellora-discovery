"""
SQLModel table definitions for the discovery service.

Two tables:
  - discovery_company   : one row per unique company (shared with backend)
  - discovery_progress  : single-row checkpoint so crashes resume mid-scrape

The backend (tellora-backend) also imports discovery_company and creates the table
at startup. The discovery service creates both tables itself via create_all().
"""

import enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from sqlmodel import SQLModel, Field, Column, String
from sqlalchemy import TIMESTAMP, Text, Boolean, Integer, Float, Index
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, TSVECTOR
from pgvector.sqlalchemy import Vector


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


# ── DiscoveryCompany ───────────────────────────────────────────────────────────

class DiscoveryCompany(SQLModel, table=True):
    """
    One row per unique company found by the Apollo scraper.
    System-level — not tied to any Tellora org.
    """
    __tablename__ = "discovery_company"

    __table_args__ = {"extend_existing": True}

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))

    # From Apollo (only field returned for free)
    apollo_org_name: str = Field(sa_column=Column(String, nullable=False, index=True))

    # Resolved by search + LLM enrichment
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    domain: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    website_url: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    linkedin_url: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    description: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    industry: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))

    # Company profile fields — sourced from Serper KG / Apollo / Gemini
    ceo_name: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    headquarters: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    hq_city: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    hq_region: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    hq_country: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    founded_year: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    funding: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    logo_url: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))

    # Embedding of description + use_case — used for semantic prospect search
    # Populated during discovery scrape via gemini-embedding-001 (768 dims)
    description_embedding: Optional[List[float]] = Field(
        default=None, sa_column=Column(Vector(768), nullable=True)
    )
    location: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    employee_range: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))

    # Which ICP profiles matched this company
    source_profiles: Optional[List[str]] = Field(
        default=None, sa_column=Column(ARRAY(Text), nullable=True)
    )

    # Origin of this row: apollo (weekly scrape) | ora_research | manual | people_upload | sec_edgar
    source: str = Field(
        default="apollo", sa_column=Column(String, nullable=False, server_default="apollo", index=True)
    )

    domain_resolved: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, server_default="false")
    )
    enrichment_status: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default="pending", index=True),
    )

    # ── Signal enrichment fields ────────────────────────────────────────────
    company_summary: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    buying_signals: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    signal_score: Optional[int] = Field(default=None, sa_column=Column(Integer, nullable=True))
    hiring_roles: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    hiring_count: Optional[int] = Field(default=None, sa_column=Column(Integer, nullable=True))
    tech_stack: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    funding_stage: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    total_raised: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    headcount: Optional[int] = Field(default=None, sa_column=Column(Integer, nullable=True))
    signal_enriched_at: Optional[datetime] = Field(default=None, sa_column=Column(TIMESTAMP(timezone=True), nullable=True))
    signal_enrichment_status: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default="pending", index=True),
        description="pending | enriched | failed | skipped",
    )
    search_tsv: Optional[Any] = Field(default=None, sa_column=Column(TSVECTOR, nullable=True))

    raw_meta: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSONB, nullable=True)
    )

    first_seen_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    last_seen_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    created_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )


# ── DiscoveryCompanySnapshot ───────────────────────────────────────────────────

class DiscoveryCompanySnapshot(SQLModel, table=True):
    """Point-in-time observation of company signals — never updated, only appended."""
    __tablename__ = "discovery_company_snapshot"

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))
    company_id: str = Field(sa_column=Column(String, nullable=False, index=True))
    captured_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    )
    hiring_count: Optional[int] = Field(default=None, sa_column=Column(Integer, nullable=True))
    hiring_roles: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    tech_stack: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    funding_stage: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    total_raised: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    headcount: Optional[int] = Field(default=None, sa_column=Column(Integer, nullable=True))
    buying_signals: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    concepts: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    pricing_model: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    page_fingerprints: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    recent_launches: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))


# ── DiscoverySignalEvent ───────────────────────────────────────────────────────

class DiscoverySignalEvent(SQLModel, table=True):
    """Typed, time-aware signal event emitted when company state changes."""
    __tablename__ = "discovery_signal_event"

    __table_args__ = (
        Index("ix_discovery_signal_event_dedupe", "dedupe_key", unique=True),
        Index("ix_discovery_signal_event_company_observed", "company_id", "observed_at"),
    )

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))
    company_id: str = Field(sa_column=Column(String, nullable=False, index=True))
    event_type: str = Field(sa_column=Column(String, nullable=False, index=True))
    title: str = Field(sa_column=Column(Text, nullable=False))
    payload: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    source: str = Field(sa_column=Column(String, nullable=False))
    observed_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    )
    confidence: float = Field(default=1.0, sa_column=Column(Float, nullable=False, server_default="1"))
    dedupe_key: str = Field(sa_column=Column(String, nullable=False))
    created_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )


# ── DiscoveryJobPost ───────────────────────────────────────────────────────────

class DiscoveryJobPost(SQLModel, table=True):
    """Individual job posting with extracted concepts — keyed by external ATS id."""
    __tablename__ = "discovery_job_post"

    __table_args__ = (
        Index("ix_discovery_job_post_company_external", "company_id", "external_id", unique=True),
    )

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))
    company_id: str = Field(sa_column=Column(String, nullable=False, index=True))
    external_id: str = Field(sa_column=Column(String, nullable=False))
    title: str = Field(sa_column=Column(String, nullable=False))
    location: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    body_text: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    role_family: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    seniority: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    concepts: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    tech: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    initiatives: Optional[List[str]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    source: str = Field(sa_column=Column(String, nullable=False))
    first_seen_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    last_seen_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    closed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )


# ── DiscoveryFiling ────────────────────────────────────────────────────────────

class DiscoveryFiling(SQLModel, table=True):
    """
    Parsed SEC Form D filing (exempt securities offering = funding round).
    Tech-relevant filings are stored even when no discovery_company matches,
    so newly funded startups can be auto-added to the index.
    """
    __tablename__ = "discovery_filing"

    __table_args__ = (
        Index("ix_discovery_filing_accession", "accession_no", unique=True),
        Index("ix_discovery_filing_normalized_name", "normalized_name"),
    )

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))
    accession_no: str = Field(sa_column=Column(String, nullable=False))
    cik: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    entity_name: str = Field(sa_column=Column(String, nullable=False))
    normalized_name: str = Field(sa_column=Column(String, nullable=False))
    form_type: str = Field(default="D", sa_column=Column(String, nullable=False, server_default="D"))
    filed_at: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    industry_group: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    is_tech: bool = Field(default=False, sa_column=Column(Boolean, nullable=False, server_default="false", index=True))
    offering_amount: Optional[float] = Field(default=None, sa_column=Column(Float, nullable=True))
    amount_sold: Optional[float] = Field(default=None, sa_column=Column(Float, nullable=True))
    revenue_range: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    date_of_first_sale: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    state: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True))
    related_persons: Optional[List[Dict[str, Any]]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    matched_company_id: Optional[str] = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    created_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )


# ── DiscoveryProgress ──────────────────────────────────────────────────────────

class DiscoveryProgress(SQLModel, table=True):
    """
    Single-row checkpoint table. The scraper upserts this row after every
    committed page so a crash can resume from the last saved position.

    One active row per run_id. Old runs are kept for audit history.
    """
    __tablename__ = "discovery_progress"

    id: str = Field(default_factory=_uuid, sa_column=Column(String, primary_key=True))

    # ISO timestamp string — uniquely identifies the run
    run_id: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))

    # running | completed | failed
    status: str = Field(default="running", sa_column=Column(String, nullable=False, index=True))

    # Which profile is currently being scraped
    current_profile: Optional[str] = Field(
        default=None, sa_column=Column(String, nullable=True)
    )
    # Last page successfully committed for current_profile (0 = not started)
    current_page: int = Field(default=0)

    # Profiles that have fully completed this run
    profiles_completed: Optional[List[str]] = Field(
        default=None, sa_column=Column(ARRAY(Text), nullable=True)
    )
    # Profiles that failed (after retries) this run
    profiles_failed: Optional[List[str]] = Field(
        default=None, sa_column=Column(ARRAY(Text), nullable=True)
    )

    # Running tally of companies added per profile: {"devtools": 4200, ...}
    stats: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSONB, nullable=True)
    )

    started_at: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    last_heartbeat: datetime = Field(
        default_factory=_utc_now, sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    completed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
