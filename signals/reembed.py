"""
Re-embed every company into the current embedding space.

description_embedding is written here (enrichment, scrape-time lookup) and
searched by the backend with query vectors from its own embedding settings.
Vectors from two models are not comparable, so after EMBEDDING_PROVIDER /
OPENAI_EMBEDDING_MODEL change, every stored vector is rebuilt from the
company's text. Resumable: a discovery_progress cursor per model
(run_id 'reembed:<model>') records the last id done. Rows with a vector but
no text to rebuild it from are cleared rather than left in the old space.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlmodel import Session, select

from models import DiscoveryProgress

logger = logging.getLogger("discovery.reembed")

BATCH = 100

_SELECT = text("""
    SELECT id, company_summary, description, industry, tech_stack,
           description_embedding IS NOT NULL AS has_vec
    FROM discovery_company
    WHERE id > :after
    ORDER BY id
    LIMIT :n
""")
_SET = text("UPDATE discovery_company SET description_embedding = CAST(:vec AS vector) WHERE id = :id")
_CLEAR = text("UPDATE discovery_company SET description_embedding = NULL WHERE id = :id")


def _cursor(session: Session, run_id: str) -> DiscoveryProgress:
    row = session.exec(select(DiscoveryProgress).where(DiscoveryProgress.run_id == run_id)).first()
    if row is None:
        row = DiscoveryProgress(run_id=run_id, status="running", current_page=0,
                                stats={"after": "", "embedded": 0, "cleared": 0})
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


async def reembed_companies(session: Session, *, batch: int = BATCH, max_batches: int | None = None) -> dict:
    from llm import embed_texts, embedding_available, embedding_model
    from signals.pipeline import company_embed_text

    model = embedding_model()
    if not embedding_available():
        return {"model": model, "skipped": "no_api_key"}
    cursor = _cursor(session, f"reembed:{model}")
    if cursor.status == "completed":
        return {"model": model, "skipped": "completed", **(cursor.stats or {})}

    stats = dict(cursor.stats or {})
    batches = 0
    while max_batches is None or batches < max_batches:
        rows = session.execute(_SELECT, {"after": stats["after"], "n": batch}).mappings().all()
        if not rows:
            cursor.status, cursor.completed_at = "completed", datetime.now(timezone.utc)
            break
        texts, ids, clear = [], [], []
        for r in rows:
            tech = r["tech_stack"] if isinstance(r["tech_stack"], list) else json.loads(r["tech_stack"] or "[]")
            t = company_embed_text(r["company_summary"], r["description"], r["industry"], tech or [])
            if t:
                texts.append(t)
                ids.append(r["id"])
            elif r["has_vec"]:
                clear.append(r["id"])
        vecs = await asyncio.to_thread(embed_texts, texts) if texts else []
        if vecs:
            session.execute(_SET, [{"id": i, "vec": "[" + ",".join(map(str, v)) + "]"} for i, v in zip(ids, vecs)])
        if clear:
            session.execute(_CLEAR, [{"id": i} for i in clear])
        stats["after"] = rows[-1]["id"]
        stats["embedded"] = stats.get("embedded", 0) + len(vecs)
        stats["cleared"] = stats.get("cleared", 0) + len(clear)
        cursor.stats = dict(stats)
        cursor.current_page += 1
        cursor.last_heartbeat = datetime.now(timezone.utc)
        session.add(cursor)
        session.commit()  # each batch is durable; a restart continues after stats["after"]
        batches += 1

    session.add(cursor)
    session.commit()
    logger.info(f"[reembed] {model}: status={cursor.status} {stats}")
    return {"model": model, "status": cursor.status, **stats}
