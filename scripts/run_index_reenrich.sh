#!/usr/bin/env bash
# One-time full index backfill:
#   1) scrape-field backfill (linkedin_url, raw_meta, logo_url, …)
#   2) signal re-enrich (HQ, funding grounding, tech stack, …)
#
# Requires: DATABASE_URL, API keys in .env, long runtime (~hours), API spend.
#
# Usage:
#   ./scripts/run_index_reenrich.sh              # full run
#   ./scripts/run_index_reenrich.sh --limit 50   # sample first
#   ./scripts/run_index_reenrich.sh --skip-scrape-backfill --limit 50

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -x .venv/bin/python ]]; then
    PYTHON=.venv/bin/python
else
    PYTHON=python3
fi

if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

SKIP_SCRAPE=0
REENRICH_ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--skip-scrape-backfill" ]]; then
        SKIP_SCRAPE=1
    else
        REENRICH_ARGS+=("$arg")
    fi
done

if [[ "$SKIP_SCRAPE" -eq 0 ]]; then
    HAS_LIMIT=0
    for arg in "${REENRICH_ARGS[@]}"; do
        if [[ "$arg" == --limit* ]]; then
            HAS_LIMIT=1
            break
        fi
    done

    echo "Step 1/2: Backfill scrape-time fields (linkedin_url, raw_meta, …)..."
    if [[ "$HAS_LIMIT" -eq 1 ]]; then
        "$PYTHON" scripts/backfill_scrape_fields.py "${REENRICH_ARGS[@]}"
    else
        "$PYTHON" scripts/backfill_scrape_fields.py --run-all "${REENRICH_ARGS[@]}"
    fi
fi

echo "Step 2/2: Re-enriching signal fields (reset → pending → full pipeline)..."
exec "$PYTHON" -m signals.runner --reset-enriched "${REENRICH_ARGS[@]}"
