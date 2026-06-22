#!/usr/bin/env bash
# Pre-push gate: coverage + LLM-graded e2e pipeline quality.
# Requires Docker (integration tests), SERPER_API_KEY, and GOOGLE_API_KEY.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

if ! docker info >/dev/null 2>&1; then
    echo "pre-push: Docker is required for integration tests and the coverage gate."
    echo "Start Docker Desktop and retry."
    exit 1
fi

if [[ -x .venv/bin/python ]]; then
    PYTHON=.venv/bin/python
elif command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
else
    echo "pre-push: python3 not found"
    exit 1
fi

if ! "$PYTHON" -c "import pytest_cov" 2>/dev/null; then
    echo "pre-push: install dev dependencies first:"
    echo "  pip install -r requirements-dev.txt"
    exit 1
fi

# Load local secrets for the e2e quality gate.
if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

missing_keys=()
[[ -z "${SERPER_API_KEY:-}" ]] && missing_keys+=("SERPER_API_KEY")
if [[ -z "${GOOGLE_API_KEY:-}" && -z "${GEMINI_API_KEY:-}" ]]; then
    missing_keys+=("GOOGLE_API_KEY or GEMINI_API_KEY")
fi
if ((${#missing_keys[@]} > 0)); then
    echo "pre-push: missing required API keys for the e2e quality gate:"
    printf '  - %s\n' "${missing_keys[@]}"
    echo "Set them in tellora-discovery/.env and retry."
    exit 1
fi

echo "pre-push: running non-live tests with 75% scoped coverage gate..."
"$PYTHON" -m pytest -m "not live and not e2e" \
    --cov \
    --cov-config=.coveragerc \
    --cov-fail-under=75 \
    -q

echo "pre-push: running e2e pipeline quality gate (3 companies, judge=${E2E_JUDGE_MODEL:-gemini-3.5-flash})..."
"$PYTHON" -m pytest -m e2e -v

echo "pre-push: passed."
