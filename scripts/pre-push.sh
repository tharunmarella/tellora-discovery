#!/usr/bin/env bash
# Pre-push gate: non-live tests with scoped coverage (75%).
# Requires Docker for integration tests.

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

echo "pre-push: running non-live tests with 75% scoped coverage gate..."
"$PYTHON" -m pytest -m "not live and not e2e" \
    --cov \
    --cov-config=.coveragerc \
    --cov-fail-under=75 \
    -q

echo "pre-push: passed."
