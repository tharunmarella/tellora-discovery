#!/usr/bin/env bash
# Install tellora-discovery git hooks (pre-push coverage gate).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_REL="$(git -C "$ROOT" rev-parse --git-path hooks)"
HOOKS_DIR="$ROOT/$HOOKS_REL"
TARGET="$HOOKS_DIR/pre-push"
SOURCE="$ROOT/scripts/pre-push.sh"

chmod +x "$SOURCE"
mkdir -p "$HOOKS_DIR"

if [[ -e "$TARGET" && ! -L "$TARGET" ]]; then
    echo "Refusing to overwrite existing hook: $TARGET"
    echo "Remove it manually or merge scripts/pre-push.sh into your hook."
    exit 1
fi

ln -sf "../../scripts/pre-push.sh" "$TARGET"
echo "Installed pre-push hook -> scripts/pre-push.sh"
