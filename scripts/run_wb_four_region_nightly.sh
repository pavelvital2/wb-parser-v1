#!/usr/bin/env bash
set -Eeuo pipefail
export PYTHONDONTWRITEBYTECODE=1

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
ADAPTER="$PROJECT_DIR/scripts/wb_nightly_coordinator_adapter.py"

if [[ ! -r "$ADAPTER" || ! -x "$PYTHON_BIN" ]]; then
  echo "WB four-region launcher prerequisites are unavailable" >&2
  exit 2
fi

cd "$PROJECT_DIR"
exec "$PYTHON_BIN" "$ADAPTER" four-region -- "$@"
