#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${PARSER_WB_PROJECT_DIR:-/home/pavel/projects/parser_wb}"
PYTHON_BIN="${PARSER_WB_PYTHON_BIN:-/home/Codex/agent-tools/parser_wb-python/bin/python}"

cd "$PROJECT_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python runtime is not executable: $PYTHON_BIN" >&2
  exit 2
fi

"$PYTHON_BIN" "$PROJECT_DIR/scripts/pre_push_check.py" "$@"
