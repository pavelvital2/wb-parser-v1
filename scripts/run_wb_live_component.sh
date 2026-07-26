#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"

target="${1:-}"
shift || true
case "$target" in
  suggest|serp|sellers|daily|monthly) ;;
  *)
    echo "unsupported live WB target" >&2
    exit 2
    ;;
esac
if [[ ! -r "$RUNTIME_LOADER" || ! -x "$PYTHON_BIN" ]]; then
  echo "live WB launcher prerequisites are unavailable" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$RUNTIME_LOADER"
wb_load_required_runtime_env "$RUNTIME_ENV_FILE"

cd "$PROJECT_DIR"
exec "$PYTHON_BIN" main.py --config "$CONFIG_FILE" run "$target" "$@"
