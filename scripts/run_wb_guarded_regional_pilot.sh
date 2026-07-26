#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
PLAN_FILE="$PROJECT_DIR/config/wb/collection_plans/shevron-moscow-rostov-top100-pilot-v1.json"

if (( $# != 0 )); then
  echo "guarded regional pilot launcher accepts no arguments" >&2
  exit 2
fi
if [[ ! -r "$RUNTIME_LOADER" || ! -x "$PYTHON_BIN" ]]; then
  echo "guarded regional pilot launcher prerequisites are unavailable" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$RUNTIME_LOADER"
wb_load_required_runtime_env "$RUNTIME_ENV_FILE"

cd "$PROJECT_DIR"
exec "$PYTHON_BIN" main.py \
  --config "$CONFIG_FILE" \
  collection-plan \
  --plan-file "$PLAN_FILE" \
  --no-publish \
  --guarded-pilot
