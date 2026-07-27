#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
COORDINATOR_ADAPTER="$PROJECT_DIR/scripts/wb_nightly_coordinator_adapter.py"
COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"

if [[ -e "$COORDINATOR_LOCK_DIR" || -L "$COORDINATOR_LOCK_DIR" ]]; then
  if [[ "${PARSER_WB_LOCK_V3_WRAPPED:-0}" != "1" ]]; then
    exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"
  fi
  if ! "$PYTHON_BIN" "$COORDINATOR_ADAPTER" entry-check; then
    echo "WB host lock-v3 lease validation failed" >&2
    exit 2
  fi
fi

for argument in "$@"; do
  case "$argument" in
    --guarded-pilot|--guarded-pilot=*)
      echo "guarded pilot requires scripts/run_wb_guarded_regional_pilot.sh" >&2
      exit 2
      ;;
  esac
done

if [[ ! -r "$RUNTIME_LOADER" || ! -x "$PYTHON_BIN" ]]; then
  echo "WB collection-plan launcher prerequisites are unavailable" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$RUNTIME_LOADER"
wb_load_required_runtime_env "$RUNTIME_ENV_FILE"

cd "$PROJECT_DIR"
exec "$PYTHON_BIN" scripts/run_wb_collection_plan.py "$@"
