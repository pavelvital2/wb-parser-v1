#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"

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
