#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/pavel/projects/parser_wb"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
WATCHDOG_SCRIPT="$PROJECT_DIR/scripts/wb_persistent_session_watchdog.py"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_persistent_watchdog.flock"
LOG_FILE="$PROJECT_DIR/data/logs/wb_persistent_watchdog.log"

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks"
exec >> "$LOG_FILE" 2>&1

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date --iso-8601=seconds) wb persistent watchdog skipped: previous watchdog is still active"
  exit 75
fi

cd "$PROJECT_DIR"

if [[ ! -r "$RUNTIME_LOADER" ]]; then
  echo "$(date --iso-8601=seconds) WB runtime loader is unavailable"
  exit 2
fi
# shellcheck disable=SC1090
source "$RUNTIME_LOADER"
wb_load_required_runtime_env "$RUNTIME_ENV_FILE"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "$(date --iso-8601=seconds) python runtime is not executable: $PYTHON_BIN"
  exit 2
fi

if [[ ! -r "$WATCHDOG_SCRIPT" ]]; then
  echo "$(date --iso-8601=seconds) watchdog script is not readable: $WATCHDOG_SCRIPT"
  exit 2
fi

echo "$(date --iso-8601=seconds) wb persistent watchdog started"
"$PYTHON_BIN" "$WATCHDOG_SCRIPT"
status=$?
echo "$(date --iso-8601=seconds) wb persistent watchdog finished: status=$status"
exit "$status"
