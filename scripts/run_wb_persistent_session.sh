#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/pavel/projects/parser_wb"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
COOKIE_FILE="$PROJECT_DIR/config/wb_cookie.txt"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
SESSION_SCRIPT="$PROJECT_DIR/scripts/wb_persistent_session.py"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_persistent_session.flock"
LOG_FILE="$PROJECT_DIR/data/logs/wb_persistent_session.log"

if [[ -e "$COORDINATOR_LOCK_DIR" || -L "$COORDINATOR_LOCK_DIR" ]]; then
  echo "WB persistent session is disabled by coordinator lock-v3 cutover" >&2
  exit 75
fi

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks"
exec >> "$LOG_FILE" 2>&1

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date --iso-8601=seconds) wb persistent session skipped: previous session is still active"
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

if [[ ! -r "$SESSION_SCRIPT" ]]; then
  echo "$(date --iso-8601=seconds) persistent session script is not readable: $SESSION_SCRIPT"
  exit 2
fi

if [[ ! -s "$COOKIE_FILE" ]]; then
  echo "$(date --iso-8601=seconds) WB cookie file is missing or empty: $COOKIE_FILE"
  exit 2
fi

export WB_COOKIE_FILE="$COOKIE_FILE"
export PYTHONUNBUFFERED=1

echo "$(date --iso-8601=seconds) wb persistent session started"
"$PYTHON_BIN" "$SESSION_SCRIPT" \
  --config "$CONFIG_FILE" \
  --cookie-file "$COOKIE_FILE" \
  --heartbeat-seconds "${PARSER_WB_BROWSER_HEARTBEAT_SECONDS:-300}" \
  --wait-ms "${PARSER_WB_BROWSER_WAIT_MS:-5000}" \
  --timeout-ms "${PARSER_WB_BROWSER_TIMEOUT_MS:-45000}"
status=$?
echo "$(date --iso-8601=seconds) wb persistent session finished: status=$status"
exit "$status"
