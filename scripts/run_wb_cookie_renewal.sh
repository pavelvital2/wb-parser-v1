#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/pavel/projects/parser_wb"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
COORDINATOR_ADAPTER="$PROJECT_DIR/scripts/wb_nightly_coordinator_adapter.py"
COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
COOKIE_FILE="$PROJECT_DIR/config/wb_cookie.txt"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
KEEPER_SCRIPT="$PROJECT_DIR/scripts/wb_cookie_keeper.py"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_cookie_renewal.flock"
PRODUCTS_SELLERS_LOCK_FILE="$PROJECT_DIR/state/locks/products_sellers_daily.flock"
LOG_FILE="$PROJECT_DIR/data/logs/wb_cookie_renewal.log"

if [[ -e "$COORDINATOR_LOCK_DIR" || -L "$COORDINATOR_LOCK_DIR" ]]; then
  if [[ "${PARSER_WB_LOCK_V3_WRAPPED:-0}" != "1" ]]; then
    exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"
  fi
  if ! "$PYTHON_BIN" "$COORDINATOR_ADAPTER" entry-check; then
    echo "WB host lock-v3 lease validation failed" >&2
    exit 2
  fi
fi

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks"

exec {renewal_lock_fd}>"$LOCK_FILE"
if ! flock -n "$renewal_lock_fd"; then
  echo "$(date --iso-8601=seconds) wb cookie renewal skipped: previous renewal is still active"
  exit 75
fi

exec {collection_probe_fd}>"$PRODUCTS_SELLERS_LOCK_FILE"
if ! flock -n "$collection_probe_fd"; then
  echo "$(date --iso-8601=seconds) wb cookie renewal skipped: products+sellers run is active"
  exit 0
fi
flock -u "$collection_probe_fd"

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

if [[ ! -r "$KEEPER_SCRIPT" ]]; then
  echo "$(date --iso-8601=seconds) keeper script is not readable: $KEEPER_SCRIPT"
  exit 2
fi

if [[ ! -s "$COOKIE_FILE" && "${PARSER_WB_COOKIE_REQUIRED:-1}" != "0" ]]; then
  echo "$(date --iso-8601=seconds) WB cookie file is missing or empty: $COOKIE_FILE"
  exit 2
fi

export WB_COOKIE_FILE="$COOKIE_FILE"

echo "$(date --iso-8601=seconds) wb cookie renewal started"
"$PYTHON_BIN" "$KEEPER_SCRIPT" "${PARSER_WB_COOKIE_RENEW_COMMAND:-ensure}" \
  --config "$CONFIG_FILE" \
  --cookie-file "$COOKIE_FILE" \
  --sample-count "${PARSER_WB_COOKIE_RENEW_SAMPLE_COUNT:-1}" \
  --page "${PARSER_WB_COOKIE_RENEW_PAGE:-1}" \
  --wait-ms "${PARSER_WB_COOKIE_RENEW_WAIT_MS:-5000}" \
  --timeout-ms "${PARSER_WB_COOKIE_RENEW_TIMEOUT_MS:-45000}"
status=$?
if [[ "$status" -ne 0 && "${PARSER_WB_COOKIELESS_FALLBACK_OK:-0}" == "1" ]]; then
  echo "$(date --iso-8601=seconds) wb cookie renewal failed; checking cookie-less fallback channel"
  if "$PYTHON_BIN" "$KEEPER_SCRIPT" smoke \
    --config "$CONFIG_FILE" \
    --cookie-file "$COOKIE_FILE" \
    --sample-count "${PARSER_WB_COOKIE_RENEW_SAMPLE_COUNT:-1}" \
    --page "${PARSER_WB_COOKIE_RENEW_PAGE:-1}" \
    --without-cookie; then
    echo "$(date --iso-8601=seconds) wb cookie-less fallback channel ok; keeping existing cookie file unchanged"
    status=0
  fi
fi
echo "$(date --iso-8601=seconds) wb cookie renewal finished: status=$status"
exit "$status"
