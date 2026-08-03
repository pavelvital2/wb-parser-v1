#!/usr/bin/env bash
set -Eeuo pipefail
export PYTHONDONTWRITEBYTECODE=1

PROJECT_DIR="/home/pavel/projects/parser_wb"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
COORDINATOR_ADAPTER="$PROJECT_DIR/scripts/wb_nightly_coordinator_adapter.py"
COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
COOKIE_FILE="$PROJECT_DIR/config/wb_cookie.txt"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
RUNTIME_LOADER="$PROJECT_DIR/scripts/wb_runtime_env.sh"
PREFLIGHT_SCRIPT="$PROJECT_DIR/scripts/wb_nightly_preflight.py"
AUTHORIZATION_HORIZON_PLAN="$PROJECT_DIR/config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
NOTIFY_SCRIPT="$PROJECT_DIR/scripts/notify_products_sellers_daily.py"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_nightly_preflight.flock"
PRODUCTS_SELLERS_LOCK_FILE="$PROJECT_DIR/state/locks/products_sellers_daily.flock"
LOG_FILE="$PROJECT_DIR/data/logs/wb_nightly_preflight.log"
STARTED_AT="$(date --iso-8601=seconds)"
RUN_STAMP="preflight_$(date +%Y%m%d_%H%M%S%z)"

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

exec {preflight_lock_fd}>"$LOCK_FILE"
if ! flock -n "$preflight_lock_fd"; then
  echo "$(date --iso-8601=seconds) wb nightly preflight skipped: previous preflight is still active"
  exit 75
fi

exec {collection_probe_fd}>"$PRODUCTS_SELLERS_LOCK_FILE"
if ! flock -n "$collection_probe_fd"; then
  echo "$(date --iso-8601=seconds) wb nightly preflight skipped: products+sellers run is active"
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

if [[ ! -r "$PREFLIGHT_SCRIPT" ]]; then
  echo "$(date --iso-8601=seconds) preflight script is not readable: $PREFLIGHT_SCRIPT"
  exit 2
fi

if [[ ! -s "$COOKIE_FILE" ]]; then
  echo "$(date --iso-8601=seconds) WB cookie file is missing or empty: $COOKIE_FILE"
  exit 2
fi

export WB_COOKIE_FILE="$COOKIE_FILE"

echo "$(date --iso-8601=seconds) wb nightly preflight started: stamp=$RUN_STAMP"
set +e
"$PYTHON_BIN" "$PREFLIGHT_SCRIPT" preflight \
  --config "$CONFIG_FILE" \
  --cookie-file "$COOKIE_FILE" \
  --sample-count "${PARSER_WB_PREFLIGHT_SAMPLE_COUNT:-3}" \
  --authorization-policy required \
  --authorization-horizon-plan-file "$AUTHORIZATION_HORIZON_PLAN" \
  --page "${PARSER_WB_PREFLIGHT_PAGE:-1}" \
  --wait-ms "${PARSER_WB_PREFLIGHT_WAIT_MS:-5000}" \
  --timeout-ms "${PARSER_WB_PREFLIGHT_TIMEOUT_MS:-45000}"
status=$?
set -e
echo "$(date --iso-8601=seconds) wb nightly preflight finished: status=$status"

if [[ "$status" != "0" && "${PARSER_WB_NOTIFY_DISABLED:-0}" != "1" && -r "$NOTIFY_SCRIPT" ]]; then
  "$PYTHON_BIN" "$NOTIFY_SCRIPT" \
    --phase preflight \
    --status "$status" \
    --run-stamp "$RUN_STAMP" \
    --started-at "$STARTED_AT" \
    --finished-at "$(date --iso-8601=seconds)" \
    --log-path "$LOG_FILE" || true
fi

exit "$status"
