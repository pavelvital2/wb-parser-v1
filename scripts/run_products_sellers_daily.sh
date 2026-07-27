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
LOCK_FILE="$PROJECT_DIR/state/locks/products_sellers_daily.flock"
LOG_FILE="$PROJECT_DIR/data/logs/cron_products_sellers.log"
NOTIFY_SCRIPT="$PROJECT_DIR/scripts/notify_products_sellers_daily.py"
KEEPER_SCRIPT="$PROJECT_DIR/scripts/wb_cookie_keeper.py"
PREFLIGHT_SCRIPT="$PROJECT_DIR/scripts/wb_nightly_preflight.py"
WAREHOUSE_REFRESH_SCRIPT="$PROJECT_DIR/scripts/run_wb_warehouse_refresh.sh"
STARTED_AT="$(date --iso-8601=seconds)"
RUN_STAMP="$(date +%Y%m%d_%H%M%S%z)"
SERP_MAX_ATTEMPTS="${PARSER_WB_SERP_MAX_ATTEMPTS:-2}"
SERP_RETRY_SLEEP_SECONDS="${PARSER_WB_SERP_RETRY_SLEEP_SECONDS:-3600}"

if [[ "${PARSER_WB_LOCK_V3_WRAPPED:-0}" != "1" \
  && ( -e "$COORDINATOR_LOCK_DIR" || -L "$COORDINATOR_LOCK_DIR" ) ]]; then
  exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"
fi

notify_on_exit() {
  local status=$?
  local finished_at
  local notify_python

  trap - EXIT
  finished_at="$(date --iso-8601=seconds)"

  if [[ "${PARSER_WB_NOTIFY_DISABLED:-0}" != "1" && -r "$NOTIFY_SCRIPT" ]]; then
    notify_python="$PYTHON_BIN"
    if [[ ! -x "$notify_python" ]]; then
      notify_python="$(command -v python3 || true)"
    fi

    if [[ -n "$notify_python" ]]; then
      "$notify_python" "$NOTIFY_SCRIPT" \
        --status "$status" \
        --run-stamp "${RUN_STAMP:-unknown}" \
        --started-at "${STARTED_AT:-unknown}" \
        --finished-at "$finished_at" \
        --log-path "$LOG_FILE" || true
    fi
  fi

  exit "$status"
}

trap notify_on_exit EXIT

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks"

exec {daily_lock_fd}>"$LOCK_FILE"
if ! flock -n "$daily_lock_fd"; then
  echo "$(date --iso-8601=seconds) products+sellers daily: previous run is still active"
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

if [[ ! -s "$COOKIE_FILE" && "${PARSER_WB_COOKIE_REQUIRED:-1}" != "0" ]]; then
  echo "$(date --iso-8601=seconds) WB cookie file is missing or empty: $COOKIE_FILE"
  exit 2
fi

export WB_COOKIE_FILE="$COOKIE_FILE"

run_keeper() {
  if [[ "${PARSER_WB_KEEPER_DISABLED:-0}" == "1" || ! -r "$KEEPER_SCRIPT" ]]; then
    return 0
  fi

  "$PYTHON_BIN" "$KEEPER_SCRIPT" ensure \
    --config "$CONFIG_FILE" \
    --cookie-file "$COOKIE_FILE" \
    --sample-count "${PARSER_WB_KEEPER_SAMPLE_COUNT:-3}" \
    --page "${PARSER_WB_KEEPER_PAGE:-1}"
}

run_access_preflight() {
  if [[ "${PARSER_WB_PREFLIGHT_DISABLED:-0}" != "1" && -r "$PREFLIGHT_SCRIPT" ]]; then
    "$PYTHON_BIN" "$PREFLIGHT_SCRIPT" preflight \
      --config "$CONFIG_FILE" \
      --cookie-file "$COOKIE_FILE" \
      --sample-count "${PARSER_WB_PREFLIGHT_SAMPLE_COUNT:-3}" \
      --page "${PARSER_WB_PREFLIGHT_PAGE:-1}" \
      --wait-ms "${PARSER_WB_PREFLIGHT_WAIT_MS:-5000}" \
      --timeout-ms "${PARSER_WB_PREFLIGHT_TIMEOUT_MS:-45000}"
    return $?
  fi

  run_keeper
}

if ! run_access_preflight; then
  echo "$(date --iso-8601=seconds) WB access preflight failed before SERP; aborting products+sellers run"
  exit 2
fi

echo "$(date --iso-8601=seconds) products+sellers daily started: stamp=$RUN_STAMP"

serp_status=1
for ((attempt=1; attempt<=SERP_MAX_ATTEMPTS; attempt++)); do
  echo "$(date --iso-8601=seconds) SERP attempt ${attempt}/${SERP_MAX_ATTEMPTS}: stamp=$RUN_STAMP"
  if "$PYTHON_BIN" main.py --config "$CONFIG_FILE" run serp --job-id "scheduled_products_${RUN_STAMP}_attempt${attempt}"; then
    serp_status=0
    break
  else
    serp_status=$?
  fi

  echo "$(date --iso-8601=seconds) SERP attempt ${attempt}/${SERP_MAX_ATTEMPTS} failed: exit_status=$serp_status"

  if (( attempt < SERP_MAX_ATTEMPTS )); then
    echo "$(date --iso-8601=seconds) sleeping before SERP retry: ${SERP_RETRY_SLEEP_SECONDS}s"
    sleep "$SERP_RETRY_SLEEP_SECONDS"
    if ! run_access_preflight; then
      echo "$(date --iso-8601=seconds) WB access preflight failed before SERP retry; aborting products+sellers run"
      exit 2
    fi
  fi
done

if (( serp_status != 0 )); then
  exit "$serp_status"
fi

"$PYTHON_BIN" main.py --config "$CONFIG_FILE" run sellers --job-id "scheduled_sellers_${RUN_STAMP}"

if [[ "${PARSER_WB_WAREHOUSE_REFRESH_DISABLED:-0}" != "1" && -x "$WAREHOUSE_REFRESH_SCRIPT" ]]; then
  warehouse_status=0
  PARSER_WB_WAREHOUSE_ALLOW_ACTIVE_DAILY=1 "$WAREHOUSE_REFRESH_SCRIPT" || warehouse_status=$?
  if (( warehouse_status != 0 )); then
    echo "$(date --iso-8601=seconds) warehouse_failed: exit_status=$warehouse_status"
  fi
fi

echo "$(date --iso-8601=seconds) products+sellers daily finished: stamp=$RUN_STAMP"
