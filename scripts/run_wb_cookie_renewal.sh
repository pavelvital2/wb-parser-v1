#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/pavel/projects/parser_wb"
PYTHON_BIN="/home/Codex/agent-tools/parser_wb-python/bin/python"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
COOKIE_FILE="$PROJECT_DIR/config/wb_cookie.txt"
RUNTIME_ENV_FILE="$PROJECT_DIR/config/runtime.env"
KEEPER_SCRIPT="$PROJECT_DIR/scripts/wb_cookie_keeper.py"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_cookie_renewal.flock"
PRODUCTS_SELLERS_LOCK_FILE="$PROJECT_DIR/state/locks/products_sellers_daily.flock"
LOG_FILE="$PROJECT_DIR/data/logs/wb_cookie_renewal.log"

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date --iso-8601=seconds) wb cookie renewal skipped: previous renewal is still active"
  exit 75
fi

exec 8>"$PRODUCTS_SELLERS_LOCK_FILE"
if ! flock -n 8; then
  echo "$(date --iso-8601=seconds) wb cookie renewal skipped: products+sellers run is active"
  exit 0
fi
flock -u 8

cd "$PROJECT_DIR"

if [[ -r "$RUNTIME_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$RUNTIME_ENV_FILE"
  set +a
fi

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
