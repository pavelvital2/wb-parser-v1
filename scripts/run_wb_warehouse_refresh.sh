#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${PARSER_WB_PROJECT_DIR:-/home/pavel/projects/parser_wb}"
PYTHON_BIN="${PARSER_WB_PYTHON_BIN:-/home/Codex/agent-tools/parser_wb-python/bin/python}"
COORDINATOR_ADAPTER="$PROJECT_DIR/scripts/wb_nightly_coordinator_adapter.py"
COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"
WAREHOUSE_SCRIPT="$PROJECT_DIR/scripts/wb_warehouse.py"
RUN_REPORT_FILE="${PARSER_WB_WAREHOUSE_RUN_REPORT:-$PROJECT_DIR/state/run_reports/latest.json}"
LOCK_FILE="$PROJECT_DIR/state/locks/wb_warehouse_refresh.flock"
PRODUCTS_SELLERS_LOCK_FILE="$PROJECT_DIR/state/locks/products_sellers_daily.flock"
LOG_FILE="${PARSER_WB_WAREHOUSE_LOG_FILE:-$PROJECT_DIR/data/logs/wb_warehouse_refresh.log}"
STATE_DIR="$PROJECT_DIR/state/wb_warehouse"
HISTORY_DIR="$STATE_DIR/history"
DRY_RUN=0
CHECK_ONLY=0
STARTED_AT="$(date --iso-8601=seconds)"

if [[ "${PARSER_WB_LOCK_V3_WRAPPED:-0}" != "1" \
  && ( -e "$COORDINATOR_LOCK_DIR" || -L "$COORDINATOR_LOCK_DIR" ) ]]; then
  exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"
fi

usage() {
  cat <<'EOF'
Usage: scripts/run_wb_warehouse_refresh.sh [--dry-run] [--check-only]

Safely refreshes the WB warehouse after a successful products+sellers run.
EOF
}

while (($#)); do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;
    --check-only)
      CHECK_ONLY=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

mkdir -p "$PROJECT_DIR/data/logs" "$PROJECT_DIR/state/locks" "$STATE_DIR" "$HISTORY_DIR"

log() {
  echo "$(date --iso-8601=seconds) $*" | tee -a "$LOG_FILE"
}

write_state() {
  local status="$1"
  local reason="$2"
  local exit_code="$3"
  local build_json="${4:-}"
  local check_json="${5:-}"
  local report_json="${6:-}"
  local finished_at
  local latest_path
  local history_path

  finished_at="$(date --iso-8601=seconds)"
  latest_path="$STATE_DIR/latest.json"
  history_path="$HISTORY_DIR/warehouse_refresh_$(date -u +%Y%m%dT%H%M%SZ).json"

  STATUS="$status" \
  REASON="$reason" \
  EXIT_CODE="$exit_code" \
  STARTED_AT="$STARTED_AT" \
  FINISHED_AT="$finished_at" \
  DRY_RUN="$DRY_RUN" \
  CHECK_ONLY="$CHECK_ONLY" \
  PROJECT_DIR="$PROJECT_DIR" \
  RUN_REPORT_FILE="$RUN_REPORT_FILE" \
  LOG_FILE="$LOG_FILE" \
  BUILD_JSON="$build_json" \
  CHECK_JSON="$check_json" \
  REPORT_JSON="$report_json" \
  LATEST_PATH="$latest_path" \
  HISTORY_PATH="$history_path" \
  "$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path


def load_json(value: str) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {"parse_error": True}


build = load_json(os.environ.get("BUILD_JSON", ""))
check = load_json(os.environ.get("CHECK_JSON", ""))
report = load_json(os.environ.get("REPORT_JSON", ""))
would_write = build.get("would_write", {}) if isinstance(build.get("would_write"), dict) else {}
payload = {
    "status": os.environ["STATUS"],
    "reason": os.environ["REASON"],
    "exit_code": int(os.environ["EXIT_CODE"]),
    "started_at": os.environ["STARTED_AT"],
    "finished_at": os.environ["FINISHED_AT"],
    "dry_run": os.environ["DRY_RUN"] == "1",
    "check_only": os.environ["CHECK_ONLY"] == "1",
    "project_dir": os.environ["PROJECT_DIR"],
    "run_report_file": os.environ["RUN_REPORT_FILE"],
    "log_file": os.environ["LOG_FILE"],
    "run_report": {
        "run_id": report.get("run_id", ""),
        "pipeline": report.get("pipeline", ""),
        "status": report.get("status", ""),
        "sellers_component_status": report.get("sellers_component_status", ""),
    },
    "warehouse": {
        "database_path": check.get("database_path") or build.get("database_path") or would_write.get("database_path", ""),
        "manifest_path": check.get("manifest_path") or build.get("manifest_path") or would_write.get("manifest_path", ""),
        "manifest_built_at_utc": check.get("manifest_built_at_utc") or build.get("built_at_utc", ""),
        "rows": check.get("rows") or build.get("rows") or {},
        "files": build.get("files") or {},
    },
}

latest = Path(os.environ["LATEST_PATH"])
history = Path(os.environ["HISTORY_PATH"])
latest.parent.mkdir(parents=True, exist_ok=True)
history.parent.mkdir(parents=True, exist_ok=True)
text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
latest.write_text(text, encoding="utf-8")
history.write_text(text, encoding="utf-8")
print(text, end="")
PY
}

exec {warehouse_lock_fd}>"$LOCK_FILE"
if ! flock -n "$warehouse_lock_fd"; then
  log "wb warehouse refresh skipped: previous refresh is still active"
  write_state "skipped" "refresh_lock_busy" 75 "" "" ""
  exit 75
fi

if [[ "${PARSER_WB_WAREHOUSE_ALLOW_ACTIVE_DAILY:-0}" != "1" ]]; then
  exec {collection_probe_fd}>"$PRODUCTS_SELLERS_LOCK_FILE"
  if ! flock -n "$collection_probe_fd"; then
    log "wb warehouse refresh skipped: products+sellers run is active"
    write_state "skipped" "products_sellers_run_active" 75 "" "" ""
    exit 75
  fi
  flock -u "$collection_probe_fd"
fi

cd "$PROJECT_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log "wb warehouse refresh failed: python runtime is not executable: $PYTHON_BIN"
  write_state "failed" "python_runtime_missing" 2 "" "" ""
  exit 2
fi

if [[ ! -r "$WAREHOUSE_SCRIPT" ]]; then
  log "wb warehouse refresh failed: script is not readable: $WAREHOUSE_SCRIPT"
  write_state "failed" "warehouse_script_missing" 2 "" "" ""
  exit 2
fi

report_json="$("$PYTHON_BIN" - "$RUN_REPORT_FILE" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print(json.dumps({"ok": False, "reason": "latest_report_missing", "path": str(path)}))
    raise SystemExit(20)

try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    print(json.dumps({"ok": False, "reason": "latest_report_parse_failed", "path": str(path), "error_class": exc.__class__.__name__}))
    raise SystemExit(20)

sellers_status = ""
for component in data.get("components") or []:
    if component.get("component") == "sellers":
        sellers_status = str(component.get("status") or "")
        break

ok = data.get("status") == "success" and data.get("pipeline") == "sellers" and sellers_status == "success"
print(json.dumps({
    "ok": ok,
    "reason": "" if ok else "latest_report_not_success",
    "run_id": data.get("run_id", ""),
    "pipeline": data.get("pipeline", ""),
    "status": data.get("status", ""),
    "sellers_component_status": sellers_status,
}, ensure_ascii=False))
raise SystemExit(0 if ok else 21)
PY
)" || report_status=$?
report_status="${report_status:-0}"

if [[ "$report_status" != "0" ]]; then
  log "wb warehouse refresh skipped: latest run report is not successful for sellers"
  write_state "skipped" "latest_report_not_success" 0 "" "" "$report_json"
  exit 0
fi

log "wb warehouse refresh started: dry_run=$DRY_RUN check_only=$CHECK_ONLY"

build_json=""
check_json=""
if [[ "$CHECK_ONLY" == "1" ]]; then
  if ! check_json="$("$PYTHON_BIN" "$WAREHOUSE_SCRIPT" check 2>&1 | tee -a "$LOG_FILE")"; then
    log "wb warehouse refresh failed: check failed"
    write_state "failed" "check_failed" 21 "$build_json" "$check_json" "$report_json"
    exit 21
  fi
  write_state "success" "check_only_ok" 0 "$build_json" "$check_json" "$report_json"
  log "wb warehouse refresh finished: status=success reason=check_only_ok"
  exit 0
fi

if [[ "$DRY_RUN" == "1" ]]; then
  if ! build_json="$("$PYTHON_BIN" "$WAREHOUSE_SCRIPT" build --dry-run 2>&1 | tee -a "$LOG_FILE")"; then
    log "wb warehouse refresh failed: dry-run build failed"
    write_state "failed" "dry_run_failed" 20 "$build_json" "$check_json" "$report_json"
    exit 20
  fi
  write_state "success" "dry_run_ok" 0 "$build_json" "$check_json" "$report_json"
  log "wb warehouse refresh finished: status=success reason=dry_run_ok"
  exit 0
fi

if ! build_json="$("$PYTHON_BIN" "$WAREHOUSE_SCRIPT" build 2>&1 | tee -a "$LOG_FILE")"; then
  log "wb warehouse refresh failed: build failed"
  write_state "failed" "build_failed" 20 "$build_json" "$check_json" "$report_json"
  exit 20
fi

if ! check_json="$("$PYTHON_BIN" "$WAREHOUSE_SCRIPT" check 2>&1 | tee -a "$LOG_FILE")"; then
  log "wb warehouse refresh failed: check failed after build"
  write_state "failed" "check_failed" 21 "$build_json" "$check_json" "$report_json"
  exit 21
fi

write_state "success" "refresh_ok" 0 "$build_json" "$check_json" "$report_json"
log "wb warehouse refresh finished: status=success reason=refresh_ok"
