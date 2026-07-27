#!/usr/bin/env bash

wb_load_required_runtime_env() {
  local runtime_env_file="${1:-}"
  local project_dir
  local python_bin
  local loader
  local assignment
  local loaded=0
  local -a assignments=()

  if [[ -z "$runtime_env_file" || ! -f "$runtime_env_file" || -L "$runtime_env_file" || ! -r "$runtime_env_file" ]]; then
    echo "WB runtime environment is unavailable" >&2
    return 2
  fi
  project_dir="$(cd "$(dirname "$runtime_env_file")/.." && pwd -P)"
  python_bin="${PARSER_WB_PYTHON_BIN:-/home/Codex/agent-tools/parser_wb-python/bin/python}"
  loader="$project_dir/scripts/wb_runtime_env.py"
  if [[ ! -x "$python_bin" || ! -r "$loader" ]]; then
    echo "WB runtime environment loader is unavailable" >&2
    return 2
  fi
  mapfile -d '' -t assignments < <(
    "$python_bin" "$loader" \
      --project-root "$project_dir" \
      --runtime-file "$runtime_env_file" \
      --export0
  )
  for assignment in "${assignments[@]}"; do
    if [[ ! "$assignment" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      echo "WB runtime environment loader returned invalid data" >&2
      return 2
    fi
    export "$assignment"
    if [[ "$assignment" == "PARSER_WB_RUNTIME_ENV_LOADED=1" ]]; then
      loaded=1
    fi
  done
  if [[ "$loaded" -ne 1 ]]; then
    echo "WB runtime environment could not be loaded" >&2
    return 2
  fi
}
