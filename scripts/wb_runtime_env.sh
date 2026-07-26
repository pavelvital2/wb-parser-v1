#!/usr/bin/env bash

wb_load_required_runtime_env() {
  local runtime_env_file="${1:-}"
  local before_sha256
  local after_sha256

  if [[ -z "$runtime_env_file" || ! -f "$runtime_env_file" || -L "$runtime_env_file" || ! -r "$runtime_env_file" ]]; then
    echo "WB runtime environment is unavailable" >&2
    return 2
  fi

  before_sha256="$(sha256sum "$runtime_env_file" | awk '{print $1}')"
  set -a
  if ! source "$runtime_env_file" >/dev/null 2>&1; then
    set +a
    echo "WB runtime environment could not be loaded" >&2
    return 2
  fi
  set +a
  if [[ ! -f "$runtime_env_file" || -L "$runtime_env_file" || ! -r "$runtime_env_file" ]]; then
    echo "WB runtime environment became unsafe while loading" >&2
    return 2
  fi
  after_sha256="$(sha256sum "$runtime_env_file" | awk '{print $1}')"
  if [[ "$before_sha256" != "$after_sha256" ]]; then
    echo "WB runtime environment changed while loading" >&2
    return 2
  fi

  export PARSER_WB_RUNTIME_ENV_LOADED=1
  export PARSER_WB_RUNTIME_ENV_SHA256="$before_sha256"
}
