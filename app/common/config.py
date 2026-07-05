from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config_validation import validate_raw_config
from .paths import ProjectPaths


@dataclass(slots=True)
class RuntimeConfig:
    retry_max_attempts: int
    retry_base_delay_seconds: float
    retry_max_delay_seconds: float
    http_timeout_seconds: int
    dry_run: bool
    debug: bool
    locking_enabled: bool
    lock_stale_seconds: int


@dataclass(slots=True)
class AppConfig:
    raw: dict[str, Any]
    config_file: Path
    project_root: Path
    paths: ProjectPaths
    runtime: RuntimeConfig


def _as_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path


def load_config(path: str) -> AppConfig:
    config_file = Path(path).resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    raw = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    validate_raw_config(raw)
    project_root = config_file.parent.parent.resolve()

    paths_raw = raw.get("paths", {})
    paths = ProjectPaths.from_config(
        project_root=project_root,
        data_raw=_as_path(project_root, paths_raw["data_raw"]),
        data_staging=_as_path(project_root, paths_raw["data_staging"]),
        data_marts=_as_path(project_root, paths_raw["data_marts"]),
        logs=_as_path(project_root, paths_raw["logs"]),
        exports=_as_path(project_root, paths_raw["exports"]),
        state_sqlite=_as_path(project_root, paths_raw["state_sqlite"]),
        checkpoints_dir=_as_path(project_root, paths_raw["checkpoints_dir"]),
    )

    runtime = raw.get("runtime", {})
    cfg_runtime = RuntimeConfig(
        retry_max_attempts=int(runtime.get("retry_max_attempts", 5)),
        retry_base_delay_seconds=float(runtime.get("retry_base_delay_seconds", 1.0)),
        retry_max_delay_seconds=float(runtime.get("retry_max_delay_seconds", 20.0)),
        http_timeout_seconds=int(runtime.get("http_timeout_seconds", 45)),
        dry_run=bool(runtime.get("dry_run", False)),
        debug=bool(runtime.get("debug", False)),
        locking_enabled=bool(runtime.get("locking_enabled", True)),
        lock_stale_seconds=int(runtime.get("lock_stale_seconds", 21600)),
    )

    paths.ensure_base_dirs()
    _inject_env(raw, project_root=project_root)

    return AppConfig(
        raw=raw,
        config_file=config_file,
        project_root=project_root,
        paths=paths,
        runtime=cfg_runtime,
    )


def _coerce_headers(raw_value: Any) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    headers: dict[str, str] = {}
    for name, value in raw_value.items():
        header_name = str(name or "").strip()
        if not header_name or value is None:
            continue
        if header_name.lower() == "cookie":
            continue
        headers[header_name] = str(value)
    return headers


def _load_headers_file(project_root: Path, value: str) -> dict[str, str]:
    if not value:
        return {}
    path = _as_path(project_root, value)
    if not path.exists():
        raise FileNotFoundError(f"request headers file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(payload, dict) and isinstance(payload.get("headers"), dict):
        payload = payload["headers"]
    return _coerce_headers(payload)


def _inject_env(raw: dict[str, Any], *, project_root: Path) -> None:
    serp = raw.get("serp", {})
    if not isinstance(serp, dict):
        return
    env_name = serp.get("wb_cookie_file_env", "")
    if env_name and os.getenv(env_name):
        serp["wb_cookie_file"] = os.getenv(env_name)

    headers_env_name = str(serp.get("request_headers_file_env") or "PARSER_WB_REQUEST_HEADERS_FILE").strip()
    headers_file = os.getenv(headers_env_name, "").strip() if headers_env_name else ""
    headers_file = headers_file or str(serp.get("request_headers_file") or "").strip()
    if headers_file:
        merged = _coerce_headers(serp.get("request_headers"))
        merged.update(_load_headers_file(project_root, headers_file))
        serp["request_headers"] = merged

    webui = raw.get("webui", {})
    pwd_env = webui.get("admin_password_env", "")
    if pwd_env and os.getenv(pwd_env):
        webui["admin_password"] = os.getenv(pwd_env)

    secret_env = webui.get("secret_key_env", "")
    if secret_env and os.getenv(secret_env):
        webui["secret_key"] = os.getenv(secret_env)
