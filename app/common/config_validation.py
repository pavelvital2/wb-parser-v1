from __future__ import annotations

from typing import Any

from .exceptions import ConfigValidationError


def _required_dict(raw: dict[str, Any], key: str, errors: list[str]) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        errors.append(f"Missing or invalid section '{key}'")
        return {}
    return value


def _require_key(section: dict[str, Any], section_name: str, key: str, errors: list[str]) -> str:
    value = section.get(key)
    if value is None or str(value).strip() == "":
        errors.append(f"Missing required key '{section_name}.{key}'")
        return ""
    return str(value)


def _to_int(value: Any, *, field: str, errors: list[str], min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        errors.append(f"Invalid integer for '{field}': {value}")
        return 0

    if min_value is not None and parsed < min_value:
        errors.append(f"'{field}' must be >= {min_value}")
    if max_value is not None and parsed > max_value:
        errors.append(f"'{field}' must be <= {max_value}")
    return parsed


def _to_float(value: Any, *, field: str, errors: list[str], min_value: float | None = None) -> float:
    try:
        parsed = float(value)
    except Exception:
        errors.append(f"Invalid float for '{field}': {value}")
        return 0.0

    if min_value is not None and parsed < min_value:
        errors.append(f"'{field}' must be >= {min_value}")
    return parsed


def validate_raw_config(raw: dict[str, Any]) -> None:
    errors: list[str] = []

    project = _required_dict(raw, "project", errors)
    _require_key(project, "project", "name", errors)
    _require_key(project, "project", "source_system", errors)

    paths = _required_dict(raw, "paths", errors)
    for key in ["data_raw", "data_staging", "data_marts", "logs", "exports", "state_sqlite", "checkpoints_dir"]:
        _require_key(paths, "paths", key, errors)

    runtime = _required_dict(raw, "runtime", errors)
    _to_int(runtime.get("retry_max_attempts", 5), field="runtime.retry_max_attempts", errors=errors, min_value=1, max_value=10)
    _to_float(runtime.get("retry_base_delay_seconds", 1.0), field="runtime.retry_base_delay_seconds", errors=errors, min_value=0.0)
    _to_float(runtime.get("retry_max_delay_seconds", 20.0), field="runtime.retry_max_delay_seconds", errors=errors, min_value=0.0)
    _to_int(runtime.get("http_timeout_seconds", 45), field="runtime.http_timeout_seconds", errors=errors, min_value=1)
    _to_int(runtime.get("lock_stale_seconds", 21600), field="runtime.lock_stale_seconds", errors=errors, min_value=60)

    serp = raw.get("serp", {}) if isinstance(raw.get("serp", {}), dict) else {}
    if serp:
        _to_int(serp.get("pages_per_query", 10), field="serp.pages_per_query", errors=errors, min_value=1, max_value=100)
        _to_int(serp.get("page_size", 100), field="serp.page_size", errors=errors, min_value=1, max_value=200)

    webui = raw.get("webui", {}) if isinstance(raw.get("webui", {}), dict) else {}
    if webui:
        _to_int(webui.get("port", 8080), field="webui.port", errors=errors, min_value=1, max_value=65535)

    retention = raw.get("retention", {}) if isinstance(raw.get("retention", {}), dict) else {}
    days = retention.get("days", {}) if isinstance(retention.get("days", {}), dict) else {}
    for key, val in days.items():
        _to_int(val, field=f"retention.days.{key}", errors=errors, min_value=1)

    if errors:
        raise ConfigValidationError("Config validation failed:\n- " + "\n- ".join(errors))
