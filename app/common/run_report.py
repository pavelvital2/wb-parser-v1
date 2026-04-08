from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _to_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _duration_seconds(started_at_utc: str, finished_at_utc: str) -> float | None:
    a = _to_dt(started_at_utc)
    b = _to_dt(finished_at_utc)
    if a is None or b is None:
        return None
    return round((b - a).total_seconds(), 3)


def _safe_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _safe_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_jsonable(v) for v in value]
    return str(value)


def write_run_report(
    *,
    state_dir: Path,
    run_id: str,
    payload: dict[str, Any],
) -> Path:
    reports_dir = state_dir / "run_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_path = reports_dir / f"{run_id}.json"
    temp_path = reports_dir / f"{run_id}.tmp"

    safe_payload = _safe_jsonable(payload)
    safe_payload["duration_seconds"] = _duration_seconds(
        str(safe_payload.get("started_at_utc", "")),
        str(safe_payload.get("finished_at_utc", "")),
    )

    temp_path.write_text(json.dumps(safe_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(report_path)

    latest_path = reports_dir / "latest.json"
    latest_temp_path = reports_dir / "latest.tmp"
    latest_temp_path.write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_temp_path.replace(latest_path)

    return report_path

