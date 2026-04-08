from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import AppConfig


def _policy_days(config: AppConfig) -> dict[str, int]:
    retention = config.raw.get("retention", {}) if isinstance(config.raw.get("retention", {}), dict) else {}
    days_cfg = retention.get("days", {}) if isinstance(retention.get("days", {}), dict) else {}
    out: dict[str, int] = {}
    for key, value in days_cfg.items():
        try:
            out[str(key)] = int(value)
        except Exception:
            continue
    return out


def _retention_enabled(config: AppConfig) -> bool:
    retention = config.raw.get("retention", {}) if isinstance(config.raw.get("retention", {}), dict) else {}
    return bool(retention.get("enabled", False))


def _delete_empty_dirs(root: Path) -> int:
    removed = 0
    for d in sorted([p for p in root.rglob("*") if p.is_dir()], reverse=True):
        try:
            if not any(d.iterdir()):
                d.rmdir()
                removed += 1
        except Exception:
            continue
    return removed


def cleanup_runtime_files(config: AppConfig, *, apply: bool) -> dict[str, Any]:
    if not _retention_enabled(config):
        return {
            "enabled": False,
            "apply": apply,
            "files_scanned": 0,
            "files_matched": 0,
            "files_deleted": 0,
            "dirs_deleted": 0,
            "matched_paths": [],
            "note": "retention.disabled",
        }

    now = datetime.now(UTC)
    days = _policy_days(config)
    retention = config.raw.get("retention", {}) if isinstance(config.raw.get("retention", {}), dict) else {}
    delete_empty_dirs = bool(retention.get("delete_empty_dirs", True))

    roots: dict[str, Path] = {
        "logs": config.paths.LOG_DIR,
        "raw": config.paths.RAW_DIR,
        "staging": config.paths.STAGING_DIR,
        "marts": config.paths.MARTS_DIR,
        "exports": config.paths.EXPORTS_DIR,
        "run_reports": config.paths.STATE_DIR / "run_reports",
    }

    files_scanned = 0
    files_matched = 0
    files_deleted = 0
    dirs_deleted = 0
    matched_paths: list[str] = []

    for key, root in roots.items():
        keep_days = int(days.get(key, 0))
        if keep_days <= 0 or not root.exists() or not root.is_dir():
            continue

        cutoff = now - timedelta(days=keep_days)
        for f in root.rglob("*"):
            if not f.is_file():
                continue
            files_scanned += 1

            rel = f.relative_to(root).as_posix().lower()
            if "/latest/" in f"/{rel}/":
                continue

            try:
                modified = datetime.fromtimestamp(f.stat().st_mtime, UTC)
            except Exception:
                continue

            if modified <= cutoff:
                files_matched += 1
                matched_paths.append(str(f))
                if apply:
                    try:
                        f.unlink(missing_ok=True)
                        files_deleted += 1
                    except Exception:
                        continue

        if apply and delete_empty_dirs:
            dirs_deleted += _delete_empty_dirs(root)

    return {
        "enabled": True,
        "apply": apply,
        "files_scanned": files_scanned,
        "files_matched": files_matched,
        "files_deleted": files_deleted,
        "dirs_deleted": dirs_deleted,
        "matched_paths": matched_paths[:200],
        "note": "ok",
    }
