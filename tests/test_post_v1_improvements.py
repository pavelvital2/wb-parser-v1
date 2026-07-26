from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

from app.common.cleanup import cleanup_runtime_files
from app.common.config import load_config
from app.common.exceptions import ConfigValidationError, RunLockedError
from app.common.runner import run_component
from app.common.state_db import StateDB
import app.common.runner as runner_mod
import app.common.state_db as state_db_mod


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_config(
    tmp_path: Path,
    *,
    retry_max_attempts: int = 5,
    retention_enabled: bool = False,
) -> Path:
    cfg = f"""
project:
  name: test-post-v1
  source_system: wildberries
  timezone: Europe/Moscow

paths:
  data_raw: {str((tmp_path / 'data' / 'raw')).replace('\\', '/')}
  data_staging: {str((tmp_path / 'data' / 'staging')).replace('\\', '/')}
  data_marts: {str((tmp_path / 'data' / 'marts')).replace('\\', '/')}
  logs: {str((tmp_path / 'data' / 'logs')).replace('\\', '/')}
  exports: {str((tmp_path / 'exports')).replace('\\', '/')}
  state_sqlite: {str((tmp_path / 'state' / 'sqlite' / 'state.sqlite')).replace('\\', '/')}
  checkpoints_dir: {str((tmp_path / 'state' / 'checkpoints')).replace('\\', '/')}

runtime:
  retry_max_attempts: {retry_max_attempts}
  retry_base_delay_seconds: 0.0
  retry_max_delay_seconds: 0.0
  http_timeout_seconds: 5
  dry_run: false
  debug: false
  locking_enabled: true
  lock_stale_seconds: 3600

retention:
  enabled: {str(retention_enabled).lower()}
  delete_empty_dirs: true
  days:
    logs: 1
    raw: 1
    staging: 1
    marts: 1
    exports: 1
    run_reports: 1

suggest:
  prefixes_file: config/prefixes.txt

filter:
  rules_file: config/query_rules.yaml
"""
    cfg_path = tmp_path / "config" / "config.yaml"
    _write_text(cfg_path, cfg)
    _write_text(tmp_path / "config" / "prefixes.txt", "шеврон\n")
    _write_text(tmp_path / "config" / "query_rules.yaml", "default:\n  top_n: 10\n")
    return cfg_path


def test_run_report_created_after_successful_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()

    monkeypatch.setattr(runner_mod, "_resolve_components", lambda target: ["filter"])
    monkeypatch.setattr(
        runner_mod,
        "_dispatch_component",
        lambda config, db, ctx: {
            "items_ok": 3,
            "items_error": 0,
            "note": "ok",
            "mart_path": str(config.paths.MARTS_DIR / "filter" / ctx.run_id / "top_queries.csv"),
        },
    )

    code = run_component(config=cfg, db=db, target="filter")
    assert code == 0

    run_row = db.list_runs(limit=1)[0]
    run_id = run_row["run_id"]
    report_path = cfg.paths.STATE_DIR / "run_reports" / f"{run_id}.json"
    assert report_path.exists()

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "success"
    assert payload["totals"]["items_ok"] == 3
    assert len(payload["components"]) == 1


def test_run_lock_blocks_parallel_run(tmp_path: Path) -> None:
    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()

    lock_dir = cfg.paths.STATE_DIR / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / "pipeline.lock"
    lock_file.write_text(json.dumps({"pid": os.getpid(), "target": "daily"}), encoding="utf-8")

    with pytest.raises(RunLockedError):
        run_component(config=cfg, db=db, target="filter")


def test_state_db_persists_error_code(tmp_path: Path) -> None:
    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()

    db.record_error(
        run_id="r1",
        component="serp",
        severity="non_critical",
        error_code="NETWORK_ERROR",
        error_class="SerpPageError",
        error_message="timeout",
        source_ref="q1|1",
        created_at_utc="2026-03-07T00:00:00+00:00",
    )
    rows = db.list_errors("r1")
    assert rows
    assert rows[0]["error_code"] == "NETWORK_ERROR"


def test_state_db_closes_connections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[sqlite3.Connection] = []
    real_connect = state_db_mod.sqlite3.connect

    class TrackingConnection(sqlite3.Connection):
        def close(self) -> None:
            closed.append(self)
            super().close()

    def _connect(*args, **kwargs):
        kwargs["factory"] = TrackingConnection
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(state_db_mod.sqlite3, "connect", _connect)

    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()
    db.save_checkpoint("sellers", "1001", "success|r1|t", "t")
    assert db.get_checkpoint("sellers", "1001") == "success|r1|t"

    assert len(closed) >= 3
    with pytest.raises(sqlite3.ProgrammingError):
        closed[-1].execute("SELECT 1")


def test_cleanup_dry_run_collects_old_files(tmp_path: Path) -> None:
    cfg = load_config(str(_make_config(tmp_path, retention_enabled=True)))

    old_log = cfg.paths.LOG_DIR / "old.log"
    old_log.parent.mkdir(parents=True, exist_ok=True)
    old_log.write_text("old", encoding="utf-8")
    two_days_ago = time.time() - (2 * 24 * 3600)
    os.utime(old_log, (two_days_ago, two_days_ago))

    result = cleanup_runtime_files(cfg, apply=False)
    assert result["enabled"] is True
    assert result["files_matched"] >= 1
    assert old_log.exists()


def test_config_validation_rejects_invalid_runtime(tmp_path: Path) -> None:
    cfg_path = _make_config(tmp_path, retry_max_attempts=0)
    with pytest.raises(ConfigValidationError):
        load_config(str(cfg_path))
