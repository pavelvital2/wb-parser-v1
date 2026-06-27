from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path


def _load_watchdog():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wb_persistent_session_watchdog.py"
    spec = importlib.util.spec_from_file_location("wb_persistent_session_watchdog", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def test_watchdog_counts_only_new_bad_heartbeats(tmp_path: Path, monkeypatch) -> None:
    watchdog = _load_watchdog()
    runner = tmp_path / "run.sh"
    runner.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    latest = tmp_path / "latest.json"
    state = tmp_path / "watchdog.json"
    checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    _write_json(
        latest,
        {
            "status": "failed",
            "checked_at_utc": checked_at,
            "http_status": 498,
            "antibot": True,
            "cookie_count": 10,
        },
    )
    _write_json(
        state,
        {
            "last_seen_checked_at_utc": checked_at,
            "consecutive_bad_heartbeats": 1,
            "last_restart_utc": "",
        },
    )
    monkeypatch.setattr(watchdog, "tmux_has_session", lambda _session: True)

    code = watchdog.main(
        [
            "--runner",
            str(runner),
            "--state-json",
            str(latest),
            "--watchdog-state",
            str(state),
            "--bad-heartbeats-before-restart",
            "2",
            "--dry-run",
        ]
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["consecutive_bad_heartbeats"] == 1
    assert payload["last_seen_checked_at_utc"] == checked_at
    assert payload["action"] == "noop"
