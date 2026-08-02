from __future__ import annotations

import importlib.util
import json
import subprocess
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


def _allow_official_lease(watchdog, monkeypatch) -> None:
    monkeypatch.setattr(
        watchdog,
        "require_official_live_entry_lease",
        lambda **_kwargs: 1,
    )


def test_watchdog_counts_only_new_bad_heartbeats(tmp_path: Path, monkeypatch) -> None:
    watchdog = _load_watchdog()
    _allow_official_lease(watchdog, monkeypatch)
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


def test_watchdog_can_be_disabled_by_env(tmp_path: Path, monkeypatch) -> None:
    watchdog = _load_watchdog()
    _allow_official_lease(watchdog, monkeypatch)
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
    monkeypatch.setenv("PARSER_WB_PERSISTENT_WATCHDOG_ENABLED", "0")
    monkeypatch.setattr(watchdog, "tmux_has_session", lambda _session: True)
    monkeypatch.setattr(watchdog, "start_tmux_session", lambda *_args: (_ for _ in ()).throw(AssertionError("started")))

    code = watchdog.main(
        [
            "--runner",
            str(runner),
            "--state-json",
            str(latest),
            "--watchdog-state",
            str(state),
        ]
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["action"] == "disabled"
    assert payload["reason"] == "disabled_by_env"
    assert payload["consecutive_bad_heartbeats"] == 0


def test_watchdog_profile_reset_after_first_498(tmp_path: Path, monkeypatch) -> None:
    watchdog = _load_watchdog()
    _allow_official_lease(watchdog, monkeypatch)
    runner = tmp_path / "run.sh"
    runner.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text("{}", encoding="utf-8")
    cookie_file = tmp_path / "wb_cookie.txt"
    cookie_file.write_text("a=b\n", encoding="utf-8")
    python_bin = tmp_path / "python"
    python_bin.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    profile_dir = tmp_path / "wb_persistent_profile"
    profile_dir.mkdir()
    (profile_dir / "old").write_text("old", encoding="utf-8")
    storage_state = tmp_path / "wb_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")
    latest = tmp_path / "latest.json"
    state = tmp_path / "watchdog.json"
    previous_checked_at = "2026-06-27T00:00:00+00:00"
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
            "last_seen_checked_at_utc": previous_checked_at,
            "consecutive_bad_heartbeats": 0,
            "last_restart_utc": checked_at,
            "last_profile_reset_utc": "",
        },
    )

    calls: list[str] = []

    def fake_run_cmd(command: list[str], **_kwargs):
        command_text = " ".join(command)
        if "wb_cookie_keeper.py" in command_text:
            return subprocess.CompletedProcess(command, 0)
        if "wb_persistent_session.py" in command_text:
            probe_profile = Path(command[command.index("--profile-dir") + 1])
            probe_state = Path(command[command.index("--state-json") + 1])
            probe_profile.mkdir(parents=True)
            (probe_profile / "probe").write_text("probe", encoding="utf-8")
            _write_json(
                probe_state,
                {
                    "status": "ok",
                    "http_status": 200,
                    "antibot": False,
                    "cookie_count": 10,
                },
            )
            return subprocess.CompletedProcess(command, 0)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(watchdog, "tmux_has_session", lambda _session: True)
    monkeypatch.setattr(watchdog, "run_cmd", fake_run_cmd)
    monkeypatch.setattr(watchdog, "stop_tmux_session", lambda *_args: calls.append("stop"))
    monkeypatch.setattr(watchdog, "start_tmux_session", lambda *_args: calls.append("start"))

    code = watchdog.main(
        [
            "--python-bin",
            str(python_bin),
            "--config",
            str(config),
            "--cookie-file",
            str(cookie_file),
            "--profile-dir",
            str(profile_dir),
            "--storage-state",
            str(storage_state),
            "--runner",
            str(runner),
            "--state-json",
            str(latest),
            "--watchdog-state",
            str(state),
            "--profile-reset-cooldown-seconds",
            "0",
        ]
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    archived = Path(payload["profile_reset_archived_profile"])
    assert code == 0
    assert payload["action"] == "profile_reset_restart"
    assert payload["profile_reset_status"] == "applied"
    assert payload["profile_reset_reason"] == "clean_profile_verified"
    assert payload["consecutive_bad_heartbeats"] == 0
    assert calls == ["stop", "start"]
    assert archived.exists()
    assert (archived / "old").read_text(encoding="utf-8") == "old"
    assert (profile_dir / "probe").read_text(encoding="utf-8") == "probe"
    assert not storage_state.exists()


def test_watchdog_does_not_restart_same_profile_when_clean_probe_fails(tmp_path: Path, monkeypatch) -> None:
    watchdog = _load_watchdog()
    _allow_official_lease(watchdog, monkeypatch)
    runner = tmp_path / "run.sh"
    runner.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text("{}", encoding="utf-8")
    cookie_file = tmp_path / "wb_cookie.txt"
    cookie_file.write_text("a=b\n", encoding="utf-8")
    python_bin = tmp_path / "python"
    python_bin.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    profile_dir = tmp_path / "wb_persistent_profile"
    profile_dir.mkdir()
    (profile_dir / "old").write_text("old", encoding="utf-8")
    storage_state = tmp_path / "wb_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")
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
            "last_seen_checked_at_utc": "",
            "consecutive_bad_heartbeats": 0,
            "last_restart_utc": checked_at,
            "last_profile_reset_utc": "",
        },
    )

    calls: list[str] = []

    def fake_run_cmd(command: list[str], **_kwargs):
        command_text = " ".join(command)
        if "wb_cookie_keeper.py" in command_text:
            return subprocess.CompletedProcess(command, 0)
        if "wb_persistent_session.py" in command_text:
            probe_profile = Path(command[command.index("--profile-dir") + 1])
            probe_state = Path(command[command.index("--state-json") + 1])
            probe_profile.mkdir(parents=True)
            _write_json(
                probe_state,
                {
                    "status": "failed",
                    "http_status": 498,
                    "antibot": True,
                    "cookie_count": 10,
                },
            )
            return subprocess.CompletedProcess(command, 20)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(watchdog, "tmux_has_session", lambda _session: True)
    monkeypatch.setattr(watchdog, "run_cmd", fake_run_cmd)
    monkeypatch.setattr(watchdog, "stop_tmux_session", lambda *_args: calls.append("stop"))
    monkeypatch.setattr(watchdog, "start_tmux_session", lambda *_args: calls.append("start"))

    code = watchdog.main(
        [
            "--python-bin",
            str(python_bin),
            "--config",
            str(config),
            "--cookie-file",
            str(cookie_file),
            "--profile-dir",
            str(profile_dir),
            "--storage-state",
            str(storage_state),
            "--runner",
            str(runner),
            "--state-json",
            str(latest),
            "--watchdog-state",
            str(state),
            "--profile-reset-cooldown-seconds",
            "0",
        ]
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["action"] == "profile_reset_skipped"
    assert payload["profile_reset_status"] == "skipped"
    assert payload["profile_reset_reason"] == "clean_profile_probe_failed"
    assert payload["consecutive_bad_heartbeats"] == 1
    assert calls == []
    assert (profile_dir / "old").read_text(encoding="utf-8") == "old"
    assert storage_state.exists()
