#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TMUX_SESSION = "wb_persistent_session"
DEFAULT_PYTHON_BIN = "/home/Codex/agent-tools/parser_wb-python/bin/python"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def resolve_path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as exc:
        return {"_read_error": f"{exc.__class__.__name__}: {exc}"}
    return data if isinstance(data, dict) else {"_read_error": "json_not_object"}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def run_cmd(command: list[str], *, cwd: Path = PROJECT_ROOT, timeout: int = 15) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
        check=False,
    )


def parse_statuses(value: str) -> set[int]:
    statuses: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            statuses.add(int(item))
        except ValueError:
            continue
    return statuses


def tmux_has_session(session_name: str) -> bool:
    result = run_cmd(["tmux", "has-session", "-t", session_name])
    return result.returncode == 0


def stop_tmux_session(session_name: str, grace_seconds: int) -> None:
    if not tmux_has_session(session_name):
        return
    run_cmd(["tmux", "send-keys", "-t", session_name, "C-c"], timeout=10)
    deadline = time.monotonic() + max(1, grace_seconds)
    while time.monotonic() < deadline:
        if not tmux_has_session(session_name):
            return
        time.sleep(1)
    if tmux_has_session(session_name):
        run_cmd(["tmux", "kill-session", "-t", session_name], timeout=10)


def start_tmux_session(session_name: str, runner: Path) -> None:
    command = f"cd {PROJECT_ROOT} && exec {runner}"
    result = subprocess.run(
        ["tmux", "new-session", "-d", "-s", session_name, command],
        cwd=str(PROJECT_ROOT),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=20,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or "").strip().splitlines()[:1]
        raise RuntimeError(f"tmux start failed: {message[0] if message else result.returncode}")


def profile_reset_enabled(value: str) -> bool:
    return value.strip().lower() not in {"0", "false", "no", "off"}


def watchdog_enabled(value: str) -> bool:
    return value.strip().lower() not in {"0", "false", "no", "off"}


def is_profile_reset_candidate(args: argparse.Namespace, latest: dict[str, Any]) -> bool:
    reset_statuses = parse_statuses(str(args.profile_reset_http_statuses))
    latest_http = int(latest.get("http_status") or 0)
    latest_antibot = bool(latest.get("antibot"))
    return latest_http in reset_statuses or latest_antibot


def run_current_cookie_smoke(
    python_bin: Path,
    config: Path,
    cookie_file: Path,
    *,
    sample_count: int,
    min_successes: int,
    timeout_seconds: int,
) -> bool:
    result = run_cmd(
        [
            str(python_bin),
            str(PROJECT_ROOT / "scripts" / "wb_cookie_keeper.py"),
            "smoke",
            "--config",
            str(config),
            "--cookie-file",
            str(cookie_file),
            "--sample-count",
            str(sample_count),
            "--min-successes",
            str(min_successes),
        ],
        timeout=timeout_seconds,
    )
    return result.returncode == 0


def run_clean_profile_probe(
    python_bin: Path,
    config: Path,
    cookie_file: Path,
    profile_dir: Path,
    storage_state: Path,
    state_json: Path,
    *,
    timeout_ms: int,
    wait_ms: int,
    timeout_seconds: int,
) -> bool:
    result = run_cmd(
        [
            str(python_bin),
            str(PROJECT_ROOT / "scripts" / "wb_persistent_session.py"),
            "--config",
            str(config),
            "--cookie-file",
            str(cookie_file),
            "--profile-dir",
            str(profile_dir),
            "--storage-state",
            str(storage_state),
            "--state-json",
            str(state_json),
            "--timeout-ms",
            str(timeout_ms),
            "--wait-ms",
            str(wait_ms),
            "--oneshot",
            "--no-promote",
        ],
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        return False
    probe_state = read_json(state_json)
    return (
        str(probe_state.get("status") or "") == "ok"
        and int(probe_state.get("http_status") or 0) == 200
        and not bool(probe_state.get("antibot"))
        and int(probe_state.get("cookie_count") or 0) > 0
    )


def maybe_reset_profile(
    args: argparse.Namespace,
    latest: dict[str, Any],
    previous_state: dict[str, Any],
    *,
    session_active: bool,
) -> dict[str, str]:
    result = {
        "status": "skipped",
        "reason": "",
        "archived_profile": "",
        "probe_state_json": "",
    }
    if args.dry_run:
        result["reason"] = "dry_run"
        return result
    if not profile_reset_enabled(str(args.profile_reset)):
        result["reason"] = "disabled"
        return result

    if not is_profile_reset_candidate(args, latest):
        result["reason"] = "latest_not_reset_candidate"
        return result

    last_reset = parse_utc(previous_state.get("last_profile_reset_utc"))
    if last_reset is not None:
        elapsed = int((utc_now() - last_reset).total_seconds())
        cooldown_left = max(0, int(args.profile_reset_cooldown_seconds) - elapsed)
        if cooldown_left > 0:
            result["reason"] = f"profile_reset_cooldown_{cooldown_left}s"
            return result

    python_bin = resolve_path(args.python_bin)
    config = resolve_path(args.config)
    cookie_file = resolve_path(args.cookie_file)
    profile_dir = resolve_path(args.profile_dir)
    storage_state = resolve_path(args.storage_state)
    if not python_bin.exists():
        result["reason"] = "python_missing"
        return result
    if not cookie_file.exists() or cookie_file.stat().st_size == 0:
        result["reason"] = "cookie_missing"
        return result

    smoke_ok = run_current_cookie_smoke(
        python_bin,
        config,
        cookie_file,
        sample_count=int(args.profile_reset_smoke_sample_count),
        min_successes=int(args.profile_reset_smoke_min_successes),
        timeout_seconds=int(args.profile_reset_timeout_seconds),
    )
    if not smoke_ok:
        result["reason"] = "cookie_smoke_failed"
        return result

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    probe_profile = resolve_path(f"state/browser/wb_profile_reset_probe_{stamp}")
    probe_storage = resolve_path(f"state/browser/wb_profile_reset_probe_{stamp}.json")
    probe_state = resolve_path(f"state/wb_persistent_session/profile_reset_probe_{stamp}.json")
    probe_ok = run_clean_profile_probe(
        python_bin,
        config,
        cookie_file,
        probe_profile,
        probe_storage,
        probe_state,
        timeout_ms=int(args.profile_reset_browser_timeout_ms),
        wait_ms=int(args.profile_reset_browser_wait_ms),
        timeout_seconds=int(args.profile_reset_timeout_seconds),
    )
    result["probe_state_json"] = str(probe_state)
    if not probe_ok:
        result["reason"] = "clean_profile_probe_failed"
        return result

    if session_active:
        stop_tmux_session(args.tmux_session, int(args.grace_seconds))

    archived_profile = ""
    if profile_dir.exists():
        archived = profile_dir.with_name(f"{profile_dir.name}.profile_reset_{stamp}")
        shutil.move(str(profile_dir), str(archived))
        archived_profile = str(archived)
    storage_state.unlink(missing_ok=True)
    if profile_dir.exists():
        result["status"] = "failed"
        result["reason"] = "profile_dir_still_exists"
        return result
    shutil.move(str(probe_profile), str(profile_dir))
    try:
        probe_storage.unlink(missing_ok=True)
    except OSError:
        pass
    result["status"] = "applied"
    result["reason"] = "clean_profile_verified"
    result["archived_profile"] = archived_profile
    return result


def is_bad_heartbeat(latest: dict[str, Any], min_cookie_count: int) -> bool:
    if latest.get("_read_error"):
        return True
    if str(latest.get("status") or "") != "ok":
        return True
    if int(latest.get("http_status") or 0) != 200:
        return True
    if bool(latest.get("antibot")):
        return True
    if int(latest.get("cookie_count") or 0) < min_cookie_count:
        return True
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watchdog for the persistent WB browser session.")
    parser.add_argument("--tmux-session", default=os.getenv("PARSER_WB_PERSISTENT_TMUX", DEFAULT_TMUX_SESSION))
    parser.add_argument("--python-bin", default=os.getenv("PARSER_WB_PYTHON_BIN", DEFAULT_PYTHON_BIN))
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--cookie-file", default="config/wb_cookie.txt")
    parser.add_argument("--profile-dir", default="state/browser/wb_persistent_profile")
    parser.add_argument("--storage-state", default="state/browser/wb_storage_state.json")
    parser.add_argument("--runner", default="scripts/run_wb_persistent_session.sh")
    parser.add_argument("--state-json", default="state/wb_persistent_session/latest.json")
    parser.add_argument("--watchdog-state", default="state/wb_persistent_session/watchdog.json")
    parser.add_argument("--max-age-seconds", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_MAX_AGE_SECONDS", "900")))
    parser.add_argument(
        "--bad-heartbeats-before-restart",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_BAD_HEARTBEATS", "1")),
    )
    parser.add_argument(
        "--restart-cooldown-seconds",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_RESTART_COOLDOWN_SECONDS", "600")),
    )
    parser.add_argument("--grace-seconds", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_GRACE_SECONDS", "20")))
    parser.add_argument("--min-cookie-count", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_MIN_COOKIE_COUNT", "2")))
    parser.add_argument("--profile-reset", default=os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET", "1"))
    parser.add_argument(
        "--profile-reset-http-statuses",
        default=os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_HTTP_STATUSES", "498"),
    )
    parser.add_argument(
        "--profile-reset-cooldown-seconds",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_COOLDOWN_SECONDS", "3600")),
    )
    parser.add_argument(
        "--profile-reset-timeout-seconds",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_TIMEOUT_SECONDS", "120")),
    )
    parser.add_argument(
        "--profile-reset-smoke-sample-count",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_SMOKE_SAMPLE_COUNT", "3")),
    )
    parser.add_argument(
        "--profile-reset-smoke-min-successes",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_SMOKE_MIN_SUCCESSES", "2")),
    )
    parser.add_argument(
        "--profile-reset-browser-timeout-ms",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_BROWSER_TIMEOUT_MS", "45000")),
    )
    parser.add_argument(
        "--profile-reset-browser-wait-ms",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_PROFILE_RESET_BROWSER_WAIT_MS", "5000")),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = resolve_path(args.runner)
    state_json = resolve_path(args.state_json)
    watchdog_state_path = resolve_path(args.watchdog_state)
    if not watchdog_enabled(os.getenv("PARSER_WB_PERSISTENT_WATCHDOG_ENABLED", "1")):
        latest = read_json(state_json)
        state = {
            "status": "ok",
            "checked_at_utc": utc_now_iso(),
            "tmux_session": args.tmux_session,
            "session_active_before": tmux_has_session(args.tmux_session),
            "latest_checked_at_utc": str(latest.get("checked_at_utc") or ""),
            "last_seen_checked_at_utc": str(latest.get("checked_at_utc") or ""),
            "heartbeat_age_seconds": None,
            "latest_status": str(latest.get("status") or ""),
            "latest_http_status": int(latest.get("http_status") or 0),
            "latest_antibot": bool(latest.get("antibot")),
            "latest_cookie_count": int(latest.get("cookie_count") or 0),
            "consecutive_bad_heartbeats": 0,
            "action": "disabled",
            "reason": "disabled_by_env",
            "profile_reset_status": "skipped",
            "profile_reset_reason": "disabled_by_env",
            "profile_reset_archived_profile": "",
            "profile_reset_probe_state_json": "",
            "last_restart_utc": "",
            "last_profile_reset_utc": "",
            "error_class": "",
        }
        write_json(watchdog_state_path, state)
        print("watchdog status=ok action=disabled reason=disabled_by_env")
        return 0
    previous_state = read_json(watchdog_state_path)
    latest = read_json(state_json)
    now = utc_now()

    session_active = tmux_has_session(args.tmux_session)
    session_active_before = session_active
    checked_at = parse_utc(latest.get("checked_at_utc"))
    heartbeat_age = int((now - checked_at).total_seconds()) if checked_at else None
    bad_heartbeat = is_bad_heartbeat(latest, args.min_cookie_count)

    previous_checked_at = str(previous_state.get("last_seen_checked_at_utc") or "")
    current_checked_at = str(latest.get("checked_at_utc") or "")
    consecutive_bad = int(previous_state.get("consecutive_bad_heartbeats") or 0)
    if current_checked_at and current_checked_at != previous_checked_at:
        consecutive_bad = consecutive_bad + 1 if bad_heartbeat else 0

    reason = ""
    should_restart = False
    if not runner.exists():
        print(f"watchdog failed: runner not found: {runner}", file=sys.stderr)
        return 2
    if not session_active:
        should_restart = True
        reason = "tmux_session_missing"
    elif latest.get("_read_error"):
        should_restart = True
        reason = "state_json_unreadable"
    elif checked_at is None:
        should_restart = True
        reason = "heartbeat_timestamp_missing"
    elif heartbeat_age is not None and heartbeat_age > int(args.max_age_seconds):
        should_restart = True
        reason = "heartbeat_stale"
    elif bad_heartbeat and consecutive_bad >= max(1, int(args.bad_heartbeats_before_restart)):
        should_restart = True
        reason = "consecutive_bad_heartbeats"

    reset_candidate = bool(
        should_restart
        and reason == "consecutive_bad_heartbeats"
        and is_profile_reset_candidate(args, latest)
    )
    last_restart = parse_utc(previous_state.get("last_restart_utc"))
    cooldown_left = 0
    if should_restart and last_restart is not None:
        elapsed = int((now - last_restart).total_seconds())
        cooldown_left = max(0, int(args.restart_cooldown_seconds) - elapsed)
        if cooldown_left > 0 and session_active and not reset_candidate:
            should_restart = False
            reason = f"restart_cooldown_{cooldown_left}s"

    action = "noop"
    error = ""
    profile_reset = {
        "status": "skipped",
        "reason": "",
        "archived_profile": "",
        "probe_state_json": "",
    }
    if should_restart:
        action = "restart" if session_active else "start"
        if not args.dry_run:
            try:
                should_start_session = True
                if session_active and reason == "consecutive_bad_heartbeats":
                    if reset_candidate:
                        profile_reset = maybe_reset_profile(args, latest, previous_state, session_active=session_active)
                        if profile_reset["status"] == "applied":
                            session_active = False
                            action = "profile_reset_restart"
                        else:
                            action = "profile_reset_skipped"
                            should_start_session = False
                if should_start_session and session_active:
                    stop_tmux_session(args.tmux_session, int(args.grace_seconds))
                if should_start_session:
                    start_tmux_session(args.tmux_session, runner)
            except Exception as exc:
                error = f"{exc.__class__.__name__}: {exc}"
                action = "restart_failed"
        if not error and action in {"start", "restart", "profile_reset_restart"}:
            consecutive_bad = 0

    state = {
        "status": "error" if error else "ok",
        "checked_at_utc": utc_now_iso(),
        "tmux_session": args.tmux_session,
        "session_active_before": session_active_before,
        "latest_checked_at_utc": current_checked_at,
        "last_seen_checked_at_utc": current_checked_at,
        "heartbeat_age_seconds": heartbeat_age,
        "latest_status": str(latest.get("status") or ""),
        "latest_http_status": int(latest.get("http_status") or 0),
        "latest_antibot": bool(latest.get("antibot")),
        "latest_cookie_count": int(latest.get("cookie_count") or 0),
        "consecutive_bad_heartbeats": consecutive_bad,
        "action": action,
        "reason": reason,
        "profile_reset_status": profile_reset["status"],
        "profile_reset_reason": profile_reset["reason"],
        "profile_reset_archived_profile": profile_reset["archived_profile"],
        "profile_reset_probe_state_json": profile_reset["probe_state_json"],
        "last_restart_utc": utc_now_iso()
        if action in {"start", "restart", "profile_reset_restart"} and not error
        else previous_state.get("last_restart_utc", ""),
        "last_profile_reset_utc": utc_now_iso()
        if profile_reset["status"] == "applied" and not error
        else previous_state.get("last_profile_reset_utc", ""),
        "error_class": error.split(":", 1)[0] if error else "",
    }
    write_json(watchdog_state_path, state)

    print(
        "watchdog",
        f"status={state['status']}",
        f"action={action}",
        f"reason={reason or 'healthy'}",
        f"session_active={str(session_active).lower()}",
        f"heartbeat_age={heartbeat_age if heartbeat_age is not None else 'unknown'}",
        f"bad_heartbeats={consecutive_bad}",
        f"profile_reset={profile_reset['status']}",
        f"profile_reset_reason={profile_reset['reason'] or 'none'}",
    )
    if error:
        print(f"watchdog error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
