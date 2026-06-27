#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TMUX_SESSION = "wb_persistent_session"


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
    parser.add_argument("--runner", default="scripts/run_wb_persistent_session.sh")
    parser.add_argument("--state-json", default="state/wb_persistent_session/latest.json")
    parser.add_argument("--watchdog-state", default="state/wb_persistent_session/watchdog.json")
    parser.add_argument("--max-age-seconds", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_MAX_AGE_SECONDS", "900")))
    parser.add_argument(
        "--bad-heartbeats-before-restart",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_BAD_HEARTBEATS", "2")),
    )
    parser.add_argument(
        "--restart-cooldown-seconds",
        type=int,
        default=int(os.getenv("PARSER_WB_WATCHDOG_RESTART_COOLDOWN_SECONDS", "600")),
    )
    parser.add_argument("--grace-seconds", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_GRACE_SECONDS", "20")))
    parser.add_argument("--min-cookie-count", type=int, default=int(os.getenv("PARSER_WB_WATCHDOG_MIN_COOKIE_COUNT", "2")))
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = resolve_path(args.runner)
    state_json = resolve_path(args.state_json)
    watchdog_state_path = resolve_path(args.watchdog_state)
    previous_state = read_json(watchdog_state_path)
    latest = read_json(state_json)
    now = utc_now()

    session_active = tmux_has_session(args.tmux_session)
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

    last_restart = parse_utc(previous_state.get("last_restart_utc"))
    cooldown_left = 0
    if should_restart and last_restart is not None:
        elapsed = int((now - last_restart).total_seconds())
        cooldown_left = max(0, int(args.restart_cooldown_seconds) - elapsed)
        if cooldown_left > 0 and session_active:
            should_restart = False
            reason = f"restart_cooldown_{cooldown_left}s"

    action = "noop"
    error = ""
    if should_restart:
        action = "restart" if session_active else "start"
        if not args.dry_run:
            try:
                if session_active:
                    stop_tmux_session(args.tmux_session, int(args.grace_seconds))
                start_tmux_session(args.tmux_session, runner)
            except Exception as exc:
                error = f"{exc.__class__.__name__}: {exc}"
                action = "restart_failed"
        if not error:
            consecutive_bad = 0

    state = {
        "status": "error" if error else "ok",
        "checked_at_utc": utc_now_iso(),
        "tmux_session": args.tmux_session,
        "session_active_before": session_active,
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
        "last_restart_utc": utc_now_iso() if action in {"start", "restart"} and not error else previous_state.get("last_restart_utc", ""),
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
    )
    if error:
        print(f"watchdog error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
