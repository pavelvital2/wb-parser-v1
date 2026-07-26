#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEEPER_PATH = PROJECT_ROOT / "scripts" / "wb_cookie_keeper.py"
EXIT_OK = 0
EXIT_PREFLIGHT_FAILED = 20


def load_keeper():
    spec = importlib.util.spec_from_file_location("wb_cookie_keeper", KEEPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load keeper module: {KEEPER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


keeper = load_keeper()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def resolve_path(value: str | Path) -> Path:
    return keeper.resolve_path(value)


def secure_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f"{dst.name}.tmp")
    shutil.copyfile(src, tmp)
    tmp.chmod(stat.S_IRUSR | stat.S_IWUSR)
    tmp.replace(dst)
    dst.chmod(stat.S_IRUSR | stat.S_IWUSR)


def write_state(path_value: str, payload: dict[str, Any]) -> None:
    if not path_value:
        return
    path = resolve_path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def backup_paths(backup_dir: Path) -> list[Path]:
    if not backup_dir.exists():
        return []
    return sorted(backup_dir.glob("wb_cookie.known_good_*.txt"), reverse=True)


def prune_backups(backup_dir: Path, retain: int) -> list[str]:
    removed: list[str] = []
    if retain <= 0:
        return removed
    for old_path in backup_paths(backup_dir)[retain:]:
        try:
            old_path.unlink()
            removed.append(str(old_path))
        except OSError:
            pass
    return removed


def save_known_good(cookie_path: Path, backup_dir: Path, retain: int) -> Path:
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"wb_cookie.known_good_{stamp}.txt"
    secure_copy(cookie_path, backup_path)
    prune_backups(backup_dir, retain)
    return backup_path


def smoke_cookie(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path) -> bool:
    smoke_args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json="",
        query=args.query,
        sample_count=args.sample_count,
        min_successes=args.min_successes,
        page=args.page,
    )
    return keeper.smoke(config, smoke_args, emit=False)


def backup_current(config: dict[str, Any], args: argparse.Namespace) -> int:
    cookie_path = keeper.resolve_cookie_path(config, args.cookie_file)
    backup_dir = resolve_path(args.backup_dir)
    ok = smoke_cookie(config, args, cookie_path)
    payload: dict[str, Any] = {
        "status": "ok" if ok else "failed",
        "mode": "backup",
        "checked_at_utc": utc_now_iso(),
        "cookie_file": str(cookie_path),
        "backup_dir": str(backup_dir),
        "actions": [],
    }
    if not ok:
        payload["actions"].append("current_cookie_smoke_failed")
        write_state(args.state_json, payload)
        print("nightly_preflight backup failed: current cookie smoke failed", file=sys.stderr)
        return EXIT_PREFLIGHT_FAILED

    backup_path = save_known_good(cookie_path, backup_dir, args.retain)
    payload["backup_path"] = str(backup_path)
    payload["actions"].append("known_good_saved")
    write_state(args.state_json, payload)
    print(f"nightly_preflight backup ok: backup_path={backup_path}")
    return EXIT_OK


def try_restore_known_good(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path) -> Path | None:
    backup_dir = resolve_path(args.backup_dir)
    for candidate in backup_paths(backup_dir):
        if smoke_cookie(config, args, candidate):
            secure_copy(candidate, cookie_path)
            return candidate
    return None


def preflight(config: dict[str, Any], args: argparse.Namespace) -> int:
    cookie_path = keeper.resolve_cookie_path(config, args.cookie_file)
    backup_dir = resolve_path(args.backup_dir)
    actions: list[str] = []
    payload: dict[str, Any] = {
        "status": "failed",
        "mode": "preflight",
        "checked_at_utc": utc_now_iso(),
        "cookie_file": str(cookie_path),
        "backup_dir": str(backup_dir),
        "actions": actions,
    }

    if smoke_cookie(config, args, cookie_path):
        backup_path = save_known_good(cookie_path, backup_dir, args.retain)
        actions.extend(["current_cookie_smoke_ok", "known_good_saved"])
        payload["status"] = "ok"
        payload["backup_path"] = str(backup_path)
        write_state(args.state_json, payload)
        print(f"nightly_preflight ok: current cookie smoke passed; backup_path={backup_path}")
        return EXIT_OK

    actions.append("current_cookie_smoke_failed")
    restored_from = try_restore_known_good(config, args, cookie_path)
    if restored_from is not None:
        actions.append("restored_known_good")
        payload["status"] = "ok"
        payload["restored_from"] = str(restored_from)
        write_state(args.state_json, payload)
        print(f"nightly_preflight ok: restored known-good cookie from {restored_from}")
        return EXIT_OK

    actions.append("known_good_restore_failed")
    if not args.no_refresh:
        refresh_args = argparse.Namespace(
            cookie_file=args.cookie_file,
            state_json=args.keeper_state_json,
            query=args.query,
            sample_count=args.sample_count,
            min_successes=args.min_successes,
            page=args.page,
            storage_state=args.storage_state,
            storage_state_out=args.storage_state_out,
            require_storage_state=args.require_storage_state,
            browser_channel=args.browser_channel,
            headed=args.headed,
            no_headless=args.no_headless,
            refresh_url=args.refresh_url,
            wait_ms=args.wait_ms,
            timeout_ms=args.timeout_ms,
        )
        if keeper.refresh_and_promote(config, refresh_args) and smoke_cookie(config, args, cookie_path):
            backup_path = save_known_good(cookie_path, backup_dir, args.retain)
            actions.extend(["refresh_promoted", "known_good_saved"])
            payload["status"] = "ok"
            payload["backup_path"] = str(backup_path)
            write_state(args.state_json, payload)
            print(f"nightly_preflight ok: refresh promoted; backup_path={backup_path}")
            return EXIT_OK
        actions.append("refresh_failed")

    write_state(args.state_json, payload)
    print("nightly_preflight failed: current, known-good restore, and refresh did not pass smoke", file=sys.stderr)
    return EXIT_PREFLIGHT_FAILED


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WB nightly access preflight with known-good cookie rollback.")
    parser.add_argument("command", choices=["preflight", "backup"])
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--cookie-file", default="")
    parser.add_argument("--state-json", default="state/wb_nightly_preflight/latest.json")
    parser.add_argument("--keeper-state-json", default="state/wb_session_keeper/latest.json")
    parser.add_argument("--backup-dir", default="state/wb_known_good")
    parser.add_argument("--retain", type=int, default=5)
    parser.add_argument("--query", default="")
    parser.add_argument("--sample-count", type=int, default=3)
    parser.add_argument("--min-successes", type=int, default=0)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--no-refresh", action="store_true")
    parser.add_argument("--refresh-url", default="https://www.wildberries.ru/")
    parser.add_argument("--storage-state", default="")
    parser.add_argument("--storage-state-out", default="")
    parser.add_argument("--require-storage-state", action="store_true")
    parser.add_argument("--browser-channel", default="")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--timeout-ms", type=int, default=45000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = keeper.load_config(resolve_path(args.config))
    if args.command == "backup":
        return backup_current(config, args)
    return preflight(config, args)


if __name__ == "__main__":
    raise SystemExit(main())
