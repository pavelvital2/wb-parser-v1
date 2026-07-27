#!/usr/bin/env python3
from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


TERM_GRACE_SECONDS = 30.0
POLL_SECONDS = 0.1
PR_SET_CHILD_SUBREAPER = 36


def _fail(message: str) -> int:
    print(f"WB lock-v3 supervisor: {message}", file=sys.stderr)
    return 2


def _inherited_fds() -> tuple[int, ...]:
    raw = os.environ.get("PARSER_WB_SUPERVISOR_PASS_FDS", "")
    parts = raw.split(",") if raw else []
    try:
        values = tuple(sorted({int(item) for item in parts}))
    except ValueError as exc:
        raise ValueError("inherited FD list is invalid") from exc
    if not values or any(value < 3 for value in values):
        raise ValueError("inherited FD list is invalid")
    for value in values:
        os.fstat(value)
    guard_fd = int(os.environ["PARSER_WB_LOCK_V3_GUARD_FD"])
    validation_fd = int(
        os.environ["PARSER_WB_LOCK_V3_VALIDATION_FD"]
    )
    if (
        guard_fd == validation_fd
        or guard_fd not in values
        or validation_fd not in values
    ):
        raise ValueError("host lock FDs are not retained")
    return values


def _parse_proc_stat(
    encoded: str,
    *,
    expected_pid: int,
) -> tuple[int, int, str]:
    prefix = f"{expected_pid} ("
    if not encoded.startswith(prefix):
        raise ValueError("process stat PID mismatch")
    closing = encoded.rfind(")")
    if closing < len(prefix):
        raise ValueError("process stat comm is malformed")
    fields = encoded[closing + 1 :].strip().split()
    if len(fields) < 3 or len(fields[0]) != 1:
        raise ValueError("process stat fields are malformed")
    return int(fields[1]), int(fields[2]), fields[0]


def _process_table() -> dict[int, tuple[int, int, str]]:
    table: dict[int, tuple[int, int, str]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            pid = int(entry.name)
            table[pid] = _parse_proc_stat(
                (entry / "stat").read_text(encoding="utf-8"),
                expected_pid=pid,
            )
        except (OSError, ValueError):
            continue
    return table


def _owned_members(pgid: int) -> set[int]:
    table = _process_table()
    members = {
        pid
        for pid, (_ppid, process_group, state) in table.items()
        if process_group == pgid and state != "Z"
    }
    frontier = {os.getpid()}
    while frontier:
        children = {
            pid
            for pid, (ppid, _process_group, state) in table.items()
            if ppid in frontier and state != "Z" and pid != os.getpid()
        }
        children -= members
        if not children:
            break
        members.update(children)
        frontier = children
    return members


def _reap_children() -> None:
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid <= 0:
            return


def _terminate_owned(pgid: int) -> bool:
    def signal_owned(signum: int) -> None:
        for pid in _owned_members(pgid):
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass

    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    signal_owned(signal.SIGTERM)
    deadline = time.monotonic() + TERM_GRACE_SECONDS
    while time.monotonic() < deadline:
        _reap_children()
        if not _owned_members(pgid):
            return True
        time.sleep(POLL_SECONDS)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    signal_owned(signal.SIGKILL)
    deadline = time.monotonic() + TERM_GRACE_SECONDS
    while time.monotonic() < deadline:
        _reap_children()
        if not _owned_members(pgid):
            return True
        time.sleep(POLL_SECONDS)
    return False


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] != "--":
        return _fail(
            "usage: marketplace_lock_v3_supervisor.py -- COMMAND [ARG...]"
        )
    try:
        inherited_fds = _inherited_fds()
    except (KeyError, OSError, ValueError) as exc:
        return _fail(str(exc))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        return _fail("cannot enable child subreaper")

    child_env = os.environ.copy()
    child_env["PARSER_WB_LOCK_V3_VALIDATION_OWNER_PID"] = str(
        os.getpid()
    )
    terminating = False
    child: subprocess.Popen[bytes] | None = None

    def forward(signum: int, _frame: object) -> None:
        nonlocal terminating
        terminating = True
        if child is not None:
            try:
                os.killpg(child.pid, signum)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    try:
        child = subprocess.Popen(
            sys.argv[2:],
            env=child_env,
            close_fds=True,
            pass_fds=inherited_fds,
            start_new_session=True,
        )
    except OSError as exc:
        return _fail(f"child spawn failed: {exc}")

    while child.poll() is None:
        if terminating:
            if not _terminate_owned(child.pid):
                return _fail("child process group cleanup failed")
            break
        time.sleep(POLL_SECONDS)
    status = child.wait()
    pgid = child.pid
    while _owned_members(pgid):
        if terminating:
            if not _terminate_owned(pgid):
                return _fail("child process group cleanup failed")
            break
        _reap_children()
        time.sleep(POLL_SECONDS)
    _reap_children()
    return status if status >= 0 else 128 + abs(status)


if __name__ == "__main__":
    raise SystemExit(main())
