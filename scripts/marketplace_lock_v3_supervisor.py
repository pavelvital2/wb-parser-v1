#!/usr/bin/env python3
from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


TERM_GRACE_SECONDS = 30.0
POLL_SECONDS = 0.1
PR_SET_CHILD_SUBREAPER = 36


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    starttime: int


@dataclass(frozen=True)
class ProcessStat:
    ppid: int
    pgrp: int
    state: str
    starttime: int


class PidfdCapabilityError(RuntimeError):
    pass


class PidfdSignalError(RuntimeError):
    pass


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
) -> ProcessStat:
    prefix = f"{expected_pid} ("
    if not encoded.startswith(prefix):
        raise ValueError("process stat PID mismatch")
    closing = encoded.rfind(")")
    if closing < len(prefix):
        raise ValueError("process stat comm is malformed")
    fields = encoded[closing + 1 :].strip().split()
    if len(fields) < 20 or len(fields[0]) != 1:
        raise ValueError("process stat fields are malformed")
    return ProcessStat(
        ppid=int(fields[1]),
        pgrp=int(fields[2]),
        state=fields[0],
        starttime=int(fields[19]),
    )


def _process_table() -> dict[int, ProcessStat]:
    table: dict[int, ProcessStat] = {}
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


def _identity(pid: int, table: dict[int, ProcessStat]) -> ProcessIdentity | None:
    process = table.get(pid)
    if process is None or process.state == "Z":
        return None
    return ProcessIdentity(pid=pid, starttime=process.starttime)


def _refresh_owned(
    owned: set[ProcessIdentity],
    *,
    supervisor_pid: int,
    baseline_children: set[ProcessIdentity],
    table: dict[int, ProcessStat] | None = None,
) -> set[ProcessIdentity]:
    current = table if table is not None else _process_table()
    active = {
        identity
        for identity in owned
        if _identity(identity.pid, current) == identity
    }
    adopted = {
        ProcessIdentity(pid=pid, starttime=process.starttime)
        for pid, process in current.items()
        if (
            process.ppid == supervisor_pid
            and process.state != "Z"
            and pid != supervisor_pid
        )
    }
    active.update(adopted - baseline_children)
    while True:
        parent_pids = {identity.pid for identity in active}
        children = {
            ProcessIdentity(pid=pid, starttime=process.starttime)
            for pid, process in current.items()
            if (
                process.ppid in parent_pids
                and process.state != "Z"
                and pid != supervisor_pid
            )
        }
        expanded = active | children
        if expanded == active:
            return active
        active = expanded


def _current_identity(pid: int) -> ProcessIdentity | None:
    try:
        process = _parse_proc_stat(
            Path(f"/proc/{pid}/stat").read_text(encoding="utf-8"),
            expected_pid=pid,
        )
    except (OSError, ValueError):
        return None
    if process.state == "Z":
        return None
    return ProcessIdentity(pid=pid, starttime=process.starttime)


def _require_pidfd_capability() -> None:
    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        raise PidfdCapabilityError("pidfd capability is unavailable")
    pid = os.getpid()
    expected = _current_identity(pid)
    if expected is None:
        raise PidfdCapabilityError("supervisor identity is unavailable")
    try:
        pidfd = pidfd_open(pid, 0)
    except OSError as exc:
        raise PidfdCapabilityError("pidfd capability check failed") from exc
    try:
        if _current_identity(pid) != expected:
            raise PidfdCapabilityError(
                "supervisor identity changed during pidfd check"
            )
        pidfd_send_signal(pidfd, 0, None, 0)
    except OSError as exc:
        raise PidfdCapabilityError("pidfd capability check failed") from exc
    finally:
        os.close(pidfd)


def _signal_identity(identity: ProcessIdentity, signum: int) -> bool:
    if _current_identity(identity.pid) != identity:
        return False
    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        raise PidfdSignalError("pidfd capability became unavailable")
    try:
        pidfd = pidfd_open(identity.pid, 0)
    except OSError as exc:
        if _current_identity(identity.pid) != identity:
            return False
        raise PidfdSignalError("pidfd open failed") from exc
    try:
        if _current_identity(identity.pid) != identity:
            return False
        try:
            pidfd_send_signal(pidfd, signum, None, 0)
        except OSError as exc:
            if _current_identity(identity.pid) != identity:
                return False
            raise PidfdSignalError("pidfd signal failed") from exc
        return True
    finally:
        os.close(pidfd)


def _reap_children() -> None:
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid <= 0:
            return


def _terminate_owned(
    owned: set[ProcessIdentity],
    *,
    supervisor_pid: int,
    baseline_children: set[ProcessIdentity],
) -> bool:
    def remaining() -> set[ProcessIdentity]:
        return _refresh_owned(
            owned,
            supervisor_pid=supervisor_pid,
            baseline_children=baseline_children,
        )

    active = remaining()
    signal_failed = False
    for identity in active:
        try:
            _signal_identity(identity, signal.SIGTERM)
        except PidfdSignalError:
            signal_failed = True
            break
    if signal_failed:
        while active:
            time.sleep(POLL_SECONDS)
            active = _refresh_owned(
                active,
                supervisor_pid=supervisor_pid,
                baseline_children=baseline_children,
            )
        return False
    deadline = time.monotonic() + TERM_GRACE_SECONDS
    while time.monotonic() < deadline:
        active = _refresh_owned(
            active,
            supervisor_pid=supervisor_pid,
            baseline_children=baseline_children,
        )
        if not active:
            return True
        time.sleep(POLL_SECONDS)
    for identity in active:
        try:
            _signal_identity(identity, signal.SIGKILL)
        except PidfdSignalError:
            signal_failed = True
            break
    if signal_failed:
        while active:
            time.sleep(POLL_SECONDS)
            active = _refresh_owned(
                active,
                supervisor_pid=supervisor_pid,
                baseline_children=baseline_children,
            )
        return False
    deadline = time.monotonic() + TERM_GRACE_SECONDS
    while time.monotonic() < deadline:
        active = _refresh_owned(
            active,
            supervisor_pid=supervisor_pid,
            baseline_children=baseline_children,
        )
        if not active:
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
    try:
        _require_pidfd_capability()
    except PidfdCapabilityError as exc:
        return _fail(str(exc))

    supervisor_pid = os.getpid()
    initial_table = _process_table()
    baseline_children = {
        ProcessIdentity(pid=pid, starttime=process.starttime)
        for pid, process in initial_table.items()
        if process.ppid == supervisor_pid and process.state != "Z"
    }
    child_env = os.environ.copy()
    child_env["PARSER_WB_LOCK_V3_VALIDATION_OWNER_PID"] = str(
        supervisor_pid
    )
    terminating = False

    def request_termination(_signum: int, _frame: object) -> None:
        nonlocal terminating
        terminating = True

    signal.signal(signal.SIGTERM, request_termination)
    signal.signal(signal.SIGINT, request_termination)
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

    owned: set[ProcessIdentity] = set()
    identity_deadline = time.monotonic() + 1.0
    while time.monotonic() < identity_deadline:
        table = _process_table()
        leader = _identity(child.pid, table)
        if leader is not None:
            owned.add(leader)
            break
        if child.poll() is not None:
            break
        time.sleep(POLL_SECONDS)

    status: int | None = None
    while True:
        owned = _refresh_owned(
            owned,
            supervisor_pid=supervisor_pid,
            baseline_children=baseline_children,
        )
        if terminating:
            if not _terminate_owned(
                owned,
                supervisor_pid=supervisor_pid,
                baseline_children=baseline_children,
            ):
                return _fail("owned descendant cleanup failed")
            if status is None:
                status = child.poll()
                if status is None:
                    status = child.wait()
            break
        if status is None:
            status = child.poll()
        if status is not None:
            _reap_children()
            owned = _refresh_owned(
                owned,
                supervisor_pid=supervisor_pid,
                baseline_children=baseline_children,
            )
            if not owned:
                break
        time.sleep(POLL_SECONDS)
    _reap_children()
    if status is None:
        status = child.wait()
    return status if status >= 0 else 128 + abs(status)


if __name__ == "__main__":
    raise SystemExit(main())
