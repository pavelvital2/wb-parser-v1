from __future__ import annotations

import ctypes
import errno
import json
import os
import socket
import time
from contextlib import contextmanager
from ctypes import wintypes
from pathlib import Path
from typing import Iterator

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from .exceptions import RunLockedError
from .run_context import utc_now_iso


def _lock_file(state_dir: Path) -> Path:
    lock_dir = state_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / "pipeline.lock"


def _guard_file(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.name}.guard")


def _lock_fd(fd: int, *, blocking: bool) -> None:
    if os.name != "nt":
        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB
        fcntl.flock(fd, operation)
        return

    if os.fstat(fd).st_size == 0:
        os.write(fd, b"\0")
        os.fsync(fd)
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise
            if not blocking:
                raise BlockingIOError(exc.errno, str(exc)) from exc
            time.sleep(0.05)


def _unlock_fd(fd: int) -> None:
    if os.name != "nt":
        fcntl.flock(fd, fcntl.LOCK_UN)
        return
    os.lseek(fd, 0, os.SEEK_SET)
    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


@contextmanager
def _acquire_recovery_guard(
    lock_path: Path,
    *,
    blocking: bool = True,
) -> Iterator[Path]:
    guard_path = _guard_file(lock_path)
    guard_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(guard_path, os.O_CREAT | os.O_RDWR)
    locked = False
    try:
        _lock_fd(fd, blocking=blocking)
        locked = True
        yield guard_path
    finally:
        try:
            if locked:
                _unlock_fd(fd)
        finally:
            os.close(fd)


@contextmanager
def acquire_advisory_lock(path: Path) -> Iterator[Path]:
    """Acquire a persistent-file advisory lock without waiting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR)
    locked = False
    try:
        try:
            _lock_fd(fd, blocking=False)
            locked = True
        except BlockingIOError as exc:
            raise RunLockedError(f"Another run is active. lock={path}") from exc
        yield path
    finally:
        if locked:
            try:
                _unlock_fd(fd)
            except OSError:
                pass
        os.close(fd)


def _pid_alive_windows(pid: int) -> bool:
    process_query_limited_information = 0x1000
    still_active = 259
    access_denied = 5

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return ctypes.get_last_error() == access_denied
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return True
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def _pid_alive(pid: object) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    if os.name == "nt":
        return _pid_alive_windows(pid_int)
    try:
        os.kill(pid_int, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _has_advisory_owner(lock_path: Path) -> bool:
    try:
        fd = os.open(lock_path, os.O_RDWR)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    try:
        locked = False
        try:
            _lock_fd(fd, blocking=False)
            locked = True
        except BlockingIOError:
            return True
        finally:
            if locked:
                try:
                    _unlock_fd(fd)
                except OSError:
                    pass
        return False
    finally:
        os.close(fd)


def _read_lock(lock_path: Path) -> tuple[str, dict[str, object] | None]:
    raw = lock_path.read_text(encoding="utf-8")
    if not raw.strip():
        return raw, None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw, None
    if not isinstance(payload, dict):
        return raw, None
    return raw, payload


def _recover_existing_lock(lock_path: Path, *, stale_seconds: int) -> tuple[bool, str, str]:
    if _has_advisory_owner(lock_path):
        return False, "active_advisory_lock", ""

    try:
        raw, payload = _read_lock(lock_path)
    except FileNotFoundError:
        return True, "missing_lock_race", ""
    except Exception:
        return True, "unreadable_lock", ""

    if payload is None:
        if raw.strip():
            return True, "corrupt_lock", raw[:300]
        return True, "empty_lock", ""

    pid = payload.get("pid")
    if _pid_alive(pid):
        return False, "active_pid", raw[:300]

    if pid is None:
        return True, "missing_pid", raw[:300]

    if stale_seconds > 0:
        try:
            age_seconds = max(0.0, time.time() - lock_path.stat().st_mtime)
        except Exception:
            age_seconds = 0.0
        if age_seconds > stale_seconds:
            return True, "stale_dead_owner_lock", raw[:300]

    return True, "dead_owner_lock", raw[:300]


@contextmanager
def acquire_run_lock(
    *,
    state_dir: Path,
    target: str,
    run_id: str,
    enabled: bool,
    stale_seconds: int,
    guard_blocking: bool = True,
) -> Iterator[Path | None]:
    if not enabled:
        yield None
        return

    lock_path = _lock_file(state_dir)

    def _create_lock() -> int:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        try:
            _lock_fd(fd, blocking=False)
        except Exception:
            os.close(fd)
            raise
        return fd

    f = None
    try:
        with _acquire_recovery_guard(lock_path, blocking=guard_blocking):
            try:
                fd = _create_lock()
            except FileExistsError:
                recoverable, reason, meta = _recover_existing_lock(lock_path, stale_seconds=stale_seconds)
                if not recoverable:
                    raise RunLockedError(f"Another run is active. lock={lock_path} reason={reason} meta={meta[:300]}")

                try:
                    lock_path.unlink(missing_ok=True)
                    fd = _create_lock()
                except FileExistsError as exc:
                    raise RunLockedError(f"Run lock recovery raced with another run: {lock_path} reason={reason}") from exc
                except Exception as exc:
                    raise RunLockedError(f"Run lock recovery failed: {lock_path} reason={reason} ({exc})") from exc

            payload = {
                "lock_version": 2,
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "hostname": socket.gethostname(),
                "target": target,
                "run_id": run_id,
                "started_at_utc": utc_now_iso(),
            }

            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            f = os.fdopen(fd, "w", encoding="utf-8")
            try:
                f.write(json.dumps(payload, ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                try:
                    lock_path.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    f.close()
                except Exception:
                    pass
                raise
    except BlockingIOError as exc:
        raise RunLockedError(
            f"Run lock recovery guard is busy: {_guard_file(lock_path)}"
        ) from exc

    try:
        yield lock_path
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        if f is not None:
            try:
                _unlock_fd(f.fileno())
            except Exception:
                pass
            f.close()
