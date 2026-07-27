from __future__ import annotations

import hashlib
import os
import stat
import uuid
from pathlib import Path
from typing import Callable


IntegrityGate = Callable[[], None]
WriteEventHook = Callable[[str, Path], None]


class DurableAtomicWriteError(RuntimeError):
    pass


def _directory_fd(path: Path) -> int:
    if not path.is_absolute() or ".." in path.parts:
        raise DurableAtomicWriteError("atomic parent path is invalid")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    current_fd = os.open(path.anchor, flags)
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        info = os.fstat(current_fd)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o002
        ):
            raise DurableAtomicWriteError("atomic parent is unsafe")
        return current_fd
    except Exception:
        os.close(current_fd)
        raise


def _target_info(directory_fd: int, name: str) -> os.stat_result | None:
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_mode & 0o002
        or info.st_nlink != 1
    ):
        raise DurableAtomicWriteError("atomic target is unsafe")
    return info


def _identity(info: os.stat_result | None) -> tuple[int, ...] | None:
    if info is None:
        return None
    return (
        info.st_dev,
        info.st_ino,
        info.st_uid,
        info.st_gid,
        stat.S_IMODE(info.st_mode),
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _directory_identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_uid,
        info.st_gid,
        stat.S_IMODE(info.st_mode),
    )


def _assert_directory_path(path: Path, expected: os.stat_result) -> None:
    try:
        verification_fd = _directory_fd(path)
    except OSError as exc:
        raise DurableAtomicWriteError(
            "atomic parent changed during commit"
        ) from exc
    try:
        if _directory_identity(os.fstat(verification_fd)) != (
            _directory_identity(expected)
        ):
            raise DurableAtomicWriteError(
                "atomic parent changed during commit"
            )
    finally:
        os.close(verification_fd)


def _read_target(
    directory_fd: int,
    name: str,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_mode & 0o002
            or before.st_nlink != 1
            or before.st_size > max_bytes
        ):
            raise DurableAtomicWriteError("atomic target verification failed")
        payload = b""
        remaining = before.st_size
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                raise DurableAtomicWriteError(
                    "atomic target verification failed"
                )
            payload += chunk
            remaining -= len(chunk)
        if os.read(fd, 1):
            raise DurableAtomicWriteError(
                "atomic target verification failed"
            )
        after = os.fstat(fd)
        if _identity(before) != _identity(after):
            raise DurableAtomicWriteError(
                "atomic target changed during verification"
            )
        return payload, after
    finally:
        os.close(fd)


def durable_atomic_replace(
    path: Path,
    payload: bytes,
    *,
    mode: int = 0o600,
    require_absent: bool = False,
    integrity_gate: IntegrityGate | None = None,
    event_hook: WriteEventHook | None = None,
) -> str:
    if (
        not path.is_absolute()
        or path.name in {"", ".", ".."}
        or "/" in path.name
    ):
        raise DurableAtomicWriteError("atomic target is invalid")
    directory_fd = _directory_fd(path.parent)
    directory_info = os.fstat(directory_fd)
    temp_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    temp_created = False
    try:
        initial = _target_info(directory_fd, path.name)
        if require_absent and initial is not None:
            raise DurableAtomicWriteError("atomic target already exists")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        temp_fd = os.open(temp_name, flags, mode, dir_fd=directory_fd)
        temp_created = True
        try:
            os.fchmod(temp_fd, mode)
            temp_info = os.fstat(temp_fd)
            if (
                not stat.S_ISREG(temp_info.st_mode)
                or temp_info.st_uid != os.geteuid()
                or temp_info.st_nlink != 1
                or temp_info.st_mode & 0o022
            ):
                raise DurableAtomicWriteError("atomic temp is unsafe")
            view = memoryview(payload)
            while view:
                written = os.write(temp_fd, view)
                if written <= 0:
                    raise DurableAtomicWriteError(
                        "atomic write made no progress"
                    )
                view = view[written:]
            os.fsync(temp_fd)
        finally:
            os.close(temp_fd)
        if event_hook is not None:
            event_hook("file_fsynced", path)
            event_hook("before_integrity_check", path)
        if integrity_gate is not None:
            integrity_gate()
        if event_hook is not None:
            event_hook("before_target_recheck", path)
        _assert_directory_path(path.parent, directory_info)
        current = _target_info(directory_fd, path.name)
        if (
            (require_absent and current is not None)
            or _identity(current) != _identity(initial)
        ):
            raise DurableAtomicWriteError(
                "atomic target changed before commit"
            )
        os.rename(
            temp_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temp_created = False
        if event_hook is not None:
            event_hook("replaced", path)
        os.fsync(directory_fd)
        _assert_directory_path(path.parent, directory_info)
        if event_hook is not None:
            event_hook("directory_fsynced", path)
        verified, info = _read_target(
            directory_fd,
            path.name,
            max_bytes=len(payload),
        )
        if (
            verified != payload
            or stat.S_IMODE(info.st_mode) != mode
        ):
            raise DurableAtomicWriteError(
                "atomic target verification failed"
            )
        return hashlib.sha256(payload).hexdigest()
    finally:
        if temp_created:
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


def durable_atomic_copy(
    source_path: Path,
    target_path: Path,
    *,
    mode: int = 0o644,
    integrity_gate: IntegrityGate | None = None,
    event_hook: WriteEventHook | None = None,
    max_bytes: int = 512 * 1024 * 1024,
) -> str:
    if (
        not source_path.is_absolute()
        or source_path.name in {"", ".", ".."}
        or max_bytes < 1
    ):
        raise DurableAtomicWriteError("atomic source is invalid")
    source_dir_fd = _directory_fd(source_path.parent)
    try:
        source_info = _target_info(source_dir_fd, source_path.name)
        if (
            source_info is None
            or source_info.st_size > max_bytes
        ):
            raise DurableAtomicWriteError("atomic source is unsafe")
        payload, verified_info = _read_target(
            source_dir_fd,
            source_path.name,
            max_bytes=max_bytes,
        )
        if _identity(source_info) != _identity(verified_info):
            raise DurableAtomicWriteError(
                "atomic source changed during verification"
            )
    finally:
        os.close(source_dir_fd)
    return durable_atomic_replace(
        target_path,
        payload,
        mode=mode,
        integrity_gate=integrity_gate,
        event_hook=event_hook,
    )
