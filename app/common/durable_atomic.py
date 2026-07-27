from __future__ import annotations

import hashlib
import logging
import os
import stat
import uuid
from pathlib import Path
from typing import Callable


IntegrityGate = Callable[[], None]
WriteEventHook = Callable[[str, Path], None]
LOGGER = logging.getLogger(__name__)


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


def _read_open_fd(
    fd: int,
    *,
    max_bytes: int,
    require_single_link: bool = True,
) -> tuple[bytes, os.stat_result]:
    before = os.fstat(fd)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or before.st_mode & 0o002
        or (require_single_link and before.st_nlink != 1)
        or before.st_size > max_bytes
    ):
        raise DurableAtomicWriteError("atomic file verification failed")
    payload = b""
    offset = 0
    while offset < before.st_size:
        chunk = os.pread(fd, min(65536, before.st_size - offset), offset)
        if not chunk:
            raise DurableAtomicWriteError("atomic file verification failed")
        payload += chunk
        offset += len(chunk)
    if os.pread(fd, 1, before.st_size):
        raise DurableAtomicWriteError("atomic file verification failed")
    after = os.fstat(fd)
    if _identity(before) != _identity(after):
        raise DurableAtomicWriteError(
            "atomic file changed during verification"
        )
    return payload, after


def _verify_open_path(
    directory_fd: int,
    name: str,
    fd: int,
    *,
    expected: os.stat_result,
    expected_payload: bytes,
) -> os.stat_result:
    path_info = _target_info(directory_fd, name)
    payload, fd_info = _read_open_fd(
        fd,
        max_bytes=len(expected_payload),
    )
    if (
        path_info is None
        or _identity(path_info) != _identity(expected)
        or _identity(fd_info) != _identity(expected)
        or payload != expected_payload
    ):
        raise DurableAtomicWriteError(
            "atomic file identity changed during commit"
        )
    return fd_info


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _published_target_matches(
    directory_fd: int,
    name: str,
    temp_fd: int,
    *,
    temp_info: os.stat_result,
    payload: bytes,
    mode: int,
) -> bool:
    try:
        target_info = _target_info(directory_fd, name)
        open_payload, open_info = _read_open_fd(
            temp_fd,
            max_bytes=len(payload),
        )
    except (OSError, DurableAtomicWriteError):
        return False
    return bool(
        target_info is not None
        and _same_inode(target_info, temp_info)
        and _same_inode(open_info, temp_info)
        and target_info.st_nlink == 1
        and open_info.st_nlink == 1
        and stat.S_IMODE(target_info.st_mode) == mode
        and open_payload == payload
    )


def _cleanup_backup(
    directory_fd: int,
    backup_name: str,
    *,
    event_hook: WriteEventHook | None,
    path: Path,
) -> bool:
    try:
        if event_hook is not None:
            event_hook("before_backup_cleanup", path)
        os.unlink(backup_name, dir_fd=directory_fd)
        if event_hook is not None:
            event_hook("backup_unlinked", path)
        os.fsync(directory_fd)
    except FileNotFoundError:
        return True
    except OSError:
        LOGGER.warning("durable atomic publication has cleanup debt")
        return False
    return True


def durable_atomic_replace(
    path: Path,
    payload: bytes,
    *,
    mode: int = 0o600,
    require_absent: bool = False,
    integrity_gate: IntegrityGate | None = None,
    event_hook: WriteEventHook | None = None,
    source_integrity_gate: IntegrityGate | None = None,
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
    backup_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.rollback"
    temp_created = False
    backup_created = False
    renamed = False
    publication_durable = False
    temp_fd = -1
    temp_info: os.stat_result | None = None
    initial: os.stat_result | None = None
    try:
        initial = _target_info(directory_fd, path.name)
        if require_absent and initial is not None:
            raise DurableAtomicWriteError("atomic target already exists")
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        temp_fd = os.open(temp_name, flags, mode, dir_fd=directory_fd)
        temp_created = True
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
        temp_info = os.fstat(temp_fd)
        _verify_open_path(
            directory_fd,
            temp_name,
            temp_fd,
            expected=temp_info,
            expected_payload=payload,
        )
        if event_hook is not None:
            event_hook("file_fsynced", path)
            event_hook("before_integrity_check", path)
        if integrity_gate is not None:
            integrity_gate()
        if source_integrity_gate is not None:
            source_integrity_gate()
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
        _verify_open_path(
            directory_fd,
            temp_name,
            temp_fd,
            expected=temp_info,
            expected_payload=payload,
        )
        if event_hook is not None:
            event_hook("before_commit", path)
        if integrity_gate is not None:
            integrity_gate()
        if source_integrity_gate is not None:
            source_integrity_gate()
        _assert_directory_path(path.parent, directory_info)
        current = _target_info(directory_fd, path.name)
        if (
            (require_absent and current is not None)
            or _identity(current) != _identity(initial)
        ):
            raise DurableAtomicWriteError(
                "atomic target changed before commit"
            )
        _verify_open_path(
            directory_fd,
            temp_name,
            temp_fd,
            expected=temp_info,
            expected_payload=payload,
        )
        if initial is not None:
            os.link(
                path.name,
                backup_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            backup_created = True
            linked_target = os.stat(
                path.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            linked_backup = os.stat(
                backup_name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            if (
                not _same_inode(linked_target, initial)
                or not _same_inode(linked_backup, initial)
                or linked_target.st_nlink != 2
                or linked_backup.st_nlink != 2
            ):
                raise DurableAtomicWriteError(
                    "atomic target changed before commit"
                )
            os.fsync(directory_fd)
        if source_integrity_gate is not None:
            source_integrity_gate()
        _verify_open_path(
            directory_fd,
            temp_name,
            temp_fd,
            expected=temp_info,
            expected_payload=payload,
        )
        os.rename(
            temp_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temp_created = False
        renamed = True
        if event_hook is not None:
            event_hook("replaced", path)
            event_hook("after_rename", path)
        os.fsync(directory_fd)
        _assert_directory_path(path.parent, directory_info)
        if integrity_gate is not None:
            integrity_gate()
        if source_integrity_gate is not None:
            source_integrity_gate()
        if not _published_target_matches(
            directory_fd,
            path.name,
            temp_fd,
            temp_info=temp_info,
            payload=payload,
            mode=mode,
        ):
            raise DurableAtomicWriteError(
                "atomic committed target identity mismatch"
            )
        if event_hook is not None:
            event_hook("directory_fsynced", path)
        if integrity_gate is not None:
            integrity_gate()
        if source_integrity_gate is not None:
            source_integrity_gate()
        if event_hook is not None:
            event_hook("before_final_proof", path)
        if not _published_target_matches(
            directory_fd,
            path.name,
            temp_fd,
            temp_info=temp_info,
            payload=payload,
            mode=mode,
        ):
            raise DurableAtomicWriteError(
                "atomic final target proof failed"
            )
        if event_hook is not None:
            event_hook("publication_proved", path)
        if integrity_gate is not None:
            integrity_gate()
        if source_integrity_gate is not None:
            source_integrity_gate()
        if not _published_target_matches(
            directory_fd,
            path.name,
            temp_fd,
            temp_info=temp_info,
            payload=payload,
            mode=mode,
        ):
            raise DurableAtomicWriteError(
                "atomic target changed after proof"
            )
        publication_durable = True
        if backup_created:
            backup_created = not _cleanup_backup(
                directory_fd,
                backup_name,
                event_hook=event_hook,
                path=path,
            )
        return hashlib.sha256(payload).hexdigest()
    except BaseException:
        rollback_error: BaseException | None = None
        if (
            renamed
            and not publication_durable
            and temp_info is not None
            and _published_target_matches(
                directory_fd,
                path.name,
                temp_fd,
                temp_info=temp_info,
                payload=payload,
                mode=mode,
            )
        ):
            try:
                if backup_created:
                    os.rename(
                        backup_name,
                        path.name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                    )
                    backup_created = False
                    os.fsync(directory_fd)
                    restored = _target_info(directory_fd, path.name)
                    if (
                        initial is None
                        or restored is None
                        or not _same_inode(restored, initial)
                        or restored.st_nlink != 1
                    ):
                        raise DurableAtomicWriteError(
                            "atomic rollback verification failed"
                        )
                elif initial is None:
                    os.unlink(path.name, dir_fd=directory_fd)
                    os.fsync(directory_fd)
            except BaseException as exc:
                rollback_error = exc
        if backup_created:
            backup_created = not _cleanup_backup(
                directory_fd,
                backup_name,
                event_hook=None,
                path=path,
            )
        if rollback_error is not None:
            raise DurableAtomicWriteError(
                "atomic publication rollback failed"
            ) from rollback_error
        raise
    finally:
        if temp_fd >= 0:
            os.close(temp_fd)
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
    source_fd = -1
    try:
        source_info = _target_info(source_dir_fd, source_path.name)
        if (
            source_info is None
            or source_info.st_size > max_bytes
        ):
            raise DurableAtomicWriteError("atomic source is unsafe")
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        source_fd = os.open(
            source_path.name,
            flags,
            dir_fd=source_dir_fd,
        )
        payload, verified_info = _read_open_fd(
            source_fd,
            max_bytes=max_bytes,
        )
        if _identity(source_info) != _identity(verified_info):
            raise DurableAtomicWriteError(
                "atomic source changed during verification"
            )

        def verify_source() -> None:
            _assert_directory_path(source_path.parent, os.fstat(source_dir_fd))
            _verify_open_path(
                source_dir_fd,
                source_path.name,
                source_fd,
                expected=verified_info,
                expected_payload=payload,
            )

        return durable_atomic_replace(
            target_path,
            payload,
            mode=mode,
            integrity_gate=integrity_gate,
            event_hook=event_hook,
            source_integrity_gate=verify_source,
        )
    finally:
        if source_fd >= 0:
            os.close(source_fd)
        os.close(source_dir_fd)
