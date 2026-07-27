from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping


IntegrityGate = Callable[[], None]
WriteEventHook = Callable[[str, Path], None]
LOGGER = logging.getLogger(__name__)
CLEANUP_DEBT_SCHEMA_VERSION = "wb_durable_cleanup_debt_v1"
CLEANUP_DEBT_LIMIT = 3
CLEANUP_DEBT_DIRECTORY = Path("state/wb_durable_cleanup_debt")
_MARKER_PREFIX = ".wb-durable-debt-v1."
_MARKER_SUFFIX = ".json"
_BACKUP_PREFIX = ".wb-rollback-v1."
_BACKUP_SUFFIX = ".debt"
_MAX_MARKER_BYTES = 16 * 1024


class DurableAtomicWriteError(RuntimeError):
    pass


class DurableCleanupDebtError(DurableAtomicWriteError):
    pass


@dataclass(frozen=True)
class DurableAtomicResult:
    sha256: str
    cleanup_debt_count: int
    cleanup_debt_limit: int
    cleanup_debt_swept: int
    cleanup_debt_status: str


@dataclass(frozen=True)
class _CleanupDebtRecord:
    marker_id: str
    marker_name: str
    backup_name: str
    payload: dict[str, object]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _strict_sha256(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise DurableCleanupDebtError("cleanup debt metadata is invalid")
    return value


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


def _hash_named_file(
    directory_fd: int,
    name: str,
    *,
    allowed_links: tuple[int, ...],
    max_bytes: int,
) -> tuple[str, os.stat_result]:
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
            or before.st_nlink not in allowed_links
            or before.st_size > max_bytes
        ):
            raise DurableCleanupDebtError(
                "cleanup debt file proof failed"
            )
        digest = hashlib.sha256()
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(
                fd,
                min(1024 * 1024, before.st_size - offset),
                offset,
            )
            if not chunk:
                raise DurableCleanupDebtError(
                    "cleanup debt file proof failed"
                )
            digest.update(chunk)
            offset += len(chunk)
        if os.pread(fd, 1, before.st_size):
            raise DurableCleanupDebtError(
                "cleanup debt file proof failed"
            )
        after = os.fstat(fd)
        if _identity(before) != _identity(after):
            raise DurableCleanupDebtError(
                "cleanup debt file changed during proof"
            )
        return digest.hexdigest(), after
    finally:
        os.close(fd)


class _CleanupDebtManager:
    def __init__(
        self,
        project_root: Path,
        lease_validator: IntegrityGate,
    ) -> None:
        self.project_root = project_root.resolve(strict=True)
        self.lease_validator = lease_validator
        self.registry_path = self.project_root / CLEANUP_DEBT_DIRECTORY

    def _validate_lease(self) -> None:
        try:
            self.lease_validator()
        except BaseException as exc:
            raise DurableCleanupDebtError(
                "cleanup debt requires verified host lease"
            ) from exc

    def _registry_fd(self, *, create: bool) -> int | None:
        self._validate_lease()
        state_path = self.project_root / "state"
        root_fd = _directory_fd(self.project_root)
        try:
            try:
                state_info = os.stat(
                    "state",
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                if not create:
                    return None
                os.mkdir("state", 0o700, dir_fd=root_fd)
                os.fsync(root_fd)
                state_info = os.stat(
                    "state",
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            if (
                not stat.S_ISDIR(state_info.st_mode)
                or state_info.st_uid != os.geteuid()
                or state_info.st_mode & 0o002
            ):
                raise DurableCleanupDebtError(
                    "cleanup debt state directory is unsafe"
                )
        finally:
            os.close(root_fd)
        state_fd = _directory_fd(state_path)
        try:
            try:
                registry_info = os.stat(
                    self.registry_path.name,
                    dir_fd=state_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                if not create:
                    return None
                os.mkdir(
                    self.registry_path.name,
                    0o700,
                    dir_fd=state_fd,
                )
                os.fsync(state_fd)
                registry_info = os.stat(
                    self.registry_path.name,
                    dir_fd=state_fd,
                    follow_symlinks=False,
                )
            if (
                not stat.S_ISDIR(registry_info.st_mode)
                or registry_info.st_uid != os.geteuid()
                or stat.S_IMODE(registry_info.st_mode) != 0o700
            ):
                raise DurableCleanupDebtError(
                    "cleanup debt registry is unsafe"
                )
        finally:
            os.close(state_fd)
        return _directory_fd(self.registry_path)

    @staticmethod
    def _marker_name(marker_id: str) -> str:
        if (
            len(marker_id) != 32
            or any(character not in "0123456789abcdef" for character in marker_id)
        ):
            raise DurableCleanupDebtError(
                "cleanup debt marker id is invalid"
            )
        return f"{_MARKER_PREFIX}{marker_id}{_MARKER_SUFFIX}"

    @staticmethod
    def _validated_marker_name(name: str) -> str:
        if not name.startswith(_MARKER_PREFIX) or not name.endswith(
            _MARKER_SUFFIX
        ):
            raise DurableCleanupDebtError(
                "cleanup debt registry contains unknown entry"
            )
        marker_id = name[
            len(_MARKER_PREFIX) : -len(_MARKER_SUFFIX)
        ]
        if _CleanupDebtManager._marker_name(marker_id) != name:
            raise DurableCleanupDebtError(
                "cleanup debt marker name is invalid"
            )
        return marker_id

    @staticmethod
    def _identity_payload(info: os.stat_result, digest: str) -> dict[str, object]:
        return {
            "device": int(info.st_dev),
            "inode": int(info.st_ino),
            "uid": int(info.st_uid),
            "gid": int(info.st_gid),
            "mode": stat.S_IMODE(info.st_mode),
            "size": int(info.st_size),
            "sha256": digest,
        }

    @staticmethod
    def _validate_identity_payload(value: object) -> dict[str, object]:
        if not isinstance(value, dict) or set(value) != {
            "device",
            "inode",
            "uid",
            "gid",
            "mode",
            "size",
            "sha256",
        }:
            raise DurableCleanupDebtError(
                "cleanup debt identity is invalid"
            )
        for key in ("device", "inode", "uid", "gid", "mode", "size"):
            item = value[key]
            if not isinstance(item, int) or isinstance(item, bool) or item < 0:
                raise DurableCleanupDebtError(
                    "cleanup debt identity is invalid"
                )
        _strict_sha256(value["sha256"])
        return value

    def _encode_marker(self, payload: dict[str, object]) -> bytes:
        unsigned = dict(payload)
        unsigned.pop("marker_sha256", None)
        payload["marker_sha256"] = _sha256_bytes(_canonical_json(unsigned))
        return _canonical_json(payload)

    def _decode_marker(
        self,
        registry_fd: int,
        name: str,
    ) -> _CleanupDebtRecord:
        marker_id = self._validated_marker_name(name)
        raw, info = _read_target(
            registry_fd,
            name,
            max_bytes=_MAX_MARKER_BYTES,
        )
        if (
            stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise DurableCleanupDebtError(
                "cleanup debt marker is unsafe"
            )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DurableCleanupDebtError(
                "cleanup debt marker is invalid"
            ) from exc
        expected_keys = {
            "schema_version",
            "marker_id",
            "target_parent",
            "target_name",
            "target_name_sha256",
            "backup_name",
            "parent_device",
            "parent_inode",
            "prior",
            "candidate",
            "marker_sha256",
        }
        if not isinstance(payload, dict) or set(payload) != expected_keys:
            raise DurableCleanupDebtError(
                "cleanup debt marker is invalid"
            )
        if (
            payload["schema_version"] != CLEANUP_DEBT_SCHEMA_VERSION
            or payload["marker_id"] != marker_id
            or not isinstance(payload["target_parent"], str)
            or not Path(payload["target_parent"]).is_absolute()
            or not isinstance(payload["target_name"], str)
            or payload["target_name"] in {"", ".", ".."}
            or "/" in payload["target_name"]
            or _strict_sha256(payload["target_name_sha256"])
            != _sha256_bytes(payload["target_name"].encode("utf-8"))
            or not isinstance(payload["backup_name"], str)
            or payload["backup_name"]
            != (
                f"{_BACKUP_PREFIX}{marker_id}."
                f"{payload['target_name_sha256'][:16]}."
                f"{payload['prior']['sha256']}{_BACKUP_SUFFIX}"
            )
        ):
            raise DurableCleanupDebtError(
                "cleanup debt marker is invalid"
            )
        for key in ("parent_device", "parent_inode"):
            value = payload[key]
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise DurableCleanupDebtError(
                    "cleanup debt marker is invalid"
                )
        self._validate_identity_payload(payload["prior"])
        self._validate_identity_payload(payload["candidate"])
        marker_sha = _strict_sha256(payload["marker_sha256"])
        unsigned = dict(payload)
        unsigned.pop("marker_sha256")
        if marker_sha != _sha256_bytes(_canonical_json(unsigned)):
            raise DurableCleanupDebtError(
                "cleanup debt marker hash mismatch"
            )
        if raw != _canonical_json(payload):
            raise DurableCleanupDebtError(
                "cleanup debt marker is not canonical"
            )
        return _CleanupDebtRecord(
            marker_id=marker_id,
            marker_name=name,
            backup_name=str(payload["backup_name"]),
            payload=payload,
        )

    def _write_marker(self, record: _CleanupDebtRecord) -> None:
        registry_fd = self._registry_fd(create=True)
        assert registry_fd is not None
        temp_name = f".{record.marker_name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        fd = -1
        created = False
        try:
            if os.stat(
                record.marker_name,
                dir_fd=registry_fd,
                follow_symlinks=False,
            ):
                raise DurableCleanupDebtError(
                    "cleanup debt marker already exists"
                )
        except FileNotFoundError:
            pass
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(temp_name, flags, 0o600, dir_fd=registry_fd)
            created = True
            raw = self._encode_marker(record.payload)
            view = memoryview(raw)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise DurableCleanupDebtError(
                        "cleanup debt marker write made no progress"
                    )
                view = view[written:]
            os.fsync(fd)
            os.rename(
                temp_name,
                record.marker_name,
                src_dir_fd=registry_fd,
                dst_dir_fd=registry_fd,
            )
            created = False
            os.fsync(registry_fd)
        finally:
            if fd >= 0:
                os.close(fd)
            if created:
                try:
                    os.unlink(temp_name, dir_fd=registry_fd)
                except FileNotFoundError:
                    pass
            os.close(registry_fd)

    def register(
        self,
        *,
        target: Path,
        parent_info: os.stat_result,
        prior_info: os.stat_result,
        prior_sha256: str,
        candidate_sha256: str,
        candidate_size: int,
        candidate_mode: int,
        candidate_gid: int,
    ) -> _CleanupDebtRecord:
        self._validate_lease()
        target_parent = target.parent.resolve(strict=True)
        if (
            target_parent != self.project_root
            and self.project_root not in target_parent.parents
            and target_parent
            != Path("/var/lib/parser-nightly-coordinator/results")
        ):
            raise DurableCleanupDebtError(
                "cleanup debt target is outside approved roots"
            )
        marker_id = uuid.uuid4().hex
        target_name_sha = _sha256_bytes(target.name.encode("utf-8"))
        backup_name = (
            f"{_BACKUP_PREFIX}{marker_id}.{target_name_sha[:16]}."
            f"{prior_sha256}{_BACKUP_SUFFIX}"
        )
        payload: dict[str, object] = {
            "schema_version": CLEANUP_DEBT_SCHEMA_VERSION,
            "marker_id": marker_id,
            "target_parent": str(target_parent),
            "target_name": target.name,
            "target_name_sha256": target_name_sha,
            "backup_name": backup_name,
            "parent_device": int(parent_info.st_dev),
            "parent_inode": int(parent_info.st_ino),
            "prior": self._identity_payload(prior_info, prior_sha256),
            "candidate": {
                "device": 0,
                "inode": 0,
                "uid": os.geteuid(),
                "gid": candidate_gid,
                "mode": candidate_mode,
                "size": candidate_size,
                "sha256": candidate_sha256,
            },
        }
        record = _CleanupDebtRecord(
            marker_id=marker_id,
            marker_name=self._marker_name(marker_id),
            backup_name=backup_name,
            payload=payload,
        )
        self._write_marker(record)
        return record

    @staticmethod
    def _matches_payload(
        info: os.stat_result,
        digest: str,
        expected: Mapping[str, object],
    ) -> bool:
        return bool(
            info.st_dev == expected["device"]
            and info.st_ino == expected["inode"]
            and info.st_uid == expected["uid"]
            and info.st_gid == expected["gid"]
            and stat.S_IMODE(info.st_mode) == expected["mode"]
            and info.st_size == expected["size"]
            and digest == expected["sha256"]
        )

    def _remove_marker(
        self,
        registry_fd: int,
        record: _CleanupDebtRecord,
    ) -> bool:
        try:
            os.unlink(record.marker_name, dir_fd=registry_fd)
            os.fsync(registry_fd)
        except OSError:
            LOGGER.warning("durable cleanup debt marker cleanup deferred")
            return False
        return True

    def _sweep_record(
        self,
        registry_fd: int,
        record: _CleanupDebtRecord,
    ) -> bool:
        self._validate_lease()
        payload = record.payload
        parent_path = Path(str(payload["target_parent"]))
        parent_fd = _directory_fd(parent_path)
        try:
            parent_info = os.fstat(parent_fd)
            if (
                parent_info.st_dev != payload["parent_device"]
                or parent_info.st_ino != payload["parent_inode"]
            ):
                raise DurableCleanupDebtError(
                    "cleanup debt parent identity changed"
                )
            try:
                backup_info = os.stat(
                    record.backup_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                backup_info = None
            try:
                target_info = os.stat(
                    str(payload["target_name"]),
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                target_info = None
            if target_info is not None and (
                not stat.S_ISREG(target_info.st_mode)
                or target_info.st_uid != os.geteuid()
                or target_info.st_mode & 0o002
                or target_info.st_nlink not in (1, 2)
            ):
                raise DurableCleanupDebtError(
                    "cleanup debt target proof failed"
                )
            target_digest = ""
            if target_info is not None:
                target_digest, target_info = _hash_named_file(
                    parent_fd,
                    str(payload["target_name"]),
                    allowed_links=(1, 2),
                    max_bytes=max(
                        int(payload["prior"]["size"]),
                        int(payload["candidate"]["size"]),
                    ),
                )
            if backup_info is None:
                if target_info is None or not (
                    self._matches_payload(
                        target_info,
                        target_digest,
                        payload["prior"],
                    )
                    or (
                        target_info.st_uid == payload["candidate"]["uid"]
                        and target_info.st_gid == payload["candidate"]["gid"]
                        and stat.S_IMODE(target_info.st_mode)
                        == payload["candidate"]["mode"]
                        and target_info.st_size
                        == payload["candidate"]["size"]
                        and target_digest
                        == payload["candidate"]["sha256"]
                    )
                ):
                    raise DurableCleanupDebtError(
                        "cleanup debt without backup is unprovable"
                    )
                try:
                    os.fsync(parent_fd)
                except OSError:
                    LOGGER.warning(
                        "durable cleanup debt directory sync deferred"
                    )
                    return False
            else:
                backup_digest, backup_info = _hash_named_file(
                    parent_fd,
                    record.backup_name,
                    allowed_links=(1, 2),
                    max_bytes=int(payload["prior"]["size"]),
                )
                if not self._matches_payload(
                    backup_info,
                    backup_digest,
                    payload["prior"],
                ):
                    raise DurableCleanupDebtError(
                        "cleanup debt backup proof failed"
                    )
                if backup_info.st_nlink == 2 and (
                    target_info is None
                    or not _same_inode(target_info, backup_info)
                    or not self._matches_payload(
                        target_info,
                        target_digest,
                        payload["prior"],
                    )
                ):
                    raise DurableCleanupDebtError(
                        "cleanup debt hardlink proof failed"
                    )
                try:
                    os.unlink(record.backup_name, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                except OSError:
                    LOGGER.warning(
                        "durable cleanup debt sweep deferred"
                    )
                    return False
        finally:
            os.close(parent_fd)
        return self._remove_marker(registry_fd, record)

    def sweep(self) -> tuple[int, int]:
        registry_fd = self._registry_fd(create=False)
        if registry_fd is None:
            return 0, 0
        swept = 0
        try:
            names = sorted(os.listdir(registry_fd))
            records = [
                self._decode_marker(registry_fd, name)
                for name in names
            ]
            for record in records:
                if self._sweep_record(registry_fd, record):
                    swept += 1
            remaining = len(os.listdir(registry_fd))
            for name in os.listdir(registry_fd):
                self._decode_marker(registry_fd, name)
            return remaining, swept
        finally:
            os.close(registry_fd)

    def discard_marker(self, record: _CleanupDebtRecord) -> bool:
        registry_fd = self._registry_fd(create=False)
        if registry_fd is None:
            return True
        try:
            current = self._decode_marker(
                registry_fd,
                record.marker_name,
            )
            if current.payload != record.payload:
                raise DurableCleanupDebtError(
                    "cleanup debt marker changed"
                )
            return self._remove_marker(registry_fd, record)
        except FileNotFoundError:
            return True
        finally:
            os.close(registry_fd)


def _cleanup_debt_manager(
    integrity_gate: IntegrityGate | None,
) -> _CleanupDebtManager | None:
    if integrity_gate is None or not bool(
        getattr(integrity_gate, "_wb_cleanup_debt_enabled", False)
    ):
        return None
    project_root = getattr(
        integrity_gate,
        "_wb_cleanup_debt_project_root",
        None,
    )
    lease_validator = getattr(
        integrity_gate,
        "_wb_cleanup_debt_validate_lease",
        None,
    )
    if not isinstance(project_root, Path) or not callable(lease_validator):
        raise DurableCleanupDebtError(
            "cleanup debt integrity gate metadata is invalid"
        )
    return _CleanupDebtManager(project_root, lease_validator)


def inspect_cleanup_debt(
    project_root: Path,
    *,
    lease_validator: IntegrityGate,
) -> dict[str, int | str]:
    manager = _CleanupDebtManager(project_root, lease_validator)
    registry_fd = manager._registry_fd(create=False)
    if registry_fd is None:
        return {
            "schema_version": CLEANUP_DEBT_SCHEMA_VERSION,
            "count": 0,
            "limit": CLEANUP_DEBT_LIMIT,
            "status": "clear",
        }
    try:
        names = os.listdir(registry_fd)
        for name in names:
            manager._decode_marker(registry_fd, name)
    finally:
        os.close(registry_fd)
    count = len(names)
    return {
        "schema_version": CLEANUP_DEBT_SCHEMA_VERSION,
        "count": count,
        "limit": CLEANUP_DEBT_LIMIT,
        "status": "blocked" if count >= CLEANUP_DEBT_LIMIT else "tracked",
    }


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
) -> DurableAtomicResult:
    if (
        not path.is_absolute()
        or path.name in {"", ".", ".."}
        or "/" in path.name
    ):
        raise DurableAtomicWriteError("atomic target is invalid")
    directory_fd = _directory_fd(path.parent)
    directory_info = os.fstat(directory_fd)
    debt_manager = _cleanup_debt_manager(integrity_gate)
    debt_count = 0
    debt_swept = 0
    if debt_manager is not None:
        debt_count, debt_swept = debt_manager.sweep()
        if debt_count >= CLEANUP_DEBT_LIMIT:
            os.close(directory_fd)
            raise DurableCleanupDebtError(
                "durable cleanup debt limit reached"
            )
    temp_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    backup_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.rollback"
    temp_created = False
    backup_created = False
    debt_record: _CleanupDebtRecord | None = None
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
            if debt_manager is not None:
                prior_sha256, prior_info = _hash_named_file(
                    directory_fd,
                    path.name,
                    allowed_links=(1,),
                    max_bytes=initial.st_size,
                )
                if _identity(prior_info) != _identity(initial):
                    raise DurableAtomicWriteError(
                        "atomic target changed before debt registration"
                    )
                debt_record = debt_manager.register(
                    target=path,
                    parent_info=directory_info,
                    prior_info=prior_info,
                    prior_sha256=prior_sha256,
                    candidate_sha256=_sha256_bytes(payload),
                    candidate_size=len(payload),
                    candidate_mode=mode,
                    candidate_gid=temp_info.st_gid,
                )
                backup_name = debt_record.backup_name
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
        cleanup_ok = True
        if backup_created:
            cleanup_ok = _cleanup_backup(
                directory_fd,
                backup_name,
                event_hook=event_hook,
                path=path,
            )
            backup_created = not cleanup_ok
        if (
            debt_manager is not None
            and debt_record is not None
            and cleanup_ok
        ):
            cleanup_ok = debt_manager.discard_marker(debt_record)
            if cleanup_ok:
                debt_record = None
        if debt_manager is not None:
            debt_count, swept_now = debt_manager.sweep()
            debt_swept += swept_now
            if debt_count >= CLEANUP_DEBT_LIMIT:
                raise DurableCleanupDebtError(
                    "durable cleanup debt limit reached after commit"
                )
        else:
            debt_count = 0 if cleanup_ok else 1
        return DurableAtomicResult(
            sha256=_sha256_bytes(payload),
            cleanup_debt_count=debt_count,
            cleanup_debt_limit=CLEANUP_DEBT_LIMIT,
            cleanup_debt_swept=debt_swept,
            cleanup_debt_status=(
                "clear" if debt_count == 0 else "tracked"
            ),
        )
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
        if backup_created and not publication_durable:
            backup_created = not _cleanup_backup(
                directory_fd,
                backup_name,
                event_hook=None,
                path=path,
            )
        if (
            debt_manager is not None
            and debt_record is not None
            and not publication_durable
            and not backup_created
        ):
            if debt_manager.discard_marker(debt_record):
                debt_record = None
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
) -> DurableAtomicResult:
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
