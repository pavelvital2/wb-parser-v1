from __future__ import annotations

import fcntl
import grp
import hashlib
import json
import os
import re
import stat
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .durable_atomic import (
    DurableAtomicWriteError,
    durable_atomic_replace,
)
from .exceptions import CriticalPipelineError
from .runtime_env import (
    RuntimeEnvironmentError,
    load_strict_runtime_environment,
)


RESULT_SCHEMA_VERSION = "marketplace_parser_result_v3"
LOCK_CONTRACT_VERSION = "marketplace_collection_lock_v3"
QUARANTINE_CONTRACT_VERSION = "marketplace_collection_quarantine_v1"
CHECK_SCHEMA_VERSION = "parser_coordinator_contract_check_v2"
ADAPTER_STATUS_SCHEMA_VERSION = "wb_four_region_adapter_status_v1"

SECURE_LOCK_DIRECTORY = Path("/run/lock/parser-nightly-coordinator")
GUARD_LOCK_PATH = SECURE_LOCK_DIRECTORY / "marketplace-collection.guard.flock"
VALIDATION_LOCK_PATH = (
    SECURE_LOCK_DIRECTORY / "marketplace-collection.validation.flock"
)
RESULT_DIRECTORY = Path("/var/lib/parser-nightly-coordinator/results")
QUARANTINE_MARKER_PATH = Path(
    "/var/lib/parser-nightly-coordinator/unsafe-cleanup-quarantine.json"
)

EXIT_BY_OUTCOME = {
    "success": 0,
    "checkpoint": 76,
    "deferred": 75,
    "hard_failure": 2,
}
PHASE_BY_STAGE = {"wb_initial": "initial", "wb_resume": "resume"}
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,199}$")
SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
RUN_ID = re.compile(r"^nightly-(\d{8})-[a-f0-9]{6,32}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
WB_RUN_REF = re.compile(r"^[0-9]{8}_[0-9]{6}Z$")
MAX_RESULT_BYTES = 1024 * 1024
MAX_QUARANTINE_BYTES = 16 * 1024

COORDINATOR_ENV_KEYS = {
    "MARKETPLACE_COORDINATOR_RESULT_CONTRACT",
    "MARKETPLACE_COORDINATOR_RESULT_FILE",
    "MARKETPLACE_COORDINATOR_RUN_ID",
    "MARKETPLACE_COORDINATOR_SCHEDULE_DATE",
    "MARKETPLACE_COORDINATOR_STAGE",
    "MARKETPLACE_COORDINATOR_ATTEMPT",
    "MARKETPLACE_COORDINATOR_INVOCATION_ID",
    "MARKETPLACE_COORDINATOR_RESUME_REF",
    "MARKETPLACE_COORDINATOR_DEADLINE_UTC",
    "MARKETPLACE_COLLECTION_LOCK_CONTRACT",
    "MARKETPLACE_COLLECTION_GUARD_PATH",
    "MARKETPLACE_COLLECTION_VALIDATION_PATH",
    "MARKETPLACE_COLLECTION_VALIDATION_OWNER_PID",
    "MARKETPLACE_COLLECTION_VALIDATION_FD",
    "MARKETPLACE_COLLECTION_QUARANTINE_CONTRACT",
    "MARKETPLACE_COLLECTION_QUARANTINE_MARKER_PATH",
}
DESCENDANT_LEASE_ENV_KEYS = {
    "PARSER_WB_LOCK_V3_CONTRACT",
    "PARSER_WB_LOCK_V3_GUARD_PATH",
    "PARSER_WB_LOCK_V3_GUARD_FD",
    "PARSER_WB_LOCK_V3_VALIDATION_PATH",
    "PARSER_WB_LOCK_V3_VALIDATION_FD",
    "PARSER_WB_LOCK_V3_VALIDATION_OWNER_PID",
    "PARSER_WB_LOCK_V3_QUARANTINE_PATH",
}
QUARANTINE_KEYS = {
    "schema_version",
    "schedule_date",
    "coordinator_run_id",
    "stage",
    "attempt",
    "invocation_id",
    "state_generation",
    "state_sha256",
    "armed_at_utc",
    "reason_code",
    "marker_sha256",
}


class NightlyCoordinatorContractError(CriticalPipelineError):
    def __init__(self, code: str, *, outcome: str = "deferred") -> None:
        self.code = code
        self.outcome = outcome
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class CoordinatorInvocation:
    result_path: Path
    coordinator_run_id: str
    schedule_date: str
    stage: str
    phase: str
    attempt: int
    invocation_id: str
    resume_ref: str
    deadline_utc: datetime
    quarantine_marker_path: Path


@dataclass(frozen=True, slots=True)
class HostLockPolicy:
    directory: Path = SECURE_LOCK_DIRECTORY
    guard_path: Path = GUARD_LOCK_PATH
    validation_path: Path = VALIDATION_LOCK_PATH
    directory_uid: int = 0
    file_uid: int = 0
    file_gid: int = -1
    directory_mode: int = 0o755
    file_mode: int = 0o660

    @classmethod
    def production(cls) -> "HostLockPolicy":
        try:
            group_id = grp.getgrnam("pavel").gr_gid
        except KeyError as exc:
            raise NightlyCoordinatorContractError(
                "coordinator_lock_group_unavailable",
                outcome="hard_failure",
            ) from exc
        return cls(file_gid=group_id)

    def validate_directory(self) -> None:
        try:
            info = self.directory.lstat()
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "coordinator_lock_directory_unavailable"
            ) from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != self.directory_uid
            or stat.S_IMODE(info.st_mode) != self.directory_mode
            or info.st_mode & 0o022
        ):
            raise NightlyCoordinatorContractError(
                "coordinator_lock_directory_unsafe",
                outcome="hard_failure",
            )

    def validate_file(self, path: Path, info: os.stat_result) -> None:
        if (
            path.parent != self.directory
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != self.file_uid
            or info.st_gid != self.file_gid
            or stat.S_IMODE(info.st_mode) != self.file_mode
        ):
            raise NightlyCoordinatorContractError(
                "coordinator_lock_file_unsafe",
                outcome="hard_failure",
            )


@dataclass(slots=True)
class MarketplaceCollectionLease(AbstractContextManager["MarketplaceCollectionLease"]):
    policy: HostLockPolicy
    invocation: CoordinatorInvocation | None
    guard_fd: int
    validation_fd: int
    owns_guard: bool
    owns_validation: bool
    quarantine_marker_path: Path

    @property
    def pass_fds(self) -> tuple[int, ...]:
        return tuple(sorted({self.guard_fd, self.validation_fd}))

    def assert_held(self) -> None:
        self.policy.validate_directory()
        validation_info = os.fstat(self.validation_fd)
        self.policy.validate_file(self.policy.validation_path, validation_info)
        _assert_same_path_inode(
            self.policy.validation_path,
            validation_info,
            self.policy,
        )
        _assert_externally_locked(
            self.policy.validation_path,
            self.policy,
            code="coordinator_validation_lease_lost",
        )
        guard_info = os.fstat(self.guard_fd)
        self.policy.validate_file(self.policy.guard_path, guard_info)
        _assert_same_path_inode(self.policy.guard_path, guard_info, self.policy)
        _assert_externally_locked(
            self.policy.guard_path,
            self.policy,
            code="coordinator_guard_lock_lost",
        )

    def __exit__(self, *_args: object) -> None:
        if self.owns_validation:
            _close_fd(self.validation_fd)
        _close_fd(self.guard_fd)


def _safe_id(value: str, field: str, *, allow_empty: bool = False) -> str:
    if allow_empty and value == "":
        return value
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise NightlyCoordinatorContractError(
            f"coordinator_{field}_invalid",
            outcome="hard_failure",
        )
    return value


def parse_utc(value: str, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z") or "T" not in value:
        raise NightlyCoordinatorContractError(
            f"coordinator_{field}_invalid",
            outcome="hard_failure",
        )
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            f"coordinator_{field}_invalid",
            outcome="hard_failure",
        ) from exc
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise NightlyCoordinatorContractError(
            f"coordinator_{field}_invalid",
            outcome="hard_failure",
        )
    return parsed


def utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def coordinator_invocation_from_environment(
    environment: Mapping[str, str] | None = None,
) -> CoordinatorInvocation | None:
    env = environment if environment is not None else os.environ
    present = {key for key in COORDINATOR_ENV_KEYS if key in env}
    if not present:
        return None
    if present != COORDINATOR_ENV_KEYS:
        raise NightlyCoordinatorContractError(
            "coordinator_environment_incomplete",
            outcome="hard_failure",
        )
    if env["MARKETPLACE_COORDINATOR_RESULT_CONTRACT"] != RESULT_SCHEMA_VERSION:
        raise NightlyCoordinatorContractError(
            "coordinator_result_contract_mismatch",
            outcome="hard_failure",
        )
    if env["MARKETPLACE_COLLECTION_LOCK_CONTRACT"] != LOCK_CONTRACT_VERSION:
        raise NightlyCoordinatorContractError(
            "coordinator_lock_contract_mismatch",
            outcome="hard_failure",
        )
    if (
        env["MARKETPLACE_COLLECTION_QUARANTINE_CONTRACT"]
        != QUARANTINE_CONTRACT_VERSION
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_contract_mismatch",
            outcome="hard_failure",
        )
    stage = env["MARKETPLACE_COORDINATOR_STAGE"]
    if stage not in PHASE_BY_STAGE:
        raise NightlyCoordinatorContractError(
            "coordinator_stage_invalid",
            outcome="hard_failure",
        )
    schedule_date = env["MARKETPLACE_COORDINATOR_SCHEDULE_DATE"]
    try:
        parsed_date = date.fromisoformat(schedule_date)
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_schedule_date_invalid",
            outcome="hard_failure",
        ) from exc
    if parsed_date.isoformat() != schedule_date:
        raise NightlyCoordinatorContractError(
            "coordinator_schedule_date_invalid",
            outcome="hard_failure",
        )
    coordinator_run_id = env["MARKETPLACE_COORDINATOR_RUN_ID"]
    run_match = RUN_ID.fullmatch(coordinator_run_id)
    if (
        run_match is None
        or run_match.group(1) != schedule_date.replace("-", "")
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_run_id_invalid",
            outcome="hard_failure",
        )
    try:
        attempt = int(env["MARKETPLACE_COORDINATOR_ATTEMPT"])
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_attempt_invalid",
            outcome="hard_failure",
        ) from exc
    if attempt != 1 or str(attempt) != env["MARKETPLACE_COORDINATOR_ATTEMPT"]:
        raise NightlyCoordinatorContractError(
            "coordinator_attempt_invalid",
            outcome="hard_failure",
        )
    invocation_id = _safe_id(
        env["MARKETPLACE_COORDINATOR_INVOCATION_ID"],
        "invocation_id",
    )
    resume_ref = _safe_id(
        env["MARKETPLACE_COORDINATOR_RESUME_REF"],
        "resume_ref",
        allow_empty=True,
    )
    if (stage == "wb_resume") != bool(resume_ref):
        raise NightlyCoordinatorContractError(
            "coordinator_resume_identity_invalid",
            outcome="hard_failure",
        )
    if resume_ref and not WB_RUN_REF.fullmatch(resume_ref):
        raise NightlyCoordinatorContractError(
            "coordinator_resume_identity_invalid",
            outcome="hard_failure",
        )
    result_path = Path(env["MARKETPLACE_COORDINATOR_RESULT_FILE"])
    expected_name = f"{coordinator_run_id}.{stage}.{attempt}.json"
    if (
        not result_path.is_absolute()
        or result_path.name != expected_name
        or result_path.parent != RESULT_DIRECTORY
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_result_path_invalid",
            outcome="hard_failure",
        )
    marker_path = Path(
        env["MARKETPLACE_COLLECTION_QUARANTINE_MARKER_PATH"]
    )
    if marker_path != QUARANTINE_MARKER_PATH:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_path_mismatch",
            outcome="hard_failure",
        )
    if Path(env["MARKETPLACE_COLLECTION_GUARD_PATH"]) != GUARD_LOCK_PATH:
        raise NightlyCoordinatorContractError(
            "coordinator_guard_path_mismatch",
            outcome="hard_failure",
        )
    if (
        Path(env["MARKETPLACE_COLLECTION_VALIDATION_PATH"])
        != VALIDATION_LOCK_PATH
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_validation_path_mismatch",
            outcome="hard_failure",
        )
    return CoordinatorInvocation(
        result_path=result_path,
        coordinator_run_id=coordinator_run_id,
        schedule_date=schedule_date,
        stage=stage,
        phase=PHASE_BY_STAGE[stage],
        attempt=attempt,
        invocation_id=invocation_id,
        resume_ref=resume_ref,
        deadline_utc=parse_utc(
            env["MARKETPLACE_COORDINATOR_DEADLINE_UTC"],
            "deadline_utc",
        ),
        quarantine_marker_path=marker_path,
    )


def _open_lock(path: Path, policy: HostLockPolicy) -> int:
    policy.validate_directory()
    flags = os.O_RDWR | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_lock_file_unavailable"
        ) from exc
    try:
        policy.validate_file(path, os.fstat(fd))
    except Exception:
        os.close(fd)
        raise
    return fd


def _assert_same_path_inode(
    path: Path,
    owned: os.stat_result,
    policy: HostLockPolicy,
) -> None:
    fd = _open_lock(path, policy)
    try:
        current = os.fstat(fd)
        if (owned.st_dev, owned.st_ino) != (current.st_dev, current.st_ino):
            raise NightlyCoordinatorContractError(
                "coordinator_lock_inode_changed",
                outcome="hard_failure",
            )
    finally:
        os.close(fd)


def _assert_externally_locked(
    path: Path,
    policy: HostLockPolicy,
    *,
    code: str,
) -> None:
    fd = _open_lock(path, policy)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        fcntl.flock(fd, fcntl.LOCK_UN)
        raise NightlyCoordinatorContractError(code, outcome="hard_failure")
    finally:
        os.close(fd)


def _parent_pid(pid: int) -> int:
    try:
        status_text = Path(f"/proc/{pid}/status").read_text(
            encoding="utf-8",
            errors="strict",
        )
    except (OSError, UnicodeDecodeError):
        return 0
    match = re.search(r"^PPid:\s+(\d+)$", status_text, flags=re.MULTILINE)
    return int(match.group(1)) if match else 0


def _owner_is_self_or_ancestor(owner_pid: int) -> bool:
    current = os.getpid()
    visited: set[int] = set()
    while current > 1 and current not in visited:
        if current == owner_pid:
            return True
        visited.add(current)
        current = _parent_pid(current)
    return False


def _validate_inherited_validation_owner(
    *,
    owner_pid: int,
    validation_fd: int,
    validation_info: os.stat_result,
) -> None:
    if not _owner_is_self_or_ancestor(owner_pid):
        raise NightlyCoordinatorContractError(
            "coordinator_validation_owner_invalid",
            outcome="hard_failure",
        )
    try:
        owner_info = os.stat(
            f"/proc/{owner_pid}/fd/{validation_fd}",
            follow_symlinks=True,
        )
    except OSError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_validation_owner_invalid",
            outcome="hard_failure",
        ) from exc
    if (owner_info.st_dev, owner_info.st_ino) != (
        validation_info.st_dev,
        validation_info.st_ino,
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_validation_owner_invalid",
            outcome="hard_failure",
        )


def _lock_nonblocking(fd: int, *, code: str) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise NightlyCoordinatorContractError(code) from exc


def _close_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _marker_digest(payload: Mapping[str, Any]) -> str:
    canonical = dict(payload)
    canonical["marker_sha256"] = ""
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _read_quarantine_marker(path: Path) -> dict[str, Any] | None:
    try:
        parent = path.parent.lstat()
    except OSError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_parent_unavailable",
            outcome="hard_failure",
        ) from exc
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or parent.st_mode & 0o022
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_parent_unsafe",
            outcome="hard_failure",
        )
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_unsafe",
            outcome="hard_failure",
        ) from exc
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o440
            or not 0 < info.st_size <= MAX_QUARANTINE_BYTES
        ):
            raise NightlyCoordinatorContractError(
                "coordinator_quarantine_marker_unsafe",
                outcome="hard_failure",
            )
        encoded = b""
        while len(encoded) <= MAX_QUARANTINE_BYTES:
            chunk = os.read(fd, min(65536, MAX_QUARANTINE_BYTES + 1 - len(encoded)))
            if not chunk:
                break
            encoded += chunk
        if len(encoded) != info.st_size:
            raise NightlyCoordinatorContractError(
                "coordinator_quarantine_marker_changed",
                outcome="hard_failure",
            )
    finally:
        os.close(fd)
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_invalid",
            outcome="hard_failure",
        ) from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != QUARANTINE_KEYS
        or payload.get("schema_version") != QUARANTINE_CONTRACT_VERSION
        or not isinstance(payload.get("schedule_date"), str)
        or not isinstance(payload.get("coordinator_run_id"), str)
        or not isinstance(payload.get("stage"), str)
        or not isinstance(payload.get("invocation_id"), str)
        or type(payload.get("attempt")) is not int
        or payload["attempt"] != 1
        or type(payload.get("state_generation")) is not int
        or payload["state_generation"] < 1
        or not isinstance(payload.get("state_sha256"), str)
        or not SHA256.fullmatch(payload["state_sha256"])
        or not isinstance(payload.get("reason_code"), str)
        or not SAFE_REASON.fullmatch(payload["reason_code"])
        or not isinstance(payload.get("marker_sha256"), str)
        or not SHA256.fullmatch(payload["marker_sha256"])
        or payload["marker_sha256"] != _marker_digest(payload)
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_invalid",
            outcome="hard_failure",
        )
    try:
        marker_date = date.fromisoformat(payload["schedule_date"])
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_invalid",
            outcome="hard_failure",
        ) from exc
    run_match = RUN_ID.fullmatch(payload["coordinator_run_id"])
    if (
        marker_date.isoformat() != payload["schedule_date"]
        or run_match is None
        or run_match.group(1) != payload["schedule_date"].replace("-", "")
        or not SAFE_ID.fullmatch(payload["stage"])
        or not SAFE_ID.fullmatch(payload["invocation_id"])
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_invalid",
            outcome="hard_failure",
        )
    parse_utc(payload["armed_at_utc"], "quarantine_armed_at_utc")
    return payload


def _validate_quarantine(
    invocation: CoordinatorInvocation | None,
    *,
    marker_path: Path,
) -> None:
    marker = _read_quarantine_marker(marker_path)
    if invocation is None:
        if marker is not None:
            raise NightlyCoordinatorContractError(
                "unsafe_cleanup_quarantine_active"
            )
        return
    if marker is None:
        raise NightlyCoordinatorContractError(
            "coordinator_quarantine_marker_missing",
            outcome="hard_failure",
        )
    expected = {
        "schedule_date": invocation.schedule_date,
        "coordinator_run_id": invocation.coordinator_run_id,
        "stage": invocation.stage,
        "attempt": invocation.attempt,
        "invocation_id": invocation.invocation_id,
        "reason_code": "child_activity_in_progress",
    }
    if any(marker.get(key) != value for key, value in expected.items()):
        raise NightlyCoordinatorContractError(
            "unsafe_cleanup_quarantine_active"
        )


def acquire_marketplace_collection_lease(
    *,
    environment: Mapping[str, str] | None = None,
    policy: HostLockPolicy | None = None,
    quarantine_marker_path: Path | None = None,
) -> MarketplaceCollectionLease:
    env = environment if environment is not None else os.environ
    invocation = coordinator_invocation_from_environment(env)
    active_policy = policy or HostLockPolicy.production()
    active_policy.validate_directory()

    if invocation is not None:
        guard_fd = _open_lock(active_policy.guard_path, active_policy)
        try:
            try:
                validation_fd = int(
                    env["MARKETPLACE_COLLECTION_VALIDATION_FD"]
                )
                owner_pid = int(
                    env["MARKETPLACE_COLLECTION_VALIDATION_OWNER_PID"]
                )
            except ValueError as exc:
                raise NightlyCoordinatorContractError(
                    "coordinator_validation_lease_invalid",
                    outcome="hard_failure",
                ) from exc
            if (
                validation_fd < 3
                or owner_pid < 2
                or not Path(f"/proc/{owner_pid}").exists()
            ):
                raise NightlyCoordinatorContractError(
                    "coordinator_validation_lease_invalid",
                    outcome="hard_failure",
                )
            try:
                info = os.fstat(validation_fd)
            except OSError as exc:
                raise NightlyCoordinatorContractError(
                    "coordinator_validation_lease_invalid",
                    outcome="hard_failure",
                ) from exc
            active_policy.validate_file(active_policy.validation_path, info)
            _validate_inherited_validation_owner(
                owner_pid=owner_pid,
                validation_fd=validation_fd,
                validation_info=info,
            )
            _assert_same_path_inode(
                active_policy.validation_path,
                info,
                active_policy,
            )
            os.set_inheritable(guard_fd, True)
            os.set_inheritable(validation_fd, True)
            lease = MarketplaceCollectionLease(
                policy=active_policy,
                invocation=invocation,
                guard_fd=guard_fd,
                validation_fd=validation_fd,
                owns_guard=False,
                owns_validation=False,
                quarantine_marker_path=invocation.quarantine_marker_path,
            )
        except Exception:
            _close_fd(guard_fd)
            raise
    else:
        guard_fd = _open_lock(active_policy.guard_path, active_policy)
        try:
            _lock_nonblocking(
                guard_fd,
                code="shared_marketplace_guard_busy",
            )
            validation_fd = _open_lock(
                active_policy.validation_path,
                active_policy,
            )
            try:
                _lock_nonblocking(
                    validation_fd,
                    code="shared_marketplace_validation_busy",
                )
                os.set_inheritable(guard_fd, True)
                os.set_inheritable(validation_fd, True)
            except Exception:
                os.close(validation_fd)
                raise
        except Exception:
            _close_fd(guard_fd)
            raise
        marker_path = quarantine_marker_path or QUARANTINE_MARKER_PATH
        lease = MarketplaceCollectionLease(
            policy=active_policy,
            invocation=None,
            guard_fd=guard_fd,
            validation_fd=validation_fd,
            owns_guard=True,
            owns_validation=True,
            quarantine_marker_path=marker_path,
        )
    try:
        lease.assert_held()
        _validate_quarantine(
            invocation,
            marker_path=(
                quarantine_marker_path
                or (
                    invocation.quarantine_marker_path
                    if invocation is not None
                    else QUARANTINE_MARKER_PATH
                )
            ),
        )
        lease.assert_held()
    except Exception:
        lease.__exit__()
        raise
    return lease


def descendant_lease_environment(
    lease: MarketplaceCollectionLease,
) -> dict[str, str]:
    lease.assert_held()
    return {
        "PARSER_WB_LOCK_V3_CONTRACT": LOCK_CONTRACT_VERSION,
        "PARSER_WB_LOCK_V3_GUARD_PATH": str(lease.policy.guard_path),
        "PARSER_WB_LOCK_V3_GUARD_FD": str(lease.guard_fd),
        "PARSER_WB_LOCK_V3_VALIDATION_PATH": str(lease.policy.validation_path),
        "PARSER_WB_LOCK_V3_VALIDATION_FD": str(lease.validation_fd),
        "PARSER_WB_LOCK_V3_VALIDATION_OWNER_PID": str(os.getpid()),
        "PARSER_WB_LOCK_V3_QUARANTINE_PATH": str(
            lease.quarantine_marker_path
        ),
    }


def validate_descendant_marketplace_lease(
    *,
    environment: Mapping[str, str] | None = None,
    policy: HostLockPolicy | None = None,
) -> int:
    env = environment if environment is not None else os.environ
    present_keys = {
        key for key in DESCENDANT_LEASE_ENV_KEYS if key in env
    }
    if not present_keys:
        raise NightlyCoordinatorContractError(
            "official_live_entry_requires_lock_v3",
            outcome="deferred",
        )
    if present_keys != DESCENDANT_LEASE_ENV_KEYS:
        raise NightlyCoordinatorContractError(
            "descendant_validation_lease_incomplete",
            outcome="hard_failure",
        )
    active_policy = policy or HostLockPolicy.production()
    if (
        env["PARSER_WB_LOCK_V3_CONTRACT"] != LOCK_CONTRACT_VERSION
        or Path(env["PARSER_WB_LOCK_V3_GUARD_PATH"])
        != active_policy.guard_path
        or Path(env["PARSER_WB_LOCK_V3_VALIDATION_PATH"])
        != active_policy.validation_path
    ):
        raise NightlyCoordinatorContractError(
            "descendant_validation_lease_invalid",
            outcome="hard_failure",
        )
    try:
        guard_fd = int(env["PARSER_WB_LOCK_V3_GUARD_FD"])
        validation_fd = int(env["PARSER_WB_LOCK_V3_VALIDATION_FD"])
        owner_pid = int(
            env["PARSER_WB_LOCK_V3_VALIDATION_OWNER_PID"]
        )
        guard_info = os.fstat(guard_fd)
        validation_info = os.fstat(validation_fd)
    except (ValueError, OSError) as exc:
        raise NightlyCoordinatorContractError(
            "descendant_validation_lease_invalid",
            outcome="hard_failure",
        ) from exc
    if (
        guard_fd < 3
        or validation_fd < 3
        or guard_fd == validation_fd
        or owner_pid < 2
    ):
        raise NightlyCoordinatorContractError(
            "descendant_validation_lease_invalid",
            outcome="hard_failure",
        )
    active_policy.validate_directory()
    active_policy.validate_file(active_policy.guard_path, guard_info)
    active_policy.validate_file(active_policy.validation_path, validation_info)
    _validate_inherited_validation_owner(
        owner_pid=owner_pid,
        validation_fd=guard_fd,
        validation_info=guard_info,
    )
    _validate_inherited_validation_owner(
        owner_pid=owner_pid,
        validation_fd=validation_fd,
        validation_info=validation_info,
    )
    _assert_same_path_inode(
        active_policy.guard_path,
        guard_info,
        active_policy,
    )
    _assert_same_path_inode(
        active_policy.validation_path,
        validation_info,
        active_policy,
    )
    _assert_externally_locked(
        active_policy.validation_path,
        active_policy,
        code="descendant_validation_lease_lost",
    )
    _assert_externally_locked(
        active_policy.guard_path,
        active_policy,
        code="descendant_guard_lock_lost",
    )
    invocation = coordinator_invocation_from_environment(env)
    marker_path = Path(env["PARSER_WB_LOCK_V3_QUARANTINE_PATH"])
    expected_marker_path = (
        invocation.quarantine_marker_path
        if invocation is not None
        else (
            QUARANTINE_MARKER_PATH
            if policy is None
            else marker_path
        )
    )
    if marker_path != expected_marker_path:
        raise NightlyCoordinatorContractError(
            "descendant_quarantine_path_invalid",
            outcome="hard_failure",
        )
    _validate_quarantine(invocation, marker_path=marker_path)
    os.set_inheritable(guard_fd, True)
    os.set_inheritable(validation_fd, True)
    return validation_fd


def require_official_live_entry_lease(
    *,
    environment: Mapping[str, str] | None = None,
    policy: HostLockPolicy | None = None,
) -> int | None:
    env = environment if environment is not None else os.environ
    active_policy = policy or HostLockPolicy.production()
    if not os.path.lexists(active_policy.directory):
        return None
    return validate_descendant_marketplace_lease(
        environment=env,
        policy=active_policy,
    )


def load_required_runtime_environment(
    *,
    project_root: Path,
    lease: MarketplaceCollectionLease,
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    lease.assert_held()
    try:
        loaded = load_strict_runtime_environment(
            project_root=project_root,
            base_environment=(
                environment if environment is not None else os.environ
            ),
        )
    except RuntimeEnvironmentError as exc:
        raise NightlyCoordinatorContractError(
            exc.code,
            outcome="hard_failure",
        ) from exc
    if loaded.environment.get("PARSER_WB_RUNTIME_ENV_LOADED") != "1":
        raise NightlyCoordinatorContractError(
            "wb_runtime_environment_not_attested",
            outcome="hard_failure",
        )
    lease.assert_held()
    return loaded.environment


def write_terminal_result(
    *,
    invocation: CoordinatorInvocation,
    outcome: str,
    run_ref: str,
    resume_ref: str,
    reason_code: str,
    started_at_utc: datetime,
    finished_at_utc: datetime,
    report_refs: tuple[str, ...] = (),
    integrity_gate: Callable[[], None] | None = None,
    write_event_hook: Callable[[str, Path], None] | None = None,
) -> int:
    if outcome not in EXIT_BY_OUTCOME:
        raise NightlyCoordinatorContractError(
            "coordinator_result_outcome_invalid",
            outcome="hard_failure",
        )
    if not WB_RUN_REF.fullmatch(run_ref):
        raise NightlyCoordinatorContractError(
            "coordinator_result_run_ref_invalid",
            outcome="hard_failure",
        )
    if outcome == "checkpoint":
        if resume_ref != run_ref:
            raise NightlyCoordinatorContractError(
                "coordinator_result_resume_ref_invalid",
                outcome="hard_failure",
            )
    elif resume_ref:
        raise NightlyCoordinatorContractError(
            "coordinator_result_resume_ref_invalid",
            outcome="hard_failure",
        )
    if len(report_refs) > 20:
        raise NightlyCoordinatorContractError(
            "coordinator_result_report_refs_invalid",
            outcome="hard_failure",
        )
    for report_ref in report_refs:
        if not isinstance(report_ref, str):
            raise NightlyCoordinatorContractError(
                "coordinator_result_report_refs_invalid",
                outcome="hard_failure",
            )
        parts = Path(report_ref).parts
        if (
            len(report_ref) > 300
            or "\x00" in report_ref
            or "\n" in report_ref
            or "\r" in report_ref
            or Path(report_ref).is_absolute()
            or ".." in parts
            or len(parts) != 3
            or parts[:2] != ("state", "run_reports")
            or not parts[2].endswith(".json")
        ):
            raise NightlyCoordinatorContractError(
                "coordinator_result_report_refs_invalid",
                outcome="hard_failure",
            )
    if not SAFE_REASON.fullmatch(reason_code):
        raise NightlyCoordinatorContractError(
            "coordinator_result_reason_invalid",
            outcome="hard_failure",
        )
    if finished_at_utc < started_at_utc:
        raise NightlyCoordinatorContractError(
            "coordinator_result_timestamps_invalid",
            outcome="hard_failure",
        )
    payload = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "lock_contract_version": LOCK_CONTRACT_VERSION,
        "parser": "wb",
        "phase": invocation.phase,
        "coordinator_run_id": invocation.coordinator_run_id,
        "schedule_date": invocation.schedule_date,
        "attempt": invocation.attempt,
        "invocation_id": invocation.invocation_id,
        "outcome": outcome,
        "terminal_exit_code": EXIT_BY_OUTCOME[outcome],
        "run_ref": run_ref,
        "started_at_utc": utc_iso(started_at_utc),
        "finished_at_utc": utc_iso(finished_at_utc),
        "resources_released": True,
        "validation_lease_held_until_exit": True,
        "resume_required": outcome == "checkpoint",
        "resume_ref": resume_ref,
        "reason_code": reason_code,
        "report_refs": list(report_refs),
    }
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    if not 0 < len(encoded) <= MAX_RESULT_BYTES:
        raise NightlyCoordinatorContractError(
            "coordinator_result_size_invalid",
            outcome="hard_failure",
        )
    result_path = invocation.result_path
    parent = result_path.parent
    if parent != RESULT_DIRECTORY:
        raise NightlyCoordinatorContractError(
            "coordinator_result_path_invalid",
            outcome="hard_failure",
        )
    try:
        parent_info = parent.lstat()
    except OSError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_result_parent_unsafe",
            outcome="hard_failure",
        ) from exc
    if (
        stat.S_ISLNK(parent_info.st_mode)
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or parent_info.st_mode & 0o022
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_result_parent_unsafe",
            outcome="hard_failure",
        )
    if result_path.exists() or result_path.is_symlink():
        raise NightlyCoordinatorContractError(
            "coordinator_result_already_exists",
            outcome="hard_failure",
        )
    try:
        durable_atomic_replace(
            result_path,
            encoded,
            mode=0o440,
            require_absent=True,
            integrity_gate=integrity_gate,
            event_hook=write_event_hook,
        )
    except DurableAtomicWriteError as exc:
        raise NightlyCoordinatorContractError(
            "coordinator_result_commit_failed",
            outcome="hard_failure",
        ) from exc
    return EXIT_BY_OUTCOME[outcome]
