from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

import duckdb

from app.common.config import AppConfig
from app.common.durable_atomic import (
    DurableAtomicWriteError,
    durable_atomic_replace,
)
from app.common.exceptions import CriticalPipelineError
from app.serp.collection_plan_runner import (
    DeadlineGuard,
    validate_resumable_collection_state,
)
from app.serp.execution_matrix import (
    ExecutionMatrix,
    ExecutionMatrixEntry,
    load_execution_matrix,
)
from app.serp.four_region_nightly import (
    validate_completed_four_region_run,
)


MATRIX_RUN_SCHEMA_VERSION = "wb_execution_matrix_run_v1"
MATRIX_LATEST_SCHEMA_VERSION = "wb_execution_matrix_latest_v1"
MARKETPLACE = "wb"
_RUN_ID_RE = re.compile(r"^[0-9]{8}_[0-9]{6}Z$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ScopeKey = tuple[str, str, str, str, str, str]
EntryExecutor = Callable[
    [ExecutionMatrixEntry, str, bool, datetime],
    None,
]
GenerationProbe = Callable[
    [ExecutionMatrixEntry, str],
    Mapping[tuple[str, str], str],
]


class ExecutionMatrixRunError(CriticalPipelineError):
    def __init__(self, message: str, *, resumable: bool = False) -> None:
        super().__init__(message)
        self.resumable = resumable


@dataclass(frozen=True, slots=True)
class MatrixEntryCompletion:
    state_path: str
    state_sha256: str


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _scope_keys(
    entry: ExecutionMatrixEntry,
    run_date: str,
) -> tuple[ScopeKey, ...]:
    plan = entry.bundle.collection_plan
    return tuple(
        (
            run_date,
            MARKETPLACE,
            entry.query_pack_id,
            entry.query_pack_version,
            region_id,
            query_id,
        )
        for region_id in plan.region_set
        for query_id in plan.query_ids
    )


def _scope_digest(scopes: tuple[ScopeKey, ...]) -> str:
    return _sha256(
        _canonical_json(
            {"scopes": [list(scope) for scope in scopes]}
        )
    )


def _run_date(run_id: str) -> str:
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ExecutionMatrixRunError("execution matrix run_id is invalid")
    return (
        f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"
    )


def _entry_run_id(matrix_run_id: str, index: int) -> str:
    try:
        value = datetime.strptime(
            matrix_run_id,
            "%Y%m%d_%H%M%SZ",
        ).replace(tzinfo=UTC)
    except ValueError as exc:
        raise ExecutionMatrixRunError(
            "execution matrix run_id is invalid"
        ) from exc
    return (value + timedelta(seconds=index)).strftime("%Y%m%d_%H%M%SZ")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_state_directory(project_root: Path, path: Path) -> None:
    root = project_root.resolve(strict=True)
    state_root = root / "state"
    try:
        path.relative_to(state_root)
    except ValueError as exc:
        raise ExecutionMatrixRunError(
            "execution matrix state path escapes project root"
        ) from exc
    current = root
    for part in path.relative_to(root).parts:
        child = current / part
        try:
            info = child.lstat()
        except FileNotFoundError:
            child.mkdir(mode=0o700)
            _fsync_directory(current)
            info = child.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o002
        ):
            raise ExecutionMatrixRunError(
                "execution matrix state directory is unsafe"
            )
        current = child


def _read_regular(path: Path) -> bytes | None:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ExecutionMatrixRunError(
            "execution matrix state file is unsafe"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o002
            or info.st_nlink != 1
        ):
            raise ExecutionMatrixRunError(
                "execution matrix state file is unsafe"
            )
        payload = b""
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                raise ExecutionMatrixRunError(
                    "execution matrix state changed while reading"
                )
            payload += chunk
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ExecutionMatrixRunError(
                "execution matrix state changed while reading"
            )
        after = os.fstat(descriptor)
        if (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ExecutionMatrixRunError(
                "execution matrix state changed while reading"
            )
        return payload
    finally:
        os.close(descriptor)


def _write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    integrity_gate: Callable[[], None],
    require_absent: bool = False,
) -> str:
    encoded = _canonical_json(payload)
    try:
        result = durable_atomic_replace(
            path,
            encoded,
            mode=0o600,
            require_absent=require_absent,
            integrity_gate=integrity_gate,
        )
    except DurableAtomicWriteError as exc:
        raise ExecutionMatrixRunError(
            "execution matrix durable state write failed"
        ) from exc
    current = _read_regular(path)
    if current != encoded or result.sha256 != _sha256(encoded):
        raise ExecutionMatrixRunError(
            "execution matrix durable state verification failed"
        )
    return result.sha256


def _reload_exact_matrix(
    matrix: ExecutionMatrix,
    *,
    project_root: Path,
) -> None:
    current = load_execution_matrix(
        project_root=project_root,
        matrix_path=matrix.source_path,
    )
    expected = [
        (
            entry.execution_id,
            entry.enabled,
            entry.plan_file,
            entry.query_pack_id,
            entry.query_pack_version,
            entry.bundle.collection_plan_sha256,
            entry.bundle.query_pack_sha256,
            entry.bundle.region_registry_sha256,
        )
        for entry in matrix.entries
    ]
    actual = [
        (
            entry.execution_id,
            entry.enabled,
            entry.plan_file,
            entry.query_pack_id,
            entry.query_pack_version,
            entry.bundle.collection_plan_sha256,
            entry.bundle.query_pack_sha256,
            entry.bundle.region_registry_sha256,
        )
        for entry in current.entries
    ]
    if (
        current.source_sha256 != matrix.source_sha256
        or current.execution_matrix_id != matrix.execution_matrix_id
        or current.enabled != matrix.enabled
        or actual != expected
    ):
        raise ExecutionMatrixRunError(
            "execution matrix source attestation changed"
        )


def _entry_state(
    entry: ExecutionMatrixEntry,
    *,
    matrix_run_id: str,
    run_date: str,
    index: int,
) -> dict[str, Any]:
    scopes = _scope_keys(entry, run_date)
    return {
        "execution_id": entry.execution_id,
        "plan_file": entry.plan_file,
        "collection_plan_id": entry.bundle.collection_plan.collection_plan_id,
        "collection_plan_sha256": entry.bundle.collection_plan_sha256,
        "query_pack_id": entry.query_pack_id,
        "query_pack_version": entry.query_pack_version,
        "query_pack_sha256": entry.bundle.query_pack_sha256,
        "region_registry_sha256": entry.bundle.region_registry_sha256,
        "plan_run_id": _entry_run_id(matrix_run_id, index),
        "scope_count": len(scopes),
        "scope_sha256": _scope_digest(scopes),
        "status": "pending",
        "attempts": 0,
        "state_path": None,
        "state_sha256": None,
    }


def _new_state(
    matrix: ExecutionMatrix,
    *,
    matrix_run_id: str,
    started_at_utc: str,
) -> dict[str, Any]:
    run_date = _run_date(matrix_run_id)
    return {
        "schema_version": MATRIX_RUN_SCHEMA_VERSION,
        "execution_matrix_id": matrix.execution_matrix_id,
        "execution_matrix_sha256": matrix.source_sha256,
        "marketplace": MARKETPLACE,
        "run_id": matrix_run_id,
        "run_date": run_date,
        "status": "running",
        "complete": False,
        "started_at_utc": started_at_utc,
        "updated_at_utc": started_at_utc,
        "finished_at_utc": None,
        "entries": [
            _entry_state(
                entry,
                matrix_run_id=matrix_run_id,
                run_date=run_date,
                index=index,
            )
            for index, entry in enumerate(matrix.enabled_entries)
        ],
        "failure_reason": None,
    }


def _validate_state(
    payload: Any,
    *,
    matrix: ExecutionMatrix,
    matrix_run_id: str,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "execution_matrix_id",
        "execution_matrix_sha256",
        "marketplace",
        "run_id",
        "run_date",
        "status",
        "complete",
        "started_at_utc",
        "updated_at_utc",
        "finished_at_utc",
        "entries",
        "failure_reason",
    }:
        raise ExecutionMatrixRunError(
            "execution matrix state contract is invalid"
        )
    if (
        payload.get("schema_version") != MATRIX_RUN_SCHEMA_VERSION
        or payload.get("execution_matrix_id") != matrix.execution_matrix_id
        or payload.get("execution_matrix_sha256") != matrix.source_sha256
        or payload.get("marketplace") != MARKETPLACE
        or payload.get("run_id") != matrix_run_id
        or payload.get("run_date") != _run_date(matrix_run_id)
        or payload.get("status")
        not in {"running", "checkpoint", "failed", "success"}
        or type(payload.get("complete")) is not bool
        or (payload["status"] == "success") != payload["complete"]
    ):
        raise ExecutionMatrixRunError(
            "execution matrix state identity is invalid"
        )
    entries = payload.get("entries")
    if (
        not isinstance(entries, list)
        or len(entries) != len(matrix.enabled_entries)
    ):
        raise ExecutionMatrixRunError(
            "execution matrix entry state is invalid"
        )
    expected_entries = _new_state(
        matrix,
        matrix_run_id=matrix_run_id,
        started_at_utc=str(payload.get("started_at_utc", "")),
    )["entries"]
    for actual, expected in zip(entries, expected_entries, strict=True):
        if not isinstance(actual, dict) or set(actual) != set(expected):
            raise ExecutionMatrixRunError(
                "execution matrix entry state is invalid"
            )
        immutable = set(expected) - {
            "status",
            "attempts",
            "state_path",
            "state_sha256",
        }
        if any(actual[field] != expected[field] for field in immutable):
            raise ExecutionMatrixRunError(
                "execution matrix entry provenance mismatch"
            )
        if (
            actual.get("status")
            not in {"pending", "checkpoint", "success"}
            or type(actual.get("attempts")) is not int
            or actual["attempts"] < 0
        ):
            raise ExecutionMatrixRunError(
                "execution matrix entry status is invalid"
            )
        if actual["status"] == "success":
            if (
                not isinstance(actual.get("state_path"), str)
                or not _SHA256_RE.fullmatch(
                    str(actual.get("state_sha256", ""))
                )
            ):
                raise ExecutionMatrixRunError(
                    "execution matrix completion evidence is invalid"
                )
        elif (
            actual.get("state_path") is not None
            or actual.get("state_sha256") is not None
        ):
            raise ExecutionMatrixRunError(
                "execution matrix incomplete entry has completion evidence"
            )
    if payload["status"] == "success" and any(
        entry["status"] != "success" for entry in entries
    ):
        raise ExecutionMatrixRunError(
            "execution matrix success state is incomplete"
        )
    return payload


def _load_state(
    state_path: Path,
    *,
    matrix: ExecutionMatrix,
    matrix_run_id: str,
) -> dict[str, Any] | None:
    encoded = _read_regular(state_path)
    if encoded is None:
        return None
    try:
        payload = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExecutionMatrixRunError(
            "execution matrix state is invalid JSON"
        ) from exc
    if _canonical_json(payload) != encoded:
        raise ExecutionMatrixRunError(
            "execution matrix state is not canonical"
        )
    return _validate_state(
        payload,
        matrix=matrix,
        matrix_run_id=matrix_run_id,
    )


def _default_generation_probe(
    config: AppConfig,
    entry: ExecutionMatrixEntry,
    run_date: str,
) -> Mapping[tuple[str, str], str]:
    database_path = (
        config.project_root
        / "data/warehouse/wb_regional/wb_regional.duckdb"
    )
    if not database_path.is_file() or database_path.is_symlink():
        return {}
    connection = duckdb.connect(
        str(database_path),
        read_only=True,
    )
    try:
        rows = connection.execute(
            """
            SELECT region_id, query_id, run_id
            FROM regional_query_generations
            WHERE marketplace = ?
              AND run_date = ?
              AND query_pack_id = ?
              AND query_pack_version = ?
            """,
            [
                MARKETPLACE,
                run_date,
                entry.query_pack_id,
                entry.query_pack_version,
            ],
        ).fetchall()
    finally:
        connection.close()
    return {
        (str(region_id), str(query_id)): str(run_id)
        for region_id, query_id, run_id in rows
    }


def _validate_generation_state(
    *,
    entry: ExecutionMatrixEntry,
    run_date: str,
    plan_run_id: str,
    observed: Mapping[tuple[str, str], str],
    before_execution: bool,
    resumable_entry: bool,
) -> None:
    expected = {
        (scope[4], scope[5])
        for scope in _scope_keys(entry, run_date)
    }
    actual = set(observed)
    if not actual:
        if not before_execution:
            raise ExecutionMatrixRunError(
                "matrix entry warehouse generation evidence is missing"
            )
        return
    if actual != expected or any(
        type(key) is not tuple
        or len(key) != 2
        or run_id != plan_run_id
        for key, run_id in observed.items()
    ):
        raise ExecutionMatrixRunError(
            "matrix entry generation deduplication conflict"
        )
    if before_execution and not resumable_entry:
        raise ExecutionMatrixRunError(
            "matrix entry generation already exists for date"
        )


def _validate_entry_completion(
    *,
    config: AppConfig,
    entry: ExecutionMatrixEntry,
    plan_run_id: str,
) -> MatrixEntryCompletion:
    evidence = validate_completed_four_region_run(
        config=config,
        bundle=entry.bundle,
        run_id=plan_run_id,
    )
    state_path = str(evidence["state_path"])
    state_sha256 = str(evidence["state_sha256"])
    if not _SHA256_RE.fullmatch(state_sha256):
        raise ExecutionMatrixRunError(
            "matrix entry completion hash is invalid"
        )
    return MatrixEntryCompletion(
        state_path=state_path,
        state_sha256=state_sha256,
    )


def _entry_is_resumable(
    *,
    config: AppConfig,
    entry: ExecutionMatrixEntry,
    plan_run_id: str,
) -> bool:
    if validate_resumable_collection_state(
        config=config,
        plan_path=config.project_root / entry.plan_file,
        run_id=plan_run_id,
    ):
        return True
    manifest_path = (
        config.project_root
        / "state/wb_collection_plans"
        / entry.bundle.collection_plan.collection_plan_id
        / plan_run_id
        / "manifest.json"
    )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return (
        isinstance(manifest, dict)
        and manifest.get("run_id") == plan_run_id
        and manifest.get("collection_plan_id")
        == entry.bundle.collection_plan.collection_plan_id
        and manifest.get("status") == "success"
        and manifest.get("complete") is True
    )


def _entry_is_pristine(
    *,
    config: AppConfig,
    entry: ExecutionMatrixEntry,
    plan_run_id: str,
) -> bool:
    plan_id = entry.bundle.collection_plan.collection_plan_id
    candidates = [
        config.project_root
        / "state/wb_collection_plans"
        / plan_id
        / plan_run_id,
        config.project_root
        / "state/wb_four_region_nightly"
        / plan_id
        / plan_run_id,
        config.project_root
        / "data/marts/wb_four_region"
        / plan_id
        / plan_run_id,
    ]
    for layer in ("raw", "staging", "marts"):
        candidates.extend(
            config.project_root
            / "data"
            / layer
            / "serp_scoped"
            / plan_id
            / region_id
            / plan_run_id
            for region_id in entry.bundle.collection_plan.region_set
        )
        candidates.append(
            config.project_root
            / "data"
            / layer
            / "sellers_scoped"
            / plan_id
            / plan_run_id
        )
    return not any(os.path.lexists(path) for path in candidates)


def _recover_failed_outer_state(
    *,
    state: dict[str, Any],
    matrix: ExecutionMatrix,
    run_date: str,
    checked_probe: Callable[
        [ExecutionMatrixEntry],
        Mapping[tuple[str, str], str],
    ],
    complete: Callable[
        [ExecutionMatrixEntry, str],
        MatrixEntryCompletion,
    ],
    can_resume: Callable[[ExecutionMatrixEntry, str], bool],
    is_pristine: Callable[[ExecutionMatrixEntry, str], bool],
) -> None:
    if (
        state["status"] != "failed"
        or state["complete"] is not False
        or state["finished_at_utc"] is not None
    ):
        raise ExecutionMatrixRunError(
            "execution matrix has no resumable state"
        )

    attempted = [
        index
        for index, entry_state in enumerate(state["entries"])
        if entry_state["status"] != "success"
        and entry_state["attempts"] > 0
    ]
    if len(attempted) != 1:
        raise ExecutionMatrixRunError(
            "failed execution matrix recovery evidence is invalid"
        )
    attempted_index = attempted[0]

    for index, (entry, entry_state) in enumerate(
        zip(matrix.enabled_entries, state["entries"], strict=True)
    ):
        plan_run_id = str(entry_state["plan_run_id"])
        observed = checked_probe(entry)
        if index < attempted_index:
            if entry_state["status"] != "success":
                raise ExecutionMatrixRunError(
                    "failed execution matrix recovery order is invalid"
                )
            completion = complete(entry, plan_run_id)
            if (
                completion.state_path != entry_state["state_path"]
                or completion.state_sha256 != entry_state["state_sha256"]
            ):
                raise ExecutionMatrixRunError(
                    "failed execution matrix completion evidence changed"
                )
            _validate_generation_state(
                entry=entry,
                run_date=run_date,
                plan_run_id=plan_run_id,
                observed=observed,
                before_execution=False,
                resumable_entry=True,
            )
            continue

        if observed:
            raise ExecutionMatrixRunError(
                "failed execution matrix generation evidence conflicts"
            )
        if index == attempted_index:
            if (
                entry_state["status"] != "pending"
                or entry_state["attempts"] != 1
                or can_resume(entry, plan_run_id) is not True
            ):
                raise ExecutionMatrixRunError(
                    "failed execution matrix child checkpoint is invalid"
                )
            continue
        if (
            entry_state["status"] != "pending"
            or entry_state["attempts"] != 0
            or is_pristine(entry, plan_run_id) is not True
        ):
            raise ExecutionMatrixRunError(
                "failed execution matrix future entry is not pristine"
            )

    state["entries"][attempted_index]["status"] = "checkpoint"
    state["status"] = "checkpoint"


def run_execution_matrix(
    *,
    config: AppConfig,
    matrix_path: Path,
    matrix_run_id: str,
    resume: bool,
    execute_entry: EntryExecutor,
    absolute_deadline_utc: datetime | None = None,
    input_integrity_gate: Callable[[], None] = lambda: None,
    generation_probe: GenerationProbe | None = None,
    completion_validator: Callable[
        [ExecutionMatrixEntry, str],
        MatrixEntryCompletion,
    ]
    | None = None,
    resumable_probe: Callable[[ExecutionMatrixEntry, str], bool] | None = None,
    pristine_probe: Callable[[ExecutionMatrixEntry, str], bool] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    matrix = load_execution_matrix(
        project_root=config.project_root,
        matrix_path=matrix_path,
    )
    if not matrix.enabled:
        raise ExecutionMatrixRunError("execution matrix is disabled")
    if not matrix.enabled_entries:
        raise ExecutionMatrixRunError(
            "execution matrix has no enabled entries"
        )
    runtime_window = (
        matrix.enabled_entries[0].bundle.collection_plan.runtime_window
    )
    if runtime_window is None or any(
        entry.bundle.collection_plan.runtime_window != runtime_window
        for entry in matrix.enabled_entries
    ):
        raise ExecutionMatrixRunError(
            "execution matrix runtime contracts differ"
        )
    matrix_deadline = DeadlineGuard.for_runtime_window(
        runtime_window,
        resume=resume,
        now=now,
        absolute_deadline_utc=absolute_deadline_utc,
    ).deadline_utc
    run_date = _run_date(matrix_run_id)
    all_scopes: set[ScopeKey] = set()
    for entry in matrix.enabled_entries:
        scopes = _scope_keys(entry, run_date)
        if all_scopes.intersection(scopes):
            raise ExecutionMatrixRunError(
                "execution matrix contains duplicate generation scopes"
            )
        all_scopes.update(scopes)

    state_root = (
        config.project_root
        / "state/wb_execution_matrices"
        / matrix.execution_matrix_id
    )
    run_root = state_root / "runs" / matrix_run_id
    _ensure_state_directory(config.project_root, run_root)
    state_path = run_root / "state.json"
    latest_path = state_root / "latest.json"
    prior_latest = _read_regular(latest_path)
    if prior_latest is not None:
        try:
            prior_pointer = json.loads(prior_latest)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ExecutionMatrixRunError(
                "execution matrix latest is invalid"
            ) from exc
        if (
            not isinstance(prior_pointer, dict)
            or prior_pointer.get("schema_version")
            != MATRIX_LATEST_SCHEMA_VERSION
            or prior_pointer.get("execution_matrix_id")
            != matrix.execution_matrix_id
            or prior_pointer.get("execution_matrix_sha256")
            != matrix.source_sha256
            or not isinstance(prior_pointer.get("run_id"), str)
            or not _RUN_ID_RE.fullmatch(prior_pointer["run_id"])
        ):
            raise ExecutionMatrixRunError(
                "execution matrix latest contract mismatch"
            )
        if (
            not resume
            and prior_pointer.get("run_id") != matrix_run_id
            and prior_pointer.get("run_date") == run_date
        ):
            raise ExecutionMatrixRunError(
                "execution matrix generation already exists for date"
            )

    def attest() -> None:
        input_integrity_gate()
        _reload_exact_matrix(matrix, project_root=config.project_root)

    attest()
    state = _load_state(
        state_path,
        matrix=matrix,
        matrix_run_id=matrix_run_id,
    )
    if resume:
        if state is None or state["status"] not in {
            "checkpoint",
            "running",
            "failed",
            "success",
        }:
            raise ExecutionMatrixRunError(
                "execution matrix has no resumable state"
            )
    elif state is not None:
        raise ExecutionMatrixRunError(
            "execution matrix run_id already exists"
        )
    else:
        started = now().astimezone(UTC).replace(microsecond=0).isoformat()
        state = _new_state(
            matrix,
            matrix_run_id=matrix_run_id,
            started_at_utc=started,
        )
        _write_json(
            state_path,
            state,
            integrity_gate=attest,
            require_absent=True,
        )

    probe = generation_probe or (
        lambda entry, date: _default_generation_probe(
            config,
            entry,
            date,
        )
    )

    def checked_probe(
        entry: ExecutionMatrixEntry,
    ) -> Mapping[tuple[str, str], str]:
        try:
            value = probe(entry, run_date)
        except (duckdb.Error, OSError) as exc:
            raise ExecutionMatrixRunError(
                "execution matrix generation evidence is unavailable"
            ) from exc
        if not isinstance(value, Mapping):
            raise ExecutionMatrixRunError(
                "execution matrix generation evidence is invalid"
            )
        return value
    complete = completion_validator or (
        lambda entry, child_run_id: _validate_entry_completion(
            config=config,
            entry=entry,
            plan_run_id=child_run_id,
        )
    )
    can_resume = resumable_probe or (
        lambda entry, child_run_id: _entry_is_resumable(
            config=config,
            entry=entry,
            plan_run_id=child_run_id,
        )
    )
    is_pristine = pristine_probe or (
        lambda entry, child_run_id: _entry_is_pristine(
            config=config,
            entry=entry,
            plan_run_id=child_run_id,
        )
    )

    if resume and state["status"] == "failed":
        _recover_failed_outer_state(
            state=state,
            matrix=matrix,
            run_date=run_date,
            checked_probe=checked_probe,
            complete=complete,
            can_resume=can_resume,
            is_pristine=is_pristine,
        )
        state["updated_at_utc"] = (
            now().astimezone(UTC).replace(microsecond=0).isoformat()
        )
        _write_json(state_path, state, integrity_gate=attest)

    if state["status"] != "success":
        for index, entry in enumerate(matrix.enabled_entries):
            entry_state = state["entries"][index]
            plan_run_id = str(entry_state["plan_run_id"])
            if entry_state["status"] == "success":
                completion = complete(entry, plan_run_id)
                if (
                    completion.state_path != entry_state["state_path"]
                    or completion.state_sha256
                    != entry_state["state_sha256"]
                ):
                    raise ExecutionMatrixRunError(
                        "completed matrix entry evidence changed"
                    )
                continue
            is_resume = entry_state["status"] == "checkpoint"
            current = now()
            if current.astimezone(UTC) >= matrix_deadline:
                has_progress = any(
                    item["status"] == "success"
                    or item["attempts"] > 0
                    for item in state["entries"]
                )
                state["status"] = (
                    "checkpoint" if has_progress else "failed"
                )
                state["failure_reason"] = "MatrixDeadlineReached"
                state["updated_at_utc"] = (
                    current.astimezone(UTC).replace(microsecond=0).isoformat()
                )
                _write_json(state_path, state, integrity_gate=attest)
                raise ExecutionMatrixRunError(
                    "execution matrix deadline reached",
                    resumable=has_progress,
                )
            before = checked_probe(entry)
            if resume and entry_state["attempts"] > 0 and not is_resume:
                if before:
                    _validate_generation_state(
                        entry=entry,
                        run_date=run_date,
                        plan_run_id=plan_run_id,
                        observed=before,
                        before_execution=True,
                        resumable_entry=True,
                    )
                    completion = complete(entry, plan_run_id)
                    entry_state["status"] = "success"
                    entry_state["state_path"] = completion.state_path
                    entry_state["state_sha256"] = completion.state_sha256
                    state["updated_at_utc"] = (
                        now()
                        .astimezone(UTC)
                        .replace(microsecond=0)
                        .isoformat()
                    )
                    try:
                        _write_json(
                            state_path,
                            state,
                            integrity_gate=attest,
                        )
                    except ExecutionMatrixRunError as exc:
                        raise ExecutionMatrixRunError(
                            "reconciled matrix entry checkpoint write failed",
                            resumable=True,
                        ) from exc
                    continue
                if can_resume(entry, plan_run_id):
                    is_resume = True
                    entry_state["status"] = "checkpoint"
                elif is_pristine(entry, plan_run_id):
                    is_resume = False
                    entry_state["status"] = "pending"
                else:
                    raise ExecutionMatrixRunError(
                        "in-flight matrix entry cannot be safely resumed"
                    )
            _validate_generation_state(
                entry=entry,
                run_date=run_date,
                plan_run_id=plan_run_id,
                observed=before,
                before_execution=True,
                resumable_entry=is_resume,
            )
            attest()
            entry_state["attempts"] += 1
            state["status"] = "running"
            state["failure_reason"] = None
            state["updated_at_utc"] = (
                now().astimezone(UTC).replace(microsecond=0).isoformat()
            )
            _write_json(state_path, state, integrity_gate=attest)
            try:
                execute_entry(
                    entry,
                    plan_run_id,
                    is_resume,
                    matrix_deadline,
                )
                attest()
                completion = complete(entry, plan_run_id)
                after = checked_probe(entry)
                _validate_generation_state(
                    entry=entry,
                    run_date=run_date,
                    plan_run_id=plan_run_id,
                    observed=after,
                    before_execution=False,
                    resumable_entry=True,
                )
            except Exception as exc:
                child_resumable = can_resume(entry, plan_run_id)
                pristine = (
                    not child_resumable
                    and not checked_probe(entry)
                    and is_pristine(entry, plan_run_id)
                )
                resumable = child_resumable or pristine
                entry_state["status"] = (
                    "checkpoint"
                    if child_resumable
                    else "pending"
                )
                state["status"] = "checkpoint" if resumable else "failed"
                state["failure_reason"] = exc.__class__.__name__
                state["updated_at_utc"] = (
                    now().astimezone(UTC).replace(microsecond=0).isoformat()
                )
                _write_json(state_path, state, integrity_gate=attest)
                raise ExecutionMatrixRunError(
                    "execution matrix entry failed",
                    resumable=resumable,
                ) from exc
            entry_state["status"] = "success"
            entry_state["state_path"] = completion.state_path
            entry_state["state_sha256"] = completion.state_sha256
            state["updated_at_utc"] = (
                now().astimezone(UTC).replace(microsecond=0).isoformat()
            )
            try:
                _write_json(state_path, state, integrity_gate=attest)
            except ExecutionMatrixRunError as exc:
                raise ExecutionMatrixRunError(
                    "matrix entry completed but checkpoint write failed",
                    resumable=True,
                ) from exc

        state["status"] = "success"
        state["complete"] = True
        state["failure_reason"] = None
        state["finished_at_utc"] = (
            now().astimezone(UTC).replace(microsecond=0).isoformat()
        )
        state["updated_at_utc"] = state["finished_at_utc"]
        try:
            state_sha256 = _write_json(
                state_path,
                state,
                integrity_gate=attest,
            )
        except ExecutionMatrixRunError as exc:
            raise ExecutionMatrixRunError(
                "execution matrix completion state write failed",
                resumable=True,
            ) from exc
    else:
        state_bytes = _read_regular(state_path)
        if state_bytes is None:
            raise ExecutionMatrixRunError(
                "completed execution matrix state is missing"
            )
        state_sha256 = _sha256(state_bytes)

    for entry, entry_state in zip(
        matrix.enabled_entries,
        state["entries"],
        strict=True,
    ):
        completion = complete(entry, str(entry_state["plan_run_id"]))
        if (
            entry_state["status"] != "success"
            or entry_state["state_path"] != completion.state_path
            or entry_state["state_sha256"] != completion.state_sha256
        ):
            raise ExecutionMatrixRunError(
                "execution matrix completion evidence mismatch"
            )

    pointer = {
        "schema_version": MATRIX_LATEST_SCHEMA_VERSION,
        "execution_matrix_id": matrix.execution_matrix_id,
        "execution_matrix_sha256": matrix.source_sha256,
        "marketplace": MARKETPLACE,
        "run_id": matrix_run_id,
        "run_date": run_date,
        "state_path": state_path.relative_to(
            config.project_root
        ).as_posix(),
        "state_sha256": state_sha256,
        "entries": [
            {
                "execution_id": entry_state["execution_id"],
                "plan_run_id": entry_state["plan_run_id"],
                "state_path": entry_state["state_path"],
                "state_sha256": entry_state["state_sha256"],
            }
            for entry_state in state["entries"]
        ],
    }
    current_latest = _read_regular(latest_path)
    if current_latest is not None:
        try:
            prior = json.loads(current_latest)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ExecutionMatrixRunError(
                "execution matrix latest is invalid"
            ) from exc
        if (
            not isinstance(prior, dict)
            or prior.get("schema_version") != MATRIX_LATEST_SCHEMA_VERSION
            or prior.get("execution_matrix_id")
            != matrix.execution_matrix_id
        ):
            raise ExecutionMatrixRunError(
                "execution matrix latest contract mismatch"
            )
        prior_run_id = prior.get("run_id")
        if (
            not isinstance(prior_run_id, str)
            or not _RUN_ID_RE.fullmatch(prior_run_id)
            or (
                prior_run_id != matrix_run_id
                and prior_run_id >= matrix_run_id
            )
        ):
            raise ExecutionMatrixRunError(
                "execution matrix latest is newer than candidate"
            )
        if prior_run_id == matrix_run_id:
            expected = _canonical_json(pointer)
            if current_latest != expected:
                raise ExecutionMatrixRunError(
                    "execution matrix same-run latest mismatch"
                )
            return state
    try:
        _write_json(latest_path, pointer, integrity_gate=attest)
    except ExecutionMatrixRunError as exc:
        raise ExecutionMatrixRunError(
            "execution matrix completed without latest publication",
            resumable=True,
        ) from exc
    final_state = _read_regular(state_path)
    final_latest = _read_regular(latest_path)
    if (
        final_state is None
        or _sha256(final_state) != state_sha256
        or final_latest != _canonical_json(pointer)
    ):
        raise ExecutionMatrixRunError(
            "execution matrix publication verification failed"
        )
    return state
