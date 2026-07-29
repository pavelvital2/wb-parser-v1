from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.serp.collection_plan import (
    CollectionPlanBundle,
    CollectionPlanValidationError,
    load_collection_plan_bundle,
)


EXECUTION_MATRIX_SCHEMA_VERSION = "wb_query_pack_execution_matrix_v1"
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True, slots=True)
class ExecutionMatrixEntry:
    execution_id: str
    enabled: bool
    plan_file: str
    query_pack_id: str
    query_pack_version: str
    bundle: CollectionPlanBundle


@dataclass(frozen=True, slots=True)
class ExecutionMatrix:
    source_path: Path
    source_sha256: str
    execution_matrix_id: str
    enabled: bool
    entries: tuple[ExecutionMatrixEntry, ...]

    @property
    def enabled_entries(self) -> tuple[ExecutionMatrixEntry, ...]:
        if not self.enabled:
            return ()
        return tuple(entry for entry in self.entries if entry.enabled)


def _fail(message: str) -> None:
    raise CollectionPlanValidationError(message)


def _safe_source(
    project_root: Path,
    path: Path,
    *,
    required_parent: Path,
) -> tuple[Path, bytes]:
    root = project_root.resolve(strict=True)
    lexical = Path(os.path.abspath(path))
    try:
        relative = lexical.relative_to(root)
    except ValueError:
        _fail("execution matrix path is outside project root")
    current = root
    for part in relative.parts:
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise CollectionPlanValidationError(
                "execution matrix path is unavailable"
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            _fail("execution matrix path uses a symlink")
    if lexical.parent != required_parent or lexical.suffix != ".json":
        _fail("execution matrix path is outside the approved directory")
    info = lexical.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        _fail("execution matrix must be a regular file")
    try:
        encoded = lexical.read_bytes()
    except OSError as exc:
        raise CollectionPlanValidationError(
            "execution matrix cannot be read"
        ) from exc
    return lexical, encoded


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{field} must be an object")
    return value


def _exact_keys(value: dict[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        _fail(f"{field} keys are invalid")


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        _fail(f"{field} is invalid")
    return value


def _version(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _VERSION_RE.fullmatch(value):
        _fail(f"{field} is invalid")
    return value


def _boolean(value: Any, field: str) -> bool:
    if type(value) is not bool:
        _fail(f"{field} must be a boolean")
    return value


def load_execution_matrix(
    *,
    project_root: Path,
    matrix_path: Path,
) -> ExecutionMatrix:
    root = project_root.resolve(strict=True)
    approved_parent = root / "config/wb/execution_matrices"
    source_path, encoded = _safe_source(
        root,
        matrix_path,
        required_parent=approved_parent,
    )
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CollectionPlanValidationError(
            "execution matrix is invalid JSON"
        ) from exc
    document = _object(payload, "execution_matrix")
    _exact_keys(
        document,
        {
            "schema_version",
            "execution_matrix_id",
            "enabled",
            "entries",
        },
        "execution_matrix",
    )
    if document["schema_version"] != EXECUTION_MATRIX_SCHEMA_VERSION:
        _fail("execution matrix schema version is unsupported")
    matrix_id = _identifier(
        document["execution_matrix_id"],
        "execution_matrix.execution_matrix_id",
    )
    enabled = _boolean(document["enabled"], "execution_matrix.enabled")
    raw_entries = document["entries"]
    if not isinstance(raw_entries, list) or not raw_entries:
        _fail("execution_matrix.entries must be a non-empty array")

    entries: list[ExecutionMatrixEntry] = []
    execution_ids: set[str] = set()
    pack_versions: set[tuple[str, str]] = set()
    plan_paths: set[str] = set()
    for index, raw_entry in enumerate(raw_entries):
        field = f"execution_matrix.entries[{index}]"
        entry = _object(raw_entry, field)
        _exact_keys(
            entry,
            {
                "execution_id",
                "enabled",
                "plan_file",
                "query_pack_id",
                "query_pack_version",
            },
            field,
        )
        execution_id = _identifier(entry["execution_id"], f"{field}.execution_id")
        if execution_id in execution_ids:
            _fail("execution matrix contains duplicate execution_id")
        execution_ids.add(execution_id)
        entry_enabled = _boolean(entry["enabled"], f"{field}.enabled")
        plan_file = entry["plan_file"]
        if (
            not isinstance(plan_file, str)
            or not plan_file
            or Path(plan_file).is_absolute()
            or Path(plan_file).parent != Path("config/wb/collection_plans")
            or Path(plan_file).suffix != ".json"
        ):
            _fail(f"{field}.plan_file is invalid")
        if plan_file in plan_paths:
            _fail("execution matrix contains duplicate plan_file")
        plan_paths.add(plan_file)
        query_pack_id = _identifier(
            entry["query_pack_id"],
            f"{field}.query_pack_id",
        )
        query_pack_version = _version(
            entry["query_pack_version"],
            f"{field}.query_pack_version",
        )
        pack_key = (query_pack_id, query_pack_version)
        if pack_key in pack_versions:
            _fail("execution matrix contains duplicate query pack version")
        pack_versions.add(pack_key)
        bundle = load_collection_plan_bundle(
            project_root=root,
            plan_path=root / plan_file,
            region_registry_path=root / "config/wb/regions.json",
        )
        if (
            bundle.query_pack.query_pack_id != query_pack_id
            or bundle.query_pack.version != query_pack_version
        ):
            _fail("execution matrix query pack identity mismatch")
        if entry_enabled and not bundle.query_pack.enabled:
            _fail("enabled execution references a disabled query pack")
        entries.append(
            ExecutionMatrixEntry(
                execution_id=execution_id,
                enabled=entry_enabled,
                plan_file=plan_file,
                query_pack_id=query_pack_id,
                query_pack_version=query_pack_version,
                bundle=bundle,
            )
        )
    if enabled and not any(entry.enabled for entry in entries):
        _fail("enabled execution matrix has no enabled entries")
    return ExecutionMatrix(
        source_path=source_path,
        source_sha256=hashlib.sha256(encoded).hexdigest(),
        execution_matrix_id=matrix_id,
        enabled=enabled,
        entries=tuple(entries),
    )
