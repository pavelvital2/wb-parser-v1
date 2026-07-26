from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import stat
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from app.common.config import AppConfig
from app.common.csv_io import read_csv_rows, write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.sellers.engine import SellersEngine, SellersRunScope
from app.serp.collection_plan import (
    CollectionPlanBundle,
    CollectionRuntimeWindow,
    load_collection_plan_bundle,
)
from app.serp.collection_plan_runner import (
    DeadlineGuard,
    PRODUCT_FIELDS,
    RESUMABLE_MANIFEST_SCHEMA_VERSION,
    ScopedPaths,
    acquire_collection_plan_locks,
)
from app.warehouse.wb_regional import ingest_regional_run


FOUR_REGION_PLAN_ID = "shevron-four-regions-top1000-v2"
FOUR_REGION_IDS = ("moscow", "rostov-on-don", "novosibirsk", "kazan")
FOUR_REGION_QUERY_PACK_ID = "shevron-core"
FOUR_REGION_QUERY_PACK_VERSION = "2026-07-26.1"
EXPECTED_QUERIES = 30
MAX_PAGES = 1200
MAX_POSITIONS = 120000
DOWNSTREAM_SCHEMA = "wb_four_region_downstream_v1"
DOWNSTREAM_ATTEMPT_SCHEMA = "wb_four_region_attempt_v1"
BRIDGE_FIELDS = list(PRODUCT_FIELDS)
PRE_CUTOVER_DOWNSTREAM_MODE = "pre_cutover_legacy_nightly_protected_v1"
LEGACY_NIGHTLY_START_MSK = "00:15"
REVIEWED_FOUR_REGION_RUNTIME_WINDOW = CollectionRuntimeWindow(
    mode="bounded_resumable",
    scheduled_start_msk="00:15",
    new_run_start_grace_seconds=1800,
    max_invocation_runtime_seconds=21600,
    absolute_cutoff_msk="23:00",
    minimum_resume_window_seconds=1800,
    finalization_reserve_seconds=60,
)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
_STATE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_RFC3339_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)$"
)
DOWNSTREAM_LINEAGE_SCHEMA = "wb_four_region_lineage_v1"
DOWNSTREAM_LATEST_SCHEMA = "wb_four_region_latest_v1"
_ATTEMPT_STAGES = frozenset(
    {
        "collection",
        "preflight",
        "lock_acquisition",
        "state_transition",
        "input_build",
        "sellers",
        "warehouse",
        "state_publication",
    }
)
_ATTEMPT_LOCK_OWNERSHIP = frozenset({"not_acquired", "acquired"})
_DOWNSTREAM_STAGE_ORDER = {
    "state_transition": 0,
    "input_build": 1,
    "sellers": 2,
    "warehouse": 3,
    "state_publication": 4,
    "complete": 5,
}


@dataclass(frozen=True, slots=True)
class FourRegionInputs:
    root: Path
    seller_input_path: Path
    bridge_path: Path
    seller_input_sha256: str
    bridge_sha256: str
    positions_count: int
    unique_products_count: int
    unique_suppliers_count: int
    missing_supplier_products: int
    duplicate_product_positions: int
    region_counts: Mapping[str, Mapping[str, int]]
    collection_lineage: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DownstreamExecutionContract:
    mode: str
    legacy_nightly_start_msk: str
    protected_duration_seconds: int
    minimum_clearance_seconds: int

    @classmethod
    def pre_cutover(cls) -> "DownstreamExecutionContract":
        return cls(
            mode=PRE_CUTOVER_DOWNSTREAM_MODE,
            legacy_nightly_start_msk=LEGACY_NIGHTLY_START_MSK,
            protected_duration_seconds=(
                REVIEWED_FOUR_REGION_RUNTIME_WINDOW
                .max_invocation_runtime_seconds
            ),
            minimum_clearance_seconds=(
                REVIEWED_FOUR_REGION_RUNTIME_WINDOW.minimum_resume_window_seconds
            ),
        )

    def ensure_start_allowed(self, current: datetime) -> None:
        if self.mode != PRE_CUTOVER_DOWNSTREAM_MODE:
            raise CriticalPipelineError(
                "unsupported downstream execution contract"
            )
        if current.tzinfo is None:
            raise CriticalPipelineError(
                "downstream clock must return timezone-aware datetime"
            )
        try:
            hour, minute = (
                int(part)
                for part in self.legacy_nightly_start_msk.split(":", 1)
            )
            protected_time = time(hour, minute)
        except (TypeError, ValueError) as exc:
            raise CriticalPipelineError(
                "downstream protected start is invalid"
            ) from exc
        current_msk = current.astimezone(MOSCOW_TZ)
        protected_start = datetime.combine(
            current_msk.date(),
            protected_time,
            tzinfo=MOSCOW_TZ,
        )
        protected_end = protected_start + timedelta(
            seconds=self.protected_duration_seconds
        )
        if protected_start <= current_msk < protected_end:
            raise CriticalPipelineError(
                "downstream blocked by protected legacy nightly window"
            )
        next_protected_start = (
            protected_start
            if current_msk < protected_start
            else protected_start + timedelta(days=1)
        )
        if (
            next_protected_start - current_msk
        ).total_seconds() < self.minimum_clearance_seconds:
            raise CriticalPipelineError(
                "downstream has insufficient clearance before legacy nightly"
            )

    def evidence(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "legacy_nightly_start_msk": self.legacy_nightly_start_msk,
            "legacy_boundary_source": "pre_cutover_contract_v1",
            "protected_duration_seconds": self.protected_duration_seconds,
            "minimum_clearance_seconds": self.minimum_clearance_seconds,
        }


@dataclass(slots=True)
class _AuthoritativeStateLease:
    project_root: Path
    state_path: Path
    latest_path: Path
    run_id: str
    prior_state_bytes: bytes | None
    prior_latest_bytes: bytes | None
    active: bool = True
    state_written: bool = False
    reconcile_only: bool = False
    already_published: bool = False
    expected_state_bytes: bytes | None = None
    expected_state_sha256: str | None = None
    latest_published: bool = False
    candidate_lineage: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class _TrustedFileSnapshot:
    payload: bytes
    sha256: str
    device: int
    inode: int
    owner_uid: int
    mode: int
    size: int
    mtime_ns: int
    ctime_ns: int


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    ).encode("utf-8")


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError(f"invalid scoped JSON artifact: {path.name}") from exc
    if not isinstance(payload, dict):
        raise CriticalPipelineError(f"scoped JSON artifact is not an object: {path.name}")
    return payload


def _safe_project_file(root: Path, relative: str, *, suffix: str) -> Path:
    path = root / relative
    lexical = Path(os.path.abspath(path))
    try:
        parts = lexical.relative_to(root).parts
    except ValueError as exc:
        raise CriticalPipelineError("scoped source path escapes project root") from exc
    current = root
    for part in parts:
        current /= part
        if current.is_symlink():
            raise CriticalPipelineError("scoped source path uses symlink")
    if lexical.suffix != suffix or not lexical.is_file():
        raise CriticalPipelineError("scoped source is not the expected regular file")
    return lexical


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _trusted_file_snapshot(
    path: Path,
    *,
    expected_bytes: bytes | None = None,
    allow_missing: bool = False,
) -> _TrustedFileSnapshot | None:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        if allow_missing:
            return None
        raise CriticalPipelineError(
            "authoritative publication file is missing"
        ) from None
    except OSError as exc:
        raise CriticalPipelineError(
            "authoritative publication file is unsafe"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_mode & 0o022
        ):
            raise CriticalPipelineError(
                "authoritative publication file metadata is unsafe"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        current = path.lstat()
    except OSError as exc:
        raise CriticalPipelineError(
            "authoritative publication path changed during verification"
        ) from exc
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_uid",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(
        getattr(before, field) != getattr(after, field)
        for field in stable_fields
    ) or (
        current.st_dev != after.st_dev
        or current.st_ino != after.st_ino
        or not stat.S_ISREG(current.st_mode)
    ):
        raise CriticalPipelineError(
            "authoritative publication file changed during verification"
        )
    if len(payload) != after.st_size:
        raise CriticalPipelineError(
            "authoritative publication file size mismatch"
        )
    if expected_bytes is not None and payload != expected_bytes:
        raise CriticalPipelineError(
            "authoritative publication bytes mismatch"
        )
    return _TrustedFileSnapshot(
        payload=payload,
        sha256=_sha256_bytes(payload),
        device=after.st_dev,
        inode=after.st_ino,
        owner_uid=after.st_uid,
        mode=stat.S_IMODE(after.st_mode),
        size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
    )


def _atomic_replace_bytes(
    path: Path,
    *,
    new_bytes: bytes,
    on_replaced: Callable[[], None] | None = None,
) -> None:
    temp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".candidate",
            delete=False,
        ) as handle:
            temp = Path(handle.name)
            os.chmod(temp, 0o600)
            handle.write(new_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        temp = None
        if on_replaced is not None:
            on_replaced()
        _fsync_directory(path.parent)
    finally:
        if temp is not None:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass


def _safe_state_parent(path: Path, *, project_root: Path) -> None:
    root = Path(os.path.abspath(project_root))
    lexical = Path(os.path.abspath(path))
    try:
        relative = lexical.parent.relative_to(root)
    except ValueError as exc:
        raise CriticalPipelineError(
            "downstream state path escapes project root"
        ) from exc
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise CriticalPipelineError(
                "downstream state path uses symlink"
            )
        if current.exists():
            if not current.is_dir():
                raise CriticalPipelineError(
                    "downstream state parent is not a directory"
                )
            continue
        try:
            current.mkdir(mode=0o700)
        except FileExistsError:
            if current.is_symlink() or not current.is_dir():
                raise CriticalPipelineError(
                    "downstream state parent is unsafe"
                ) from None
            continue
        parent_fd = os.open(current.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)


def _immutable_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    project_root: Path,
) -> None:
    _safe_state_parent(path, project_root=project_root)
    encoded = _json_bytes(payload)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _optional_regular_bytes(path: Path) -> bytes | None:
    snapshot = _trusted_file_snapshot(path, allow_missing=True)
    return snapshot.payload if snapshot is not None else None


def _strict_utc(value: Any, *, field: str) -> datetime:
    if type(value) is not str or not _RFC3339_UTC_RE.fullmatch(value):
        raise CriticalPipelineError(f"{field} is not strict RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CriticalPipelineError(
            f"{field} is not strict RFC3339 UTC"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise CriticalPipelineError(f"{field} is not UTC")
    return parsed.astimezone(UTC)


def _epoch_microseconds(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def _strict_int(
    value: Any,
    *,
    field: str,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if type(value) is not int or value < minimum:
        raise CriticalPipelineError(f"{field} is invalid")
    if maximum is not None and value > maximum:
        raise CriticalPipelineError(f"{field} is invalid")
    return value


def _strict_sha256(value: Any, *, field: str) -> str:
    if type(value) is not str or not _SHA256_RE.fullmatch(value):
        raise CriticalPipelineError(f"{field} is invalid")
    return value


def _collection_lineage(
    *,
    config: AppConfig,
    bundle: CollectionPlanBundle,
    paths: ScopedPaths,
    run_id: str,
) -> dict[str, Any]:
    snapshot = _trusted_file_snapshot(paths.manifest_path)
    if snapshot is None:
        raise CriticalPipelineError("four-region collection manifest is missing")
    try:
        manifest = json.loads(snapshot.payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError(
            "four-region collection manifest is invalid"
        ) from exc
    if not isinstance(manifest, dict):
        raise CriticalPipelineError(
            "four-region collection manifest is invalid"
        )
    required = {
        "schema_version": RESUMABLE_MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "collection_plan_id": FOUR_REGION_PLAN_ID,
        "query_pack_id": bundle.query_pack.query_pack_id,
        "query_pack_version": bundle.query_pack.version,
        "query_pack_sha256": bundle.query_pack_sha256,
        "collection_plan_sha256": bundle.collection_plan_sha256,
        "region_registry_sha256": bundle.region_registry_sha256,
        "publication_mode": "none",
        "sellers_mode": "disabled",
        "proxy_rotation_mode": "disabled",
        "status": "success",
        "complete": True,
    }
    if any(
        manifest.get(field) != expected
        or type(manifest.get(field)) is not type(expected)
        for field, expected in required.items()
    ):
        raise CriticalPipelineError(
            "four-region collection lineage mismatch"
        )
    effective_sha256 = _strict_sha256(
        manifest.get("effective_plan_sha256"),
        field="effective_plan_sha256",
    )
    started_text = manifest.get("started_at_utc")
    finished_text = manifest.get("finished_at_utc")
    started = _strict_utc(started_text, field="started_at_utc")
    finished = _strict_utc(finished_text, field="finished_at_utc")
    if finished < started:
        raise CriticalPipelineError(
            "four-region collection timestamps are inconsistent"
        )
    return {
        "schema_version": DOWNSTREAM_LINEAGE_SCHEMA,
        "collection_run_id": run_id,
        "collection_started_at_utc": started_text,
        "collection_finished_at_utc": finished_text,
        "collection_order_epoch_us": _epoch_microseconds(started),
        "collection_manifest_path": paths.manifest_path.relative_to(
            config.project_root
        ).as_posix(),
        "collection_manifest_sha256": snapshot.sha256,
        "collection_plan_sha256": bundle.collection_plan_sha256,
        "query_pack_id": bundle.query_pack.query_pack_id,
        "query_pack_version": bundle.query_pack.version,
        "query_pack_sha256": bundle.query_pack_sha256,
        "region_registry_sha256": bundle.region_registry_sha256,
        "effective_plan_sha256": effective_sha256,
    }


def _validate_collection_lineage(
    lineage: Any,
    *,
    project_root: Path,
    expected_run_id: str,
) -> dict[str, Any]:
    if not isinstance(lineage, dict) or set(lineage) != {
        "schema_version",
        "collection_run_id",
        "collection_started_at_utc",
        "collection_finished_at_utc",
        "collection_order_epoch_us",
        "collection_manifest_path",
        "collection_manifest_sha256",
        "collection_plan_sha256",
        "query_pack_id",
        "query_pack_version",
        "query_pack_sha256",
        "region_registry_sha256",
        "effective_plan_sha256",
    }:
        raise CriticalPipelineError(
            "downstream collection lineage contract mismatch"
        )
    if (
        lineage.get("schema_version") != DOWNSTREAM_LINEAGE_SCHEMA
        or lineage.get("collection_run_id") != expected_run_id
        or not _STATE_ID_RE.fullmatch(expected_run_id)
        or lineage.get("query_pack_id") != FOUR_REGION_QUERY_PACK_ID
        or lineage.get("query_pack_version") != FOUR_REGION_QUERY_PACK_VERSION
    ):
        raise CriticalPipelineError(
            "downstream collection lineage identity mismatch"
        )
    started = _strict_utc(
        lineage.get("collection_started_at_utc"),
        field="collection_started_at_utc",
    )
    finished = _strict_utc(
        lineage.get("collection_finished_at_utc"),
        field="collection_finished_at_utc",
    )
    order = _strict_int(
        lineage.get("collection_order_epoch_us"),
        field="collection_order_epoch_us",
        minimum=1,
    )
    if finished < started or order != _epoch_microseconds(started):
        raise CriticalPipelineError(
            "downstream collection lineage order mismatch"
        )
    for field in (
        "collection_manifest_sha256",
        "collection_plan_sha256",
        "query_pack_sha256",
        "region_registry_sha256",
        "effective_plan_sha256",
    ):
        _strict_sha256(lineage.get(field), field=field)
    expected_manifest = (
        Path("state/wb_collection_plans")
        / FOUR_REGION_PLAN_ID
        / expected_run_id
        / "manifest.json"
    )
    if lineage.get("collection_manifest_path") != expected_manifest.as_posix():
        raise CriticalPipelineError(
            "downstream collection manifest path mismatch"
        )
    manifest_path = _safe_project_file(
        project_root,
        expected_manifest.as_posix(),
        suffix=".json",
    )
    snapshot = _trusted_file_snapshot(manifest_path)
    if (
        snapshot is None
        or snapshot.sha256 != lineage["collection_manifest_sha256"]
    ):
        raise CriticalPipelineError(
            "downstream collection manifest hash mismatch"
        )
    try:
        manifest = json.loads(snapshot.payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError(
            "downstream collection manifest is invalid"
        ) from exc
    manifest_required = {
        "schema_version": RESUMABLE_MANIFEST_SCHEMA_VERSION,
        "run_id": expected_run_id,
        "collection_plan_id": FOUR_REGION_PLAN_ID,
        "query_pack_id": lineage["query_pack_id"],
        "query_pack_version": lineage["query_pack_version"],
        "query_pack_sha256": lineage["query_pack_sha256"],
        "collection_plan_sha256": lineage["collection_plan_sha256"],
        "region_registry_sha256": lineage["region_registry_sha256"],
        "effective_plan_sha256": lineage["effective_plan_sha256"],
        "publication_mode": "none",
        "sellers_mode": "disabled",
        "proxy_rotation_mode": "disabled",
        "started_at_utc": lineage["collection_started_at_utc"],
        "finished_at_utc": lineage["collection_finished_at_utc"],
        "status": "success",
        "complete": True,
    }
    if not isinstance(manifest, dict) or any(
        manifest.get(field) != expected
        or type(manifest.get(field)) is not type(expected)
        for field, expected in manifest_required.items()
    ):
        raise CriticalPipelineError(
            "downstream collection manifest provenance mismatch"
        )
    return dict(lineage)


def _validated_artifact(
    *,
    project_root: Path,
    relative_path: Any,
    expected_sha256: Any,
    suffix: str,
) -> Path:
    if type(relative_path) is not str:
        raise CriticalPipelineError("downstream artifact path is invalid")
    expected = _strict_sha256(
        expected_sha256,
        field="downstream artifact sha256",
    )
    path = _safe_project_file(project_root, relative_path, suffix=suffix)
    snapshot = _trusted_file_snapshot(path)
    if snapshot is None or snapshot.sha256 != expected:
        raise CriticalPipelineError("downstream artifact hash mismatch")
    return path


def _validate_legacy_evidence(value: Any) -> None:
    if not isinstance(value, dict):
        raise CriticalPipelineError("legacy warehouse evidence is invalid")
    status = value.get("status")
    if status == "source_absent":
        if (
            set(value) != {"status", "positions", "sellers"}
            or _strict_int(value.get("positions"), field="legacy positions")
            != 0
            or _strict_int(value.get("sellers"), field="legacy sellers") != 0
        ):
            raise CriticalPipelineError(
                "legacy warehouse source-absent evidence is invalid"
            )
        return
    if status not in {"updated", "no_changes"} or set(value) != {
        "status",
        "positions",
        "sellers",
        "inserted_positions",
        "inserted_sellers",
        "run_quality",
        "inserted_run_quality",
        "revision_id",
    }:
        raise CriticalPipelineError("legacy warehouse evidence is invalid")
    for field in (
        "positions",
        "sellers",
        "inserted_positions",
        "inserted_sellers",
        "run_quality",
        "inserted_run_quality",
    ):
        _strict_int(value.get(field), field=f"legacy {field}")
    _strict_sha256(value.get("revision_id"), field="legacy revision_id")


def _validate_completed_state_bytes(
    payload_bytes: bytes,
    *,
    project_root: Path,
    expected_run_id: str | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(payload_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError(
            "completed downstream state is invalid"
        ) from exc
    if not isinstance(payload, dict) or _json_bytes(payload) != payload_bytes:
        raise CriticalPipelineError(
            "completed downstream state is not canonical"
        )
    if set(payload) != {
        "schema_version",
        "run_id",
        "collection_plan_id",
        "status",
        "complete",
        "stage",
        "execution_contract",
        "finished_at_utc",
        "lineage",
        "regions",
        "totals",
        "sellers",
        "warehouse",
        "failure_reason",
        "artifacts",
    }:
        raise CriticalPipelineError(
            "completed downstream state contract mismatch"
        )
    run_id = payload.get("run_id")
    if (
        type(run_id) is not str
        or not _STATE_ID_RE.fullmatch(run_id)
        or (expected_run_id is not None and run_id != expected_run_id)
        or payload.get("schema_version") != DOWNSTREAM_SCHEMA
        or payload.get("collection_plan_id") != FOUR_REGION_PLAN_ID
        or payload.get("status") != "success"
        or payload.get("complete") is not True
        or payload.get("stage") != "complete"
        or payload.get("failure_reason") is not None
        or payload.get("execution_contract")
        != DownstreamExecutionContract.pre_cutover().evidence()
    ):
        raise CriticalPipelineError(
            "completed downstream state semantic mismatch"
        )
    lineage = _validate_collection_lineage(
        payload.get("lineage"),
        project_root=project_root,
        expected_run_id=run_id,
    )
    finished = _strict_utc(
        payload.get("finished_at_utc"),
        field="downstream finished_at_utc",
    )
    collection_finished = _strict_utc(
        lineage["collection_finished_at_utc"],
        field="collection_finished_at_utc",
    )
    if finished < collection_finished:
        raise CriticalPipelineError(
            "downstream completion predates collection completion"
        )

    regions = payload.get("regions")
    if not isinstance(regions, list) or len(regions) != len(FOUR_REGION_IDS):
        raise CriticalPipelineError("completed downstream regions are invalid")
    pages = positions = duplicates = 0
    for expected_region, region in zip(FOUR_REGION_IDS, regions, strict=True):
        if not isinstance(region, dict) or set(region) != {
            "region_id",
            "pages",
            "positions",
            "duplicate_product_positions",
            "max_position_capacity",
        }:
            raise CriticalPipelineError(
                "completed downstream region evidence is invalid"
            )
        if region.get("region_id") != expected_region:
            raise CriticalPipelineError(
                "completed downstream region order mismatch"
            )
        pages += _strict_int(
            region.get("pages"),
            field="region pages",
            minimum=1,
            maximum=EXPECTED_QUERIES * 10,
        )
        region_positions = _strict_int(
            region.get("positions"),
            field="region positions",
            minimum=1,
            maximum=EXPECTED_QUERIES * 1000,
        )
        positions += region_positions
        region_duplicates = _strict_int(
            region.get("duplicate_product_positions"),
            field="region duplicate positions",
            maximum=region_positions,
        )
        duplicates += region_duplicates
        if region.get("max_position_capacity") != EXPECTED_QUERIES * 1000:
            raise CriticalPipelineError(
                "completed downstream region capacity mismatch"
            )

    totals = payload.get("totals")
    if not isinstance(totals, dict) or set(totals) != {
        "pages",
        "positions",
        "unique_products",
        "unique_suppliers",
        "missing_supplier_products",
        "duplicate_product_positions",
        "max_position_capacity",
    }:
        raise CriticalPipelineError("completed downstream totals are invalid")
    unique_products = _strict_int(
        totals.get("unique_products"),
        field="unique products",
        minimum=1,
        maximum=positions,
    )
    unique_suppliers = _strict_int(
        totals.get("unique_suppliers"),
        field="unique suppliers",
        maximum=unique_products,
    )
    missing_suppliers = _strict_int(
        totals.get("missing_supplier_products"),
        field="missing supplier products",
        maximum=unique_products,
    )
    if (
        totals.get("pages") != pages
        or totals.get("positions") != positions
        or totals.get("duplicate_product_positions") != duplicates
        or totals.get("max_position_capacity") != MAX_POSITIONS
        or unique_suppliers + missing_suppliers > unique_products
    ):
        raise CriticalPipelineError(
            "completed downstream totals are inconsistent"
        )

    sellers = payload.get("sellers")
    if not isinstance(sellers, dict) or set(sellers) != {
        "status",
        "items_ok",
        "items_error",
        "source_sha256",
        "output_path",
        "output_sha256",
    }:
        raise CriticalPipelineError("completed seller evidence is invalid")
    if (
        sellers.get("status") != "success"
        or _strict_int(sellers.get("items_ok"), field="seller items_ok")
        != unique_suppliers
        or _strict_int(sellers.get("items_error"), field="seller items_error")
        != 0
    ):
        raise CriticalPipelineError("completed seller result is invalid")

    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != {
        "bridge_path",
        "bridge_sha256",
        "seller_input_path",
        "seller_input_sha256",
        "seller_output_path",
        "seller_output_sha256",
    }:
        raise CriticalPipelineError("completed artifact evidence is invalid")
    expected_artifact_paths = {
        "bridge_path": (
            Path("data/marts/wb_four_region")
            / FOUR_REGION_PLAN_ID
            / run_id
            / "regional_query_product_position_bridge.csv"
        ).as_posix(),
        "seller_input_path": (
            Path("data/marts/wb_four_region")
            / FOUR_REGION_PLAN_ID
            / run_id
            / "products_for_sellers.csv"
        ).as_posix(),
        "seller_output_path": (
            Path("data/marts/sellers_scoped")
            / FOUR_REGION_PLAN_ID
            / run_id
            / "sellers_daily.csv"
        ).as_posix(),
    }
    if any(
        artifacts.get(field) != expected
        for field, expected in expected_artifact_paths.items()
    ):
        raise CriticalPipelineError(
            "completed artifact path contract mismatch"
        )
    _validated_artifact(
        project_root=project_root,
        relative_path=artifacts.get("bridge_path"),
        expected_sha256=artifacts.get("bridge_sha256"),
        suffix=".csv",
    )
    _validated_artifact(
        project_root=project_root,
        relative_path=artifacts.get("seller_input_path"),
        expected_sha256=artifacts.get("seller_input_sha256"),
        suffix=".csv",
    )
    _validated_artifact(
        project_root=project_root,
        relative_path=artifacts.get("seller_output_path"),
        expected_sha256=artifacts.get("seller_output_sha256"),
        suffix=".csv",
    )
    if (
        sellers.get("source_sha256") != artifacts.get("seller_input_sha256")
        or sellers.get("output_path") != artifacts.get("seller_output_path")
        or sellers.get("output_sha256") != artifacts.get("seller_output_sha256")
    ):
        raise CriticalPipelineError(
            "completed seller artifact provenance mismatch"
        )

    warehouse = payload.get("warehouse")
    if not isinstance(warehouse, dict) or set(warehouse) != {
        "status",
        "positions_count",
        "sellers_count",
        "legacy_yaroslavl",
        "ingestion_evidence",
    }:
        raise CriticalPipelineError("completed warehouse evidence is invalid")
    if (
        warehouse.get("status") not in {"success", "already_ingested"}
        or _strict_int(
            warehouse.get("positions_count"),
            field="warehouse positions",
        )
        != positions
    ):
        raise CriticalPipelineError("completed warehouse result is invalid")
    _strict_int(
        warehouse.get("sellers_count"),
        field="warehouse sellers",
    )
    _validate_legacy_evidence(warehouse.get("legacy_yaroslavl"))
    ingestion = warehouse.get("ingestion_evidence")
    if not isinstance(ingestion, dict) or set(ingestion) != {
        "collection_manifest_sha256",
        "bridge_sha256",
        "sellers_sha256",
    }:
        raise CriticalPipelineError(
            "completed warehouse ingestion evidence is invalid"
        )
    if (
        ingestion.get("collection_manifest_sha256")
        != lineage["collection_manifest_sha256"]
        or ingestion.get("bridge_sha256") != artifacts["bridge_sha256"]
        or ingestion.get("sellers_sha256") != artifacts["seller_output_sha256"]
    ):
        raise CriticalPipelineError(
            "completed warehouse ingestion provenance mismatch"
        )
    return payload


def _validate_latest_bytes(
    payload_bytes: bytes,
    *,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    try:
        pointer = json.loads(payload_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError("downstream latest pointer is invalid") from exc
    if (
        not isinstance(pointer, dict)
        or _json_bytes(pointer) != payload_bytes
        or set(pointer)
        != {
            "schema_version",
            "run_id",
            "state_path",
            "state_sha256",
            "lineage",
        }
        or pointer.get("schema_version") != DOWNSTREAM_LATEST_SCHEMA
    ):
        raise CriticalPipelineError("downstream latest pointer is invalid")
    run_id = pointer.get("run_id")
    if type(run_id) is not str or not _STATE_ID_RE.fullmatch(run_id):
        raise CriticalPipelineError("downstream latest run identity is invalid")
    expected_path = (
        Path("state/wb_four_region_nightly")
        / FOUR_REGION_PLAN_ID
        / run_id
        / "state.json"
    )
    if pointer.get("state_path") != expected_path.as_posix():
        raise CriticalPipelineError("downstream latest state path mismatch")
    state_path = _safe_project_file(
        project_root,
        expected_path.as_posix(),
        suffix=".json",
    )
    state_snapshot = _trusted_file_snapshot(state_path)
    expected_sha = _strict_sha256(
        pointer.get("state_sha256"),
        field="latest state_sha256",
    )
    if state_snapshot is None or state_snapshot.sha256 != expected_sha:
        raise CriticalPipelineError(
            "published downstream state integrity mismatch"
        )
    state = _validate_completed_state_bytes(
        state_snapshot.payload,
        project_root=project_root,
        expected_run_id=run_id,
    )
    if pointer.get("lineage") != state.get("lineage"):
        raise CriticalPipelineError("downstream latest lineage mismatch")
    return pointer, state, state_snapshot.payload


def _lineage_order(lineage: Mapping[str, Any]) -> int:
    return _strict_int(
        lineage.get("collection_order_epoch_us"),
        field="collection_order_epoch_us",
        minimum=1,
    )


def _begin_authoritative_state_transition(
    *,
    state_path: Path,
    latest_path: Path,
    run_id: str,
    project_root: Path,
    candidate_lineage: Mapping[str, Any],
) -> _AuthoritativeStateLease:
    _safe_state_parent(state_path, project_root=project_root)
    _safe_state_parent(latest_path, project_root=project_root)
    state_bytes = _optional_regular_bytes(state_path)
    latest_bytes = _optional_regular_bytes(latest_path)
    validated_candidate_lineage = _validate_collection_lineage(
        candidate_lineage,
        project_root=project_root,
        expected_run_id=run_id,
    )
    state_payload: dict[str, Any] | None = None
    completed_state = False
    if state_bytes is not None:
        try:
            loaded = json.loads(state_bytes)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CriticalPipelineError(
                "downstream authoritative state is invalid"
            ) from exc
        if not isinstance(loaded, dict):
            raise CriticalPipelineError(
                "downstream authoritative state is invalid"
            )
        state_payload = loaded
        if (
            state_payload.get("schema_version") != DOWNSTREAM_SCHEMA
            or state_payload.get("run_id") != run_id
            or state_payload.get("collection_plan_id")
            != FOUR_REGION_PLAN_ID
            or state_payload.get("status") not in {"failed", "success"}
            or type(state_payload.get("complete")) is not bool
            or (
                state_payload.get("status") == "success"
                and state_payload.get("complete") is not True
            )
            or (
                state_payload.get("status") == "failed"
                and state_payload.get("complete") is not False
            )
        ):
            raise CriticalPipelineError(
                "downstream authoritative state contract mismatch"
            )
        completed_state = (
            state_payload["status"] == "success"
            and state_payload["complete"] is True
        )
        if completed_state:
            state_payload = _validate_completed_state_bytes(
                state_bytes,
                project_root=project_root,
                expected_run_id=run_id,
            )
            if state_payload["lineage"] != validated_candidate_lineage:
                raise CriticalPipelineError(
                    "completed downstream state lineage mismatch"
                )

    latest_payload: dict[str, Any] | None = None
    latest_state_bytes: bytes | None = None
    if latest_bytes is not None:
        latest_payload, latest_state, latest_state_bytes = _validate_latest_bytes(
            latest_bytes,
            project_root=project_root,
        )
        if latest_payload["run_id"] == run_id:
            if not completed_state or latest_state_bytes != state_bytes:
                raise CriticalPipelineError(
                    "published downstream state integrity mismatch"
                )
        elif (
            _lineage_order(validated_candidate_lineage)
            <= _lineage_order(latest_state["lineage"])
        ):
            raise CriticalPipelineError(
                "downstream latest is newer than reconcile candidate"
            )

    return _AuthoritativeStateLease(
        project_root=project_root,
        state_path=state_path,
        latest_path=latest_path,
        run_id=run_id,
        prior_state_bytes=state_bytes,
        prior_latest_bytes=latest_bytes,
        state_written=completed_state,
        reconcile_only=completed_state,
        already_published=(
            completed_state
            and latest_payload is not None
            and latest_payload["run_id"] == run_id
        ),
        expected_state_bytes=state_bytes if completed_state else None,
        expected_state_sha256=(
            _sha256_bytes(state_bytes)
            if completed_state and state_bytes is not None
            else None
        ),
        candidate_lineage=validated_candidate_lineage,
    )


def _write_authoritative_state(
    lease: _AuthoritativeStateLease,
    payload: Mapping[str, Any],
) -> None:
    if not lease.active or lease.state_written:
        raise CriticalPipelineError(
            "downstream authoritative state lease is not writable"
        )
    if (
        payload.get("schema_version") != DOWNSTREAM_SCHEMA
        or payload.get("run_id") != lease.run_id
        or payload.get("collection_plan_id") != FOUR_REGION_PLAN_ID
        or payload.get("status") not in {"failed", "success"}
        or type(payload.get("complete")) is not bool
        or (
            payload.get("status") == "success"
            and payload.get("complete") is not True
        )
        or (
            payload.get("status") == "failed"
            and payload.get("complete") is not False
        )
    ):
        raise CriticalPipelineError(
            "downstream authoritative state transition is invalid"
        )
    encoded = _json_bytes(payload)
    if lease.prior_state_bytes is not None:
        try:
            prior_payload = json.loads(lease.prior_state_bytes)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CriticalPipelineError(
                "prior downstream state is invalid"
            ) from exc
        if not isinstance(prior_payload, dict):
            raise CriticalPipelineError("prior downstream state is invalid")
        if prior_payload.get("status") == "success":
            raise CriticalPipelineError(
                "completed downstream state is immutable"
            )
        if payload.get("status") == "failed":
            prior_stage = _DOWNSTREAM_STAGE_ORDER.get(
                str(prior_payload.get("stage"))
            )
            next_stage = _DOWNSTREAM_STAGE_ORDER.get(str(payload.get("stage")))
            if (
                prior_stage is None
                or next_stage is None
                or next_stage < prior_stage
            ):
                raise CriticalPipelineError(
                    "downstream failure state transition is not monotonic"
                )
    if payload.get("status") == "success":
        _validate_completed_state_bytes(
            encoded,
            project_root=lease.project_root,
            expected_run_id=lease.run_id,
        )
        if payload.get("lineage") != lease.candidate_lineage:
            raise CriticalPipelineError(
                "downstream authoritative state lineage mismatch"
            )
    expected_sha256 = _sha256_bytes(encoded)
    current = _optional_regular_bytes(lease.state_path)
    if current != lease.prior_state_bytes:
        raise CriticalPipelineError(
            "downstream authoritative state changed during transition"
        )

    def mark_state_replaced() -> None:
        lease.expected_state_bytes = encoded
        lease.expected_state_sha256 = expected_sha256
        lease.state_written = True

    _atomic_replace_bytes(
        lease.state_path,
        new_bytes=encoded,
        on_replaced=mark_state_replaced,
    )
    verified = _trusted_file_snapshot(
        lease.state_path,
        expected_bytes=encoded,
    )
    if verified is None or verified.sha256 != expected_sha256:
        raise CriticalPipelineError(
            "downstream authoritative state verification failed"
        )
def _write_authoritative_latest(
    lease: _AuthoritativeStateLease,
    payload: Mapping[str, Any],
) -> None:
    if (
        not lease.active
        or not lease.state_written
        or lease.latest_published
        or lease.expected_state_bytes is None
        or lease.expected_state_sha256 is None
        or payload.get("schema_version") != DOWNSTREAM_LATEST_SCHEMA
        or payload.get("run_id") != lease.run_id
        or payload.get("state_path")
        != lease.state_path.relative_to(lease.project_root).as_posix()
        or payload.get("state_sha256") != lease.expected_state_sha256
        or payload.get("lineage") != lease.candidate_lineage
    ):
        raise CriticalPipelineError(
            "downstream latest transition is invalid"
        )
    if lease.already_published:
        current = _optional_regular_bytes(lease.latest_path)
        if current != lease.prior_latest_bytes:
            raise CriticalPipelineError(
                "downstream latest changed during same-run reconcile"
            )
        _validate_latest_bytes(current or b"", project_root=lease.project_root)
        lease.latest_published = True
        return
    encoded = _json_bytes(payload)
    _validate_completed_state_bytes(
        lease.expected_state_bytes,
        project_root=lease.project_root,
        expected_run_id=lease.run_id,
    )
    _trusted_file_snapshot(
        lease.state_path,
        expected_bytes=lease.expected_state_bytes,
    )
    current = _optional_regular_bytes(lease.latest_path)
    if current != lease.prior_latest_bytes:
        raise CriticalPipelineError(
            "downstream latest changed during transition"
        )
    if current is not None:
        current_pointer, current_state, _current_state_bytes = (
            _validate_latest_bytes(
                current,
                project_root=lease.project_root,
            )
        )
        candidate_order = _lineage_order(lease.candidate_lineage or {})
        current_order = _lineage_order(current_state["lineage"])
        if current_pointer["run_id"] != lease.run_id and (
            candidate_order <= current_order
        ):
            raise CriticalPipelineError(
                "downstream latest is newer than publication candidate"
            )
    _atomic_replace_bytes(lease.latest_path, new_bytes=encoded)
    state_snapshot = _trusted_file_snapshot(
        lease.state_path,
        expected_bytes=lease.expected_state_bytes,
    )
    latest_snapshot = _trusted_file_snapshot(
        lease.latest_path,
        expected_bytes=encoded,
    )
    final_state_snapshot = _trusted_file_snapshot(
        lease.state_path,
        expected_bytes=lease.expected_state_bytes,
    )
    if (
        state_snapshot is None
        or latest_snapshot is None
        or final_state_snapshot is None
        or state_snapshot.sha256 != lease.expected_state_sha256
        or final_state_snapshot.sha256 != lease.expected_state_sha256
        or latest_snapshot.sha256 != _sha256_bytes(encoded)
    ):
        raise CriticalPipelineError(
            "downstream publication transaction verification failed"
        )
    _validate_latest_bytes(encoded, project_root=lease.project_root)
    lease.latest_published = True


def _load_scoped_products(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def validate_four_region_bundle(bundle: CollectionPlanBundle) -> None:
    plan = bundle.collection_plan
    if plan.collection_plan_id != FOUR_REGION_PLAN_ID:
        raise CriticalPipelineError("unexpected four-region collection plan")
    if (
        bundle.query_pack.query_pack_id != FOUR_REGION_QUERY_PACK_ID
        or bundle.query_pack.version != FOUR_REGION_QUERY_PACK_VERSION
    ):
        raise CriticalPipelineError("four-region query-pack contract mismatch")
    if plan.region_set != FOUR_REGION_IDS:
        raise CriticalPipelineError("four-region order mismatch")
    if len(plan.query_ids) != EXPECTED_QUERIES or plan.depth != 1000:
        raise CriticalPipelineError("four-region query/depth contract mismatch")
    if plan.runtime_window is None:
        raise CriticalPipelineError("four-region bounded runtime contract is missing")
    if plan.runtime_window != REVIEWED_FOUR_REGION_RUNTIME_WINDOW:
        raise CriticalPipelineError(
            "four-region reviewed runtime contract mismatch"
        )


def deterministic_seller_rows(
    rows: list[dict[str, str]],
    *,
    region_ids: tuple[str, ...],
    query_ids: tuple[str, ...],
) -> list[dict[str, str]]:
    region_order = {region_id: index for index, region_id in enumerate(region_ids)}
    query_order = {query_id: index for index, query_id in enumerate(query_ids)}
    try:
        rows.sort(
            key=lambda row: (
                region_order[row["region_id"]],
                query_order[row["query_id"]],
                int(row["absolute_position"]),
                row["nmId"],
            )
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CriticalPipelineError(
            "regional seller input ordering fields are invalid"
        ) from exc
    unique_products: dict[str, dict[str, str]] = {}
    for row in rows:
        product_id = row.get("nmId", "")
        if not product_id:
            raise CriticalPipelineError("regional seller input product ID is missing")
        selected = unique_products.setdefault(product_id, row)
        if (
            not str(selected.get("supplier_id", "")).strip()
            and str(row.get("supplier_id", "")).strip()
        ):
            unique_products[product_id] = row
    return list(unique_products.values())


def build_four_region_inputs(
    *,
    config: AppConfig,
    bundle: CollectionPlanBundle,
    run_id: str,
) -> FourRegionInputs:
    validate_four_region_bundle(bundle)
    paths = ScopedPaths.build(
        project_root=config.project_root,
        collection_plan_id=bundle.collection_plan.collection_plan_id,
        run_id=run_id,
    )
    manifest = _json(paths.manifest_path)
    if (
        manifest.get("status") != "success"
        or manifest.get("complete") is not True
        or manifest.get("run_id") != run_id
        or manifest.get("collection_plan_sha256") != bundle.collection_plan_sha256
        or manifest.get("query_pack_sha256") != bundle.query_pack_sha256
        or manifest.get("region_registry_sha256") != bundle.region_registry_sha256
    ):
        raise CriticalPipelineError("four-region collection is not complete")
    collection_lineage = _collection_lineage(
        config=config,
        bundle=bundle,
        paths=paths,
        run_id=run_id,
    )
    totals = manifest.get("totals")
    if (
        not isinstance(totals, dict)
        or totals.get("regions_ok") != 4
        or totals.get("queries_ok") != len(FOUR_REGION_IDS) * EXPECTED_QUERIES
        or type(totals.get("pages_ok")) is not int
        or not 0 < totals["pages_ok"] <= MAX_PAGES
        or type(totals.get("products_ok")) is not int
        or not 0 < totals["products_ok"] <= MAX_POSITIONS
    ):
        raise CriticalPipelineError("four-region collection totals are invalid")
    resume = manifest.get("resume")
    segment_refs = resume.get("segments") if isinstance(resume, dict) else None
    if not isinstance(segment_refs, list):
        raise CriticalPipelineError("four-region query completion evidence is missing")
    completion_by_scope: dict[tuple[str, str], dict[str, Any]] = {}
    for ref in segment_refs:
        if not isinstance(ref, dict):
            raise CriticalPipelineError("four-region segment reference is invalid")
        scope = (str(ref.get("region_id", "")), str(ref.get("query_id", "")))
        completion = ref.get("completion")
        if (
            scope[0] not in FOUR_REGION_IDS
            or scope[1] not in bundle.collection_plan.query_ids
            or scope in completion_by_scope
            or not isinstance(completion, dict)
            or completion.get("complete") is not True
        ):
            raise CriticalPipelineError("four-region segment completion is invalid")
        completion_by_scope[scope] = completion
    if len(completion_by_scope) != len(FOUR_REGION_IDS) * EXPECTED_QUERIES:
        raise CriticalPipelineError("four-region query scopes are incomplete")

    latest = _json(paths.latest_path)
    if (
        latest.get("run_id") != run_id
        or latest.get("collection_plan_id") != FOUR_REGION_PLAN_ID
        or latest.get("effective_plan_sha256")
        != manifest.get("effective_plan_sha256")
    ):
        raise CriticalPipelineError("four-region scoped latest mismatch")
    region_refs = latest.get("regions")
    if not isinstance(region_refs, list) or [
        item.get("region_id") for item in region_refs if isinstance(item, dict)
    ] != list(FOUR_REGION_IDS):
        raise CriticalPipelineError("four-region latest region order mismatch")

    query_order = {
        query_id: index for index, query_id in enumerate(bundle.collection_plan.query_ids)
    }
    rows: list[dict[str, str]] = []
    region_counts: dict[str, dict[str, int]] = {}
    for region_ref in region_refs:
        region_id = region_ref["region_id"]
        region_manifest_path = _safe_project_file(
            config.project_root,
            str(region_ref.get("manifest_path", "")),
            suffix=".json",
        )
        if _sha256(region_manifest_path) != region_ref.get("manifest_sha256"):
            raise CriticalPipelineError("regional generation manifest hash mismatch")
        region_manifest = _json(region_manifest_path)
        outputs = region_manifest.get("outputs")
        if not isinstance(outputs, dict):
            raise CriticalPipelineError("regional generation outputs are missing")
        products_path = _safe_project_file(
            config.project_root,
            str(outputs.get("mart_products_path", "")),
            suffix=".csv",
        )
        if _sha256(products_path) != outputs.get("products_sha256"):
            raise CriticalPipelineError("regional products hash mismatch")
        region_rows = _load_scoped_products(products_path)
        by_query: dict[str, list[int]] = {}
        product_occurrences: dict[tuple[str, str], int] = {}
        for row in region_rows:
            if (
                row.get("run_id") != run_id
                or row.get("collection_plan_id") != FOUR_REGION_PLAN_ID
                or row.get("region_id") != region_id
                or row.get("query_id") not in query_order
            ):
                raise CriticalPipelineError("regional product provenance mismatch")
            product_key = (row["query_id"], row.get("nmId", ""))
            if not product_key[1]:
                raise CriticalPipelineError("regional product ID is missing")
            product_occurrences[product_key] = (
                product_occurrences.get(product_key, 0) + 1
            )
            try:
                position = int(row.get("absolute_position", ""))
            except ValueError as exc:
                raise CriticalPipelineError("regional position is invalid") from exc
            by_query.setdefault(row["query_id"], []).append(position)
        if list(by_query) != list(bundle.collection_plan.query_ids):
            raise CriticalPipelineError("regional query order mismatch")
        expected_region_pages = 0
        expected_region_positions = 0
        for query_id in bundle.collection_plan.query_ids:
            completion = completion_by_scope[(region_id, query_id)]
            capped_total = completion.get("capped_total")
            pages_count = completion.get("pages_count")
            products_count = completion.get("products_count")
            if (
                type(capped_total) is not int
                or type(pages_count) is not int
                or type(products_count) is not int
                or products_count != capped_total
                or by_query.get(query_id) != list(range(1, capped_total + 1))
            ):
                raise CriticalPipelineError(
                    "regional position sequence/completion mismatch"
                )
            expected_region_pages += pages_count
            expected_region_positions += products_count
        if (
            len(region_rows) != expected_region_positions
            or int(region_manifest.get("pages_count", 0)) != expected_region_pages
            or int(region_manifest.get("products_count", 0))
            != expected_region_positions
        ):
            raise CriticalPipelineError("regional products count mismatch")
        rows.extend(region_rows)
        region_counts[region_id] = {
            "pages": expected_region_pages,
            "positions": len(region_rows),
            "duplicate_product_positions": sum(
                count - 1
                for count in product_occurrences.values()
                if count > 1
            ),
            "max_position_capacity": EXPECTED_QUERIES * 1000,
        }

    if len(rows) != totals["products_ok"]:
        raise CriticalPipelineError("four-region position total mismatch")
    seller_rows = deterministic_seller_rows(
        rows,
        region_ids=FOUR_REGION_IDS,
        query_ids=bundle.collection_plan.query_ids,
    )
    unique_suppliers = {
        str(row.get("supplier_id", "")).strip()
        for row in seller_rows
        if str(row.get("supplier_id", "")).strip()
    }
    missing_supplier_products = sum(
        not str(row.get("supplier_id", "")).strip()
        for row in seller_rows
    )

    output_root = (
        config.project_root
        / "data/marts/wb_four_region"
        / FOUR_REGION_PLAN_ID
        / run_id
    )
    bridge_path = output_root / "regional_query_product_position_bridge.csv"
    seller_input_path = output_root / "products_for_sellers.csv"
    write_csv_rows(bridge_path, rows, BRIDGE_FIELDS)
    write_csv_rows(seller_input_path, seller_rows, BRIDGE_FIELDS)
    return FourRegionInputs(
        root=output_root,
        seller_input_path=seller_input_path,
        bridge_path=bridge_path,
        seller_input_sha256=_sha256(seller_input_path),
        bridge_sha256=_sha256(bridge_path),
        positions_count=len(rows),
        unique_products_count=len(seller_rows),
        unique_suppliers_count=len(unique_suppliers),
        missing_supplier_products=missing_supplier_products,
        duplicate_product_positions=sum(
            values["duplicate_product_positions"]
            for values in region_counts.values()
        ),
        region_counts=region_counts,
        collection_lineage=collection_lineage,
    )


def write_four_region_failure_attempt(
    *,
    config: AppConfig,
    run_id: str,
    error: Exception,
    stage: str = "collection",
    lock_ownership: str = "not_acquired",
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    attempt_id: str | None = None,
) -> dict[str, Any] | None:
    try:
        if (
            not _STATE_ID_RE.fullmatch(run_id)
            or stage not in _ATTEMPT_STAGES
            or lock_ownership not in _ATTEMPT_LOCK_OWNERSHIP
        ):
            return None
        created_at = now()
        if created_at.tzinfo is None:
            return None
        effective_attempt_id = attempt_id or (
            created_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            + "-"
            + uuid.uuid4().hex
        )
        if not _STATE_ID_RE.fullmatch(effective_attempt_id):
            return None
        manifest_path = (
            config.project_root
            / "state/wb_collection_plans"
            / FOUR_REGION_PLAN_ID
            / run_id
            / "manifest.json"
        )
        manifest: dict[str, Any] = {}
        if manifest_path.is_file() and not manifest_path.is_symlink():
            try:
                manifest = _json(manifest_path)
            except CriticalPipelineError:
                manifest = {}
        manifest_regions = {
            item.get("region_id"): item
            for item in manifest.get("regions", [])
            if isinstance(item, dict)
            and item.get("region_id") in FOUR_REGION_IDS
        }
        artifact_path = (
            config.project_root
            / "state/wb_four_region_nightly"
            / FOUR_REGION_PLAN_ID
            / "attempts"
            / run_id
            / f"{effective_attempt_id}.json"
        )
        payload = {
            "schema_version": DOWNSTREAM_ATTEMPT_SCHEMA,
            "attempt_id": effective_attempt_id,
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "failed",
            "stage": stage,
            "created_at_utc": created_at.astimezone(UTC)
            .replace(microsecond=0)
            .isoformat(),
            "lock_ownership": lock_ownership,
            "authoritative_state_changed": False,
            "execution_contract": (
                DownstreamExecutionContract.pre_cutover().evidence()
            ),
            "regions": [
                {
                    "region_id": region_id,
                    "status": manifest_regions.get(region_id, {}).get(
                        "status",
                        "not_started",
                    ),
                    "pages": int(
                        manifest_regions.get(region_id, {}).get(
                            "pages_ok",
                            0,
                        )
                    ),
                    "positions": int(
                        manifest_regions.get(region_id, {}).get(
                            "products_ok",
                            0,
                        )
                    ),
                }
                for region_id in FOUR_REGION_IDS
            ],
            "totals": {
                "pages": int(
                    manifest.get("totals", {}).get("pages_ok", 0)
                ),
                "positions": int(
                    manifest.get("totals", {}).get("products_ok", 0)
                ),
                "duplicate_product_positions": int(
                    manifest.get("totals", {}).get(
                        "duplicate_product_positions",
                        0,
                    )
                ),
                "max_position_capacity": MAX_POSITIONS,
            },
            "failure_reason": error.__class__.__name__,
            "artifact_path": artifact_path.relative_to(
                config.project_root
            ).as_posix(),
        }
        _immutable_json(
            artifact_path,
            payload,
            project_root=config.project_root,
        )
        return payload
    except Exception:
        return None


def _input_state(
    inputs: FourRegionInputs | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if inputs is None:
        return [], {
            "pages": 0,
            "positions": 0,
            "unique_products": 0,
            "unique_suppliers": 0,
            "missing_supplier_products": None,
            "duplicate_product_positions": 0,
            "max_position_capacity": MAX_POSITIONS,
        }
    return (
        [
            {
                "region_id": region_id,
                "status": "success",
                **inputs.region_counts[region_id],
            }
            for region_id in FOUR_REGION_IDS
        ],
        {
            "pages": sum(
                values["pages"]
                for values in inputs.region_counts.values()
            ),
            "positions": inputs.positions_count,
            "unique_products": inputs.unique_products_count,
            "unique_suppliers": inputs.unique_suppliers_count,
            "missing_supplier_products": inputs.missing_supplier_products,
            "duplicate_product_positions": (
                inputs.duplicate_product_positions
            ),
            "max_position_capacity": MAX_POSITIONS,
        },
    )


def _seller_failure_state(
    *,
    result: Mapping[str, Any] | None,
    scope: SellersRunScope | None,
    stage: str,
    source_sha256: str | None,
) -> dict[str, Any]:
    if result is not None:
        return {
            "status": str(result.get("status", "failed")),
            "items_ok": int(result.get("items_ok", 0)),
            "items_error": int(result.get("items_error", 0)),
            "processed_sellers": int(
                result.get("processed_sellers", 0)
            ),
            "source_sha256": source_sha256,
        }
    if stage != "sellers" or scope is None:
        return {"status": "not_run"}
    progress: dict[str, Any] = {
        "status": "interrupted",
        "items_ok": 0,
        "items_error": 0,
        "processed_sellers": 0,
        "source_sha256": source_sha256,
    }
    mart_path = scope.mart_dir / "sellers_daily.csv"
    if not mart_path.is_file() or mart_path.is_symlink():
        return progress
    try:
        latest_by_seller: dict[str, str] = {}
        for row in read_csv_rows(mart_path):
            seller_id = str(row.get("supplier_id", "")).strip()
            status = str(row.get("status", "")).strip()
            if seller_id and status in {"success", "error"}:
                latest_by_seller[seller_id] = status
        progress["items_ok"] = sum(
            status == "success"
            for status in latest_by_seller.values()
        )
        progress["items_error"] = sum(
            status == "error"
            for status in latest_by_seller.values()
        )
        progress["processed_sellers"] = len(latest_by_seller)
    except (OSError, UnicodeError, csv.Error):
        progress["progress_status"] = "unavailable"
    return progress


def _warehouse_failure_state(
    *,
    result: Mapping[str, Any] | None,
    stage: str,
) -> dict[str, Any]:
    if result is not None:
        state = {"status": str(result.get("status", "failed"))}
        for field in ("positions_count", "sellers_count"):
            value = result.get(field)
            if type(value) is int and value >= 0:
                state[field] = value
        return state
    if stage == "warehouse":
        return {"status": "failed"}
    return {"status": "not_run"}


def _run_locked_four_region_downstream(
    *,
    config: AppConfig,
    bundle: CollectionPlanBundle,
    paths: ScopedPaths,
    state_dir: Path,
    state_path: Path,
    latest_path: Path,
    run_id: str,
    deadline: DeadlineGuard,
    execution_contract: DownstreamExecutionContract,
    sellers_factory: Callable[..., SellersEngine],
    warehouse_ingest: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    stage = "state_transition"
    try:
        candidate_lineage = _collection_lineage(
            config=config,
            bundle=bundle,
            paths=paths,
            run_id=run_id,
        )
        lease = _begin_authoritative_state_transition(
            state_path=state_path,
            latest_path=latest_path,
            run_id=run_id,
            project_root=config.project_root,
            candidate_lineage=candidate_lineage,
        )
    except Exception as exc:
        write_four_region_failure_attempt(
            config=config,
            run_id=run_id,
            error=exc,
            stage=stage,
            lock_ownership="acquired",
        )
        raise

    if lease.reconcile_only:
        stage = "state_publication"
        try:
            if lease.expected_state_bytes is None:
                raise CriticalPipelineError(
                    "completed downstream state bytes are missing"
                )
            completed_state = json.loads(lease.expected_state_bytes)
            if not isinstance(completed_state, dict):
                raise CriticalPipelineError(
                    "completed downstream state is invalid"
                )
            pointer = {
                "schema_version": DOWNSTREAM_LATEST_SCHEMA,
                "run_id": run_id,
                "state_path": state_path.relative_to(
                    config.project_root
                ).as_posix(),
                "state_sha256": lease.expected_state_sha256,
                "lineage": completed_state["lineage"],
            }
            deadline.ensure_active()
            _write_authoritative_latest(lease, pointer)
            return completed_state
        except Exception as exc:
            write_four_region_failure_attempt(
                config=config,
                run_id=run_id,
                error=exc,
                stage=stage,
                lock_ownership="acquired",
            )
            raise
        finally:
            lease.active = False

    inputs: FourRegionInputs | None = None
    seller_scope: SellersRunScope | None = None
    sellers_result: Mapping[str, Any] | None = None
    warehouse_result: Mapping[str, Any] | None = None
    try:
        stage = "input_build"
        inputs = build_four_region_inputs(
            config=config,
            bundle=bundle,
            run_id=run_id,
        )
        db = StateDB(state_dir / "sellers.sqlite")
        db.init_schema()
        context = RunContext(
            run_id=run_id,
            pipeline="wb_four_region_nightly",
            component="sellers_regional",
            started_at_utc=utc_now_iso(),
        )
        seller_scope = SellersRunScope(
            input_products_path=inputs.seller_input_path,
            raw_dir=config.project_root
            / "data/raw/sellers_scoped"
            / FOUR_REGION_PLAN_ID
            / run_id,
            staging_dir=config.project_root
            / "data/staging/sellers_scoped"
            / FOUR_REGION_PLAN_ID
            / run_id,
            mart_dir=config.project_root
            / "data/marts/sellers_scoped"
            / FOUR_REGION_PLAN_ID
            / run_id,
            checkpoint_component=(
                f"sellers_regional:{FOUR_REGION_PLAN_ID}"
            ),
            request_timeout_provider=deadline.request_timeout,
        )
        stage = "sellers"
        sellers_result = sellers_factory(
            config=config,
            db=db,
            ctx=context,
            run_scope=seller_scope,
        ).run()
        if (
            sellers_result.get("status") != "success"
            or int(sellers_result.get("items_error", 0)) != 0
        ):
            raise CriticalPipelineError(
                "regional sellers stage is partial"
            )
        seller_output_path = Path(
            str(sellers_result["mart_sellers_path"])
        )
        try:
            seller_output_relative = seller_output_path.relative_to(
                config.project_root
            ).as_posix()
        except ValueError as exc:
            raise CriticalPipelineError(
                "regional seller output escapes project root"
            ) from exc
        seller_output_path = _safe_project_file(
            config.project_root,
            seller_output_relative,
            suffix=".csv",
        )
        seller_output_sha256 = _sha256(seller_output_path)
        deadline.ensure_active()
        stage = "warehouse"
        warehouse_result = warehouse_ingest(
            project_root=config.project_root,
            run_id=run_id,
            collection_plan_id=FOUR_REGION_PLAN_ID,
            bridge_path=inputs.bridge_path,
            sellers_path=Path(
                str(sellers_result["mart_sellers_path"])
            ),
            collection_manifest_path=paths.manifest_path,
        )
        if warehouse_result.get("status") not in {
            "success",
            "already_ingested",
        }:
            raise CriticalPipelineError(
                "regional warehouse stage failed"
            )
        deadline.ensure_active()
        stage = "state_publication"
        state = {
            "schema_version": DOWNSTREAM_SCHEMA,
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "success",
            "complete": True,
            "stage": "complete",
            "execution_contract": execution_contract.evidence(),
            "finished_at_utc": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat(),
            "lineage": dict(inputs.collection_lineage),
            "regions": [
                {
                    "region_id": region_id,
                    **inputs.region_counts[region_id],
                }
                for region_id in FOUR_REGION_IDS
            ],
            "totals": {
                "pages": sum(
                    values["pages"]
                    for values in inputs.region_counts.values()
                ),
                "positions": inputs.positions_count,
                "unique_products": inputs.unique_products_count,
                "unique_suppliers": inputs.unique_suppliers_count,
                "missing_supplier_products": (
                    inputs.missing_supplier_products
                ),
                "duplicate_product_positions": (
                    inputs.duplicate_product_positions
                ),
                "max_position_capacity": MAX_POSITIONS,
            },
            "sellers": {
                "status": "success",
                "items_ok": int(sellers_result["items_ok"]),
                "items_error": int(sellers_result["items_error"]),
                "source_sha256": inputs.seller_input_sha256,
                "output_path": seller_output_relative,
                "output_sha256": seller_output_sha256,
            },
            "warehouse": {
                "status": warehouse_result["status"],
                "positions_count": int(
                    warehouse_result["positions_count"]
                ),
                "sellers_count": int(
                    warehouse_result["sellers_count"]
                ),
                "legacy_yaroslavl": warehouse_result["legacy"],
                "ingestion_evidence": {
                    "collection_manifest_sha256": (
                        inputs.collection_lineage[
                            "collection_manifest_sha256"
                        ]
                    ),
                    "bridge_sha256": inputs.bridge_sha256,
                    "sellers_sha256": seller_output_sha256,
                },
            },
            "failure_reason": None,
            "artifacts": {
                "bridge_path": inputs.bridge_path.relative_to(
                    config.project_root
                ).as_posix(),
                "bridge_sha256": inputs.bridge_sha256,
                "seller_input_path": (
                    inputs.seller_input_path.relative_to(
                        config.project_root
                    ).as_posix()
                ),
                "seller_input_sha256": inputs.seller_input_sha256,
                "seller_output_path": seller_output_relative,
                "seller_output_sha256": seller_output_sha256,
            },
        }
        _write_authoritative_state(lease, state)
        pointer = {
            "schema_version": DOWNSTREAM_LATEST_SCHEMA,
            "run_id": run_id,
            "state_path": state_path.relative_to(
                config.project_root
            ).as_posix(),
            "state_sha256": lease.expected_state_sha256,
            "lineage": state["lineage"],
        }
        _write_authoritative_latest(lease, pointer)
        return state
    except Exception as exc:
        if not lease.state_written:
            regions, totals = _input_state(inputs)
            failure = {
                "schema_version": DOWNSTREAM_SCHEMA,
                "run_id": run_id,
                "collection_plan_id": FOUR_REGION_PLAN_ID,
                "status": "failed",
                "complete": False,
                "stage": stage,
                "execution_contract": execution_contract.evidence(),
                "finished_at_utc": datetime.now(UTC)
                .replace(microsecond=0)
                .isoformat(),
                "regions": regions,
                "totals": totals,
                "sellers": _seller_failure_state(
                    result=sellers_result,
                    scope=seller_scope,
                    stage=stage,
                    source_sha256=(
                        inputs.seller_input_sha256
                        if inputs is not None
                        else None
                    ),
                ),
                "warehouse": _warehouse_failure_state(
                    result=warehouse_result,
                    stage=stage,
                ),
                "failure_reason": exc.__class__.__name__,
            }
            try:
                _write_authoritative_state(lease, failure)
            except Exception:
                pass
        write_four_region_failure_attempt(
            config=config,
            run_id=run_id,
            error=exc,
            stage=stage,
            lock_ownership="acquired",
        )
        raise
    finally:
        lease.active = False


def run_four_region_downstream(
    *,
    config: AppConfig,
    plan_path: Path,
    run_id: str,
    sellers_factory: Callable[..., SellersEngine] = SellersEngine,
    warehouse_ingest: Callable[..., dict[str, Any]] = ingest_regional_run,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    execution_mode: str = PRE_CUTOVER_DOWNSTREAM_MODE,
) -> dict[str, Any]:
    paths = ScopedPaths.build(
        project_root=config.project_root,
        collection_plan_id=FOUR_REGION_PLAN_ID,
        run_id=run_id,
    )
    state_dir = (
        config.project_root
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / run_id
    )
    state_path = state_dir / "state.json"
    latest_path = state_dir.parent / "latest.json"
    execution_contract = DownstreamExecutionContract.pre_cutover()
    stage = "preflight"
    locks_owned = False
    try:
        bundle = load_collection_plan_bundle(
            project_root=config.project_root,
            plan_path=plan_path,
            region_registry_path=config.project_root / "config/wb/regions.json",
        )
        validate_four_region_bundle(bundle)
        runtime_window = bundle.collection_plan.runtime_window
        if runtime_window is None:
            raise CriticalPipelineError(
                "four-region runtime window is missing"
            )
        if execution_mode != execution_contract.mode:
            raise CriticalPipelineError(
                "downstream execution mode is not approved"
            )
        execution_contract.ensure_start_allowed(now())
        deadline = DeadlineGuard.for_runtime_window(
            runtime_window,
            resume=True,
            now=now,
        )
        stage = "lock_acquisition"
        with acquire_collection_plan_locks(
            paths=paths,
            stale_seconds=config.runtime.lock_stale_seconds,
        ):
            locks_owned = True
            return _run_locked_four_region_downstream(
                config=config,
                bundle=bundle,
                paths=paths,
                state_dir=state_dir,
                state_path=state_path,
                latest_path=latest_path,
                run_id=run_id,
                deadline=deadline,
                execution_contract=execution_contract,
                sellers_factory=sellers_factory,
                warehouse_ingest=warehouse_ingest,
            )
    except Exception as exc:
        if not locks_owned:
            write_four_region_failure_attempt(
                config=config,
                run_id=run_id,
                error=exc,
                stage=stage,
                lock_ownership="not_acquired",
            )
        raise
