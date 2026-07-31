from __future__ import annotations

import csv
import hashlib
import io
import ipaddress
import json
import os
import re
import secrets
import time as time_module
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol
from urllib.parse import parse_qs, quote, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import requests

from app.common.config import AppConfig
from app.common.durable_atomic import (
    DurableAtomicWriteError,
    durable_atomic_replace,
)
from app.common.exceptions import CriticalPipelineError
from app.common.proxy_required import (
    assert_requests_session_proxy,
    build_requests_session,
    require_marketplace_proxy,
)
from app.common.run_lock import acquire_advisory_lock, acquire_run_lock
from app.serp.collection_plan import (
    CollectionPlanBundle,
    CollectionRuntimeWindow,
    CollectionPlanValidationError,
    EffectiveEndpointPolicy,
    RegionDefinition,
    ResolvedDestination,
    build_effective_plan_snapshot,
    canonical_effective_plan_bytes,
    canonical_effective_plan_sha256,
    load_collection_plan_bundle,
    register_query_pack_provenance,
)


MANIFEST_SCHEMA_VERSION = "wb_collection_plan_manifest_v1"
RESUMABLE_MANIFEST_SCHEMA_VERSION = "wb_collection_plan_manifest_v2"
REGION_STATE_SCHEMA_VERSION = "wb_collection_plan_region_v1"
CHECKPOINT_SCHEMA_VERSION = "wb_collection_plan_checkpoint_v1"
RESUMABLE_CHECKPOINT_SCHEMA_VERSION = "wb_collection_plan_checkpoint_v2"
SEGMENT_SCHEMA_VERSION = "wb_collection_plan_segment_v1"
BOUNDED_CHECKPOINT_SCHEMA_VERSION = "wb_collection_plan_checkpoint_v3"
BOUNDED_SEGMENT_SCHEMA_VERSION = "wb_collection_plan_segment_v2"
REGIONAL_LATEST_SCHEMA_VERSION = "wb_regional_latest_v1"
REGIONAL_LATEST_REGION_SCHEMA_VERSION = "wb_regional_latest_region_v1"
COLLECTION_SCOPE = "regional"
GEO_RESOLVER_URL = "https://user-geo-data.wildberries.ru/get-geo-info"
EGRESS_CHECK_URL = "https://api.ipify.org"
EGRESS_FALLBACK_TIMEOUT_SECONDS = 5.0
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
NIGHTLY_PREFLIGHT_CUTOFF = time(23, 45)
NIGHTLY_COLLECTION_START = time(0, 15)
MINIMUM_START_WINDOW_SECONDS = 300
FINALIZATION_RESERVE_SECONDS = 5
NIGHTLY_SAFETY_RESERVE_SECONDS = 900
ESTIMATED_REQUEST_OVERHEAD_SECONDS = 1.0
RESUME_ATTESTATION_TRANSITION_SCHEMA_VERSION = (
    "wb_resume_attestation_transition_v1"
)
_RESUME_ATTESTATION_MANIFEST_RELATIVE = Path(
    "config/wb/nightly_coordinator_adapter_inputs.json"
)
_RESUME_ATTESTATION_RUNNER_RELATIVE = Path(
    "app/serp/collection_plan_runner.py"
)

# Run-scoped repair exceptions are retired once their immutable collection
# manifest is complete. Any future attestation drift therefore fails closed.
_APPROVED_RESUME_ATTESTATION_TRANSITIONS: dict[
    tuple[str, str, str, str, str], dict[str, str]
] = {}

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RUN_ID_RE = re.compile(r"^[0-9]{8}_[0-9]{6}Z$")
_DEST_RE = re.compile(r"^[+-]?[0-9]{1,16}$")
_PRODUCT_ID_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MASKED_IPV4_RE = re.compile(
    r"^(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\."
    r"(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.x\.x$"
)
_MASKED_IPV6_RE = re.compile(r"^[0-9a-f]{4}:[0-9a-f]{4}::x$")
_TRANSPORT_IDENTITY_RE = re.compile(
    r"^transport:[A-Za-z0-9][A-Za-z0-9 ._()-]{0,79}$"
)
_MASKED_TRANSPORT_RE = re.compile(r"^transport:x$")

LockEventHook = Callable[[str, str, Path], None]
WriteEventHook = Callable[[str, Path], None]


class CollectionPlanRunError(CriticalPipelineError):
    pass


class EgressIdentityChangedError(CollectionPlanRunError):
    pass


class ScopedTransportError(CollectionPlanRunError):
    def __init__(
        self,
        code: str,
        *,
        request_sent: bool = False,
        dest_id_sent: str = "",
        http_status: int | None = None,
        retry_after_status: str | None = None,
        retry_after_seconds: int | None = None,
        endpoint_id: str = "",
        attempted_endpoint_ids: tuple[str, ...] = (),
    ) -> None:
        super().__init__(code)
        self.code = code
        self.request_sent = request_sent
        self.dest_id_sent = dest_id_sent
        self.http_status = http_status
        self.retry_after_status = retry_after_status
        self.retry_after_seconds = retry_after_seconds
        self.endpoint_id = endpoint_id
        self.attempted_endpoint_ids = attempted_endpoint_ids


@dataclass(frozen=True, slots=True)
class ScopedTask:
    collection_plan_id: str
    query_pack_id: str
    query_pack_version: str
    query_id: str
    category_id: str
    query: str
    query_group: str
    region_id: str
    region_name: str
    page: int
    page_size: int
    depth: int

    @property
    def checkpoint_key(self) -> str:
        return "|".join(
            (
                self.collection_plan_id,
                self.query_pack_version,
                self.region_id,
                self.query_id,
                str(self.page),
            )
        )


@dataclass(frozen=True, slots=True)
class ScopedSearchRequest:
    task: ScopedTask
    dest_id_observed: str
    endpoint_id: str
    params: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class ScopedSearchResult:
    payload: Mapping[str, Any]
    endpoint_id: str
    dest_id_sent: str
    http_status: int = 200
    attempted_endpoint_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EndpointProbeResult:
    endpoint_id: str
    suitable: bool
    http_status: int | None
    error_code: str | None
    reusable_request: ScopedSearchRequest | None = None
    reusable_result: ScopedSearchResult | None = None
    retry_after_status: str | None = None
    retry_after_seconds: int | None = None


def parse_retry_after_delta(value: Any) -> tuple[str, int | None]:
    if value is None:
        return "missing", None
    if type(value) is not str or not re.fullmatch(r"[0-9]{1,3}", value):
        return "invalid", None
    seconds = int(value)
    if not 1 <= seconds <= 120:
        return "out_of_range", None
    return "valid", seconds


class ScopedTransport(Protocol):
    request_params: Mapping[str, Any]
    endpoint_policy: EffectiveEndpointPolicy

    def egress_identity(self, *, timeout_seconds: float) -> str: ...

    def resolve_destination(
        self,
        region: RegionDefinition,
        *,
        timeout_seconds: float,
    ) -> str: ...

    def search(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult: ...

    def search_ordered(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult: ...

    def probe_endpoint(
        self,
        request: ScopedSearchRequest,
        *,
        endpoint_id: str,
        timeout_seconds: float,
    ) -> EndpointProbeResult: ...

    def pin_endpoint(self, endpoint_id: str) -> None: ...

    def close(self) -> None: ...


def _safe_id(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise CollectionPlanRunError(f"{field} is not a safe ID")
    return value


def _safe_run_id(value: str) -> str:
    if not isinstance(value, str) or not _RUN_ID_RE.fullmatch(value):
        raise CollectionPlanRunError("run_id must use YYYYMMDD_HHMMSSZ")
    return value


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise CollectionPlanRunError("clock must return timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def _default_run_id(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


@dataclass(frozen=True, slots=True)
class ScopedPaths:
    project_root: Path
    collection_plan_id: str
    run_id: str

    @classmethod
    def build(
        cls,
        *,
        project_root: Path,
        collection_plan_id: str,
        run_id: str,
    ) -> "ScopedPaths":
        return cls(
            project_root=project_root.resolve(),
            collection_plan_id=_safe_id(
                collection_plan_id,
                field="collection_plan_id",
            ),
            run_id=_safe_run_id(run_id),
        )

    @property
    def state_run_dir(self) -> Path:
        return (
            self.project_root
            / "state/wb_collection_plans"
            / self.collection_plan_id
            / self.run_id
        )

    @property
    def effective_plan_path(self) -> Path:
        return self.state_run_dir / "effective_plan.json"

    @property
    def manifest_path(self) -> Path:
        return self.state_run_dir / "manifest.json"

    @property
    def segment_dir(self) -> Path:
        return self.state_run_dir / "segments"

    def segment_path(self, segment_id: str) -> Path:
        return self.segment_dir / f"{_safe_id(segment_id, field='segment_id')}.json"

    def segment_pending_raw_path(self, segment_id: str, task: ScopedTask) -> Path:
        return (
            self.layer_region_run_dir("raw", task.region_id)
            / "pending_segments"
            / _safe_id(segment_id, field="segment_id")
            / task.query_id
            / f"page_{task.page:03d}.json"
        )

    def segment_pending_checkpoint_path(
        self,
        segment_id: str,
        task: ScopedTask,
    ) -> Path:
        return (
            self.state_run_dir
            / "pending_segments"
            / _safe_id(segment_id, field="segment_id")
            / task.region_id
            / task.query_id
            / f"page_{task.page:03d}.json"
        )

    @property
    def plan_state_dir(self) -> Path:
        return (
            self.project_root
            / "state/wb_collection_plans"
            / self.collection_plan_id
        )

    @property
    def latest_path(self) -> Path:
        return self.plan_state_dir / "latest.json"

    def latest_region_manifest_path(self, region_id: str) -> Path:
        return (
            self.plan_state_dir
            / "latest_generations"
            / self.run_id
            / f"{_safe_id(region_id, field='region_id')}.json"
        )

    @property
    def provenance_path(self) -> Path:
        return (
            self.project_root
            / "state/wb_collection_plans/provenance/query_pack_versions.json"
        )

    @property
    def lock_paths(self) -> tuple[Path, Path, Path, Path]:
        lock_dir = self.project_root / "state/locks"
        return (
            lock_dir / "products_sellers_daily.flock",
            lock_dir / "pipeline.lock",
            lock_dir / "wb_warehouse_refresh.flock",
            lock_dir / "wb_collection_plan.flock",
        )

    def region_state_path(self, region_id: str) -> Path:
        return self.state_run_dir / "regions" / f"{_safe_id(region_id, field='region_id')}.json"

    def checkpoint_path(self, task: ScopedTask) -> Path:
        return (
            self.state_run_dir
            / "checkpoints"
            / task.region_id
            / task.query_id
            / f"page_{task.page:03d}.json"
        )

    def layer_region_run_dir(self, layer: str, region_id: str) -> Path:
        if layer not in {"raw", "staging", "marts"}:
            raise CollectionPlanRunError(f"unsupported scoped layer: {layer}")
        return (
            self.project_root
            / "data"
            / layer
            / "serp_scoped"
            / self.collection_plan_id
            / _safe_id(region_id, field="region_id")
            / self.run_id
        )

    def raw_page_path(self, task: ScopedTask) -> Path:
        return (
            self.layer_region_run_dir("raw", task.region_id)
            / "pages"
            / task.query_id
            / f"page_{task.page:03d}.json"
        )


@dataclass(slots=True)
class DeadlineGuard:
    deadline_utc: datetime
    now: Callable[[], datetime]
    finalization_reserve_seconds: int = FINALIZATION_RESERVE_SECONDS
    enforce_deadline_estimate: bool = False

    @classmethod
    def for_current_day(
        cls,
        *,
        now: Callable[[], datetime] = _default_now,
    ) -> "DeadlineGuard":
        current = now()
        if current.tzinfo is None:
            raise CollectionPlanRunError("clock must return timezone-aware datetime")
        current_msk = current.astimezone(MOSCOW_TZ)
        cutoff_msk = datetime.combine(
            current_msk.date(),
            NIGHTLY_PREFLIGHT_CUTOFF,
            tzinfo=MOSCOW_TZ,
        )
        guard = cls(deadline_utc=cutoff_msk.astimezone(timezone.utc), now=now)
        guard.ensure_start_window()
        return guard

    @classmethod
    def for_runtime_window(
        cls,
        window: CollectionRuntimeWindow,
        *,
        resume: bool,
        now: Callable[[], datetime] = _default_now,
        absolute_deadline_utc: datetime | None = None,
    ) -> "DeadlineGuard":
        current = now()
        if current.tzinfo is None:
            raise CollectionPlanRunError("clock must return timezone-aware datetime")
        current_msk = current.astimezone(MOSCOW_TZ)

        def parse_hhmm(value: str) -> time:
            hour, minute = (int(item) for item in value.split(":", 1))
            return time(hour, minute)

        scheduled = datetime.combine(
            current_msk.date(),
            parse_hhmm(window.scheduled_start_msk),
            tzinfo=MOSCOW_TZ,
        )
        cutoff = datetime.combine(
            current_msk.date(),
            parse_hhmm(window.absolute_cutoff_msk),
            tzinfo=MOSCOW_TZ,
        )
        if not resume:
            latest_new_start = scheduled + timedelta(
                seconds=window.new_run_start_grace_seconds
            )
            if current_msk < scheduled or current_msk > latest_new_start:
                raise CollectionPlanRunError(
                    "new bounded collection run is outside its reviewed start window"
                )
        invocation_deadline = min(
            current_msk
            + timedelta(seconds=window.max_invocation_runtime_seconds),
            cutoff,
        )
        if absolute_deadline_utc is not None:
            if (
                absolute_deadline_utc.tzinfo is None
                or absolute_deadline_utc.utcoffset()
                != timezone.utc.utcoffset(absolute_deadline_utc)
            ):
                raise CollectionPlanRunError(
                    "coordinator absolute deadline must be UTC"
                )
            invocation_deadline = min(
                invocation_deadline,
                absolute_deadline_utc.astimezone(MOSCOW_TZ),
            )
        guard = cls(
            deadline_utc=invocation_deadline.astimezone(timezone.utc),
            now=now,
            finalization_reserve_seconds=window.finalization_reserve_seconds,
            enforce_deadline_estimate=True,
        )
        minimum_window = (
            window.minimum_resume_window_seconds
            if resume
            else window.finalization_reserve_seconds + 1
        )
        if guard.remaining_seconds() < minimum_window:
            raise CollectionPlanRunError(
                "bounded collection invocation has insufficient time before cutoff"
            )
        return guard

    def remaining_seconds(self) -> float:
        return (self.deadline_utc - self.now().astimezone(timezone.utc)).total_seconds()

    def ensure_start_window(self) -> None:
        if self.remaining_seconds() < MINIMUM_START_WINDOW_SECONDS:
            raise CollectionPlanRunError(
                "collection plan cannot start within 5 minutes of 23:45 MSK"
            )

    def ensure_active(self) -> None:
        if self.remaining_seconds() <= self.finalization_reserve_seconds:
            raise CollectionPlanRunError(
                "collection plan runtime deadline reached"
            )

    def request_timeout(self, configured_timeout: float) -> float:
        self.ensure_active()
        available = self.remaining_seconds() - self.finalization_reserve_seconds
        return max(0.1, min(float(configured_timeout), available))

    def ensure_estimated_window(self, estimated_seconds: float) -> None:
        if estimated_seconds < 0:
            raise CollectionPlanRunError("estimated runtime must not be negative")
        current_utc = self.now().astimezone(timezone.utc)
        if (
            self.enforce_deadline_estimate
            and current_utc + timedelta(seconds=estimated_seconds)
            > self.deadline_utc
        ):
            raise CollectionPlanRunError(
                "estimated safe work unit exceeds collection runtime deadline"
            )
        current = self.now().astimezone(MOSCOW_TZ)
        nightly = datetime.combine(
            current.date(),
            NIGHTLY_COLLECTION_START,
            tzinfo=MOSCOW_TZ,
        )
        if nightly <= current:
            nightly += timedelta(days=1)
        latest_finish = nightly - timedelta(seconds=NIGHTLY_SAFETY_RESERVE_SECONDS)
        if current + timedelta(seconds=estimated_seconds) > latest_finish:
            raise CollectionPlanRunError(
                "estimated collection window overlaps nightly 00:15 MSK"
            )


@contextmanager
def acquire_collection_plan_locks(
    *,
    paths: ScopedPaths,
    stale_seconds: int,
    event_hook: LockEventHook | None = None,
) -> Iterator[None]:
    names = ("daily", "pipeline", "warehouse", "collection_plan")
    daily_path, _pipeline_path, warehouse_path, plan_path = paths.lock_paths

    def emit(event: str, name: str, path: Path) -> None:
        if event_hook is not None:
            event_hook(event, name, path)

    @contextmanager
    def tracked(
        name: str,
        path: Path,
        manager: Any,
    ) -> Iterator[Path | None]:
        emit("before_acquire", name, path)
        with manager as acquired_path:
            emit("acquired", name, acquired_path or path)
            try:
                yield acquired_path
            finally:
                emit("before_release", name, acquired_path or path)

    with ExitStack() as stack:
        stack.enter_context(
            tracked(
                names[0],
                daily_path,
                acquire_advisory_lock(daily_path),
            )
        )
        stack.enter_context(
            tracked(
                names[1],
                paths.lock_paths[1],
                acquire_run_lock(
                state_dir=paths.project_root / "state",
                target="collection-plan",
                run_id=paths.run_id,
                enabled=True,
                stale_seconds=stale_seconds,
                guard_blocking=False,
                ),
            )
        )
        stack.enter_context(
            tracked(
                names[2],
                warehouse_path,
                acquire_advisory_lock(warehouse_path),
            )
        )
        stack.enter_context(
            tracked(
                names[3],
                plan_path,
                acquire_advisory_lock(plan_path),
            )
        )
        yield


def _ensure_scoped_parent(
    path: Path,
    *,
    project_root: Path,
    event_hook: WriteEventHook | None = None,
) -> None:
    root = project_root.resolve()
    try:
        relative = path.parent.relative_to(root)
    except ValueError as exc:
        raise CollectionPlanRunError(f"scoped path escapes project root: {path}") from exc

    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise CollectionPlanRunError(f"scoped path uses symlink: {current}")
        if current.exists():
            if not current.is_dir():
                raise CollectionPlanRunError(
                    f"scoped parent is not a directory: {current}"
                )
            continue
        parent = current.parent
        current.mkdir()
        for directory in (parent, current):
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        if event_hook is not None:
            event_hook("parent_entry_fsynced", current)


def _write_new_bytes(
    path: Path,
    payload: bytes,
    *,
    project_root: Path,
    event_hook: WriteEventHook | None = None,
) -> None:
    _ensure_scoped_parent(
        path,
        project_root=project_root,
        event_hook=event_hook,
    )
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = -1
    try:
        fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
        if event_hook is not None:
            event_hook("file_fsynced", path)
        os.close(fd)
        fd = -1
        os.link(temp_path, path)
        temp_path.unlink()
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        if event_hook is not None:
            event_hook("directory_fsynced", path)
    except FileExistsError as exc:
        raise CollectionPlanRunError(f"immutable file already exists: {path}") from exc
    finally:
        if fd >= 0:
            os.close(fd)
        temp_path.unlink(missing_ok=True)


def _write_atomic_bytes(
    path: Path,
    payload: bytes,
    *,
    project_root: Path,
    event_hook: WriteEventHook | None = None,
    integrity_gate: Callable[[], None] | None = None,
    require_absent: bool = False,
) -> None:
    _ensure_scoped_parent(
        path,
        project_root=project_root,
        event_hook=event_hook,
    )
    try:
        durable_atomic_replace(
            path,
            payload,
            mode=0o600,
            require_absent=require_absent,
            integrity_gate=integrity_gate,
            event_hook=event_hook,
        )
    except DurableAtomicWriteError as exc:
        raise CollectionPlanRunError(
            f"durable atomic write failed: {path}"
        ) from exc


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_sha256(value: Any) -> str:
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CollectionPlanRunError(
            "transport fingerprint input is not canonical JSON"
        ) from exc
    return _sha256_bytes(payload)


def _validated_transport_fingerprint(value: Any) -> dict[str, str]:
    base_fields = frozenset(
        {
            "schema_version",
            "ordered_endpoint_urls_sha256",
            "request_params_sha256",
            "proxy_route_sha256",
            "fingerprint_sha256",
        }
    )
    attested_fields = base_fields | {
        "input_manifest_sha256",
        "runtime_input_sha256",
    }
    if (
        not isinstance(value, dict)
        or frozenset(value) not in {base_fields, attested_fields}
    ):
        raise CollectionPlanRunError(
            "transport fingerprint contract is invalid"
        )
    normalized = dict(value)
    if normalized.get("schema_version") != "wb_transport_fingerprint_v1":
        raise CollectionPlanRunError(
            "transport fingerprint schema is invalid"
        )
    for field in set(normalized) - {"schema_version"}:
        item = normalized.get(field)
        if not isinstance(item, str) or not _SHA256_RE.fullmatch(item):
            raise CollectionPlanRunError(
                "transport fingerprint hash is invalid"
            )
    claimed = normalized.pop("fingerprint_sha256")
    if claimed != _canonical_sha256(normalized):
        raise CollectionPlanRunError(
            "transport fingerprint digest is invalid"
        )
    normalized["fingerprint_sha256"] = claimed
    return normalized


def _single_projection_substitution(
    payload: bytes,
    *,
    pattern: bytes,
    field: str,
) -> bytes:
    projected, substitutions = re.subn(
        pattern,
        rb"\g<1>"
        + (b"0" * 64)
        + rb"\g<2>",
        payload,
    )
    if substitutions != 1:
        raise CollectionPlanRunError(
            f"{field} projection contract is invalid"
        )
    return projected


def _target_manifest_projection(payload: bytes) -> str:
    projected = _single_projection_substitution(
        payload,
        pattern=(
            rb'("app/serp/collection_plan_runner\.py": ")'
            rb"[0-9a-f]{64}"
            rb'(")'
        ),
        field="resume target manifest",
    )
    return _sha256_bytes(projected)


def _target_runner_projection(payload: bytes) -> str:
    projected = payload
    substitution_counts: list[int] = []
    for field in (
        b"target_manifest_projection_sha256",
        b"target_runner_projection_sha256",
    ):
        projected, substitutions = re.subn(
            (
                rb'("' + field + rb'": \(\s*")'
                rb"[0-9a-f]{64}"
                rb'("\s*\))'
            ),
            rb"\g<1>" + (b"0" * 64) + rb"\g<2>",
            projected,
        )
        substitution_counts.append(substitutions)
    if substitution_counts not in ([0, 0], [1, 1]):
        raise CollectionPlanRunError(
            "resume target runner projection contract is invalid"
        )
    return _sha256_bytes(projected)


def _validate_approved_resume_target(
    *,
    project_root: Path,
    current_fingerprint: dict[str, str],
    approval: Mapping[str, str],
) -> None:
    if set(approval) != {
        "transition_id",
        "target_manifest_projection_sha256",
        "target_runner_projection_sha256",
    }:
        raise CollectionPlanRunError(
            "resume input attestation approval is invalid"
        )
    for field in (
        "target_manifest_projection_sha256",
        "target_runner_projection_sha256",
    ):
        if not _SHA256_RE.fullmatch(approval.get(field, "")):
            raise CollectionPlanRunError(
                "resume input attestation approval is invalid"
            )
    manifest_payload = _read_regular_bytes(
        project_root / _RESUME_ATTESTATION_MANIFEST_RELATIVE,
        project_root=project_root,
    )
    runner_payload = _read_regular_bytes(
        project_root / _RESUME_ATTESTATION_RUNNER_RELATIVE,
        project_root=project_root,
    )
    if (
        _sha256_bytes(manifest_payload)
        != current_fingerprint["input_manifest_sha256"]
    ):
        raise CollectionPlanRunError(
            "resume target input manifest digest is invalid"
        )
    manifest = _json_object_from_bytes(
        manifest_payload,
        field="resume target input manifest",
    )
    file_sha256 = manifest.get("file_sha256")
    if (
        not isinstance(file_sha256, dict)
        or file_sha256.get(
            _RESUME_ATTESTATION_RUNNER_RELATIVE.as_posix()
        )
        != _sha256_bytes(runner_payload)
    ):
        raise CollectionPlanRunError(
            "resume target runner attestation is invalid"
        )
    if (
        _target_manifest_projection(manifest_payload)
        != approval["target_manifest_projection_sha256"]
        or _target_runner_projection(runner_payload)
        != approval["target_runner_projection_sha256"]
    ):
        raise CollectionPlanRunError(
            "resume target input attestation is not approved"
        )


def _resume_attestation_transition(
    *,
    stored: Any,
    current: Any,
    project_root: Path,
    run_id: str,
    collection_plan_id: str,
    effective_plan_sha256: str,
) -> dict[str, str] | None:
    stored_fingerprint = _validated_transport_fingerprint(stored)
    current_fingerprint = _validated_transport_fingerprint(current)
    if stored_fingerprint == current_fingerprint:
        return None
    required_fields = {
        "schema_version",
        "ordered_endpoint_urls_sha256",
        "request_params_sha256",
        "proxy_route_sha256",
        "input_manifest_sha256",
        "runtime_input_sha256",
        "fingerprint_sha256",
    }
    if (
        set(stored_fingerprint) != required_fields
        or set(current_fingerprint) != required_fields
    ):
        raise CollectionPlanRunError(
            "resume transport fingerprint mismatch"
        )
    exact_fields = {
        "schema_version",
        "ordered_endpoint_urls_sha256",
        "request_params_sha256",
        "proxy_route_sha256",
        "runtime_input_sha256",
    }
    if any(
        stored_fingerprint[field] != current_fingerprint[field]
        for field in exact_fields
    ):
        raise CollectionPlanRunError(
            "resume transport fingerprint mismatch"
        )
    approval = _APPROVED_RESUME_ATTESTATION_TRANSITIONS.get(
        (
            run_id,
            collection_plan_id,
            effective_plan_sha256,
            stored_fingerprint["fingerprint_sha256"],
            stored_fingerprint["input_manifest_sha256"],
        )
    )
    if approval is None:
        raise CollectionPlanRunError(
            "resume input attestation transition is not approved"
        )
    _validate_approved_resume_target(
        project_root=project_root,
        current_fingerprint=current_fingerprint,
        approval=approval,
    )
    return {
        "schema_version": RESUME_ATTESTATION_TRANSITION_SCHEMA_VERSION,
        "transition_id": approval["transition_id"],
        "from_input_manifest_sha256": stored_fingerprint[
            "input_manifest_sha256"
        ],
        "to_input_manifest_sha256": current_fingerprint[
            "input_manifest_sha256"
        ],
        "from_transport_fingerprint_sha256": stored_fingerprint[
            "fingerprint_sha256"
        ],
        "to_transport_fingerprint_sha256": current_fingerprint[
            "fingerprint_sha256"
        ],
    }


def _resume_attestation_history(
    *,
    value: Any,
    expected: tuple[dict[str, str] | None, ...],
    allow_initial_missing: bool,
) -> list[dict[str, str]]:
    unique = {
        _canonical_sha256(item): item
        for item in expected
        if item is not None
    }
    if len(unique) > 1:
        raise CollectionPlanRunError(
            "resume input attestation evidence conflicts"
    )
    expected_history = [dict(item) for item in unique.values()]
    if value is None:
        if not expected_history or allow_initial_missing:
            return expected_history
        raise CollectionPlanRunError(
            "resume input attestation history is missing"
        )
    if value == [] and allow_initial_missing:
        return expected_history
    if not isinstance(value, list) or value != expected_history:
        raise CollectionPlanRunError(
            "resume input attestation history is invalid"
        )
    return [dict(item) for item in value]


def _read_regular_bytes(path: Path, *, project_root: Path) -> bytes:
    root = project_root.resolve()
    candidate = Path(os.path.abspath(path))
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise CollectionPlanRunError(f"scoped read escapes project root: {path}") from exc
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise CollectionPlanRunError(f"scoped read uses symlink: {current}")
    if not candidate.is_file():
        raise CollectionPlanRunError(f"scoped artifact is not a regular file: {path}")
    return candidate.read_bytes()


def _json_object_from_bytes(payload: bytes, *, field: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CollectionPlanRunError(f"{field} contains duplicate key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CollectionPlanRunError(f"{field} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise CollectionPlanRunError(f"{field} must be a JSON object")
    return value


def _csv_bytes(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


class RequestsScopedTransport:
    def __init__(
        self,
        *,
        session: requests.Session,
        request_params: Mapping[str, Any],
        endpoint_urls: tuple[str, ...],
        timeout_seconds: float,
        referer_base: str,
        request_headers: Mapping[str, str] | None = None,
        resolver_url: str = GEO_RESOLVER_URL,
        egress_check_url: str = EGRESS_CHECK_URL,
        egress_session: requests.Session | None = None,
        egress_fallback_url: str | None = None,
        egress_fallback_session: requests.Session | None = None,
        retry_http_statuses: frozenset[int] | None = None,
    ) -> None:
        if not endpoint_urls:
            raise CollectionPlanRunError("SERP endpoint list is empty")
        self.session = session
        self.request_params = dict(request_params)
        self.endpoint_urls = endpoint_urls
        self.timeout_seconds = float(timeout_seconds)
        self.referer_base = referer_base
        self.request_headers = dict(request_headers or {})
        self.resolver_url = resolver_url
        self.egress_check_url = egress_check_url
        self.egress_session = egress_session or session
        self.egress_fallback_url = egress_fallback_url
        self.egress_fallback_session = egress_fallback_session
        self._egress_fallback_active = False
        self.retry_http_statuses = retry_http_statuses or frozenset(
            {429, 498, 500, 502, 503, 504}
        )
        self.proxy_route_sha256 = getattr(
            session,
            "_wb_marketplace_proxy_sha256",
            None,
        )
        endpoint_ids = tuple(
            "primary" if index == 0 else f"fallback-{index}"
            for index in range(len(endpoint_urls))
        )
        self.endpoint_policy = EffectiveEndpointPolicy(
            selection_mode="ordered_fallbacks",
            endpoint_ids=endpoint_ids,
            pinned_endpoint_id=endpoint_ids[0],
        )
        self._endpoint_pin_finalized = False
        self._network_timeout_provider: Callable[[float], float] | None = None

    def set_network_timeout_provider(
        self,
        provider: Callable[[float], float],
    ) -> None:
        self._network_timeout_provider = provider

    def _network_timeout(self, requested_timeout: float) -> float:
        value = min(self.timeout_seconds, requested_timeout)
        if self._network_timeout_provider is not None:
            value = self._network_timeout_provider(value)
        if value <= 0:
            raise CollectionPlanRunError("network timeout must be positive")
        return value

    @classmethod
    def from_config(cls, config: AppConfig) -> "RequestsScopedTransport":
        serp = config.raw.get("serp", {})
        route = require_marketplace_proxy(config.raw)
        session = build_requests_session(route)
        headers = {
            "user-agent": str(
                serp.get(
                    "user_agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                )
            ),
            "x-requested-with": str(serp.get("x_requested_with", "XMLHttpRequest")),
            "accept": "application/json, text/plain, */*",
        }
        configured_headers = serp.get("request_headers")
        if isinstance(configured_headers, dict):
            for key, value in configured_headers.items():
                if value is not None and str(key).lower() != "cookie":
                    headers[str(key)] = str(value)

        cookie_path_value = str(serp.get("wb_cookie_file") or "").strip()
        cookie_required_env = str(
            serp.get("cookie_required_env") or "PARSER_WB_COOKIE_REQUIRED"
        )
        cookie_required_value = os.getenv(cookie_required_env, "").strip().lower()
        if cookie_required_value:
            cookie_required = cookie_required_value not in {
                "0",
                "false",
                "no",
                "off",
            }
        elif "cookie_required" in serp:
            cookie_required = bool(serp.get("cookie_required"))
        else:
            cookie_required = True
        cookie_value = ""
        if cookie_path_value:
            cookie_path = Path(cookie_path_value)
            if not cookie_path.is_absolute():
                cookie_path = config.project_root / cookie_path
            if cookie_path.exists():
                cookie_value = cookie_path.read_text(encoding="utf-8").strip()
        if cookie_required and not cookie_value:
            raise CollectionPlanRunError("configured WB cookie is missing")
        if cookie_value:
            headers["cookie"] = cookie_value
        assert_requests_session_proxy(session, route)
        fallback_url = cls._proxy_health_url(
            os.getenv("PARSER_WB_PROXY_ROTATE_URL", "")
        )
        fallback_session: requests.Session | None = None
        if fallback_url is not None:
            fallback_session = requests.Session()
            fallback_session.trust_env = False
            fallback_session.proxies.clear()

        urls: list[str] = []
        candidates: list[Any] = [serp.get("base_url")]
        fallback_urls = serp.get("fallback_base_urls")
        if isinstance(fallback_urls, list):
            candidates.extend(fallback_urls)
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value and value not in urls:
                urls.append(value)
        return cls(
            session=session,
            request_params=serp.get("request_params") or {},
            endpoint_urls=tuple(urls),
            timeout_seconds=config.runtime.http_timeout_seconds,
            referer_base=str(
                serp.get(
                    "referer_base",
                    "https://www.wildberries.ru/catalog/0/search.aspx?search=",
                )
            ),
            request_headers=headers,
            egress_session=session,
            egress_fallback_url=fallback_url,
            egress_fallback_session=fallback_session,
            retry_http_statuses=frozenset(
                int(value)
                for value in (
                    serp.get("retry_http_statuses")
                    or [429, 498, 500, 502, 503, 504]
                )
                if not isinstance(value, bool)
                and isinstance(value, (int, str))
                and str(value).isdigit()
            ),
        )

    @staticmethod
    def _proxy_health_url(rotate_url: str) -> str | None:
        if not rotate_url:
            return None
        try:
            parsed = urlsplit(rotate_url)
            port = parsed.port
        except ValueError as exc:
            raise CollectionPlanRunError(
                "proxy rotation URL is invalid"
            ) from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or port is None
        ):
            raise CollectionPlanRunError("proxy rotation URL is invalid")
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return urlunsplit(
            (parsed.scheme, f"{host}:{port}", "/health", "", "")
        )

    @staticmethod
    def _response_json(response: Any, *, code: str, request_sent: bool = False, dest: str = "") -> Mapping[str, Any]:
        try:
            payload = response.json()
        except Exception as exc:
            raise ScopedTransportError(
                f"{code}_invalid_json",
                request_sent=request_sent,
                dest_id_sent=dest,
                http_status=getattr(response, "status_code", None),
            ) from exc
        if not isinstance(payload, dict):
            raise ScopedTransportError(
                f"{code}_payload_not_object",
                request_sent=request_sent,
                dest_id_sent=dest,
                http_status=getattr(response, "status_code", None),
            )
        return payload

    @staticmethod
    def _extract_dest(payload: Mapping[str, Any]) -> str:
        xinfo = payload.get("xinfo")
        if isinstance(xinfo, str):
            try:
                values = parse_qs(
                    xinfo,
                    keep_blank_values=True,
                    strict_parsing=True,
                )
            except ValueError as exc:
                raise ScopedTransportError("resolver_malformed_xinfo") from exc
            dest_values = values.get("dest") or []
            if len(dest_values) != 1:
                raise ScopedTransportError("resolver_missing_or_duplicate_dest")
            dest = dest_values[0]
        elif isinstance(xinfo, dict):
            dest = xinfo.get("dest")
        else:
            raise ScopedTransportError("resolver_missing_xinfo")
        if not isinstance(dest, str) or not _DEST_RE.fullmatch(dest):
            raise ScopedTransportError("resolver_invalid_dest")
        return dest

    def egress_identity(self, *, timeout_seconds: float) -> str:
        if self._egress_fallback_active:
            return self._fallback_egress_identity(
                timeout_seconds=timeout_seconds
            )
        try:
            response = self.egress_session.get(
                self.egress_check_url,
                headers={
                    "accept": "text/plain",
                    "user-agent": "parser-wb-egress-check/1",
                },
                timeout=min(
                    self._network_timeout(timeout_seconds),
                    EGRESS_FALLBACK_TIMEOUT_SECONDS,
                ),
            )
        except requests.RequestException as exc:
            if (
                self.egress_fallback_url is not None
                and self.egress_fallback_session is not None
            ):
                self._egress_fallback_active = True
                return self._fallback_egress_identity(
                    timeout_seconds=timeout_seconds
                )
            raise ScopedTransportError("egress_network_error") from exc
        if response.status_code != 200:
            raise ScopedTransportError(
                f"egress_http_{response.status_code}",
                http_status=response.status_code,
            )
        value = str(response.text or "").strip()
        try:
            return str(ipaddress.ip_address(value))
        except ValueError as exc:
            raise ScopedTransportError("egress_invalid_ip") from exc

    def _fallback_egress_identity(self, *, timeout_seconds: float) -> str:
        if (
            self.egress_fallback_url is None
            or self.egress_fallback_session is None
        ):
            raise ScopedTransportError("egress_fallback_unavailable")
        try:
            response = self.egress_fallback_session.get(
                self.egress_fallback_url,
                headers={
                    "accept": "application/json",
                    "user-agent": "parser-wb-egress-control/1",
                },
                timeout=self._network_timeout(timeout_seconds),
            )
        except requests.RequestException as exc:
            raise ScopedTransportError(
                "egress_fallback_network_error"
            ) from exc
        if response.status_code != 200:
            raise ScopedTransportError(
                f"egress_fallback_http_{response.status_code}",
                http_status=response.status_code,
            )
        payload = self._response_json(
            response,
            code="egress_fallback",
        )
        if (
            payload.get("ok") is not True
            or payload.get("marketplaceTransportVerified") is not True
        ):
            raise ScopedTransportError("egress_fallback_unhealthy")
        value = payload.get("external_ip")
        if isinstance(value, str) and value.strip():
            try:
                return str(ipaddress.ip_address(value.strip()))
            except ValueError as exc:
                raise ScopedTransportError(
                    "egress_fallback_invalid_ip"
                ) from exc
        channel = payload.get("channel")
        if (
            payload.get("external_ip_verified") is False
            and isinstance(channel, str)
            and _TRANSPORT_IDENTITY_RE.fullmatch(
                f"transport:{channel.strip()}"
            )
        ):
            return f"transport:{channel.strip()}"
        raise ScopedTransportError("egress_fallback_invalid_ip")

    def resolve_destination(
        self,
        region: RegionDefinition,
        *,
        timeout_seconds: float,
    ) -> str:
        try:
            response = self.session.get(
                self.resolver_url,
                params={
                    "latitude": region.latitude,
                    "longitude": region.longitude,
                    "address": region.address_label,
                },
                headers=self.request_headers,
                timeout=self._network_timeout(timeout_seconds),
            )
        except requests.RequestException as exc:
            raise ScopedTransportError("resolver_network_error") from exc
        if response.status_code != 200:
            retry_after_status: str | None = None
            retry_after_seconds: int | None = None
            if response.status_code in {429, 498}:
                response_headers = getattr(response, "headers", {})
                raw_retry_after = (
                    response_headers.get("Retry-After")
                    if isinstance(response_headers, Mapping)
                    else None
                )
                retry_after_status, retry_after_seconds = (
                    parse_retry_after_delta(raw_retry_after)
                )
            raise ScopedTransportError(
                f"resolver_http_{response.status_code}",
                http_status=response.status_code,
                retry_after_status=retry_after_status,
                retry_after_seconds=retry_after_seconds,
            )
        return self._extract_dest(self._response_json(response, code="resolver"))

    def _endpoint_url(self, endpoint_id: str) -> str:
        try:
            index = self.endpoint_policy.endpoint_ids.index(endpoint_id)
        except ValueError as exc:
            raise ScopedTransportError("endpoint_id_unknown") from exc
        return self.endpoint_urls[index]

    def probe_endpoint(
        self,
        request: ScopedSearchRequest,
        *,
        endpoint_id: str,
        timeout_seconds: float,
    ) -> EndpointProbeResult:
        if self._endpoint_pin_finalized:
            raise ScopedTransportError("endpoint_probe_after_pin")
        if request.endpoint_id != endpoint_id:
            raise ScopedTransportError("endpoint_probe_identity_mismatch")
        endpoint_url = self._endpoint_url(endpoint_id)
        try:
            headers = dict(self.request_headers)
            headers["referer"] = (
                f"{self.referer_base}{quote(request.task.query)}"
            )
            response = self.session.get(
                endpoint_url,
                params=dict(request.params),
                headers=headers,
                timeout=self._network_timeout(timeout_seconds),
            )
        except requests.RequestException:
            return EndpointProbeResult(
                endpoint_id=endpoint_id,
                suitable=False,
                http_status=None,
                error_code="endpoint_probe_network_error",
            )
        if response.status_code != 200:
            retry_after_status: str | None = None
            retry_after_seconds: int | None = None
            if response.status_code in {429, 498}:
                response_headers = getattr(response, "headers", {})
                raw_retry_after = (
                    response_headers.get("Retry-After")
                    if isinstance(response_headers, Mapping)
                    else None
                )
                retry_after_status, retry_after_seconds = (
                    parse_retry_after_delta(raw_retry_after)
                )
            return EndpointProbeResult(
                endpoint_id=endpoint_id,
                suitable=False,
                http_status=response.status_code,
                error_code=f"endpoint_probe_http_{response.status_code}",
                retry_after_status=retry_after_status,
                retry_after_seconds=retry_after_seconds,
            )
        try:
            payload = self._response_json(response, code="endpoint_probe")
            products = _extract_products(payload)
            if len(products) != request.task.page_size:
                raise CollectionPlanRunError("endpoint_probe_products_count_invalid")
            seen: set[str] = set()
            for product in products:
                product_id = _normalize_product_id(product)
                if product_id in seen:
                    raise CollectionPlanRunError(
                        "endpoint_probe_product_duplicate"
                    )
                seen.add(product_id)
        except (CollectionPlanRunError, ScopedTransportError) as exc:
            error_code = str(
                getattr(exc, "code", "endpoint_probe_payload_invalid")
            ).replace("\n", " ")[:100]
            return EndpointProbeResult(
                endpoint_id=endpoint_id,
                suitable=False,
                http_status=200,
                error_code=error_code,
            )
        return EndpointProbeResult(
            endpoint_id=endpoint_id,
            suitable=True,
            http_status=200,
            error_code=None,
            reusable_request=request,
            reusable_result=ScopedSearchResult(
                payload=payload,
                endpoint_id=endpoint_id,
                dest_id_sent=request.dest_id_observed,
                http_status=200,
            ),
        )

    def pin_endpoint(self, endpoint_id: str) -> None:
        if self._endpoint_pin_finalized:
            raise ScopedTransportError("endpoint_pin_already_finalized")
        self._endpoint_url(endpoint_id)
        self.endpoint_policy = EffectiveEndpointPolicy(
            selection_mode=self.endpoint_policy.selection_mode,
            endpoint_ids=self.endpoint_policy.endpoint_ids,
            pinned_endpoint_id=endpoint_id,
        )
        self._endpoint_pin_finalized = True

    def search(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        if request.endpoint_id != self.endpoint_policy.pinned_endpoint_id:
            raise ScopedTransportError("search_endpoint_not_pinned")
        endpoint_url = self._endpoint_url(request.endpoint_id)
        try:
            headers = dict(self.request_headers)
            headers["referer"] = (
                f"{self.referer_base}{quote(request.task.query)}"
            )
            response = self.session.get(
                endpoint_url,
                params=dict(request.params),
                headers=headers,
                timeout=self._network_timeout(timeout_seconds),
            )
        except requests.RequestException as exc:
            raise ScopedTransportError(
                "search_network_error",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
            ) from exc
        if response.status_code != 200:
            retry_after_status: str | None = None
            retry_after_seconds: int | None = None
            if response.status_code in {429, 498}:
                response_headers = getattr(response, "headers", {})
                raw_retry_after = (
                    response_headers.get("Retry-After")
                    if isinstance(response_headers, Mapping)
                    else None
                )
                retry_after_status, retry_after_seconds = (
                    parse_retry_after_delta(raw_retry_after)
                )
            raise ScopedTransportError(
                f"search_http_{response.status_code}",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=response.status_code,
                retry_after_status=retry_after_status,
                retry_after_seconds=retry_after_seconds,
            )
        payload = self._response_json(
            response,
            code="search",
            request_sent=True,
            dest=request.dest_id_observed,
        )
        return ScopedSearchResult(
            payload=payload,
            endpoint_id=request.endpoint_id,
            dest_id_sent=request.dest_id_observed,
            http_status=200,
            attempted_endpoint_ids=(request.endpoint_id,),
        )

    def search_ordered(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        if self._endpoint_pin_finalized:
            raise ScopedTransportError("ordered_search_after_endpoint_pin")
        if request.endpoint_id not in self.endpoint_policy.endpoint_ids:
            raise ScopedTransportError("search_endpoint_unknown")

        active_index = self.endpoint_policy.endpoint_ids.index(request.endpoint_id)
        ordered_ids = (
            request.endpoint_id,
            *(
                endpoint_id
                for index, endpoint_id in enumerate(self.endpoint_policy.endpoint_ids)
                if index != active_index
            ),
        )
        attempted: list[str] = []
        last_rate_limited: ScopedTransportError | None = None
        last_payload_anomaly: ScopedTransportError | None = None

        for endpoint_id in ordered_ids:
            endpoint_url = self._endpoint_url(endpoint_id)
            attempted.append(endpoint_id)
            try:
                headers = dict(self.request_headers)
                headers["referer"] = (
                    f"{self.referer_base}{quote(request.task.query)}"
                )
                response = self.session.get(
                    endpoint_url,
                    params=dict(request.params),
                    headers=headers,
                    timeout=self._network_timeout(timeout_seconds),
                )
            except requests.RequestException as exc:
                raise ScopedTransportError(
                    "search_network_error",
                    request_sent=True,
                    dest_id_sent=request.dest_id_observed,
                    endpoint_id=endpoint_id,
                    attempted_endpoint_ids=tuple(attempted),
                ) from exc

            if response.status_code in self.retry_http_statuses:
                retry_after_status: str | None = None
                retry_after_seconds: int | None = None
                if response.status_code in {429, 498}:
                    response_headers = getattr(response, "headers", {})
                    raw_retry_after = (
                        response_headers.get("Retry-After")
                        if isinstance(response_headers, Mapping)
                        else None
                    )
                    retry_after_status, retry_after_seconds = (
                        parse_retry_after_delta(raw_retry_after)
                    )
                last_rate_limited = ScopedTransportError(
                    f"search_http_{response.status_code}",
                    request_sent=True,
                    dest_id_sent=request.dest_id_observed,
                    http_status=response.status_code,
                    retry_after_status=retry_after_status,
                    retry_after_seconds=retry_after_seconds,
                    endpoint_id=endpoint_id,
                    attempted_endpoint_ids=tuple(attempted),
                )
                continue

            if response.status_code != 200:
                raise ScopedTransportError(
                    f"search_http_{response.status_code}",
                    request_sent=True,
                    dest_id_sent=request.dest_id_observed,
                    http_status=response.status_code,
                    endpoint_id=endpoint_id,
                    attempted_endpoint_ids=tuple(attempted),
                )

            try:
                payload = self._response_json(
                    response,
                    code="search",
                    request_sent=True,
                    dest=request.dest_id_observed,
                )
            except ScopedTransportError as exc:
                raise ScopedTransportError(
                    exc.code,
                    request_sent=True,
                    dest_id_sent=request.dest_id_observed,
                    http_status=exc.http_status,
                    endpoint_id=endpoint_id,
                    attempted_endpoint_ids=tuple(attempted),
                ) from exc
            try:
                products = _extract_products(payload)
                product_ids = [
                    _normalize_product_id(product) for product in products
                ]
                if len(product_ids) != len(set(product_ids)):
                    raise CollectionPlanRunError(
                        "search_product_duplicate"
                    )
            except CollectionPlanRunError as exc:
                error_code = str(exc)
                if error_code not in {
                    "retryable_payload_anomaly_nested_promo",
                    "search_product_duplicate",
                }:
                    raise ScopedTransportError(
                        error_code,
                        request_sent=True,
                        dest_id_sent=request.dest_id_observed,
                        http_status=200,
                        endpoint_id=endpoint_id,
                        attempted_endpoint_ids=tuple(attempted),
                    ) from exc
                last_payload_anomaly = ScopedTransportError(
                    (
                        "search_payload_anomaly_nested_promo"
                        if error_code
                        == "retryable_payload_anomaly_nested_promo"
                        else error_code
                    ),
                    request_sent=True,
                    dest_id_sent=request.dest_id_observed,
                    http_status=200,
                    endpoint_id=endpoint_id,
                    attempted_endpoint_ids=tuple(attempted),
                )
                continue
            self.endpoint_policy = EffectiveEndpointPolicy(
                selection_mode=self.endpoint_policy.selection_mode,
                endpoint_ids=self.endpoint_policy.endpoint_ids,
                pinned_endpoint_id=endpoint_id,
            )
            return ScopedSearchResult(
                payload=payload,
                endpoint_id=endpoint_id,
                dest_id_sent=request.dest_id_observed,
                http_status=200,
                attempted_endpoint_ids=tuple(attempted),
            )

        if last_payload_anomaly is not None:
            raise ScopedTransportError(
                last_payload_anomaly.code,
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=last_payload_anomaly.http_status,
                endpoint_id=last_payload_anomaly.endpoint_id,
                attempted_endpoint_ids=tuple(attempted),
            )
        if last_rate_limited is not None:
            raise last_rate_limited
        raise ScopedTransportError(
            "search_no_endpoint_attempted",
            request_sent=False,
            dest_id_sent=request.dest_id_observed,
        )

    def close(self) -> None:
        self.session.close()
        if self.egress_session is not self.session:
            self.egress_session.close()
        if (
            self.egress_fallback_session is not None
            and self.egress_fallback_session is not self.session
            and self.egress_fallback_session is not self.egress_session
        ):
            self.egress_fallback_session.close()


def _normalize_egress_identity(value: str) -> str:
    if _TRANSPORT_IDENTITY_RE.fullmatch(value):
        return value
    return str(ipaddress.ip_address(value))


def _mask_egress(value: str) -> str:
    normalized = _normalize_egress_identity(value)
    if normalized.startswith("transport:"):
        return "transport:x"
    parsed = ipaddress.ip_address(normalized)
    if parsed.version == 4:
        first, second, *_rest = parsed.exploded.split(".")
        return f"{first}.{second}.x.x"
    first, second, *_rest = parsed.exploded.split(":")
    return f"{first}:{second}::x"


def _egress_hash(value: str, *, salt: bytes) -> str:
    return hashlib.sha256(salt + value.encode("ascii")).hexdigest()


PRODUCT_FIELDS = (
    "marketplace",
    "run_id",
    "collected_at_utc",
    "status",
    "collection_scope",
    "collection_plan_id",
    "query_pack_id",
    "query_pack_version",
    "query_id",
    "category_id",
    "query",
    "query_group",
    "region_id",
    "region_name",
    "displayed_region",
    "dest_id_observed",
    "dest_resolved_at_utc",
    "dest_resolution_source",
    "dest_resolution_status",
    "page",
    "page_size",
    "depth",
    "endpoint_id",
    "position_on_page",
    "absolute_position",
    "nmId",
    "imtId",
    "product_name",
    "brand",
    "brandId",
    "supplier_id",
    "supplier_name",
    "final_price",
    "price",
    "sale_price",
    "discount",
    "rating",
    "feedbacks",
    "total_quantity",
    "raw_file",
)

PAGE_FIELDS = (
    "marketplace",
    "run_id",
    "collection_scope",
    "collection_plan_id",
    "query_pack_id",
    "query_pack_version",
    "query_id",
    "category_id",
    "query",
    "query_group",
    "region_id",
    "region_name",
    "displayed_region",
    "dest_id_observed",
    "dest_resolved_at_utc",
    "dest_resolution_source",
    "dest_resolution_status",
    "page",
    "page_size",
    "depth",
    "http_status",
    "products_count",
    "endpoint_id",
    "checkpoint_key",
    "raw_file",
)


def _extract_products(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    products = payload.get("products")
    if products is None:
        nested = payload.get("data")
        products = nested.get("products") if isinstance(nested, dict) else None
    if not isinstance(products, list) or not products:
        raise CollectionPlanRunError("search_products_empty")
    if any(not isinstance(product, dict) for product in products):
        raise CollectionPlanRunError("search_product_malformed")
    if payload.get("products") is None and all(
        isinstance(product.get("log"), dict)
        and product["log"].get("promotion") == 1
        for product in products
    ):
        raise CollectionPlanRunError("retryable_payload_anomaly_nested_promo")
    return products


@dataclass(frozen=True, slots=True)
class BoundedPageContract:
    products: list[Mapping[str, Any]]
    payload_total: int
    capped_total: int
    expected_pages: int
    terminal: bool
    terminal_reason: str | None


def _bounded_page_contract(
    payload: Mapping[str, Any],
    *,
    page: int,
    depth: int,
    page_size: int = 100,
) -> BoundedPageContract:
    products = _extract_products(payload)
    nested = payload.get("data")
    raw_total = payload.get("total")
    if raw_total is None and isinstance(nested, Mapping):
        raw_total = nested.get("total")
    if type(raw_total) is not int or raw_total <= 0:
        raise CollectionPlanRunError("search_payload_total_invalid")
    capped_total = min(raw_total, depth)
    expected_pages = (capped_total + page_size - 1) // page_size
    if page < 1 or page > expected_pages:
        raise CollectionPlanRunError("search_page_exceeds_payload_total")
    expected_count = min(page_size, capped_total - ((page - 1) * page_size))
    if len(products) != expected_count:
        raise CollectionPlanRunError(
            "search_products_count_inconsistent_with_total "
            f"expected={expected_count} actual={len(products)}"
        )
    terminal = page == expected_pages
    terminal_reason = None
    if terminal:
        terminal_reason = (
            "depth_cap_reached" if raw_total > depth else "payload_total_reached"
        )
    return BoundedPageContract(
        products=products,
        payload_total=raw_total,
        capped_total=capped_total,
        expected_pages=expected_pages,
        terminal=terminal,
        terminal_reason=terminal_reason,
    )


def _normalize_product_id(product: Mapping[str, Any]) -> str:
    raw = product.get("id")
    if raw in {None, ""}:
        raw = product.get("nmId")
    if isinstance(raw, bool):
        raise CollectionPlanRunError("search_product_id_malformed")
    value = str(raw or "")
    if not _PRODUCT_ID_RE.fullmatch(value):
        raise CollectionPlanRunError("search_product_id_malformed")
    return value


def _product_prices(
    product: Mapping[str, Any],
) -> tuple[float | None, float | None, float | None]:
    sizes = product.get("sizes")
    price: float | None = None
    sale_price: float | None = None
    if isinstance(sizes, list) and sizes and isinstance(sizes[0], Mapping):
        price_object = sizes[0].get("price")
        if isinstance(price_object, Mapping):
            basic = price_object.get("basic")
            sale = price_object.get("product")
            if isinstance(basic, (int, float)) and not isinstance(basic, bool):
                price = round(float(basic) / 100.0, 2)
            if isinstance(sale, (int, float)) and not isinstance(sale, bool):
                sale_price = round(float(sale) / 100.0, 2)
    return sale_price if sale_price is not None else price, price, sale_price


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


class CollectionPlanRunner:
    def __init__(
        self,
        *,
        config: AppConfig,
        plan_path: Path,
        transport: ScopedTransport,
        no_publish: bool,
        run_id: str | None = None,
        resume_run_id: str | None = None,
        now: Callable[[], datetime] = _default_now,
        lock_event_hook: LockEventHook | None = None,
        write_event_hook: WriteEventHook | None = None,
        egress_hash_salt: bytes | None = None,
        sleeper: Callable[[float], None] = time_module.sleep,
        absolute_deadline_utc: datetime | None = None,
        input_integrity_gate: Callable[[], None] | None = None,
        matrix_continuation: bool = False,
    ) -> None:
        self.config = config
        self.plan_path = plan_path
        self.transport = transport
        self.no_publish = no_publish
        self.now = now
        started = now()
        if run_id is not None and resume_run_id is not None:
            raise CollectionPlanRunError(
                "run_id and resume_run_id are mutually exclusive"
            )
        self.resume = resume_run_id is not None
        if matrix_continuation and (
            self.resume
            or run_id is None
            or absolute_deadline_utc is None
        ):
            raise CollectionPlanRunError(
                "matrix continuation contract is invalid"
            )
        self.matrix_continuation = matrix_continuation
        self.run_id = _safe_run_id(
            resume_run_id or run_id or _default_run_id(started)
        )
        self.started_at_utc = _utc_iso(started)
        self.deadline = DeadlineGuard.for_current_day(now=now)
        self.lock_event_hook = lock_event_hook
        self.write_event_hook = write_event_hook
        self.egress_hash_salt = egress_hash_salt or secrets.token_bytes(32)
        self.sleeper = sleeper
        self.absolute_deadline_utc = absolute_deadline_utc
        self.input_integrity_gate = input_integrity_gate or (lambda: None)

    def _configure_runtime_deadline(self, bundle: CollectionPlanBundle) -> None:
        window = bundle.collection_plan.runtime_window
        if window is not None:
            self.deadline = DeadlineGuard.for_runtime_window(
                window,
                resume=self.resume or self.matrix_continuation,
                now=self.now,
                absolute_deadline_utc=self.absolute_deadline_utc,
            )
        setter = getattr(self.transport, "set_network_timeout_provider", None)
        if callable(setter):
            setter(
                lambda requested: self.deadline.request_timeout(
                    min(
                        requested,
                        float(self.config.runtime.http_timeout_seconds),
                    )
                )
            )

    def _load_bundle(self) -> CollectionPlanBundle:
        return load_collection_plan_bundle(
            project_root=self.config.project_root,
            plan_path=self.plan_path,
            region_registry_path=self.config.project_root / "config/wb/regions.json",
        )

    @staticmethod
    def _bundle_identity(bundle: CollectionPlanBundle) -> tuple[str, ...]:
        return (
            bundle.collection_plan.collection_plan_id,
            bundle.query_pack.query_pack_id,
            bundle.query_pack.version,
            bundle.query_pack_sha256,
            bundle.collection_plan_sha256,
            bundle.region_registry_sha256,
        )

    def _validate_mode(self, bundle: CollectionPlanBundle) -> None:
        plan = bundle.collection_plan
        if not self.no_publish:
            raise CollectionPlanRunError("--no-publish is mandatory")
        if not plan.enabled:
            raise CollectionPlanRunError("collection plan is disabled")
        if plan.publication_mode != "none":
            raise CollectionPlanRunError("collection plan publication must be none")
        if plan.sellers_mode != "disabled":
            raise CollectionPlanRunError("collection plan sellers must be disabled")
        if plan.proxy_rotation_mode != "disabled":
            raise CollectionPlanRunError("collection plan rotation must be disabled")
        expected_pages = plan.depth // 100
        if (
            plan.depth % 100 != 0
            or plan.quality.expected_pages_per_query != expected_pages
        ):
            raise CollectionPlanRunError(
                "collection plan depth must map to complete 100-item pages"
            )

    def _check_egress(self, expected: str | None = None) -> str:
        value = self.transport.egress_identity(
            timeout_seconds=self.deadline.request_timeout(
                self.config.runtime.http_timeout_seconds
            )
        )
        try:
            normalized = _normalize_egress_identity(value)
        except ValueError as exc:
            raise CollectionPlanRunError(
                "egress identity is neither an IP address nor a verified transport"
            ) from exc
        if expected is not None and normalized != expected:
            raise EgressIdentityChangedError(
                "egress identity changed during scoped run"
            )
        return normalized

    def _sleep_from_serp_config(self, key: str) -> None:
        raw_value = self.config.raw.get("serp", {}).get(key, 0)
        try:
            milliseconds = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise CollectionPlanRunError(f"invalid SERP pacing value: {key}") from exc
        if milliseconds < 0:
            raise CollectionPlanRunError(f"invalid SERP pacing value: {key}")
        if milliseconds:
            self.sleeper(milliseconds / 1000.0)

    def _sanitized_endpoint_error_evidence(
        self,
        error: ScopedTransportError,
    ) -> tuple[tuple[str, ...], str]:
        allowed = set(self.transport.endpoint_policy.endpoint_ids)
        attempts = tuple(
            endpoint_id
            for endpoint_id in error.attempted_endpoint_ids
            if endpoint_id in allowed
        )
        if (
            len(attempts) != len(error.attempted_endpoint_ids)
            or len(set(attempts)) != len(attempts)
        ):
            attempts = ()
        endpoint_id = error.endpoint_id if error.endpoint_id in allowed else ""
        if endpoint_id and endpoint_id not in attempts:
            endpoint_id = ""
        return attempts, endpoint_id

    def _write(
        self,
        path: Path,
        payload: bytes,
        *,
        final_manifest: bool = False,
    ) -> None:
        if final_manifest:
            if (
                self.deadline.remaining_seconds()
                <= self.deadline.finalization_reserve_seconds
            ):
                raise CollectionPlanRunError(
                    "collection plan deadline reserve reached before final manifest"
                )
        else:
            self.deadline.ensure_active()
        _write_new_bytes(
            path,
            payload,
            project_root=self.config.project_root,
            event_hook=self.write_event_hook,
        )

    def _replace(
        self,
        path: Path,
        payload: bytes,
        *,
        final_manifest: bool = False,
    ) -> None:
        if final_manifest:
            if (
                self.deadline.remaining_seconds()
                <= self.deadline.finalization_reserve_seconds
            ):
                raise CollectionPlanRunError(
                    "collection plan deadline reserve reached before final manifest"
                )
        else:
            self.deadline.ensure_active()
        _write_atomic_bytes(
            path,
            payload,
            project_root=self.config.project_root,
            event_hook=self.write_event_hook,
            integrity_gate=self.input_integrity_gate,
        )

    def _write_or_verify(
        self,
        path: Path,
        payload: bytes,
        *,
        publication: bool = False,
    ) -> None:
        if path.exists():
            existing = _read_regular_bytes(
                path,
                project_root=self.config.project_root,
            )
            if existing != payload:
                raise CollectionPlanRunError(
                    f"immutable artifact content mismatch: {path}"
                )
            return
        if publication:
            _ensure_scoped_parent(
                path,
                project_root=self.config.project_root,
                event_hook=self.write_event_hook,
            )
            _write_atomic_bytes(
                path,
                payload,
                project_root=self.config.project_root,
                event_hook=self.write_event_hook,
                integrity_gate=self.input_integrity_gate,
                require_absent=True,
            )
        else:
            self._write(path, payload)

    def _estimated_remaining_seconds(
        self,
        *,
        bundle: CollectionPlanBundle,
        pending_pages: int,
    ) -> float:
        serp = self.config.raw.get("serp", {})
        page_sleep = max(0.0, float(serp.get("sleep_between_pages_ms", 0))) / 1000.0
        query_sleep = max(0.0, float(serp.get("sleep_between_queries_ms", 0))) / 1000.0
        endpoint_count = len(self.transport.endpoint_policy.endpoint_ids)
        if endpoint_count < 1:
            raise CollectionPlanRunError("endpoint policy must not be empty")
        request_seconds = (
            float(self.config.runtime.http_timeout_seconds) * endpoint_count
            + page_sleep
            + ESTIMATED_REQUEST_OVERHEAD_SECONDS
        )
        runtime_window = bundle.collection_plan.runtime_window
        if runtime_window is not None:
            segment_pages = min(
                bundle.collection_plan.quality.expected_pages_per_query,
                pending_pages,
            )
            resolver_and_egress_calls = len(bundle.enabled_regions) + 2
            return (
                segment_pages * request_seconds
                + query_sleep
                + resolver_and_egress_calls
                * float(self.config.runtime.http_timeout_seconds)
                + runtime_window.finalization_reserve_seconds
            )
        pending_queries = min(
            len(bundle.enabled_queries) * len(bundle.enabled_regions),
            pending_pages,
        )
        return (
            pending_pages * request_seconds
            + pending_queries * query_sleep
            + NIGHTLY_SAFETY_RESERVE_SECONDS
        )

    def _transport_fingerprint(self) -> dict[str, str]:
        endpoint_urls = getattr(self.transport, "endpoint_urls", None)
        proxy_route_sha256 = getattr(
            self.transport,
            "proxy_route_sha256",
            None,
        )
        if (
            not isinstance(endpoint_urls, tuple)
            or len(endpoint_urls)
            != len(self.transport.endpoint_policy.endpoint_ids)
            or any(type(url) is not str or not url for url in endpoint_urls)
        ):
            raise CollectionPlanRunError(
                "transport endpoint URL provenance is unavailable"
            )
        if (
            not isinstance(proxy_route_sha256, str)
            or not _SHA256_RE.fullmatch(proxy_route_sha256)
        ):
            raise CollectionPlanRunError(
                "transport proxy route provenance is unavailable"
            )
        fingerprint = {
            "schema_version": "wb_transport_fingerprint_v1",
            "ordered_endpoint_urls_sha256": _canonical_sha256(endpoint_urls),
            "request_params_sha256": _canonical_sha256(
                dict(self.transport.request_params)
            ),
            "proxy_route_sha256": proxy_route_sha256,
        }
        manifest_sha256 = os.getenv(
            "PARSER_WB_COORDINATOR_INPUT_MANIFEST_SHA256",
            "",
        )
        runtime_input_sha256 = os.getenv(
            "PARSER_WB_COORDINATOR_RUNTIME_INPUT_SHA256", ""
        )
        if manifest_sha256 or runtime_input_sha256:
            if (
                not _SHA256_RE.fullmatch(manifest_sha256)
                or not _SHA256_RE.fullmatch(runtime_input_sha256)
            ):
                raise CollectionPlanRunError(
                    "runtime input provenance is invalid"
                )
            fingerprint["input_manifest_sha256"] = manifest_sha256
            fingerprint["runtime_input_sha256"] = runtime_input_sha256
        fingerprint["fingerprint_sha256"] = _canonical_sha256(fingerprint)
        return fingerprint

    def _validate_discarded_segments(
        self,
        *,
        value: Any,
        bundle: CollectionPlanBundle,
        verified_segment_ids: set[str],
    ) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise CollectionPlanRunError("discarded segment history must be a list")
        endpoint_ids = self.transport.endpoint_policy.endpoint_ids
        allowed_endpoints = set(endpoint_ids)
        allowed_regions = {region.region_id for region in bundle.enabled_regions}
        allowed_queries = {query.query_id for query in bundle.enabled_queries}
        seen_ids = set(verified_segment_ids)
        normalized: list[dict[str, Any]] = []

        def validate_egress_item(item: Any, *, field: str) -> dict[str, str]:
            if not isinstance(item, dict) or set(item) != {
                "source",
                "masked",
                "ephemeral_sha256",
            }:
                raise CollectionPlanRunError(f"{field} is invalid")
            source = item.get("source")
            masked = item.get("masked")
            digest = item.get("ephemeral_sha256")
            if (
                source not in {"segment_start_check", "previous_segment_end", "segment_end_check"}
                or not isinstance(masked, str)
                or len(masked) > 64
                or "x" not in masked
                or any(character in masked for character in "\r\n")
                or not isinstance(digest, str)
                or not _SHA256_RE.fullmatch(digest)
            ):
                raise CollectionPlanRunError(f"{field} is invalid")
            return {
                "source": source,
                "masked": masked,
                "ephemeral_sha256": digest,
            }

        for item in value:
            if not isinstance(item, dict):
                raise CollectionPlanRunError("discarded segment entry is invalid")
            segment_id = item.get("segment_id")
            region_id = item.get("region_id")
            query_id = item.get("query_id")
            pages_written = item.get("pages_written")
            if (
                not isinstance(segment_id, str)
                or not _ID_RE.fullmatch(segment_id)
                or segment_id in seen_ids
            ):
                raise CollectionPlanRunError(
                    "discarded segment identity is invalid or duplicated"
                )
            if region_id not in allowed_regions or query_id not in allowed_queries:
                raise CollectionPlanRunError("discarded segment scope is invalid")
            if item.get("status") != "incomplete_not_reusable":
                raise CollectionPlanRunError("discarded segment status is invalid")
            if (
                type(pages_written) is not int
                or not 0
                <= pages_written
                <= bundle.collection_plan.quality.expected_pages_per_query
            ):
                raise CollectionPlanRunError(
                    "discarded segment pages_written is invalid"
                )
            usage = item.get("endpoint_usage")
            if not isinstance(usage, dict) or set(usage) != allowed_endpoints:
                raise CollectionPlanRunError(
                    "discarded segment endpoint usage is invalid"
                )
            normalized_usage: dict[str, dict[str, int]] = {}
            for endpoint_id in endpoint_ids:
                counters = usage.get(endpoint_id)
                if not isinstance(counters, dict) or set(counters) != {
                    "attempts",
                    "pages_ok",
                }:
                    raise CollectionPlanRunError(
                        "discarded segment endpoint counters are invalid"
                    )
                attempts = counters.get("attempts")
                pages_ok = counters.get("pages_ok")
                if (
                    type(attempts) is not int
                    or type(pages_ok) is not int
                    or attempts < 0
                    or pages_ok < 0
                    or pages_ok > attempts
                ):
                    raise CollectionPlanRunError(
                        "discarded segment endpoint counters are invalid"
                    )
                normalized_usage[endpoint_id] = {
                    "attempts": attempts,
                    "pages_ok": pages_ok,
                }
            if sum(
                counters["pages_ok"] for counters in normalized_usage.values()
            ) != pages_written:
                raise CollectionPlanRunError(
                    "discarded segment pages do not match endpoint counters"
                )

            attempted = item.get("attempted_endpoint_ids", [])
            endpoint_id = item.get("endpoint_id")
            if (
                not isinstance(attempted, list)
                or any(type(entry) is not str for entry in attempted)
                or len(set(attempted)) != len(attempted)
                or any(entry not in allowed_endpoints for entry in attempted)
                or endpoint_id is not None
                and endpoint_id not in allowed_endpoints
                or endpoint_id is not None
                and endpoint_id not in attempted
            ):
                raise CollectionPlanRunError(
                    "discarded segment endpoint evidence is invalid"
                )
            error_code = item.get("error_code")
            if (
                not isinstance(error_code, str)
                or not 1 <= len(error_code) <= 100
                or any(character in error_code for character in "\r\n")
            ):
                raise CollectionPlanRunError(
                    "discarded segment error code is invalid"
                )
            egress = item.get("egress")
            if not isinstance(egress, dict):
                raise CollectionPlanRunError(
                    "discarded segment egress evidence is invalid"
                )
            verification_status = egress.get("verification_status")
            constant = egress.get("constant")
            checks_completed = egress.get("checks_completed")
            checks_expected = egress.get("checks_expected")
            start = validate_egress_item(
                egress.get("start"),
                field="discarded segment egress start",
            )
            end_value = egress.get("end")
            if verification_status == "unverified":
                if (
                    constant is not None
                    or checks_completed != 1
                    or checks_expected != 2
                    or end_value is not None
                ):
                    raise CollectionPlanRunError(
                        "discarded unverified egress evidence is invalid"
                    )
                end = None
            elif verification_status == "changed":
                if (
                    constant is not False
                    or checks_completed != 2
                    or checks_expected != 2
                ):
                    raise CollectionPlanRunError(
                        "discarded changed egress evidence is invalid"
                    )
                end = validate_egress_item(
                    end_value,
                    field="discarded segment egress end",
                )
            else:
                raise CollectionPlanRunError(
                    "discarded segment egress status is invalid"
                )

            seen_ids.add(segment_id)
            normalized.append(
                {
                    "segment_id": segment_id,
                    "region_id": region_id,
                    "query_id": query_id,
                    "pages_written": pages_written,
                    "status": "incomplete_not_reusable",
                    "endpoint_id": endpoint_id,
                    "attempted_endpoint_ids": list(attempted),
                    "error_code": error_code,
                    "endpoint_usage": normalized_usage,
                    "egress": {
                        "verification_status": verification_status,
                        "constant": constant,
                        "checks_completed": checks_completed,
                        "checks_expected": checks_expected,
                        "start": start,
                        "end": end,
                    },
                }
            )
        return normalized

    def _task(
        self,
        *,
        bundle: CollectionPlanBundle,
        query_id: str,
        region_id: str,
        page: int = 1,
    ) -> ScopedTask:
        query = next(item for item in bundle.enabled_queries if item.query_id == query_id)
        region = next(item for item in bundle.enabled_regions if item.region_id == region_id)
        return ScopedTask(
            collection_plan_id=bundle.collection_plan.collection_plan_id,
            query_pack_id=bundle.query_pack.query_pack_id,
            query_pack_version=bundle.query_pack.version,
            query_id=query.query_id,
            category_id=query.category_id,
            query=query.text,
            query_group=query.category_id,
            region_id=region.region_id,
            region_name=region.region_name,
            page=page,
            page_size=100,
            depth=bundle.collection_plan.depth,
        )

    def _search_request(
        self,
        *,
        task: ScopedTask,
        dest_id: str,
    ) -> ScopedSearchRequest:
        params = {
            str(key): str(value)
            for key, value in self.transport.request_params.items()
            if value is not None
        }
        params.update(
            {
                "query": task.query,
                "page": str(task.page),
                "dest": dest_id,
            }
        )
        return ScopedSearchRequest(
            task=task,
            dest_id_observed=dest_id,
            endpoint_id=self.transport.endpoint_policy.pinned_endpoint_id,
            params=params,
        )

    def _rows_for_page(
        self,
        *,
        task: ScopedTask,
        resolution: ResolvedDestination,
        products: list[Mapping[str, Any]],
        raw_path: Path,
        endpoint_id: str,
        expected_products_count: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        expected_count = (
            task.page_size
            if expected_products_count is None
            else expected_products_count
        )
        if len(products) != expected_count:
            raise CollectionPlanRunError(
                f"search_products_short expected={expected_count} actual={len(products)}"
            )
        seen: set[str] = set()
        rows: list[dict[str, Any]] = []
        raw_file = _relative(raw_path, self.config.project_root)
        collected_at_utc = _utc_iso(self.now())
        for index, product in enumerate(products, start=1):
            product_id = _normalize_product_id(product)
            if product_id in seen:
                raise CollectionPlanRunError("search_product_duplicate")
            seen.add(product_id)
            final_price, price, sale_price = _product_prices(product)
            rows.append(
                {
                    "marketplace": "wb",
                    "run_id": self.run_id,
                    "collected_at_utc": collected_at_utc,
                    "status": "success",
                    "collection_scope": COLLECTION_SCOPE,
                    "collection_plan_id": task.collection_plan_id,
                    "query_pack_id": task.query_pack_id,
                    "query_pack_version": task.query_pack_version,
                    "query_id": task.query_id,
                    "category_id": task.category_id,
                    "query": task.query,
                    "query_group": task.query_group,
                    "region_id": task.region_id,
                    "region_name": task.region_name,
                    "displayed_region": task.region_name,
                    "dest_id_observed": resolution.dest_id_observed,
                    "dest_resolved_at_utc": resolution.dest_resolved_at_utc,
                    "dest_resolution_source": resolution.dest_resolution_source,
                    "dest_resolution_status": "resolved_and_sent",
                    "page": task.page,
                    "page_size": task.page_size,
                    "depth": task.depth,
                    "endpoint_id": endpoint_id,
                    "position_on_page": index,
                    "absolute_position": ((task.page - 1) * task.page_size) + index,
                    "nmId": product_id,
                    "imtId": product.get("imtId") or "",
                    "product_name": product.get("name") or "",
                    "brand": product.get("brand") or "",
                    "brandId": product.get("brandId") or "",
                    "supplier_id": product.get("supplierId") or "",
                    "supplier_name": product.get("supplier") or "",
                    "final_price": (
                        final_price if final_price is not None else ""
                    ),
                    "price": price if price is not None else "",
                    "sale_price": sale_price if sale_price is not None else "",
                    "discount": (
                        product.get("discount")
                        if product.get("discount") is not None
                        else ""
                    ),
                    "rating": product.get("rating")
                    if product.get("rating") is not None
                    else "",
                    "feedbacks": product.get("feedbacks")
                    if product.get("feedbacks") is not None
                    else "",
                    "total_quantity": product.get("totalQuantity")
                    if product.get("totalQuantity") is not None
                    else product.get("stock", ""),
                    "raw_file": raw_file,
                }
            )
        page_row = {
            **{key: rows[0][key] for key in PAGE_FIELDS if key in rows[0]},
            "http_status": 200,
            "products_count": len(rows),
            "endpoint_id": endpoint_id,
            "checkpoint_key": task.checkpoint_key,
            "raw_file": raw_file,
        }
        return rows, page_row

    def _write_scope_outputs(
        self,
        *,
        paths: ScopedPaths,
        region_id: str,
        product_rows: list[dict[str, Any]],
        page_rows: list[dict[str, Any]],
        replace: bool = False,
    ) -> dict[str, Any]:
        outputs: dict[str, Any] = {}
        if product_rows:
            raw_products = paths.layer_region_run_dir("raw", region_id) / "products_raw.csv"
            staging_products = (
                paths.layer_region_run_dir("staging", region_id)
                / "products_staging.csv"
            )
            mart_products = (
                paths.layer_region_run_dir("marts", region_id) / "products_daily.csv"
            )
            pages_index = (
                paths.layer_region_run_dir("raw", region_id) / "pages_raw_index.csv"
            )
            product_bytes = _csv_bytes(product_rows, PRODUCT_FIELDS)
            pages_bytes = _csv_bytes(page_rows, PAGE_FIELDS)
            writer = self._replace if replace else self._write
            writer(raw_products, product_bytes)
            writer(staging_products, product_bytes)
            writer(mart_products, product_bytes)
            writer(pages_index, pages_bytes)
            outputs = {
                "raw_products_path": _relative(raw_products, paths.project_root),
                "staging_products_path": _relative(
                    staging_products,
                    paths.project_root,
                ),
                "mart_products_path": _relative(mart_products, paths.project_root),
                "pages_index_path": _relative(pages_index, paths.project_root),
                "products_sha256": _sha256_bytes(product_bytes),
                "pages_index_sha256": _sha256_bytes(pages_bytes),
                "products_count": len(product_rows),
                "pages_count": len(page_rows),
            }
        return outputs

    def _checkpoint_payload(
        self,
        *,
        task: ScopedTask,
        resolution: ResolvedDestination,
        raw_path: Path,
        endpoint_id: str = "",
        attempted_endpoint_ids: tuple[str, ...] = (),
        raw_sha256: str | None = None,
        effective_plan_sha256: str | None = None,
        bundle: CollectionPlanBundle | None = None,
        segment_id: str | None = None,
        products_count: int | None = None,
        payload_total: int | None = None,
        capped_total: int | None = None,
        terminal: bool | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "checkpoint_key": task.checkpoint_key,
            "collection_plan_id": task.collection_plan_id,
            "query_pack_id": task.query_pack_id,
            "query_pack_version": task.query_pack_version,
            "region_id": task.region_id,
            "query_id": task.query_id,
            "page": task.page,
            "dest_id_observed": resolution.dest_id_observed,
            "dest_resolution_status": "resolved_and_sent",
            "endpoint_id": endpoint_id,
            "attempted_endpoint_ids": list(attempted_endpoint_ids),
            "raw_file": _relative(raw_path, self.config.project_root),
        }
        resumable_values = (
            raw_sha256,
            effective_plan_sha256,
            bundle,
            segment_id,
        )
        if any(value is not None for value in resumable_values):
            if any(value is None for value in resumable_values):
                raise CollectionPlanRunError(
                    "resumable checkpoint provenance is incomplete"
                )
            assert bundle is not None
            bounded = bundle.collection_plan.runtime_window is not None
            actual_products_count = (
                task.page_size if products_count is None else products_count
            )
            payload.update(
                {
                    "schema_version": (
                        BOUNDED_CHECKPOINT_SCHEMA_VERSION
                        if bounded
                        else RESUMABLE_CHECKPOINT_SCHEMA_VERSION
                    ),
                    "query": task.query,
                    "page_size": task.page_size,
                    "depth": task.depth,
                    "raw_sha256": raw_sha256,
                    "products_count": actual_products_count,
                    "query_pack_sha256": bundle.query_pack_sha256,
                    "collection_plan_sha256": bundle.collection_plan_sha256,
                    "region_registry_sha256": bundle.region_registry_sha256,
                    "effective_plan_sha256": effective_plan_sha256,
                    "segment_id": segment_id,
                }
            )
            if bounded:
                if (
                    type(payload_total) is not int
                    or type(capped_total) is not int
                    or type(terminal) is not bool
                ):
                    raise CollectionPlanRunError(
                        "bounded checkpoint completion evidence is incomplete"
                    )
                payload.update(
                    {
                        "payload_total": payload_total,
                        "capped_total": capped_total,
                        "terminal": terminal,
                    }
                )
        return payload

    def _load_reusable_page(
        self,
        *,
        paths: ScopedPaths,
        task: ScopedTask,
        resolution: ResolvedDestination,
        bundle: CollectionPlanBundle,
        effective_plan_sha256: str,
        verified_segment_ids: set[str],
    ) -> tuple[list[dict[str, Any]], dict[str, Any], str] | None:
        checkpoint_path = paths.checkpoint_path(task)
        raw_path = paths.raw_page_path(task)
        if not checkpoint_path.exists() and not raw_path.exists():
            return None
        if not checkpoint_path.exists() or not raw_path.exists():
            raise CollectionPlanRunError(
                f"resume artifact pair incomplete: {task.checkpoint_key}"
            )
        checkpoint = _json_object_from_bytes(
            _read_regular_bytes(
                checkpoint_path,
                project_root=self.config.project_root,
            ),
            field="checkpoint",
        )
        bounded = bundle.collection_plan.runtime_window is not None
        checkpoint_products_count = checkpoint.get("products_count")
        if (
            type(checkpoint_products_count) is not int
            or not 1 <= checkpoint_products_count <= task.page_size
        ):
            raise CollectionPlanRunError(
                f"resume checkpoint products count invalid: {task.checkpoint_key}"
            )
        expected = {
            "schema_version": (
                BOUNDED_CHECKPOINT_SCHEMA_VERSION
                if bounded
                else RESUMABLE_CHECKPOINT_SCHEMA_VERSION
            ),
            "checkpoint_key": task.checkpoint_key,
            "collection_plan_id": task.collection_plan_id,
            "query_pack_id": task.query_pack_id,
            "query_pack_version": task.query_pack_version,
            "query_id": task.query_id,
            "query": task.query,
            "region_id": task.region_id,
            "page": task.page,
            "page_size": task.page_size,
            "depth": task.depth,
            "dest_id_observed": resolution.dest_id_observed,
            "dest_resolution_status": "resolved_and_sent",
            "raw_file": _relative(raw_path, self.config.project_root),
            "products_count": checkpoint_products_count,
            "query_pack_sha256": bundle.query_pack_sha256,
            "collection_plan_sha256": bundle.collection_plan_sha256,
            "region_registry_sha256": bundle.region_registry_sha256,
            "effective_plan_sha256": effective_plan_sha256,
        }
        for key, value in expected.items():
            if checkpoint.get(key) != value or type(checkpoint.get(key)) is not type(value):
                raise CollectionPlanRunError(
                    f"resume checkpoint metadata mismatch: {task.checkpoint_key}:{key}"
                )
        segment_id = checkpoint.get("segment_id")
        if not isinstance(segment_id, str) or segment_id not in verified_segment_ids:
            raise CollectionPlanRunError(
                f"resume checkpoint segment is not verified: {task.checkpoint_key}"
            )
        endpoint_id = checkpoint.get("endpoint_id")
        attempts = checkpoint.get("attempted_endpoint_ids")
        allowed = self.transport.endpoint_policy.endpoint_ids
        if (
            not isinstance(endpoint_id, str)
            or endpoint_id not in allowed
            or not isinstance(attempts, list)
            or not attempts
            or any(type(item) is not str or item not in allowed for item in attempts)
            or len(set(attempts)) != len(attempts)
            or endpoint_id not in attempts
        ):
            raise CollectionPlanRunError(
                f"resume checkpoint endpoint evidence mismatch: {task.checkpoint_key}"
            )
        raw_bytes = _read_regular_bytes(raw_path, project_root=self.config.project_root)
        if checkpoint.get("raw_sha256") != _sha256_bytes(raw_bytes):
            raise CollectionPlanRunError(
                f"resume raw checksum mismatch: {task.checkpoint_key}"
            )
        payload = _json_object_from_bytes(raw_bytes, field="raw page")
        if bounded:
            page_contract = _bounded_page_contract(
                payload,
                page=task.page,
                depth=task.depth,
                page_size=task.page_size,
            )
            for key, expected_value in {
                "payload_total": page_contract.payload_total,
                "capped_total": page_contract.capped_total,
                "terminal": page_contract.terminal,
            }.items():
                if (
                    checkpoint.get(key) != expected_value
                    or type(checkpoint.get(key)) is not type(expected_value)
                ):
                    raise CollectionPlanRunError(
                        f"resume checkpoint completion mismatch: {task.checkpoint_key}:{key}"
                    )
            products = page_contract.products
        else:
            products = _extract_products(payload)
        rows, page_row = self._rows_for_page(
            task=task,
            resolution=resolution,
            products=products,
            raw_path=raw_path,
            endpoint_id=endpoint_id,
            expected_products_count=checkpoint_products_count,
        )
        return rows, page_row, segment_id

    def _publish_regional_latest(
        self,
        *,
        paths: ScopedPaths,
        bundle: CollectionPlanBundle,
        effective_plan_sha256: str,
        region_manifests: list[dict[str, Any]],
    ) -> dict[str, Any]:
        region_refs: list[dict[str, Any]] = []
        for region in region_manifests:
            if region.get("status") != "success" or region.get("complete") is not True:
                raise CollectionPlanRunError("regional latest requires all regions complete")
            payload = {
                "schema_version": REGIONAL_LATEST_REGION_SCHEMA_VERSION,
                "collection_plan_id": bundle.collection_plan.collection_plan_id,
                "run_id": self.run_id,
                "region_id": region["region_id"],
                "effective_plan_sha256": effective_plan_sha256,
                "pages_count": region["pages_ok"],
                "products_count": region["products_ok"],
                "outputs": region["outputs"],
            }
            payload_bytes = _json_bytes(payload)
            target = paths.latest_region_manifest_path(region["region_id"])
            self._write_or_verify(
                target,
                payload_bytes,
                publication=True,
            )
            region_refs.append(
                {
                    "region_id": region["region_id"],
                    "manifest_path": _relative(target, paths.project_root),
                    "manifest_sha256": _sha256_bytes(payload_bytes),
                    "pages_count": region["pages_ok"],
                    "products_count": region["products_ok"],
                }
            )
        latest = {
            "schema_version": REGIONAL_LATEST_SCHEMA_VERSION,
            "collection_plan_id": bundle.collection_plan.collection_plan_id,
            "run_id": self.run_id,
            "effective_plan_sha256": effective_plan_sha256,
            "published_at_utc": _utc_iso(self.now()),
            "regions": region_refs,
        }
        def pointer_matches() -> bool:
            if not paths.latest_path.exists():
                return False
            try:
                current = _json_object_from_bytes(
                    _read_regular_bytes(
                        paths.latest_path,
                        project_root=paths.project_root,
                    ),
                    field="regional latest",
                )
            except CollectionPlanRunError:
                return False
            return (
                current.get("schema_version") == REGIONAL_LATEST_SCHEMA_VERSION
                and current.get("collection_plan_id")
                == bundle.collection_plan.collection_plan_id
                and current.get("run_id") == self.run_id
                and current.get("effective_plan_sha256")
                == effective_plan_sha256
                and current.get("regions") == region_refs
            )

        if pointer_matches():
            return {
                "status": "reconciled",
                "path": _relative(paths.latest_path, paths.project_root),
                "regions": len(region_refs),
            }
        try:
            self._replace(paths.latest_path, _json_bytes(latest))
        except Exception:
            if pointer_matches():
                return {
                    "status": "reconciled_after_durable_replace",
                    "path": _relative(paths.latest_path, paths.project_root),
                    "regions": len(region_refs),
                }
            raise
        return {
            "status": "published",
            "path": _relative(paths.latest_path, paths.project_root),
            "regions": len(region_refs),
        }

    def _verified_segments(
        self,
        *,
        paths: ScopedPaths,
        bundle: CollectionPlanBundle,
        effective_plan_sha256: str,
        expected_refs: tuple[Mapping[str, Any], ...],
        promote: bool = True,
    ) -> tuple[list[dict[str, Any]], set[str]]:
        records: list[dict[str, Any]] = []
        verified_ids: set[str] = set()
        scopes: set[tuple[str, str]] = set()
        validated_segments: list[dict[str, Any]] = []
        if not expected_refs:
            return records, verified_ids
        allowed_regions = {region.region_id for region in bundle.enabled_regions}
        allowed_queries = {query.query_id for query in bundle.enabled_queries}
        endpoint_ids = self.transport.endpoint_policy.endpoint_ids
        allowed_endpoints = set(endpoint_ids)
        expected_pages = (
            bundle.collection_plan.quality.expected_pages_per_query
        )
        bounded = bundle.collection_plan.runtime_window is not None

        def validate_egress_item(
            value: Any,
            *,
            field: str,
            allowed_sources: set[str],
        ) -> dict[str, str]:
            if not isinstance(value, dict) or set(value) != {
                "source",
                "masked",
                "ephemeral_sha256",
            }:
                raise CollectionPlanRunError(
                    f"verified segment {field} is invalid"
                )
            source = value.get("source")
            masked = value.get("masked")
            digest = value.get("ephemeral_sha256")
            if (
                source not in allowed_sources
                or type(masked) is not str
                or not (
                    _MASKED_IPV4_RE.fullmatch(masked)
                    or _MASKED_IPV6_RE.fullmatch(masked)
                    or _MASKED_TRANSPORT_RE.fullmatch(masked)
                )
                or type(digest) is not str
                or not _SHA256_RE.fullmatch(digest)
            ):
                raise CollectionPlanRunError(
                    f"verified segment {field} is invalid"
                )
            return {
                "source": source,
                "masked": masked,
                "ephemeral_sha256": digest,
            }

        for expected_ref in expected_refs:
            expected_ref_fields = {
                "segment_id",
                "region_id",
                "query_id",
                "path",
                "sha256",
                "egress",
                "pages_count",
            }
            if bounded:
                expected_ref_fields.update({"products_count", "completion"})
            if not isinstance(expected_ref, dict) or set(expected_ref) != expected_ref_fields:
                raise CollectionPlanRunError("segment reference structure is invalid")
            segment_id = expected_ref.get("segment_id")
            expected_sha256 = expected_ref.get("sha256")
            if (
                type(segment_id) is not str
                or not _ID_RE.fullmatch(segment_id)
                or type(expected_sha256) is not str
                or not _SHA256_RE.fullmatch(expected_sha256)
            ):
                raise CollectionPlanRunError("segment reference identity is invalid")
            path = paths.segment_path(segment_id)
            expected_path = _relative(path, paths.project_root)
            if expected_ref.get("path") != expected_path:
                raise CollectionPlanRunError("segment reference path mismatch")
            payload_bytes = _read_regular_bytes(
                path,
                project_root=self.config.project_root,
            )
            payload_sha256 = _sha256_bytes(payload_bytes)
            if expected_sha256 != payload_sha256:
                raise CollectionPlanRunError("segment reference checksum mismatch")
            segment = _json_object_from_bytes(payload_bytes, field="segment")
            segment_fields = {
                "schema_version",
                "run_id",
                "segment_id",
                "collection_plan_id",
                "query_pack_sha256",
                "collection_plan_sha256",
                "region_registry_sha256",
                "effective_plan_sha256",
                "region_id",
                "query_id",
                "pages",
                "endpoint_usage",
                "egress",
            }
            if bounded:
                segment_fields.add("completion")
            if set(segment) != segment_fields:
                raise CollectionPlanRunError("segment structure is invalid")
            identity = {
                "schema_version": (
                    BOUNDED_SEGMENT_SCHEMA_VERSION
                    if bounded
                    else SEGMENT_SCHEMA_VERSION
                ),
                "run_id": self.run_id,
                "collection_plan_id": bundle.collection_plan.collection_plan_id,
                "query_pack_sha256": bundle.query_pack_sha256,
                "collection_plan_sha256": bundle.collection_plan_sha256,
                "region_registry_sha256": bundle.region_registry_sha256,
                "effective_plan_sha256": effective_plan_sha256,
            }
            if any(segment.get(key) != value for key, value in identity.items()):
                raise CollectionPlanRunError("segment provenance mismatch")
            segment_id = segment.get("segment_id")
            region_id = segment.get("region_id")
            query_id = segment.get("query_id")
            if (
                type(segment_id) is not str
                or path != paths.segment_path(segment_id)
                or type(region_id) is not str
                or type(query_id) is not str
            ):
                raise CollectionPlanRunError("segment identity is invalid")
            if region_id not in allowed_regions or query_id not in allowed_queries:
                raise CollectionPlanRunError("segment scope is outside enabled plan")
            scope = (region_id, query_id)
            if scope in scopes:
                raise CollectionPlanRunError("multiple verified segments for one query scope")
            egress = segment.get("egress")
            pages = segment.get("pages")
            if (
                not isinstance(egress, dict)
                or set(egress) != {
                    "verification_status",
                    "constant",
                    "checks_completed",
                    "checks_expected",
                    "start",
                    "end",
                }
                or egress.get("verification_status") != "verified_constant"
                or egress.get("constant") is not True
                or type(egress.get("checks_completed")) is not int
                or egress.get("checks_completed") != 2
                or type(egress.get("checks_expected")) is not int
                or egress.get("checks_expected") != 2
                or not isinstance(pages, list)
                or (
                    not 1 <= len(pages) <= expected_pages
                    if bounded
                    else len(pages) != expected_pages
                )
            ):
                raise CollectionPlanRunError("segment is not complete and verified")
            completion: dict[str, Any] | None = None
            if bounded:
                raw_completion = segment.get("completion")
                if not isinstance(raw_completion, dict) or set(raw_completion) != {
                    "payload_total",
                    "capped_total",
                    "pages_count",
                    "products_count",
                    "terminal_page",
                    "terminal_reason",
                    "complete",
                    "duplicate_product_positions",
                }:
                    raise CollectionPlanRunError(
                        "bounded segment completion evidence is invalid"
                    )
                payload_total = raw_completion.get("payload_total")
                capped_total = raw_completion.get("capped_total")
                pages_count = raw_completion.get("pages_count")
                products_count = raw_completion.get("products_count")
                terminal_page = raw_completion.get("terminal_page")
                terminal_reason = raw_completion.get("terminal_reason")
                duplicate_positions = raw_completion.get(
                    "duplicate_product_positions"
                )
                expected_capped_total = (
                    min(payload_total, bundle.collection_plan.depth)
                    if type(payload_total) is int and payload_total > 0
                    else -1
                )
                expected_page_count = (
                    (expected_capped_total + 99) // 100
                    if expected_capped_total > 0
                    else -1
                )
                expected_reason = (
                    "depth_cap_reached"
                    if type(payload_total) is int
                    and payload_total > bundle.collection_plan.depth
                    else "payload_total_reached"
                )
                if (
                    type(capped_total) is not int
                    or capped_total != expected_capped_total
                    or type(pages_count) is not int
                    or pages_count != expected_page_count
                    or pages_count != len(pages)
                    or type(products_count) is not int
                    or products_count != capped_total
                    or type(terminal_page) is not int
                    or terminal_page != pages_count
                    or terminal_reason != expected_reason
                    or raw_completion.get("complete") is not True
                    or type(duplicate_positions) is not int
                    or not 0 <= duplicate_positions < products_count
                ):
                    raise CollectionPlanRunError(
                        "bounded segment completion evidence is inconsistent"
                    )
                completion = {
                    "payload_total": payload_total,
                    "capped_total": capped_total,
                    "pages_count": pages_count,
                    "products_count": products_count,
                    "terminal_page": terminal_page,
                    "terminal_reason": terminal_reason,
                    "complete": True,
                    "duplicate_product_positions": duplicate_positions,
                }
            start_egress = validate_egress_item(
                egress.get("start"),
                field="egress start",
                allowed_sources={
                    "segment_start_check",
                    "previous_segment_end",
                },
            )
            end_egress = validate_egress_item(
                egress.get("end"),
                field="egress end",
                allowed_sources={"segment_end_check"},
            )
            if (
                start_egress["masked"] != end_egress["masked"]
                or start_egress["ephemeral_sha256"]
                != end_egress["ephemeral_sha256"]
            ):
                raise CollectionPlanRunError(
                    "verified segment egress is not constant"
                )
            normalized_egress = {
                "verification_status": "verified_constant",
                "constant": True,
                "checks_completed": 2,
                "checks_expected": 2,
                "start": start_egress,
                "end": end_egress,
            }

            usage = segment.get("endpoint_usage")
            if not isinstance(usage, dict) or set(usage) != allowed_endpoints:
                raise CollectionPlanRunError(
                    "verified segment endpoint usage is invalid"
                )
            normalized_usage: dict[str, dict[str, int]] = {}
            for endpoint_id in endpoint_ids:
                counters = usage.get(endpoint_id)
                if not isinstance(counters, dict) or set(counters) != {
                    "attempts",
                    "pages_ok",
                }:
                    raise CollectionPlanRunError(
                        "verified segment endpoint counters are invalid"
                    )
                attempts = counters.get("attempts")
                pages_ok = counters.get("pages_ok")
                if (
                    type(attempts) is not int
                    or type(pages_ok) is not int
                    or attempts < 0
                    or pages_ok < 0
                    or pages_ok > attempts
                ):
                    raise CollectionPlanRunError(
                        "verified segment endpoint counters are invalid"
                    )
                normalized_usage[endpoint_id] = {
                    "attempts": attempts,
                    "pages_ok": pages_ok,
                }
            if sum(
                counters["pages_ok"]
                for counters in normalized_usage.values()
            ) != len(pages):
                raise CollectionPlanRunError(
                    "verified segment pages do not match endpoint counters"
                )

            normalized_pages: list[dict[str, Any]] = []
            for expected_page, page_ref in enumerate(pages, start=1):
                if not isinstance(page_ref, dict) or set(page_ref) != {
                    "page",
                    "pending_raw_path",
                    "canonical_raw_path",
                    "raw_sha256",
                    "pending_checkpoint_path",
                    "canonical_checkpoint_path",
                    "checkpoint_sha256",
                }:
                    raise CollectionPlanRunError(
                        "verified segment page reference is invalid"
                    )
                page = page_ref.get("page")
                raw_sha256 = page_ref.get("raw_sha256")
                checkpoint_sha256 = page_ref.get("checkpoint_sha256")
                if (
                    type(page) is not int
                    or page != expected_page
                    or type(raw_sha256) is not str
                    or not _SHA256_RE.fullmatch(raw_sha256)
                    or type(checkpoint_sha256) is not str
                    or not _SHA256_RE.fullmatch(checkpoint_sha256)
                ):
                    raise CollectionPlanRunError(
                        "verified segment page identity is invalid"
                    )
                task = self._task(
                    bundle=bundle,
                    query_id=query_id,
                    region_id=region_id,
                    page=page,
                )
                expected_page_ref = {
                    "page": page,
                    "pending_raw_path": _relative(
                        paths.segment_pending_raw_path(segment_id, task),
                        paths.project_root,
                    ),
                    "canonical_raw_path": _relative(
                        paths.raw_page_path(task),
                        paths.project_root,
                    ),
                    "raw_sha256": raw_sha256,
                    "pending_checkpoint_path": _relative(
                        paths.segment_pending_checkpoint_path(segment_id, task),
                        paths.project_root,
                    ),
                    "canonical_checkpoint_path": _relative(
                        paths.checkpoint_path(task),
                        paths.project_root,
                    ),
                    "checkpoint_sha256": checkpoint_sha256,
                }
                if page_ref != expected_page_ref:
                    raise CollectionPlanRunError(
                        "verified segment page path mismatch"
                    )
                normalized_pages.append(expected_page_ref)

            if bounded:
                actual_product_ids: list[str] = []
                observed_total: int | None = None
                observed_capped_total: int | None = None
                for page_ref in normalized_pages:
                    page = int(page_ref["page"])
                    raw_bytes = _read_regular_bytes(
                        paths.project_root / page_ref["pending_raw_path"],
                        project_root=paths.project_root,
                    )
                    if _sha256_bytes(raw_bytes) != page_ref["raw_sha256"]:
                        raise CollectionPlanRunError(
                            "bounded segment raw checksum mismatch"
                        )
                    raw_payload = _json_object_from_bytes(
                        raw_bytes,
                        field="bounded segment raw page",
                    )
                    page_contract = _bounded_page_contract(
                        raw_payload,
                        page=page,
                        depth=bundle.collection_plan.depth,
                    )
                    if (
                        observed_capped_total is not None
                        and observed_capped_total
                        != page_contract.capped_total
                    ):
                        raise CollectionPlanRunError(
                            "bounded segment capped total changed"
                        )
                    observed_total = page_contract.payload_total
                    observed_capped_total = page_contract.capped_total
                    page_product_ids = [
                        _normalize_product_id(product)
                        for product in page_contract.products
                    ]
                    if len(page_product_ids) != len(set(page_product_ids)):
                        raise CollectionPlanRunError(
                            "bounded segment contains duplicate product on one page"
                        )
                    actual_product_ids.extend(page_product_ids)
                    checkpoint_bytes = _read_regular_bytes(
                        paths.project_root
                        / page_ref["pending_checkpoint_path"],
                        project_root=paths.project_root,
                    )
                    if (
                        _sha256_bytes(checkpoint_bytes)
                        != page_ref["checkpoint_sha256"]
                    ):
                        raise CollectionPlanRunError(
                            "bounded segment checkpoint checksum mismatch"
                        )
                    checkpoint = _json_object_from_bytes(
                        checkpoint_bytes,
                        field="bounded segment checkpoint",
                    )
                    for key, expected_value in {
                        "schema_version": BOUNDED_CHECKPOINT_SCHEMA_VERSION,
                        "page": page,
                        "products_count": len(page_contract.products),
                        "payload_total": page_contract.payload_total,
                        "capped_total": page_contract.capped_total,
                        "terminal": page_contract.terminal,
                        "segment_id": segment_id,
                    }.items():
                        if (
                            checkpoint.get(key) != expected_value
                            or type(checkpoint.get(key))
                            is not type(expected_value)
                        ):
                            raise CollectionPlanRunError(
                                "bounded segment checkpoint metadata mismatch"
                            )
                actual_completion = {
                    "payload_total": observed_total,
                    "capped_total": observed_capped_total,
                    "pages_count": len(normalized_pages),
                    "products_count": len(actual_product_ids),
                    "terminal_page": len(normalized_pages),
                    "terminal_reason": (
                        "depth_cap_reached"
                        if int(observed_total) > bundle.collection_plan.depth
                        else "payload_total_reached"
                    ),
                    "complete": True,
                    "duplicate_product_positions": (
                        len(actual_product_ids) - len(set(actual_product_ids))
                    ),
                }
                if completion != actual_completion:
                    raise CollectionPlanRunError(
                        "bounded segment completion does not match page artifacts"
                    )

            normalized_segment = {
                **identity,
                "segment_id": segment_id,
                "region_id": region_id,
                "query_id": query_id,
                "pages": normalized_pages,
                "endpoint_usage": normalized_usage,
                "egress": normalized_egress,
            }
            if bounded:
                normalized_segment["completion"] = completion
            if segment != normalized_segment:
                raise CollectionPlanRunError(
                    "verified segment canonical content mismatch"
                )
            record = {
                "segment_id": segment_id,
                "region_id": region_id,
                "query_id": query_id,
                "path": expected_path,
                "sha256": payload_sha256,
                "egress": normalized_egress,
                "pages_count": len(pages),
            }
            if bounded:
                record.update(
                    {
                        "products_count": completion["products_count"],
                        "completion": completion,
                    }
                )
            if expected_ref != record:
                raise CollectionPlanRunError(
                    "segment reference does not match canonical segment"
                )
            scopes.add(scope)
            verified_ids.add(segment_id)
            records.append(record)
            validated_segments.append(normalized_segment)

        for segment in validated_segments:
            self._validate_verified_segment_artifacts(
                paths=paths,
                segment=segment,
            )
        if promote:
            for segment in validated_segments:
                self._promote_verified_segment(paths=paths, segment=segment)
        return records, verified_ids

    def _validate_verified_segment_artifacts(
        self,
        *,
        paths: ScopedPaths,
        segment: Mapping[str, Any],
    ) -> None:
        for page in segment["pages"]:
            for kind in ("raw", "checkpoint"):
                pending = paths.project_root / page[f"pending_{kind}_path"]
                canonical = paths.project_root / page[f"canonical_{kind}_path"]
                expected_hash = page[f"{kind}_sha256"]
                pending_bytes = _read_regular_bytes(
                    pending,
                    project_root=paths.project_root,
                )
                if _sha256_bytes(pending_bytes) != expected_hash:
                    raise CollectionPlanRunError(
                        f"segment pending {kind} checksum mismatch"
                    )
                if canonical.exists():
                    canonical_bytes = _read_regular_bytes(
                        canonical,
                        project_root=paths.project_root,
                    )
                    if _sha256_bytes(canonical_bytes) != expected_hash:
                        raise CollectionPlanRunError(
                            f"canonical {kind} conflicts with verified segment"
                        )

    def _promote_verified_segment(
        self,
        *,
        paths: ScopedPaths,
        segment: Mapping[str, Any],
    ) -> None:
        for page in segment["pages"]:
            if not isinstance(page, dict):
                raise CollectionPlanRunError("segment page reference is invalid")
            for kind in ("raw", "checkpoint"):
                pending_key = f"pending_{kind}_path"
                canonical_key = f"canonical_{kind}_path"
                hash_key = f"{kind}_sha256"
                try:
                    pending = paths.project_root / str(page[pending_key])
                    canonical = paths.project_root / str(page[canonical_key])
                    expected_hash = str(page[hash_key])
                except KeyError as exc:
                    raise CollectionPlanRunError(
                        "segment page reference is incomplete"
                    ) from exc
                pending_bytes = _read_regular_bytes(
                    pending,
                    project_root=paths.project_root,
                )
                if _sha256_bytes(pending_bytes) != expected_hash:
                    raise CollectionPlanRunError(
                        f"segment pending {kind} checksum mismatch"
                    )
                if canonical.exists():
                    canonical_bytes = _read_regular_bytes(
                        canonical,
                        project_root=paths.project_root,
                    )
                    if _sha256_bytes(canonical_bytes) != expected_hash:
                        raise CollectionPlanRunError(
                            f"canonical {kind} conflicts with verified segment"
                        )
                else:
                    self._write(canonical, pending_bytes)

    def _next_segment_id(
        self,
        paths: ScopedPaths,
        *,
        reserved_ids: set[str] | None = None,
    ) -> str:
        indices: list[int] = []
        for segment_id in reserved_ids or set():
            match = re.fullmatch(r"segment-([0-9]{3,6})", segment_id)
            if match:
                indices.append(int(match.group(1)))
        for base in (paths.segment_dir, paths.state_run_dir / "pending_segments"):
            if not base.exists():
                continue
            for path in base.glob("segment-*"):
                match = re.fullmatch(r"segment-([0-9]{3,6})(?:\.json)?", path.name)
                if match:
                    indices.append(int(match.group(1)))
        return f"segment-{max(indices, default=0) + 1:03d}"

    def _run_resumable(
        self,
        *,
        initial_bundle: CollectionPlanBundle,
        paths: ScopedPaths,
    ) -> dict[str, Any]:
        with acquire_collection_plan_locks(
            paths=paths,
            stale_seconds=self.config.runtime.lock_stale_seconds,
            event_hook=self.lock_event_hook,
        ):
            self.input_integrity_gate()
            bundle = self._load_bundle()
            self._validate_mode(bundle)
            if self._bundle_identity(bundle) != self._bundle_identity(initial_bundle):
                raise CollectionPlanRunError(
                    "collection plan sources changed during lock acquisition"
                )
            if self.resume:
                if not paths.state_run_dir.is_dir():
                    raise CollectionPlanRunError("resume run state does not exist")
            elif paths.state_run_dir.exists():
                raise CollectionPlanRunError(
                    f"immutable scoped run state already exists: {paths.state_run_dir}"
                )

            planned_pages = (
                len(bundle.enabled_regions)
                * len(bundle.enabled_queries)
                * bundle.collection_plan.quality.expected_pages_per_query
            )
            transport_fingerprint = self._transport_fingerprint()
            register_query_pack_provenance(
                provenance_path=paths.provenance_path,
                query_pack=bundle.query_pack,
                project_root=paths.project_root,
            )

            prior_manifest: dict[str, Any] | None = None
            attestation_transitions: list[dict[str, str]] = []
            if self.resume:
                prior_manifest = _json_object_from_bytes(
                    _read_regular_bytes(
                        paths.manifest_path,
                        project_root=paths.project_root,
                    ),
                    field="resume manifest",
                )
                for key, expected in {
                    "run_id": self.run_id,
                    "collection_plan_id": bundle.collection_plan.collection_plan_id,
                    "query_pack_sha256": bundle.query_pack_sha256,
                    "collection_plan_sha256": bundle.collection_plan_sha256,
                    "region_registry_sha256": bundle.region_registry_sha256,
                }.items():
                    if prior_manifest.get(key) != expected:
                        raise CollectionPlanRunError(
                            f"resume manifest provenance mismatch: {key}"
                        )
                if prior_manifest.get("complete") is True:
                    raise CollectionPlanRunError("completed run cannot be resumed")

            snapshot: dict[str, Any] | None = None
            effective_sha256 = ""
            segment_refs: list[dict[str, Any]] = []
            verified_segment_ids: set[str] = set()
            discarded_segments: list[dict[str, Any]] = []
            if self.resume:
                snapshot_bytes = _read_regular_bytes(
                    paths.effective_plan_path,
                    project_root=paths.project_root,
                )
                snapshot = _json_object_from_bytes(
                    snapshot_bytes,
                    field="effective plan",
                )
                effective_sha256 = canonical_effective_plan_sha256(snapshot)
                if prior_manifest is None or prior_manifest.get(
                    "effective_plan_sha256"
                ) != effective_sha256:
                    raise CollectionPlanRunError("resume effective plan hash mismatch")
                manifest_transition = _resume_attestation_transition(
                    stored=prior_manifest.get("transport_fingerprint"),
                    current=transport_fingerprint,
                    project_root=paths.project_root,
                    run_id=self.run_id,
                    collection_plan_id=(
                        bundle.collection_plan.collection_plan_id
                    ),
                    effective_plan_sha256=effective_sha256,
                )
                snapshot_transition = _resume_attestation_transition(
                    stored=snapshot.get("transport_fingerprint"),
                    current=transport_fingerprint,
                    project_root=paths.project_root,
                    run_id=self.run_id,
                    collection_plan_id=(
                        bundle.collection_plan.collection_plan_id
                    ),
                    effective_plan_sha256=effective_sha256,
                )
                attestation_transitions = _resume_attestation_history(
                    value=prior_manifest.get(
                        "transport_attestation_transitions"
                    ),
                    expected=(
                        manifest_transition,
                        snapshot_transition,
                    ),
                    allow_initial_missing=manifest_transition is not None,
                )
                for key, expected in {
                    "query_pack_sha256": bundle.query_pack_sha256,
                    "collection_plan_sha256": bundle.collection_plan_sha256,
                    "region_registry_sha256": bundle.region_registry_sha256,
                    "depth": bundle.collection_plan.depth,
                }.items():
                    if snapshot.get(key) != expected:
                        raise CollectionPlanRunError(
                            f"resume effective plan mismatch: {key}"
                        )
                runtime_window = bundle.collection_plan.runtime_window
                if runtime_window is not None:
                    expected_runtime_window = {
                        "mode": runtime_window.mode,
                        "scheduled_start_msk": runtime_window.scheduled_start_msk,
                        "new_run_start_grace_seconds": runtime_window.new_run_start_grace_seconds,
                        "max_invocation_runtime_seconds": runtime_window.max_invocation_runtime_seconds,
                        "absolute_cutoff_msk": runtime_window.absolute_cutoff_msk,
                        "minimum_resume_window_seconds": runtime_window.minimum_resume_window_seconds,
                        "finalization_reserve_seconds": runtime_window.finalization_reserve_seconds,
                    }
                    if snapshot.get("runtime_window") != expected_runtime_window:
                        raise CollectionPlanRunError(
                            "resume effective plan mismatch: runtime_window"
                        )
                resume_state = prior_manifest.get("resume")
                prior_refs = (
                    resume_state.get("segments")
                    if isinstance(resume_state, dict)
                    else None
                )
                if not isinstance(prior_refs, list):
                    raise CollectionPlanRunError(
                        "resume manifest segment references are invalid"
                    )
                segment_refs, verified_segment_ids = self._verified_segments(
                    paths=paths,
                    bundle=bundle,
                    effective_plan_sha256=effective_sha256,
                    expected_refs=tuple(prior_refs),
                )
                discarded_segments = self._validate_discarded_segments(
                    value=resume_state.get("discarded_segments", []),
                    bundle=bundle,
                    verified_segment_ids=verified_segment_ids,
                )

            expected_scopes = len(bundle.enabled_regions) * len(
                bundle.enabled_queries
            )
            confirmed_scopes = len(segment_refs)
            if confirmed_scopes > expected_scopes:
                raise CollectionPlanRunError(
                    "resume has more query segments than the plan"
                )
            pending_pages = (
                expected_scopes - confirmed_scopes
            ) * bundle.collection_plan.quality.expected_pages_per_query
            self.deadline.ensure_estimated_window(
                self._estimated_remaining_seconds(
                    bundle=bundle,
                    pending_pages=pending_pages,
                )
            )

            publication_reconcile_only = (
                self.resume
                and confirmed_scopes == expected_scopes
                and prior_manifest is not None
                and prior_manifest.get("status") == "publication_pending"
            )
            resolved_now: dict[str, ResolvedDestination] = {}
            if not publication_reconcile_only:
                for region in bundle.enabled_regions:
                    dest_id = self.transport.resolve_destination(
                        region,
                        timeout_seconds=self.deadline.request_timeout(
                            self.config.runtime.http_timeout_seconds
                        ),
                    )
                    if type(dest_id) is not str or not _DEST_RE.fullmatch(dest_id):
                        raise CollectionPlanRunError(
                            f"resolver returned invalid dest for {region.region_id}"
                        )
                    resolved_now[region.region_id] = ResolvedDestination(
                        region_id=region.region_id,
                        dest_id_observed=dest_id,
                        dest_resolved_at_utc=_utc_iso(self.now()),
                    )

            if self.resume:
                if snapshot is None:
                    raise CollectionPlanRunError("resume effective plan is missing")
                stored_regions = {
                    item.get("region_id"): item
                    for item in snapshot.get("regions", [])
                    if isinstance(item, dict)
                }
                resolved: dict[str, ResolvedDestination] = {}
                for region in bundle.enabled_regions:
                    region_id = region.region_id
                    stored = stored_regions.get(region_id)
                    if not isinstance(stored, dict):
                        raise CollectionPlanRunError(
                            f"resume destination is missing: {region_id}"
                        )
                    current = resolved_now.get(region_id)
                    if (
                        current is not None
                        and stored.get("dest_id_observed")
                        != current.dest_id_observed
                    ):
                        raise CollectionPlanRunError(
                            f"resume destination mismatch: {region_id}"
                        )
                    resolved[region_id] = ResolvedDestination(
                        region_id=region_id,
                        dest_id_observed=stored["dest_id_observed"],
                        dest_resolved_at_utc=stored["dest_resolved_at_utc"],
                    )
            else:
                resolved = resolved_now
                snapshot = build_effective_plan_snapshot(
                    bundle,
                    resolved_destinations=resolved,
                    page_size=100,
                    endpoint_policy=self.transport.endpoint_policy,
                    transport_fingerprint=transport_fingerprint,
                )
                snapshot_bytes = canonical_effective_plan_bytes(snapshot)
                effective_sha256 = canonical_effective_plan_sha256(snapshot)
                self._write(paths.effective_plan_path, snapshot_bytes)

            if not self.resume:
                segment_refs, verified_segment_ids = self._verified_segments(
                    paths=paths,
                    bundle=bundle,
                    effective_plan_sha256=effective_sha256,
                    expected_refs=(),
                )
            endpoint_usage = {
                endpoint_id: {"attempts": 0, "pages_ok": 0}
                for endpoint_id in self.transport.endpoint_policy.endpoint_ids
            }
            for ref in segment_refs:
                segment = _json_object_from_bytes(
                    _read_regular_bytes(
                        paths.segment_path(ref["segment_id"]),
                        project_root=paths.project_root,
                    ),
                    field="segment",
                )
                for endpoint_id, usage in segment.get("endpoint_usage", {}).items():
                    if endpoint_id not in endpoint_usage or not isinstance(usage, dict):
                        raise CollectionPlanRunError("segment endpoint usage is invalid")
                    endpoint_usage[endpoint_id]["attempts"] += int(
                        usage.get("attempts", 0)
                    )
                    endpoint_usage[endpoint_id]["pages_ok"] += int(
                        usage.get("pages_ok", 0)
                    )
            for discarded in discarded_segments:
                for endpoint_id, usage in discarded["endpoint_usage"].items():
                    endpoint_usage[endpoint_id]["attempts"] += usage["attempts"]
                    endpoint_usage[endpoint_id]["pages_ok"] += usage["pages_ok"]

            def write_progress_manifest() -> None:
                progress = {
                    "schema_version": RESUMABLE_MANIFEST_SCHEMA_VERSION,
                    "run_id": self.run_id,
                    "collection_scope": COLLECTION_SCOPE,
                    "collection_plan_id": bundle.collection_plan.collection_plan_id,
                    "query_pack_id": bundle.query_pack.query_pack_id,
                    "query_pack_version": bundle.query_pack.version,
                    "query_pack_sha256": bundle.query_pack_sha256,
                    "collection_plan_sha256": bundle.collection_plan_sha256,
                    "region_registry_sha256": bundle.region_registry_sha256,
                    "effective_plan_sha256": effective_sha256,
                    "transport_fingerprint": transport_fingerprint,
                    "transport_attestation_transitions": (
                        attestation_transitions
                    ),
                    "effective_plan_snapshot_path": _relative(
                        paths.effective_plan_path,
                        paths.project_root,
                    ),
                    "publication_mode": "none",
                    "sellers_mode": "disabled",
                    "proxy_rotation_mode": "disabled",
                    "started_at_utc": (
                        prior_manifest.get("started_at_utc")
                        if prior_manifest is not None
                        else self.started_at_utc
                    ),
                    "updated_at_utc": _utc_iso(self.now()),
                    "status": "running",
                    "complete": False,
                    "resume": {
                        "resumed": self.resume,
                        "segments": segment_refs,
                        "verified_segments": len(segment_refs),
                        "discarded_segments": discarded_segments,
                        "failed_segment": None,
                        "maximum_repeated_pages": bundle.collection_plan.quality.expected_pages_per_query,
                    },
                }
                self._replace(paths.manifest_path, _json_bytes(progress))

            write_progress_manifest()

            manifest_regions: list[dict[str, Any]] = []
            region_rows: dict[str, tuple[list[dict[str, Any]], list[dict[str, Any]]]] = {}
            totals = {"regions_ok": 0, "pages_ok": 0, "products_ok": 0}
            bounded = bundle.collection_plan.runtime_window is not None
            verified_refs_by_scope = {
                (ref["region_id"], ref["query_id"]): ref
                for ref in segment_refs
            }
            boundary_egress: str | None = None
            failed_segment: dict[str, Any] | None = None
            caught: Exception | None = None

            try:
                for region in bundle.enabled_regions:
                    products_all: list[dict[str, Any]] = []
                    pages_all: list[dict[str, Any]] = []
                    region_manifest = {
                        "region_id": region.region_id,
                        "dest_id_observed": resolved[region.region_id].dest_id_observed,
                        "dest_resolution_status": "resolved_not_sent",
                        "status": "pending",
                        "complete": False,
                        "pages_ok": 0,
                        "products_ok": 0,
                    }
                    manifest_regions.append(region_manifest)
                    for query in bundle.enabled_queries:
                        verified_ref = verified_refs_by_scope.get(
                            (region.region_id, query.query_id)
                        )
                        task_count = (
                            int(verified_ref["pages_count"])
                            if verified_ref is not None
                            else bundle.collection_plan.quality.expected_pages_per_query
                        )
                        tasks = [
                            self._task(
                                bundle=bundle,
                                query_id=query.query_id,
                                region_id=region.region_id,
                                page=page,
                            )
                            for page in range(
                                1,
                                task_count + 1,
                            )
                        ]
                        loaded = [
                            self._load_reusable_page(
                                paths=paths,
                                task=task,
                                resolution=resolved[region.region_id],
                                bundle=bundle,
                                effective_plan_sha256=effective_sha256,
                                verified_segment_ids=verified_segment_ids,
                            )
                            for task in tasks
                        ]
                        if any(item is not None for item in loaded):
                            if any(item is None for item in loaded):
                                raise CollectionPlanRunError(
                                    f"verified query segment is incomplete: {region.region_id}/{query.query_id}"
                                )
                            segment_ids = {item[2] for item in loaded if item is not None}
                            if len(segment_ids) != 1:
                                raise CollectionPlanRunError(
                                    "query pages span multiple verified segments"
                                )
                            for rows, page_row, _segment_id in loaded:  # type: ignore[misc]
                                products_all.extend(rows)
                                pages_all.append(page_row)
                            continue

                        segment_id = self._next_segment_id(
                            paths,
                            reserved_ids={
                                *verified_segment_ids,
                                *(
                                    item["segment_id"]
                                    for item in discarded_segments
                                ),
                            },
                        )
                        start_egress = boundary_egress or self._check_egress()
                        end_egress: str | None = None
                        page_refs: list[dict[str, Any]] = []
                        segment_usage = {
                            endpoint_id: {"attempts": 0, "pages_ok": 0}
                            for endpoint_id in endpoint_usage
                        }
                        last_attempts: tuple[str, ...] = ()
                        last_endpoint_id = ""
                        bounded_completion: dict[str, Any] | None = None
                        segment_product_ids: list[str] = []
                        try:
                            for task in tasks:
                                last_attempts = ()
                                last_endpoint_id = ""
                                request = self._search_request(
                                    task=task,
                                    dest_id=resolved[region.region_id].dest_id_observed,
                                )
                                result = self.transport.search_ordered(
                                    request,
                                    timeout_seconds=self.deadline.request_timeout(
                                        self.config.runtime.http_timeout_seconds
                                    ),
                                )
                                attempts = result.attempted_endpoint_ids or (
                                    result.endpoint_id,
                                )
                                if (
                                    result.endpoint_id not in endpoint_usage
                                    or result.endpoint_id not in attempts
                                    or len(set(attempts)) != len(attempts)
                                    or any(item not in endpoint_usage for item in attempts)
                                ):
                                    raise CollectionPlanRunError(
                                        "search endpoint evidence mismatch"
                                    )
                                last_attempts = tuple(attempts)
                                last_endpoint_id = result.endpoint_id
                                for endpoint_id in attempts:
                                    segment_usage[endpoint_id]["attempts"] += 1
                                if result.dest_id_sent != request.dest_id_observed:
                                    raise CollectionPlanRunError(
                                        "search destination evidence mismatch"
                                    )
                                pending_raw = paths.segment_pending_raw_path(
                                    segment_id, task
                                )
                                raw_bytes = _json_bytes(result.payload)
                                self._write(pending_raw, raw_bytes)
                                page_contract = (
                                    _bounded_page_contract(
                                        result.payload,
                                        page=task.page,
                                        depth=task.depth,
                                        page_size=task.page_size,
                                    )
                                    if bounded
                                    else None
                                )
                                products = (
                                    page_contract.products
                                    if page_contract is not None
                                    else _extract_products(result.payload)
                                )
                                rows, page_row = self._rows_for_page(
                                    task=task,
                                    resolution=resolved[region.region_id],
                                    products=products,
                                    raw_path=paths.raw_page_path(task),
                                    endpoint_id=result.endpoint_id,
                                    expected_products_count=(
                                        len(products) if bounded else task.page_size
                                    ),
                                )
                                segment_product_ids.extend(
                                    str(row["nmId"]) for row in rows
                                )
                                segment_usage[result.endpoint_id]["pages_ok"] += 1
                                checkpoint = self._checkpoint_payload(
                                    task=task,
                                    resolution=resolved[region.region_id],
                                    raw_path=paths.raw_page_path(task),
                                    endpoint_id=result.endpoint_id,
                                    attempted_endpoint_ids=attempts,
                                    raw_sha256=_sha256_bytes(raw_bytes),
                                    effective_plan_sha256=effective_sha256,
                                    bundle=bundle,
                                    segment_id=segment_id,
                                    products_count=len(products),
                                    payload_total=(
                                        page_contract.payload_total
                                        if page_contract is not None
                                        else None
                                    ),
                                    capped_total=(
                                        page_contract.capped_total
                                        if page_contract is not None
                                        else None
                                    ),
                                    terminal=(
                                        page_contract.terminal
                                        if page_contract is not None
                                        else None
                                    ),
                                )
                                checkpoint_bytes = _json_bytes(checkpoint)
                                pending_checkpoint = (
                                    paths.segment_pending_checkpoint_path(
                                        segment_id, task
                                    )
                                )
                                self._write(pending_checkpoint, checkpoint_bytes)
                                page_refs.append(
                                    {
                                        "page": task.page,
                                        "pending_raw_path": _relative(
                                            pending_raw, paths.project_root
                                        ),
                                        "canonical_raw_path": _relative(
                                            paths.raw_page_path(task), paths.project_root
                                        ),
                                        "raw_sha256": _sha256_bytes(raw_bytes),
                                        "pending_checkpoint_path": _relative(
                                            pending_checkpoint, paths.project_root
                                        ),
                                        "canonical_checkpoint_path": _relative(
                                            paths.checkpoint_path(task),
                                            paths.project_root,
                                        ),
                                        "checkpoint_sha256": _sha256_bytes(
                                            checkpoint_bytes
                                        ),
                                    }
                                )
                                self._sleep_from_serp_config(
                                    "sleep_between_pages_ms"
                                )
                                if page_contract is not None:
                                    if bounded_completion is None:
                                        bounded_completion = {
                                            "payload_total": page_contract.payload_total,
                                            "capped_total": page_contract.capped_total,
                                        }
                                    elif (
                                        bounded_completion["capped_total"]
                                        != page_contract.capped_total
                                    ):
                                        raise CollectionPlanRunError(
                                            "search_capped_total_changed_between_pages"
                                        )
                                    else:
                                        bounded_completion["payload_total"] = (
                                            page_contract.payload_total
                                        )
                                    if page_contract.terminal:
                                        bounded_completion.update(
                                            {
                                                "pages_count": len(page_refs),
                                                "products_count": len(
                                                    segment_product_ids
                                                ),
                                                "terminal_page": task.page,
                                                "terminal_reason": (
                                                    page_contract.terminal_reason
                                                ),
                                                "complete": True,
                                                "duplicate_product_positions": (
                                                    len(segment_product_ids)
                                                    - len(set(segment_product_ids))
                                                ),
                                            }
                                        )
                                        break
                            if bounded and (
                                bounded_completion is None
                                or bounded_completion.get("complete") is not True
                            ):
                                raise CollectionPlanRunError(
                                    "bounded query segment has no proven terminal page"
                                )
                            end_egress = self._check_egress()
                            if end_egress != start_egress:
                                raise EgressIdentityChangedError(
                                    "egress identity changed during query segment"
                                )
                        except Exception as exc:
                            egress_changed = (
                                end_egress is not None
                                and end_egress != start_egress
                            )
                            failed_segment = {
                                "segment_id": segment_id,
                                "region_id": region.region_id,
                                "query_id": query.query_id,
                                "pages_written": sum(
                                    usage["pages_ok"]
                                    for usage in segment_usage.values()
                                ),
                                "status": "incomplete_not_reusable",
                                "egress": {
                                    "verification_status": (
                                        "changed"
                                        if egress_changed
                                        else "unverified"
                                    ),
                                    "constant": False if egress_changed else None,
                                    "checks_completed": 2 if egress_changed else 1,
                                    "checks_expected": 2,
                                    "start": {
                                        "source": (
                                            "previous_segment_end"
                                            if boundary_egress is not None
                                            else "segment_start_check"
                                        ),
                                        "masked": _mask_egress(start_egress),
                                        "ephemeral_sha256": _egress_hash(
                                            start_egress,
                                            salt=self.egress_hash_salt,
                                        ),
                                    },
                                    "end": (
                                        {
                                            "source": "segment_end_check",
                                            "masked": _mask_egress(end_egress),
                                            "ephemeral_sha256": _egress_hash(
                                                end_egress,
                                                salt=self.egress_hash_salt,
                                            ),
                                        }
                                        if egress_changed
                                        else None
                                    ),
                                },
                            }
                            if isinstance(exc, ScopedTransportError):
                                attempts, endpoint_id = (
                                    self._sanitized_endpoint_error_evidence(exc)
                                )
                                for attempted in attempts:
                                    segment_usage[attempted]["attempts"] += 1
                                failed_segment.update(
                                    {
                                        "endpoint_id": endpoint_id or None,
                                        "attempted_endpoint_ids": list(attempts),
                                        "error_code": exc.code,
                                    }
                                )
                            else:
                                local_attempts = (
                                    last_attempts
                                    if not egress_changed
                                    else ()
                                )
                                local_endpoint_id = (
                                    last_endpoint_id
                                    if local_attempts
                                    else None
                                )
                                failed_segment.update(
                                    {
                                        "endpoint_id": local_endpoint_id,
                                        "attempted_endpoint_ids": list(
                                            local_attempts
                                        ),
                                        "error_code": (
                                            "egress_identity_changed"
                                            if isinstance(
                                                exc,
                                                EgressIdentityChangedError,
                                            )
                                            else exc.__class__.__name__
                                        ),
                                    }
                                )
                            failed_segment["endpoint_usage"] = {
                                endpoint_id: dict(usage)
                                for endpoint_id, usage in segment_usage.items()
                            }
                            discarded_segments = self._validate_discarded_segments(
                                value=[*discarded_segments, failed_segment],
                                bundle=bundle,
                                verified_segment_ids=verified_segment_ids,
                            )
                            failed_segment = discarded_segments[-1]
                            for endpoint_id, usage in segment_usage.items():
                                endpoint_usage[endpoint_id]["attempts"] += usage[
                                    "attempts"
                                ]
                                endpoint_usage[endpoint_id]["pages_ok"] += usage[
                                    "pages_ok"
                                ]
                            raise exc

                        segment_payload = {
                            "schema_version": (
                                BOUNDED_SEGMENT_SCHEMA_VERSION
                                if bounded
                                else SEGMENT_SCHEMA_VERSION
                            ),
                            "run_id": self.run_id,
                            "segment_id": segment_id,
                            "collection_plan_id": bundle.collection_plan.collection_plan_id,
                            "query_pack_sha256": bundle.query_pack_sha256,
                            "collection_plan_sha256": bundle.collection_plan_sha256,
                            "region_registry_sha256": bundle.region_registry_sha256,
                            "effective_plan_sha256": effective_sha256,
                            "region_id": region.region_id,
                            "query_id": query.query_id,
                            "pages": page_refs,
                            "endpoint_usage": segment_usage,
                            "egress": {
                                "verification_status": "verified_constant",
                                "constant": True,
                                "checks_completed": 2,
                                "checks_expected": 2,
                                "start": {
                                    "source": (
                                        "previous_segment_end"
                                        if boundary_egress is not None
                                        else "segment_start_check"
                                    ),
                                    "masked": _mask_egress(start_egress),
                                    "ephemeral_sha256": _egress_hash(
                                        start_egress,
                                        salt=self.egress_hash_salt,
                                    ),
                                },
                                "end": {
                                    "source": "segment_end_check",
                                    "masked": _mask_egress(end_egress),
                                    "ephemeral_sha256": _egress_hash(
                                        end_egress,
                                        salt=self.egress_hash_salt,
                                    ),
                                },
                            },
                        }
                        if bounded:
                            segment_payload["completion"] = bounded_completion
                        segment_bytes = _json_bytes(segment_payload)
                        self._write(paths.segment_path(segment_id), segment_bytes)
                        segment_ref = {
                            "segment_id": segment_id,
                            "region_id": region.region_id,
                            "query_id": query.query_id,
                            "path": _relative(
                                paths.segment_path(segment_id),
                                paths.project_root,
                            ),
                            "sha256": _sha256_bytes(segment_bytes),
                            "egress": segment_payload["egress"],
                            "pages_count": len(page_refs),
                        }
                        if bounded:
                            segment_ref.update(
                                {
                                    "products_count": bounded_completion[
                                        "products_count"
                                    ],
                                    "completion": bounded_completion,
                                }
                            )
                        segment_refs.append(segment_ref)
                        verified_refs_by_scope[
                            (region.region_id, query.query_id)
                        ] = segment_ref
                        write_progress_manifest()
                        self._promote_verified_segment(
                            paths=paths,
                            segment=segment_payload,
                        )
                        verified_segment_ids.add(segment_id)
                        for endpoint_id, usage in segment_usage.items():
                            endpoint_usage[endpoint_id]["attempts"] += usage["attempts"]
                            endpoint_usage[endpoint_id]["pages_ok"] += usage["pages_ok"]
                        for task in tasks[: len(page_refs)]:
                            reused = self._load_reusable_page(
                                paths=paths,
                                task=task,
                                resolution=resolved[region.region_id],
                                bundle=bundle,
                                effective_plan_sha256=effective_sha256,
                                verified_segment_ids=verified_segment_ids,
                            )
                            if reused is None:
                                raise CollectionPlanRunError(
                                    "verified segment promotion is incomplete"
                                )
                            products_all.extend(reused[0])
                            pages_all.append(reused[1])
                        boundary_egress = end_egress
                        self._sleep_from_serp_config("sleep_between_queries_ms")

                    region_manifest["pages_ok"] = len(pages_all)
                    region_manifest["products_ok"] = len(products_all)
                    region_segment_refs = [
                        ref
                        for ref in segment_refs
                        if ref["region_id"] == region.region_id
                    ]
                    region_manifest["queries_ok"] = len(region_segment_refs)
                    region_manifest["duplicate_product_positions"] = sum(
                        int(ref.get("completion", {}).get(
                            "duplicate_product_positions", 0
                        ))
                        for ref in region_segment_refs
                    )
                    if bounded:
                        region_manifest["complete"] = (
                            len(region_segment_refs) == len(bundle.enabled_queries)
                            and len(pages_all)
                            == sum(int(ref["pages_count"]) for ref in region_segment_refs)
                            and len(products_all)
                            == sum(
                                int(ref["products_count"])
                                for ref in region_segment_refs
                            )
                        )
                    else:
                        expected_region_pages = (
                            len(bundle.enabled_queries)
                            * bundle.collection_plan.quality.expected_pages_per_query
                        )
                        region_manifest["complete"] = (
                            len(pages_all) == expected_region_pages
                            and len(products_all) == expected_region_pages * 100
                        )
                    if not region_manifest["complete"]:
                        raise CollectionPlanRunError(
                            f"region scope incomplete: {region.region_id}"
                        )
                    region_manifest["status"] = "success"
                    region_rows[region.region_id] = (products_all, pages_all)
            except Exception as exc:
                caught = exc

            if caught is None:
                self.input_integrity_gate()
                for region_manifest in manifest_regions:
                    products_all, pages_all = region_rows[region_manifest["region_id"]]
                    region_manifest["outputs"] = self._write_scope_outputs(
                        paths=paths,
                        region_id=region_manifest["region_id"],
                        product_rows=products_all,
                        page_rows=pages_all,
                        replace=self.resume,
                    )
                    self._replace(
                        paths.region_state_path(region_manifest["region_id"]),
                        _json_bytes(
                            {
                                "schema_version": REGION_STATE_SCHEMA_VERSION,
                                **region_manifest,
                            }
                        ),
                    )

            canonical_pages = sum(
                int(ref["pages_count"]) for ref in segment_refs
            )
            canonical_products = sum(
                int(
                    ref.get(
                        "products_count",
                        int(ref["pages_count"]) * 100,
                    )
                )
                for ref in segment_refs
            )
            verified_queries_by_region: dict[str, set[str]] = {}
            for ref in segment_refs:
                verified_queries_by_region.setdefault(
                    ref["region_id"],
                    set(),
                ).add(ref["query_id"])
            totals = {
                "regions_ok": sum(
                    len(query_ids) == len(bundle.enabled_queries)
                    for query_ids in verified_queries_by_region.values()
                ),
                "pages_ok": canonical_pages,
                "products_ok": canonical_products,
            }
            if bounded:
                totals.update({
                    "queries_ok": len(segment_refs),
                    "duplicate_product_positions": sum(
                    int(
                        ref.get("completion", {}).get(
                            "duplicate_product_positions",
                            0,
                        )
                    )
                    for ref in segment_refs
                ),
                })
            expected_regions = len(bundle.enabled_regions)
            collection_complete = (
                caught is None
                and totals["regions_ok"] == expected_regions
                and (
                    totals.get("queries_ok")
                    == expected_regions * len(bundle.enabled_queries)
                    if bounded
                    else True
                )
                and (
                    (
                        0 < totals["pages_ok"] <= planned_pages
                        and 0 < totals["products_ok"] <= planned_pages * 100
                    )
                    if bounded
                    else (
                        totals["pages_ok"] == planned_pages
                        and totals["products_ok"] == planned_pages * 100
                    )
                )
            )
            manifest = {
                "schema_version": RESUMABLE_MANIFEST_SCHEMA_VERSION,
                "run_id": self.run_id,
                "collection_scope": COLLECTION_SCOPE,
                "collection_plan_id": bundle.collection_plan.collection_plan_id,
                "query_pack_id": bundle.query_pack.query_pack_id,
                "query_pack_version": bundle.query_pack.version,
                "query_pack_sha256": bundle.query_pack_sha256,
                "collection_plan_sha256": bundle.collection_plan_sha256,
                "region_registry_sha256": bundle.region_registry_sha256,
                "effective_plan_sha256": effective_sha256,
                "transport_fingerprint": transport_fingerprint,
                "transport_attestation_transitions": (
                    attestation_transitions
                ),
                "effective_plan_snapshot_path": _relative(
                    paths.effective_plan_path,
                    paths.project_root,
                ),
                "publication_mode": "none",
                "sellers_mode": "disabled",
                "proxy_rotation_mode": "disabled",
                "started_at_utc": (
                    prior_manifest.get("started_at_utc")
                    if prior_manifest is not None
                    else self.started_at_utc
                ),
                "finished_at_utc": _utc_iso(self.now()),
                "deadline_utc": _utc_iso(self.deadline.deadline_utc),
                "status": (
                    "publication_pending"
                    if collection_complete
                    else "failed"
                ),
                "complete": False,
                "totals": totals,
                "endpoint_usage": endpoint_usage,
                "regions": manifest_regions,
                "resume": {
                    "resumed": self.resume,
                    "segments": segment_refs,
                    "verified_segments": len(segment_refs),
                    "discarded_segments": discarded_segments,
                    "failed_segment": failed_segment,
                    "maximum_repeated_pages": bundle.collection_plan.quality.expected_pages_per_query,
                },
                "error": (
                    None
                    if caught is None
                    else {
                        "error_class": caught.__class__.__name__,
                        "error_code": str(
                            getattr(caught, "code", "collection_plan_failed")
                        ).replace("\n", " ")[:100],
                    }
                ),
                "regional_latest": {
                    "status": (
                        "publication_pending"
                        if collection_complete
                        else "not_published"
                    )
                },
            }
            if bounded:
                manifest["capacity"] = {
                    "pages_max": planned_pages,
                    "products_max": planned_pages * 100,
                }
            self._replace(paths.manifest_path, _json_bytes(manifest), final_manifest=True)
            if caught is not None:
                raise caught
            if not collection_complete:
                raise CollectionPlanRunError("collection plan run is incomplete")
            try:
                self.input_integrity_gate()
                manifest["regional_latest"] = self._publish_regional_latest(
                    paths=paths,
                    bundle=bundle,
                    effective_plan_sha256=effective_sha256,
                    region_manifests=manifest_regions,
                )
            except Exception as exc:
                manifest["status"] = "publication_pending"
                manifest["complete"] = False
                manifest["error"] = {
                    "error_class": exc.__class__.__name__,
                    "error_code": "regional_latest_publication_pending",
                }
                self._replace(
                    paths.manifest_path,
                    _json_bytes(manifest),
                    final_manifest=True,
                )
                raise
            manifest["status"] = "success"
            manifest["complete"] = True
            manifest["error"] = None
            self.input_integrity_gate()
            self._replace(paths.manifest_path, _json_bytes(manifest), final_manifest=True)
            return manifest

    def run(self) -> dict[str, Any]:
        initial_bundle = self._load_bundle()
        self._configure_runtime_deadline(initial_bundle)
        self._validate_mode(initial_bundle)
        paths = ScopedPaths.build(
            project_root=self.config.project_root,
            collection_plan_id=initial_bundle.collection_plan.collection_plan_id,
            run_id=self.run_id,
        )
        if initial_bundle.collection_plan.depth > 500:
            return self._run_resumable(
                initial_bundle=initial_bundle,
                paths=paths,
            )
        if self.resume:
            raise CollectionPlanRunError(
                "resume is supported only for collection plans deeper than 500"
            )
        return self._run_legacy(initial_bundle=initial_bundle, paths=paths)

    def _run_legacy(
        self,
        *,
        initial_bundle: CollectionPlanBundle,
        paths: ScopedPaths,
    ) -> dict[str, Any]:
        with acquire_collection_plan_locks(
            paths=paths,
            stale_seconds=self.config.runtime.lock_stale_seconds,
            event_hook=self.lock_event_hook,
        ):
            self.deadline.ensure_active()
            bundle = self._load_bundle()
            self._validate_mode(bundle)
            if self._bundle_identity(bundle) != self._bundle_identity(initial_bundle):
                raise CollectionPlanRunError(
                    "collection plan sources changed during lock acquisition"
                )
            if paths.state_run_dir.exists():
                raise CollectionPlanRunError(
                    f"immutable scoped run state already exists: {paths.state_run_dir}"
                )
            register_query_pack_provenance(
                provenance_path=paths.provenance_path,
                query_pack=bundle.query_pack,
                project_root=paths.project_root,
            )
            egress_checks_expected = len(bundle.enabled_regions) + 1
            egress_checks_completed = 0
            egress_verification_status = "unverified"
            egress_constant: bool | None = None
            initial_egress = self._check_egress()
            egress_checks_completed += 1

            resolved: dict[str, ResolvedDestination] = {}
            for region in bundle.enabled_regions:
                self.deadline.ensure_active()
                dest_id = self.transport.resolve_destination(
                    region,
                    timeout_seconds=self.deadline.request_timeout(
                        self.config.runtime.http_timeout_seconds
                    ),
                )
                if not isinstance(dest_id, str) or not _DEST_RE.fullmatch(dest_id):
                    raise CollectionPlanRunError(
                        f"resolver returned invalid dest for {region.region_id}"
                    )
                resolved[region.region_id] = ResolvedDestination(
                    region_id=region.region_id,
                    dest_id_observed=dest_id,
                    dest_resolved_at_utc=_utc_iso(self.now()),
                )

            snapshot = build_effective_plan_snapshot(
                bundle,
                resolved_destinations=resolved,
                page_size=100,
                endpoint_policy=self.transport.endpoint_policy,
            )
            snapshot_bytes = canonical_effective_plan_bytes(snapshot)
            effective_sha256 = canonical_effective_plan_sha256(snapshot)
            self._write(paths.effective_plan_path, snapshot_bytes)

            manifest_regions = [
                {
                    "region_id": region.region_id,
                    "dest_id_observed": resolved[region.region_id].dest_id_observed,
                    "dest_resolved_at_utc": resolved[
                        region.region_id
                    ].dest_resolved_at_utc,
                    "dest_resolution_source": "wb_geo_xinfo",
                    "dest_resolution_status": "resolved_not_sent",
                    "status": "pending",
                    "pages_ok": 0,
                    "products_ok": 0,
                    "failed_endpoint_attempt": None,
                }
                for region in bundle.enabled_regions
            ]
            totals = {"regions_ok": 0, "pages_ok": 0, "products_ok": 0}
            endpoint_usage = {
                endpoint_id: {"attempts": 0, "pages_ok": 0}
                for endpoint_id in self.transport.endpoint_policy.endpoint_ids
            }
            error: dict[str, Any] | None = None
            caught: Exception | None = None

            def verify_egress() -> None:
                nonlocal egress_checks_completed
                nonlocal egress_constant
                nonlocal egress_verification_status
                try:
                    self._check_egress(initial_egress)
                except EgressIdentityChangedError:
                    egress_checks_completed += 1
                    egress_verification_status = "changed"
                    egress_constant = False
                    raise
                except CollectionPlanRunError:
                    egress_verification_status = "unverified"
                    egress_constant = None
                    raise
                egress_checks_completed += 1

            try:
                for region_index, region in enumerate(bundle.enabled_regions):
                    if region_index > 0:
                        verify_egress()
                    region_manifest = next(
                        item
                        for item in manifest_regions
                        if item["region_id"] == region.region_id
                    )
                    product_rows: list[dict[str, Any]] = []
                    page_rows: list[dict[str, Any]] = []
                    region_error: Exception | None = None
                    for query in bundle.enabled_queries:
                        for page in range(
                            1,
                            bundle.collection_plan.quality.expected_pages_per_query
                            + 1,
                        ):
                            task = self._task(
                                bundle=bundle,
                                query_id=query.query_id,
                                region_id=region.region_id,
                                page=page,
                            )
                            request = self._search_request(
                                task=task,
                                dest_id=resolved[region.region_id].dest_id_observed,
                            )
                            self.deadline.ensure_active()
                            try:
                                result = self.transport.search_ordered(
                                    request,
                                    timeout_seconds=self.deadline.request_timeout(
                                        self.config.runtime.http_timeout_seconds
                                    ),
                                )
                            except ScopedTransportError as exc:
                                (
                                    failed_attempts,
                                    failed_endpoint_id,
                                ) = self._sanitized_endpoint_error_evidence(exc)
                                for endpoint_id in failed_attempts:
                                    endpoint_usage[endpoint_id]["attempts"] += 1
                                region_manifest["failed_endpoint_attempt"] = {
                                    "query_id": task.query_id,
                                    "page": task.page,
                                    "endpoint_id": failed_endpoint_id or None,
                                    "attempted_endpoint_ids": list(
                                        failed_attempts
                                    ),
                                    "error_code": exc.code,
                                }
                                if (
                                    exc.request_sent
                                    and exc.dest_id_sent
                                    == resolved[region.region_id].dest_id_observed
                                ):
                                    region_manifest[
                                        "dest_resolution_status"
                                    ] = "resolved_and_sent"
                                region_error = exc
                                break
                            try:
                                if result.dest_id_sent != request.dest_id_observed:
                                    raise CollectionPlanRunError(
                                        "search destination evidence mismatch"
                                    )
                                if (
                                    result.endpoint_id
                                    not in self.transport.endpoint_policy.endpoint_ids
                                ):
                                    raise CollectionPlanRunError(
                                        "search endpoint evidence mismatch"
                                    )
                                attempted_endpoint_ids = (
                                    result.attempted_endpoint_ids
                                    or (result.endpoint_id,)
                                )
                                if (
                                    result.endpoint_id not in attempted_endpoint_ids
                                    or len(set(attempted_endpoint_ids))
                                    != len(attempted_endpoint_ids)
                                    or any(
                                        endpoint_id
                                        not in self.transport.endpoint_policy.endpoint_ids
                                        for endpoint_id in attempted_endpoint_ids
                                    )
                                ):
                                    raise CollectionPlanRunError(
                                        "search endpoint attempt evidence mismatch"
                                    )
                                for endpoint_id in attempted_endpoint_ids:
                                    endpoint_usage[endpoint_id]["attempts"] += 1
                                region_manifest[
                                    "dest_resolution_status"
                                ] = "resolved_and_sent"
                                raw_path = paths.raw_page_path(task)
                                self._write(raw_path, _json_bytes(result.payload))
                                try:
                                    products = _extract_products(result.payload)
                                    rows, page_row = self._rows_for_page(
                                        task=task,
                                        resolution=resolved[region.region_id],
                                        products=products,
                                        raw_path=raw_path,
                                        endpoint_id=result.endpoint_id,
                                    )
                                except CollectionPlanRunError as exc:
                                    raise ScopedTransportError(
                                        str(exc),
                                        request_sent=True,
                                        dest_id_sent=result.dest_id_sent,
                                        http_status=result.http_status,
                                        endpoint_id=result.endpoint_id,
                                        attempted_endpoint_ids=attempted_endpoint_ids,
                                    ) from exc
                                self._write(
                                    paths.checkpoint_path(task),
                                    _json_bytes(
                                        self._checkpoint_payload(
                                            task=task,
                                            resolution=resolved[region.region_id],
                                            raw_path=raw_path,
                                            endpoint_id=result.endpoint_id,
                                            attempted_endpoint_ids=attempted_endpoint_ids,
                                        )
                                    ),
                                )
                                endpoint_usage[result.endpoint_id][
                                    "pages_ok"
                                ] += 1
                                product_rows.extend(rows)
                                page_rows.append(page_row)
                                region_manifest["pages_ok"] += 1
                                region_manifest["products_ok"] += len(rows)
                                totals["pages_ok"] += 1
                                totals["products_ok"] += len(rows)
                                self._sleep_from_serp_config(
                                    "sleep_between_pages_ms"
                                )
                            except ScopedTransportError as exc:
                                (
                                    failed_attempts,
                                    failed_endpoint_id,
                                ) = self._sanitized_endpoint_error_evidence(exc)
                                region_manifest["failed_endpoint_attempt"] = {
                                    "query_id": task.query_id,
                                    "page": task.page,
                                    "endpoint_id": failed_endpoint_id or None,
                                    "attempted_endpoint_ids": list(
                                        failed_attempts
                                    ),
                                    "error_code": exc.code,
                                }
                                region_error = exc
                                break
                            except Exception as exc:
                                region_error = exc
                                break
                        if region_error is not None:
                            break
                        self._sleep_from_serp_config(
                            "sleep_between_queries_ms"
                        )

                    outputs = self._write_scope_outputs(
                        paths=paths,
                        region_id=region.region_id,
                        product_rows=product_rows,
                        page_rows=page_rows,
                    )
                    region_manifest["outputs"] = outputs
                    if region_error is not None:
                        region_manifest["status"] = "failed"
                        region_manifest["complete"] = False
                        self._write(
                            paths.region_state_path(region.region_id),
                            _json_bytes(
                                {
                                    "schema_version": REGION_STATE_SCHEMA_VERSION,
                                    **region_manifest,
                                }
                            ),
                        )
                        raise region_error

                    expected_pages = (
                        len(bundle.enabled_queries)
                        * bundle.collection_plan.quality.expected_pages_per_query
                    )
                    region_manifest["complete"] = (
                        region_manifest["pages_ok"] == expected_pages
                        and region_manifest["products_ok"] == expected_pages * 100
                    )
                    if not region_manifest["complete"]:
                        raise CollectionPlanRunError(
                            f"region scope incomplete: {region.region_id}"
                        )
                    region_manifest["status"] = "success"
                    totals["regions_ok"] += 1
                    self._write(
                        paths.region_state_path(region.region_id),
                        _json_bytes(
                            {
                                "schema_version": REGION_STATE_SCHEMA_VERSION,
                                **region_manifest,
                            }
                        ),
                    )
                verify_egress()
                if egress_checks_completed == egress_checks_expected:
                    egress_verification_status = "verified_constant"
                    egress_constant = True
            except Exception as exc:
                caught = exc
                error_code = str(
                    getattr(exc, "code", "collection_plan_failed")
                ).replace("\n", " ")[:100]
                error = {
                    "error_class": exc.__class__.__name__,
                    "error_code": error_code,
                    "error_message": error_code,
                }
                if isinstance(exc, ScopedTransportError):
                    (
                        failed_attempts,
                        failed_endpoint_id,
                    ) = self._sanitized_endpoint_error_evidence(exc)
                    error["endpoint_id"] = failed_endpoint_id or None
                    error["attempted_endpoint_ids"] = list(failed_attempts)

            expected_regions = len(bundle.enabled_regions)
            expected_pages = (
                expected_regions
                * len(bundle.enabled_queries)
                * bundle.collection_plan.quality.expected_pages_per_query
            )
            complete = (
                caught is None
                and totals["regions_ok"] == expected_regions
                and totals["pages_ok"] == expected_pages
                and totals["products_ok"] == expected_pages * 100
                and egress_verification_status == "verified_constant"
                and egress_checks_completed == egress_checks_expected
                and all(
                    item["dest_resolution_status"] == "resolved_and_sent"
                    for item in manifest_regions
                )
            )
            manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "run_id": self.run_id,
                "collection_scope": COLLECTION_SCOPE,
                "collection_plan_id": bundle.collection_plan.collection_plan_id,
                "query_pack_id": bundle.query_pack.query_pack_id,
                "query_pack_version": bundle.query_pack.version,
                "query_pack_sha256": bundle.query_pack_sha256,
                "collection_plan_sha256": bundle.collection_plan_sha256,
                "region_registry_sha256": bundle.region_registry_sha256,
                "effective_plan_sha256": effective_sha256,
                "effective_plan_snapshot_path": _relative(
                    paths.effective_plan_path,
                    paths.project_root,
                ),
                "publication_mode": "none",
                "sellers_mode": "disabled",
                "proxy_rotation_mode": "disabled",
                "started_at_utc": self.started_at_utc,
                "finished_at_utc": _utc_iso(self.now()),
                "deadline_utc": _utc_iso(self.deadline.deadline_utc),
                "egress": {
                    "masked": _mask_egress(initial_egress),
                    "ephemeral_sha256": _egress_hash(
                        initial_egress,
                        salt=self.egress_hash_salt,
                    ),
                    "verification_status": egress_verification_status,
                    "constant": egress_constant,
                    "checks_completed": egress_checks_completed,
                    "checks_expected": egress_checks_expected,
                },
                "status": "success" if complete else "failed",
                "complete": complete,
                "totals": totals,
                "endpoint_usage": endpoint_usage,
                "regions": manifest_regions,
                "error": error,
            }
            self._write(
                paths.manifest_path,
                _json_bytes(manifest),
                final_manifest=True,
            )
            if caught is not None:
                raise caught
            if not complete:
                raise CollectionPlanRunError("collection plan run is incomplete")
            return manifest


def run_collection_plan(
    *,
    config: AppConfig,
    plan_path: Path,
    no_publish: bool,
    transport: ScopedTransport | None = None,
    run_id: str | None = None,
    resume_run_id: str | None = None,
    now: Callable[[], datetime] = _default_now,
    lock_event_hook: LockEventHook | None = None,
    write_event_hook: WriteEventHook | None = None,
    egress_hash_salt: bytes | None = None,
    sleeper: Callable[[float], None] = time_module.sleep,
    absolute_deadline_utc: datetime | None = None,
    input_integrity_gate: Callable[[], None] | None = None,
    matrix_continuation: bool = False,
) -> dict[str, Any]:
    owned_transport = False
    active_transport = transport
    if active_transport is None:
        bundle = load_collection_plan_bundle(
            project_root=config.project_root,
            plan_path=plan_path,
            region_registry_path=config.project_root / "config/wb/regions.json",
        )
        if not no_publish:
            raise CollectionPlanRunError("--no-publish is mandatory")
        if not bundle.collection_plan.enabled:
            raise CollectionPlanRunError("collection plan is disabled")
        active_transport = RequestsScopedTransport.from_config(config)
        owned_transport = True
    try:
        return CollectionPlanRunner(
            config=config,
            plan_path=plan_path,
            transport=active_transport,
            no_publish=no_publish,
            run_id=run_id,
            resume_run_id=resume_run_id,
            now=now,
            lock_event_hook=lock_event_hook,
            write_event_hook=write_event_hook,
            egress_hash_salt=egress_hash_salt,
            sleeper=sleeper,
            absolute_deadline_utc=absolute_deadline_utc,
            input_integrity_gate=input_integrity_gate,
            matrix_continuation=matrix_continuation,
        ).run()
    finally:
        if owned_transport:
            active_transport.close()


def validate_resumable_collection_state(
    *,
    config: AppConfig,
    plan_path: Path,
    run_id: str,
    transport: ScopedTransport | None = None,
) -> bool:
    active_transport: ScopedTransport | None = transport
    owned_transport = transport is None
    try:
        if active_transport is None:
            active_transport = RequestsScopedTransport.from_config(config)
        runner = CollectionPlanRunner(
            config=config,
            plan_path=plan_path,
            transport=active_transport,
            no_publish=True,
            resume_run_id=run_id,
        )
        bundle = runner._load_bundle()
        runner._validate_mode(bundle)
        paths = ScopedPaths.build(
            project_root=config.project_root,
            collection_plan_id=bundle.collection_plan.collection_plan_id,
            run_id=runner.run_id,
        )
        manifest = _json_object_from_bytes(
            _read_regular_bytes(
                paths.manifest_path,
                project_root=config.project_root,
            ),
            field="resume manifest",
        )
        snapshot = _json_object_from_bytes(
            _read_regular_bytes(
                paths.effective_plan_path,
                project_root=config.project_root,
            ),
            field="effective plan",
        )
        if (
            manifest.get("schema_version")
            != RESUMABLE_MANIFEST_SCHEMA_VERSION
            or manifest.get("run_id") != run_id
            or manifest.get("collection_plan_id")
            != bundle.collection_plan.collection_plan_id
            or manifest.get("complete") is True
            or manifest.get("status") == "success"
        ):
            return False
        effective_sha256 = canonical_effective_plan_sha256(snapshot)
        if manifest.get("effective_plan_sha256") != effective_sha256:
            return False
        current_fingerprint = runner._transport_fingerprint()
        manifest_transition = _resume_attestation_transition(
            stored=manifest.get("transport_fingerprint"),
            current=current_fingerprint,
            project_root=config.project_root,
            run_id=run_id,
            collection_plan_id=bundle.collection_plan.collection_plan_id,
            effective_plan_sha256=effective_sha256,
        )
        snapshot_transition = _resume_attestation_transition(
            stored=snapshot.get("transport_fingerprint"),
            current=current_fingerprint,
            project_root=config.project_root,
            run_id=run_id,
            collection_plan_id=bundle.collection_plan.collection_plan_id,
            effective_plan_sha256=effective_sha256,
        )
        _resume_attestation_history(
            value=manifest.get("transport_attestation_transitions"),
            expected=(manifest_transition, snapshot_transition),
            allow_initial_missing=manifest_transition is not None,
        )
        resume = manifest.get("resume")
        refs = resume.get("segments") if isinstance(resume, dict) else None
        if not isinstance(refs, list) or not refs:
            return False
        verified, verified_ids = runner._verified_segments(
            paths=paths,
            bundle=bundle,
            effective_plan_sha256=effective_sha256,
            expected_refs=tuple(refs),
            promote=False,
        )
        discarded = runner._validate_discarded_segments(
            value=resume.get("discarded_segments"),
            bundle=bundle,
            verified_segment_ids=verified_ids,
        )
        failed_segment = resume.get("failed_segment")
        if failed_segment is not None and (
            not discarded or failed_segment != discarded[-1]
        ):
            return False
        expected_scopes = len(bundle.enabled_regions) * len(
            bundle.enabled_queries
        )
        return (
            len(verified) < expected_scopes
            or manifest.get("status") == "publication_pending"
        )
    except (
        CollectionPlanRunError,
        CollectionPlanValidationError,
        CriticalPipelineError,
        OSError,
        ValueError,
        AttributeError,
    ):
        return False
    finally:
        if owned_transport and active_transport is not None:
            active_transport.close()
