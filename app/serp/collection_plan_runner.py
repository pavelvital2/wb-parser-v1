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
from urllib.parse import parse_qs, quote
from zoneinfo import ZoneInfo

import requests

from app.common.config import AppConfig
from app.common.exceptions import CriticalPipelineError
from app.common.proxy_required import (
    assert_requests_session_proxy,
    build_requests_session,
    require_marketplace_proxy,
)
from app.common.run_lock import acquire_advisory_lock, acquire_run_lock
from app.serp.collection_plan import (
    CollectionPlanBundle,
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
REGIONAL_LATEST_SCHEMA_VERSION = "wb_regional_latest_v1"
REGIONAL_LATEST_REGION_SCHEMA_VERSION = "wb_regional_latest_region_v1"
COLLECTION_SCOPE = "regional"
GEO_RESOLVER_URL = "https://user-geo-data.wildberries.ru/get-geo-info"
EGRESS_CHECK_URL = "https://api.ipify.org"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
NIGHTLY_PREFLIGHT_CUTOFF = time(23, 45)
NIGHTLY_COLLECTION_START = time(0, 15)
MINIMUM_START_WINDOW_SECONDS = 300
FINALIZATION_RESERVE_SECONDS = 5
NIGHTLY_SAFETY_RESERVE_SECONDS = 900
ESTIMATED_REQUEST_OVERHEAD_SECONDS = 1.0

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RUN_ID_RE = re.compile(r"^[0-9]{8}_[0-9]{6}Z$")
_DEST_RE = re.compile(r"^[+-]?[0-9]{1,16}$")
_PRODUCT_ID_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

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

    def remaining_seconds(self) -> float:
        return (self.deadline_utc - self.now().astimezone(timezone.utc)).total_seconds()

    def ensure_start_window(self) -> None:
        if self.remaining_seconds() < MINIMUM_START_WINDOW_SECONDS:
            raise CollectionPlanRunError(
                "collection plan cannot start within 5 minutes of 23:45 MSK"
            )

    def ensure_active(self) -> None:
        if self.remaining_seconds() <= FINALIZATION_RESERVE_SECONDS:
            raise CollectionPlanRunError(
                "collection plan deadline reached before 23:45 MSK"
            )

    def request_timeout(self, configured_timeout: float) -> float:
        self.ensure_active()
        available = self.remaining_seconds() - FINALIZATION_RESERVE_SECONDS
        return max(0.1, min(float(configured_timeout), available))

    def ensure_estimated_window(self, estimated_seconds: float) -> None:
        if estimated_seconds < 0:
            raise CollectionPlanRunError("estimated runtime must not be negative")
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


def _ensure_scoped_parent(path: Path, *, project_root: Path) -> None:
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
        current.mkdir(exist_ok=True)


def _write_new_bytes(
    path: Path,
    payload: bytes,
    *,
    project_root: Path,
    event_hook: WriteEventHook | None = None,
) -> None:
    _ensure_scoped_parent(path, project_root=project_root)
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
) -> None:
    _ensure_scoped_parent(path, project_root=project_root)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise CollectionPlanRunError(f"atomic target must be a regular file: {path}")
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
        os.replace(temp_path, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        if event_hook is not None:
            event_hook("directory_fsynced", path)
    finally:
        if fd >= 0:
            os.close(fd)
        temp_path.unlink(missing_ok=True)


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
        try:
            response = self.egress_session.get(
                self.egress_check_url,
                headers={
                    "accept": "text/plain",
                    "user-agent": "parser-wb-egress-check/1",
                },
                timeout=min(self.timeout_seconds, timeout_seconds),
            )
        except requests.RequestException as exc:
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
                timeout=min(self.timeout_seconds, timeout_seconds),
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
                timeout=min(self.timeout_seconds, timeout_seconds),
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
                timeout=min(self.timeout_seconds, timeout_seconds),
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
                    timeout=min(self.timeout_seconds, timeout_seconds),
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
                _extract_products(payload)
            except CollectionPlanRunError as exc:
                if str(exc) != "retryable_payload_anomaly_nested_promo":
                    raise ScopedTransportError(
                        str(exc),
                        request_sent=True,
                        dest_id_sent=request.dest_id_observed,
                        http_status=200,
                        endpoint_id=endpoint_id,
                        attempted_endpoint_ids=tuple(attempted),
                    ) from exc
                last_payload_anomaly = ScopedTransportError(
                    "search_payload_anomaly_nested_promo",
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
            raise last_payload_anomaly
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


def _mask_egress(value: str) -> str:
    parsed = ipaddress.ip_address(value)
    if parsed.version == 4:
        first, second, *_rest = parsed.exploded.split(".")
        return f"{first}.{second}.x.x"
    first, second, *_rest = parsed.exploded.split(":")
    return f"{first}:{second}::x"


def _egress_hash(value: str, *, salt: bytes) -> str:
    return hashlib.sha256(salt + value.encode("ascii")).hexdigest()


PRODUCT_FIELDS = (
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
    "product_name",
    "brand",
    "supplier_id",
    "rating",
    "feedbacks",
    "total_quantity",
    "raw_file",
)

PAGE_FIELDS = (
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
        self.run_id = _safe_run_id(
            resume_run_id or run_id or _default_run_id(started)
        )
        self.started_at_utc = _utc_iso(started)
        self.deadline = DeadlineGuard.for_current_day(now=now)
        self.lock_event_hook = lock_event_hook
        self.write_event_hook = write_event_hook
        self.egress_hash_salt = egress_hash_salt or secrets.token_bytes(32)
        self.sleeper = sleeper

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
            normalized = str(ipaddress.ip_address(value))
        except ValueError as exc:
            raise CollectionPlanRunError("egress identity is not an IP address") from exc
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
                <= FINALIZATION_RESERVE_SECONDS
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
            if self.deadline.remaining_seconds() <= FINALIZATION_RESERVE_SECONDS:
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
        )

    def _write_or_verify(self, path: Path, payload: bytes) -> None:
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
        pending_queries = min(
            len(bundle.enabled_queries) * len(bundle.enabled_regions),
            pending_pages,
        )
        return (
            pending_pages * request_seconds
            + pending_queries * query_sleep
            + NIGHTLY_SAFETY_RESERVE_SECONDS
        )

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
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if len(products) != task.page_size:
            raise CollectionPlanRunError(
                f"search_products_short expected={task.page_size} actual={len(products)}"
            )
        seen: set[str] = set()
        rows: list[dict[str, Any]] = []
        raw_file = _relative(raw_path, self.config.project_root)
        for index, product in enumerate(products, start=1):
            product_id = _normalize_product_id(product)
            if product_id in seen:
                raise CollectionPlanRunError("search_product_duplicate")
            seen.add(product_id)
            rows.append(
                {
                    "run_id": self.run_id,
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
                    "product_name": product.get("name") or "",
                    "brand": product.get("brand") or "",
                    "supplier_id": product.get("supplierId") or "",
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
            payload.update(
                {
                    "schema_version": RESUMABLE_CHECKPOINT_SCHEMA_VERSION,
                    "query": task.query,
                    "page_size": task.page_size,
                    "depth": task.depth,
                    "raw_sha256": raw_sha256,
                    "products_count": task.page_size,
                    "query_pack_sha256": bundle.query_pack_sha256,
                    "collection_plan_sha256": bundle.collection_plan_sha256,
                    "region_registry_sha256": bundle.region_registry_sha256,
                    "effective_plan_sha256": effective_plan_sha256,
                    "segment_id": segment_id,
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
        expected = {
            "schema_version": RESUMABLE_CHECKPOINT_SCHEMA_VERSION,
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
            "products_count": task.page_size,
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
        rows, page_row = self._rows_for_page(
            task=task,
            resolution=resolution,
            products=_extract_products(payload),
            raw_path=raw_path,
            endpoint_id=endpoint_id,
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
            self._write_or_verify(target, payload_bytes)
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
        self._replace(paths.latest_path, _json_bytes(latest))
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
    ) -> tuple[list[dict[str, Any]], set[str]]:
        records: list[dict[str, Any]] = []
        verified_ids: set[str] = set()
        scopes: set[tuple[str, str]] = set()
        if not expected_refs:
            return records, verified_ids
        for expected_ref in expected_refs:
            segment_id = expected_ref.get("segment_id")
            if not isinstance(segment_id, str):
                raise CollectionPlanRunError("segment reference identity is invalid")
            path = paths.segment_path(segment_id)
            payload_bytes = _read_regular_bytes(
                path,
                project_root=self.config.project_root,
            )
            if expected_ref.get("sha256") != _sha256_bytes(payload_bytes):
                raise CollectionPlanRunError("segment reference checksum mismatch")
            segment = _json_object_from_bytes(payload_bytes, field="segment")
            identity = {
                "schema_version": SEGMENT_SCHEMA_VERSION,
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
                not isinstance(segment_id, str)
                or path != paths.segment_path(segment_id)
                or not isinstance(region_id, str)
                or not isinstance(query_id, str)
            ):
                raise CollectionPlanRunError("segment identity is invalid")
            scope = (region_id, query_id)
            if scope in scopes:
                raise CollectionPlanRunError("multiple verified segments for one query scope")
            egress = segment.get("egress")
            pages = segment.get("pages")
            if (
                not isinstance(egress, dict)
                or egress.get("verification_status") != "verified_constant"
                or egress.get("constant") is not True
                or egress.get("checks_completed") != 2
                or not isinstance(pages, list)
                or len(pages)
                != bundle.collection_plan.quality.expected_pages_per_query
            ):
                raise CollectionPlanRunError("segment is not complete and verified")
            scopes.add(scope)
            verified_ids.add(segment_id)
            records.append(
                {
                    "segment_id": segment_id,
                    "region_id": region_id,
                    "query_id": query_id,
                    "path": _relative(path, paths.project_root),
                    "sha256": _sha256_bytes(payload_bytes),
                    "egress": egress,
                    "pages_count": len(pages),
                }
            )
            self._promote_verified_segment(paths=paths, segment=segment)
        return records, verified_ids

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
            register_query_pack_provenance(
                provenance_path=paths.provenance_path,
                query_pack=bundle.query_pack,
            )

            prior_manifest: dict[str, Any] | None = None
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

            confirmed_pages = sum(
                int(ref.get("pages_count", 0)) for ref in segment_refs
            )
            pending_pages = planned_pages - confirmed_pages
            if pending_pages < 0:
                raise CollectionPlanRunError("resume has more pages than the plan")
            self.deadline.ensure_estimated_window(
                self._estimated_remaining_seconds(
                    bundle=bundle,
                    pending_pages=pending_pages,
                )
            )

            resolved_now: dict[str, ResolvedDestination] = {}
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
                for region_id, current in resolved_now.items():
                    stored = stored_regions.get(region_id)
                    if (
                        not isinstance(stored, dict)
                        or stored.get("dest_id_observed") != current.dest_id_observed
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
                        tasks = [
                            self._task(
                                bundle=bundle,
                                query_id=query.query_id,
                                region_id=region.region_id,
                                page=page,
                            )
                            for page in range(
                                1,
                                bundle.collection_plan.quality.expected_pages_per_query + 1,
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
                        try:
                            for task in tasks:
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
                                if result.dest_id_sent != request.dest_id_observed:
                                    raise CollectionPlanRunError(
                                        "search destination evidence mismatch"
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
                                pending_raw = paths.segment_pending_raw_path(
                                    segment_id, task
                                )
                                raw_bytes = _json_bytes(result.payload)
                                self._write(pending_raw, raw_bytes)
                                rows, page_row = self._rows_for_page(
                                    task=task,
                                    resolution=resolved[region.region_id],
                                    products=_extract_products(result.payload),
                                    raw_path=paths.raw_page_path(task),
                                    endpoint_id=result.endpoint_id,
                                )
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
                                )
                                checkpoint_bytes = _json_bytes(checkpoint)
                                pending_checkpoint = (
                                    paths.segment_pending_checkpoint_path(
                                        segment_id, task
                                    )
                                )
                                self._write(pending_checkpoint, checkpoint_bytes)
                                for endpoint_id in attempts:
                                    segment_usage[endpoint_id]["attempts"] += 1
                                segment_usage[result.endpoint_id]["pages_ok"] += 1
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
                                "pages_written": len(page_refs),
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
                                failed_segment.update(
                                    {
                                        "endpoint_id": None,
                                        "attempted_endpoint_ids": [],
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
                            "schema_version": SEGMENT_SCHEMA_VERSION,
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
                        segment_bytes = _json_bytes(segment_payload)
                        self._write(paths.segment_path(segment_id), segment_bytes)
                        segment_refs.append(
                            {
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
                        )
                        write_progress_manifest()
                        self._promote_verified_segment(
                            paths=paths,
                            segment=segment_payload,
                        )
                        verified_segment_ids.add(segment_id)
                        for endpoint_id, usage in segment_usage.items():
                            endpoint_usage[endpoint_id]["attempts"] += usage["attempts"]
                            endpoint_usage[endpoint_id]["pages_ok"] += usage["pages_ok"]
                        for task in tasks:
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
                for region_manifest in manifest_regions:
                    products_all, pages_all = region_rows[region_manifest["region_id"]]
                    region_manifest["outputs"] = self._write_scope_outputs(
                        paths=paths,
                        region_id=region_manifest["region_id"],
                        product_rows=products_all,
                        page_rows=pages_all,
                        replace=self.resume,
                    )
                    totals["regions_ok"] += 1
                    totals["pages_ok"] += len(pages_all)
                    totals["products_ok"] += len(products_all)
                    self._replace(
                        paths.region_state_path(region_manifest["region_id"]),
                        _json_bytes(
                            {
                                "schema_version": REGION_STATE_SCHEMA_VERSION,
                                **region_manifest,
                            }
                        ),
                    )

            expected_regions = len(bundle.enabled_regions)
            complete = (
                caught is None
                and totals["regions_ok"] == expected_regions
                and totals["pages_ok"] == planned_pages
                and totals["products_ok"] == planned_pages * 100
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
                "status": "success" if complete else "failed",
                "complete": complete,
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
                "regional_latest": {"status": "not_published"},
            }
            self._replace(paths.manifest_path, _json_bytes(manifest), final_manifest=True)
            if caught is not None:
                raise caught
            if not complete:
                raise CollectionPlanRunError("collection plan run is incomplete")
            try:
                manifest["regional_latest"] = self._publish_regional_latest(
                    paths=paths,
                    bundle=bundle,
                    effective_plan_sha256=effective_sha256,
                    region_manifests=manifest_regions,
                )
            except Exception as exc:
                manifest["status"] = "failed"
                manifest["complete"] = False
                manifest["error"] = {
                    "error_class": exc.__class__.__name__,
                    "error_code": "regional_latest_publish_failed",
                }
                self._replace(
                    paths.manifest_path,
                    _json_bytes(manifest),
                    final_manifest=True,
                )
                raise
            self._replace(paths.manifest_path, _json_bytes(manifest), final_manifest=True)
            return manifest

    def run(self) -> dict[str, Any]:
        initial_bundle = self._load_bundle()
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
        ).run()
    finally:
        if owned_transport:
            active_transport.close()
