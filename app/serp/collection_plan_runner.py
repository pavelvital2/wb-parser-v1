from __future__ import annotations

import csv
import hashlib
import io
import ipaddress
import json
import os
import re
import secrets
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
REGION_STATE_SCHEMA_VERSION = "wb_collection_plan_region_v1"
CHECKPOINT_SCHEMA_VERSION = "wb_collection_plan_checkpoint_v1"
COLLECTION_SCOPE = "regional"
GEO_RESOLVER_URL = "https://user-geo-data.wildberries.ru/get-geo-info"
EGRESS_CHECK_URL = "https://api.ipify.org"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
NIGHTLY_PREFLIGHT_CUTOFF = time(23, 45)
MINIMUM_START_WINDOW_SECONDS = 300
FINALIZATION_RESERVE_SECONDS = 5

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RUN_ID_RE = re.compile(r"^[0-9]{8}_[0-9]{6}Z$")
_DEST_RE = re.compile(r"^[+-]?[0-9]{1,16}$")
_PRODUCT_ID_RE = re.compile(r"^[1-9][0-9]{0,19}$")

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
    ) -> None:
        super().__init__(code)
        self.code = code
        self.request_sent = request_sent
        self.dest_id_sent = dest_id_sent
        self.http_status = http_status


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
        resolver_url: str = GEO_RESOLVER_URL,
        egress_check_url: str = EGRESS_CHECK_URL,
        egress_session: requests.Session | None = None,
    ) -> None:
        if not endpoint_urls:
            raise CollectionPlanRunError("SERP endpoint list is empty")
        self.session = session
        self.request_params = dict(request_params)
        self.endpoint_urls = endpoint_urls
        self.timeout_seconds = float(timeout_seconds)
        self.referer_base = referer_base
        self.resolver_url = resolver_url
        self.egress_check_url = egress_check_url
        self.egress_session = egress_session or requests.Session()
        endpoint_ids = tuple(
            "primary" if index == 0 else f"fallback-{index}"
            for index in range(len(endpoint_urls))
        )
        self.endpoint_policy = EffectiveEndpointPolicy(
            selection_mode="ordered_fallbacks",
            endpoint_ids=endpoint_ids,
            pinned_endpoint_id=endpoint_ids[0],
        )

    @classmethod
    def from_config(cls, config: AppConfig) -> "RequestsScopedTransport":
        serp = config.raw.get("serp", {})
        session = requests.Session()
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
        session.headers.update(headers)

        proxy_env = str(serp.get("proxy_url_env") or "PARSER_WB_PROXY_URL")
        proxy_url = os.getenv(proxy_env, "").strip() or str(
            serp.get("proxy_url") or ""
        ).strip()
        if proxy_url:
            session.proxies.update({"http": proxy_url, "https": proxy_url})
        egress_session = requests.Session()
        if proxy_url:
            egress_session.proxies.update(
                {"http": proxy_url, "https": proxy_url}
            )

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
            egress_session=egress_session,
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
                timeout=min(self.timeout_seconds, timeout_seconds),
            )
        except requests.RequestException as exc:
            raise ScopedTransportError("resolver_network_error") from exc
        if response.status_code != 200:
            raise ScopedTransportError(
                f"resolver_http_{response.status_code}",
                http_status=response.status_code,
            )
        return self._extract_dest(self._response_json(response, code="resolver"))

    def search(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        if request.endpoint_id != self.endpoint_policy.pinned_endpoint_id:
            raise ScopedTransportError("search_endpoint_not_pinned")
        endpoint_url = self.endpoint_urls[0]
        try:
            response = self.session.get(
                endpoint_url,
                params=dict(request.params),
                headers={"referer": f"{self.referer_base}{quote(request.task.query)}"},
                timeout=min(self.timeout_seconds, timeout_seconds),
            )
        except requests.RequestException as exc:
            raise ScopedTransportError(
                "search_network_error",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
            ) from exc
        if response.status_code != 200:
            raise ScopedTransportError(
                f"search_http_{response.status_code}",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=response.status_code,
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
        now: Callable[[], datetime] = _default_now,
        lock_event_hook: LockEventHook | None = None,
        write_event_hook: WriteEventHook | None = None,
        egress_hash_salt: bytes | None = None,
    ) -> None:
        self.config = config
        self.plan_path = plan_path
        self.transport = transport
        self.no_publish = no_publish
        self.now = now
        started = now()
        self.run_id = _safe_run_id(run_id or _default_run_id(started))
        self.started_at_utc = _utc_iso(started)
        self.deadline = DeadlineGuard.for_current_day(now=now)
        self.lock_event_hook = lock_event_hook
        self.write_event_hook = write_event_hook
        self.egress_hash_salt = egress_hash_salt or secrets.token_bytes(32)

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
        if plan.depth != 100:
            raise CollectionPlanRunError("Stage 2 accepts depth=100 only")

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

    def _task(
        self,
        *,
        bundle: CollectionPlanBundle,
        query_id: str,
        region_id: str,
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
            page=1,
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
                    "position_on_page": index,
                    "absolute_position": index,
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
    ) -> dict[str, str]:
        outputs: dict[str, str] = {}
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
            self._write(raw_products, product_bytes)
            self._write(staging_products, product_bytes)
            self._write(mart_products, product_bytes)
            self._write(pages_index, _csv_bytes(page_rows, PAGE_FIELDS))
            outputs = {
                "raw_products_path": _relative(raw_products, paths.project_root),
                "staging_products_path": _relative(
                    staging_products,
                    paths.project_root,
                ),
                "mart_products_path": _relative(mart_products, paths.project_root),
                "pages_index_path": _relative(pages_index, paths.project_root),
            }
        return outputs

    def _checkpoint_payload(
        self,
        *,
        task: ScopedTask,
        resolution: ResolvedDestination,
        raw_path: Path,
    ) -> dict[str, Any]:
        return {
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
            "raw_file": _relative(raw_path, self.config.project_root),
        }

    def run(self) -> dict[str, Any]:
        initial_bundle = self._load_bundle()
        self._validate_mode(initial_bundle)
        paths = ScopedPaths.build(
            project_root=self.config.project_root,
            collection_plan_id=initial_bundle.collection_plan.collection_plan_id,
            run_id=self.run_id,
        )

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
                }
                for region in bundle.enabled_regions
            ]
            totals = {"regions_ok": 0, "pages_ok": 0, "products_ok": 0}
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
                        task = self._task(
                            bundle=bundle,
                            query_id=query.query_id,
                            region_id=region.region_id,
                        )
                        request = self._search_request(
                            task=task,
                            dest_id=resolved[region.region_id].dest_id_observed,
                        )
                        self.deadline.ensure_active()
                        try:
                            result = self.transport.search(
                                request,
                                timeout_seconds=self.deadline.request_timeout(
                                    self.config.runtime.http_timeout_seconds
                                ),
                            )
                        except ScopedTransportError as exc:
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
                                != self.transport.endpoint_policy.pinned_endpoint_id
                            ):
                                raise CollectionPlanRunError(
                                    "search endpoint evidence mismatch"
                                )
                            region_manifest[
                                "dest_resolution_status"
                            ] = "resolved_and_sent"
                            raw_path = paths.raw_page_path(task)
                            self._write(raw_path, _json_bytes(result.payload))
                            products = _extract_products(result.payload)
                            rows, page_row = self._rows_for_page(
                                task=task,
                                resolution=resolved[region.region_id],
                                products=products,
                                raw_path=raw_path,
                                endpoint_id=result.endpoint_id,
                            )
                            self._write(
                                paths.checkpoint_path(task),
                                _json_bytes(
                                    self._checkpoint_payload(
                                        task=task,
                                        resolution=resolved[region.region_id],
                                        raw_path=raw_path,
                                    )
                                ),
                            )
                            product_rows.extend(rows)
                            page_rows.append(page_row)
                            region_manifest["pages_ok"] += 1
                            region_manifest["products_ok"] += len(rows)
                            totals["pages_ok"] += 1
                            totals["products_ok"] += len(rows)
                        except Exception as exc:
                            region_error = exc
                            break

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

                    expected_pages = len(bundle.enabled_queries)
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

            expected_regions = len(bundle.enabled_regions)
            expected_pages = expected_regions * len(bundle.enabled_queries)
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
    now: Callable[[], datetime] = _default_now,
    lock_event_hook: LockEventHook | None = None,
    write_event_hook: WriteEventHook | None = None,
    egress_hash_salt: bytes | None = None,
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
            now=now,
            lock_event_hook=lock_event_hook,
            write_event_hook=write_event_hook,
            egress_hash_salt=egress_hash_salt,
        ).run()
    finally:
        if owned_transport:
            active_transport.close()
