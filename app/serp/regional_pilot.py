from __future__ import annotations

import hashlib
import os
import random
import re
import stat
import subprocess
import time as time_module
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from app.common.config import AppConfig
from app.common.proxy_required import (
    assert_requests_session_proxy,
    require_marketplace_proxy,
    require_runtime_env_loaded,
)
from app.serp.collection_plan import (
    CollectionPlanBundle,
    ResolvedDestination,
    build_effective_plan_snapshot,
    canonical_effective_plan_bytes,
    canonical_effective_plan_sha256,
    load_collection_plan_bundle,
    register_query_pack_provenance,
)
from app.serp.collection_plan_runner import (
    COLLECTION_SCOPE,
    FINALIZATION_RESERVE_SECONDS,
    REGION_STATE_SCHEMA_VERSION,
    CollectionPlanRunError,
    CollectionPlanRunner,
    EgressIdentityChangedError,
    EndpointProbeResult,
    RequestsScopedTransport,
    ScopedPaths,
    ScopedSearchRequest,
    ScopedSearchResult,
    ScopedTransport,
    ScopedTransportError,
    _egress_hash,
    _extract_products,
    _json_bytes,
    _mask_egress,
    _normalize_product_id,
    _relative,
    _utc_iso,
    acquire_collection_plan_locks,
)


PILOT_MANIFEST_SCHEMA_VERSION = "wb_regional_pilot_manifest_v1"
ENDPOINT_EVIDENCE_SCHEMA_VERSION = "wb_endpoint_preflight_v1"
CONTROL_SCHEMA_VERSION = "wb_regional_pilot_control_v1"
COMPARISON_SCHEMA_VERSION = "wb_regional_comparison_v1"
REQUEST_BUDGET_SCHEMA_VERSION = "wb_regional_request_budget_v1"
PROTECTED_EVIDENCE_SCHEMA_VERSION = "wb_protected_evidence_v1"
CONTOUR_EVIDENCE_SCHEMA_VERSION = "wb_pilot_contour_v1"
PACING_EVIDENCE_SCHEMA_VERSION = "wb_pilot_search_pacing_v1"
RATE_LIMIT_EVIDENCE_SCHEMA_VERSION = "wb_pilot_rate_limit_v1"
CONTROL_JACCARD_THRESHOLD = 0.95
PILOT_QUERY_IDS = ("shevron", "shevrony", "shevron-na-lipuchke")
PILOT_REGION_IDS = ("moscow", "rostov-on-don")
PILOT_EGRESS_CHECKS = 4
PILOT_MINIMUM_START_WINDOW_SECONDS = 20 * 60
PILOT_HARD_RUNTIME_SECONDS = 18 * 60
PILOT_SEARCH_MIN_INTERVAL_SECONDS = 17.0
PILOT_SEARCH_MAX_JITTER_SECONDS = 2.0
PILOT_DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS = 45
_SAFE_ERROR_CODE_RE = re.compile(r"^[a-z0-9_]{1,100}$")

PROTECTED_RELATIVE_PATHS = (
    "config/runtime.env",
    "config/wb_cookie.txt",
    "config/wb_request_headers.json",
    "exports/queries.txt",
    "exports/products_for_sellers.csv",
    "state/run_reports/latest.json",
    "state/wb_warehouse/latest.json",
    "data/raw/serp/latest/pages_raw_index.csv",
    "data/raw/serp/latest/products_raw.csv",
    "data/staging/serp/latest/products_staging.csv",
    "data/marts/serp/latest/products_daily.csv",
    "data/marts/sellers/latest/sellers_daily.csv",
    "data/marts/sellers/latest/seller_query_product_bridge.csv",
    "data/warehouse/wb/wb.duckdb",
    "data/warehouse/wb/manifests/latest.json",
)
PILOT_SOURCE_NAMES = (
    "config_file",
    "collection_plan",
    "region_registry",
    "query_pack",
)


@dataclass(slots=True)
class PilotRequestBudget:
    limits: dict[str, int] = field(
        default_factory=lambda: {
            "geo": 2,
            "endpoint_probe": 2,
            "regional_search": 5,
            "repeat_search": 1,
        }
    )
    total_limit: int = 10
    used: dict[str, int] = field(init=False)

    def __post_init__(self) -> None:
        expected = {"geo", "endpoint_probe", "regional_search", "repeat_search"}
        if set(self.limits) != expected:
            raise CollectionPlanRunError("pilot request budget categories are invalid")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in self.limits.values()
        ):
            raise CollectionPlanRunError("pilot request budget limits are invalid")
        if (
            isinstance(self.total_limit, bool)
            or not isinstance(self.total_limit, int)
            or self.total_limit < 0
        ):
            raise CollectionPlanRunError("pilot total request budget is invalid")
        self.used = {name: 0 for name in self.limits}

    @property
    def total_used(self) -> int:
        return sum(self.used.values())

    def reserve(self, category: str) -> None:
        if category not in self.limits:
            raise CollectionPlanRunError("pilot request budget category is unknown")
        if self.used[category] >= self.limits[category]:
            raise CollectionPlanRunError(
                f"pilot request budget exceeded before HTTP: {category}"
            )
        if self.total_used >= self.total_limit:
            raise CollectionPlanRunError(
                "pilot total request budget exceeded before HTTP"
            )
        self.used[category] += 1

    def is_complete(self) -> bool:
        return (
            self.used["geo"] == 2
            and 1 <= self.used["endpoint_probe"] <= 2
            and self.used["regional_search"] == 5
            and self.used["repeat_search"] == 1
            and self.total_used <= self.total_limit
        )

    def artifact(self, *, egress_checks_completed: int) -> dict[str, Any]:
        return {
            "schema_version": REQUEST_BUDGET_SCHEMA_VERSION,
            "limits": {**self.limits, "total_wb": self.total_limit},
            "used": {**self.used, "total_wb": self.total_used},
            "neutral_egress_checks": {
                "counted_in_wb_budget": False,
                "completed": egress_checks_completed,
                "expected": PILOT_EGRESS_CHECKS,
            },
            "status": "within_budget"
            if self.total_used <= self.total_limit
            else "exceeded",
        }


@dataclass(slots=True)
class PilotRuntimeGuard:
    wall_deadline_utc: datetime
    wall_now: Callable[[], datetime]
    monotonic: Callable[[], float]
    started_monotonic: float
    hard_runtime_seconds: float = PILOT_HARD_RUNTIME_SECONDS

    @classmethod
    def build(
        cls,
        *,
        wall_deadline_utc: datetime,
        wall_now: Callable[[], datetime],
        monotonic: Callable[[], float],
    ) -> "PilotRuntimeGuard":
        guard = cls(
            wall_deadline_utc=wall_deadline_utc,
            wall_now=wall_now,
            monotonic=monotonic,
            started_monotonic=monotonic(),
        )
        if guard.wall_remaining_seconds() < PILOT_MINIMUM_START_WINDOW_SECONDS:
            raise CollectionPlanRunError(
                "guarded pilot requires 20 minutes before 23:45 MSK"
            )
        return guard

    def wall_remaining_seconds(self) -> float:
        current = self.wall_now()
        if current.tzinfo is None:
            raise CollectionPlanRunError("pilot wall clock must be timezone-aware")
        return (
            self.wall_deadline_utc
            - current.astimezone(timezone.utc)
        ).total_seconds()

    def runtime_remaining_seconds(self) -> float:
        elapsed = self.monotonic() - self.started_monotonic
        if elapsed < 0:
            raise CollectionPlanRunError("pilot monotonic clock moved backwards")
        return self.hard_runtime_seconds - elapsed

    def available_seconds(self) -> float:
        return min(
            self.wall_remaining_seconds(),
            self.runtime_remaining_seconds(),
        )

    def ensure_can_wait(self, seconds: float, *, reserve_seconds: float) -> None:
        if (
            not isinstance(seconds, (int, float))
            or isinstance(seconds, bool)
            or seconds < 0
        ):
            raise CollectionPlanRunError("pilot wait interval is invalid")
        if self.available_seconds() <= float(seconds) + float(reserve_seconds):
            raise CollectionPlanRunError("pilot deadline reached before wait")

    def request_timeout(self, configured_timeout: float) -> float:
        available = self.available_seconds() - FINALIZATION_RESERVE_SECONDS
        if available <= 0.1:
            raise CollectionPlanRunError("pilot hard runtime reached before HTTP")
        return min(float(configured_timeout), available)


@dataclass(slots=True)
class PilotSearchPacer:
    runtime_guard: PilotRuntimeGuard
    monotonic: Callable[[], float]
    sleeper: Callable[[float], None]
    jitter: Callable[[], float]
    last_attempt_monotonic: float | None = None
    blocked: bool = False
    events: list[dict[str, Any]] = field(default_factory=list)

    def before_attempt(
        self,
        *,
        phase: str,
        endpoint_id: str,
        region_id: str,
        query_id: str,
        page: int,
        request_timeout_seconds: float,
    ) -> None:
        if self.blocked:
            raise CollectionPlanRunError(
                "pilot search attempts are blocked after rate limit"
            )
        now_value = self.monotonic()
        wait_seconds = 0.0
        jitter_seconds = 0.0
        required_interval = 0.0
        if self.last_attempt_monotonic is not None:
            raw_jitter = self.jitter()
            if (
                isinstance(raw_jitter, bool)
                or not isinstance(raw_jitter, (int, float))
                or not 0.0 <= float(raw_jitter) <= PILOT_SEARCH_MAX_JITTER_SECONDS
            ):
                raise CollectionPlanRunError("pilot search jitter is invalid")
            jitter_seconds = float(raw_jitter)
            required_interval = (
                PILOT_SEARCH_MIN_INTERVAL_SECONDS + jitter_seconds
            )
            elapsed = now_value - self.last_attempt_monotonic
            if elapsed < 0:
                raise CollectionPlanRunError(
                    "pilot search monotonic clock moved backwards"
                )
            wait_seconds = max(0.0, required_interval - elapsed)
            self.runtime_guard.ensure_can_wait(
                wait_seconds,
                reserve_seconds=(
                    float(request_timeout_seconds)
                    + FINALIZATION_RESERVE_SECONDS
                ),
            )
            if wait_seconds > 0:
                self.sleeper(wait_seconds)
            actual_interval = (
                self.monotonic() - self.last_attempt_monotonic
            )
            if actual_interval + 1e-6 < required_interval:
                raise CollectionPlanRunError(
                    "pilot search pacing interval was not satisfied"
                )
        else:
            actual_interval = None

        self.runtime_guard.request_timeout(request_timeout_seconds)
        self.last_attempt_monotonic = self.monotonic()
        self.events.append(
            {
                "sequence": len(self.events) + 1,
                "phase": phase,
                "endpoint_id": endpoint_id,
                "region_id": region_id,
                "query_id": query_id,
                "page": page,
                "required_interval_seconds": round(required_interval, 6),
                "jitter_seconds": round(jitter_seconds, 6),
                "sleep_seconds": round(wait_seconds, 6),
                "actual_interval_seconds": (
                    round(float(actual_interval), 6)
                    if actual_interval is not None
                    else None
                ),
            }
        )

    def block(self) -> None:
        self.blocked = True

    def artifact(self) -> dict[str, Any]:
        return {
            "schema_version": PACING_EVIDENCE_SCHEMA_VERSION,
            "status": "blocked" if self.blocked else "active",
            "minimum_interval_seconds": PILOT_SEARCH_MIN_INTERVAL_SECONDS,
            "maximum_jitter_seconds": PILOT_SEARCH_MAX_JITTER_SECONDS,
            "attempts": self.events,
        }


def _strict_bool_env(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise CollectionPlanRunError("pilot cookie-required env is invalid")


def build_pilot_contour_evidence(
    *,
    config: AppConfig,
    bundle: CollectionPlanBundle,
    transport: ScopedTransport,
) -> dict[str, Any]:
    runtime_sha256 = require_runtime_env_loaded()
    route = require_marketplace_proxy(config.raw)
    serp = config.raw.get("serp", {})
    if not isinstance(serp, Mapping):
        raise CollectionPlanRunError("pilot SERP config is invalid")

    headers_env_name = str(
        serp.get("request_headers_file_env")
        or "PARSER_WB_REQUEST_HEADERS_FILE"
    ).strip()
    headers_env_value = os.getenv(headers_env_name, "")
    headers_path = config.runtime_request_headers_file
    if (
        not headers_env_value.strip()
        or headers_path is None
        or config.runtime_request_headers_sha256 is None
        or config.runtime_request_headers_count <= 0
    ):
        raise CollectionPlanRunError("pilot request headers contour is unavailable")
    expected_headers_path = Path(headers_env_value)
    if not expected_headers_path.is_absolute():
        expected_headers_path = config.project_root / expected_headers_path
    try:
        expected_relative = expected_headers_path.relative_to(config.project_root)
        configured_relative = headers_path.relative_to(config.project_root)
    except ValueError as exc:
        raise CollectionPlanRunError(
            "pilot request headers source is outside project"
        ) from exc
    if expected_relative != configured_relative:
        raise CollectionPlanRunError("pilot request headers source mismatch")
    if headers_path.is_symlink() or not headers_path.is_file():
        raise CollectionPlanRunError("pilot request headers source is unsafe")
    current_headers_sha256 = hashlib.sha256(headers_path.read_bytes()).hexdigest()
    if current_headers_sha256 != config.runtime_request_headers_sha256:
        raise CollectionPlanRunError(
            "pilot request headers changed after config load"
        )

    cookie_required_env_name = str(
        serp.get("cookie_required_env") or "PARSER_WB_COOKIE_REQUIRED"
    ).strip()
    if cookie_required_env_name not in os.environ:
        raise CollectionPlanRunError("pilot cookie-required env is missing")
    cookie_required = _strict_bool_env(os.environ[cookie_required_env_name])
    if bundle.collection_plan.proxy_rotation_mode != "disabled":
        raise CollectionPlanRunError("pilot proxy rotation is not disabled")

    if isinstance(transport, RequestsScopedTransport):
        assert_requests_session_proxy(transport.session, route)
        if transport.egress_session is not transport.session:
            raise CollectionPlanRunError(
                "pilot transport must use one requests session"
            )
        proxy_applied = True
        session_count = 1
        transport_route_sha256 = transport.proxy_route_sha256
    else:
        proxy_applied = getattr(transport, "proxy_applied", None) is True
        session_count = getattr(transport, "proxy_session_count", None)
        transport_route_sha256 = getattr(
            transport,
            "proxy_route_sha256",
            None,
        )
    if (
        not proxy_applied
        or session_count != 1
        or transport_route_sha256 != route.sha256
    ):
        raise CollectionPlanRunError("pilot transport proxy contour mismatch")

    return {
        "schema_version": CONTOUR_EVIDENCE_SCHEMA_VERSION,
        "status": "ready",
        "runtime_env": {
            "loaded": True,
            "sha256": runtime_sha256,
        },
        "proxy": {
            "configured": True,
            "valid": True,
            "applied": True,
            "single_session": True,
            "route_sha256": route.sha256,
        },
        "request_headers": {
            "configured": True,
            "readable": True,
            "loaded": True,
            "count": config.runtime_request_headers_count,
            "source_sha256": current_headers_sha256,
        },
        "cookie_required": {
            "explicit": True,
            "value_is_required": cookie_required,
        },
        "rotation": {
            "enabled": False,
        },
    }


def _default_crontab_reader() -> bytes | None:
    result = subprocess.run(
        ["crontab", "-l"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode == 0:
        return result.stdout
    if result.returncode == 1:
        return None
    raise CollectionPlanRunError("protected crontab read failed")


@dataclass(frozen=True, slots=True)
class ProtectedSnapshot:
    entries: Mapping[str, tuple[str, str | None]]


class ProtectedStateAuditor:
    def __init__(
        self,
        *,
        project_root: Path,
        crontab_reader: Callable[[], bytes | None] = _default_crontab_reader,
    ) -> None:
        self.project_root = project_root.resolve()
        self.crontab_reader = crontab_reader
        self._source_relative_paths: dict[str, str] | None = None

    def bind_source_paths(
        self,
        *,
        config_file: Path,
        collection_plan: Path,
        region_registry: Path,
        query_pack: Path,
    ) -> Mapping[str, str]:
        if self._source_relative_paths is not None:
            raise CollectionPlanRunError(
                "protected pilot source paths are already bound"
            )
        candidates = {
            "config_file": config_file,
            "collection_plan": collection_plan,
            "region_registry": region_registry,
            "query_pack": query_pack,
        }
        if set(candidates) != set(PILOT_SOURCE_NAMES):
            raise CollectionPlanRunError("protected pilot source set is invalid")

        relative_paths: dict[str, str] = {}
        for name, source_path in candidates.items():
            if not source_path.is_absolute():
                raise CollectionPlanRunError(
                    f"protected pilot source path is not absolute: {name}"
                )
            lexical_path = Path(os.path.abspath(source_path))
            try:
                relative_path = lexical_path.relative_to(self.project_root)
            except ValueError as exc:
                raise CollectionPlanRunError(
                    f"protected pilot source path is outside project root: {name}"
                ) from exc

            current = self.project_root
            for part in relative_path.parts:
                current /= part
                if current.is_symlink():
                    raise CollectionPlanRunError(
                        f"protected pilot source path uses a symlink: {name}"
                    )
            try:
                mode = lexical_path.stat(follow_symlinks=False).st_mode
            except OSError as exc:
                raise CollectionPlanRunError(
                    f"protected pilot source path is unavailable: {name}"
                ) from exc
            if not stat.S_ISREG(mode):
                raise CollectionPlanRunError(
                    f"protected pilot source path is not a regular file: {name}"
                )
            try:
                resolved_path = lexical_path.resolve(strict=True)
            except OSError as exc:
                raise CollectionPlanRunError(
                    f"protected pilot source path is unavailable: {name}"
                ) from exc
            if resolved_path != lexical_path:
                raise CollectionPlanRunError(
                    f"protected pilot source path is non-canonical: {name}"
                )
            relative_paths[name] = relative_path.as_posix()

        if len(set(relative_paths.values())) != len(relative_paths):
            raise CollectionPlanRunError("protected pilot source paths overlap")
        self._source_relative_paths = relative_paths
        return dict(relative_paths)

    @property
    def source_relative_paths(self) -> Mapping[str, str]:
        if self._source_relative_paths is None:
            raise CollectionPlanRunError("protected pilot source paths are not bound")
        return dict(self._source_relative_paths)

    def capture(self) -> ProtectedSnapshot:
        entries: dict[str, tuple[str, str | None]] = {}
        source_paths = self.source_relative_paths
        source_relative_set = set(source_paths.values())
        protected_paths = tuple(
            dict.fromkeys((*PROTECTED_RELATIVE_PATHS, *source_paths.values()))
        )
        for relative in protected_paths:
            path = self.project_root / relative
            has_symlink_component = False
            if relative in source_relative_set:
                current = self.project_root
                for part in Path(relative).parts:
                    current /= part
                    if current.is_symlink():
                        has_symlink_component = True
                        break
            if has_symlink_component or path.is_symlink():
                entries[relative] = ("unsafe_symlink", None)
            else:
                try:
                    mode = path.stat(follow_symlinks=False).st_mode
                except FileNotFoundError:
                    entries[relative] = ("missing", None)
                except OSError:
                    entries[relative] = ("unreadable", None)
                else:
                    if not stat.S_ISREG(mode):
                        entries[relative] = ("not_regular_file", None)
                    else:
                        try:
                            source_bytes = path.read_bytes()
                        except OSError:
                            entries[relative] = ("unreadable", None)
                        else:
                            entries[relative] = (
                                "present",
                                hashlib.sha256(source_bytes).hexdigest(),
                            )
        crontab = self.crontab_reader()
        entries["user_crontab"] = (
            ("missing", None)
            if crontab is None
            else ("present", hashlib.sha256(crontab).hexdigest())
        )
        return ProtectedSnapshot(entries=entries)

    def source_hashes(self, snapshot: ProtectedSnapshot) -> dict[str, str]:
        hashes: dict[str, str] = {}
        for name, relative in self.source_relative_paths.items():
            status, source_hash = snapshot.entries.get(
                relative,
                ("missing", None),
            )
            if status != "present" or source_hash is None:
                raise CollectionPlanRunError(
                    f"protected pilot source is not present: {name}"
                )
            hashes[name] = source_hash
        return hashes

    @staticmethod
    def compare(
        before: ProtectedSnapshot,
        after: ProtectedSnapshot,
    ) -> dict[str, Any]:
        if set(before.entries) != set(after.entries):
            raise CollectionPlanRunError("protected evidence path set changed")
        rows: list[dict[str, Any]] = []
        all_unchanged = True
        for path in sorted(before.entries):
            before_status, before_hash = before.entries[path]
            after_status, after_hash = after.entries[path]
            safe_states = {"present", "missing"}
            unchanged = (
                before_status in safe_states
                and after_status in safe_states
                and before_status == after_status
                and before_hash == after_hash
            )
            if not unchanged:
                all_unchanged = False
            rows.append(
                {
                    "path": path,
                    "before_status": before_status,
                    "before_sha256": before_hash,
                    "after_status": after_status,
                    "after_sha256": after_hash,
                    "status": "unchanged" if unchanged else "changed",
                }
            )
        return {
            "schema_version": PROTECTED_EVIDENCE_SCHEMA_VERSION,
            "status": "unchanged" if all_unchanged else "changed",
            "entries": rows,
        }


@dataclass(slots=True)
class PilotEgressEvidence:
    expected: int = PILOT_EGRESS_CHECKS
    completed: int = 0
    verification_status: str = "unverified"
    constant: bool | None = None
    initial_value: str | None = None

    def record_initial(self, value: str) -> None:
        self.initial_value = value
        self.completed = 1

    def verify(self, runner: CollectionPlanRunner) -> None:
        if self.initial_value is None:
            raise CollectionPlanRunError("pilot initial egress is unavailable")
        try:
            runner._check_egress(self.initial_value)
        except EgressIdentityChangedError:
            self.completed += 1
            self.verification_status = "changed"
            self.constant = False
            raise
        except CollectionPlanRunError:
            self.verification_status = "unverified"
            self.constant = None
            raise
        self.completed += 1

    def finalize(self) -> None:
        if self.completed == self.expected:
            self.verification_status = "verified_constant"
            self.constant = True

    def artifact(self, *, salt: bytes) -> dict[str, Any]:
        masked: str | None = None
        ephemeral_sha256: str | None = None
        if self.initial_value is not None:
            masked = _mask_egress(self.initial_value)
            ephemeral_sha256 = _egress_hash(self.initial_value, salt=salt)
        return {
            "masked": masked,
            "ephemeral_sha256": ephemeral_sha256,
            "verification_status": self.verification_status,
            "constant": self.constant,
            "checks_completed": self.completed,
            "checks_expected": self.expected,
        }


def _safe_error_code(value: Any, *, fallback: str) -> str:
    candidate = str(value or "")
    return candidate if _SAFE_ERROR_CODE_RE.fullmatch(candidate) else fallback


def _position_map(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {str(row["nmId"]): int(row["absolute_position"]) for row in rows}


def _membership_comparison(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    left_label: str,
    right_label: str,
) -> dict[str, Any]:
    left = _position_map(left_rows)
    right = _position_map(right_rows)
    intersection = [product_id for product_id in left if product_id in right]
    left_only = [product_id for product_id in left if product_id not in right]
    right_only = [product_id for product_id in right if product_id not in left]
    union_count = len(set(left) | set(right))
    jaccard = len(intersection) / union_count if union_count else 0.0
    return {
        "jaccard": round(jaccard, 6),
        "intersection_count": len(intersection),
        f"{left_label}_only_count": len(left_only),
        f"{right_label}_only_count": len(right_only),
        "intersection_product_ids": intersection,
        f"{left_label}_only_product_ids": left_only,
        f"{right_label}_only_product_ids": right_only,
        "position_deltas": [
            {
                "product_id": product_id,
                f"{left_label}_position": left[product_id],
                f"{right_label}_position": right[product_id],
                "delta": right[product_id] - left[product_id],
            }
            for product_id in intersection
        ],
    }


@dataclass(frozen=True, slots=True)
class ReusableProbePage:
    request: ScopedSearchRequest
    result: ScopedSearchResult


class PilotRateLimitError(CollectionPlanRunError):
    def __init__(
        self,
        *,
        phase: str,
        http_status: int,
        retry_after_status: str | None,
        retry_after_seconds: int | None,
        endpoint_evidence: dict[str, Any] | None = None,
    ) -> None:
        self.code = f"pilot_rate_limit_http_{http_status}"
        super().__init__(self.code)
        self.phase = phase
        self.http_status = http_status
        self.retry_after_status = retry_after_status or "missing"
        self.retry_after_seconds = retry_after_seconds
        self.endpoint_evidence = endpoint_evidence


class GuardedRegionalPilotRunner(CollectionPlanRunner):
    def __init__(
        self,
        *,
        config: AppConfig,
        plan_path: Path,
        transport: ScopedTransport,
        no_publish: bool,
        guarded_pilot: bool,
        request_budget: PilotRequestBudget | None = None,
        protected_auditor: ProtectedStateAuditor | None = None,
        monotonic: Callable[[], float] = time_module.monotonic,
        sleeper: Callable[[float], None] = time_module.sleep,
        jitter: Callable[[], float] = lambda: random.uniform(
            0.0,
            PILOT_SEARCH_MAX_JITTER_SECONDS,
        ),
        **kwargs: Any,
    ) -> None:
        super().__init__(
            config=config,
            plan_path=plan_path,
            transport=transport,
            no_publish=no_publish,
            **kwargs,
        )
        if not guarded_pilot:
            raise CollectionPlanRunError("--guarded-pilot is mandatory")
        self.request_budget = request_budget or PilotRequestBudget()
        self.protected_auditor = protected_auditor or ProtectedStateAuditor(
            project_root=config.project_root
        )
        self.runtime_guard = PilotRuntimeGuard.build(
            wall_deadline_utc=self.deadline.deadline_utc,
            wall_now=self.now,
            monotonic=monotonic,
        )
        self.sleeper = sleeper
        self.search_pacer = PilotSearchPacer(
            runtime_guard=self.runtime_guard,
            monotonic=monotonic,
            sleeper=sleeper,
            jitter=jitter,
        )

    def _network_timeout(self) -> float:
        return min(
            self.deadline.request_timeout(
                self.config.runtime.http_timeout_seconds
            ),
            self.runtime_guard.request_timeout(
                self.config.runtime.http_timeout_seconds
            ),
        )

    def _check_egress(self, expected: str | None = None) -> str:
        self.runtime_guard.request_timeout(
            self.config.runtime.http_timeout_seconds
        )
        return super()._check_egress(expected)

    def _write(
        self,
        path: Path,
        payload: bytes,
        *,
        final_manifest: bool = False,
    ) -> None:
        self.runtime_guard.ensure_can_wait(
            0.0,
            reserve_seconds=FINALIZATION_RESERVE_SECONDS,
        )
        super()._write(
            path,
            payload,
            final_manifest=final_manifest,
        )

    def _validate_pilot_contract(self, bundle: CollectionPlanBundle) -> None:
        self._validate_mode(bundle)
        if tuple(query.query_id for query in bundle.enabled_queries) != PILOT_QUERY_IDS:
            raise CollectionPlanRunError(
                "guarded pilot requires the approved three query IDs in order"
            )
        if tuple(region.region_id for region in bundle.enabled_regions) != PILOT_REGION_IDS:
            raise CollectionPlanRunError(
                "guarded pilot requires Moscow then Rostov-on-Don"
            )

    def _resolve_destinations(
        self,
        bundle: CollectionPlanBundle,
    ) -> dict[str, ResolvedDestination]:
        resolved: dict[str, ResolvedDestination] = {}
        for region in bundle.enabled_regions:
            timeout_seconds = self._network_timeout()
            self.request_budget.reserve("geo")
            try:
                dest_id = self.transport.resolve_destination(
                    region,
                    timeout_seconds=timeout_seconds,
                )
            except ScopedTransportError as exc:
                if type(exc.http_status) is int and exc.http_status in {
                    429,
                    498,
                }:
                    retry_after_status = (
                        exc.retry_after_status
                        if exc.retry_after_status
                        in {"missing", "valid", "invalid", "out_of_range"}
                        else "missing"
                    )
                    retry_after_seconds = (
                        exc.retry_after_seconds
                        if (
                            retry_after_status == "valid"
                            and type(exc.retry_after_seconds) is int
                            and 1 <= exc.retry_after_seconds <= 120
                        )
                        else None
                    )
                    raise PilotRateLimitError(
                        phase="geo_resolver",
                        http_status=exc.http_status,
                        retry_after_status=retry_after_status,
                        retry_after_seconds=retry_after_seconds,
                    ) from exc
                raise
            if not isinstance(dest_id, str):
                raise CollectionPlanRunError(
                    f"resolver returned invalid dest for {region.region_id}"
                )
            valid_dest = bool(re.fullmatch(r"^[+-]?[0-9]{1,16}$", dest_id))
            if not valid_dest:
                raise CollectionPlanRunError(
                    f"resolver returned invalid dest for {region.region_id}"
                )
            resolved[region.region_id] = ResolvedDestination(
                region_id=region.region_id,
                dest_id_observed=dest_id,
                dest_resolved_at_utc=_utc_iso(self.now()),
            )
        if len({item.dest_id_observed for item in resolved.values()}) != len(resolved):
            raise CollectionPlanRunError(
                "guarded pilot requires distinct destination values"
            )
        return resolved

    def _probe_and_pin_endpoint(
        self,
        *,
        bundle: CollectionPlanBundle,
        resolved: Mapping[str, ResolvedDestination],
    ) -> tuple[dict[str, Any], ReusableProbePage]:
        first_task = self._task(
            bundle=bundle,
            query_id=PILOT_QUERY_IDS[0],
            region_id=PILOT_REGION_IDS[0],
        )
        base_request = self._search_request(
            task=first_task,
            dest_id=resolved[PILOT_REGION_IDS[0]].dest_id_observed,
        )
        attempts: list[dict[str, Any]] = []
        pinned_endpoint_id: str | None = None
        reusable_page: ReusableProbePage | None = None
        for endpoint_id in self.transport.endpoint_policy.endpoint_ids[:2]:
            timeout_seconds = self._network_timeout()
            self.request_budget.reserve("endpoint_probe")
            request = ScopedSearchRequest(
                task=base_request.task,
                dest_id_observed=base_request.dest_id_observed,
                endpoint_id=endpoint_id,
                params=base_request.params,
            )
            self.search_pacer.before_attempt(
                phase="endpoint_probe",
                endpoint_id=endpoint_id,
                region_id=first_task.region_id,
                query_id=first_task.query_id,
                page=first_task.page,
                request_timeout_seconds=timeout_seconds,
            )
            result = self.transport.probe_endpoint(
                request,
                endpoint_id=endpoint_id,
                timeout_seconds=timeout_seconds,
            )
            if not isinstance(result, EndpointProbeResult):
                raise CollectionPlanRunError("endpoint probe result is invalid")
            if (
                type(result.endpoint_id) is not str
                or result.endpoint_id != endpoint_id
            ):
                raise CollectionPlanRunError("endpoint probe identity mismatch")
            http_status = (
                result.http_status
                if isinstance(result.http_status, int)
                and not isinstance(result.http_status, bool)
                and 100 <= result.http_status <= 599
                else None
            )
            error_code = (
                None
                if result.error_code is None
                else _safe_error_code(
                    result.error_code,
                    fallback="endpoint_probe_invalid_error_code",
                )
            )
            suitable = (
                type(result.suitable) is bool
                and result.suitable is True
                and type(result.http_status) is int
                and result.http_status == 200
                and error_code is None
            )
            candidate_page: ReusableProbePage | None = None
            if suitable:
                try:
                    candidate_page = self._validate_reusable_probe_page(
                        expected_request=request,
                        probe_result=result,
                        endpoint_id=endpoint_id,
                    )
                except CollectionPlanRunError:
                    candidate_page = None
                    suitable = False
                    error_code = "endpoint_probe_reusable_invalid"
            if not suitable and error_code is None:
                error_code = "endpoint_probe_unsuitable"
            attempts.append(
                {
                    "endpoint_id": endpoint_id,
                    "attempted": True,
                    "outcome": "usable" if suitable else "unusable",
                    "status": http_status,
                    "error_code": error_code,
                    "reused_as_first_page": suitable,
                    "reuse_task": (
                        {
                            "region_id": first_task.region_id,
                            "query_id": first_task.query_id,
                            "page": first_task.page,
                        }
                        if suitable
                        else None
                    ),
                }
            )
            if type(result.http_status) is int and result.http_status in {
                429,
                498,
            }:
                retry_after_status = (
                    result.retry_after_status
                    if result.retry_after_status
                    in {"missing", "valid", "invalid", "out_of_range"}
                    else "invalid"
                )
                retry_after_seconds = (
                    result.retry_after_seconds
                    if (
                        retry_after_status == "valid"
                        and type(result.retry_after_seconds) is int
                        and 1 <= result.retry_after_seconds <= 120
                    )
                    else None
                )
                evidence = {
                    "schema_version": ENDPOINT_EVIDENCE_SCHEMA_VERSION,
                    "status": "failed",
                    "attempts": attempts,
                    "pinned_endpoint_id": None,
                    "reuse": {
                        "reused_as_first_page": False,
                        "region_id": None,
                        "query_id": None,
                        "page": None,
                    },
                }
                raise PilotRateLimitError(
                    phase="endpoint_probe",
                    http_status=result.http_status,
                    retry_after_status=retry_after_status,
                    retry_after_seconds=retry_after_seconds,
                    endpoint_evidence=evidence,
                )
            if suitable:
                self.transport.pin_endpoint(endpoint_id)
                pinned_endpoint_id = endpoint_id
                reusable_page = candidate_page
                break
        evidence = {
            "schema_version": ENDPOINT_EVIDENCE_SCHEMA_VERSION,
            "status": "success" if pinned_endpoint_id is not None else "failed",
            "attempts": attempts,
            "pinned_endpoint_id": pinned_endpoint_id,
            "reuse": {
                "reused_as_first_page": reusable_page is not None,
                "region_id": (
                    reusable_page.request.task.region_id
                    if reusable_page is not None
                    else None
                ),
                "query_id": (
                    reusable_page.request.task.query_id
                    if reusable_page is not None
                    else None
                ),
                "page": (
                    reusable_page.request.task.page
                    if reusable_page is not None
                    else None
                ),
            },
        }
        if pinned_endpoint_id is None or reusable_page is None:
            raise EndpointPreflightFailed(evidence)
        return evidence, reusable_page

    @staticmethod
    def _same_search_request(
        left: ScopedSearchRequest,
        right: ScopedSearchRequest,
    ) -> bool:
        return (
            left.task == right.task
            and left.dest_id_observed == right.dest_id_observed
            and left.endpoint_id == right.endpoint_id
            and dict(left.params) == dict(right.params)
        )

    def _validate_reusable_probe_page(
        self,
        *,
        expected_request: ScopedSearchRequest,
        probe_result: EndpointProbeResult,
        endpoint_id: str,
    ) -> ReusableProbePage:
        request = probe_result.reusable_request
        result = probe_result.reusable_result
        if (
            not isinstance(request, ScopedSearchRequest)
            or request is not expected_request
        ):
            raise CollectionPlanRunError("endpoint probe reusable request is invalid")
        if not self._same_search_request(request, expected_request):
            raise CollectionPlanRunError(
                "endpoint probe reusable request identity mismatch"
            )
        if (
            type(request.endpoint_id) is not str
            or request.endpoint_id != endpoint_id
            or type(request.dest_id_observed) is not str
            or request.dest_id_observed != expected_request.dest_id_observed
        ):
            raise CollectionPlanRunError(
                "endpoint probe reusable request route mismatch"
            )
        if not isinstance(result, ScopedSearchResult):
            raise CollectionPlanRunError("endpoint probe reusable result is missing")
        if type(result.http_status) is not int or result.http_status != 200:
            raise CollectionPlanRunError("endpoint probe reusable status is invalid")
        if (
            type(result.endpoint_id) is not str
            or result.endpoint_id != endpoint_id
            or type(result.dest_id_sent) is not str
            or result.dest_id_sent != expected_request.dest_id_observed
        ):
            raise CollectionPlanRunError(
                "endpoint probe reusable response route mismatch"
            )
        if not isinstance(result.payload, Mapping):
            raise CollectionPlanRunError("endpoint probe reusable payload is invalid")
        products = _extract_products(result.payload)
        if len(products) != 100:
            raise CollectionPlanRunError(
                "endpoint probe reusable products count is invalid"
            )
        product_ids: set[str] = set()
        for product in products:
            if not isinstance(product, Mapping):
                raise CollectionPlanRunError(
                    "endpoint probe reusable product is invalid"
                )
            product_id = _normalize_product_id(product)
            if product_id in product_ids:
                raise CollectionPlanRunError(
                    "endpoint probe reusable product ID is duplicated"
                )
            product_ids.add(product_id)
        if len(product_ids) != 100:
            raise CollectionPlanRunError(
                "endpoint probe reusable product IDs are incomplete"
            )
        return ReusableProbePage(request=request, result=result)

    def _validate_search_result(
        self,
        *,
        request: ScopedSearchRequest,
        result: ScopedSearchResult,
    ) -> None:
        if not isinstance(result, ScopedSearchResult):
            raise CollectionPlanRunError("search result is invalid")
        if type(result.http_status) is not int or result.http_status != 200:
            raise CollectionPlanRunError("search response is not HTTP 200")
        if not isinstance(result.payload, Mapping):
            raise CollectionPlanRunError("search payload is invalid")
        if result.dest_id_sent != request.dest_id_observed:
            raise CollectionPlanRunError("search destination evidence mismatch")
        if result.endpoint_id != self.transport.endpoint_policy.pinned_endpoint_id:
            raise CollectionPlanRunError("search endpoint evidence mismatch")
        if result.endpoint_id != request.endpoint_id:
            raise CollectionPlanRunError("search request endpoint mismatch")

    def _collect_region(
        self,
        *,
        bundle: CollectionPlanBundle,
        paths: ScopedPaths,
        resolution: ResolvedDestination,
        region_manifest: dict[str, Any],
        totals: dict[str, int],
        reusable_first_page: ReusableProbePage | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        product_rows: list[dict[str, Any]] = []
        page_rows: list[dict[str, Any]] = []
        rows_by_query: dict[str, list[dict[str, Any]]] = {}
        region_error: Exception | None = None
        reusable_consumed = False
        for query in bundle.enabled_queries:
            task = self._task(
                bundle=bundle,
                query_id=query.query_id,
                region_id=resolution.region_id,
            )
            request = self._search_request(
                task=task,
                dest_id=resolution.dest_id_observed,
            )
            timeout_seconds = self._network_timeout()
            try:
                if reusable_first_page is not None and not reusable_consumed:
                    if not self._same_search_request(
                        reusable_first_page.request,
                        request,
                    ):
                        raise CollectionPlanRunError(
                            "reusable endpoint probe task identity mismatch"
                        )
                    result = reusable_first_page.result
                    reusable_consumed = True
                else:
                    self.request_budget.reserve("regional_search")
                    self.search_pacer.before_attempt(
                        phase="regional_search",
                        endpoint_id=request.endpoint_id,
                        region_id=task.region_id,
                        query_id=task.query_id,
                        page=task.page,
                        request_timeout_seconds=timeout_seconds,
                    )
                    result = self.transport.search(
                        request,
                        timeout_seconds=timeout_seconds,
                    )
                self._validate_search_result(request=request, result=result)
                region_manifest["dest_resolution_status"] = "resolved_and_sent"
                raw_path = paths.raw_page_path(task)
                self._write(raw_path, _json_bytes(result.payload))
                products = _extract_products(result.payload)
                rows, page_row = self._rows_for_page(
                    task=task,
                    resolution=resolution,
                    products=products,
                    raw_path=raw_path,
                    endpoint_id=result.endpoint_id,
                )
                self._write(
                    paths.checkpoint_path(task),
                    _json_bytes(
                        self._checkpoint_payload(
                            task=task,
                            resolution=resolution,
                            raw_path=raw_path,
                        )
                    ),
                )
                rows_by_query[query.query_id] = rows
                product_rows.extend(rows)
                page_rows.append(page_row)
                region_manifest["pages_ok"] += 1
                region_manifest["products_ok"] += len(rows)
                totals["pages_ok"] += 1
                totals["products_ok"] += len(rows)
            except ScopedTransportError as exc:
                if (
                    exc.request_sent
                    and exc.dest_id_sent == resolution.dest_id_observed
                ):
                    region_manifest["dest_resolution_status"] = "resolved_and_sent"
                region_error = exc
                break
            except Exception as exc:
                region_error = exc
                break

        region_manifest["outputs"] = self._write_scope_outputs(
            paths=paths,
            region_id=resolution.region_id,
            product_rows=product_rows,
            page_rows=page_rows,
        )
        expected_pages = len(bundle.enabled_queries)
        if reusable_first_page is not None and not reusable_consumed:
            region_error = region_error or CollectionPlanRunError(
                "reusable endpoint probe page was not consumed"
            )
        region_manifest["complete"] = (
            region_error is None
            and region_manifest["pages_ok"] == expected_pages
            and region_manifest["products_ok"] == expected_pages * 100
        )
        region_manifest["status"] = (
            "success" if region_manifest["complete"] else "failed"
        )
        self._write(
            paths.region_state_path(resolution.region_id),
            _json_bytes(
                {
                    "schema_version": REGION_STATE_SCHEMA_VERSION,
                    **region_manifest,
                }
            ),
        )
        if region_error is not None:
            raise region_error
        if not region_manifest["complete"]:
            raise CollectionPlanRunError(
                f"region scope incomplete: {resolution.region_id}"
            )
        totals["regions_ok"] += 1
        return rows_by_query

    def _repeat_control(
        self,
        *,
        bundle: CollectionPlanBundle,
        paths: ScopedPaths,
        resolution: ResolvedDestination,
        initial_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        task = self._task(
            bundle=bundle,
            query_id=PILOT_QUERY_IDS[0],
            region_id=PILOT_REGION_IDS[0],
        )
        request = self._search_request(
            task=task,
            dest_id=resolution.dest_id_observed,
        )
        timeout_seconds = self._network_timeout()
        self.request_budget.reserve("repeat_search")
        self.search_pacer.before_attempt(
            phase="repeat_search",
            endpoint_id=request.endpoint_id,
            region_id=task.region_id,
            query_id=task.query_id,
            page=task.page,
            request_timeout_seconds=timeout_seconds,
        )
        result = self.transport.search(
            request,
            timeout_seconds=timeout_seconds,
        )
        self._validate_search_result(request=request, result=result)
        raw_path = (
            paths.layer_region_run_dir("raw", PILOT_REGION_IDS[0])
            / "control"
            / f"{PILOT_QUERY_IDS[0]}_repeat_page_001.json"
        )
        self._write(raw_path, _json_bytes(result.payload))
        repeat_rows, _page_row = self._rows_for_page(
            task=task,
            resolution=resolution,
            products=_extract_products(result.payload),
            raw_path=raw_path,
            endpoint_id=result.endpoint_id,
        )
        membership = _membership_comparison(
            initial_rows,
            repeat_rows,
            left_label="initial",
            right_label="repeat",
        )
        eligible = membership["jaccard"] >= CONTROL_JACCARD_THRESHOLD
        return {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "control_id": "moscow-a-a-first-query",
            "region_id": PILOT_REGION_IDS[0],
            "query_id": PILOT_QUERY_IDS[0],
            "endpoint_id": result.endpoint_id,
            "dest_id_observed": resolution.dest_id_observed,
            "http_status": result.http_status,
            "products_count": len(repeat_rows),
            "raw_file": _relative(raw_path, paths.project_root),
            "threshold": CONTROL_JACCARD_THRESHOLD,
            "status": "eligible" if eligible else "not_eligible",
            **membership,
        }

    @staticmethod
    def _rate_limit_signal(exc: Exception) -> dict[str, Any] | None:
        if isinstance(exc, PilotRateLimitError):
            return {
                "phase": exc.phase,
                "http_status": exc.http_status,
                "retry_after_status": exc.retry_after_status,
                "retry_after_seconds": exc.retry_after_seconds,
            }
        if (
            isinstance(exc, ScopedTransportError)
            and type(exc.http_status) is int
            and exc.http_status in {429, 498}
        ):
            retry_after_status = (
                exc.retry_after_status
                if exc.retry_after_status
                in {"missing", "valid", "invalid", "out_of_range"}
                else "missing"
            )
            retry_after_seconds = (
                exc.retry_after_seconds
                if (
                    retry_after_status == "valid"
                    and type(exc.retry_after_seconds) is int
                    and 1 <= exc.retry_after_seconds <= 120
                )
                else None
            )
            return {
                "phase": "regional_or_repeat_search",
                "http_status": exc.http_status,
                "retry_after_status": retry_after_status,
                "retry_after_seconds": retry_after_seconds,
            }
        return None

    def _finalize_rate_limit(
        self,
        *,
        signal: Mapping[str, Any],
        egress: PilotEgressEvidence,
    ) -> dict[str, Any]:
        self.search_pacer.block()
        retry_after_seconds = signal.get("retry_after_seconds")
        cooldown_seconds = (
            int(retry_after_seconds)
            if type(retry_after_seconds) is int
            else PILOT_DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS
        )
        evidence = {
            "schema_version": RATE_LIMIT_EVIDENCE_SCHEMA_VERSION,
            "status": "triggered",
            "http_status": signal["http_status"],
            "phase": signal["phase"],
            "retry_after": {
                "status": signal["retry_after_status"],
                "seconds": retry_after_seconds,
            },
            "cooldown_seconds": cooldown_seconds,
            "cooldown_status": "pending",
            "failure_final_egress": {
                "attempted": False,
                "status": "not_attempted",
            },
            "subsequent_wb_calls_allowed": False,
            "retry_count": 0,
        }
        try:
            self.runtime_guard.ensure_can_wait(
                cooldown_seconds,
                reserve_seconds=(
                    self.config.runtime.http_timeout_seconds
                    + FINALIZATION_RESERVE_SECONDS
                ),
            )
        except CollectionPlanRunError:
            evidence["cooldown_status"] = "skipped_deadline"
            return evidence

        self.sleeper(float(cooldown_seconds))
        evidence["cooldown_status"] = "completed"
        evidence["failure_final_egress"]["attempted"] = True
        try:
            egress.verify(self)
        except EgressIdentityChangedError:
            evidence["failure_final_egress"]["status"] = "changed"
        except CollectionPlanRunError:
            evidence["failure_final_egress"]["status"] = "unverified"
        else:
            evidence["failure_final_egress"]["status"] = "matched_initial"
        return evidence

    @staticmethod
    def _comparison_artifact(
        *,
        rows_by_region: Mapping[str, Mapping[str, list[dict[str, Any]]]],
        control: Mapping[str, Any] | None,
        protected_status: str,
        prior_error: Exception | None,
    ) -> dict[str, Any]:
        base: dict[str, Any] = {
            "schema_version": COMPARISON_SCHEMA_VERSION,
            "control_threshold": CONTROL_JACCARD_THRESHOLD,
            "claims_scope": "serp_membership_and_position_only",
        }
        if protected_status != "unchanged":
            return {
                **base,
                "status": "failed",
                "reason": "protected_state_changed",
                "queries": [],
            }
        if prior_error is not None:
            return {
                **base,
                "status": "failed",
                "reason": "pilot_run_failed",
                "queries": [],
            }
        if control is None or control.get("status") != "eligible":
            return {
                **base,
                "status": "not_eligible",
                "reason": "control_jaccard_below_threshold",
                "queries": [],
            }
        queries: list[dict[str, Any]] = []
        for query_id in PILOT_QUERY_IDS:
            membership = _membership_comparison(
                rows_by_region[PILOT_REGION_IDS[0]][query_id],
                rows_by_region[PILOT_REGION_IDS[1]][query_id],
                left_label="moscow",
                right_label="rostov",
            )
            queries.append({"query_id": query_id, **membership})
        return {
            **base,
            "status": "eligible",
            "reason": None,
            "control_jaccard": control["jaccard"],
            "queries": queries,
        }

    def run(self) -> dict[str, Any]:
        initial_bundle = self._load_bundle()
        self._validate_pilot_contract(initial_bundle)
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
            self._validate_pilot_contract(bundle)
            if self._bundle_identity(bundle) != self._bundle_identity(initial_bundle):
                raise CollectionPlanRunError(
                    "collection plan sources changed during lock acquisition"
                )
            if paths.state_run_dir.exists():
                raise CollectionPlanRunError(
                    f"immutable scoped run state already exists: {paths.state_run_dir}"
                )
            self.protected_auditor.bind_source_paths(
                config_file=self.config.config_file,
                collection_plan=bundle.collection_plan.source_path,
                region_registry=bundle.region_registry.source_path,
                query_pack=bundle.query_pack.source_path,
            )
            protected_before = self.protected_auditor.capture()
            before_source_hashes = self.protected_auditor.source_hashes(
                protected_before
            )
            expected_source_hashes = {
                "config_file": self.config.config_file_sha256,
                "collection_plan": bundle.collection_plan_sha256,
                "region_registry": bundle.region_registry_sha256,
                "query_pack": bundle.query_pack_sha256,
            }
            if any(
                before_source_hashes[name] != source_hash
                for name, source_hash in expected_source_hashes.items()
            ):
                raise CollectionPlanRunError(
                    "protected pilot source hash changed before network"
                )
            register_query_pack_provenance(
                provenance_path=paths.provenance_path,
                query_pack=bundle.query_pack,
                project_root=paths.project_root,
            )

            egress = PilotEgressEvidence()
            contour_evidence: dict[str, Any] = {
                "schema_version": CONTOUR_EVIDENCE_SCHEMA_VERSION,
                "status": "not_checked",
            }
            endpoint_evidence: dict[str, Any] = {
                "schema_version": ENDPOINT_EVIDENCE_SCHEMA_VERSION,
                "status": "not_attempted",
                "attempts": [],
                "pinned_endpoint_id": None,
            }
            resolved: dict[str, ResolvedDestination] = {}
            rows_by_region: dict[str, dict[str, list[dict[str, Any]]]] = {}
            manifest_regions = [
                {
                    "region_id": region.region_id,
                    "dest_id_observed": None,
                    "dest_resolved_at_utc": None,
                    "dest_resolution_source": "wb_geo_xinfo",
                    "dest_resolution_status": "unresolved",
                    "status": "pending",
                    "pages_ok": 0,
                    "products_ok": 0,
                }
                for region in bundle.enabled_regions
            ]
            totals = {"regions_ok": 0, "pages_ok": 0, "products_ok": 0}
            snapshot_sha256: str | None = None
            control: dict[str, Any] | None = None
            rate_limit_evidence: dict[str, Any] = {
                "schema_version": RATE_LIMIT_EVIDENCE_SCHEMA_VERSION,
                "status": "not_triggered",
                "subsequent_wb_calls_allowed": True,
                "retry_count": 0,
            }
            caught: Exception | None = None

            try:
                contour_evidence = build_pilot_contour_evidence(
                    config=self.config,
                    bundle=bundle,
                    transport=self.transport,
                )
                initial_egress = self._check_egress()
                egress.record_initial(initial_egress)
                resolved = self._resolve_destinations(bundle)
                for item in manifest_regions:
                    resolution = resolved[item["region_id"]]
                    item.update(
                        {
                            "dest_id_observed": resolution.dest_id_observed,
                            "dest_resolved_at_utc": resolution.dest_resolved_at_utc,
                            "dest_resolution_status": "resolved_not_sent",
                        }
                    )
                endpoint_evidence, reusable_probe_page = self._probe_and_pin_endpoint(
                    bundle=bundle,
                    resolved=resolved,
                )
                snapshot = build_effective_plan_snapshot(
                    bundle,
                    resolved_destinations=resolved,
                    page_size=100,
                    endpoint_policy=self.transport.endpoint_policy,
                )
                snapshot_sha256 = canonical_effective_plan_sha256(snapshot)
                self._write(
                    paths.effective_plan_path,
                    canonical_effective_plan_bytes(snapshot),
                )

                for region_id in PILOT_REGION_IDS:
                    region_manifest = next(
                        item
                        for item in manifest_regions
                        if item["region_id"] == region_id
                    )
                    rows_by_region[region_id] = self._collect_region(
                        bundle=bundle,
                        paths=paths,
                        resolution=resolved[region_id],
                        region_manifest=region_manifest,
                        totals=totals,
                        reusable_first_page=(
                            reusable_probe_page
                            if region_id == PILOT_REGION_IDS[0]
                            else None
                        ),
                    )
                    egress.verify(self)

                control = self._repeat_control(
                    bundle=bundle,
                    paths=paths,
                    resolution=resolved[PILOT_REGION_IDS[0]],
                    initial_rows=rows_by_region[PILOT_REGION_IDS[0]][
                        PILOT_QUERY_IDS[0]
                    ],
                )
                egress.verify(self)
                egress.finalize()
            except PilotRateLimitError as exc:
                if exc.endpoint_evidence is not None:
                    endpoint_evidence = exc.endpoint_evidence
                caught = exc
            except EndpointPreflightFailed as exc:
                endpoint_evidence = exc.evidence
                caught = exc
            except Exception as exc:
                caught = exc

            if contour_evidence["status"] == "not_checked":
                contour_evidence = {
                    "schema_version": CONTOUR_EVIDENCE_SCHEMA_VERSION,
                    "status": "failed",
                }
            rate_limit_signal = (
                self._rate_limit_signal(caught)
                if caught is not None
                else None
            )
            if rate_limit_signal is not None:
                rate_limit_evidence = self._finalize_rate_limit(
                    signal=rate_limit_signal,
                    egress=egress,
                )

            self._write(
                paths.state_run_dir / "contour_preflight.json",
                _json_bytes(contour_evidence),
            )
            self._write(
                paths.state_run_dir / "endpoint_preflight.json",
                _json_bytes(endpoint_evidence),
            )
            self._write(
                paths.state_run_dir / "search_pacing.json",
                _json_bytes(self.search_pacer.artifact()),
            )
            self._write(
                paths.state_run_dir / "rate_limit.json",
                _json_bytes(rate_limit_evidence),
            )
            self._write(
                paths.state_run_dir / "request_budget.json",
                _json_bytes(
                    self.request_budget.artifact(
                        egress_checks_completed=egress.completed
                    )
                ),
            )
            if control is not None:
                self._write(
                    paths.state_run_dir / "control/moscow_repeat.json",
                    _json_bytes(control),
                )

            protected_after = self.protected_auditor.capture()
            protected_evidence = self.protected_auditor.compare(
                protected_before,
                protected_after,
            )
            confirmed_source_hashes: dict[str, str] | None = None
            try:
                after_source_hashes = self.protected_auditor.source_hashes(
                    protected_after
                )
            except CollectionPlanRunError:
                after_source_hashes = {}
            if (
                protected_evidence["status"] == "unchanged"
                and all(
                    after_source_hashes.get(name) == source_hash
                    for name, source_hash in expected_source_hashes.items()
                )
            ):
                confirmed_source_hashes = after_source_hashes
            self._write(
                paths.state_run_dir / "protected_evidence.json",
                _json_bytes(protected_evidence),
            )
            if caught is None and protected_evidence["status"] != "unchanged":
                caught = CollectionPlanRunError("protected state changed during pilot")

            comparison = self._comparison_artifact(
                rows_by_region=rows_by_region,
                control=control,
                protected_status=protected_evidence["status"],
                prior_error=caught,
            )
            if (
                caught is None
                and control is not None
                and control["status"] != "eligible"
            ):
                caught = CollectionPlanRunError(
                    "control Jaccard is below pilot threshold"
                )
                comparison = self._comparison_artifact(
                    rows_by_region=rows_by_region,
                    control=control,
                    protected_status=protected_evidence["status"],
                    prior_error=None,
                )
            self._write(
                paths.state_run_dir / "comparison.json",
                _json_bytes(comparison),
            )

            complete = (
                caught is None
                and totals == {
                    "regions_ok": 2,
                    "pages_ok": 6,
                    "products_ok": 600,
                }
                and control is not None
                and control["status"] == "eligible"
                and comparison["status"] == "eligible"
                and protected_evidence["status"] == "unchanged"
                and self.request_budget.is_complete()
                and egress.verification_status == "verified_constant"
                and egress.completed == egress.expected
                and endpoint_evidence["status"] == "success"
                and contour_evidence["status"] == "ready"
                and rate_limit_evidence["status"] == "not_triggered"
                and snapshot_sha256 is not None
                and confirmed_source_hashes is not None
            )
            error: dict[str, Any] | None = None
            if caught is not None:
                error_code = _safe_error_code(
                    getattr(caught, "code", None),
                    fallback="guarded_pilot_failed",
                )
                error = {
                    "error_class": caught.__class__.__name__,
                    "error_code": error_code,
                }
            manifest = {
                "schema_version": PILOT_MANIFEST_SCHEMA_VERSION,
                "mode": "guarded_pilot",
                "run_id": self.run_id,
                "collection_scope": COLLECTION_SCOPE,
                "collection_plan_id": bundle.collection_plan.collection_plan_id,
                "query_pack_id": bundle.query_pack.query_pack_id,
                "query_pack_version": bundle.query_pack.version,
                "query_pack_sha256": (
                    confirmed_source_hashes["query_pack"]
                    if confirmed_source_hashes is not None
                    else None
                ),
                "collection_plan_sha256": (
                    confirmed_source_hashes["collection_plan"]
                    if confirmed_source_hashes is not None
                    else None
                ),
                "region_registry_sha256": (
                    confirmed_source_hashes["region_registry"]
                    if confirmed_source_hashes is not None
                    else None
                ),
                "effective_plan_sha256": snapshot_sha256,
                "effective_plan_snapshot_path": (
                    _relative(paths.effective_plan_path, paths.project_root)
                    if snapshot_sha256 is not None
                    else None
                ),
                "publication_mode": "none",
                "sellers_mode": "disabled",
                "proxy_rotation_mode": "disabled",
                "notification_mode": "disabled",
                "started_at_utc": self.started_at_utc,
                "finished_at_utc": _utc_iso(self.now()),
                "deadline_utc": _utc_iso(self.deadline.deadline_utc),
                "egress": egress.artifact(salt=self.egress_hash_salt),
                "contour_preflight_path": _relative(
                    paths.state_run_dir / "contour_preflight.json",
                    paths.project_root,
                ),
                "endpoint_preflight": {
                    "status": endpoint_evidence["status"],
                    "pinned_endpoint_id": endpoint_evidence[
                        "pinned_endpoint_id"
                    ],
                    "artifact_path": _relative(
                        paths.state_run_dir / "endpoint_preflight.json",
                        paths.project_root,
                    ),
                },
                "request_budget_path": _relative(
                    paths.state_run_dir / "request_budget.json",
                    paths.project_root,
                ),
                "search_pacing_path": _relative(
                    paths.state_run_dir / "search_pacing.json",
                    paths.project_root,
                ),
                "rate_limit_path": _relative(
                    paths.state_run_dir / "rate_limit.json",
                    paths.project_root,
                ),
                "control_path": (
                    _relative(
                        paths.state_run_dir / "control/moscow_repeat.json",
                        paths.project_root,
                    )
                    if control is not None
                    else None
                ),
                "comparison_path": _relative(
                    paths.state_run_dir / "comparison.json",
                    paths.project_root,
                ),
                "protected_evidence_path": _relative(
                    paths.state_run_dir / "protected_evidence.json",
                    paths.project_root,
                ),
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
                raise CollectionPlanRunError("guarded pilot run is incomplete")
            return manifest


class EndpointPreflightFailed(CollectionPlanRunError):
    def __init__(self, evidence: dict[str, Any]) -> None:
        super().__init__("endpoint_preflight_failed")
        self.code = "endpoint_preflight_failed"
        self.evidence = evidence


def run_guarded_regional_pilot(
    *,
    config: AppConfig,
    plan_path: Path,
    no_publish: bool,
    guarded_pilot: bool,
    transport: ScopedTransport | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    if not guarded_pilot:
        raise CollectionPlanRunError("--guarded-pilot is mandatory")
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
        return GuardedRegionalPilotRunner(
            config=config,
            plan_path=plan_path,
            transport=active_transport,
            no_publish=no_publish,
            guarded_pilot=guarded_pilot,
            **kwargs,
        ).run()
    finally:
        if owned_transport:
            active_transport.close()
