from __future__ import annotations

import csv
import hashlib
import json
import os
import re
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
    ScopedPaths,
    acquire_collection_plan_locks,
)
from app.warehouse.wb_regional import ingest_regional_run


FOUR_REGION_PLAN_ID = "shevron-four-regions-top1000-v2"
FOUR_REGION_IDS = ("moscow", "rostov-on-don", "novosibirsk", "kazan")
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
    state_path: Path
    latest_path: Path
    run_id: str
    prior_state_sha256: str | None
    prior_latest_sha256: str | None
    active: bool = True
    state_written: bool = False


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


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


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    ).encode("utf-8")
    temp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        temp = None
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


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
        current.mkdir(mode=0o700)
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
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    ).encode("utf-8")
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
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise CriticalPipelineError(
            "downstream authoritative state path is unsafe"
        )
    return path.read_bytes()


def _begin_authoritative_state_transition(
    *,
    state_path: Path,
    latest_path: Path,
    run_id: str,
    project_root: Path,
) -> _AuthoritativeStateLease:
    _safe_state_parent(state_path, project_root=project_root)
    _safe_state_parent(latest_path, project_root=project_root)
    state_bytes = _optional_regular_bytes(state_path)
    latest_bytes = _optional_regular_bytes(latest_path)
    state_payload: dict[str, Any] | None = None
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
        ):
            raise CriticalPipelineError(
                "downstream authoritative state contract mismatch"
            )

    if latest_bytes is not None:
        try:
            loaded_latest = json.loads(latest_bytes)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CriticalPipelineError(
                "downstream latest pointer is invalid"
            ) from exc
        if not isinstance(loaded_latest, dict):
            raise CriticalPipelineError(
                "downstream latest pointer is invalid"
            )
        if loaded_latest.get("run_id") == run_id:
            if state_bytes is None:
                raise CriticalPipelineError(
                    "published downstream state is missing"
                )
            expected_state_path = state_path.relative_to(
                project_root
            ).as_posix()
            if (
                loaded_latest.get("state_path") != expected_state_path
                or loaded_latest.get("state_sha256")
                != _sha256_bytes(state_bytes)
            ):
                raise CriticalPipelineError(
                    "published downstream state integrity mismatch"
                )
            raise CriticalPipelineError(
                "published downstream state is immutable"
            )

    if state_payload is not None and (
        state_payload["status"] == "success"
        or state_payload["complete"] is True
    ):
        raise CriticalPipelineError(
            "completed downstream state is immutable"
        )
    return _AuthoritativeStateLease(
        state_path=state_path,
        latest_path=latest_path,
        run_id=run_id,
        prior_state_sha256=(
            _sha256_bytes(state_bytes)
            if state_bytes is not None
            else None
        ),
        prior_latest_sha256=(
            _sha256_bytes(latest_bytes)
            if latest_bytes is not None
            else None
        ),
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
    current = _optional_regular_bytes(lease.state_path)
    current_sha256 = (
        _sha256_bytes(current)
        if current is not None
        else None
    )
    if current_sha256 != lease.prior_state_sha256:
        raise CriticalPipelineError(
            "downstream authoritative state changed during transition"
        )
    _atomic_json(lease.state_path, payload)
    lease.prior_state_sha256 = _sha256(lease.state_path)
    lease.state_written = True


def _write_authoritative_latest(
    lease: _AuthoritativeStateLease,
    payload: Mapping[str, Any],
) -> None:
    if (
        not lease.active
        or not lease.state_written
        or payload.get("run_id") != lease.run_id
        or payload.get("state_sha256") != lease.prior_state_sha256
    ):
        raise CriticalPipelineError(
            "downstream latest transition is invalid"
        )
    current = _optional_regular_bytes(lease.latest_path)
    current_sha256 = (
        _sha256_bytes(current)
        if current is not None
        else None
    )
    if current_sha256 != lease.prior_latest_sha256:
        raise CriticalPipelineError(
            "downstream latest changed during transition"
        )
    _atomic_json(lease.latest_path, payload)
    lease.prior_latest_sha256 = _sha256(lease.latest_path)


def _load_scoped_products(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def validate_four_region_bundle(bundle: CollectionPlanBundle) -> None:
    plan = bundle.collection_plan
    if plan.collection_plan_id != FOUR_REGION_PLAN_ID:
        raise CriticalPipelineError("unexpected four-region collection plan")
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
        if not _STATE_ID_RE.fullmatch(run_id):
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
        lease = _begin_authoritative_state_transition(
            state_path=state_path,
            latest_path=latest_path,
            run_id=run_id,
            project_root=config.project_root,
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
            },
        }
        _write_authoritative_state(lease, state)
        pointer = {
            "schema_version": DOWNSTREAM_SCHEMA,
            "run_id": run_id,
            "state_path": state_path.relative_to(
                config.project_root
            ).as_posix(),
            "state_sha256": lease.prior_state_sha256,
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
