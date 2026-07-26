from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from app.common.config import AppConfig
from app.common.csv_io import write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.sellers.engine import SellersEngine, SellersRunScope
from app.serp.collection_plan import CollectionPlanBundle, load_collection_plan_bundle
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
BRIDGE_FIELDS = list(PRODUCT_FIELDS)


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
    duplicate_product_positions: int
    region_counts: Mapping[str, Mapping[str, int]]


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
        unique_products.setdefault(product_id, row)
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
        row.get("supplier_id", "")
        for row in seller_rows
        if row.get("supplier_id", "")
    }

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
        duplicate_product_positions=sum(
            values["duplicate_product_positions"]
            for values in region_counts.values()
        ),
        region_counts=region_counts,
    )


def write_four_region_failure_preview(
    *,
    config: AppConfig,
    run_id: str,
    error: Exception,
) -> dict[str, Any]:
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
    state = {
        "schema_version": DOWNSTREAM_SCHEMA,
        "run_id": run_id,
        "collection_plan_id": FOUR_REGION_PLAN_ID,
        "status": "failed",
        "complete": False,
        "finished_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "regions": [
            {
                "region_id": region_id,
                "status": manifest_regions.get(region_id, {}).get(
                    "status",
                    "not_started",
                ),
                "pages": int(
                    manifest_regions.get(region_id, {}).get("pages_ok", 0)
                ),
                "positions": int(
                    manifest_regions.get(region_id, {}).get("products_ok", 0)
                ),
            }
            for region_id in FOUR_REGION_IDS
        ],
        "totals": {
            "pages": int(manifest.get("totals", {}).get("pages_ok", 0)),
            "positions": int(
                manifest.get("totals", {}).get("products_ok", 0)
            ),
            "unique_products": 0,
            "unique_suppliers": 0,
            "duplicate_product_positions": int(
                manifest.get("totals", {}).get(
                    "duplicate_product_positions",
                    0,
                )
            ),
            "max_position_capacity": MAX_POSITIONS,
        },
        "sellers": {"status": "not_run"},
        "warehouse": {"status": "not_run"},
        "failure_reason": str(
            getattr(error, "code", error.__class__.__name__)
        ).replace("\n", " ")[:100],
    }
    state_path = (
        config.project_root
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / run_id
        / "state.json"
    )
    _atomic_json(state_path, state)
    return state


def run_four_region_downstream(
    *,
    config: AppConfig,
    plan_path: Path,
    run_id: str,
    sellers_factory: Callable[..., SellersEngine] = SellersEngine,
    warehouse_ingest: Callable[..., dict[str, Any]] = ingest_regional_run,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    bundle = load_collection_plan_bundle(
        project_root=config.project_root,
        plan_path=plan_path,
        region_registry_path=config.project_root / "config/wb/regions.json",
    )
    validate_four_region_bundle(bundle)
    runtime_window = bundle.collection_plan.runtime_window
    if runtime_window is None:
        raise CriticalPipelineError("four-region runtime window is missing")
    deadline = DeadlineGuard.for_runtime_window(
        runtime_window,
        resume=True,
        now=now,
    )
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
    try:
        with acquire_collection_plan_locks(
            paths=paths,
            stale_seconds=config.runtime.lock_stale_seconds,
        ):
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
                checkpoint_component=f"sellers_regional:{FOUR_REGION_PLAN_ID}",
                request_timeout_provider=deadline.request_timeout,
            )
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
                raise CriticalPipelineError("regional sellers stage is partial")
            deadline.ensure_active()
            warehouse_result = warehouse_ingest(
                project_root=config.project_root,
                run_id=run_id,
                collection_plan_id=FOUR_REGION_PLAN_ID,
                bridge_path=inputs.bridge_path,
                sellers_path=Path(str(sellers_result["mart_sellers_path"])),
                collection_manifest_path=paths.manifest_path,
            )
            if warehouse_result.get("status") not in {
                "success",
                "already_ingested",
            }:
                raise CriticalPipelineError("regional warehouse stage failed")
            deadline.ensure_active()
            state = {
                "schema_version": DOWNSTREAM_SCHEMA,
                "run_id": run_id,
                "collection_plan_id": FOUR_REGION_PLAN_ID,
                "status": "success",
                "complete": True,
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
                        values["pages"] for values in inputs.region_counts.values()
                    ),
                    "positions": inputs.positions_count,
                    "unique_products": inputs.unique_products_count,
                    "unique_suppliers": inputs.unique_suppliers_count,
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
                    "sellers_count": int(warehouse_result["sellers_count"]),
                    "legacy_yaroslavl": warehouse_result["legacy"],
                },
                "failure_reason": None,
                "artifacts": {
                    "bridge_path": inputs.bridge_path.relative_to(
                        config.project_root
                    ).as_posix(),
                    "bridge_sha256": inputs.bridge_sha256,
                    "seller_input_path": inputs.seller_input_path.relative_to(
                        config.project_root
                    ).as_posix(),
                    "seller_input_sha256": inputs.seller_input_sha256,
                },
            }
            _atomic_json(state_path, state)
            deadline.ensure_active()
            pointer = {
                "schema_version": DOWNSTREAM_SCHEMA,
                "run_id": run_id,
                "state_path": state_path.relative_to(
                    config.project_root
                ).as_posix(),
                "state_sha256": _sha256(state_path),
            }
            _atomic_json(latest_path, pointer)
            return state
    except Exception as exc:
        failure = {
            "schema_version": DOWNSTREAM_SCHEMA,
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "failed",
            "complete": False,
            "finished_at_utc": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat(),
            "regions": [],
            "totals": {
                "pages": 0,
                "positions": 0,
                "unique_products": 0,
                "unique_suppliers": 0,
                "duplicate_product_positions": 0,
                "max_position_capacity": MAX_POSITIONS,
            },
            "sellers": {"status": "not_run"},
            "warehouse": {"status": "not_run"},
            "failure_reason": exc.__class__.__name__,
        }
        _atomic_json(state_path, failure)
        raise
