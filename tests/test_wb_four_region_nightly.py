from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import sys
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from app.common.config import load_config
from app.common.exceptions import CriticalPipelineError
from app.common.run_lock import acquire_advisory_lock
from app.serp import four_region_nightly as four_region
from app.serp.collection_plan import (
    CollectionPlanValidationError,
    EffectiveEndpointPolicy,
    load_collection_plan_bundle,
)
from app.serp.collection_plan_runner import (
    CollectionPlanRunner,
    CollectionPlanRunError,
    DeadlineGuard,
    PRODUCT_FIELDS,
    ScopedPaths,
    ScopedSearchRequest,
    ScopedSearchResult,
    ScopedTransportError,
    _bounded_page_contract,
)
from app.serp.four_region_nightly import (
    FOUR_REGION_IDS,
    FOUR_REGION_PLAN_ID,
    LEGACY_NIGHTLY_START_MSK,
    PRE_CUTOVER_DOWNSTREAM_MODE,
    REVIEWED_FOUR_REGION_RUNTIME_WINDOW,
    DownstreamExecutionContract,
    FourRegionInputs,
    build_four_region_inputs,
    deterministic_seller_rows,
    run_four_region_downstream,
    validate_four_region_bundle,
)
from scripts import run_wb_four_region_nightly as four_region_launcher


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_RELATIVE = Path(
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
REGISTRY_RELATIVE = Path("config/wb/regions.json")
RUN_ID = "20260726_001600Z"


@pytest.fixture(autouse=True)
def _isolate_host_lock_v3(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        four_region_launcher,
        "require_official_live_entry_lease",
        lambda **_kwargs: None,
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _write_canonical_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_bytes(four_region._json_bytes(payload))
    path.chmod(0o600)


def test_safe_root_cause_reports_controlled_validation_detail() -> None:
    try:
        try:
            raise CollectionPlanValidationError(
                "effective_plan requires distinct dest_id_observed values"
            )
        except CollectionPlanValidationError as cause:
            raise four_region_launcher.ExecutionMatrixRunError(
                "execution matrix entry failed",
                resumable=True,
            ) from cause
    except four_region_launcher.ExecutionMatrixRunError as exc:
        assert four_region_launcher._safe_root_cause(exc) == (
            "CollectionPlanValidationError: "
            "effective_plan requires distinct dest_id_observed values"
        )


def test_safe_root_cause_does_not_render_unknown_exception_message() -> None:
    secret = "proxy-password-must-not-appear"
    try:
        try:
            raise RuntimeError(secret)
        except RuntimeError as cause:
            raise four_region_launcher.ExecutionMatrixRunError(
                "execution matrix entry failed",
                resumable=True,
            ) from cause
    except four_region_launcher.ExecutionMatrixRunError as exc:
        diagnostic = four_region_launcher._safe_root_cause(exc)
        assert diagnostic == "RuntimeError"
        assert secret not in diagnostic


def _downstream_state_paths(
    root: Path,
    *,
    run_id: str = RUN_ID,
) -> tuple[Path, Path]:
    state_path = (
        root
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / run_id
        / "state.json"
    )
    return state_path, state_path.parent.parent / "latest.json"


def _write_published_downstream_state(
    root: Path,
    *,
    run_id: str = RUN_ID,
    started_at_utc: str = "2026-07-26T00:16:00+00:00",
    finished_at_utc: str = "2026-07-26T00:30:00+00:00",
) -> tuple[Path, Path]:
    state_path, latest_path = _downstream_state_paths(root, run_id=run_id)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = _ensure_collection_manifest(
        root,
        run_id=run_id,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
    )
    if manifest_path.is_file():
        config = load_config(str(root / "config/config.yaml"))
        bundle = load_collection_plan_bundle(
            project_root=root,
            plan_path=root / PLAN_RELATIVE,
            region_registry_path=root / REGISTRY_RELATIVE,
        )
        paths = ScopedPaths.build(
            project_root=root,
            collection_plan_id=FOUR_REGION_PLAN_ID,
            run_id=run_id,
        )
        lineage = four_region._collection_lineage(
            config=config,
            bundle=bundle,
            paths=paths,
            run_id=run_id,
        )
        artifacts_root = (
            root
            / "data/marts/wb_four_region"
            / FOUR_REGION_PLAN_ID
            / run_id
        )
        artifacts_root.mkdir(parents=True, exist_ok=True)
        bridge = (
            artifacts_root
            / "regional_query_product_position_bridge.csv"
        )
        seller_input = artifacts_root / "products_for_sellers.csv"
        seller_output = (
            root
            / "data/marts/sellers_scoped"
            / FOUR_REGION_PLAN_ID
            / run_id
            / "sellers_daily.csv"
        )
        seller_output.parent.mkdir(parents=True, exist_ok=True)
        for path, content in (
            (bridge, "bridge\n"),
            (seller_input, "seller-input\n"),
            (seller_output, "seller-output\n"),
        ):
            path.write_text(content, encoding="utf-8")
            path.chmod(0o600)
        state = {
            "schema_version": "wb_four_region_downstream_v1",
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "success",
            "complete": True,
            "stage": "complete",
            "execution_contract": (
                DownstreamExecutionContract.pre_cutover().evidence()
            ),
            "finished_at_utc": finished_at_utc,
            "lineage": lineage,
            "regions": [
                {
                    "region_id": region_id,
                    "pages": 1,
                    "positions": 1,
                    "duplicate_product_positions": 0,
                    "max_position_capacity": 30000,
                }
                for region_id in FOUR_REGION_IDS
            ],
            "totals": {
                "pages": 4,
                "positions": 4,
                "unique_products": 4,
                "unique_suppliers": 4,
                "missing_supplier_products": 0,
                "duplicate_product_positions": 0,
                "max_position_capacity": 120000,
            },
            "sellers": {
                "status": "success",
                "items_ok": 4,
                "items_error": 0,
                "source_sha256": hashlib.sha256(
                    seller_input.read_bytes()
                ).hexdigest(),
                "output_path": seller_output.relative_to(root).as_posix(),
                "output_sha256": hashlib.sha256(
                    seller_output.read_bytes()
                ).hexdigest(),
            },
            "warehouse": {
                "status": "success",
                "positions_count": 4,
                "sellers_count": 4,
                "legacy_yaroslavl": {
                    "status": "source_absent",
                    "positions": 0,
                    "sellers": 0,
                },
                "ingestion_evidence": {
                    "collection_manifest_sha256": lineage[
                        "collection_manifest_sha256"
                    ],
                    "bridge_sha256": hashlib.sha256(
                        bridge.read_bytes()
                    ).hexdigest(),
                    "sellers_sha256": hashlib.sha256(
                        seller_output.read_bytes()
                    ).hexdigest(),
                },
            },
            "failure_reason": None,
            "artifacts": {
                "bridge_path": bridge.relative_to(root).as_posix(),
                "bridge_sha256": hashlib.sha256(
                    bridge.read_bytes()
                ).hexdigest(),
                "seller_input_path": seller_input.relative_to(root).as_posix(),
                "seller_input_sha256": hashlib.sha256(
                    seller_input.read_bytes()
                ).hexdigest(),
                "seller_output_path": seller_output.relative_to(root).as_posix(),
                "seller_output_sha256": hashlib.sha256(
                    seller_output.read_bytes()
                ).hexdigest(),
            },
        }
    else:
        state = {
            "schema_version": "wb_four_region_downstream_v1",
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "success",
            "complete": True,
            "stage": "complete",
        }
    _write_canonical_json(state_path, state)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    pointer = {
        "schema_version": (
            "wb_four_region_latest_v1"
            if manifest_path.is_file()
            else "wb_four_region_downstream_v1"
        ),
        "run_id": run_id,
        "state_path": state_path.relative_to(root).as_posix(),
        "state_sha256": hashlib.sha256(state_path.read_bytes()).hexdigest(),
    }
    if manifest_path.is_file():
        pointer["lineage"] = state["lineage"]
    _write_canonical_json(latest_path, pointer)
    return state_path, latest_path


def _attempt_artifacts(root: Path) -> list[Path]:
    return sorted(
        (
            root
            / "state/wb_four_region_nightly"
            / FOUR_REGION_PLAN_ID
            / "attempts"
            / RUN_ID
        ).glob("*.json")
    )


def _project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    shutil.copy2(PROJECT_ROOT / "config/config.yaml", root / "config/config.yaml")
    shutil.copytree(PROJECT_ROOT / "config/wb", root / "config/wb")
    for name in (
        "PARSER_WB_REQUEST_HEADERS_FILE",
        "PARSER_WB_PROXY_URL",
        "WB_COOKIE_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["enabled"] = True
    _write_json(plan_path, plan)
    registry_path = root / REGISTRY_RELATIVE
    registry = _read_json(registry_path)
    for region in registry["regions"]:
        if region["region_id"] in FOUR_REGION_IDS:
            region["enabled"] = True
    _write_json(registry_path, registry)
    config = load_config(str(root / "config/config.yaml"))
    return root, config, plan_path


def _ensure_collection_manifest(
    root: Path,
    *,
    run_id: str = RUN_ID,
    started_at_utc: str = "2026-07-26T00:16:00+00:00",
    finished_at_utc: str = "2026-07-26T00:30:00+00:00",
) -> Path:
    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id=FOUR_REGION_PLAN_ID,
        run_id=run_id,
    )
    if paths.manifest_path.exists():
        return paths.manifest_path
    config_path = root / "config/config.yaml"
    plan_path = root / PLAN_RELATIVE
    registry_path = root / REGISTRY_RELATIVE
    if not (
        config_path.is_file()
        and plan_path.is_file()
        and registry_path.is_file()
    ):
        return paths.manifest_path
    bundle = load_collection_plan_bundle(
        project_root=root,
        plan_path=plan_path,
        region_registry_path=registry_path,
    )
    paths.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(
        paths.manifest_path,
        {
            "schema_version": "wb_collection_plan_manifest_v2",
            "run_id": run_id,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "query_pack_id": bundle.query_pack.query_pack_id,
            "query_pack_version": bundle.query_pack.version,
            "query_pack_sha256": bundle.query_pack_sha256,
            "collection_plan_sha256": bundle.collection_plan_sha256,
            "region_registry_sha256": bundle.region_registry_sha256,
            "effective_plan_sha256": "a" * 64,
            "publication_mode": "none",
            "sellers_mode": "disabled",
            "proxy_rotation_mode": "disabled",
            "started_at_utc": started_at_utc,
            "finished_at_utc": finished_at_utc,
            "status": "success",
            "complete": True,
        },
    )
    return paths.manifest_path


def _test_collection_lineage(
    root: Path,
    config: Any,
    plan_path: Path,
    *,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    _ensure_collection_manifest(root, run_id=run_id)
    bundle = load_collection_plan_bundle(
        project_root=root,
        plan_path=plan_path,
        region_registry_path=root / REGISTRY_RELATIVE,
    )
    return four_region._collection_lineage(
        config=config,
        bundle=bundle,
        paths=ScopedPaths.build(
            project_root=root,
            collection_plan_id=FOUR_REGION_PLAN_ID,
            run_id=run_id,
        ),
        run_id=run_id,
    )


class FourRegionFakeTransport:
    request_params = {"appType": "1", "curr": "rub"}
    endpoint_urls = (
        "https://primary.example.test",
        "https://fallback.example.test",
    )
    proxy_route_sha256 = hashlib.sha256(b"four-region-test-route").hexdigest()
    endpoint_policy = EffectiveEndpointPolicy(
        selection_mode="ordered_fallbacks",
        endpoint_ids=("primary", "fallback-1"),
        pinned_endpoint_id="primary",
    )

    def __init__(
        self,
        *,
        clock: "MutableClock | None" = None,
        advance_per_search: timedelta | None = None,
        failure_call: int | None = None,
        totals_by_query: Mapping[str, int] | None = None,
        repeat_first_product_across_pages: bool = False,
        duplicate_within_page: bool = False,
        duplicate_within_page_at: int | None = None,
    ) -> None:
        self.clock = clock
        self.advance_per_search = advance_per_search
        self.failure_call = failure_call
        self.totals_by_query = dict(totals_by_query or {})
        self.repeat_first_product_across_pages = repeat_first_product_across_pages
        self.duplicate_within_page = duplicate_within_page
        self.duplicate_within_page_at = duplicate_within_page_at
        self.resolve_calls: list[str] = []
        self.search_calls: list[ScopedSearchRequest] = []
        self.egress_calls = 0
        self.closed = False
        self.destinations = {
            "moscow": "-535680",
            "rostov-on-don": "-2228364",
            "novosibirsk": "-1257786",
            "kazan": "-2133462",
        }

    def egress_identity(self, *, timeout_seconds: float) -> str:
        self.egress_calls += 1
        return "203.0.113.10"

    def resolve_destination(self, region, *, timeout_seconds: float) -> str:
        self.resolve_calls.append(region.region_id)
        return self.destinations[region.region_id]

    def search_ordered(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        self.search_calls.append(request)
        if self.clock is not None and self.advance_per_search is not None:
            self.clock.value += self.advance_per_search
        if self.failure_call == len(self.search_calls):
            raise ScopedTransportError(
                "search_http_498",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=498,
                endpoint_id="primary",
                attempted_endpoint_ids=("primary",),
            )
        region_index = FOUR_REGION_IDS.index(request.task.region_id) + 1
        query_seed = sum(ord(char) for char in request.task.query_id) % 10_000
        page_seed = request.task.page * 100
        base = region_index * 1_000_000_000 + query_seed * 100_000 + page_seed
        total = self.totals_by_query.get(request.task.query_id, 1000)
        capped_total = min(total, request.task.depth)
        products_count = min(
            100,
            max(0, capped_total - ((request.task.page - 1) * 100)),
        )
        products = [
            {
                "id": base + index,
                "imtId": 700000 + index,
                "name": f"product-{base + index}",
                "brand": "brand",
                "brandId": 42,
                "supplierId": 1000 + (index % 5),
                "supplier": "supplier",
                "sizes": [
                    {
                        "price": {
                            "basic": 12500,
                            "product": 9900,
                        }
                    }
                ],
                "discount": 21,
                "rating": 4.8,
                "feedbacks": 10,
                "totalQuantity": 20,
            }
            for index in range(1, products_count + 1)
        ]
        if (
            self.repeat_first_product_across_pages
            and request.task.page > 1
            and products
        ):
            products[0]["id"] = (
                region_index * 1_000_000_000 + query_seed * 100_000 + 101
            )
        if (
            self.duplicate_within_page
            or self.duplicate_within_page_at == request.task.page
        ) and len(products) >= 2:
            products[1]["id"] = products[0]["id"]
        return ScopedSearchResult(
            payload={"total": total, "products": products},
            endpoint_id="primary",
            dest_id_sent=request.dest_id_observed,
            attempted_endpoint_ids=("primary",),
        )

    def close(self) -> None:
        self.closed = True


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def _small_bounded_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["query_ids"] = plan["query_ids"][:2]
    plan["region_set"] = ["moscow"]
    plan["quality"]["expected_queries_per_region"] = 2
    _write_json(plan_path, plan)
    return root, config, plan_path, tuple(plan["query_ids"])


@pytest.mark.parametrize(
    ("total", "expected_counts", "terminal_reason"),
    [
        (70, [70], "payload_total_reached"),
        (217, [100, 100, 17], "payload_total_reached"),
        (1500, [100] * 10, "depth_cap_reached"),
    ],
)
def test_bounded_payload_total_contract(
    total: int,
    expected_counts: list[int],
    terminal_reason: str,
) -> None:
    for page, expected_count in enumerate(expected_counts, start=1):
        contract = _bounded_page_contract(
            {
                "total": total,
                "products": [
                    {"id": (page * 1000) + index}
                    for index in range(1, expected_count + 1)
                ],
            },
            page=page,
            depth=1000,
        )
        assert len(contract.products) == expected_count
        assert contract.terminal is (page == len(expected_counts))
        assert contract.terminal_reason == (
            terminal_reason if page == len(expected_counts) else None
        )


def test_bounded_payload_rejects_inconsistent_short_and_empty() -> None:
    with pytest.raises(
        CollectionPlanRunError,
        match="count_inconsistent_with_total",
    ):
        _bounded_page_contract(
            {"total": 217, "products": [{"id": index} for index in range(1, 100)]},
            page=1,
            depth=1000,
        )


def test_bounded_same_and_cross_page_duplicates_are_position_facts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path, query_ids = _small_bounded_project(
        tmp_path,
        monkeypatch,
    )
    plan = _read_json(plan_path)
    plan["query_ids"] = plan["query_ids"][:1]
    plan["quality"]["expected_queries_per_region"] = 1
    _write_json(plan_path, plan)
    same_page_result = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=FourRegionFakeTransport(
            totals_by_query={query_ids[0]: 217},
            duplicate_within_page=True,
        ),
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc),
        sleeper=lambda _seconds: None,
    ).run()
    assert same_page_result["totals"]["products_ok"] == 217
    assert same_page_result["totals"]["duplicate_product_positions"] == 3
    same_page_mart = (
        root
        / "data/marts/serp_scoped"
        / FOUR_REGION_PLAN_ID
        / "moscow"
        / RUN_ID
        / "products_daily.csv"
    )
    same_page_rows = list(
        csv.DictReader(same_page_mart.open(encoding="utf-8", newline=""))
    )
    first_page_rows = [row for row in same_page_rows if row["page"] == "1"]
    assert len(first_page_rows) == 100
    assert first_page_rows[0]["nmId"] == first_page_rows[1]["nmId"]
    assert [int(row["absolute_position"]) for row in first_page_rows] == list(
        range(1, 101)
    )

    root2, config2, plan_path2, query_ids2 = _small_bounded_project(
        tmp_path / "cross",
        monkeypatch,
    )
    plan2 = _read_json(plan_path2)
    plan2["query_ids"] = plan2["query_ids"][:1]
    plan2["quality"]["expected_queries_per_region"] = 1
    _write_json(plan_path2, plan2)
    result = CollectionPlanRunner(
        config=config2,
        plan_path=plan_path2,
        transport=FourRegionFakeTransport(
            totals_by_query={query_ids2[0]: 217},
            repeat_first_product_across_pages=True,
        ),
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc),
        sleeper=lambda _seconds: None,
    ).run()
    assert result["totals"]["products_ok"] == 217
    assert result["totals"]["duplicate_product_positions"] == 2
    mart_path = (
        root2
        / "data/marts/serp_scoped"
        / FOUR_REGION_PLAN_ID
        / "moscow"
        / RUN_ID
        / "products_daily.csv"
    )
    first_row = next(csv.DictReader(mart_path.open("r", encoding="utf-8")))
    assert first_row["imtId"] == "700001"
    assert first_row["brandId"] == "42"
    assert first_row["supplier_name"] == "supplier"
    assert first_row["final_price"] == "99.0"
    assert first_row["price"] == "125.0"
    assert first_row["sale_price"] == "99.0"
    assert first_row["discount"] == "21"
    assert first_row["status"] == "success"
    assert first_row["collected_at_utc"]
    with pytest.raises(CollectionPlanRunError, match="search_products_empty"):
        _bounded_page_contract(
            {"total": 70, "products": []},
            page=1,
            depth=1000,
        )


def test_depth1000_page10_duplicate_completes_exact_position_segment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path, query_ids = _small_bounded_project(
        tmp_path,
        monkeypatch,
    )
    plan = _read_json(plan_path)
    plan["query_ids"] = plan["query_ids"][:1]
    plan["quality"]["expected_queries_per_region"] = 1
    _write_json(plan_path, plan)

    result = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=FourRegionFakeTransport(
            totals_by_query={query_ids[0]: 1000},
            duplicate_within_page_at=10,
        ),
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc),
        sleeper=lambda _seconds: None,
    ).run()

    assert result["totals"] == {
        "regions_ok": 1,
        "pages_ok": 10,
        "products_ok": 1000,
        "queries_ok": 1,
        "duplicate_product_positions": 1,
    }
    segment = result["resume"]["segments"][0]
    assert segment["pages_count"] == 10
    assert segment["completion"]["duplicate_product_positions"] == 1
    mart_path = (
        root
        / "data/marts/serp_scoped"
        / FOUR_REGION_PLAN_ID
        / "moscow"
        / RUN_ID
        / "products_daily.csv"
    )
    rows = list(csv.DictReader(mart_path.open(encoding="utf-8", newline="")))
    page10 = [row for row in rows if row["page"] == "10"]
    assert len(page10) == 100
    assert page10[0]["nmId"] == page10[1]["nmId"]
    assert [int(row["absolute_position"]) for row in page10] == list(
        range(901, 1001)
    )


def test_variable_length_segments_complete_and_resume_repeats_only_failed_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path, query_ids = _small_bounded_project(
        tmp_path,
        monkeypatch,
    )
    totals = {query_ids[0]: 70, query_ids[1]: 217}
    first = FourRegionFakeTransport(
        failure_call=3,
        totals_by_query=totals,
    )
    runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=first,
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc),
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"variable-segment-test",
    )
    with pytest.raises(ScopedTransportError, match="search_http_498"):
        runner.run()
    assert [(call.task.query_id, call.task.page) for call in first.search_calls] == [
        (query_ids[0], 1),
        (query_ids[1], 1),
        (query_ids[1], 2),
    ]
    progress = _read_json(
        root
        / "state/wb_collection_plans"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "manifest.json"
    )
    assert progress["resume"]["segments"][0]["pages_count"] == 1
    assert progress["resume"]["segments"][0]["products_count"] == 70

    resumed = FourRegionFakeTransport(totals_by_query=totals)
    result = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=resumed,
        no_publish=True,
        resume_run_id=RUN_ID,
        now=lambda: datetime(2026, 7, 26, 4, 0, tzinfo=timezone.utc),
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"variable-segment-test",
    ).run()
    assert [(call.task.query_id, call.task.page) for call in resumed.search_calls] == [
        (query_ids[1], 1),
        (query_ids[1], 2),
        (query_ids[1], 3),
    ]
    assert result["totals"]["queries_ok"] == 2
    assert result["totals"]["pages_ok"] == 4
    assert result["totals"]["products_ok"] == 287


def test_tracked_four_region_plan_is_exact_and_enabled() -> None:
    bundle = load_collection_plan_bundle(
        project_root=PROJECT_ROOT,
        plan_path=PROJECT_ROOT / PLAN_RELATIVE,
        region_registry_path=PROJECT_ROOT / REGISTRY_RELATIVE,
    )
    validate_four_region_bundle(bundle)
    assert bundle.collection_plan.enabled is True
    assert bundle.collection_plan.region_set == FOUR_REGION_IDS
    assert len(bundle.collection_plan.query_ids) == 30
    assert bundle.collection_plan.query_ids == tuple(
        query.query_id for query in bundle.query_pack.queries
    )
    assert bundle.collection_plan.depth == 1000
    assert bundle.collection_plan.quality.expected_pages_per_query == 10
    assert (
        bundle.collection_plan.runtime_window
        == REVIEWED_FOUR_REGION_RUNTIME_WINDOW
    )
    registry = {region.region_id: region for region in bundle.region_registry.regions}
    assert all(registry[region_id].enabled is True for region_id in FOUR_REGION_IDS)
    assert bundle.collection_plan.publication_mode == "none"
    assert bundle.collection_plan.sellers_mode == "disabled"
    assert bundle.collection_plan.proxy_rotation_mode == "disabled"


def test_bounded_1200_page_plan_starts_just_after_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FourRegionFakeTransport(failure_call=1)
    now = datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc)
    runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=transport,
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: now,
        sleeper=lambda _seconds: None,
    )
    bundle = runner._load_bundle()
    runner._configure_runtime_deadline(bundle)
    estimate = runner._estimated_remaining_seconds(
        bundle=bundle,
        pending_pages=1200,
    )
    assert estimate < bundle.collection_plan.runtime_window.minimum_resume_window_seconds
    with pytest.raises(ScopedTransportError, match="search_http_498"):
        runner.run()
    assert transport.resolve_calls == list(FOUR_REGION_IDS)
    assert len(transport.search_calls) == 1
    snapshot = _read_json(
        root
        / "state/wb_collection_plans"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "effective_plan.json"
    )
    assert snapshot["schema_version"] == "wb_effective_collection_plan_v3"
    assert snapshot["runtime_window"] == _read_json(plan_path)["runtime_window"]


def test_bounded_plan_rejects_late_new_start_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FourRegionFakeTransport()
    now = datetime(2026, 7, 26, 20, 1, tzinfo=timezone.utc)
    runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=transport,
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: now,
    )
    with pytest.raises(CollectionPlanRunError, match="reviewed start window"):
        runner.run()
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 0


def test_bounded_plan_allows_new_start_after_previous_twelve_hour_window(
) -> None:
    now = datetime(2026, 7, 26, 11, 50, tzinfo=timezone.utc)
    guard = DeadlineGuard.for_runtime_window(
        REVIEWED_FOUR_REGION_RUNTIME_WINDOW,
        resume=False,
        now=lambda: now,
    )
    assert guard.remaining_seconds() == 21600


def test_four_region_grace_drift_rejected_by_exact_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, _config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["runtime_window"]["new_run_start_grace_seconds"] = 43200
    _write_json(plan_path, plan)
    bundle = load_collection_plan_bundle(
        project_root=plan_path.parents[3],
        plan_path=plan_path,
        region_registry_path=plan_path.parents[1] / "regions.json",
    )

    with pytest.raises(
        CriticalPipelineError,
        match="reviewed runtime contract mismatch",
    ):
        validate_four_region_bundle(bundle)


def test_cutoff_keeps_verified_segment_and_resume_starts_next_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    clock = MutableClock(
        datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc)
    )
    first = FourRegionFakeTransport(
        clock=clock,
        advance_per_search=timedelta(minutes=30),
    )
    runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=first,
        no_publish=True,
        run_id=RUN_ID,
        now=clock,
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"bounded-runtime-test",
    )
    with pytest.raises(CollectionPlanRunError, match="deadline"):
        runner.run()
    manifest_path = (
        root
        / "state/wb_collection_plans"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "manifest.json"
    )
    progress = _read_json(manifest_path)
    assert len(progress["resume"]["segments"]) == 1
    assert progress["resume"]["segments"][0]["query_id"] == (
        _read_json(plan_path)["query_ids"][0]
    )

    resume_clock = MutableClock(
        datetime(2026, 7, 26, 4, 0, tzinfo=timezone.utc)
    )
    resumed = FourRegionFakeTransport(failure_call=1)
    resume_runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=resumed,
        no_publish=True,
        resume_run_id=RUN_ID,
        now=resume_clock,
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"bounded-runtime-test",
    )
    with pytest.raises(ScopedTransportError, match="search_http_498"):
        resume_runner.run()
    assert resumed.search_calls[0].task.query_id == _read_json(plan_path)[
        "query_ids"
    ][1]


def test_deterministic_seller_rows_deduplicate_across_regions() -> None:
    rows = [
        {
            "region_id": "rostov-on-don",
            "query_id": "q1",
            "absolute_position": "1",
            "nmId": "10",
            "supplier_id": "2",
        },
        {
            "region_id": "moscow",
            "query_id": "q2",
            "absolute_position": "1",
            "nmId": "10",
            "supplier_id": "2",
        },
        {
            "region_id": "moscow",
            "query_id": "q1",
            "absolute_position": "2",
            "nmId": "20",
            "supplier_id": "3",
        },
        {
            "region_id": "moscow",
            "query_id": "q1",
            "absolute_position": "1",
            "nmId": "10",
            "supplier_id": "2",
        },
    ]
    deduplicated = deterministic_seller_rows(
        rows,
        region_ids=("moscow", "rostov-on-don"),
        query_ids=("q1", "q2"),
    )
    assert [row["nmId"] for row in deduplicated] == ["10", "20"]
    assert deduplicated[0]["region_id"] == "moscow"
    assert len(rows) == 4


def test_deterministic_seller_rows_prefers_first_nonempty_supplier() -> None:
    rows = [
        {
            "region_id": "rostov-on-don",
            "query_id": "q1",
            "absolute_position": "1",
            "nmId": "10",
            "supplier_id": "300",
        },
        {
            "region_id": "moscow",
            "query_id": "q1",
            "absolute_position": "1",
            "nmId": "10",
            "supplier_id": "",
        },
        {
            "region_id": "moscow",
            "query_id": "q1",
            "absolute_position": "2",
            "nmId": "10",
            "supplier_id": "100",
        },
        {
            "region_id": "moscow",
            "query_id": "q2",
            "absolute_position": "1",
            "nmId": "20",
            "supplier_id": "",
        },
        {
            "region_id": "rostov-on-don",
            "query_id": "q1",
            "absolute_position": "2",
            "nmId": "20",
            "supplier_id": " ",
        },
    ]
    selected = deterministic_seller_rows(
        rows,
        region_ids=("moscow", "rostov-on-don"),
        query_ids=("q1", "q2"),
    )
    assert [
        (row["nmId"], row["region_id"], row["absolute_position"], row["supplier_id"])
        for row in selected
    ] == [
        ("10", "moscow", "2", "100"),
        ("20", "moscow", "1", ""),
    ]


def test_four_region_inputs_preserve_repeated_position_facts_and_dedup_sellers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    bundle = load_collection_plan_bundle(
        project_root=root,
        plan_path=plan_path,
        region_registry_path=root / REGISTRY_RELATIVE,
    )
    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id=FOUR_REGION_PLAN_ID,
        run_id=RUN_ID,
    )
    segment_refs = []
    latest_regions = []
    total_positions = 0
    total_pages = 0
    for region in bundle.enabled_regions:
        rows: list[dict[str, Any]] = []
        for query_index, query in enumerate(bundle.enabled_queries):
            count = 2 if query_index == 0 else 1
            for position in range(1, count + 1):
                rows.append(
                    {
                        field: ""
                        for field in PRODUCT_FIELDS
                    }
                )
                rows[-1].update(
                    {
                        "run_id": RUN_ID,
                        "collection_plan_id": FOUR_REGION_PLAN_ID,
                        "query_pack_id": bundle.query_pack.query_pack_id,
                        "query_pack_version": bundle.query_pack.version,
                        "query_id": query.query_id,
                        "query": query.text,
                        "query_group": query.category_id,
                        "region_id": region.region_id,
                        "region_name": region.region_name,
                        "page": "1",
                        "position_on_page": str(position),
                        "absolute_position": str(position),
                        "nmId": (
                            f"{1000 + query_index}"
                            if query_index == 0
                            else f"{2000 + query_index}"
                        ),
                        "supplier_id": (
                            ""
                            if query_index == 1
                            or (
                                query_index == 0
                                and region.region_id == "moscow"
                                and position == 1
                            )
                            else "5001"
                        ),
                        "endpoint_id": "primary",
                    }
                )
            completion = {
                "payload_total": count,
                "capped_total": count,
                "pages_count": 1,
                "products_count": count,
                "terminal_page": 1,
                "terminal_reason": "payload_total_reached",
                "complete": True,
                "duplicate_product_positions": count - 1,
            }
            segment_refs.append(
                {
                    "region_id": region.region_id,
                    "query_id": query.query_id,
                    "completion": completion,
                }
            )
        mart_path = paths.layer_region_run_dir(
            "marts",
            region.region_id,
        ) / "products_daily.csv"
        mart_path.parent.mkdir(parents=True, exist_ok=True)
        with mart_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=PRODUCT_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        region_manifest = {
            "schema_version": "wb_regional_latest_region_v1",
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "run_id": RUN_ID,
            "region_id": region.region_id,
            "effective_plan_sha256": "a" * 64,
            "pages_count": len(bundle.enabled_queries),
            "products_count": len(rows),
            "outputs": {
                "mart_products_path": mart_path.relative_to(root).as_posix(),
                "products_sha256": hashlib.sha256(mart_path.read_bytes()).hexdigest(),
            },
        }
        region_manifest_path = paths.latest_region_manifest_path(region.region_id)
        region_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(region_manifest_path, region_manifest)
        latest_regions.append(
            {
                "region_id": region.region_id,
                "manifest_path": region_manifest_path.relative_to(root).as_posix(),
                "manifest_sha256": hashlib.sha256(
                    region_manifest_path.read_bytes()
                ).hexdigest(),
                "pages_count": len(bundle.enabled_queries),
                "products_count": len(rows),
            }
        )
        total_positions += len(rows)
        total_pages += len(bundle.enabled_queries)
    paths.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(
        paths.manifest_path,
        {
            "schema_version": "wb_collection_plan_manifest_v2",
            "run_id": RUN_ID,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "query_pack_id": bundle.query_pack.query_pack_id,
            "query_pack_version": bundle.query_pack.version,
            "status": "success",
            "complete": True,
            "collection_plan_sha256": bundle.collection_plan_sha256,
            "query_pack_sha256": bundle.query_pack_sha256,
            "region_registry_sha256": bundle.region_registry_sha256,
            "effective_plan_sha256": "a" * 64,
            "publication_mode": "none",
            "sellers_mode": "disabled",
            "proxy_rotation_mode": "disabled",
            "started_at_utc": "2026-07-26T00:16:00+00:00",
            "finished_at_utc": "2026-07-26T00:30:00+00:00",
            "totals": {
                "regions_ok": 4,
                "queries_ok": 120,
                "pages_ok": total_pages,
                "products_ok": total_positions,
                "duplicate_product_positions": 4,
            },
            "resume": {"segments": segment_refs},
        },
    )
    paths.latest_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(
        paths.latest_path,
        {
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "run_id": RUN_ID,
            "effective_plan_sha256": "a" * 64,
            "regions": latest_regions,
        },
    )
    inputs = build_four_region_inputs(
        config=config,
        bundle=bundle,
        run_id=RUN_ID,
    )
    assert inputs.positions_count == total_positions == 124
    assert inputs.duplicate_product_positions == 4
    assert inputs.unique_products_count == 30
    assert inputs.unique_suppliers_count == 1
    assert inputs.missing_supplier_products == 1
    bridge_rows = list(
        csv.DictReader(
            inputs.bridge_path.open("r", encoding="utf-8-sig", newline=""),
            delimiter=";",
        )
    )
    first_query_rows = [
        row
        for row in bridge_rows
        if row["region_id"] == "moscow"
        and row["query_id"] == bundle.enabled_queries[0].query_id
    ]
    assert [row["absolute_position"] for row in first_query_rows] == ["1", "2"]
    assert len({row["nmId"] for row in first_query_rows}) == 1
    assert [row["supplier_id"] for row in first_query_rows] == ["", "5001"]
    seller_rows = list(
        csv.DictReader(
            inputs.seller_input_path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ),
            delimiter=";",
        )
    )
    seller_by_product = {row["nmId"]: row for row in seller_rows}
    assert seller_by_product["1000"]["supplier_id"] == "5001"
    assert seller_by_product["2001"]["supplier_id"] == ""


def test_legacy_nightly_wrapper_diff_is_only_lock_v3_bootstrap_and_safe_fd() -> None:
    source = (
        PROJECT_ROOT / "scripts/run_products_sellers_daily.sh"
    ).read_text(encoding="utf-8")
    source = source.replace(
        "export PYTHONDONTWRITEBYTECODE=1\n",
        "",
        1,
    )
    source = source.replace(
        'COORDINATOR_ADAPTER="$PROJECT_DIR/scripts/'
        'wb_nightly_coordinator_adapter.py"\n'
        'COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"\n',
        "",
        1,
    )
    source = source.replace(
        '\nif [[ -e "$COORDINATOR_LOCK_DIR" '
        '|| -L "$COORDINATOR_LOCK_DIR" ]]; then\n'
        '  if [[ "${PARSER_WB_LOCK_V3_WRAPPED:-0}" != "1" ]]; then\n'
        '    exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" '
        'passthrough -- "$0" "$@"\n'
        "  fi\n"
        '  if ! "$PYTHON_BIN" "$COORDINATOR_ADAPTER" entry-check; then\n'
        '    echo "WB host lock-v3 lease validation failed" >&2\n'
        "    exit 2\n"
        "  fi\n"
        "fi\n",
        "",
        1,
    )
    source = source.replace(
        'exec {daily_lock_fd}>"$LOCK_FILE"\n'
        'if ! flock -n "$daily_lock_fd"; then\n',
        'exec 9>"$LOCK_FILE"\n'
        "if ! flock -n 9; then\n",
        1,
    )
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    assert digest == "423f1cf6efa8eb3c13b5ddcee3df183b885a757454b175240e8374e2a7d286c4"


def test_launcher_blocks_downstream_for_partial_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"downstream": False}
    monkeypatch.setattr(
        four_region_launcher,
        "validate_resumable_collection_state",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "run_collection_plan",
        lambda **_kwargs: {
            "run_id": RUN_ID,
            "status": "failed",
            "complete": False,
        },
    )

    def forbidden_downstream(**_kwargs):
        called["downstream"] = True
        raise AssertionError("downstream must not run")

    monkeypatch.setattr(
        four_region_launcher,
        "run_four_region_downstream",
        forbidden_downstream,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--config",
            "config/config.yaml",
            "--plan-file",
            str(tmp_path / "plan.json"),
            "--no-publish",
            "--resume-run-id",
            RUN_ID,
        ],
    )
    assert four_region_launcher.main() == 2
    assert called["downstream"] is False
    state_path, _latest_path = _downstream_state_paths(tmp_path)
    assert not state_path.exists()
    artifacts = _attempt_artifacts(tmp_path)
    assert len(artifacts) == 1
    attempt = _read_json(artifacts[0])
    assert [item["region_id"] for item in attempt["regions"]] == list(
        FOUR_REGION_IDS
    )
    assert attempt["stage"] == "collection"
    assert attempt["lock_ownership"] == "not_acquired"
    assert attempt["authoritative_state_changed"] is False


def test_completed_collection_resume_skips_serp_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"downstream": 0}
    bundle = SimpleNamespace(
        collection_plan=SimpleNamespace(collection_plan_id=FOUR_REGION_PLAN_ID)
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_collection_plan_bundle",
        lambda **_kwargs: bundle,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "_completed_collection_manifest",
        lambda *_args: {
            "schema_version": "wb_collection_plan_manifest_v2",
            "run_id": RUN_ID,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "success",
            "complete": True,
        },
    )

    def forbidden_collection(**_kwargs):
        raise AssertionError("completed resume must not invoke SERP collection")

    def completed_downstream(**kwargs):
        called["downstream"] += 1
        assert kwargs["run_id"] == RUN_ID
        return {"run_id": RUN_ID, "status": "success", "complete": True}

    monkeypatch.setattr(
        four_region_launcher,
        "run_collection_plan",
        forbidden_collection,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "run_four_region_downstream",
        completed_downstream,
    )

    manifest, downstream = four_region_launcher.execute_four_region_plan(
        config=SimpleNamespace(project_root=tmp_path),
        plan_path=tmp_path / "plan.json",
        run_id=RUN_ID,
        resume=True,
        downstream_only=False,
        absolute_deadline_utc=None,
        input_integrity_gate=lambda: None,
    )

    assert manifest == {
        "run_id": RUN_ID,
        "status": "previously_completed",
        "complete": True,
    }
    assert downstream["status"] == "success"
    assert called["downstream"] == 1


def test_downstream_state_reports_all_regions_and_updates_scoped_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    artifact_root = (
        root
        / "data/marts/wb_four_region"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
    )
    artifact_root.mkdir(parents=True)
    seller_input = artifact_root / "products_for_sellers.csv"
    bridge = artifact_root / "regional_query_product_position_bridge.csv"
    seller_output = (
        root
        / "data/marts/sellers_scoped"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "sellers_daily.csv"
    )
    seller_output.parent.mkdir(parents=True, exist_ok=True)
    seller_input.write_text("input\n", encoding="utf-8")
    bridge.write_text("bridge\n", encoding="utf-8")
    seller_output.write_text("sellers\n", encoding="utf-8")
    for path in (seller_input, bridge, seller_output):
        path.chmod(0o664)
    artifact_bytes = {
        path: path.read_bytes()
        for path in (seller_input, bridge, seller_output)
    }
    inputs = FourRegionInputs(
        root=artifact_root,
        seller_input_path=seller_input,
        bridge_path=bridge,
        seller_input_sha256=hashlib.sha256(seller_input.read_bytes()).hexdigest(),
        bridge_sha256=hashlib.sha256(bridge.read_bytes()).hexdigest(),
        positions_count=120000,
        unique_products_count=40000,
        unique_suppliers_count=800,
        missing_supplier_products=7,
        duplicate_product_positions=25,
        region_counts={
            region_id: {
                "pages": 300,
                "positions": 30000,
                "duplicate_product_positions": (
                    25 if region_id == FOUR_REGION_IDS[0] else 0
                ),
                "max_position_capacity": 30000,
            }
            for region_id in FOUR_REGION_IDS
        },
        collection_lineage=_test_collection_lineage(
            root,
            config,
            plan_path,
        ),
    )
    monkeypatch.setattr(
        "app.serp.four_region_nightly.build_four_region_inputs",
        lambda **_kwargs: inputs,
    )

    class FakeSellers:
        def __init__(self, **_kwargs) -> None:
            pass

        def run(self) -> dict[str, Any]:
            return {
                "status": "success",
                "items_ok": 800,
                "items_error": 0,
                "mart_sellers_path": str(seller_output),
            }

    state = run_four_region_downstream(
        config=config,
        plan_path=plan_path,
        run_id=RUN_ID,
        sellers_factory=FakeSellers,
        warehouse_ingest=lambda **_kwargs: {
            "status": "success",
            "positions_count": 120000,
            "sellers_count": 3200,
            "legacy": {
                "status": "source_absent",
                "positions": 0,
                "sellers": 0,
            },
        },
        now=lambda: datetime(
            2026,
            7,
            26,
            4,
            0,
            tzinfo=timezone.utc,
        ),
    )
    assert state["complete"] is True
    assert [item["region_id"] for item in state["regions"]] == list(
        FOUR_REGION_IDS
    )
    assert state["totals"] == {
        "pages": 1200,
        "positions": 120000,
        "unique_products": 40000,
        "unique_suppliers": 800,
        "missing_supplier_products": 7,
        "duplicate_product_positions": 25,
        "max_position_capacity": 120000,
    }
    latest = _read_json(
        root
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / "latest.json"
    )
    assert latest["run_id"] == RUN_ID
    for path, expected_bytes in artifact_bytes.items():
        assert path.read_bytes() == expected_bytes
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_downstream_artifact_finalization_rejects_unsafe_identity(
    tmp_path: Path,
) -> None:
    regular = tmp_path / "regular.csv"
    regular.write_bytes(b"safe\n")
    regular.chmod(0o664)
    alias = tmp_path / "alias.csv"
    os.link(regular, alias)

    with pytest.raises(
        CriticalPipelineError,
        match="metadata cannot be safely finalized",
    ):
        four_region._finalize_downstream_artifact(
            regular,
            project_root=tmp_path,
        )
    assert stat.S_IMODE(regular.stat().st_mode) == 0o664

    alias.unlink()
    regular.chmod(0o666)
    with pytest.raises(
        CriticalPipelineError,
        match="metadata cannot be safely finalized",
    ):
        four_region._finalize_downstream_artifact(
            regular,
            project_root=tmp_path,
        )
    assert stat.S_IMODE(regular.stat().st_mode) == 0o666


def test_downstream_failure_leaves_previous_scoped_latest_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    _old_state_path, latest_path = _write_published_downstream_state(
        root,
        run_id="20260725_001600Z",
        started_at_utc="2026-07-25T00:16:00+00:00",
        finished_at_utc="2026-07-25T00:30:00+00:00",
    )
    previous_bytes = latest_path.read_bytes()
    previous = _read_json(latest_path)
    artifact_root = root / "data/marts/wb_four_region/test"
    artifact_root.mkdir(parents=True)
    seller_input = artifact_root / "products_for_sellers.csv"
    bridge = artifact_root / "bridge.csv"
    seller_output = artifact_root / "sellers.csv"
    for path in (seller_input, bridge, seller_output):
        path.write_text("x\n", encoding="utf-8")
        path.chmod(0o600)
    inputs = FourRegionInputs(
        root=artifact_root,
        seller_input_path=seller_input,
        bridge_path=bridge,
        seller_input_sha256=hashlib.sha256(seller_input.read_bytes()).hexdigest(),
        bridge_sha256=hashlib.sha256(bridge.read_bytes()).hexdigest(),
        positions_count=120000,
        unique_products_count=1,
        unique_suppliers_count=1,
        missing_supplier_products=1,
        duplicate_product_positions=0,
        region_counts={
            region_id: {
                "pages": 300,
                "positions": 30000,
                "duplicate_product_positions": 0,
                "max_position_capacity": 30000,
            }
            for region_id in FOUR_REGION_IDS
        },
        collection_lineage=_test_collection_lineage(
            root,
            config,
            plan_path,
        ),
    )
    monkeypatch.setattr(
        "app.serp.four_region_nightly.build_four_region_inputs",
        lambda **_kwargs: inputs,
    )

    class FakeSellers:
        def __init__(self, **_kwargs) -> None:
            pass

        def run(self) -> dict[str, Any]:
            return {
                "status": "success",
                "items_ok": 1,
                "items_error": 0,
                "mart_sellers_path": str(seller_output),
            }

    with pytest.raises(RuntimeError, match="warehouse failed"):
        run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=RUN_ID,
            sellers_factory=FakeSellers,
            warehouse_ingest=lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("warehouse failed")
            ),
            now=lambda: datetime(
                2026,
                7,
                26,
                4,
                0,
                tzinfo=timezone.utc,
            ),
        )
    assert latest_path.read_bytes() == previous_bytes
    assert _read_json(latest_path) == previous
    failure = _read_json(
        root
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "state.json"
    )
    assert failure["complete"] is False
    assert failure["stage"] == "warehouse"
    assert failure["warehouse"]["status"] == "failed"
    assert failure["sellers"] == {
        "status": "success",
        "items_ok": 1,
        "items_error": 0,
        "processed_sellers": 0,
        "source_sha256": inputs.seller_input_sha256,
    }
    assert [item["region_id"] for item in failure["regions"]] == list(
        FOUR_REGION_IDS
    )
    assert failure["totals"]["pages"] == 1200
    assert failure["totals"]["positions"] == 120000
    assert failure["totals"]["unique_products"] == 1
    assert failure["totals"]["missing_supplier_products"] == 1
    assert failure["failure_reason"] == "RuntimeError"
    assert "warehouse failed" not in json.dumps(failure)


@pytest.mark.parametrize(
    "current",
    [
        datetime(2026, 7, 25, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 25, 21, 16, tzinfo=timezone.utc),
    ],
)
def test_pre_cutover_downstream_guard_rejects_before_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    current: datetime,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    lock_calls = 0

    def forbidden_locks(**_kwargs):
        nonlocal lock_calls
        lock_calls += 1
        raise AssertionError("legacy window guard must run before locks")

    monkeypatch.setattr(
        "app.serp.four_region_nightly.acquire_collection_plan_locks",
        forbidden_locks,
    )
    with pytest.raises(CriticalPipelineError, match="legacy nightly"):
        run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=RUN_ID,
            now=lambda: current,
        )
    assert lock_calls == 0
    state_path, _latest_path = _downstream_state_paths(root)
    assert not state_path.exists()
    artifacts = _attempt_artifacts(root)
    assert len(artifacts) == 1
    attempt = _read_json(artifacts[0])
    assert attempt["stage"] == "preflight"
    assert attempt["lock_ownership"] == "not_acquired"
    assert attempt["execution_contract"]["mode"] == (
        PRE_CUTOVER_DOWNSTREAM_MODE
    )
    assert attempt["execution_contract"]["legacy_nightly_start_msk"] == (
        LEGACY_NIGHTLY_START_MSK
    )


def test_pre_cutover_runtime_drift_rejected_before_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    state_before = state_path.read_bytes()
    latest_before = latest_path.read_bytes()
    plan = _read_json(plan_path)
    plan["runtime_window"]["scheduled_start_msk"] = "01:00"
    plan["runtime_window"]["new_run_start_grace_seconds"] = 79200
    _write_json(plan_path, plan)
    lock_calls = 0

    def forbidden_locks(**_kwargs):
        nonlocal lock_calls
        lock_calls += 1
        raise AssertionError("runtime contract must be checked before locks")

    monkeypatch.setattr(
        "app.serp.four_region_nightly.acquire_collection_plan_locks",
        forbidden_locks,
    )
    with pytest.raises(
        CriticalPipelineError,
        match="reviewed runtime contract mismatch",
    ):
        run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=RUN_ID,
            now=lambda: datetime(
                2026,
                7,
                25,
                21,
                5,
                tzinfo=timezone.utc,
            ),
        )
    assert lock_calls == 0
    assert state_path.read_bytes() == state_before
    assert latest_path.read_bytes() == latest_before
    artifacts = _attempt_artifacts(root)
    assert len(artifacts) == 1
    attempt = _read_json(artifacts[0])
    assert attempt["stage"] == "preflight"
    assert attempt["execution_contract"] == {
        "mode": PRE_CUTOVER_DOWNSTREAM_MODE,
        "legacy_nightly_start_msk": "00:15",
        "legacy_boundary_source": "pre_cutover_contract_v1",
        "protected_duration_seconds": 21600,
        "minimum_clearance_seconds": 1800,
    }


def test_pre_cutover_contract_allows_after_protected_window() -> None:
    contract = DownstreamExecutionContract.pre_cutover()
    contract.ensure_start_allowed(
        datetime(2026, 7, 26, 4, 0, tzinfo=timezone.utc)
    )
    assert contract.evidence()["legacy_nightly_start_msk"] == "00:15"


def test_launcher_preserves_authoritative_downstream_failure_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        four_region_launcher,
        "validate_resumable_collection_state",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )
    state_path = (
        tmp_path
        / "state/wb_four_region_nightly"
        / FOUR_REGION_PLAN_ID
        / RUN_ID
        / "state.json"
    )

    def failed_downstream(**_kwargs):
        state_path.parent.mkdir(parents=True)
        _write_json(
            state_path,
            {
                "status": "failed",
                "stage": "warehouse",
                "authoritative": True,
            },
        )
        raise CriticalPipelineError("sanitized downstream failure")

    def failed_pipeline(**kwargs):
        kwargs["on_downstream_start"]()
        return (
            {
                "run_id": RUN_ID,
                "status": "success",
                "complete": True,
            },
            failed_downstream(),
        )

    monkeypatch.setattr(
        four_region_launcher,
        "execute_four_region_plan",
        failed_pipeline,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "write_four_region_failure_attempt",
        lambda **_kwargs: pytest.fail(
            "launcher must not overwrite downstream state"
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--config",
            "config/config.yaml",
            "--plan-file",
            str(tmp_path / "plan.json"),
            "--no-publish",
            "--resume-run-id",
            RUN_ID,
        ],
    )
    assert four_region_launcher.main() == 2
    assert _read_json(state_path) == {
        "status": "failed",
        "stage": "warehouse",
        "authoritative": True,
    }


def test_launcher_rejected_published_resume_preserves_state_and_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_path, latest_path = _write_published_downstream_state(tmp_path)
    state_before = state_path.read_bytes()
    latest_before = latest_path.read_bytes()
    monkeypatch.setattr(
        four_region_launcher,
        "validate_resumable_collection_state",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )

    def rejected_resume(**_kwargs):
        raise CriticalPipelineError("sensitive-marker-should-not-persist")

    monkeypatch.setattr(
        four_region_launcher,
        "execute_four_region_plan",
        rejected_resume,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--config",
            "config/config.yaml",
            "--plan-file",
            str(tmp_path / "plan.json"),
            "--no-publish",
            "--resume-run-id",
            RUN_ID,
        ],
    )

    assert four_region_launcher.main() == 2
    assert state_path.read_bytes() == state_before
    assert latest_path.read_bytes() == latest_before
    latest = _read_json(latest_path)
    assert latest["state_sha256"] == hashlib.sha256(state_before).hexdigest()
    artifacts = _attempt_artifacts(tmp_path)
    assert len(artifacts) == 1
    attempt = _read_json(artifacts[0])
    assert attempt["failure_reason"] == "CriticalPipelineError"
    assert attempt["lock_ownership"] == "not_acquired"
    assert attempt["authoritative_state_changed"] is False
    captured = capsys.readouterr()
    assert "sensitive-marker" not in captured.err
    assert "sensitive-marker" not in artifacts[0].read_text(
        encoding="utf-8"
    )


def test_same_run_downstream_lock_contention_preserves_published_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    state_before = state_path.read_bytes()
    latest_before = latest_path.read_bytes()
    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id=FOUR_REGION_PLAN_ID,
        run_id=RUN_ID,
    )

    with acquire_advisory_lock(paths.lock_paths[0]):
        with pytest.raises(CriticalPipelineError):
            run_four_region_downstream(
                config=config,
                plan_path=plan_path,
                run_id=RUN_ID,
                now=lambda: datetime(
                    2026,
                    7,
                    26,
                    4,
                    0,
                    tzinfo=timezone.utc,
                ),
            )

    assert state_path.read_bytes() == state_before
    assert latest_path.read_bytes() == latest_before
    artifacts = _attempt_artifacts(root)
    assert len(artifacts) == 1
    attempt = _read_json(artifacts[0])
    assert attempt["stage"] == "lock_acquisition"
    assert attempt["lock_ownership"] == "not_acquired"


def test_downstream_same_run_reconcile_is_idempotent_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    state_before = state_path.read_bytes()
    latest_before = latest_path.read_bytes()

    result = run_four_region_downstream(
        config=config,
        plan_path=plan_path,
        run_id=RUN_ID,
        sellers_factory=lambda **_kwargs: pytest.fail(
            "completed state must not run sellers"
        ),
        warehouse_ingest=lambda **_kwargs: pytest.fail(
            "completed state must not run warehouse"
        ),
        now=lambda: datetime(
            2026,
            7,
            26,
            4,
            0,
            tzinfo=timezone.utc,
        ),
    )

    assert result["run_id"] == RUN_ID
    assert state_path.read_bytes() == state_before
    assert latest_path.read_bytes() == latest_before
    assert _attempt_artifacts(root) == []


def test_attempt_artifact_failure_does_not_mask_preflight_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["runtime_window"]["scheduled_start_msk"] = "01:00"
    plan["runtime_window"]["new_run_start_grace_seconds"] = 79200
    _write_json(plan_path, plan)
    monkeypatch.setattr(
        "app.serp.four_region_nightly._immutable_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("diagnostic write failed")
        ),
    )

    with pytest.raises(
        CriticalPipelineError,
        match="reviewed runtime contract mismatch",
    ):
        run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=RUN_ID,
            now=lambda: datetime(
                2026,
                7,
                25,
                21,
                5,
                tzinfo=timezone.utc,
            ),
        )


def _publication_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_prior_latest: bool = True,
) -> tuple[
    Path,
    four_region._AuthoritativeStateLease,
    Path,
    Path,
    dict[str, Any],
    bytes | None,
]:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    state_payload = _read_json(state_path)
    candidate_lineage = dict(state_payload["lineage"])
    state_path.unlink()
    latest_path.unlink()
    prior_latest: bytes | None = None
    if with_prior_latest:
        _prior_state, latest_path = _write_published_downstream_state(
            root,
            run_id="20260725_001600Z",
            started_at_utc="2026-07-25T00:16:00+00:00",
            finished_at_utc="2026-07-25T00:30:00+00:00",
        )
        prior_latest = latest_path.read_bytes()
    lease = four_region._begin_authoritative_state_transition(
        state_path=state_path,
        latest_path=latest_path,
        run_id=RUN_ID,
        project_root=root,
        candidate_lineage=candidate_lineage,
    )
    four_region._write_authoritative_state(lease, state_payload)
    pointer = {
        "schema_version": "wb_four_region_latest_v1",
        "run_id": RUN_ID,
        "state_path": state_path.relative_to(root).as_posix(),
        "state_sha256": lease.expected_state_sha256,
        "lineage": candidate_lineage,
    }
    return root, lease, state_path, latest_path, pointer, prior_latest


def test_publication_rejects_state_mutation_before_latest_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, lease, state_path, latest_path, pointer, prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    state_path.write_bytes(b'{"mutated":true}\n')
    state_path.chmod(0o600)

    with pytest.raises(
        CriticalPipelineError,
        match="publication bytes mismatch",
    ):
        four_region._write_authoritative_latest(lease, pointer)

    assert latest_path.read_bytes() == prior_latest
    assert lease.latest_published is False


def test_publication_integrity_gate_rejects_latest_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, lease, _state_path, latest_path, pointer, prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )

    def reject_drift() -> None:
        raise CriticalPipelineError("input attestation changed")

    lease.integrity_gate = reject_drift
    with pytest.raises(CriticalPipelineError, match="input attestation changed"):
        four_region._write_authoritative_latest(lease, pointer)

    assert latest_path.read_bytes() == prior_latest
    assert lease.latest_published is False


def test_completed_unpublished_reconcile_publishes_without_stage_rerun(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, lease, state_path, latest_path, pointer, prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    state_before = state_path.read_bytes()
    state_inode = state_path.stat().st_ino
    original_replace = four_region._atomic_replace_bytes

    def fail_before_latest(path: Path, **kwargs) -> None:
        if path == latest_path:
            raise OSError("injected replace failure")
        original_replace(path, **kwargs)

    monkeypatch.setattr(
        four_region,
        "_atomic_replace_bytes",
        fail_before_latest,
    )
    with pytest.raises(OSError, match="injected replace failure"):
        four_region._write_authoritative_latest(lease, pointer)
    assert latest_path.read_bytes() == prior_latest

    monkeypatch.setattr(
        four_region,
        "_atomic_replace_bytes",
        original_replace,
    )
    retry = four_region._begin_authoritative_state_transition(
        state_path=state_path,
        latest_path=latest_path,
        run_id=RUN_ID,
        project_root=root,
        candidate_lineage=pointer["lineage"],
    )
    assert retry.reconcile_only is True
    assert retry.already_published is False
    retry_pointer = {
        **pointer,
        "state_sha256": retry.expected_state_sha256,
    }
    four_region._write_authoritative_latest(retry, retry_pointer)

    assert state_path.read_bytes() == state_before
    assert state_path.stat().st_ino == state_inode
    assert _read_json(latest_path)["run_id"] == RUN_ID


def test_crash_after_latest_replace_keeps_consistent_candidate_for_reconcile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, lease, state_path, latest_path, pointer, _prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    original_replace = four_region._atomic_replace_bytes

    def crash_after_replace(path: Path, **kwargs) -> None:
        original_replace(path, **kwargs)
        if path == latest_path:
            raise RuntimeError("injected crash after replace")

    monkeypatch.setattr(
        four_region,
        "_atomic_replace_bytes",
        crash_after_replace,
    )
    with pytest.raises(RuntimeError, match="injected crash"):
        four_region._write_authoritative_latest(lease, pointer)

    latest_before = latest_path.read_bytes()
    assert _read_json(latest_path)["run_id"] == RUN_ID
    assert _read_json(latest_path)["state_sha256"] == hashlib.sha256(
        state_path.read_bytes()
    ).hexdigest()
    retry = four_region._begin_authoritative_state_transition(
        state_path=state_path,
        latest_path=latest_path,
        run_id=RUN_ID,
        project_root=root,
        candidate_lineage=pointer["lineage"],
    )
    assert retry.already_published is True
    monkeypatch.setattr(
        four_region,
        "_atomic_replace_bytes",
        original_replace,
    )
    four_region._write_authoritative_latest(retry, pointer)
    assert latest_path.read_bytes() == latest_before


def test_post_replace_second_writer_is_never_rolled_back_to_older_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, lease, _state_path, latest_path, pointer, _prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    original_replace = four_region._atomic_replace_bytes
    newer_run_id = "20260727_001600Z"

    def install_newer_after_candidate(path: Path, **kwargs) -> None:
        original_replace(path, **kwargs)
        if path == latest_path:
            _write_published_downstream_state(
                root,
                run_id=newer_run_id,
                started_at_utc="2026-07-27T00:16:00+00:00",
                finished_at_utc="2026-07-27T00:30:00+00:00",
            )

    monkeypatch.setattr(
        four_region,
        "_atomic_replace_bytes",
        install_newer_after_candidate,
    )
    with pytest.raises(
        CriticalPipelineError,
        match="publication bytes mismatch",
    ):
        four_region._write_authoritative_latest(lease, pointer)

    assert _read_json(latest_path)["run_id"] == newer_run_id


def test_directory_fsync_failure_never_triggers_compensating_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, lease, state_path, latest_path, pointer, _prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    original_fsync = four_region._fsync_directory
    failed = False

    def fail_latest_fsync(path: Path) -> None:
        nonlocal failed
        if path == latest_path.parent and not failed:
            failed = True
            raise OSError("injected latest fsync failure")
        original_fsync(path)

    monkeypatch.setattr(four_region, "_fsync_directory", fail_latest_fsync)
    with pytest.raises(OSError, match="injected latest fsync failure"):
        four_region._write_authoritative_latest(lease, pointer)

    assert _read_json(latest_path)["run_id"] == RUN_ID
    assert _read_json(latest_path)["state_sha256"] == hashlib.sha256(
        state_path.read_bytes()
    ).hexdigest()
    assert four_region._validate_latest_bytes(
        latest_path.read_bytes(),
        project_root=root,
    )[0]["run_id"] == RUN_ID


def test_minimal_completed_state_is_rejected_before_reconcile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    lineage = _test_collection_lineage(root, config, plan_path)
    state_path, latest_path = _downstream_state_paths(root)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    _write_canonical_json(
        state_path,
        {
            "schema_version": "wb_four_region_downstream_v1",
            "run_id": RUN_ID,
            "collection_plan_id": FOUR_REGION_PLAN_ID,
            "status": "success",
            "complete": True,
        },
    )

    with pytest.raises(
        CriticalPipelineError,
        match="state contract mismatch",
    ):
        four_region._begin_authoritative_state_transition(
            state_path=state_path,
            latest_path=latest_path,
            run_id=RUN_ID,
            project_root=root,
            candidate_lineage=lineage,
        )


def test_older_completed_reconcile_cannot_replace_newer_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    old_state, latest_path = _write_published_downstream_state(
        root,
        run_id=RUN_ID,
        started_at_utc="2026-07-26T00:16:00+00:00",
        finished_at_utc="2026-07-26T00:30:00+00:00",
    )
    latest_path.unlink()
    _new_state, latest_path = _write_published_downstream_state(
        root,
        run_id="20260727_001600Z",
        started_at_utc="2026-07-27T00:16:00+00:00",
        finished_at_utc="2026-07-27T00:30:00+00:00",
    )
    old_state_before = old_state.read_bytes()
    latest_before = latest_path.read_bytes()

    with pytest.raises(
        CriticalPipelineError,
        match="latest is newer",
    ):
        run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=RUN_ID,
            sellers_factory=lambda **_kwargs: pytest.fail(
                "older reconcile must not run sellers"
            ),
            warehouse_ingest=lambda **_kwargs: pytest.fail(
                "older reconcile must not run warehouse"
            ),
            now=lambda: datetime(
                2026,
                7,
                26,
                4,
                0,
                tzinfo=timezone.utc,
            ),
        )

    assert old_state.read_bytes() == old_state_before
    assert latest_path.read_bytes() == latest_before


def test_newer_completed_state_can_replace_older_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, lease, _state_path, latest_path, pointer, _prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    four_region._write_authoritative_latest(lease, pointer)
    assert _read_json(latest_path)["run_id"] == RUN_ID


def test_equal_lineage_time_for_different_run_cannot_replace_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    state_payload = _read_json(state_path)
    state_path.unlink()
    latest_path.unlink()
    _other_state, latest_path = _write_published_downstream_state(
        root,
        run_id="20260726_001601Z",
        started_at_utc="2026-07-26T00:16:00+00:00",
        finished_at_utc="2026-07-26T00:31:00+00:00",
    )
    latest_before = latest_path.read_bytes()
    _write_canonical_json(state_path, state_payload)
    lineage = _test_collection_lineage(root, config, plan_path)

    with pytest.raises(CriticalPipelineError, match="latest is newer"):
        four_region._begin_authoritative_state_transition(
            state_path=state_path,
            latest_path=latest_path,
            run_id=RUN_ID,
            project_root=root,
            candidate_lineage=lineage,
        )

    assert latest_path.read_bytes() == latest_before


@pytest.mark.parametrize(
    ("field_path", "replacement"),
    [
        (("artifacts", "bridge_sha256"), "0" * 64),
        (("warehouse", "status"), "failed"),
        (("lineage", "collection_manifest_sha256"), "1" * 64),
    ],
)
def test_completed_reconcile_rejects_malformed_hash_or_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_path: tuple[str, str],
    replacement: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    state_path, latest_path = _write_published_downstream_state(root)
    latest_path.unlink()
    payload = _read_json(state_path)
    payload[field_path[0]][field_path[1]] = replacement
    _write_canonical_json(state_path, payload)
    lineage = _test_collection_lineage(root, config, plan_path)

    with pytest.raises(CriticalPipelineError):
        four_region._begin_authoritative_state_transition(
            state_path=state_path,
            latest_path=latest_path,
            run_id=RUN_ID,
            project_root=root,
            candidate_lineage=lineage,
        )


def test_authoritative_lease_rejects_inactive_and_reused_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, lease, _state_path, _latest_path, pointer, _prior_latest = (
        _publication_transaction(tmp_path, monkeypatch)
    )
    four_region._write_authoritative_latest(lease, pointer)

    with pytest.raises(
        CriticalPipelineError,
        match="latest transition is invalid",
    ):
        four_region._write_authoritative_latest(lease, pointer)
    with pytest.raises(
        CriticalPipelineError,
        match="state lease is not writable",
    ):
        four_region._write_authoritative_state(
            lease,
            {
                "schema_version": "wb_four_region_downstream_v1",
                "run_id": RUN_ID,
                "collection_plan_id": FOUR_REGION_PLAN_ID,
                "status": "failed",
                "complete": False,
            },
        )
    lease.active = False
    lease.latest_published = False
    with pytest.raises(
        CriticalPipelineError,
        match="latest transition is invalid",
    ):
        four_region._write_authoritative_latest(lease, pointer)


def test_attempt_artifacts_use_concurrent_unique_ids_and_reject_unsafe_fields(
    tmp_path: Path,
) -> None:
    config = SimpleNamespace(project_root=tmp_path)
    created_at = datetime(2026, 7, 26, 20, 0, tzinfo=timezone.utc)

    def create_attempt(_index: int) -> dict[str, Any] | None:
        return four_region.write_four_region_failure_attempt(
            config=config,
            run_id=RUN_ID,
            error=RuntimeError("sensitive-marker-must-not-persist"),
            stage="preflight",
            lock_ownership="not_acquired",
            now=lambda: created_at,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(create_attempt, range(16)))

    assert all(result is not None for result in results)
    attempt_ids = {
        str(result["attempt_id"])
        for result in results
        if result is not None
    }
    assert len(attempt_ids) == 16
    artifacts = _attempt_artifacts(tmp_path)
    assert len(artifacts) == 16
    assert all(
        "sensitive-marker" not in path.read_text(encoding="utf-8")
        for path in artifacts
    )

    assert (
        four_region.write_four_region_failure_attempt(
            config=config,
            run_id="../escape",
            error=RuntimeError("ignored"),
        )
        is None
    )
    assert (
        four_region.write_four_region_failure_attempt(
            config=config,
            run_id=RUN_ID,
            error=RuntimeError("ignored"),
            stage="sensitive-marker",
        )
        is None
    )
    assert (
        four_region.write_four_region_failure_attempt(
            config=config,
            run_id=RUN_ID,
            error=RuntimeError("ignored"),
            attempt_id="../escape",
        )
        is None
    )
    assert (
        four_region.write_four_region_failure_attempt(
            config=config,
            run_id=RUN_ID,
            error=RuntimeError("ignored"),
            lock_ownership="unknown",
        )
        is None
    )
    assert len(_attempt_artifacts(tmp_path)) == 16
