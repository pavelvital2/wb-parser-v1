from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import socket
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest
import requests

from app.common.cli import build_parser
from app.common.config import load_config
from app.common.exceptions import RunLockedError
from app.common.run_lock import acquire_advisory_lock, acquire_run_lock
from app.serp.collection_plan import (
    EffectiveEndpointPolicy,
    canonical_effective_plan_sha256,
    exact_file_sha256,
)
from app.serp.collection_plan_runner import (
    CollectionPlanRunError,
    DeadlineGuard,
    RequestsScopedTransport,
    ScopedPaths,
    ScopedSearchRequest,
    ScopedSearchResult,
    ScopedTransportError,
    acquire_collection_plan_locks,
    run_collection_plan,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_RELATIVE = Path(
    "config/wb/collection_plans/shevron-moscow-rostov-top100-pilot-v1.json"
)
PACK_RELATIVE = Path(
    "config/wb/query_packs/shevron-core/2026-07-26.1.json"
)
REGIONS_RELATIVE = Path("config/wb/regions.json")
RUN_ID = "20260726_120000Z"
FIXED_NOW = datetime(2026, 7, 26, 9, 0, 0, tzinfo=timezone.utc)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
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

    registry_path = root / REGIONS_RELATIVE
    registry = _read_json(registry_path)
    for region in registry["regions"]:
        region["enabled"] = True
    _write_json(registry_path, registry)
    return root, load_config(str(root / "config/config.yaml")), plan_path


def _products(seed: int, count: int = 100) -> list[dict[str, Any]]:
    return [
        {
            "id": seed + index,
            "name": f"product-{seed + index}",
            "brand": "test-brand",
            "supplierId": 4516781,
            "rating": 4.8,
            "feedbacks": 100,
            "totalQuantity": 25,
        }
        for index in range(1, count + 1)
    ]


class FakeTransport:
    request_params = {"appType": "1", "dest": "-legacy", "curr": "rub"}
    endpoint_policy = EffectiveEndpointPolicy(
        selection_mode="ordered_fallbacks",
        endpoint_ids=("primary", "fallback-1"),
        pinned_endpoint_id="primary",
    )

    def __init__(
        self,
        *,
        destinations: Mapping[str, str] | None = None,
        egress_values: list[str] | None = None,
        failure_call: int | None = None,
        failure_code: str = "search_http_498",
        product_count: int = 100,
        duplicate: bool = False,
        malformed: bool = False,
        empty: bool = False,
    ) -> None:
        self.destinations = dict(
            destinations
            or {
                "moscow": "-535680",
                "rostov-on-don": "-2228364",
            }
        )
        self.egress_values = list(egress_values or ["203.0.113.10"] * 3)
        self.failure_call = failure_call
        self.failure_code = failure_code
        self.product_count = product_count
        self.duplicate = duplicate
        self.malformed = malformed
        self.empty = empty
        self.resolve_calls: list[str] = []
        self.search_calls: list[ScopedSearchRequest] = []
        self.egress_calls = 0
        self.events: list[str] = []
        self.closed = False
        self.secret_header = "Bearer this-value-must-never-be-persisted"

    def egress_identity(self, *, timeout_seconds: float) -> str:
        self.events.append("egress")
        index = min(self.egress_calls, len(self.egress_values) - 1)
        self.egress_calls += 1
        return self.egress_values[index]

    def resolve_destination(self, region, *, timeout_seconds: float) -> str:
        self.events.append(f"resolve:{region.region_id}")
        self.resolve_calls.append(region.region_id)
        return self.destinations[region.region_id]

    def search(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        self.events.append(
            f"search:{request.task.region_id}:{request.task.query_id}"
        )
        self.search_calls.append(request)
        if self.failure_call == len(self.search_calls):
            raise ScopedTransportError(
                self.failure_code,
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=498,
            )
        if self.empty:
            products: list[Any] = []
        elif self.malformed:
            products = [{"id": "not-an-id"}] * self.product_count
        else:
            region_offset = 100_000 if request.task.region_id == "moscow" else 200_000
            query_offset = {
                "shevron": 1_000,
                "shevrony": 2_000,
                "shevron-na-lipuchke": 3_000,
            }[request.task.query_id]
            products = _products(region_offset + query_offset, self.product_count)
            if self.duplicate and len(products) > 1:
                products[1]["id"] = products[0]["id"]
        return ScopedSearchResult(
            payload={"products": products},
            endpoint_id=request.endpoint_id,
            dest_id_sent=request.dest_id_observed,
        )

    def close(self) -> None:
        self.closed = True


def _run(config, plan_path: Path, transport: FakeTransport, **kwargs):
    return run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=transport,
        run_id=RUN_ID,
        now=lambda: FIXED_NOW,
        egress_hash_salt=b"test-only-salt",
        **kwargs,
    )


def test_successful_runner_writes_only_scoped_outputs_and_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()

    def forbidden_network(*args, **kwargs):
        raise AssertionError("real network is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden_network)
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden_network)
    manifest = _run(config, plan_path, transport)

    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    stored_manifest = _read_json(state_dir / "manifest.json")
    snapshot = _read_json(state_dir / "effective_plan.json")
    assert manifest == stored_manifest
    assert manifest["status"] == "success"
    assert manifest["complete"] is True
    assert manifest["totals"] == {
        "regions_ok": 2,
        "pages_ok": 6,
        "products_ok": 600,
    }
    assert manifest["query_pack_sha256"] == exact_file_sha256(root / PACK_RELATIVE)
    assert manifest["collection_plan_sha256"] == exact_file_sha256(plan_path)
    assert manifest["region_registry_sha256"] == exact_file_sha256(
        root / REGIONS_RELATIVE
    )
    assert manifest["effective_plan_sha256"] == canonical_effective_plan_sha256(
        snapshot
    )
    assert [item["dest_resolution_status"] for item in manifest["regions"]] == [
        "resolved_and_sent",
        "resolved_and_sent",
    ]
    assert manifest["egress"]["masked"] == "203.0.x.x"
    assert "203.0.113.10" not in json.dumps(manifest)

    assert transport.resolve_calls == ["moscow", "rostov-on-don"]
    assert transport.egress_calls == 3
    assert len(transport.search_calls) == 6
    assert transport.events[:3] == [
        "egress",
        "resolve:moscow",
        "resolve:rostov-on-don",
    ]
    for request in transport.search_calls:
        assert request.params["dest"] == request.dest_id_observed
        assert request.params["query"] == request.task.query
        assert request.params["page"] == "1"
        assert request.task.checkpoint_key == "|".join(
            (
                request.task.collection_plan_id,
                request.task.query_pack_version,
                request.task.region_id,
                request.task.query_id,
                "1",
            )
        )

    for region_id in ("moscow", "rostov-on-don"):
        raw_dir = (
            root
            / "data/raw/serp_scoped"
            / "shevron-moscow-rostov-top100-pilot-v1"
            / region_id
            / RUN_ID
        )
        assert (raw_dir / "products_raw.csv").exists()
        assert (raw_dir / "pages_raw_index.csv").exists()
        assert (
            root
            / "data/staging/serp_scoped"
            / "shevron-moscow-rostov-top100-pilot-v1"
            / region_id
            / RUN_ID
            / "products_staging.csv"
        ).exists()
        mart_path = (
            root
            / "data/marts/serp_scoped"
            / "shevron-moscow-rostov-top100-pilot-v1"
            / region_id
            / RUN_ID
            / "products_daily.csv"
        )
        with mart_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 300
        assert {row["region_id"] for row in rows} == {region_id}
        assert {row["collection_scope"] for row in rows} == {"regional"}

    moscow_checkpoint = _read_json(
        state_dir / "checkpoints/moscow/shevron/page_001.json"
    )
    rostov_checkpoint = _read_json(
        state_dir / "checkpoints/rostov-on-don/shevron/page_001.json"
    )
    assert moscow_checkpoint["checkpoint_key"] != rostov_checkpoint["checkpoint_key"]
    assert "|moscow|shevron|1" in moscow_checkpoint["checkpoint_key"]
    assert "|rostov-on-don|shevron|1" in rostov_checkpoint["checkpoint_key"]

    assert not (root / "data/raw/serp/latest").exists()
    assert not (root / "data/staging/serp/latest").exists()
    assert not (root / "data/marts/serp/latest").exists()
    assert not (root / "exports/products_for_sellers.csv").exists()
    assert not (root / "state/run_reports/latest.json").exists()
    assert not (root / "data/warehouse").exists()

    serialized = (state_dir / "effective_plan.json").read_text(encoding="utf-8")
    serialized += (state_dir / "manifest.json").read_text(encoding="utf-8")
    assert transport.secret_header not in serialized
    assert "cookie" not in serialized.lower()
    assert "authorization" not in serialized.lower()
    assert "proxy_url" not in serialized.lower()


def test_protected_global_files_are_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    protected = [
        root / "exports/products_for_sellers.csv",
        root / "state/run_reports/latest.json",
        root / "data/raw/serp/latest/pages_raw_index.csv",
        root / "data/staging/serp/latest/products_staging.csv",
        root / "data/marts/serp/latest/products_daily.csv",
        root / "data/warehouse/wb/wb.duckdb",
    ]
    for index, path in enumerate(protected):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"protected-{index}".encode())
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected}

    _run(config, plan_path, FakeTransport())

    after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected}
    assert after == before


def test_all_write_opens_are_limited_to_locks_provenance_and_scoped_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    original_open = os.open
    write_paths: list[Path] = []

    def tracked_open(path, flags, *args, **kwargs):
        write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
        if flags & write_flags:
            write_paths.append(Path(path).absolute())
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", tracked_open)
    _run(config, plan_path, FakeTransport())

    allowed_prefixes = (
        root / "state/locks",
        root / "state/wb_collection_plans",
        root / "data/raw/serp_scoped",
        root / "data/staging/serp_scoped",
        root / "data/marts/serp_scoped",
    )
    assert write_paths
    for path in write_paths:
        assert any(
            path == prefix or prefix in path.parents for prefix in allowed_prefixes
        ), path


@pytest.mark.parametrize("failure_name", ["daily", "pipeline", "warehouse", "collection_plan"])
def test_failure_at_each_lock_releases_all_earlier_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_name: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()

    def fail_before_acquire(event: str, name: str, path: Path) -> None:
        if event == "before_acquire" and name == failure_name:
            raise CollectionPlanRunError(f"forced-{name}")

    with pytest.raises(CollectionPlanRunError, match="forced"):
        _run(
            config,
            plan_path,
            transport,
            lock_event_hook=fail_before_acquire,
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert not (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    ).exists()

    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id="shevron-moscow-rostov-top100-pilot-v1",
        run_id="20260726_120001Z",
    )
    with acquire_collection_plan_locks(paths=paths, stale_seconds=21600):
        pass


def test_source_change_during_lock_acquisition_fails_before_provenance_or_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()

    def mutate_source(event: str, name: str, path: Path) -> None:
        if event == "before_acquire" and name == "collection_plan":
            plan_path.write_bytes(plan_path.read_bytes() + b" ")

    with pytest.raises(CollectionPlanRunError, match="sources changed"):
        _run(
            config,
            plan_path,
            transport,
            lock_event_hook=mutate_source,
        )
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert not (
        root / "state/wb_collection_plans/provenance/query_pack_versions.json"
    ).exists()


def test_held_warehouse_lock_blocks_before_http_and_scoped_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()
    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id="shevron-moscow-rostov-top100-pilot-v1",
        run_id=RUN_ID,
    )

    with acquire_advisory_lock(paths.lock_paths[2]):
        with pytest.raises(RunLockedError):
            _run(config, plan_path, transport)
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert not paths.state_run_dir.exists()

    with acquire_collection_plan_locks(
        paths=ScopedPaths.build(
            project_root=root,
            collection_plan_id=paths.collection_plan_id,
            run_id="20260726_120001Z",
        ),
        stale_seconds=21600,
    ):
        pass


def test_held_daily_lock_blocks_before_http_and_scoped_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()
    paths = ScopedPaths.build(
        project_root=root,
        collection_plan_id="shevron-moscow-rostov-top100-pilot-v1",
        run_id=RUN_ID,
    )

    with acquire_advisory_lock(paths.lock_paths[0]):
        with pytest.raises(RunLockedError):
            _run(config, plan_path, transport)
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert not paths.state_run_dir.exists()


def test_deterministic_contention_allows_exactly_one_complete_lock_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _config, _plan_path = _project(tmp_path, monkeypatch)
    first = ScopedPaths.build(
        project_root=root,
        collection_plan_id="shevron-moscow-rostov-top100-pilot-v1",
        run_id="20260726_120000Z",
    )
    second = ScopedPaths.build(
        project_root=root,
        collection_plan_id=first.collection_plan_id,
        run_id="20260726_120001Z",
    )

    with acquire_collection_plan_locks(paths=first, stale_seconds=21600):
        with pytest.raises(RunLockedError):
            with acquire_collection_plan_locks(paths=second, stale_seconds=21600):
                pass
        with pytest.raises(RunLockedError):
            with acquire_advisory_lock(first.lock_paths[0]):
                pass
        with pytest.raises(RunLockedError):
            with acquire_advisory_lock(first.lock_paths[2]):
                pass

    with acquire_collection_plan_locks(paths=second, stale_seconds=21600):
        pass


def test_all_locks_remain_held_through_final_manifest_fsync_and_release_reverse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    release_events: list[str] = []
    checked = False

    def lock_hook(event: str, name: str, path: Path) -> None:
        if event == "before_release":
            release_events.append(name)

    def write_hook(event: str, path: Path) -> None:
        nonlocal checked
        if event != "file_fsynced" or path.name != "manifest.json":
            return
        checked = True
        lock_dir = root / "state/locks"
        for name in (
            "products_sellers_daily.flock",
            "wb_warehouse_refresh.flock",
            "wb_collection_plan.flock",
        ):
            with pytest.raises(RunLockedError):
                with acquire_advisory_lock(lock_dir / name):
                    pass
        with pytest.raises(RunLockedError):
            with acquire_run_lock(
                state_dir=root / "state",
                target="probe",
                run_id="probe",
                enabled=True,
                stale_seconds=21600,
                guard_blocking=False,
            ):
                pass

    _run(
        config,
        plan_path,
        FakeTransport(),
        lock_event_hook=lock_hook,
        write_event_hook=write_hook,
    )
    assert checked
    assert release_events == ["collection_plan", "warehouse", "pipeline", "daily"]


def test_partial_http_failure_has_no_retry_and_cannot_be_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport(failure_call=2, failure_code="search_http_498")

    with pytest.raises(ScopedTransportError, match="search_http_498"):
        _run(config, plan_path, transport)
    assert len(transport.search_calls) == 2
    assert transport.resolve_calls == ["moscow", "rostov-on-don"]

    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    manifest = _read_json(state_dir / "manifest.json")
    assert manifest["status"] == "failed"
    assert manifest["complete"] is False
    assert manifest["regions"][0]["dest_resolution_status"] == "resolved_and_sent"
    assert manifest["regions"][0]["pages_ok"] == 1
    assert manifest["regions"][1]["status"] == "pending"
    checkpoints = list((state_dir / "checkpoints").rglob("*.json"))
    assert len(checkpoints) == 1
    assert "rotation" not in manifest["error"]["error_code"]


@pytest.mark.parametrize(
    ("transport", "error_match"),
    [
        (FakeTransport(empty=True), "products_empty"),
        (FakeTransport(product_count=99), "products_short"),
        (FakeTransport(duplicate=True), "product_duplicate"),
        (FakeTransport(malformed=True), "product_id_malformed"),
    ],
)
def test_payload_failures_are_fail_closed_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport: FakeTransport,
    error_match: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    with pytest.raises(CollectionPlanRunError, match=error_match):
        _run(config, plan_path, transport)
    assert len(transport.search_calls) == 1
    manifest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "manifest.json"
    )
    assert manifest["complete"] is False


def test_equal_destinations_fail_before_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport(
        destinations={"moscow": "-535680", "rostov-on-don": "-535680"}
    )
    with pytest.raises(Exception, match="distinct"):
        _run(config, plan_path, transport)
    assert transport.search_calls == []
    assert not (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "effective_plan.json"
    ).exists()


def test_changed_egress_stops_before_second_region(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport(
        egress_values=["203.0.113.10", "203.0.113.11"],
    )
    with pytest.raises(CollectionPlanRunError, match="egress identity changed"):
        _run(config, plan_path, transport)
    assert len(transport.search_calls) == 3
    manifest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "manifest.json"
    )
    assert manifest["complete"] is False
    assert manifest["egress"]["constant"] is False


def test_effective_snapshot_is_immutable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    _run(config, plan_path, FakeTransport())
    snapshot_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "effective_plan.json"
    )
    original = snapshot_path.read_bytes()
    second_transport = FakeTransport()
    with pytest.raises(CollectionPlanRunError, match="run state already exists"):
        _run(config, plan_path, second_transport)
    assert snapshot_path.read_bytes() == original
    assert second_transport.egress_calls == 0
    assert second_transport.resolve_calls == []


def test_deadline_rejects_start_before_locks_or_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport()
    too_late = datetime(2026, 7, 26, 20, 42, 0, tzinfo=timezone.utc)
    with pytest.raises(CollectionPlanRunError, match="23:45"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            run_id=RUN_ID,
            now=lambda: too_late,
        )
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert not (root / "state/wb_collection_plans").exists()


def test_deadline_bounds_request_timeout() -> None:
    now = datetime(2026, 7, 26, 20, 35, 0, tzinfo=timezone.utc)
    guard = DeadlineGuard.for_current_day(now=lambda: now)
    assert 0 < guard.request_timeout(3_600) < 600


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: Any = None,
        text: str = "",
        json_error: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self.payload = payload
        self.text = text
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    def close(self) -> None:
        pass


def _requests_transport(session: FakeSession) -> RequestsScopedTransport:
    return RequestsScopedTransport(
        session=session,  # type: ignore[arg-type]
        request_params={"appType": "1"},
        endpoint_urls=("https://search.example.test",),
        timeout_seconds=45,
        referer_base="https://www.wildberries.ru/search?query=",
        resolver_url="https://geo.example.test",
        egress_check_url="https://ip.example.test",
        egress_session=session,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    ("payload", "error_match"),
    [
        ({}, "missing_xinfo"),
        ({"xinfo": "appType=1&curr=rub"}, "missing_or_duplicate_dest"),
        ({"xinfo": "dest=-1&dest=-2"}, "missing_or_duplicate_dest"),
        ({"xinfo": "dest=Bearer%20secret"}, "invalid_dest"),
        ({"xinfo": "not-a-query-string"}, "malformed_xinfo"),
    ],
)
def test_resolver_rejects_malformed_xinfo(
    payload: Mapping[str, Any],
    error_match: str,
) -> None:
    session = FakeSession([FakeResponse(payload=payload)])
    transport = _requests_transport(session)
    region = type(
        "Region",
        (),
        {
            "latitude": "55.0",
            "longitude": "37.0",
            "address_label": "Москва",
        },
    )()
    with pytest.raises(ScopedTransportError, match=error_match):
        transport.resolve_destination(region, timeout_seconds=10)
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    ("response", "error_match"),
    [
        (FakeResponse(status_code=429), "resolver_http_429"),
        (FakeResponse(status_code=498), "resolver_http_498"),
        (
            FakeResponse(json_error=ValueError("invalid")),
            "resolver_invalid_json",
        ),
        (FakeResponse(payload=[]), "resolver_payload_not_object"),
    ],
)
def test_resolver_http_and_json_failures_have_no_retry(
    response: FakeResponse,
    error_match: str,
) -> None:
    session = FakeSession([response])
    transport = _requests_transport(session)
    region = type(
        "Region",
        (),
        {
            "latitude": "55.0",
            "longitude": "37.0",
            "address_label": "Москва",
        },
    )()
    with pytest.raises(ScopedTransportError, match=error_match):
        transport.resolve_destination(region, timeout_seconds=10)
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    ("response", "error_match"),
    [
        (FakeResponse(status_code=429), "search_http_429"),
        (FakeResponse(status_code=498), "search_http_498"),
        (
            FakeResponse(json_error=ValueError("invalid")),
            "search_invalid_json",
        ),
        (FakeResponse(payload=[]), "payload_not_object"),
    ],
)
def test_search_http_and_json_failures_have_no_retry(
    response: FakeResponse,
    error_match: str,
) -> None:
    session = FakeSession([response])
    transport = _requests_transport(session)
    task = type("Task", (), {"query": "шеврон"})()
    request = ScopedSearchRequest(
        task=task,  # type: ignore[arg-type]
        dest_id_observed="-535680",
        endpoint_id="primary",
        params={"query": "шеврон", "page": "1", "dest": "-535680"},
    )
    with pytest.raises(ScopedTransportError, match=error_match) as caught:
        transport.search(request, timeout_seconds=10)
    assert caught.value.request_sent is True
    assert caught.value.dest_id_sent == "-535680"
    assert len(session.calls) == 1
    assert session.calls[0][1]["params"]["dest"] == "-535680"
    assert not hasattr(transport, "rotate")


def test_requests_transport_uses_one_session_for_egress_resolver_and_search() -> None:
    session = FakeSession(
        [
            FakeResponse(text="203.0.113.10"),
            FakeResponse(payload={"xinfo": "appType=1&dest=-535680&spp=30"}),
            FakeResponse(payload={"products": _products(1_000)}),
        ]
    )
    transport = _requests_transport(session)
    region = type(
        "Region",
        (),
        {
            "latitude": "55.0",
            "longitude": "37.0",
            "address_label": "Москва",
        },
    )()
    assert transport.egress_identity(timeout_seconds=10) == "203.0.113.10"
    assert transport.resolve_destination(region, timeout_seconds=10) == "-535680"
    task = type("Task", (), {"query": "шеврон"})()
    result = transport.search(
        ScopedSearchRequest(
            task=task,  # type: ignore[arg-type]
            dest_id_observed="-535680",
            endpoint_id="primary",
            params={"query": "шеврон", "page": "1", "dest": "-535680"},
        ),
        timeout_seconds=10,
    )
    assert result.dest_id_sent == "-535680"
    assert [call[0] for call in session.calls] == [
        "https://ip.example.test",
        "https://geo.example.test",
        "https://search.example.test",
    ]


def test_from_config_keeps_wb_secrets_out_of_neutral_egress_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, _plan_path = _project(tmp_path, monkeypatch)
    cookie_path = root / "config/test-cookie.txt"
    cookie_path.write_text("session=fake-test-value", encoding="utf-8")
    config.raw["serp"]["wb_cookie_file"] = str(cookie_path)
    config.raw["serp"]["request_headers"] = {
        "authorization": "Bearer fake-test-token"
    }
    monkeypatch.setenv("PARSER_WB_PROXY_URL", "http://proxy.example.test:8080")

    class CaptureSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.proxies: dict[str, str] = {}

        def close(self) -> None:
            pass

    sessions: list[CaptureSession] = []

    def session_factory():
        session = CaptureSession()
        sessions.append(session)
        return session

    monkeypatch.setattr(requests, "Session", session_factory)
    transport = RequestsScopedTransport.from_config(config)

    assert len(sessions) == 2
    assert "cookie" in transport.session.headers
    assert "authorization" in transport.session.headers
    assert "cookie" not in transport.egress_session.headers
    assert "authorization" not in transport.egress_session.headers
    assert transport.session.proxies == transport.egress_session.proxies


def test_cli_adds_explicit_collection_plan_without_changing_legacy_aliases() -> None:
    parser = build_parser()
    legacy = parser.parse_args(["run", "serp", "--job-id", "legacy"])
    alias = parser.parse_args(["serp", "--dry-run"])
    scoped = parser.parse_args(
        [
            "collection-plan",
            "--plan-file",
            PLAN_RELATIVE.as_posix(),
            "--no-publish",
        ]
    )
    assert (legacy.command, legacy.target, legacy.job_id) == (
        "run",
        "serp",
        "legacy",
    )
    assert alias.command == "serp"
    assert alias.dry_run is True
    assert scoped.command == "collection-plan"
    assert scoped.no_publish is True


def test_committed_disabled_plan_fails_before_transport_or_runtime_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        RequestsScopedTransport,
        "from_config",
        classmethod(
            lambda cls, config: (_ for _ in ()).throw(
                AssertionError("transport must not be created")
            )
        ),
    )
    config = load_config(str(PROJECT_ROOT / "config/config.yaml"))
    with pytest.raises(CollectionPlanRunError, match="disabled"):
        run_collection_plan(
            config=config,
            plan_path=PROJECT_ROOT / PLAN_RELATIVE,
            no_publish=True,
        )
