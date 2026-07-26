from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import socket
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import pytest
import requests

from app.common.cli import build_parser
from app.common.config import load_config
from app.common.proxy_required import MarketplaceProxyError
from app.serp.collection_plan import EffectiveEndpointPolicy
from app.serp.collection_plan_runner import (
    CollectionPlanRunError,
    EndpointProbeResult,
    RequestsScopedTransport,
    ScopedSearchRequest,
    ScopedSearchResult,
    ScopedTransportError,
)
from app.serp.regional_pilot import (
    EndpointPreflightFailed,
    GuardedRegionalPilotRunner,
    PilotRateLimitError,
    PilotRequestBudget,
    PilotRuntimeGuard,
    ProtectedStateAuditor,
    run_guarded_regional_pilot,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_RELATIVE = Path(
    "config/wb/collection_plans/shevron-moscow-rostov-top100-pilot-v1.json"
)
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
    runtime_env = root / "config/runtime.env"
    runtime_env.write_text("# test runtime\n", encoding="utf-8")
    headers_path = root / "config/wb_request_headers.json"
    headers_path.write_text('{"authorization":"test-only"}\n', encoding="utf-8")
    proxy_url = "http://proxy.example.test:8080"
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv(
        "PARSER_WB_RUNTIME_ENV_SHA256",
        hashlib.sha256(runtime_env.read_bytes()).hexdigest(),
    )
    monkeypatch.setenv("PARSER_WB_REQUEST_HEADERS_FILE", str(headers_path))
    monkeypatch.setenv("PARSER_WB_PROXY_URL", proxy_url)
    monkeypatch.setenv("PARSER_WB_COOKIE_REQUIRED", "0")
    monkeypatch.delenv("WB_COOKIE_FILE", raising=False)

    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["enabled"] = True
    _write_json(plan_path, plan)

    registry_path = root / "config/wb/regions.json"
    registry = _read_json(registry_path)
    for region in registry["regions"]:
        region["enabled"] = True
    _write_json(registry_path, registry)
    return root, load_config(str(root / "config/config.yaml")), plan_path


def _products(ids: list[int]) -> list[dict[str, Any]]:
    return [
        {
            "id": product_id,
            "name": f"product-{product_id}",
            "brand": "test-brand",
            "supplierId": 4516781,
            "rating": 4.8,
            "feedbacks": 100,
            "totalQuantity": 25,
        }
        for product_id in ids
    ]


def _usable_probe_result(
    request: ScopedSearchRequest,
    *,
    endpoint_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> EndpointProbeResult:
    selected_endpoint = endpoint_id or request.endpoint_id
    reusable = ScopedSearchResult(
        payload=payload
        if payload is not None
        else {"products": _products(list(range(100_001, 100_101)))},
        endpoint_id=selected_endpoint,
        dest_id_sent=request.dest_id_observed,
        http_status=200,
    )
    return EndpointProbeResult(
        endpoint_id=selected_endpoint,
        suitable=True,
        http_status=200,
        error_code=None,
        reusable_request=request,
        reusable_result=reusable,
    )


class PilotFakeTransport:
    request_params = {"appType": "1", "dest": "-legacy", "curr": "rub"}
    proxy_applied = True
    proxy_session_count = 1
    proxy_route_sha256 = hashlib.sha256(
        b"http://proxy.example.test:8080"
    ).hexdigest()

    def __init__(
        self,
        *,
        probe_results: Mapping[
            str,
            EndpointProbeResult
            | Callable[[ScopedSearchRequest], EndpointProbeResult],
        ]
        | None = None,
        repeat_overlap: int = 100,
        repeat_failure: bool = False,
        repeat_product_count: int = 100,
        repeat_duplicate: bool = False,
        egress_values: list[str] | None = None,
        egress_failure_calls: set[int] | None = None,
        on_search: Callable[[int], None] | None = None,
        search_failure_call: int | None = None,
        search_failure_status: int = 429,
        retry_after_status: str | None = None,
        retry_after_seconds: int | None = None,
        resolver_failure_region: str | None = None,
        resolver_failure_status: int = 429,
    ) -> None:
        self.endpoint_policy = EffectiveEndpointPolicy(
            selection_mode="ordered_fallbacks",
            endpoint_ids=("primary", "fallback-1", "fallback-2"),
            pinned_endpoint_id="primary",
        )
        self.default_primary_probe = probe_results is None
        self.probe_results = dict(probe_results or {})
        self.repeat_overlap = repeat_overlap
        self.repeat_failure = repeat_failure
        self.repeat_product_count = repeat_product_count
        self.repeat_duplicate = repeat_duplicate
        self.egress_values = list(egress_values or ["203.0.113.10"] * 4)
        self.egress_failure_calls = set(egress_failure_calls or ())
        self.on_search = on_search
        self.search_failure_call = search_failure_call
        self.search_failure_status = search_failure_status
        self.retry_after_status = retry_after_status
        self.retry_after_seconds = retry_after_seconds
        self.resolver_failure_region = resolver_failure_region
        self.resolver_failure_status = resolver_failure_status
        self.events: list[str] = []
        self.resolve_calls: list[str] = []
        self.probe_calls: list[str] = []
        self.search_calls: list[ScopedSearchRequest] = []
        self.pin_calls: list[str] = []
        self.egress_calls = 0
        self.closed = False

    def egress_identity(self, *, timeout_seconds: float) -> str:
        call_number = self.egress_calls + 1
        self.events.append(f"egress:{call_number}")
        index = min(self.egress_calls, len(self.egress_values) - 1)
        self.egress_calls += 1
        if call_number in self.egress_failure_calls:
            raise ScopedTransportError("egress_network_error")
        return self.egress_values[index]

    def resolve_destination(self, region, *, timeout_seconds: float) -> str:
        self.events.append(f"resolve:{region.region_id}")
        self.resolve_calls.append(region.region_id)
        if region.region_id == self.resolver_failure_region:
            raise ScopedTransportError(
                f"resolver_http_{self.resolver_failure_status}",
                http_status=self.resolver_failure_status,
                retry_after_status=self.retry_after_status,
                retry_after_seconds=self.retry_after_seconds,
            )
        return {
            "moscow": "-535680",
            "rostov-on-don": "-2228364",
        }[region.region_id]

    def probe_endpoint(
        self,
        request: ScopedSearchRequest,
        *,
        endpoint_id: str,
        timeout_seconds: float,
    ) -> EndpointProbeResult:
        self.events.append(f"probe:{endpoint_id}")
        self.probe_calls.append(endpoint_id)
        configured = self.probe_results.get(endpoint_id)
        if callable(configured):
            return configured(request)
        if configured is not None:
            return configured
        if self.default_primary_probe and endpoint_id == "primary":
            return _usable_probe_result(request)
        return EndpointProbeResult(
            endpoint_id=endpoint_id,
            suitable=False,
            http_status=498,
            error_code="endpoint_probe_http_498",
        )

    def pin_endpoint(self, endpoint_id: str) -> None:
        if self.pin_calls:
            raise ScopedTransportError("endpoint_pin_already_finalized")
        if endpoint_id not in self.endpoint_policy.endpoint_ids:
            raise ScopedTransportError("endpoint_id_unknown")
        self.pin_calls.append(endpoint_id)
        self.events.append(f"pin:{endpoint_id}")
        self.endpoint_policy = EffectiveEndpointPolicy(
            selection_mode="ordered_fallbacks",
            endpoint_ids=self.endpoint_policy.endpoint_ids,
            pinned_endpoint_id=endpoint_id,
        )

    def _search_ids(self, request: ScopedSearchRequest) -> list[int]:
        if (
            request.task.region_id == "moscow"
            and request.task.query_id == "shevron"
        ):
            initial = list(range(100_001, 100_101))
            retained = initial[: self.repeat_overlap]
            replacements = list(
                range(900_001, 900_001 + (100 - self.repeat_overlap))
            )
            product_ids = (retained + replacements)[: self.repeat_product_count]
            if self.repeat_duplicate and len(product_ids) > 1:
                product_ids[1] = product_ids[0]
            return product_ids
        query_offset = {
            "shevron": 100_000,
            "shevrony": 200_000,
            "shevron-na-lipuchke": 300_000,
        }[request.task.query_id]
        region_offset = 0 if request.task.region_id == "moscow" else 10_000
        return list(
            range(query_offset + region_offset + 1, query_offset + region_offset + 101)
        )

    def search(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        self.search_calls.append(request)
        call_number = len(self.search_calls)
        self.events.append(
            f"search:{request.task.region_id}:{request.task.query_id}:{call_number}"
        )
        if self.on_search is not None:
            self.on_search(call_number)
        if call_number == self.search_failure_call:
            raise ScopedTransportError(
                f"search_http_{self.search_failure_status}",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=self.search_failure_status,
                retry_after_status=self.retry_after_status,
                retry_after_seconds=self.retry_after_seconds,
            )
        if request.endpoint_id != self.endpoint_policy.pinned_endpoint_id:
            raise ScopedTransportError("search_endpoint_not_pinned")
        if (
            self.repeat_failure
            and request.task.region_id == "moscow"
            and request.task.query_id == "shevron"
        ):
            raise ScopedTransportError(
                "search_http_498",
                request_sent=True,
                dest_id_sent=request.dest_id_observed,
                http_status=498,
            )
        return ScopedSearchResult(
            payload={"products": _products(self._search_ids(request))},
            endpoint_id=request.endpoint_id,
            dest_id_sent=request.dest_id_observed,
            http_status=200,
        )

    def close(self) -> None:
        self.closed = True


class FakeMonotonicClock:
    def __init__(self) -> None:
        self.value = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


def _run(
    config,
    plan_path: Path,
    transport: PilotFakeTransport,
    *,
    root: Path,
    **kwargs,
):
    auditor = kwargs.pop(
        "protected_auditor",
        ProtectedStateAuditor(
            project_root=root,
            crontab_reader=lambda: b"test-crontab\n",
        ),
    )
    clock = kwargs.pop("pilot_clock", FakeMonotonicClock())
    wall_now = kwargs.pop("wall_now", lambda: FIXED_NOW)
    jitter = kwargs.pop("jitter", lambda: 0.0)
    return run_guarded_regional_pilot(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        guarded_pilot=True,
        transport=transport,
        run_id=RUN_ID,
        now=wall_now,
        egress_hash_salt=b"test-only-salt",
        protected_auditor=auditor,
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
        jitter=jitter,
        **kwargs,
    )


def _state_dir(root: Path) -> Path:
    return (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )


def _pilot_source_paths(root: Path, plan_path: Path) -> dict[str, Path]:
    plan = _read_json(plan_path)
    return {
        "config_file": root / "config/config.yaml",
        "collection_plan": plan_path,
        "region_registry": root / "config/wb/regions.json",
        "query_pack": root / str(plan["query_pack_file"]),
    }


def test_primary_probe_success_runs_exact_a_b_a_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport()

    def forbidden_network(*args, **kwargs):
        raise AssertionError("real network is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden_network)
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden_network)
    clock = FakeMonotonicClock()
    manifest = _run(
        config,
        plan_path,
        transport,
        root=root,
        pilot_clock=clock,
    )

    state_dir = _state_dir(root)
    budget = _read_json(state_dir / "request_budget.json")
    endpoint = _read_json(state_dir / "endpoint_preflight.json")
    control = _read_json(state_dir / "control/moscow_repeat.json")
    comparison = _read_json(state_dir / "comparison.json")
    snapshot = _read_json(state_dir / "effective_plan.json")
    protected = _read_json(state_dir / "protected_evidence.json")
    pacing = _read_json(state_dir / "search_pacing.json")

    assert manifest["complete"] is True
    assert manifest["totals"] == {
        "regions_ok": 2,
        "pages_ok": 6,
        "products_ok": 600,
    }
    assert endpoint["attempts"] == [
        {
            "endpoint_id": "primary",
            "attempted": True,
            "outcome": "usable",
            "status": 200,
            "error_code": None,
            "reused_as_first_page": True,
            "reuse_task": {
                "region_id": "moscow",
                "query_id": "shevron",
                "page": 1,
            },
        }
    ]
    assert endpoint["reuse"] == {
        "reused_as_first_page": True,
        "region_id": "moscow",
        "query_id": "shevron",
        "page": 1,
    }
    assert endpoint["pinned_endpoint_id"] == "primary"
    assert snapshot["endpoint_policy"]["pinned_endpoint_id"] == "primary"
    assert budget["used"] == {
        "geo": 2,
        "endpoint_probe": 1,
        "regional_search": 5,
        "repeat_search": 1,
        "total_wb": 9,
    }
    assert len(pacing["attempts"]) == 7
    assert pacing["attempts"][0]["phase"] == "endpoint_probe"
    assert pacing["attempts"][0]["sleep_seconds"] == 0.0
    assert pacing["attempts"][1]["phase"] == "regional_search"
    assert pacing["attempts"][1]["query_id"] == "shevrony"
    assert all(
        event["required_interval_seconds"] == 17.0
        for event in pacing["attempts"][1:]
    )
    assert clock.sleeps == [17.0] * 6
    assert manifest["egress"]["verification_status"] == "verified_constant"
    assert manifest["egress"]["constant"] is True
    assert manifest["egress"]["checks_completed"] == 4
    assert control["status"] == "eligible"
    assert control["jaccard"] == 1.0
    assert comparison["status"] == "eligible"
    assert len(comparison["queries"]) == 3
    protected_by_path = {row["path"]: row for row in protected["entries"]}
    source_paths = _pilot_source_paths(root, plan_path)
    for name, source_path in source_paths.items():
        relative = source_path.relative_to(root).as_posix()
        evidence = protected_by_path[relative]
        assert evidence["status"] == "unchanged"
        assert evidence["after_sha256"] == hashlib.sha256(
            source_path.read_bytes()
        ).hexdigest()
        if name != "config_file":
            assert manifest[f"{name}_sha256"] == evidence["after_sha256"]
    assert len(transport.search_calls) == 6
    assert transport.events == [
        "egress:1",
        "resolve:moscow",
        "resolve:rostov-on-don",
        "probe:primary",
        "pin:primary",
        "search:moscow:shevrony:1",
        "search:moscow:shevron-na-lipuchke:2",
        "egress:2",
        "search:rostov-on-don:shevron:3",
        "search:rostov-on-don:shevrony:4",
        "search:rostov-on-don:shevron-na-lipuchke:5",
        "egress:3",
        "search:moscow:shevron:6",
        "egress:4",
    ]
    assert all(
        not (
            call.task.region_id == "moscow"
            and call.task.query_id == "shevron"
        )
        for call in transport.search_calls[:-1]
    )
    assert all(call.params["page"] == "1" for call in transport.search_calls)
    assert all(call.endpoint_id == "primary" for call in transport.search_calls)
    serialized_state = "\n".join(
        path.read_text(encoding="utf-8")
        for path in state_dir.rglob("*.json")
    )
    assert "203.0.113.10" not in serialized_state

    for region_id in ("moscow", "rostov-on-don"):
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
    reused_raw = (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "pages/shevron/page_001.json"
    )
    reused_payload = _read_json(reused_raw)
    assert len(reused_payload["products"]) == 100
    assert len({str(row["id"]) for row in reused_payload["products"]}) == 100
    assert list(reused_raw.parent.glob("page_001.json")) == [reused_raw]
    reused_checkpoint = _read_json(
        state_dir / "checkpoints/moscow/shevron/page_001.json"
    )
    assert reused_checkpoint["raw_file"] == reused_raw.relative_to(root).as_posix()
    assert (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "control/shevron_repeat_page_001.json"
    ).exists()
    assert not (root / "data/marts/serp/latest").exists()
    assert not (root / "state/run_reports/latest.json").exists()
    assert not (root / "data/warehouse").exists()


def test_search_pacer_applies_positive_bounded_jitter_to_every_later_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    clock = FakeMonotonicClock()

    _run(
        config,
        plan_path,
        PilotFakeTransport(),
        root=root,
        pilot_clock=clock,
        jitter=lambda: 2.0,
    )

    pacing = _read_json(_state_dir(root) / "search_pacing.json")
    assert pacing["attempts"][0]["required_interval_seconds"] == 0.0
    assert pacing["attempts"][0]["sleep_seconds"] == 0.0
    assert all(
        event["required_interval_seconds"] == 19.0
        and event["jitter_seconds"] == 2.0
        and event["actual_interval_seconds"] >= 19.0
        for event in pacing["attempts"][1:]
    )
    assert clock.sleeps == [19.0] * 6


@pytest.mark.parametrize(
    ("http_status", "retry_status", "retry_seconds", "expected_cooldown"),
    [
        (429, "valid", 17, 17),
        (498, "invalid", None, 45),
    ],
)
def test_rate_limit_blocks_all_later_wb_calls_and_allows_one_neutral_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    http_status: int,
    retry_status: str,
    retry_seconds: int | None,
    expected_cooldown: int,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    clock = FakeMonotonicClock()
    transport = PilotFakeTransport(
        search_failure_call=1,
        search_failure_status=http_status,
        retry_after_status=retry_status,
        retry_after_seconds=retry_seconds,
    )

    with pytest.raises(ScopedTransportError, match=f"search_http_{http_status}"):
        _run(
            config,
            plan_path,
            transport,
            root=root,
            pilot_clock=clock,
        )

    state_dir = _state_dir(root)
    rate_limit = _read_json(state_dir / "rate_limit.json")
    budget = _read_json(state_dir / "request_budget.json")
    manifest = _read_json(state_dir / "manifest.json")
    assert len(transport.probe_calls) == 1
    assert len(transport.search_calls) == 1
    assert transport.egress_calls == 2
    assert rate_limit["status"] == "triggered"
    assert rate_limit["http_status"] == http_status
    assert rate_limit["retry_after"] == {
        "status": retry_status,
        "seconds": retry_seconds,
    }
    assert rate_limit["cooldown_seconds"] == expected_cooldown
    assert rate_limit["cooldown_status"] == "completed"
    assert rate_limit["failure_final_egress"] == {
        "attempted": True,
        "status": "matched_initial",
    }
    assert rate_limit["subsequent_wb_calls_allowed"] is False
    assert rate_limit["retry_count"] == 0
    assert budget["used"] == {
        "geo": 2,
        "endpoint_probe": 1,
        "regional_search": 1,
        "repeat_search": 0,
        "total_wb": 4,
    }
    assert clock.sleeps == [17.0, float(expected_cooldown)]
    assert manifest["complete"] is False


def test_resolver_rate_limit_blocks_second_geo_probe_and_search_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    clock = FakeMonotonicClock()
    transport = PilotFakeTransport(
        resolver_failure_region="moscow",
        resolver_failure_status=429,
        retry_after_status="valid",
        retry_after_seconds=5,
    )

    with pytest.raises(PilotRateLimitError, match="pilot_rate_limit_http_429"):
        _run(
            config,
            plan_path,
            transport,
            root=root,
            pilot_clock=clock,
        )

    rate_limit = _read_json(_state_dir(root) / "rate_limit.json")
    assert transport.resolve_calls == ["moscow"]
    assert transport.probe_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 2
    assert rate_limit["phase"] == "geo_resolver"
    assert rate_limit["retry_after"] == {
        "status": "valid",
        "seconds": 5,
    }
    assert rate_limit["subsequent_wb_calls_allowed"] is False
    assert clock.sleeps == [5.0]


@pytest.mark.parametrize(
    "missing_name",
    [
        "PARSER_WB_RUNTIME_ENV_LOADED",
        "PARSER_WB_RUNTIME_ENV_SHA256",
        "PARSER_WB_PROXY_URL",
        "PARSER_WB_REQUEST_HEADERS_FILE",
        "PARSER_WB_COOKIE_REQUIRED",
    ],
)
def test_contour_preflight_failure_makes_zero_transport_calls_and_leaks_no_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport()
    monkeypatch.delenv(missing_name, raising=False)

    with pytest.raises((CollectionPlanRunError, MarketplaceProxyError)):
        _run(config, plan_path, transport, root=root)

    assert transport.events == []
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert transport.probe_calls == []
    assert transport.search_calls == []
    serialized = "\n".join(
        path.read_text(encoding="utf-8")
        for path in _state_dir(root).rglob("*.json")
    )
    assert "proxy.example.test" not in serialized
    assert "authorization" not in serialized.lower()
    assert "test-only" not in serialized


def test_request_headers_change_after_config_load_fails_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    headers_path = root / "config/wb_request_headers.json"
    headers_path.write_text('{"authorization":"changed"}\n', encoding="utf-8")
    transport = PilotFakeTransport()

    with pytest.raises(
        CollectionPlanRunError,
        match="request headers changed after config load",
    ):
        _run(config, plan_path, transport, root=root)

    assert transport.events == []
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert transport.probe_calls == []
    assert transport.search_calls == []


def test_start_gate_requires_twenty_minutes_before_cutoff_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport()
    too_late = datetime(2026, 7, 26, 20, 26, 0, tzinfo=timezone.utc)

    with pytest.raises(CollectionPlanRunError, match="requires 20 minutes"):
        _run(
            config,
            plan_path,
            transport,
            root=root,
            wall_now=lambda: too_late,
        )

    assert transport.events == []
    assert not _state_dir(root).exists()


def test_hard_runtime_guard_fails_before_late_http_timeout() -> None:
    clock = FakeMonotonicClock()
    guard = PilotRuntimeGuard.build(
        wall_deadline_utc=datetime(
            2026,
            7,
            26,
            20,
            45,
            tzinfo=timezone.utc,
        ),
        wall_now=lambda: FIXED_NOW,
        monotonic=clock.monotonic,
    )
    clock.value += 18 * 60

    with pytest.raises(CollectionPlanRunError, match="hard runtime"):
        guard.request_timeout(45)


def test_primary_failure_pins_one_fallback_for_all_searches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(
        probe_results={
            "primary": EndpointProbeResult(
                endpoint_id="primary",
                suitable=False,
                http_status=403,
                error_code="unsafe error code",
            ),
            "fallback-1": (
                lambda request: _usable_probe_result(
                    request,
                    endpoint_id="fallback-1",
                )
            ),
        }
    )
    clock = FakeMonotonicClock()
    manifest = _run(
        config,
        plan_path,
        transport,
        root=root,
        pilot_clock=clock,
    )

    endpoint = _read_json(_state_dir(root) / "endpoint_preflight.json")
    budget = _read_json(_state_dir(root) / "request_budget.json")
    snapshot = _read_json(_state_dir(root) / "effective_plan.json")
    pacing = _read_json(_state_dir(root) / "search_pacing.json")
    assert manifest["complete"] is True
    assert transport.probe_calls == ["primary", "fallback-1"]
    assert transport.pin_calls == ["fallback-1"]
    assert all(
        request.endpoint_id == "fallback-1" for request in transport.search_calls
    )
    assert endpoint["pinned_endpoint_id"] == "fallback-1"
    assert endpoint["attempts"][0]["error_code"] == (
        "endpoint_probe_invalid_error_code"
    )
    assert "unsafe error code" not in json.dumps(endpoint)
    assert snapshot["endpoint_policy"]["pinned_endpoint_id"] == "fallback-1"
    assert budget["used"]["endpoint_probe"] == 2
    assert budget["used"]["regional_search"] == 5
    assert budget["used"]["total_wb"] == 10
    assert len(transport.search_calls) == 6
    assert [event["phase"] for event in pacing["attempts"][:3]] == [
        "endpoint_probe",
        "endpoint_probe",
        "regional_search",
    ]
    assert pacing["attempts"][0]["sleep_seconds"] == 0.0
    assert all(
        event["actual_interval_seconds"] >= 17.0
        for event in pacing["attempts"][1:]
    )
    assert clock.sleeps == [17.0] * 7
    assert all(
        not (
            call.task.region_id == "moscow"
            and call.task.query_id == "shevron"
        )
        for call in transport.search_calls[:-1]
    )


def test_both_endpoint_probes_fail_without_regional_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(probe_results={})
    transport.probe_results = {
        endpoint_id: EndpointProbeResult(
            endpoint_id=endpoint_id,
            suitable=False,
            http_status=498,
            error_code="endpoint_probe_http_498",
        )
        for endpoint_id in ("primary", "fallback-1")
    }

    with pytest.raises(PilotRateLimitError):
        _run(config, plan_path, transport, root=root)

    endpoint = _read_json(_state_dir(root) / "endpoint_preflight.json")
    rate_limit = _read_json(_state_dir(root) / "rate_limit.json")
    manifest = _read_json(_state_dir(root) / "manifest.json")
    assert transport.probe_calls == ["primary"]
    assert transport.search_calls == []
    assert endpoint["status"] == "failed"
    assert endpoint["pinned_endpoint_id"] is None
    assert rate_limit["status"] == "triggered"
    assert rate_limit["http_status"] == 498
    assert rate_limit["retry_count"] == 0
    assert manifest["complete"] is False


@pytest.mark.parametrize(
    ("suitable", "http_status"),
    [
        (1, 200),
        ("yes", 200),
        (True, True),
    ],
)
def test_malformed_truthy_probe_never_pins_or_starts_regional_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suitable: Any,
    http_status: Any,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(
        probe_results={
            endpoint_id: EndpointProbeResult(
                endpoint_id=endpoint_id,
                suitable=suitable,
                http_status=http_status,
                error_code=None,
            )
            for endpoint_id in ("primary", "fallback-1")
        }
    )

    with pytest.raises(EndpointPreflightFailed):
        _run(config, plan_path, transport, root=root)

    endpoint = _read_json(_state_dir(root) / "endpoint_preflight.json")
    assert transport.probe_calls == ["primary", "fallback-1"]
    assert transport.pin_calls == []
    assert transport.search_calls == []
    assert endpoint["status"] == "failed"
    assert endpoint["pinned_endpoint_id"] is None
    assert all(
        attempt["outcome"] == "unusable"
        and attempt["error_code"] == "endpoint_probe_unsuitable"
        for attempt in endpoint["attempts"]
    )


@pytest.mark.parametrize(
    "malformation",
    [
        "missing_payload",
        "wrong_endpoint",
        "wrong_destination",
        "wrong_status",
        "wrong_task",
        "short_payload",
        "duplicate_products",
        "non_mapping_payload",
    ],
)
def test_successful_looking_probe_requires_valid_reusable_page_before_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    malformation: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)

    def malformed(request: ScopedSearchRequest) -> EndpointProbeResult:
        probe = _usable_probe_result(request)
        reusable_request = probe.reusable_request
        reusable_result = probe.reusable_result
        assert reusable_request is not None
        assert reusable_result is not None
        if malformation == "missing_payload":
            reusable_result = None
        elif malformation == "wrong_endpoint":
            reusable_result = replace(reusable_result, endpoint_id="wrong")
        elif malformation == "wrong_destination":
            reusable_result = replace(reusable_result, dest_id_sent="-1")
        elif malformation == "wrong_status":
            reusable_result = replace(reusable_result, http_status=True)
        elif malformation == "wrong_task":
            reusable_request = replace(
                reusable_request,
                task=replace(reusable_request.task, query_id="wrong-query"),
            )
        elif malformation == "short_payload":
            reusable_result = replace(
                reusable_result,
                payload={"products": _products(list(range(1, 100)))},
            )
        elif malformation == "duplicate_products":
            product_ids = list(range(1, 100)) + [1]
            reusable_result = replace(
                reusable_result,
                payload={"products": _products(product_ids)},
            )
        elif malformation == "non_mapping_payload":
            reusable_result = replace(  # type: ignore[arg-type]
                reusable_result,
                payload=[],
            )
        return replace(
            probe,
            reusable_request=reusable_request,
            reusable_result=reusable_result,
        )

    transport = PilotFakeTransport(
        probe_results={
            "primary": malformed,
            "fallback-1": malformed,
        }
    )

    with pytest.raises(EndpointPreflightFailed):
        _run(config, plan_path, transport, root=root)

    endpoint = _read_json(_state_dir(root) / "endpoint_preflight.json")
    assert transport.probe_calls == ["primary", "fallback-1"]
    assert transport.pin_calls == []
    assert transport.search_calls == []
    assert endpoint["status"] == "failed"
    assert endpoint["reuse"]["reused_as_first_page"] is False
    assert all(
        attempt["error_code"] == "endpoint_probe_reusable_invalid"
        and attempt["reused_as_first_page"] is False
        and attempt["reuse_task"] is None
        for attempt in endpoint["attempts"]
    )
    serialized_state = "\n".join(
        path.read_text(encoding="utf-8")
        for path in _state_dir(root).rglob("*.json")
    )
    assert '"products"' not in serialized_state


def test_budget_exceed_fails_before_the_disallowed_search_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport()
    budget = PilotRequestBudget(
        limits={
            "geo": 2,
            "endpoint_probe": 2,
            "regional_search": 4,
            "repeat_search": 1,
        }
    )

    with pytest.raises(CollectionPlanRunError, match="budget exceeded before HTTP"):
        _run(
            config,
            plan_path,
            transport,
            root=root,
            request_budget=budget,
        )

    assert len(transport.search_calls) == 4
    artifact = _read_json(_state_dir(root) / "request_budget.json")
    assert artifact["used"]["regional_search"] == 4
    assert artifact["used"]["total_wb"] == 7
    assert _read_json(_state_dir(root) / "manifest.json")["complete"] is False


@pytest.mark.parametrize(
    ("repeat_overlap", "expected_status"),
    [(98, "eligible"), (94, "not_eligible")],
)
def test_control_jaccard_gates_regional_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    repeat_overlap: int,
    expected_status: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(repeat_overlap=repeat_overlap)
    if expected_status == "eligible":
        manifest = _run(config, plan_path, transport, root=root)
        assert manifest["complete"] is True
    else:
        with pytest.raises(CollectionPlanRunError, match="Jaccard"):
            _run(config, plan_path, transport, root=root)

    control = _read_json(_state_dir(root) / "control/moscow_repeat.json")
    comparison = _read_json(_state_dir(root) / "comparison.json")
    assert control["status"] == expected_status
    assert comparison["status"] == expected_status
    if expected_status == "not_eligible":
        assert comparison["queries"] == []
        assert _read_json(_state_dir(root) / "manifest.json")["complete"] is False


@pytest.mark.parametrize(
    ("transport_kwargs", "error_match"),
    [
        ({"repeat_failure": True}, "search_http_498"),
        ({"repeat_product_count": 99}, "search_products_short"),
        ({"repeat_duplicate": True}, "search_product_duplicate"),
    ],
)
def test_repeat_failure_cannot_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport_kwargs: dict[str, Any],
    error_match: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(**transport_kwargs)

    with pytest.raises(CollectionPlanRunError, match=error_match):
        _run(config, plan_path, transport, root=root)

    manifest = _read_json(_state_dir(root) / "manifest.json")
    assert manifest["complete"] is False
    assert manifest["egress"]["verification_status"] == "unverified"
    assert manifest["egress"]["constant"] is None
    assert manifest["egress"]["checks_completed"] == (
        4 if transport_kwargs.get("repeat_failure") else 3
    )
    assert not (_state_dir(root) / "control/moscow_repeat.json").exists()


@pytest.mark.parametrize(
    (
        "egress_failure_calls",
        "egress_values",
        "expected_status",
        "expected_constant",
        "expected_completed",
        "expected_search_calls",
    ),
    [
        ({3}, None, "unverified", None, 2, 5),
        ({4}, None, "unverified", None, 3, 6),
        (set(), ["203.0.113.10"] * 3 + ["203.0.113.11"], "changed", False, 4, 6),
    ],
)
def test_egress_evidence_covers_before_and_after_repeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    egress_failure_calls: set[int],
    egress_values: list[str] | None,
    expected_status: str,
    expected_constant: bool | None,
    expected_completed: int,
    expected_search_calls: int,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = PilotFakeTransport(
        egress_failure_calls=egress_failure_calls,
        egress_values=egress_values,
    )

    with pytest.raises(CollectionPlanRunError):
        _run(config, plan_path, transport, root=root)

    manifest = _read_json(_state_dir(root) / "manifest.json")
    assert len(transport.search_calls) == expected_search_calls
    assert manifest["complete"] is False
    assert manifest["egress"]["verification_status"] == expected_status
    assert manifest["egress"]["constant"] is expected_constant
    assert manifest["egress"]["checks_completed"] == expected_completed
    assert manifest["egress"]["checks_expected"] == 4


def test_protected_mismatch_fails_and_unchanged_run_records_hashes_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unchanged_root, unchanged_config, unchanged_plan = _project(
        tmp_path / "unchanged",
        monkeypatch,
    )
    unchanged_manifest = _run(
        unchanged_config,
        unchanged_plan,
        PilotFakeTransport(),
        root=unchanged_root,
    )
    unchanged_evidence = _read_json(
        _state_dir(unchanged_root) / "protected_evidence.json"
    )
    assert unchanged_manifest["complete"] is True
    assert unchanged_evidence["status"] == "unchanged"
    assert all(
        set(row)
        == {
            "path",
            "before_status",
            "before_sha256",
            "after_status",
            "after_sha256",
            "status",
        }
        for row in unchanged_evidence["entries"]
    )

    changed_root, changed_config, changed_plan = _project(
        tmp_path / "changed",
        monkeypatch,
    )
    protected_path = changed_root / "exports/queries.txt"
    protected_path.parent.mkdir(parents=True, exist_ok=True)
    protected_path.write_text("before\n", encoding="utf-8")

    def mutate_protected(call_number: int) -> None:
        if call_number == 1:
            protected_path.write_text("after\n", encoding="utf-8")

    with pytest.raises(CollectionPlanRunError, match="protected state changed"):
        _run(
            changed_config,
            changed_plan,
            PilotFakeTransport(on_search=mutate_protected),
            root=changed_root,
        )
    changed_evidence = _read_json(
        _state_dir(changed_root) / "protected_evidence.json"
    )
    assert changed_evidence["status"] == "changed"
    changed_row = next(
        row
        for row in changed_evidence["entries"]
        if row["path"] == "exports/queries.txt"
    )
    assert changed_row["status"] == "changed"
    assert _read_json(_state_dir(changed_root) / "manifest.json")["complete"] is False


@pytest.mark.parametrize(
    "source_name",
    [
        "config_file",
        "collection_plan",
        "region_registry",
        "query_pack",
    ],
)
def test_each_pilot_source_change_fails_and_suppresses_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    source_path = _pilot_source_paths(root, plan_path)[source_name]

    def mutate_source(call_number: int) -> None:
        if call_number == 1:
            source_path.write_bytes(source_path.read_bytes() + b"\n")

    with pytest.raises(CollectionPlanRunError, match="protected state changed"):
        _run(
            config,
            plan_path,
            PilotFakeTransport(on_search=mutate_source),
            root=root,
        )

    state_dir = _state_dir(root)
    evidence = _read_json(state_dir / "protected_evidence.json")
    relative = source_path.relative_to(root).as_posix()
    source_row = next(
        row for row in evidence["entries"] if row["path"] == relative
    )
    comparison = _read_json(state_dir / "comparison.json")
    manifest = _read_json(state_dir / "manifest.json")
    assert evidence["status"] == "changed"
    assert source_row["status"] == "changed"
    assert source_row["before_sha256"] != source_row["after_sha256"]
    assert comparison["status"] == "failed"
    assert comparison["reason"] == "protected_state_changed"
    assert comparison["queries"] == []
    assert manifest["complete"] is False
    assert manifest["status"] == "failed"
    assert manifest["query_pack_sha256"] is None
    assert manifest["collection_plan_sha256"] is None
    assert manifest["region_registry_sha256"] is None


def test_config_change_after_load_fails_before_any_transport_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    config_path = root / "config/config.yaml"
    loaded_bytes = config_path.read_bytes()
    assert config.config_file_sha256 == hashlib.sha256(loaded_bytes).hexdigest()
    config_path.write_bytes(loaded_bytes + b"\n# changed after load_config\n")
    transport = PilotFakeTransport()

    with pytest.raises(
        CollectionPlanRunError,
        match="protected pilot source hash changed before network",
    ):
        _run(config, plan_path, transport, root=root)

    assert transport.events == []
    assert transport.egress_calls == 0
    assert transport.resolve_calls == []
    assert transport.probe_calls == []
    assert transport.pin_calls == []
    assert transport.search_calls == []
    assert not _state_dir(root).exists()


@pytest.mark.parametrize(
    "replacement_kind",
    ["missing", "symlink", "non_regular"],
)
def test_pilot_source_replacement_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    source_path = _pilot_source_paths(root, plan_path)["query_pack"]
    replacement_target = root / "config/config.yaml"

    def replace_source(call_number: int) -> None:
        if call_number != 1:
            return
        source_path.unlink()
        if replacement_kind == "symlink":
            source_path.symlink_to(replacement_target)
        elif replacement_kind == "non_regular":
            source_path.mkdir()

    with pytest.raises(CollectionPlanRunError, match="protected state changed"):
        _run(
            config,
            plan_path,
            PilotFakeTransport(on_search=replace_source),
            root=root,
        )

    state_dir = _state_dir(root)
    evidence = _read_json(state_dir / "protected_evidence.json")
    relative = source_path.relative_to(root).as_posix()
    source_row = next(
        row for row in evidence["entries"] if row["path"] == relative
    )
    expected_status = {
        "missing": "missing",
        "symlink": "unsafe_symlink",
        "non_regular": "not_regular_file",
    }[replacement_kind]
    assert source_row["after_status"] == expected_status
    assert source_row["status"] == "changed"
    assert _read_json(state_dir / "comparison.json")["queries"] == []
    manifest = _read_json(state_dir / "manifest.json")
    assert manifest["complete"] is False
    assert manifest["query_pack_sha256"] is None


@pytest.mark.parametrize(
    ("source_name", "invalid_kind"),
    [
        ("config_file", "external"),
        ("collection_plan", "symlink"),
        ("region_registry", "non_regular"),
        ("query_pack", "symlink"),
    ],
)
def test_pilot_source_paths_fail_closed_before_capture_or_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
    invalid_kind: str,
) -> None:
    root, _config, plan_path = _project(tmp_path, monkeypatch)
    source_paths = _pilot_source_paths(root, plan_path)
    if invalid_kind == "external":
        invalid_path = tmp_path / "outside-source.json"
        invalid_path.write_text("{}\n", encoding="utf-8")
    elif invalid_kind == "symlink":
        invalid_path = root / f"{source_name}-source-link.json"
        invalid_path.symlink_to(source_paths[source_name])
    else:
        invalid_path = root / f"{source_name}-source-directory"
        invalid_path.mkdir()
    source_paths[source_name] = invalid_path
    auditor = ProtectedStateAuditor(
        project_root=root,
        crontab_reader=lambda: b"test-crontab\n",
    )

    with pytest.raises(CollectionPlanRunError, match="protected pilot source path"):
        auditor.bind_source_paths(**source_paths)


def test_guarded_pilot_never_opens_protected_paths_for_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    protected_path = root / "exports/queries.txt"
    protected_path.parent.mkdir(parents=True, exist_ok=True)
    protected_path.write_text("protected\n", encoding="utf-8")
    original_open = os.open
    write_paths: list[Path] = []

    def tracked_open(path, flags, *args, **kwargs):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC):
            write_paths.append(Path(path).absolute())
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", tracked_open)
    _run(config, plan_path, PilotFakeTransport(), root=root)

    assert protected_path.absolute() not in write_paths
    forbidden = (
        root / "data/raw/serp/latest",
        root / "data/staging/serp/latest",
        root / "data/marts/serp/latest",
        root / "data/warehouse",
        root / "state/run_reports",
        root / "exports",
    )
    for path in write_paths:
        assert not any(path == base or base in path.parents for base in forbidden)


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: Any = None,
    ) -> None:
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}
        self.proxies: dict[str, str] = {}

    def get(self, url: str, **kwargs):
        self.calls.append(url)
        return self.responses.pop(0)

    def close(self) -> None:
        pass


def test_requests_transport_probe_validates_primary_without_pinning() -> None:
    session = FakeSession(
        [
            FakeResponse(payload={"products": _products(list(range(1, 101)))}),
        ]
    )
    transport = RequestsScopedTransport(
        session=session,  # type: ignore[arg-type]
        request_params={"appType": "1"},
        endpoint_urls=(
            "https://primary.example.test",
            "https://fallback.example.test",
        ),
        timeout_seconds=45,
        referer_base="https://www.wildberries.ru/search?query=",
        egress_session=session,  # type: ignore[arg-type]
    )
    task = type("Task", (), {"query": "шеврон", "page_size": 100})()
    request = ScopedSearchRequest(
        task=task,  # type: ignore[arg-type]
        dest_id_observed="-535680",
        endpoint_id="primary",
        params={"query": "шеврон", "page": "1", "dest": "-535680"},
    )
    result = transport.probe_endpoint(
        request,
        endpoint_id="primary",
        timeout_seconds=10,
    )

    assert result.endpoint_id == "primary"
    assert result.suitable is True
    assert result.http_status == 200
    assert result.error_code is None
    assert result.reusable_request is request
    assert result.reusable_result is not None
    assert result.reusable_result.endpoint_id == "primary"
    assert result.reusable_result.dest_id_sent == "-535680"
    assert result.reusable_result.http_status == 200
    assert len(result.reusable_result.payload["products"]) == 100
    assert session.calls == ["https://primary.example.test"]
    transport.pin_endpoint("fallback-1")
    assert transport.endpoint_policy.pinned_endpoint_id == "fallback-1"


def test_requests_transport_failed_probe_has_no_reusable_page() -> None:
    session = FakeSession([FakeResponse(status_code=403)])
    transport = RequestsScopedTransport(
        session=session,  # type: ignore[arg-type]
        request_params={"appType": "1"},
        endpoint_urls=(
            "https://primary.example.test",
            "https://fallback.example.test",
        ),
        timeout_seconds=45,
        referer_base="https://www.wildberries.ru/search?query=",
        egress_session=session,  # type: ignore[arg-type]
    )
    task = type("Task", (), {"query": "шеврон", "page_size": 100})()
    request = ScopedSearchRequest(
        task=task,  # type: ignore[arg-type]
        dest_id_observed="-535680",
        endpoint_id="primary",
        params={"query": "шеврон", "page": "1", "dest": "-535680"},
    )

    result = transport.probe_endpoint(
        request,
        endpoint_id="primary",
        timeout_seconds=10,
    )

    assert result.suitable is False
    assert result.http_status == 403
    assert result.reusable_request is None
    assert result.reusable_result is None


def test_requests_transport_uses_pinned_fallback_url_and_rejects_switch() -> None:
    session = FakeSession(
        [
            FakeResponse(payload={"products": _products(list(range(1, 101)))}),
        ]
    )
    transport = RequestsScopedTransport(
        session=session,  # type: ignore[arg-type]
        request_params={"appType": "1"},
        endpoint_urls=(
            "https://primary.example.test",
            "https://fallback.example.test",
        ),
        timeout_seconds=45,
        referer_base="https://www.wildberries.ru/search?query=",
        egress_session=session,  # type: ignore[arg-type]
    )
    transport.pin_endpoint("fallback-1")
    task = type("Task", (), {"query": "шеврон"})()
    request = ScopedSearchRequest(
        task=task,  # type: ignore[arg-type]
        dest_id_observed="-535680",
        endpoint_id="fallback-1",
        params={"query": "шеврон", "page": "1", "dest": "-535680"},
    )
    result = transport.search(request, timeout_seconds=10)

    assert result.endpoint_id == "fallback-1"
    assert session.calls == ["https://fallback.example.test"]
    with pytest.raises(ScopedTransportError, match="already_finalized"):
        transport.pin_endpoint("primary")


def test_guarded_flag_is_explicit_and_disabled_tracked_plan_stays_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parser = build_parser()
    ordinary = parser.parse_args(
        [
            "collection-plan",
            "--plan-file",
            PLAN_RELATIVE.as_posix(),
            "--no-publish",
        ]
    )
    guarded = parser.parse_args(
        [
            "collection-plan",
            "--plan-file",
            PLAN_RELATIVE.as_posix(),
            "--no-publish",
            "--guarded-pilot",
        ]
    )
    assert ordinary.guarded_pilot is False
    assert guarded.guarded_pilot is True

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
        run_guarded_regional_pilot(
            config=config,
            plan_path=PROJECT_ROOT / PLAN_RELATIVE,
            no_publish=True,
            guarded_pilot=True,
        )


def test_runner_requires_guarded_mode_even_with_fake_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    with pytest.raises(CollectionPlanRunError, match="guarded-pilot"):
        GuardedRegionalPilotRunner(
            config=config,
            plan_path=plan_path,
            transport=PilotFakeTransport(),
            no_publish=True,
            guarded_pilot=False,
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            protected_auditor=ProtectedStateAuditor(
                project_root=root,
                crontab_reader=lambda: b"test\n",
            ),
        )
