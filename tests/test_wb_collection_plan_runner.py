from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import socket
import sys
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest
import requests

from app.common.cli import build_parser
from app.common.config import load_config
from app.common.exceptions import CriticalPipelineError, RunLockedError
from app.common.run_lock import acquire_advisory_lock, acquire_run_lock
from app.serp import collection_plan_runner as runner_module
from app.serp.collection_plan import (
    EffectiveEndpointPolicy,
    canonical_effective_plan_sha256,
    exact_file_sha256,
    load_collection_plan_bundle,
)
from app.serp.collection_plan_runner import (
    CollectionPlanRunner,
    CollectionPlanRunError,
    DeadlineGuard,
    EgressIdentityChangedError,
    RequestsScopedTransport,
    ScopedPaths,
    ScopedSearchRequest,
    ScopedSearchResult,
    ScopedTransportError,
    acquire_collection_plan_locks,
    parse_retry_after_delta,
    run_collection_plan,
    validate_resumable_collection_state,
)
from scripts import run_wb_collection_plan as dedicated_launcher


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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ("missing", None)),
        ("1", ("valid", 1)),
        ("120", ("valid", 120)),
        ("0", ("out_of_range", None)),
        ("121", ("out_of_range", None)),
        ("000", ("out_of_range", None)),
        ("001", ("valid", 1)),
        ("1.0", ("invalid", None)),
        (" 17", ("invalid", None)),
        ("17 ", ("invalid", None)),
        ("+17", ("invalid", None)),
        ("-1", ("invalid", None)),
        ("1234", ("invalid", None)),
        (17, ("invalid", None)),
        (True, ("invalid", None)),
    ],
)
def test_retry_after_accepts_only_strict_bounded_delta_seconds(
    value: Any,
    expected: tuple[str, int | None],
) -> None:
    assert parse_retry_after_delta(value) == expected


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
    endpoint_urls = (
        "https://primary.example.test",
        "https://fallback.example.test",
    )
    proxy_route_sha256 = hashlib.sha256(b"fake-proxy-route").hexdigest()
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
        egress_failure_calls: set[int] | None = None,
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
        self.egress_failure_calls = set(egress_failure_calls or ())
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
        call_number = self.egress_calls + 1
        index = min(self.egress_calls, len(self.egress_values) - 1)
        self.egress_calls += 1
        if call_number in self.egress_failure_calls:
            raise ScopedTransportError("egress_network_error")
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
                endpoint_id=request.endpoint_id,
                attempted_endpoint_ids=(request.endpoint_id,),
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
            page_offset = (request.task.page - 1) * 100
            products = _products(
                region_offset + query_offset + page_offset,
                self.product_count,
            )
            if self.duplicate and len(products) > 1:
                products[1]["id"] = products[0]["id"]
        return ScopedSearchResult(
            payload={"products": products},
            endpoint_id=request.endpoint_id,
            dest_id_sent=request.dest_id_observed,
            attempted_endpoint_ids=(request.endpoint_id,),
        )

    def search_ordered(
        self,
        request: ScopedSearchRequest,
        *,
        timeout_seconds: float,
    ) -> ScopedSearchResult:
        return self.search(request, timeout_seconds=timeout_seconds)

    def close(self) -> None:
        self.closed = True


def _run(config, plan_path: Path, transport: FakeTransport, **kwargs):
    kwargs.setdefault("sleeper", lambda _seconds: None)
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
    assert manifest["egress"]["verification_status"] == "verified_constant"
    assert manifest["egress"]["constant"] is True
    assert manifest["egress"]["checks_completed"] == 3
    assert manifest["egress"]["checks_expected"] == 3
    assert manifest["endpoint_usage"] == {
        "primary": {"attempts": 6, "pages_ok": 6},
        "fallback-1": {"attempts": 0, "pages_ok": 0},
    }
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
        assert {row["endpoint_id"] for row in rows} == {"primary"}

    moscow_checkpoint = _read_json(
        state_dir / "checkpoints/moscow/shevron/page_001.json"
    )
    rostov_checkpoint = _read_json(
        state_dir / "checkpoints/rostov-on-don/shevron/page_001.json"
    )
    assert moscow_checkpoint["checkpoint_key"] != rostov_checkpoint["checkpoint_key"]
    assert "|moscow|shevron|1" in moscow_checkpoint["checkpoint_key"]
    assert "|rostov-on-don|shevron|1" in rostov_checkpoint["checkpoint_key"]
    assert moscow_checkpoint["endpoint_id"] == "primary"
    assert moscow_checkpoint["attempted_endpoint_ids"] == ["primary"]

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


def test_runner_honors_plan_depth_with_distinct_page_identity_and_positions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 200
    plan["quality"]["expected_pages_per_query"] = 2
    _write_json(plan_path, plan)
    transport = FakeTransport()

    manifest = _run(config, plan_path, transport)

    assert manifest["complete"] is True
    assert manifest["totals"] == {
        "regions_ok": 2,
        "pages_ok": 12,
        "products_ok": 1200,
    }
    assert len(transport.search_calls) == 12
    assert [
        (request.task.query_id, request.task.page)
        for request in transport.search_calls[:6]
    ] == [
        ("shevron", 1),
        ("shevron", 2),
        ("shevrony", 1),
        ("shevrony", 2),
        ("shevron-na-lipuchke", 1),
        ("shevron-na-lipuchke", 2),
    ]
    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    assert (
        state_dir / "checkpoints/moscow/shevron/page_002.json"
    ).exists()
    mart_path = (
        root
        / "data/marts/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "products_daily.csv"
    )
    with mart_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    shevron_rows = [row for row in rows if row["query_id"] == "shevron"]
    assert len(shevron_rows) == 200
    assert [int(row["absolute_position"]) for row in shevron_rows] == list(
        range(1, 201)
    )
    assert {row["raw_file"] for row in shevron_rows} == {
        (
            "data/raw/serp_scoped/"
            "shevron-moscow-rostov-top100-pilot-v1/"
            f"moscow/{RUN_ID}/pages/shevron/page_001.json"
        ),
        (
            "data/raw/serp_scoped/"
            "shevron-moscow-rostov-top100-pilot-v1/"
            f"moscow/{RUN_ID}/pages/shevron/page_002.json"
        ),
    }


def test_top1000_resume_repeats_only_unfinished_query_segment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)

    first = FakeTransport(failure_call=25, failure_code="search_http_498")
    with pytest.raises(ScopedTransportError, match="search_http_498"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=first,
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            egress_hash_salt=b"resume-test",
        )

    assert len(first.search_calls) == 25
    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    failed_manifest = _read_json(state_dir / "manifest.json")
    assert failed_manifest["complete"] is False
    assert failed_manifest["resume"]["verified_segments"] == 2
    assert failed_manifest["totals"] == {
        "regions_ok": 0,
        "pages_ok": 20,
        "products_ok": 2000,
    }
    assert failed_manifest["resume"]["failed_segment"]["pages_written"] == 4
    assert failed_manifest["resume"]["failed_segment"][
        "attempted_endpoint_ids"
    ] == ["primary"]
    assert failed_manifest["resume"]["failed_segment"]["egress"][
        "verification_status"
    ] == "unverified"
    assert failed_manifest["resume"]["failed_segment"]["egress"]["end"] is None
    assert not (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    ).exists()
    assert validate_resumable_collection_state(
        config=config,
        plan_path=plan_path,
        run_id=RUN_ID,
        transport=FakeTransport(),
    )

    resumed = FakeTransport(egress_values=["198.51.100.20"] * 8)
    manifest = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=resumed,
        resume_run_id=RUN_ID,
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"resume-test",
    )

    assert manifest["complete"] is True
    assert len(resumed.search_calls) == 40
    assert [
        (call.task.region_id, call.task.query_id, call.task.page)
        for call in resumed.search_calls[:10]
    ] == [
        ("moscow", "shevron-na-lipuchke", page)
        for page in range(1, 11)
    ]
    assert manifest["totals"] == {
        "regions_ok": 2,
        "pages_ok": 60,
        "products_ok": 6000,
    }
    assert manifest["resume"]["verified_segments"] == 6
    assert manifest["resume"]["maximum_repeated_pages"] == 10
    assert len(manifest["resume"]["discarded_segments"]) == 1
    assert manifest["endpoint_usage"]["primary"] == {
        "attempts": 65,
        "pages_ok": 64,
    }
    mart_path = (
        root
        / "data/marts/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "products_daily.csv"
    )
    rows = list(csv.DictReader(mart_path.open(encoding="utf-8")))
    identities = {
        (
            row["region_id"],
            row["query_id"],
            row["page"],
            row["absolute_position"],
        )
        for row in rows
    }
    assert len(rows) == 3000
    assert len(identities) == len(rows)
    latest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    )
    assert latest["run_id"] == RUN_ID
    assert [item["region_id"] for item in latest["regions"]] == [
        "moscow",
        "rostov-on-don",
    ]
    segment_refs = manifest["resume"]["segments"]
    assert len(segment_refs) == 6
    assert all(
        ref["egress"]["verification_status"] == "verified_constant"
        and ref["egress"]["constant"] is True
        and "start" in ref["egress"]
        and "end" in ref["egress"]
        for ref in segment_refs
    )
    assert {
        ref["egress"]["start"]["masked"] for ref in segment_refs
    } == {"203.0.x.x", "198.51.x.x"}
    assert resumed.egress_calls == 5


def test_top1000_empty_checkpoint_is_not_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    run_dir = (
        root
        / "state/wb_collection_plans"
        / plan["collection_plan_id"]
        / RUN_ID
    )
    run_dir.mkdir(parents=True)
    _write_json(
        run_dir / "manifest.json",
        {
            "schema_version": "wb_collection_plan_manifest_v2",
            "run_id": RUN_ID,
            "collection_plan_id": plan["collection_plan_id"],
            "complete": False,
            "status": "failed",
            "resume": {
                "segments": [],
                "discarded_segments": [],
                "failed_segment": None,
            },
        },
    )
    _write_json(run_dir / "effective_plan.json", {})
    assert not validate_resumable_collection_state(
        config=config,
        plan_path=plan_path,
        run_id=RUN_ID,
        transport=FakeTransport(),
    )


def test_top1000_short_payload_records_attempt_without_canonical_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)

    with pytest.raises(CollectionPlanRunError, match="search_products_short"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(product_count=99),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )

    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    manifest = _read_json(state_dir / "manifest.json")
    discarded = manifest["resume"]["discarded_segments"]
    assert len(discarded) == 1
    assert discarded[0]["endpoint_usage"]["primary"] == {
        "attempts": 1,
        "pages_ok": 0,
    }
    assert manifest["endpoint_usage"]["primary"] == {
        "attempts": 1,
        "pages_ok": 0,
    }
    assert manifest["totals"] == {
        "regions_ok": 0,
        "pages_ok": 0,
        "products_ok": 0,
    }
    assert not (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "pages/shevron/page_001.json"
    ).exists()


def test_top1000_multiple_resume_failures_do_not_double_count_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)

    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=25),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=5),
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    manifest = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=FakeTransport(),
        resume_run_id=RUN_ID,
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
    )

    discarded = manifest["resume"]["discarded_segments"]
    assert len(discarded) == 2
    assert len({item["segment_id"] for item in discarded}) == 2
    assert manifest["endpoint_usage"]["primary"] == {
        "attempts": 70,
        "pages_ok": 68,
    }
    assert manifest["totals"]["pages_ok"] == 60


@pytest.mark.parametrize(
    "corruption",
    [
        "negative_counter",
        "boolean_counter",
        "unknown_endpoint",
        "unknown_scope",
        "duplicate_segment_id",
    ],
)
def test_top1000_resume_rejects_invalid_discarded_history_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=1),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )

    manifest_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "manifest.json"
    )
    manifest = _read_json(manifest_path)
    history = manifest["resume"]["discarded_segments"]
    if corruption == "negative_counter":
        history[0]["endpoint_usage"]["primary"]["attempts"] = -1
    elif corruption == "boolean_counter":
        history[0]["endpoint_usage"]["primary"]["attempts"] = True
    elif corruption == "unknown_endpoint":
        history[0]["endpoint_usage"]["unknown"] = {
            "attempts": 0,
            "pages_ok": 0,
        }
    elif corruption == "unknown_scope":
        history[0]["region_id"] = "unknown-region"
    elif corruption == "duplicate_segment_id":
        history.append(dict(history[0]))
    _write_json(manifest_path, manifest)

    transport = FakeTransport()
    with pytest.raises(CollectionPlanRunError, match="discarded segment"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 0


def test_top1000_changed_end_egress_is_honest_and_segment_is_repeated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)

    first = FakeTransport(
        egress_values=["203.0.113.10", "198.51.100.20"],
    )
    with pytest.raises(EgressIdentityChangedError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=first,
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            egress_hash_salt=b"changed-egress",
        )
    assert len(first.search_calls) == 10
    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    failed = _read_json(state_dir / "manifest.json")
    evidence = failed["resume"]["discarded_segments"][0]["egress"]
    assert evidence["verification_status"] == "changed"
    assert evidence["constant"] is False
    assert evidence["checks_completed"] == 2
    assert evidence["start"]["masked"] == "203.0.x.x"
    assert evidence["end"]["masked"] == "198.51.x.x"
    assert evidence["start"]["ephemeral_sha256"] != evidence["end"][
        "ephemeral_sha256"
    ]
    assert failed["resume"]["segments"] == []
    assert not (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "pages/shevron/page_001.json"
    ).exists()
    assert not (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    ).exists()

    resumed = FakeTransport(egress_values=["192.0.2.30"] * 8)
    manifest = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=resumed,
        resume_run_id=RUN_ID,
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
        egress_hash_salt=b"changed-egress",
    )
    assert len(resumed.search_calls) == 60
    assert manifest["complete"] is True
    assert manifest["totals"]["pages_ok"] == 60
    assert len(manifest["resume"]["discarded_segments"]) == 1


def test_top1000_resume_rejects_corrupt_confirmed_raw_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=11),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )

    raw_path = (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "pages/shevron/page_001.json"
    )
    raw_path.write_bytes(raw_path.read_bytes() + b"\n")
    transport = FakeTransport()
    with pytest.raises(CollectionPlanRunError, match="canonical raw conflicts"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 0


def test_top1000_resume_rejects_checkpoint_metadata_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=11),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    checkpoint_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "checkpoints/moscow/shevron/page_001.json"
    )
    checkpoint = _read_json(checkpoint_path)
    checkpoint["page"] = 9
    _write_json(checkpoint_path, checkpoint)
    transport = FakeTransport()
    with pytest.raises(CollectionPlanRunError, match="canonical checkpoint conflicts"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []


@pytest.mark.parametrize(
    "corruption",
    [
        "unknown_scope",
        "mismatched_page",
        "mismatched_path",
        "negative_endpoint_counter",
        "boolean_endpoint_counter",
        "changed_end_hash",
        "changed_end_masked",
        "full_ip_masked",
        "manifest_ref_mismatch",
    ],
)
def test_top1000_resume_rejects_tampered_verified_segment_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=11),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            egress_hash_salt=b"segment-integrity",
        )

    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    manifest_path = state_dir / "manifest.json"
    manifest = _read_json(manifest_path)
    reference = manifest["resume"]["segments"][0]
    segment_path = root / reference["path"]
    segment = _read_json(segment_path)
    canonical_raw = (
        root
        / "data/raw/serp_scoped"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "moscow"
        / RUN_ID
        / "pages/shevron/page_001.json"
    )
    canonical_checkpoint = (
        state_dir / "checkpoints/moscow/shevron/page_001.json"
    )
    checkpoint_before = canonical_checkpoint.read_bytes()
    canonical_raw.unlink()
    outside = tmp_path / "outside-sentinel.json"
    outside.write_bytes(b"outside-unchanged")

    if corruption == "unknown_scope":
        segment["region_id"] = "unknown-region"
    elif corruption == "mismatched_page":
        segment["pages"][0]["page"] = 2
    elif corruption == "mismatched_path":
        segment["pages"][0]["canonical_raw_path"] = "../outside-sentinel.json"
    elif corruption == "negative_endpoint_counter":
        segment["endpoint_usage"]["primary"]["attempts"] = -1
    elif corruption == "boolean_endpoint_counter":
        segment["endpoint_usage"]["primary"]["pages_ok"] = True
    elif corruption == "changed_end_hash":
        segment["egress"]["end"]["ephemeral_sha256"] = hashlib.sha256(
            b"different-egress"
        ).hexdigest()
    elif corruption == "changed_end_masked":
        segment["egress"]["end"]["masked"] = "198.51.x.x"
    elif corruption == "full_ip_masked":
        segment["egress"]["end"]["masked"] = "198.51.100.20"
    elif corruption == "manifest_ref_mismatch":
        reference["region_id"] = "rostov-on-don"

    if corruption != "manifest_ref_mismatch":
        _write_json(segment_path, segment)
        reference.update(
            {
                "region_id": segment["region_id"],
                "query_id": segment["query_id"],
                "sha256": hashlib.sha256(segment_path.read_bytes()).hexdigest(),
                "egress": json.loads(json.dumps(segment["egress"])),
                "pages_count": len(segment["pages"]),
            }
        )
    _write_json(manifest_path, manifest)

    transport = FakeTransport()
    with pytest.raises(CollectionPlanRunError, match="segment"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            egress_hash_salt=b"segment-integrity",
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 0
    assert not canonical_raw.exists()
    assert canonical_checkpoint.read_bytes() == checkpoint_before
    assert outside.read_bytes() == b"outside-unchanged"


def test_top1000_failed_run_leaves_previous_dual_region_latest_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    first_run_id = "20260726_110000Z"
    second_run_id = "20260726_120000Z"

    first_manifest = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=FakeTransport(),
        run_id=first_run_id,
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
    )
    assert first_manifest["complete"] is True
    latest_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    )
    before = latest_path.read_bytes()

    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=35),
            run_id=second_run_id,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )

    assert latest_path.read_bytes() == before
    assert _read_json(latest_path)["run_id"] == first_run_id
    failed = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / second_run_id
        / "manifest.json"
    )
    assert failed["complete"] is False
    assert failed["regional_latest"]["status"] == "not_published"


def test_top1000_dual_region_latest_visibility_is_atomic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=FakeTransport(),
        run_id="20260726_110000Z",
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
    )
    latest_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    )
    before = latest_path.read_bytes()

    def fail_second_region_latest(event: str, path: Path) -> None:
        if (
            event == "file_fsynced"
            and "latest_generations" in path.parts
            and path.name == "rostov-on-don.json"
        ):
            raise OSError("injected latest publication failure")

    with pytest.raises(OSError, match="injected latest"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(),
            run_id="20260726_120000Z",
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            write_event_hook=fail_second_region_latest,
        )
    assert latest_path.read_bytes() == before
    assert _read_json(latest_path)["run_id"] == "20260726_110000Z"
    pending_manifest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "20260726_120000Z"
        / "manifest.json"
    )
    assert pending_manifest["status"] == "publication_pending"
    assert pending_manifest["complete"] is False

    resume_transport = FakeTransport()
    reconciled = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=resume_transport,
        resume_run_id="20260726_120000Z",
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
    )
    assert reconciled["status"] == "success"
    assert reconciled["complete"] is True
    assert _read_json(latest_path)["run_id"] == "20260726_120000Z"
    assert resume_transport.resolve_calls == []
    assert resume_transport.search_calls == []
    assert resume_transport.egress_calls == 0


def test_attested_input_mutation_before_latest_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    calls = 0

    def integrity_gate() -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise CollectionPlanRunError("attested input changed")

    with pytest.raises(CollectionPlanRunError, match="attested input changed"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            input_integrity_gate=integrity_gate,
        )
    latest = (
        root
        / "state/wb_collection_plans"
        / plan["collection_plan_id"]
        / "latest.json"
    )
    assert not latest.exists()


def test_top1000_resume_reconciles_crash_after_durable_latest_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    latest_path = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / "latest.json"
    )
    state_dir = (
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
    )
    pointer_durable = False
    injected = False

    def fail_after_pointer(event: str, path: Path) -> None:
        nonlocal pointer_durable, injected
        if event == "directory_fsynced" and path == latest_path:
            pointer_durable = True
            return
        if (
            pointer_durable
            and not injected
            and event == "file_fsynced"
            and path == state_dir / "manifest.json"
        ):
            injected = True
            raise OSError("injected post-pointer manifest failure")

    with pytest.raises(OSError, match="post-pointer"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
            write_event_hook=fail_after_pointer,
        )

    pointer_before = latest_path.read_bytes()
    assert _read_json(latest_path)["run_id"] == RUN_ID
    pending = _read_json(state_dir / "manifest.json")
    assert pending["status"] == "publication_pending"
    assert pending["complete"] is False

    resume_transport = FakeTransport()
    manifest = run_collection_plan(
        config=config,
        plan_path=plan_path,
        no_publish=True,
        transport=resume_transport,
        resume_run_id=RUN_ID,
        now=lambda: FIXED_NOW,
        sleeper=lambda _seconds: None,
    )
    assert manifest["status"] == "success"
    assert manifest["complete"] is True
    assert latest_path.read_bytes() == pointer_before
    assert resume_transport.resolve_calls == []
    assert resume_transport.search_calls == []
    assert resume_transport.egress_calls == 0


@pytest.mark.parametrize(
    "mismatch",
    ["endpoint_urls", "request_params", "proxy_route"],
)
def test_top1000_resume_rejects_transport_fingerprint_mismatch_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    plan = _read_json(plan_path)
    plan["depth"] = 1000
    plan["quality"]["expected_pages_per_query"] = 10
    _write_json(plan_path, plan)
    with pytest.raises(ScopedTransportError):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=FakeTransport(failure_call=11),
            run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )

    transport = FakeTransport()
    if mismatch == "endpoint_urls":
        transport.endpoint_urls = (
            "https://changed-primary.example.test",
            "https://fallback.example.test",
        )
    elif mismatch == "request_params":
        transport.request_params = {
            **transport.request_params,
            "curr": "changed",
        }
    else:
        transport.proxy_route_sha256 = hashlib.sha256(
            b"changed-proxy-route"
        ).hexdigest()

    with pytest.raises(CollectionPlanRunError, match="transport fingerprint"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=transport,
            resume_run_id=RUN_ID,
            now=lambda: FIXED_NOW,
            sleeper=lambda _seconds: None,
        )
    assert transport.resolve_calls == []
    assert transport.search_calls == []
    assert transport.egress_calls == 0


def test_top1000_estimated_window_rejects_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, _plan_path = _project(tmp_path, monkeypatch)
    plan_path = (
        root
        / "config/wb/collection_plans/"
        "shevron-moscow-rostov-top1000-v1.json"
    )
    plan = _read_json(plan_path)
    plan["enabled"] = True
    _write_json(plan_path, plan)
    bundle = load_collection_plan_bundle(
        project_root=root,
        plan_path=plan_path,
        region_registry_path=root / REGIONS_RELATIVE,
    )
    single = FakeTransport()
    single.endpoint_policy = EffectiveEndpointPolicy(
        selection_mode="ordered_fallbacks",
        endpoint_ids=("primary",),
        pinned_endpoint_id="primary",
    )
    single.endpoint_urls = ("https://primary.example.test",)
    double = FakeTransport()
    one_runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=single,
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: FIXED_NOW,
    )
    two_runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=double,
        no_publish=True,
        run_id=RUN_ID,
        now=lambda: FIXED_NOW,
    )
    planned_pages = 600
    one_estimate = one_runner._estimated_remaining_seconds(
        bundle=bundle,
        pending_pages=planned_pages,
    )
    two_estimate = two_runner._estimated_remaining_seconds(
        bundle=bundle,
        pending_pages=planned_pages,
    )
    assert two_estimate - one_estimate == pytest.approx(
        planned_pages * config.runtime.http_timeout_seconds
    )
    latest_safe_finish = datetime(
        2026,
        7,
        26,
        21,
        0,
        0,
        tzinfo=timezone.utc,
    )
    gate_now = latest_safe_finish - timedelta(
        seconds=(one_estimate + two_estimate) / 2
    )
    DeadlineGuard.for_current_day(
        now=lambda: gate_now
    ).ensure_estimated_window(one_estimate)
    with pytest.raises(CollectionPlanRunError, match="overlaps nightly 00:15"):
        DeadlineGuard.for_current_day(
            now=lambda: gate_now
        ).ensure_estimated_window(two_estimate)

    with pytest.raises(CollectionPlanRunError, match="overlaps nightly 00:15"):
        run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            transport=double,
            run_id=RUN_ID,
            now=lambda: gate_now,
            sleeper=lambda _seconds: None,
        )
    assert double.resolve_calls == []
    assert double.search_calls == []
    assert double.egress_calls == 0

def test_runner_uses_production_serp_pacing_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    config.raw["serp"]["sleep_between_pages_ms"] = 4500
    config.raw["serp"]["sleep_between_queries_ms"] = 12000
    sleeps: list[float] = []

    _run(
        config,
        plan_path,
        FakeTransport(),
        sleeper=sleeps.append,
    )

    assert sleeps == [4.5, 12.0] * 6


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
    assert manifest["egress"]["verification_status"] == "unverified"
    assert manifest["egress"]["constant"] is None
    assert manifest["egress"]["checks_completed"] == 1
    assert manifest["egress"]["checks_expected"] == 3
    assert manifest["endpoint_usage"] == {
        "primary": {"attempts": 2, "pages_ok": 1},
        "fallback-1": {"attempts": 0, "pages_ok": 0},
    }
    assert manifest["regions"][0]["failed_endpoint_attempt"] == {
        "query_id": "shevrony",
        "page": 1,
        "endpoint_id": "primary",
        "attempted_endpoint_ids": ["primary"],
        "error_code": "search_http_498",
    }
    assert manifest["error"]["endpoint_id"] == "primary"
    assert manifest["error"]["attempted_endpoint_ids"] == ["primary"]
    assert manifest["regions"][0]["dest_resolution_status"] == "resolved_and_sent"
    assert manifest["regions"][0]["pages_ok"] == 1
    assert manifest["regions"][1]["status"] == "pending"
    checkpoints = list((state_dir / "checkpoints").rglob("*.json"))
    assert len(checkpoints) == 1
    assert "rotation" not in manifest["error"]["error_code"]


@pytest.mark.parametrize(
    ("transport", "error_match", "expected_error"),
    [
        (FakeTransport(empty=True), "products_empty", "search_products_empty"),
        (
            FakeTransport(product_count=99),
            "products_short",
            "search_products_short expected=100 actual=99",
        ),
        (
            FakeTransport(duplicate=True),
            "product_duplicate",
            "search_product_duplicate",
        ),
        (
            FakeTransport(malformed=True),
            "product_id_malformed",
            "search_product_id_malformed",
        ),
    ],
)
def test_payload_failures_are_fail_closed_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport: FakeTransport,
    error_match: str,
    expected_error: str,
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
    assert manifest["egress"]["verification_status"] == "unverified"
    assert manifest["egress"]["constant"] is None
    assert manifest["egress"]["checks_completed"] == 1
    assert manifest["egress"]["checks_expected"] == 3
    assert manifest["endpoint_usage"] == {
        "primary": {"attempts": 1, "pages_ok": 0},
        "fallback-1": {"attempts": 0, "pages_ok": 0},
    }
    assert manifest["regions"][0]["failed_endpoint_attempt"] == {
        "query_id": "shevron",
        "page": 1,
        "endpoint_id": "primary",
        "attempted_endpoint_ids": ["primary"],
        "error_code": expected_error,
    }
    assert manifest["error"]["endpoint_id"] == "primary"
    assert manifest["error"]["attempted_endpoint_ids"] == ["primary"]


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
    assert manifest["egress"]["verification_status"] == "changed"
    assert manifest["egress"]["constant"] is False
    assert manifest["egress"]["checks_completed"] == 2
    assert manifest["egress"]["checks_expected"] == 3


@pytest.mark.parametrize(
    ("failure_call", "expected_completed", "expected_search_calls"),
    [
        (2, 1, 3),
        (3, 2, 6),
    ],
)
def test_egress_transport_failure_is_unverified_not_changed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_call: int,
    expected_completed: int,
    expected_search_calls: int,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    transport = FakeTransport(egress_failure_calls={failure_call})

    with pytest.raises(ScopedTransportError, match="egress_network_error"):
        _run(config, plan_path, transport)

    manifest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "manifest.json"
    )
    assert len(transport.search_calls) == expected_search_calls
    assert manifest["complete"] is False
    assert manifest["egress"]["verification_status"] == "unverified"
    assert manifest["egress"]["constant"] is None
    assert manifest["egress"]["checks_completed"] == expected_completed
    assert manifest["egress"]["checks_expected"] == 3


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


def test_final_manifest_write_is_rejected_inside_deadline_reserve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)

    class MutableClock:
        current = FIXED_NOW

        def __call__(self) -> datetime:
            return self.current

    clock = MutableClock()
    runner = CollectionPlanRunner(
        config=config,
        plan_path=plan_path,
        transport=FakeTransport(),
        no_publish=True,
        run_id=RUN_ID,
        now=clock,
    )
    target = root / "state/wb_collection_plans/test/manifest.json"

    def forbidden_write(*args, **kwargs):
        raise AssertionError("late final manifest write must not be attempted")

    monkeypatch.setattr(runner_module, "_write_new_bytes", forbidden_write)
    clock.current = datetime(2026, 7, 26, 20, 44, 56, tzinfo=timezone.utc)
    with pytest.raises(CollectionPlanRunError, match="deadline reserve"):
        runner._write(target, b"{}", final_manifest=True)
    assert not target.exists()


def test_dedicated_launcher_handles_lock_contention_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_collection_plan.py",
            "--config",
            str(root / "config/config.yaml"),
            "--plan-file",
            str(plan_path),
            "--no-publish",
        ],
    )
    monkeypatch.setattr(dedicated_launcher, "load_config", lambda _path: config)

    def locked(**kwargs):
        raise RunLockedError("collection plan lock is busy")

    monkeypatch.setattr(dedicated_launcher, "run_collection_plan", locked)

    assert dedicated_launcher.main() == 1
    captured = capsys.readouterr()
    assert "collection plan lock is busy" in captured.err
    assert "Traceback" not in captured.err


def test_dedicated_launcher_forged_wrapped_flag_fails_before_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("PARSER_WB_LOCK_V3_WRAPPED", "1")
    monkeypatch.setattr(
        dedicated_launcher,
        "require_official_live_entry_lease",
        lambda **_kwargs: (_ for _ in ()).throw(
            CriticalPipelineError("host lease invalid")
        ),
    )
    monkeypatch.setattr(
        dedicated_launcher,
        "load_config",
        lambda _path: pytest.fail("config must not load"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_collection_plan.py",
            "--config",
            str(tmp_path / "config.yaml"),
            "--plan-file",
            str(tmp_path / "plan.json"),
            "--no-publish",
        ],
    )
    assert dedicated_launcher.main() == 1
    assert "host lease invalid" in capsys.readouterr().err


def test_dedicated_launcher_forwards_explicit_resume_run_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _root, config, plan_path = _project(tmp_path, monkeypatch)
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_collection_plan.py",
            "--config",
            str(config.config_file),
            "--plan-file",
            str(plan_path),
            "--no-publish",
            "--resume-run-id",
            RUN_ID,
        ],
    )
    monkeypatch.setattr(dedicated_launcher, "load_config", lambda _path: config)

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"run_id": RUN_ID, "status": "success", "complete": True}

    monkeypatch.setattr(dedicated_launcher, "run_collection_plan", fake_run)
    assert dedicated_launcher.main() == 0
    assert captured["resume_run_id"] == RUN_ID
    assert captured["no_publish"] is True


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
    def __init__(self, responses: list[FakeResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

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


def _ordered_requests_transport(session: FakeSession) -> RequestsScopedTransport:
    return RequestsScopedTransport(
        session=session,  # type: ignore[arg-type]
        request_params={"appType": "1"},
        endpoint_urls=(
            "https://primary.example.test",
            "https://fallback.example.test",
        ),
        timeout_seconds=45,
        referer_base="https://www.wildberries.ru/search?query=",
        resolver_url="https://geo.example.test",
        egress_check_url="https://ip.example.test",
        egress_session=session,  # type: ignore[arg-type]
    )


def _scoped_search_request(endpoint_id: str = "primary") -> ScopedSearchRequest:
    task = type("Task", (), {"query": "шеврон"})()
    return ScopedSearchRequest(
        task=task,  # type: ignore[arg-type]
        dest_id_observed="-535680",
        endpoint_id=endpoint_id,
        params={"query": "шеврон", "page": "1", "dest": "-535680"},
    )


def test_ordered_search_uses_production_endpoint_order_and_promotes_success() -> None:
    session = FakeSession(
        [
            FakeResponse(status_code=498),
            FakeResponse(payload={"products": _products(1_000)}),
            FakeResponse(payload={"products": _products(2_000)}),
        ]
    )
    transport = _ordered_requests_transport(session)

    first = transport.search_ordered(
        _scoped_search_request("primary"),
        timeout_seconds=10,
    )
    second = transport.search_ordered(
        _scoped_search_request(transport.endpoint_policy.pinned_endpoint_id),
        timeout_seconds=10,
    )

    assert first.endpoint_id == "fallback-1"
    assert first.attempted_endpoint_ids == ("primary", "fallback-1")
    assert transport.endpoint_policy.pinned_endpoint_id == "fallback-1"
    assert second.endpoint_id == "fallback-1"
    assert second.attempted_endpoint_ids == ("fallback-1",)
    assert [url for url, _kwargs in session.calls] == [
        "https://primary.example.test",
        "https://fallback.example.test",
        "https://fallback.example.test",
    ]
    assert all(
        kwargs["params"]["dest"] == "-535680"
        for _url, kwargs in session.calls
    )


def test_ordered_search_does_not_fallback_for_non_retryable_http() -> None:
    session = FakeSession(
        [
            FakeResponse(status_code=403),
            FakeResponse(payload={"products": _products(1_000)}),
        ]
    )
    transport = _ordered_requests_transport(session)

    with pytest.raises(ScopedTransportError, match="search_http_403"):
        transport.search_ordered(
            _scoped_search_request("primary"),
            timeout_seconds=10,
        )

    assert [url for url, _kwargs in session.calls] == [
        "https://primary.example.test"
    ]


def test_ordered_search_falls_back_for_production_nested_promo_anomaly() -> None:
    promo_products = [
        {**product, "log": {"promotion": 1}}
        for product in _products(1_000)
    ]
    session = FakeSession(
        [
            FakeResponse(payload={"data": {"products": promo_products}}),
            FakeResponse(payload={"products": _products(2_000)}),
        ]
    )
    transport = _ordered_requests_transport(session)

    result = transport.search_ordered(
        _scoped_search_request("primary"),
        timeout_seconds=10,
    )

    assert result.endpoint_id == "fallback-1"
    assert result.attempted_endpoint_ids == ("primary", "fallback-1")


def test_ordered_search_rechecks_runtime_deadline_before_each_endpoint_attempt() -> None:
    session = FakeSession(
        [
            FakeResponse(status_code=498),
            FakeResponse(payload={"products": _products(1_000)}),
        ]
    )
    transport = _ordered_requests_transport(session)
    checked: list[float] = []

    def deadline_gate(requested: float) -> float:
        checked.append(requested)
        return requested

    transport.set_network_timeout_provider(deadline_gate)
    result = transport.search_ordered(
        _scoped_search_request(),
        timeout_seconds=45,
    )
    assert result.endpoint_id == "fallback-1"
    assert checked == [45, 45]
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    (
        "search_responses",
        "expected_attempts",
        "expected_endpoint",
        "expected_error",
    ),
    [
        (
            [FakeResponse(status_code=498), FakeResponse(status_code=498)],
            ["primary", "fallback-1"],
            "fallback-1",
            "search_http_498",
        ),
        (
            [FakeResponse(status_code=403)],
            ["primary"],
            "primary",
            "search_http_403",
        ),
        (
            [requests.RequestException("offline network failure")],
            ["primary"],
            "primary",
            "search_network_error",
        ),
        (
            [FakeResponse(json_error=ValueError("invalid"))],
            ["primary"],
            "primary",
            "search_invalid_json",
        ),
        (
            [FakeResponse(payload={})],
            ["primary"],
            "primary",
            "search_products_empty",
        ),
    ],
)
def test_failed_ordered_search_persists_sanitized_attempt_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    search_responses: list[FakeResponse | Exception],
    expected_attempts: list[str],
    expected_endpoint: str,
    expected_error: str,
) -> None:
    root, config, plan_path = _project(tmp_path, monkeypatch)
    session = FakeSession(
        [
            FakeResponse(text="203.0.113.10"),
            FakeResponse(payload={"xinfo": "dest=-535680"}),
            FakeResponse(payload={"xinfo": "dest=-2228364"}),
            *search_responses,
        ]
    )
    transport = _ordered_requests_transport(session)

    with pytest.raises(ScopedTransportError, match=expected_error):
        _run(config, plan_path, transport)

    manifest = _read_json(
        root
        / "state/wb_collection_plans"
        / "shevron-moscow-rostov-top100-pilot-v1"
        / RUN_ID
        / "manifest.json"
    )
    failed_region = manifest["regions"][0]
    assert manifest["status"] == "failed"
    assert manifest["complete"] is False
    assert manifest["error"]["error_code"] == expected_error
    assert manifest["error"]["endpoint_id"] == expected_endpoint
    assert manifest["error"]["attempted_endpoint_ids"] == expected_attempts
    assert failed_region["status"] == "failed"
    assert failed_region["failed_endpoint_attempt"] == {
        "query_id": "shevron",
        "page": 1,
        "endpoint_id": expected_endpoint,
        "attempted_endpoint_ids": expected_attempts,
        "error_code": expected_error,
    }
    assert manifest["endpoint_usage"] == {
        "primary": {
            "attempts": 1,
            "pages_ok": 0,
        },
        "fallback-1": {
            "attempts": int("fallback-1" in expected_attempts),
            "pages_ok": 0,
        },
    }
    serialized = json.dumps(manifest, ensure_ascii=False)
    assert "example.test" not in serialized
    assert "offline network failure" not in serialized


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
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)

    class CaptureSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.proxies: dict[str, str] = {}
            self.trust_env = True

        def close(self) -> None:
            pass

    sessions: list[CaptureSession] = []

    def session_factory():
        session = CaptureSession()
        sessions.append(session)
        return session

    monkeypatch.setattr(requests, "Session", session_factory)
    transport = RequestsScopedTransport.from_config(config)

    assert len(sessions) == 1
    assert transport.egress_session is transport.session
    assert "cookie" not in transport.session.headers
    assert "authorization" not in transport.session.headers
    assert "cookie" in transport.request_headers
    assert "authorization" in transport.request_headers
    assert transport.session.trust_env is False


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
