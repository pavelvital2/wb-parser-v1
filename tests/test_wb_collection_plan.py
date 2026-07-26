from __future__ import annotations

import copy
import hashlib
import json
import shutil
import socket
from pathlib import Path

import pytest

from app.serp.collection_plan import (
    CollectionPlanValidationError,
    EffectiveEndpointPolicy,
    ResolvedDestination,
    build_effective_plan_snapshot,
    canonical_effective_plan_bytes,
    canonical_effective_plan_sha256,
    exact_file_sha256,
    load_collection_plan,
    load_collection_plan_bundle,
    load_query_pack,
    load_region_registry,
    register_query_pack_provenance,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACK_RELATIVE = Path("config/wb/query_packs/shevron-core/2026-07-26.1.json")
PLAN_RELATIVE = Path(
    "config/wb/collection_plans/shevron-moscow-rostov-top100-pilot-v1.json"
)
REGIONS_RELATIVE = Path("config/wb/regions.json")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _copy_stage1_config(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    shutil.copytree(PROJECT_ROOT / "config/wb", root / "config/wb")
    return root


def _load_bundle(root: Path, *, provenance_path: Path | None = None):
    return load_collection_plan_bundle(
        project_root=root,
        plan_path=root / PLAN_RELATIVE,
        region_registry_path=root / REGIONS_RELATIVE,
        provenance_path=provenance_path,
    )


def _enable_pilot(root: Path) -> None:
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["enabled"] = True
    _write_json(plan_path, plan)

    regions_path = root / REGIONS_RELATIVE
    registry = _read_json(regions_path)
    for region in registry["regions"]:
        region["enabled"] = True
    _write_json(regions_path, registry)


def _resolved_destinations() -> dict[str, ResolvedDestination]:
    return {
        "moscow": ResolvedDestination(
            region_id="moscow",
            dest_id_observed="-535680",
            dest_resolved_at_utc="2026-07-26T07:31:00Z",
        ),
        "rostov-on-don": ResolvedDestination(
            region_id="rostov-on-don",
            dest_id_observed="-2228364",
            dest_resolved_at_utc="2026-07-26T07:31:01+00:00",
        ),
    }


def _endpoint_policy() -> EffectiveEndpointPolicy:
    return EffectiveEndpointPolicy(
        selection_mode="ordered_fallbacks",
        endpoint_ids=("primary-internal-v18", "fallback-search-v18"),
        pinned_endpoint_id="fallback-search-v18",
    )


def test_committed_stage1_bundle_is_valid_and_disabled() -> None:
    bundle = _load_bundle(PROJECT_ROOT)

    assert bundle.query_pack.query_pack_id == "shevron-core"
    assert bundle.query_pack.version == "2026-07-26.1"
    assert len(bundle.query_pack.queries) == 30
    assert bundle.collection_plan.enabled is False
    assert bundle.collection_plan.publication_mode == "none"
    assert bundle.collection_plan.sellers_mode == "disabled"
    assert bundle.collection_plan.proxy_rotation_mode == "disabled"
    assert bundle.query_pack_sha256 == exact_file_sha256(PROJECT_ROOT / PACK_RELATIVE)
    assert bundle.collection_plan_sha256 == exact_file_sha256(
        PROJECT_ROOT / PLAN_RELATIVE
    )
    assert bundle.region_registry_sha256 == exact_file_sha256(
        PROJECT_ROOT / REGIONS_RELATIVE
    )
    assert bundle.enabled_queries == ()
    assert bundle.enabled_regions == ()
    assert [region.region_id for region in bundle.region_registry.regions] == [
        "moscow",
        "rostov-on-don",
    ]
    assert all(not region.enabled for region in bundle.region_registry.regions)
    assert all(region.dest_id is None for region in bundle.region_registry.regions)
    assert all(
        region.dest_resolution_status == "unresolved"
        for region in bundle.region_registry.regions
    )


def test_stable_query_ids_are_explicit_unique_and_not_positional() -> None:
    pack = load_query_pack(PROJECT_ROOT / PACK_RELATIVE)
    query_ids = [query.query_id for query in pack.queries]

    assert len(query_ids) == len(set(query_ids)) == 30
    assert query_ids[:3] == ["shevron-na-lipuchke", "shevron", "shevrony"]
    assert all(not query_id.removeprefix("q").isdigit() for query_id in query_ids)


def test_source_hashes_use_exact_file_bytes(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    bundle = _load_bundle(root)

    assert bundle.query_pack.source_sha256 == hashlib.sha256(
        (root / PACK_RELATIVE).read_bytes()
    ).hexdigest()
    assert bundle.collection_plan.source_sha256 == exact_file_sha256(
        root / PLAN_RELATIVE
    )
    assert bundle.region_registry.source_sha256 == exact_file_sha256(
        root / REGIONS_RELATIVE
    )

    original_hash = bundle.query_pack.source_sha256
    pack_path = root / PACK_RELATIVE
    pack_path.write_bytes(pack_path.read_bytes() + b" ")
    changed = load_query_pack(pack_path)

    assert changed.query_pack_id == bundle.query_pack.query_pack_id
    assert changed.version == bundle.query_pack.version
    assert changed.source_sha256 != original_hash


@pytest.mark.parametrize(
    ("relative_path", "loader"),
    [
        (PACK_RELATIVE, load_query_pack),
        (PLAN_RELATIVE, load_collection_plan),
        (REGIONS_RELATIVE, load_region_registry),
    ],
)
def test_every_source_hash_is_formatting_sensitive(
    tmp_path: Path,
    relative_path: Path,
    loader,
) -> None:
    root = _copy_stage1_config(tmp_path)
    path = root / relative_path
    original = loader(path)
    path.write_bytes(path.read_bytes() + b" ")
    changed = loader(path)

    assert changed.source_sha256 != original.source_sha256
    assert changed.source_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_query_pack_provenance_is_idempotent_and_fails_closed_on_hash_mismatch(
    tmp_path: Path,
) -> None:
    root = _copy_stage1_config(tmp_path)
    pack_path = root / PACK_RELATIVE
    provenance_path = (
        root
        / "state/wb_collection_plans/provenance/query_pack_versions.json"
    )
    first_pack = load_query_pack(pack_path)

    assert register_query_pack_provenance(
        provenance_path=provenance_path,
        query_pack=first_pack,
        project_root=root,
    )
    first_bytes = provenance_path.read_bytes()
    assert not register_query_pack_provenance(
        provenance_path=provenance_path,
        query_pack=first_pack,
        project_root=root,
    )
    assert provenance_path.read_bytes() == first_bytes

    pack_path.write_bytes(pack_path.read_bytes() + b" ")
    changed_pack = load_query_pack(pack_path)
    with pytest.raises(CollectionPlanValidationError, match="provenance mismatch"):
        register_query_pack_provenance(
            provenance_path=provenance_path,
            query_pack=changed_pack,
            project_root=root,
        )
    assert provenance_path.read_bytes() == first_bytes


def test_malformed_provenance_fails_closed_without_overwrite(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    pack = load_query_pack(root / PACK_RELATIVE)
    provenance_path = (
        root
        / "state/wb_collection_plans/provenance/query_pack_versions.json"
    )
    provenance_path.parent.mkdir(parents=True)
    malformed = b"{not-json\n"
    provenance_path.write_bytes(malformed)

    with pytest.raises(CollectionPlanValidationError, match="invalid JSON"):
        register_query_pack_provenance(
            provenance_path=provenance_path,
            query_pack=pack,
            project_root=root,
        )
    assert provenance_path.read_bytes() == malformed


def test_bundle_load_with_provenance_records_exact_pack_hash(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    provenance_path = (
        root
        / "state/wb_collection_plans/provenance/query_pack_versions.json"
    )
    bundle = _load_bundle(root, provenance_path=provenance_path)
    payload = _read_json(provenance_path)

    assert payload == {
        "schema_version": "wb_query_pack_provenance_v1",
        "query_packs": [
            {
                "query_pack_id": "shevron-core",
                "query_pack_sha256": bundle.query_pack.source_sha256,
                "version": "2026-07-26.1",
            }
        ],
    }


def test_provenance_rejects_symlinked_parent_and_target(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    pack = load_query_pack(root / PACK_RELATIVE)
    provenance_root = root / "state/wb_collection_plans"
    provenance_root.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    provenance_parent = provenance_root / "provenance"
    provenance_parent.symlink_to(outside, target_is_directory=True)
    expected = provenance_parent / "query_pack_versions.json"

    with pytest.raises(CollectionPlanValidationError, match="symlink"):
        register_query_pack_provenance(
            provenance_path=expected,
            query_pack=pack,
            project_root=root,
        )
    assert not (outside / "query_pack_versions.json").exists()

    provenance_parent.unlink()
    provenance_parent.mkdir()
    outside_target = outside / "target.json"
    outside_target.write_bytes(b"unchanged")
    expected.symlink_to(outside_target)
    with pytest.raises(CollectionPlanValidationError, match="regular non-symlink"):
        register_query_pack_provenance(
            provenance_path=expected,
            query_pack=pack,
            project_root=root,
        )
    assert outside_target.read_bytes() == b"unchanged"


def test_loader_performs_zero_network_calls_and_no_implicit_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_stage1_config(tmp_path)
    before = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }

    def forbidden_network(*args, **kwargs):
        raise AssertionError("network access is forbidden in Stage 1")

    monkeypatch.setattr(socket, "socket", forbidden_network)
    monkeypatch.setattr(socket, "create_connection", forbidden_network)

    bundle = _load_bundle(root)
    after = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }

    assert bundle.collection_plan.collection_plan_id
    assert after == before


def test_bundle_rejects_plan_and_registry_outside_project_root(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    external_plan = tmp_path / "external-plan.json"
    external_registry = tmp_path / "external-regions.json"
    shutil.copy2(root / PLAN_RELATIVE, external_plan)
    shutil.copy2(root / REGIONS_RELATIVE, external_registry)

    with pytest.raises(CollectionPlanValidationError, match="plan_path must be inside"):
        load_collection_plan_bundle(
            project_root=root,
            plan_path=external_plan,
            region_registry_path=root / REGIONS_RELATIVE,
        )
    with pytest.raises(
        CollectionPlanValidationError,
        match="region_registry_path must be inside",
    ):
        load_collection_plan_bundle(
            project_root=root,
            plan_path=root / PLAN_RELATIVE,
            region_registry_path=external_registry,
        )


def test_bundle_rejects_noncanonical_and_symlinked_sources(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    nested_plan = root / "config/wb/collection_plans/nested/plan.json"
    nested_plan.parent.mkdir()
    shutil.copy2(root / PLAN_RELATIVE, nested_plan)
    alternate_registry = root / "config/wb/alternate-regions.json"
    shutil.copy2(root / REGIONS_RELATIVE, alternate_registry)

    with pytest.raises(CollectionPlanValidationError, match="direct child"):
        load_collection_plan_bundle(
            project_root=root,
            plan_path=nested_plan,
            region_registry_path=root / REGIONS_RELATIVE,
        )
    with pytest.raises(CollectionPlanValidationError, match="must be exactly"):
        load_collection_plan_bundle(
            project_root=root,
            plan_path=root / PLAN_RELATIVE,
            region_registry_path=alternate_registry,
        )

    plan_link = root / "config/wb/collection_plans/linked-plan.json"
    plan_link.symlink_to(root / PLAN_RELATIVE)
    with pytest.raises(CollectionPlanValidationError, match="symlinks"):
        load_collection_plan_bundle(
            project_root=root,
            plan_path=plan_link,
            region_registry_path=root / REGIONS_RELATIVE,
        )

    registry_target = root / "config/wb/regions-target.json"
    (root / REGIONS_RELATIVE).replace(registry_target)
    (root / REGIONS_RELATIVE).symlink_to(registry_target)
    with pytest.raises(CollectionPlanValidationError, match="symlinks"):
        _load_bundle(root)


def test_bundle_rejects_symlinked_query_pack(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    pack_path = root / PACK_RELATIVE
    pack_target = root / "config/wb/query_packs/pack-target.json"
    pack_path.replace(pack_target)
    pack_path.symlink_to(pack_target)

    with pytest.raises(CollectionPlanValidationError, match="symlinks"):
        _load_bundle(root)


@pytest.mark.parametrize(
    ("mutator", "error_match"),
    [
        (
            lambda root: _mutate_plan(root, "depth", 150),
            "depth must be one of",
        ),
        (
            lambda root: _mutate_plan(
                root,
                "query_ids",
                ["shevron", "shevron", "shevron-na-lipuchke"],
            ),
            "duplicate ID",
        ),
        (
            lambda root: _mutate_plan(
                root,
                "query_ids",
                ["unknown-query", "shevrony", "shevron-na-lipuchke"],
            ),
            "unknown query",
        ),
        (
            lambda root: _mutate_plan(
                root,
                "region_set",
                ["unknown-region", "rostov-on-don"],
            ),
            "unknown region",
        ),
    ],
)
def test_plan_rejects_invalid_depth_duplicate_and_unknown_references(
    tmp_path: Path,
    mutator,
    error_match: str,
) -> None:
    root = _copy_stage1_config(tmp_path)
    mutator(root)

    with pytest.raises(CollectionPlanValidationError, match=error_match):
        _load_bundle(root)


def _mutate_plan(root: Path, key: str, value: object) -> None:
    path = root / PLAN_RELATIVE
    payload = _read_json(path)
    payload[key] = value
    if key == "query_ids":
        payload["quality"]["expected_queries_per_region"] = len(value)
    _write_json(path, payload)


def test_enabled_plan_rejects_disabled_region_reference(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["enabled"] = True
    _write_json(plan_path, plan)

    with pytest.raises(CollectionPlanValidationError, match="disabled regions"):
        _load_bundle(root)


def test_enabled_plan_rejects_disabled_query_reference(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    pack_path = root / PACK_RELATIVE
    pack = _read_json(pack_path)
    query = next(item for item in pack["queries"] if item["query_id"] == "shevron")
    query["enabled"] = False
    _write_json(pack_path, pack)

    with pytest.raises(CollectionPlanValidationError, match="disabled queries"):
        _load_bundle(root)


def test_enabled_selection_preserves_collection_plan_order(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    bundle = _load_bundle(root)

    assert [query.query_id for query in bundle.enabled_queries] == [
        "shevron",
        "shevrony",
        "shevron-na-lipuchke",
    ]
    assert [region.region_id for region in bundle.enabled_regions] == [
        "moscow",
        "rostov-on-don",
    ]


def test_query_pack_rejects_duplicate_id_and_unknown_category(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    pack_path = root / PACK_RELATIVE
    pack = _read_json(pack_path)
    pack["queries"][1]["query_id"] = pack["queries"][0]["query_id"]
    _write_json(pack_path, pack)

    with pytest.raises(CollectionPlanValidationError, match="duplicate ID"):
        load_query_pack(pack_path)

    pack = _read_json(PROJECT_ROOT / PACK_RELATIVE)
    pack["queries"][0]["category_id"] = "unknown-category"
    _write_json(pack_path, pack)
    with pytest.raises(CollectionPlanValidationError, match="unknown category"):
        load_query_pack(pack_path)


def test_region_registry_rejects_duplicate_id_and_unknown_keys(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    regions_path = root / REGIONS_RELATIVE
    registry = _read_json(regions_path)
    registry["regions"][1]["region_id"] = registry["regions"][0]["region_id"]
    _write_json(regions_path, registry)

    with pytest.raises(CollectionPlanValidationError, match="duplicate ID"):
        load_region_registry(regions_path)

    registry = _read_json(PROJECT_ROOT / REGIONS_RELATIVE)
    registry["unexpected"] = True
    _write_json(regions_path, registry)
    with pytest.raises(CollectionPlanValidationError, match="unknown keys"):
        load_region_registry(regions_path)


def test_schema_version_and_unsafe_pack_path_are_rejected(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["schema_version"] = "wb_collection_plan_v999"
    _write_json(plan_path, plan)

    with pytest.raises(CollectionPlanValidationError, match="schema_version"):
        load_collection_plan(plan_path)

    plan = _read_json(PROJECT_ROOT / PLAN_RELATIVE)
    plan["query_pack_file"] = "../outside.json"
    _write_json(plan_path, plan)
    with pytest.raises(CollectionPlanValidationError, match="safe project-relative"):
        load_collection_plan(plan_path)


def test_duplicate_json_keys_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text(
        '{"schema_version":"wb_query_pack_v1","query_pack_id":"a",'
        '"query_pack_id":"b","version":"1","enabled":true,'
        '"categories":[],"queries":[]}',
        encoding="utf-8",
    )

    with pytest.raises(CollectionPlanValidationError, match="duplicate JSON key"):
        load_query_pack(path)


def test_effective_plan_hash_is_canonical_and_secret_free(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    bundle = _load_bundle(root)
    snapshot = build_effective_plan_snapshot(
        bundle,
        resolved_destinations=_resolved_destinations(),
        page_size=100,
        endpoint_policy=_endpoint_policy(),
    )

    canonical = canonical_effective_plan_bytes(snapshot)
    digest = canonical_effective_plan_sha256(snapshot)
    reordered = dict(reversed(list(snapshot.items())))

    assert digest == hashlib.sha256(canonical).hexdigest()
    assert canonical_effective_plan_sha256(reordered) == digest
    assert not canonical.endswith(b"\n")
    assert b'"authorization"' not in canonical
    assert b'"cookie"' not in canonical
    assert b'"headers"' not in canonical
    assert b'"proxy_url"' not in canonical
    assert [item["query_id"] for item in snapshot["queries"]] == [
        "shevron",
        "shevrony",
        "shevron-na-lipuchke",
    ]
    assert all(
        item["dest_resolution_status"] == "resolved_not_sent"
        for item in snapshot["regions"]
    )
    assert snapshot["page_size"] == 100
    assert snapshot["endpoint_policy"] == {
        "selection_mode": "ordered_fallbacks",
        "endpoint_ids": ["primary-internal-v18", "fallback-search-v18"],
        "pinned_endpoint_id": "fallback-search-v18",
    }
    assert snapshot["regions"][0]["latitude"] == "55.6255780"
    assert snapshot["regions"][1]["address_label"] == "Ростов-на-Дону, Россия"

    valid_change = copy.deepcopy(snapshot)
    valid_change["regions"][0]["dest_id_observed"] = "-535681"
    assert canonical_effective_plan_sha256(valid_change) != digest

    invalid_quality = copy.deepcopy(snapshot)
    invalid_quality["quality"]["expected_queries_per_region"] = 2
    with pytest.raises(CollectionPlanValidationError, match="queries count"):
        canonical_effective_plan_sha256(invalid_quality)

    changed = copy.deepcopy(snapshot)
    changed["depth"] = 150
    with pytest.raises(CollectionPlanValidationError, match="depth must be one of"):
        canonical_effective_plan_sha256(changed)


@pytest.mark.parametrize(
    "depth",
    [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
)
def test_collection_plan_accepts_page_aligned_depths(
    tmp_path: Path,
    depth: int,
) -> None:
    root = _copy_stage1_config(tmp_path)
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["depth"] = depth
    plan["quality"]["expected_pages_per_query"] = depth // 100
    _write_json(plan_path, plan)

    loaded = load_collection_plan(plan_path)

    assert loaded.depth == depth
    assert loaded.quality.expected_pages_per_query == depth // 100


def test_tracked_top1000_plan_covers_exact_pack_order_and_600_pages() -> None:
    bundle = load_collection_plan_bundle(
        project_root=PROJECT_ROOT,
        plan_path=(
            PROJECT_ROOT
            / "config/wb/collection_plans/"
            "shevron-moscow-rostov-top1000-v1.json"
        ),
        region_registry_path=PROJECT_ROOT / REGIONS_RELATIVE,
    )

    assert bundle.collection_plan.enabled is False
    assert bundle.collection_plan.depth == 1000
    assert bundle.collection_plan.region_set == ("moscow", "rostov-on-don")
    assert bundle.collection_plan.query_ids == tuple(
        query.query_id for query in bundle.query_pack.queries
    )
    assert len(bundle.collection_plan.query_ids) == 30
    assert bundle.collection_plan.quality.expected_pages_per_query == 10
    assert (
        len(bundle.collection_plan.region_set)
        * len(bundle.collection_plan.query_ids)
        * bundle.collection_plan.quality.expected_pages_per_query
        == 600
    )


def test_collection_plan_rejects_depth_page_count_mismatch(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    plan_path = root / PLAN_RELATIVE
    plan = _read_json(plan_path)
    plan["depth"] = 300
    plan["quality"]["expected_pages_per_query"] = 2
    _write_json(plan_path, plan)

    with pytest.raises(CollectionPlanValidationError, match="depth/page_size"):
        load_collection_plan(plan_path)


@pytest.mark.parametrize(
    "invalid_dest",
    [
        "Bearer secret\nsecond-line",
        " -535680",
        "-535680 ",
        "1\t2",
        "12345678901234567",
        "",
    ],
)
def test_effective_plan_builder_rejects_unsafe_destination_ids(
    tmp_path: Path,
    invalid_dest: str,
) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    bundle = _load_bundle(root)
    destinations = _resolved_destinations()
    destinations["moscow"] = ResolvedDestination(
        region_id="moscow",
        dest_id_observed=invalid_dest,
        dest_resolved_at_utc="2026-07-26T07:31:00Z",
    )

    with pytest.raises(CollectionPlanValidationError, match="must match"):
        build_effective_plan_snapshot(
            bundle,
            resolved_destinations=destinations,
            page_size=100,
            endpoint_policy=_endpoint_policy(),
        )


def test_effective_plan_validator_rejects_unsafe_destination_id(
    tmp_path: Path,
) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    snapshot = build_effective_plan_snapshot(
        _load_bundle(root),
        resolved_destinations=_resolved_destinations(),
        page_size=100,
        endpoint_policy=_endpoint_policy(),
    )
    snapshot["regions"][0]["dest_id_observed"] = "Bearer secret\nsecond-line"

    with pytest.raises(CollectionPlanValidationError, match="must match"):
        canonical_effective_plan_sha256(snapshot)


def test_effective_plan_rejects_duplicate_destinations(tmp_path: Path) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    bundle = _load_bundle(root)
    destinations = _resolved_destinations()
    destinations["rostov-on-don"] = ResolvedDestination(
        region_id="rostov-on-don",
        dest_id_observed=destinations["moscow"].dest_id_observed,
        dest_resolved_at_utc="2026-07-26T07:31:01Z",
    )

    with pytest.raises(CollectionPlanValidationError, match="distinct"):
        build_effective_plan_snapshot(
            bundle,
            resolved_destinations=destinations,
            page_size=100,
            endpoint_policy=_endpoint_policy(),
        )


@pytest.mark.parametrize(
    "invalid_timestamp",
    [
        "2026-07-26 07:31:00+00:00",
        "2026-07-26T10:31:00+03:00",
        "2026-02-30T07:31:00Z",
    ],
)
def test_effective_plan_rejects_non_rfc3339_utc_timestamps(
    tmp_path: Path,
    invalid_timestamp: str,
) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    bundle = _load_bundle(root)
    destinations = _resolved_destinations()
    destinations["moscow"] = ResolvedDestination(
        region_id="moscow",
        dest_id_observed="-535680",
        dest_resolved_at_utc=invalid_timestamp,
    )

    with pytest.raises(CollectionPlanValidationError, match="RFC 3339 UTC"):
        build_effective_plan_snapshot(
            bundle,
            resolved_destinations=destinations,
            page_size=100,
            endpoint_policy=_endpoint_policy(),
        )


def test_effective_plan_validator_rejects_non_rfc3339_utc_timestamp(
    tmp_path: Path,
) -> None:
    root = _copy_stage1_config(tmp_path)
    _enable_pilot(root)
    snapshot = build_effective_plan_snapshot(
        _load_bundle(root),
        resolved_destinations=_resolved_destinations(),
        page_size=100,
        endpoint_policy=_endpoint_policy(),
    )
    snapshot["regions"][0]["dest_resolved_at_utc"] = "2026-07-26 07:31:00+00:00"

    with pytest.raises(CollectionPlanValidationError, match="RFC 3339 UTC"):
        canonical_effective_plan_sha256(snapshot)


def test_committed_disabled_plan_cannot_create_runtime_snapshot() -> None:
    bundle = _load_bundle(PROJECT_ROOT)

    with pytest.raises(CollectionPlanValidationError, match="disabled collection plan"):
        build_effective_plan_snapshot(
            bundle,
            resolved_destinations=_resolved_destinations(),
            page_size=100,
            endpoint_policy=_endpoint_policy(),
        )
