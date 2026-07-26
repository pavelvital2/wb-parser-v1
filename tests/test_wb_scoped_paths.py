from __future__ import annotations

from pathlib import Path

import pytest

from app.serp.collection_plan_runner import (
    CollectionPlanRunError,
    ScopedPaths,
    ScopedTask,
    _write_new_bytes,
)


RUN_ID = "20260726_120000Z"
PLAN_ID = "shevron-moscow-rostov-top100-pilot-v1"


def _task(region_id: str = "moscow") -> ScopedTask:
    return ScopedTask(
        collection_plan_id=PLAN_ID,
        query_pack_id="shevron-core",
        query_pack_version="2026-07-26.1",
        query_id="shevron",
        category_id="shevrons",
        query="шеврон",
        query_group="shevrons",
        region_id=region_id,
        region_name="Москва",
        page=1,
        page_size=100,
        depth=100,
    )


def test_scoped_paths_match_the_approved_layout(tmp_path: Path) -> None:
    paths = ScopedPaths.build(
        project_root=tmp_path,
        collection_plan_id=PLAN_ID,
        run_id=RUN_ID,
    )
    task = _task()

    assert paths.layer_region_run_dir("raw", "moscow") == (
        tmp_path
        / "data/raw/serp_scoped"
        / PLAN_ID
        / "moscow"
        / RUN_ID
    )
    assert paths.layer_region_run_dir("staging", "moscow") == (
        tmp_path
        / "data/staging/serp_scoped"
        / PLAN_ID
        / "moscow"
        / RUN_ID
    )
    assert paths.layer_region_run_dir("marts", "moscow") == (
        tmp_path
        / "data/marts/serp_scoped"
        / PLAN_ID
        / "moscow"
        / RUN_ID
    )
    assert paths.state_run_dir == (
        tmp_path / "state/wb_collection_plans" / PLAN_ID / RUN_ID
    )
    assert paths.effective_plan_path == paths.state_run_dir / "effective_plan.json"
    assert paths.manifest_path == paths.state_run_dir / "manifest.json"
    assert paths.latest_path == (
        tmp_path / "state/wb_collection_plans" / PLAN_ID / "latest.json"
    )
    assert paths.latest_region_manifest_path("moscow") == (
        tmp_path
        / "state/wb_collection_plans"
        / PLAN_ID
        / "latest_generations"
        / RUN_ID
        / "moscow.json"
    )
    assert task.checkpoint_key == (
        "shevron-moscow-rostov-top100-pilot-v1|"
        "2026-07-26.1|moscow|shevron|1"
    )
    assert paths.checkpoint_path(task) == (
        paths.state_run_dir / "checkpoints/moscow/shevron/page_001.json"
    )
    assert paths.raw_page_path(task) == (
        paths.layer_region_run_dir("raw", "moscow")
        / "pages/shevron/page_001.json"
    )


@pytest.mark.parametrize(
    ("plan_id", "run_id"),
    [
        ("../escape", RUN_ID),
        ("valid", "../escape"),
        ("Valid", RUN_ID),
        ("valid", "2026-07-26"),
    ],
)
def test_scoped_paths_reject_unsafe_identity(
    tmp_path: Path,
    plan_id: str,
    run_id: str,
) -> None:
    with pytest.raises(CollectionPlanRunError):
        ScopedPaths.build(
            project_root=tmp_path,
            collection_plan_id=plan_id,
            run_id=run_id,
        )


def test_scoped_paths_never_overlap_global_latest_or_warehouse(
    tmp_path: Path,
) -> None:
    paths = ScopedPaths.build(
        project_root=tmp_path,
        collection_plan_id=PLAN_ID,
        run_id=RUN_ID,
    )
    candidates = {
        paths.layer_region_run_dir(layer, "moscow")
        for layer in ("raw", "staging", "marts")
    }
    candidates.add(paths.state_run_dir)

    forbidden = {
        tmp_path / "data/raw/serp/latest",
        tmp_path / "data/staging/serp/latest",
        tmp_path / "data/marts/serp/latest",
        tmp_path / "data/warehouse",
        tmp_path / "state/run_reports",
        tmp_path / "exports",
    }
    for candidate in candidates:
        assert all(
            candidate != path and path not in candidate.parents for path in forbidden
        )


def test_immutable_writer_rejects_overwrite_and_symlink_parent(
    tmp_path: Path,
) -> None:
    target = tmp_path / "state/wb_collection_plans/plan/run/manifest.json"
    _write_new_bytes(target, b"first", project_root=tmp_path)
    with pytest.raises(CollectionPlanRunError, match="already exists"):
        _write_new_bytes(target, b"second", project_root=tmp_path)
    assert target.read_bytes() == b"first"

    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "state/linked"
    linked.parent.mkdir(exist_ok=True)
    linked.symlink_to(outside, target_is_directory=True)
    with pytest.raises(CollectionPlanRunError, match="symlink"):
        _write_new_bytes(
            linked / "artifact.json",
            b"blocked",
            project_root=tmp_path,
        )
    assert not (outside / "artifact.json").exists()
