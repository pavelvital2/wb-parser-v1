from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.execution_matrix import (
    EXECUTION_MATRIX_SCHEMA_VERSION,
    load_execution_matrix,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = (
    PROJECT_ROOT
    / "config/wb/execution_matrices/four-region-nightly-v1.json"
)


def test_production_matrix_pins_only_reviewed_shevron_pack() -> None:
    matrix = load_execution_matrix(
        project_root=PROJECT_ROOT,
        matrix_path=MATRIX_PATH,
    )
    assert matrix.execution_matrix_id == "four-region-nightly-v1"
    assert matrix.enabled is False
    assert matrix.enabled_entries == ()
    assert [
        (
            entry.execution_id,
            entry.enabled,
            entry.query_pack_id,
            entry.query_pack_version,
        )
        for entry in matrix.entries
    ] == [
        (
            "shevron-core-four-regions-top1000",
            True,
            "shevron-core",
            "2026-07-26.1",
        )
    ]
    bundle = matrix.entries[0].bundle
    assert len(bundle.collection_plan.query_ids) == 30
    assert bundle.collection_plan.depth == 1000
    assert bundle.collection_plan.region_set == (
        "moscow",
        "rostov-on-don",
        "novosibirsk",
        "kazan",
    )


def _matrix_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    shutil.copytree(PROJECT_ROOT / "config", root / "config")
    return root


def test_matrix_supports_multiple_enabled_pack_entries_without_code_change(
    tmp_path: Path,
) -> None:
    root = _matrix_root(tmp_path)
    source_pack = root / "config/wb/query_packs/shevron-core/2026-07-26.1.json"
    second_pack = root / "config/wb/query_packs/approved-second/v1.json"
    second_pack.parent.mkdir(parents=True)
    pack = json.loads(source_pack.read_text(encoding="utf-8"))
    pack["query_pack_id"] = "approved-second"
    pack["version"] = "v1"
    second_pack.write_text(
        json.dumps(pack, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    source_plan = (
        root
        / "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
    )
    second_plan = root / "config/wb/collection_plans/approved-second-v1.json"
    plan = json.loads(source_plan.read_text(encoding="utf-8"))
    plan["collection_plan_id"] = "approved-second-v1"
    plan["query_pack_file"] = (
        "config/wb/query_packs/approved-second/v1.json"
    )
    second_plan.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    matrix_path = (
        root / "config/wb/execution_matrices/four-region-nightly-v1.json"
    )
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    matrix["enabled"] = True
    matrix["entries"].append(
        {
            "execution_id": "approved-second-four-regions",
            "enabled": True,
            "plan_file": "config/wb/collection_plans/approved-second-v1.json",
            "query_pack_id": "approved-second",
            "query_pack_version": "v1",
        }
    )
    matrix_path.write_text(
        json.dumps(matrix, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    loaded = load_execution_matrix(
        project_root=root,
        matrix_path=matrix_path,
    )
    assert [entry.query_pack_id for entry in loaded.enabled_entries] == [
        "shevron-core",
        "approved-second",
    ]


@pytest.mark.parametrize(
    "mutation",
    ("duplicate_pack", "unknown_pack", "path_escape", "enabled_without_entry"),
)
def test_matrix_fails_closed_on_invalid_contract(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = _matrix_root(tmp_path)
    matrix_path = (
        root / "config/wb/execution_matrices/four-region-nightly-v1.json"
    )
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    if mutation == "duplicate_pack":
        duplicate = dict(matrix["entries"][0])
        duplicate["execution_id"] = "duplicate"
        duplicate["plan_file"] = (
            "config/wb/collection_plans/shevron-moscow-rostov-top1000-v1.json"
        )
        matrix["entries"].append(duplicate)
    elif mutation == "unknown_pack":
        matrix["entries"][0]["query_pack_id"] = "unknown"
    elif mutation == "path_escape":
        matrix["entries"][0]["plan_file"] = "../outside.json"
    else:
        matrix["enabled"] = True
        matrix["entries"][0]["enabled"] = False
    matrix_path.write_text(
        json.dumps(matrix, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(CollectionPlanValidationError):
        load_execution_matrix(project_root=root, matrix_path=matrix_path)


def test_matrix_rejects_symlink_source(tmp_path: Path) -> None:
    root = _matrix_root(tmp_path)
    real = root / "config/wb/execution_matrices/four-region-nightly-v1.json"
    outside = tmp_path / "outside.json"
    outside.write_bytes(real.read_bytes())
    real.unlink()
    real.symlink_to(outside)
    with pytest.raises(CollectionPlanValidationError):
        load_execution_matrix(project_root=root, matrix_path=real)


def test_matrix_schema_version_is_exact() -> None:
    payload = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    assert payload["schema_version"] == EXECUTION_MATRIX_SCHEMA_VERSION
