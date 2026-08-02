from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.common.durable_atomic import DurableAtomicWriteError
from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.execution_matrix import (
    EXECUTION_MATRIX_SCHEMA_VERSION,
    load_execution_matrix,
)
from app.serp.execution_matrix_runner import (
    ExecutionMatrixRunError,
    MATRIX_LATEST_SCHEMA_VERSION,
    MatrixEntryCompletion,
    run_execution_matrix,
)
from app.serp.four_region_nightly import validate_four_region_bundle


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
    assert matrix.enabled is True
    assert matrix.enabled_entries == matrix.entries
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
    assert bundle.collection_plan.enabled is True
    assert all(region.enabled for region in bundle.enabled_regions)
    assert bundle.collection_plan.publication_mode == "none"
    assert bundle.collection_plan.sellers_mode == "disabled"
    assert bundle.collection_plan.proxy_rotation_mode == "disabled"
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


def _enabled_matrix_root(
    tmp_path: Path,
    *,
    second_pack: bool = False,
) -> tuple[Path, Path]:
    root = _matrix_root(tmp_path)
    matrix_path = (
        root / "config/wb/execution_matrices/four-region-nightly-v1.json"
    )
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    matrix["enabled"] = True
    if second_pack:
        source_pack = (
            root
            / "config/wb/query_packs/shevron-core/2026-07-26.1.json"
        )
        second_pack_path = (
            root / "config/wb/query_packs/approved-second/v1.json"
        )
        second_pack_path.parent.mkdir(parents=True)
        pack = json.loads(source_pack.read_text(encoding="utf-8"))
        pack["query_pack_id"] = "approved-second"
        pack["version"] = "v1"
        second_pack_path.write_text(
            json.dumps(pack, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        source_plan = (
            root
            / "config/wb/collection_plans/"
            "shevron-four-regions-top1000-v2.json"
        )
        second_plan_path = (
            root / "config/wb/collection_plans/approved-second-v1.json"
        )
        plan = json.loads(source_plan.read_text(encoding="utf-8"))
        plan["collection_plan_id"] = "approved-second-v1"
        plan["query_pack_file"] = (
            "config/wb/query_packs/approved-second/v1.json"
        )
        second_plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        matrix["entries"].append(
            {
                "execution_id": "approved-second-four-regions",
                "enabled": True,
                "plan_file": (
                    "config/wb/collection_plans/approved-second-v1.json"
                ),
                "query_pack_id": "approved-second",
                "query_pack_version": "v1",
            }
        )
    matrix_path.write_text(
        json.dumps(matrix, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return root, matrix_path


class _MatrixHarness:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool]] = []
        self.generations: dict[
            str,
            dict[tuple[str, str], str],
        ] = {}
        self.completions: dict[
            tuple[str, str],
            MatrixEntryCompletion,
        ] = {}
        self.fail_execution_id: str | None = None
        self.failures_remaining = 0

    def execute(
        self,
        entry: Any,
        run_id: str,
        resume: bool,
        _deadline_utc: datetime,
    ) -> None:
        self.calls.append((entry.execution_id, run_id, resume))
        if (
            entry.execution_id == self.fail_execution_id
            and self.failures_remaining
        ):
            self.failures_remaining -= 1
            raise RuntimeError("sanitized fixture failure")
        self.generations[entry.execution_id] = {
            (region_id, query_id): run_id
            for region_id in entry.bundle.collection_plan.region_set
            for query_id in entry.bundle.collection_plan.query_ids
        }
        self.completions[(entry.execution_id, run_id)] = (
            MatrixEntryCompletion(
                state_path=(
                    "state/wb_four_region_nightly/"
                    f"{entry.bundle.collection_plan.collection_plan_id}/"
                    f"{run_id}/state.json"
                ),
                state_sha256=(
                    "a" if entry.query_pack_id == "shevron-core" else "b"
                )
                * 64,
            )
        )

    def generation(
        self,
        entry: Any,
        _run_date: str,
    ) -> dict[tuple[str, str], str]:
        return dict(self.generations.get(entry.execution_id, {}))

    def completion(
        self,
        entry: Any,
        run_id: str,
    ) -> MatrixEntryCompletion:
        return self.completions[(entry.execution_id, run_id)]

    def resumable(self, entry: Any, _run_id: str) -> bool:
        return entry.execution_id == self.fail_execution_id


def _run_matrix(
    *,
    root: Path,
    matrix_path: Path,
    harness: _MatrixHarness,
    resume: bool,
    run_id: str = "20260728_211500Z",
    generation_date: str | None = None,
) -> dict[str, Any]:
    return run_execution_matrix(
        config=SimpleNamespace(project_root=root),
        matrix_path=matrix_path,
        matrix_run_id=run_id,
        generation_date=generation_date,
        resume=resume,
        execute_entry=harness.execute,
        input_integrity_gate=lambda: None,
        generation_probe=harness.generation,
        completion_validator=harness.completion,
        resumable_probe=harness.resumable,
        now=lambda: datetime(2026, 7, 28, 21, 15, tzinfo=UTC),
    )


def _failed_pending_matrix_state(
    *,
    root: Path,
    matrix_path: Path,
    harness: _MatrixHarness,
) -> Path:
    harness.fail_execution_id = "shevron-core-four-regions-top1000"
    harness.failures_remaining = 1
    with pytest.raises(ExecutionMatrixRunError) as captured:
        run_execution_matrix(
            config=SimpleNamespace(project_root=root),
            matrix_path=matrix_path,
            matrix_run_id="20260728_211500Z",
            resume=False,
            execute_entry=harness.execute,
            input_integrity_gate=lambda: None,
            generation_probe=harness.generation,
            completion_validator=harness.completion,
            resumable_probe=lambda *_args: False,
            pristine_probe=lambda *_args: False,
            now=lambda: datetime(2026, 7, 28, 21, 15, tzinfo=UTC),
        )
    assert captured.value.resumable is False
    state_path = (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1/"
        "runs/20260728_211500Z/state.json"
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "failed"
    assert state["complete"] is False
    assert state["failure_reason"] == "RuntimeError"
    assert [
        (
            entry["status"],
            entry["attempts"],
            entry["state_path"],
            entry["state_sha256"],
        )
        for entry in state["entries"]
    ] == [("pending", 1, None, None)]
    return state_path


def test_matrix_runner_refuses_disabled_matrix_without_state(
    tmp_path: Path,
) -> None:
    root = _matrix_root(tmp_path)
    matrix_path = (
        root
        / "config/wb/execution_matrices/"
        "four-region-nightly-v1.json"
    )
    payload = json.loads(matrix_path.read_text(encoding="utf-8"))
    payload["enabled"] = False
    matrix_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ExecutionMatrixRunError, match="disabled"):
        run_execution_matrix(
            config=SimpleNamespace(project_root=root),
            matrix_path=matrix_path,
            matrix_run_id="20260728_211500Z",
            resume=False,
            execute_entry=lambda *_args: pytest.fail("must not execute"),
        )
    assert not (root / "state/wb_execution_matrices").exists()


def test_matrix_runner_two_packs_checkpoint_and_resume_without_repeat(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path, second_pack=True)
    harness = _MatrixHarness()
    harness.fail_execution_id = "approved-second-four-regions"
    harness.failures_remaining = 1
    with pytest.raises(ExecutionMatrixRunError) as captured:
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=False,
        )
    assert captured.value.resumable is True
    assert [call[0] for call in harness.calls] == [
        "shevron-core-four-regions-top1000",
        "approved-second-four-regions",
    ]
    assert not (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1/latest.json"
    ).exists()

    state = _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=True,
    )
    assert state["status"] == "success"
    assert state["complete"] is True
    assert [call[0] for call in harness.calls] == [
        "shevron-core-four-regions-top1000",
        "approved-second-four-regions",
        "approved-second-four-regions",
    ]
    assert harness.calls[-1][2] is True
    latest = json.loads(
        (
            root
            / "state/wb_execution_matrices/four-region-nightly-v1/"
            "latest.json"
        ).read_text(encoding="utf-8")
    )
    assert latest["schema_version"] == MATRIX_LATEST_SCHEMA_VERSION
    assert [entry["execution_id"] for entry in latest["entries"]] == [
        "shevron-core-four-regions-top1000",
        "approved-second-four-regions",
    ]


def test_matrix_pristine_failed_entry_resumes_as_fresh_same_child_run(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    harness.fail_execution_id = "shevron-core-four-regions-top1000"
    harness.failures_remaining = 1
    arguments = {
        "config": SimpleNamespace(project_root=root),
        "matrix_path": matrix_path,
        "matrix_run_id": "20260728_211500Z",
        "execute_entry": harness.execute,
        "input_integrity_gate": lambda: None,
        "generation_probe": harness.generation,
        "completion_validator": harness.completion,
        "resumable_probe": lambda *_args: False,
        "now": lambda: datetime(2026, 7, 28, 21, 15, tzinfo=UTC),
    }
    with pytest.raises(ExecutionMatrixRunError) as captured:
        run_execution_matrix(resume=False, **arguments)
    assert captured.value.resumable is True
    state = run_execution_matrix(resume=True, **arguments)
    assert state["status"] == "success"
    assert [call[2] for call in harness.calls] == [False, False]
    assert harness.calls[0][1] == harness.calls[1][1]


def test_matrix_recovers_exact_failed_pending_attempt_from_child_checkpoint(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    _failed_pending_matrix_state(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
    )

    state = _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=True,
    )

    assert state["status"] == "success"
    assert state["complete"] is True
    assert harness.calls == [
        (
            "shevron-core-four-regions-top1000",
            "20260728_211500Z",
            False,
        ),
        (
            "shevron-core-four-regions-top1000",
            "20260728_211500Z",
            True,
        ),
    ]


@pytest.mark.parametrize("attempts", (0, 2))
def test_matrix_failed_outer_rejects_non_exact_attempt_count_before_entry(
    tmp_path: Path,
    attempts: int,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    state_path = _failed_pending_matrix_state(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["entries"][0]["attempts"] = attempts
    state_path.write_text(
        json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    before = state_path.read_bytes()

    with pytest.raises(ExecutionMatrixRunError, match="recovery|checkpoint"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=True,
        )

    assert len(harness.calls) == 1
    assert state_path.read_bytes() == before


def test_matrix_failed_outer_rejects_invalid_child_checkpoint_before_entry(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    state_path = _failed_pending_matrix_state(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
    )
    harness.fail_execution_id = None
    before = state_path.read_bytes()

    with pytest.raises(ExecutionMatrixRunError, match="child checkpoint"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=True,
        )

    assert len(harness.calls) == 1
    assert state_path.read_bytes() == before


def test_matrix_failed_outer_rejects_generation_conflict_before_entry(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    state_path = _failed_pending_matrix_state(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
    )
    matrix = load_execution_matrix(
        project_root=root,
        matrix_path=matrix_path,
    )
    entry = matrix.enabled_entries[0]
    harness.generations[entry.execution_id] = {
        (
            entry.bundle.collection_plan.region_set[0],
            entry.bundle.collection_plan.query_ids[0],
        ): "20260728_211500Z",
    }
    before = state_path.read_bytes()

    with pytest.raises(ExecutionMatrixRunError, match="generation"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=True,
        )

    assert len(harness.calls) == 1
    assert state_path.read_bytes() == before


def test_second_approved_pack_uses_generic_four_region_contract(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path, second_pack=True)
    matrix = load_execution_matrix(
        project_root=root,
        matrix_path=matrix_path,
    )
    spec = validate_four_region_bundle(matrix.enabled_entries[1].bundle)
    assert spec.collection_plan_id == "approved-second-v1"
    assert spec.query_pack_id == "approved-second"
    assert spec.query_pack_version == "v1"
    assert spec.query_count == 30
    assert spec.max_pages == 1200
    assert spec.max_positions == 120000


def test_matrix_runner_blocks_existing_generation_before_execution(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    matrix = load_execution_matrix(
        project_root=root,
        matrix_path=matrix_path,
    )
    entry = matrix.enabled_entries[0]
    harness = _MatrixHarness()
    harness.generations[entry.execution_id] = {
        (region_id, query_id): "20260729_000001Z"
        for region_id in entry.bundle.collection_plan.region_set
        for query_id in entry.bundle.collection_plan.query_ids
    }
    with pytest.raises(ExecutionMatrixRunError, match="conflict"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=False,
        )
    assert harness.calls == []


def test_matrix_latest_is_atomic_and_reconciles_without_entry_repeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    from app.serp import execution_matrix_runner as runner

    real_replace = runner.durable_atomic_replace
    failed = {"value": False}

    def fail_latest(path: Path, payload: bytes, **kwargs: Any):
        if path.name == "latest.json" and not failed["value"]:
            failed["value"] = True
            raise DurableAtomicWriteError("fixture publication failure")
        return real_replace(path, payload, **kwargs)

    monkeypatch.setattr(runner, "durable_atomic_replace", fail_latest)
    with pytest.raises(ExecutionMatrixRunError) as captured:
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=False,
        )
    assert captured.value.resumable is True
    assert len(harness.calls) == 1
    assert not (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1/latest.json"
    ).exists()
    state = _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=True,
    )
    assert state["status"] == "success"
    assert len(harness.calls) == 1


def test_matrix_resume_reconciles_completed_inflight_entry_without_repeat(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    with pytest.raises(ExecutionMatrixRunError) as captured:
        from app.serp import execution_matrix_runner as runner

        original = runner._write_json
        writes = {"count": 0}

        def crash_after_entry(
            path: Path,
            payload: dict[str, Any],
            **kwargs: Any,
        ) -> str:
            if (
                path.name == "state.json"
                and payload.get("entries", [{}])[0].get("status")
                == "success"
            ):
                raise ExecutionMatrixRunError("fixture crash")
            writes["count"] += 1
            return original(path, payload, **kwargs)

        runner._write_json = crash_after_entry
        try:
            _run_matrix(
                root=root,
                matrix_path=matrix_path,
                harness=harness,
                resume=False,
            )
        finally:
            runner._write_json = original
    assert captured.value.resumable is True
    assert len(harness.calls) == 1
    state = _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=True,
    )
    assert state["status"] == "success"
    assert len(harness.calls) == 1


def test_matrix_blocks_second_successful_run_for_same_date(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=False,
    )
    second = _MatrixHarness()
    with pytest.raises(ExecutionMatrixRunError, match="date"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=second,
            resume=False,
            run_id="20260728_220000Z",
        )
    assert second.calls == []


def test_matrix_uses_authenticated_local_generation_date_across_utc_midnight(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    first = _MatrixHarness()
    _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=first,
        resume=False,
        run_id="20260802_031937Z",
        generation_date="2026-08-02",
    )

    crossing = _MatrixHarness()
    state = _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=crossing,
        resume=False,
        run_id="20260802_220018Z",
        generation_date="2026-08-03",
    )

    assert state["run_date"] == "2026-08-03"
    assert len(crossing.calls) == 1
    latest = json.loads(
        (
            root
            / "state/wb_execution_matrices/four-region-nightly-v1/latest.json"
        ).read_text(encoding="utf-8")
    )
    assert latest["run_id"] == "20260802_220018Z"
    assert latest["run_date"] == "2026-08-03"


def test_matrix_blocks_duplicate_authenticated_local_generation_date(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    first = _MatrixHarness()
    _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=first,
        resume=False,
        run_id="20260802_220018Z",
        generation_date="2026-08-03",
    )

    duplicate = _MatrixHarness()
    with pytest.raises(ExecutionMatrixRunError, match="date"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=duplicate,
            resume=False,
            run_id="20260803_010000Z",
            generation_date="2026-08-03",
        )
    assert duplicate.calls == []
    assert not (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1/runs/"
        "20260803_010000Z"
    ).exists()


def test_matrix_resume_rejects_generation_date_mismatch_before_entry(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path)
    harness = _MatrixHarness()
    _run_matrix(
        root=root,
        matrix_path=matrix_path,
        harness=harness,
        resume=False,
        run_id="20260802_220018Z",
        generation_date="2026-08-03",
    )
    calls_before = list(harness.calls)

    with pytest.raises(ExecutionMatrixRunError, match="identity"):
        _run_matrix(
            root=root,
            matrix_path=matrix_path,
            harness=harness,
            resume=True,
            run_id="20260802_220018Z",
            generation_date="2026-08-02",
        )
    assert harness.calls == calls_before


def test_matrix_plan_or_pack_mutation_fails_before_next_entry(
    tmp_path: Path,
) -> None:
    root, matrix_path = _enabled_matrix_root(tmp_path, second_pack=True)
    harness = _MatrixHarness()

    def mutate_after_first(
        entry: Any,
        run_id: str,
        resume: bool,
        deadline_utc: datetime,
    ) -> None:
        harness.execute(entry, run_id, resume, deadline_utc)
        if entry.query_pack_id == "shevron-core":
            pack_path = (
                root
                / "config/wb/query_packs/shevron-core/2026-07-26.1.json"
            )
            payload = json.loads(pack_path.read_text(encoding="utf-8"))
            payload["queries"][0]["text"] += " changed"
            pack_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    with pytest.raises(
        ExecutionMatrixRunError,
        match="source attestation changed",
    ):
        run_execution_matrix(
            config=SimpleNamespace(project_root=root),
            matrix_path=matrix_path,
            matrix_run_id="20260728_211500Z",
            resume=False,
            execute_entry=mutate_after_first,
            generation_probe=harness.generation,
            completion_validator=harness.completion,
            resumable_probe=lambda *_args: False,
            now=lambda: datetime(2026, 7, 28, 21, 15, tzinfo=UTC),
        )
    assert len(harness.calls) == 1
