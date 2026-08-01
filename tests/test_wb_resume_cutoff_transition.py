from __future__ import annotations

import hashlib
import json
import shutil
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.serp import collection_plan_runner as runner_module
from app.serp.execution_matrix_runner import (
    ExecutionMatrixRunError,
    MatrixEntryCompletion,
    run_execution_matrix,
)
from app.serp.collection_plan import load_collection_plan_bundle
from app.serp.execution_matrix import load_execution_matrix
from app.serp.resume_cutoff_transition import (
    COLLECTION_PLAN_ID,
    COLLECTION_RUN_ID,
    COORDINATOR_RUN_ID,
    COORDINATOR_STAGE,
    EFFECTIVE_PLAN_SHA256,
    EXECUTION_MATRIX_ID,
    FROM_CUTOFF_MSK,
    MATRIX_RUN_ID,
    STORED_TRANSPORT_FINGERPRINT,
    TO_CUTOFF_MSK,
    TO_DEADLINE_UTC,
    TRANSITION_ID,
    ApprovedResumeCutoffTransition,
    ResumeCutoffTransitionError,
    canonical_transition_bytes,
    resolve_resume_cutoff_transition,
)
from scripts import run_wb_four_region_nightly as four_region_launcher


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = PROJECT_ROOT / (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
MATRIX_PATH = PROJECT_ROOT / (
    "config/wb/execution_matrices/four-region-nightly-v1.json"
)
REGISTRY_PATH = PROJECT_ROOT / "config/wb/regions.json"


def _transition() -> ApprovedResumeCutoffTransition:
    return resolve_resume_cutoff_transition(
        run_id=COLLECTION_RUN_ID,
        resume=True,
        coordinator_run_id=COORDINATOR_RUN_ID,
        coordinator_stage=COORDINATOR_STAGE,
        transition_id=TRANSITION_ID,
        absolute_deadline_utc=TO_DEADLINE_UTC,
    )


def _bundle():
    return load_collection_plan_bundle(
        project_root=PROJECT_ROOT,
        plan_path=PLAN_PATH,
        region_registry_path=REGISTRY_PATH,
    )


def _effective_plan() -> dict[str, object]:
    bundle = _bundle()
    window = bundle.collection_plan.runtime_window
    assert window is not None
    return {
        "collection_plan_id": COLLECTION_PLAN_ID,
        "collection_plan_sha256": bundle.collection_plan_sha256,
        "query_pack_sha256": bundle.query_pack_sha256,
        "region_registry_sha256": bundle.region_registry_sha256,
        "transport_fingerprint": dict(STORED_TRANSPORT_FINGERPRINT),
        "runtime_window": {
            "mode": window.mode,
            "scheduled_start_msk": window.scheduled_start_msk,
            "new_run_start_grace_seconds": window.new_run_start_grace_seconds,
            "max_invocation_runtime_seconds": window.max_invocation_runtime_seconds,
            "absolute_cutoff_msk": window.absolute_cutoff_msk,
            "minimum_resume_window_seconds": window.minimum_resume_window_seconds,
            "finalization_reserve_seconds": window.finalization_reserve_seconds,
        },
    }


def _current_fingerprint() -> dict[str, str]:
    current = dict(STORED_TRANSPORT_FINGERPRINT)
    current["input_manifest_sha256"] = "c" * 64
    current["fingerprint_sha256"] = hashlib.sha256(
        json.dumps(
            {
                key: value
                for key, value in current.items()
                if key != "fingerprint_sha256"
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return current


def _attestation(current: dict[str, str]) -> dict[str, str]:
    return {
        "schema_version": "wb_resume_attestation_transition_v1",
        "transition_id": TRANSITION_ID,
        "from_input_manifest_sha256": STORED_TRANSPORT_FINGERPRINT[
            "input_manifest_sha256"
        ],
        "to_input_manifest_sha256": current["input_manifest_sha256"],
        "from_transport_fingerprint_sha256": STORED_TRANSPORT_FINGERPRINT[
            "fingerprint_sha256"
        ],
        "to_transport_fingerprint_sha256": current["fingerprint_sha256"],
    }


def _prior_manifest(bundle) -> dict[str, object]:
    return {
        "run_id": COLLECTION_RUN_ID,
        "status": "failed",
        "complete": False,
        "collection_plan_id": COLLECTION_PLAN_ID,
        "collection_plan_sha256": bundle.collection_plan_sha256,
        "query_pack_sha256": bundle.query_pack_sha256,
        "region_registry_sha256": bundle.region_registry_sha256,
        "effective_plan_sha256": EFFECTIVE_PLAN_SHA256,
        "transport_fingerprint": dict(STORED_TRANSPORT_FINGERPRINT),
    }


def test_exact_transition_binds_run_coordinator_stage_and_same_day_deadline() -> None:
    transition = _transition()
    assert transition is not None
    window = _bundle().collection_plan.runtime_window
    assert window is not None
    assert transition.runtime_window(window).absolute_cutoff_msk == TO_CUTOFF_MSK

    variants = (
        {"run_id": "20260801_183813Z"},
        {"resume": False},
        {"coordinator_run_id": "nightly-20260801-000000000000"},
        {"coordinator_stage": "wb_initial"},
        {"transition_id": "other"},
        {"absolute_deadline_utc": datetime(2026, 8, 2, 20, 59, tzinfo=UTC)},
    )
    base = {
        "run_id": COLLECTION_RUN_ID,
        "resume": True,
        "coordinator_run_id": COORDINATOR_RUN_ID,
        "coordinator_stage": COORDINATOR_STAGE,
        "transition_id": TRANSITION_ID,
        "absolute_deadline_utc": TO_DEADLINE_UTC,
    }
    for changed in variants:
        with pytest.raises(ResumeCutoffTransitionError):
            resolve_resume_cutoff_transition(**{**base, **changed})


def test_transition_keeps_exact_plan_matrix_and_old_runtime_provenance() -> None:
    transition = _transition()
    assert transition is not None
    bundle = _bundle()
    matrix = load_execution_matrix(
        project_root=PROJECT_ROOT,
        matrix_path=MATRIX_PATH,
    )
    transition.validate_bundle(bundle)
    transition.validate_matrix(matrix)
    assert matrix.execution_matrix_id == EXECUTION_MATRIX_ID
    assert bundle.collection_plan.runtime_window is not None
    assert bundle.collection_plan.runtime_window.absolute_cutoff_msk == FROM_CUTOFF_MSK

    changed_plan = replace(
        bundle.collection_plan,
        source_sha256="f" * 64,
    )
    with pytest.raises(ResumeCutoffTransitionError, match="source identity"):
        transition.validate_bundle(
            replace(bundle, collection_plan=changed_plan)
        )


def test_validated_evidence_binds_old_and_current_attestation_without_secrets() -> None:
    transition = _transition()
    assert transition is not None
    bundle = _bundle()
    current = _current_fingerprint()
    prior_manifest = _prior_manifest(bundle)
    evidence = transition.validated_evidence(
        bundle=bundle,
        effective_plan_sha256=EFFECTIVE_PLAN_SHA256,
        prior_manifest=prior_manifest,
        effective_plan=_effective_plan(),
        current_transport_fingerprint=current,
        attestation_transition=_attestation(current),
    )
    assert evidence["validation_status"] == "validated_before_resume_network"
    assert evidence["deadline_scope"] == "same_day_collection_and_downstream"
    assert evidence["from_cutoff_msk"] == "23:00"
    assert evidence["to_cutoff_msk"] == "23:59"
    assert evidence["from_deadline_utc"] == "2026-08-01T20:00:00Z"
    assert evidence["to_deadline_utc"] == "2026-08-01T20:59:00Z"
    encoded = canonical_transition_bytes(evidence)
    assert b"cookie" not in encoded.lower()
    assert b"proxy_url" not in encoded.lower()


@pytest.mark.parametrize(
    "mutation",
    ("effective", "endpoint", "request", "proxy", "runtime", "attestation"),
)
def test_transition_rejects_provenance_drift_before_resume(
    mutation: str,
) -> None:
    transition = _transition()
    assert transition is not None
    bundle = _bundle()
    current = _current_fingerprint()
    prior = _prior_manifest(bundle)
    effective = _effective_plan()
    attestation = _attestation(current)
    effective_sha = EFFECTIVE_PLAN_SHA256
    if mutation == "effective":
        effective_sha = "e" * 64
    elif mutation in {"endpoint", "request", "proxy", "runtime"}:
        field = {
            "endpoint": "ordered_endpoint_urls_sha256",
            "request": "request_params_sha256",
            "proxy": "proxy_route_sha256",
            "runtime": "runtime_input_sha256",
        }[mutation]
        current[field] = "e" * 64
    else:
        attestation["from_input_manifest_sha256"] = "e" * 64

    with pytest.raises(ResumeCutoffTransitionError):
        transition.validated_evidence(
            bundle=bundle,
            effective_plan_sha256=effective_sha,
            prior_manifest=prior,
            effective_plan=effective,
            current_transport_fingerprint=current,
            attestation_transition=attestation,
        )


def test_authorization_evidence_is_deterministic_and_run_scoped() -> None:
    first = _transition().authorization_evidence()
    second = _transition().authorization_evidence()
    assert first == second
    assert canonical_transition_bytes(first) == canonical_transition_bytes(second)
    assert first["coordinator_run_id"] == COORDINATOR_RUN_ID
    assert first["matrix_run_id"] == MATRIX_RUN_ID
    assert first["collection_run_id"] == COLLECTION_RUN_ID
    with pytest.raises(ResumeCutoffTransitionError, match="ID mismatch"):
        ApprovedResumeCutoffTransition("wrong-transition")


def test_matrix_transition_evidence_is_written_after_checkpoint_validation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    source_config = PROJECT_ROOT / "config/wb"
    target_config = root / "config/wb"
    shutil.copytree(source_config, target_config)
    matrix_path = target_config / "execution_matrices/four-region-nightly-v1.json"
    events: list[str] = []
    generations: dict[tuple[str, str], str] = {}
    completion: dict[str, MatrixEntryCompletion] = {}
    failures = {"remaining": 1}

    def execute(entry, run_id, resume, _deadline):
        events.append(f"execute:{resume}")
        if failures["remaining"]:
            failures["remaining"] -= 1
            raise RuntimeError("fixture failure")
        generations.update(
            {
                (region_id, query_id): run_id
                for region_id in entry.bundle.collection_plan.region_set
                for query_id in entry.bundle.collection_plan.query_ids
            }
        )
        completion[run_id] = MatrixEntryCompletion(
            state_path=(
                "state/wb_four_region_nightly/"
                f"{COLLECTION_PLAN_ID}/{run_id}/state.json"
            ),
            state_sha256="a" * 64,
        )

    def resumable(_entry, _run_id):
        events.append("validate_checkpoint")
        return True

    common = {
        "config": SimpleNamespace(project_root=root),
        "matrix_path": matrix_path,
        "matrix_run_id": MATRIX_RUN_ID,
        "execute_entry": execute,
        "input_integrity_gate": lambda: None,
        "generation_probe": lambda _entry, _date: dict(generations),
        "completion_validator": lambda _entry, run_id: completion[run_id],
        "resumable_probe": resumable,
        "now": lambda: datetime(2026, 8, 1, 19, 0, tzinfo=UTC),
    }
    with pytest.raises(ExecutionMatrixRunError) as captured:
        run_execution_matrix(resume=False, **common)
    assert captured.value.resumable is True

    events.clear()
    state = run_execution_matrix(
        resume=True,
        absolute_deadline_utc=TO_DEADLINE_UTC,
        resume_cutoff_transition=_transition(),
        **common,
    )

    assert state["status"] == "success"
    assert events[:2] == ["validate_checkpoint", "execute:True"]
    evidence_path = (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1"
        / "runs"
        / MATRIX_RUN_ID
        / "resume_cutoff_transition.json"
    )
    assert evidence_path.read_bytes() == canonical_transition_bytes(
        _transition().authorization_evidence()
    )


def test_invalid_child_checkpoint_creates_no_transition_evidence_or_execution(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    shutil.copytree(PROJECT_ROOT / "config", root / "config")
    matrix_path = root / (
        "config/wb/execution_matrices/four-region-nightly-v1.json"
    )
    calls: list[str] = []

    def fail_once(_entry, _run_id, _resume, _deadline):
        calls.append("initial")
        raise RuntimeError("fixture failure")

    common = {
        "config": SimpleNamespace(project_root=root),
        "matrix_path": matrix_path,
        "matrix_run_id": MATRIX_RUN_ID,
        "input_integrity_gate": lambda: None,
        "generation_probe": lambda _entry, _date: {},
        "completion_validator": lambda _entry, _run_id: pytest.fail(
            "completion must not run"
        ),
        "now": lambda: datetime(2026, 8, 1, 19, 0, tzinfo=UTC),
    }
    with pytest.raises(ExecutionMatrixRunError):
        run_execution_matrix(
            resume=False,
            execute_entry=fail_once,
            resumable_probe=lambda _entry, _run_id: True,
            **common,
        )
    calls.clear()

    with pytest.raises(ExecutionMatrixRunError, match="child checkpoint"):
        run_execution_matrix(
            resume=True,
            execute_entry=lambda *_args: calls.append("resume"),
            resumable_probe=lambda _entry, _run_id: False,
            absolute_deadline_utc=TO_DEADLINE_UTC,
            resume_cutoff_transition=_transition(),
            **common,
        )
    evidence_path = (
        root
        / "state/wb_execution_matrices/four-region-nightly-v1"
        / "runs"
        / MATRIX_RUN_ID
        / "resume_cutoff_transition.json"
    )
    assert calls == []
    assert not evidence_path.exists()


def test_completed_collection_skips_serp_and_passes_same_deadline_to_downstream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition = _transition()
    assert transition is not None
    config = SimpleNamespace(project_root=tmp_path)
    plan_path = tmp_path / "plan.json"
    monkeypatch.setattr(
        four_region_launcher,
        "load_collection_plan_bundle",
        lambda **_kwargs: SimpleNamespace(
            collection_plan=SimpleNamespace(
                collection_plan_id=COLLECTION_PLAN_ID,
            )
        ),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "_completed_collection_manifest",
        lambda *_args: {"status": "success", "complete": True},
    )
    monkeypatch.setattr(
        four_region_launcher,
        "run_collection_plan",
        lambda **_kwargs: pytest.fail("completed collection must not rerun SERP"),
    )
    validation_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        four_region_launcher,
        "validate_resumable_collection_state",
        lambda **kwargs: validation_calls.append(kwargs) or True,
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        four_region_launcher,
        "run_four_region_downstream",
        lambda **kwargs: captured.update(kwargs)
        or {"status": "success", "complete": True},
    )

    manifest, downstream = four_region_launcher.execute_four_region_plan(
        config=config,
        plan_path=plan_path,
        run_id=COLLECTION_RUN_ID,
        resume=True,
        downstream_only=False,
        absolute_deadline_utc=TO_DEADLINE_UTC,
        input_integrity_gate=lambda: None,
        resume_cutoff_transition=transition,
    )

    assert manifest["status"] == "previously_completed"
    assert downstream["complete"] is True
    assert captured["run_id"] == COLLECTION_RUN_ID
    assert captured["absolute_deadline_utc"] == TO_DEADLINE_UTC
    assert captured["resume_cutoff_transition"] is transition
    assert validation_calls[0]["absolute_deadline_utc"] == TO_DEADLINE_UTC
    assert validation_calls[0]["resume_cutoff_transition"] is transition


def test_exact_input_attestation_transition_accepts_only_built_target_manifest() -> None:
    manifest_path = (
        PROJECT_ROOT / "config/wb/nightly_coordinator_adapter_inputs.json"
    )
    current = dict(STORED_TRANSPORT_FINGERPRINT)
    current["input_manifest_sha256"] = runner_module._sha256_bytes(
        manifest_path.read_bytes()
    )
    current["fingerprint_sha256"] = runner_module._canonical_sha256(
        {
            key: value
            for key, value in current.items()
            if key != "fingerprint_sha256"
        }
    )

    transition = runner_module._resume_attestation_transition(
        stored=dict(STORED_TRANSPORT_FINGERPRINT),
        current=current,
        project_root=PROJECT_ROOT,
        run_id=COLLECTION_RUN_ID,
        collection_plan_id=COLLECTION_PLAN_ID,
        effective_plan_sha256=EFFECTIVE_PLAN_SHA256,
    )

    assert transition is not None
    assert transition["transition_id"] == TRANSITION_ID
    assert transition["to_input_manifest_sha256"] == current[
        "input_manifest_sha256"
    ]

    changed = dict(current)
    changed["input_manifest_sha256"] = "e" * 64
    changed["fingerprint_sha256"] = runner_module._canonical_sha256(
        {
            key: value
            for key, value in changed.items()
            if key != "fingerprint_sha256"
        }
    )
    with pytest.raises(
        runner_module.CollectionPlanRunError,
        match="target input manifest digest",
    ):
        runner_module._resume_attestation_transition(
            stored=dict(STORED_TRANSPORT_FINGERPRINT),
            current=changed,
            project_root=PROJECT_ROOT,
            run_id=COLLECTION_RUN_ID,
            collection_plan_id=COLLECTION_PLAN_ID,
            effective_plan_sha256=EFFECTIVE_PLAN_SHA256,
        )


def test_launcher_refuses_exact_resume_without_transition_before_config_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        four_region_launcher,
        "require_official_live_entry_lease",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "integrity_gate",
        lambda _root: (lambda: None),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_config",
        lambda _path: pytest.fail("config load must not run"),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "_emit_adapter_status",
        lambda **_kwargs: None,
    )
    monkeypatch.setenv("MARKETPLACE_COORDINATOR_RUN_ID", COORDINATOR_RUN_ID)
    monkeypatch.setenv("MARKETPLACE_COORDINATOR_STAGE", COORDINATOR_STAGE)
    monkeypatch.setenv(
        "MARKETPLACE_COORDINATOR_DEADLINE_UTC",
        "2026-08-01T20:59:00Z",
    )
    monkeypatch.delenv(
        "MARKETPLACE_COORDINATOR_CUTOFF_TRANSITION_ID",
        raising=False,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--matrix-file",
            "config/wb/execution_matrices/four-region-nightly-v1.json",
            "--no-publish",
            "--resume-run-id",
            COLLECTION_RUN_ID,
        ],
    )

    assert four_region_launcher.main() == 2


def test_launcher_passes_exact_transition_to_matrix_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        four_region_launcher,
        "require_official_live_entry_lease",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        four_region_launcher,
        "integrity_gate",
        lambda _root: (lambda: None),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )
    monkeypatch.setattr(
        four_region_launcher,
        "run_execution_matrix",
        lambda **kwargs: captured.update(kwargs)
        or {"status": "success", "complete": True},
    )
    monkeypatch.setattr(
        four_region_launcher,
        "_emit_adapter_status",
        lambda **_kwargs: None,
    )
    monkeypatch.setenv("MARKETPLACE_COORDINATOR_RUN_ID", COORDINATOR_RUN_ID)
    monkeypatch.setenv("MARKETPLACE_COORDINATOR_STAGE", COORDINATOR_STAGE)
    monkeypatch.setenv(
        "MARKETPLACE_COORDINATOR_DEADLINE_UTC",
        "2026-08-01T20:59:00Z",
    )
    monkeypatch.setenv(
        "MARKETPLACE_COORDINATOR_CUTOFF_TRANSITION_ID",
        TRANSITION_ID,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--matrix-file",
            "config/wb/execution_matrices/four-region-nightly-v1.json",
            "--no-publish",
            "--resume-run-id",
            COLLECTION_RUN_ID,
        ],
    )

    assert four_region_launcher.main() == 0
    transition = captured["resume_cutoff_transition"]
    assert isinstance(transition, ApprovedResumeCutoffTransition)
    assert captured["absolute_deadline_utc"] == TO_DEADLINE_UTC
    assert captured["resume"] is True
