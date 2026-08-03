#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.config import load_config
from app.common.exceptions import CriticalPipelineError, RunLockedError
from app.common.nightly_coordinator import (
    ADAPTER_STATUS_SCHEMA_VERSION,
    NightlyCoordinatorContractError,
    WB_RUN_REF,
    coordinator_invocation_from_environment,
    parse_utc,
    require_official_live_entry_lease,
)
from app.common.nightly_attestation import integrity_gate
from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.collection_plan import load_collection_plan_bundle
from app.serp.collection_plan_runner import (
    CollectionPlanRunError,
    run_collection_plan,
    validate_resumable_collection_state,
)
from app.serp.execution_matrix_runner import (
    ExecutionMatrixRunError,
    run_execution_matrix,
)
from app.serp.four_region_nightly import (
    FOUR_REGION_PLAN_ID,
    POST_CUTOVER_DOWNSTREAM_MODE,
    PRE_CUTOVER_DOWNSTREAM_MODE,
    DownstreamExecutionContract,
    run_four_region_downstream,
    write_four_region_failure_attempt,
)
from app.serp.resume_cutoff_transition import (
    ApprovedResumeCutoffTransition,
    resolve_resume_cutoff_transition,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the isolated four-region WB pipeline"
    )
    parser.add_argument("--config", default="config/config.yaml")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan-file")
    source.add_argument("--matrix-file")
    parser.add_argument("--no-publish", action="store_true", required=True)
    parser.add_argument("--resume-run-id")
    parser.add_argument("--downstream-only-run-id")
    return parser


def _adapter_deadline() -> datetime | None:
    value = os.getenv("MARKETPLACE_COORDINATOR_DEADLINE_UTC", "")
    if not value:
        return None
    return parse_utc(value, "deadline_utc")


def _adapter_run_ref(now: datetime) -> str:
    value = os.getenv("PARSER_WB_ADAPTER_RUN_REF", "")
    if value:
        if not WB_RUN_REF.fullmatch(value):
            raise NightlyCoordinatorContractError(
                "adapter_run_ref_invalid",
                outcome="hard_failure",
            )
        return value
    return now.strftime("%Y%m%d_%H%M%SZ")


def _coordinator_invocation_under_validated_lease() -> Any:
    validation_fd = require_official_live_entry_lease(environment=os.environ)
    invocation = coordinator_invocation_from_environment(os.environ)
    if invocation is not None and (
        type(validation_fd) is not int or validation_fd < 3
    ):
        raise NightlyCoordinatorContractError(
            "coordinator_schedule_date_requires_lock_v3",
            outcome="hard_failure",
        )
    return invocation


def _emit_adapter_status(
    *,
    outcome: str,
    run_ref: str,
    resume_ref: str,
    reason_code: str,
    report_refs: tuple[str, ...] = (),
) -> None:
    value = os.getenv("PARSER_WB_ADAPTER_STATUS_FD", "")
    if not value:
        return
    try:
        fd = int(value)
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            "adapter_status_fd_invalid",
            outcome="hard_failure",
        ) from exc
    payload = {
        "schema_version": ADAPTER_STATUS_SCHEMA_VERSION,
        "outcome": outcome,
        "run_ref": run_ref,
        "resume_ref": resume_ref,
        "reason_code": reason_code,
        "report_refs": list(report_refs),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    view = memoryview(encoded)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise NightlyCoordinatorContractError(
                "adapter_status_write_failed",
                outcome="hard_failure",
            )
        view = view[written:]
    os.close(fd)


def _read_json(path: Path) -> Mapping[str, Any] | None:
    try:
        if not path.is_file() or path.is_symlink():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _completed_collection_manifest(
    project_root: Path,
    collection_plan_id: str,
    run_id: str,
) -> Mapping[str, Any] | None:
    path = (
        project_root
        / "state/wb_collection_plans"
        / collection_plan_id
        / run_id
        / "manifest.json"
    )
    manifest = _read_json(path)
    if (
        manifest is None
        or manifest.get("schema_version") != "wb_collection_plan_manifest_v2"
        or manifest.get("run_id") != run_id
        or manifest.get("collection_plan_id") != collection_plan_id
        or manifest.get("status") != "success"
        or manifest.get("complete") is not True
    ):
        return None
    regions = manifest.get("regions")
    if (
        not isinstance(regions, list)
        or len(regions) != 4
        or any(
            not isinstance(item, dict)
            or item.get("status") != "success"
            or item.get("complete") is not True
            for item in regions
        )
    ):
        return None
    return manifest


def execute_four_region_plan(
    *,
    config: Any,
    plan_path: Path,
    run_id: str,
    resume: bool,
    downstream_only: bool,
    absolute_deadline_utc: datetime | None,
    input_integrity_gate: Any,
    generation_date: str | None = None,
    downstream_execution_mode: str = PRE_CUTOVER_DOWNSTREAM_MODE,
    on_downstream_start: Callable[[], None] = lambda: None,
    matrix_continuation: bool = False,
    resume_cutoff_transition: ApprovedResumeCutoffTransition | None = None,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    bundle = load_collection_plan_bundle(
        project_root=config.project_root,
        plan_path=plan_path,
        region_registry_path=config.project_root / "config/wb/regions.json",
    )
    collection_plan_id = bundle.collection_plan.collection_plan_id
    completed_manifest = (
        _completed_collection_manifest(
            config.project_root,
            collection_plan_id,
            run_id,
        )
        if resume or downstream_only
        else None
    )
    if completed_manifest is not None and resume_cutoff_transition is not None:
        if not validate_resumable_collection_state(
            config=config,
            plan_path=plan_path,
            run_id=run_id,
            resume_cutoff_transition=resume_cutoff_transition,
            absolute_deadline_utc=absolute_deadline_utc,
        ):
            raise CriticalPipelineError(
                "completed collection cutoff transition validation failed"
            )
    if downstream_only or completed_manifest is not None:
        manifest: Mapping[str, Any] = {
            "run_id": run_id,
            "status": "previously_completed",
            "complete": True,
        }
    else:
        manifest = run_collection_plan(
            config=config,
            plan_path=plan_path,
            no_publish=True,
            run_id=None if resume else run_id,
            resume_run_id=run_id if resume else None,
            absolute_deadline_utc=absolute_deadline_utc,
            input_integrity_gate=input_integrity_gate,
            matrix_continuation=matrix_continuation,
            resume_cutoff_transition=resume_cutoff_transition,
        )
        if (
            manifest.get("status") != "success"
            or manifest.get("complete") is not True
        ):
            raise CriticalPipelineError(
                "four-region collection is incomplete; downstream blocked"
            )
    on_downstream_start()
    downstream = run_four_region_downstream(
        config=config,
        plan_path=plan_path,
        run_id=str(manifest["run_id"]),
        generation_date=generation_date,
        execution_mode=downstream_execution_mode,
        absolute_deadline_utc=absolute_deadline_utc,
        input_integrity_gate=input_integrity_gate,
        resume_cutoff_transition=resume_cutoff_transition,
    )
    return manifest, downstream


def _is_deferred_error(exc: BaseException) -> bool:
    if isinstance(exc, NightlyCoordinatorContractError):
        return exc.outcome == "deferred"
    if isinstance(exc, RunLockedError):
        return True
    code = str(getattr(exc, "code", ""))
    message = str(exc).lower()
    return (
        code
        in {
            "shared_marketplace_guard_busy",
            "shared_marketplace_validation_busy",
        }
        or "deadline" in message
        or "outside its reviewed start window" in message
        or "insufficient time" in message
        or "legacy nightly" in message
        or "another run is active" in message
    )


def _safe_root_cause(exc: BaseException) -> str:
    current = exc
    for _ in range(8):
        cause = current.__cause__
        if cause is None:
            break
        current = cause
    allowed = (
        CollectionPlanValidationError,
        CollectionPlanRunError,
        NightlyCoordinatorContractError,
    )
    if not isinstance(current, allowed):
        return current.__class__.__name__
    message = " ".join(str(current).split())
    message = "".join(
        character
        for character in message
        if character.isprintable()
    )[:240]
    return (
        f"{current.__class__.__name__}: {message}"
        if message
        else current.__class__.__name__
    )


def _is_coordinator_resume_phase() -> bool:
    return os.getenv("MARKETPLACE_COORDINATOR_STAGE", "") == "wb_resume"


def main() -> int:
    args = build_parser().parse_args()
    if args.resume_run_id and args.downstream_only_run_id:
        print(
            "--resume-run-id and --downstream-only-run-id are mutually exclusive",
            file=sys.stderr,
        )
        return 2
    if args.matrix_file and args.downstream_only_run_id:
        print(
            "--downstream-only-run-id is not supported with --matrix-file",
            file=sys.stderr,
        )
        return 2
    now = datetime.now(UTC)
    run_id = (
        args.downstream_only_run_id
        or args.resume_run_id
        or now.strftime("%Y%m%d_%H%M%SZ")
    )
    config = None
    stage_marker = {"downstream_started": False}
    downstream_execution_mode = PRE_CUTOVER_DOWNSTREAM_MODE
    collection_plan_id = FOUR_REGION_PLAN_ID
    plan_path: Path | None = None
    try:
        coordinator_invocation = _coordinator_invocation_under_validated_lease()
        if coordinator_invocation is not None:
            downstream_execution_mode = POST_CUTOVER_DOWNSTREAM_MODE
        verify_inputs = integrity_gate(PROJECT_ROOT)
        absolute_deadline_utc = _adapter_deadline()
        if not args.downstream_only_run_id and not args.resume_run_id:
            run_id = _adapter_run_ref(now)
        resume_cutoff_transition = resolve_resume_cutoff_transition(
            run_id=run_id,
            resume=bool(args.resume_run_id),
            coordinator_run_id=os.getenv(
                "MARKETPLACE_COORDINATOR_RUN_ID",
                "",
            ),
            coordinator_stage=os.getenv(
                "MARKETPLACE_COORDINATOR_STAGE",
                "",
            ),
            transition_id=os.getenv(
                "MARKETPLACE_COORDINATOR_CUTOFF_TRANSITION_ID",
                "",
            ),
            absolute_deadline_utc=absolute_deadline_utc,
        )
        config = load_config(args.config)
        if args.matrix_file:
            matrix_path = Path(args.matrix_file)
            if not matrix_path.is_absolute():
                matrix_path = config.project_root / matrix_path

            def execute_entry(
                entry: Any,
                plan_run_id: str,
                resume: bool,
                matrix_deadline_utc: datetime,
            ) -> None:
                execute_four_region_plan(
                    config=config,
                    plan_path=config.project_root / entry.plan_file,
                    run_id=plan_run_id,
                    resume=resume,
                    downstream_only=False,
                    absolute_deadline_utc=matrix_deadline_utc,
                    input_integrity_gate=verify_inputs,
                    generation_date=(
                        coordinator_invocation.schedule_date
                        if coordinator_invocation is not None
                        else None
                    ),
                    downstream_execution_mode=downstream_execution_mode,
                    matrix_continuation=not resume,
                    resume_cutoff_transition=resume_cutoff_transition,
                )

            matrix_state = run_execution_matrix(
                config=config,
                matrix_path=matrix_path,
                matrix_run_id=run_id,
                generation_date=(
                    coordinator_invocation.schedule_date
                    if coordinator_invocation is not None
                    else None
                ),
                resume=bool(args.resume_run_id),
                execute_entry=execute_entry,
                absolute_deadline_utc=absolute_deadline_utc,
                input_integrity_gate=verify_inputs,
                resume_cutoff_transition=resume_cutoff_transition,
            )
            manifest = {
                "run_id": run_id,
                "status": matrix_state["status"],
                "complete": matrix_state["complete"],
            }
            downstream = {
                "status": matrix_state["status"],
                "complete": matrix_state["complete"],
            }
        else:
            plan_path = Path(str(args.plan_file))
            if not plan_path.is_absolute():
                plan_path = config.project_root / plan_path
            manifest, downstream = execute_four_region_plan(
                config=config,
                plan_path=plan_path,
                run_id=run_id,
                resume=bool(args.resume_run_id),
                downstream_only=bool(args.downstream_only_run_id),
                absolute_deadline_utc=absolute_deadline_utc,
                input_integrity_gate=verify_inputs,
                generation_date=(
                    coordinator_invocation.schedule_date
                    if coordinator_invocation is not None
                    else None
                ),
                downstream_execution_mode=downstream_execution_mode,
                on_downstream_start=lambda: stage_marker.__setitem__(
                    "downstream_started",
                    True,
                ),
                resume_cutoff_transition=resume_cutoff_transition,
            )
    except (
        CriticalPipelineError,
        CollectionPlanValidationError,
        FileNotFoundError,
        CollectionPlanRunError,
        ExecutionMatrixRunError,
    ) as exc:
        if (
            config is not None
            and plan_path is not None
            and not stage_marker["downstream_started"]
        ):
            write_four_region_failure_attempt(
                config=config,
                run_id=run_id,
                error=exc,
                execution_contract=(
                    DownstreamExecutionContract.for_mode(
                        downstream_execution_mode
                    )
                ),
            )
        resumable = bool(
            isinstance(exc, ExecutionMatrixRunError) and exc.resumable
        )
        if config is not None and plan_path is not None:
            resumable = resumable or (
                validate_resumable_collection_state(
                    config=config,
                    plan_path=plan_path,
                    run_id=run_id,
                    resume_cutoff_transition=resume_cutoff_transition,
                    absolute_deadline_utc=absolute_deadline_utc,
                )
                or (
                    bool(collection_plan_id)
                    and _completed_collection_manifest(
                        config.project_root,
                        collection_plan_id,
                        run_id,
                    )
                    is not None
                )
            )
        if isinstance(exc, RunLockedError) or str(getattr(exc, "code", "")) in {
            "shared_marketplace_guard_busy",
            "shared_marketplace_validation_busy",
        }:
            _emit_adapter_status(
                outcome="deferred",
                run_ref=run_id,
                resume_ref="",
                reason_code="local_preflight_deferred",
            )
            print(f"{exc.__class__.__name__}: operation deferred", file=sys.stderr)
            return 75
        if resumable and not _is_coordinator_resume_phase():
            try:
                verify_inputs()
            except CriticalPipelineError:
                resumable = False
        if resumable and not _is_coordinator_resume_phase():
            _emit_adapter_status(
                outcome="checkpoint",
                run_ref=run_id,
                resume_ref=run_id,
                reason_code="checkpoint_saved",
            )
            print(
                f"{exc.__class__.__name__}: operation checkpointed; "
                f"root_cause={_safe_root_cause(exc)}",
                file=sys.stderr,
            )
            return 76
        if _is_deferred_error(exc):
            _emit_adapter_status(
                outcome="deferred",
                run_ref=run_id,
                resume_ref="",
                reason_code="local_preflight_deferred",
            )
            print(f"{exc.__class__.__name__}: operation deferred", file=sys.stderr)
            return 75
        _emit_adapter_status(
            outcome="hard_failure",
            run_ref=run_id,
            resume_ref="",
            reason_code="pipeline_hard_failure",
        )
        print(
            f"{exc.__class__.__name__}: operation failed; "
            f"root_cause={_safe_root_cause(exc)}",
            file=sys.stderr,
        )
        return 2
    try:
        verify_inputs()
    except CriticalPipelineError:
        _emit_adapter_status(
            outcome="hard_failure",
            run_ref=run_id,
            resume_ref="",
            reason_code="attested_input_changed",
        )
        print("WB attested input changed", file=sys.stderr)
        return 2
    _emit_adapter_status(
        outcome="success",
        run_ref=str(manifest["run_id"]),
        resume_ref="",
        reason_code="completed",
    )
    print(
        json.dumps(
            {
                "run_id": manifest["run_id"],
                "collection_status": manifest["status"],
                "downstream_status": downstream["status"],
                "complete": downstream["complete"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
