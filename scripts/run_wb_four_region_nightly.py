#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.config import load_config
from app.common.exceptions import CriticalPipelineError, RunLockedError
from app.common.nightly_coordinator import (
    ADAPTER_STATUS_SCHEMA_VERSION,
    NightlyCoordinatorContractError,
    WB_RUN_REF,
    parse_utc,
    require_official_live_entry_lease,
)
from app.common.nightly_attestation import integrity_gate
from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.collection_plan_runner import (
    CollectionPlanRunError,
    run_collection_plan,
    validate_resumable_collection_state,
)
from app.serp.four_region_nightly import (
    FOUR_REGION_PLAN_ID,
    PRE_CUTOVER_DOWNSTREAM_MODE,
    run_four_region_downstream,
    write_four_region_failure_attempt,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the isolated four-region WB pipeline"
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--plan-file", required=True)
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
    run_id: str,
) -> Mapping[str, Any] | None:
    path = (
        project_root
        / "state/wb_collection_plans"
        / FOUR_REGION_PLAN_ID
        / run_id
        / "manifest.json"
    )
    manifest = _read_json(path)
    if (
        manifest is None
        or manifest.get("schema_version") != "wb_collection_plan_manifest_v2"
        or manifest.get("run_id") != run_id
        or manifest.get("collection_plan_id") != FOUR_REGION_PLAN_ID
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
    now = datetime.now(UTC)
    run_id = (
        args.downstream_only_run_id
        or args.resume_run_id
        or now.strftime("%Y%m%d_%H%M%SZ")
    )
    config = None
    downstream_started = False
    try:
        require_official_live_entry_lease(environment=os.environ)
        verify_inputs = integrity_gate(PROJECT_ROOT)
        absolute_deadline_utc = _adapter_deadline()
        if not args.downstream_only_run_id and not args.resume_run_id:
            run_id = _adapter_run_ref(now)
        config = load_config(args.config)
        plan_path = Path(args.plan_file)
        if not plan_path.is_absolute():
            plan_path = config.project_root / plan_path
        completed_manifest = (
            _completed_collection_manifest(config.project_root, run_id)
            if args.resume_run_id or args.downstream_only_run_id
            else None
        )
        if args.downstream_only_run_id or completed_manifest is not None:
            manifest = {
                "run_id": run_id,
                "status": "previously_completed",
                "complete": True,
            }
        else:
            manifest = run_collection_plan(
                config=config,
                plan_path=plan_path,
                no_publish=args.no_publish,
                run_id=None if args.resume_run_id else run_id,
                resume_run_id=args.resume_run_id,
                absolute_deadline_utc=absolute_deadline_utc,
                input_integrity_gate=verify_inputs,
            )
            if manifest.get("status") != "success" or manifest.get("complete") is not True:
                raise CriticalPipelineError(
                    "four-region collection is incomplete; downstream blocked"
                )
        downstream_started = True
        downstream = run_four_region_downstream(
            config=config,
            plan_path=plan_path,
            run_id=str(manifest["run_id"]),
            execution_mode=PRE_CUTOVER_DOWNSTREAM_MODE,
            absolute_deadline_utc=absolute_deadline_utc,
            input_integrity_gate=verify_inputs,
        )
    except (
        CriticalPipelineError,
        CollectionPlanValidationError,
        FileNotFoundError,
        CollectionPlanRunError,
    ) as exc:
        if config is not None and not downstream_started:
            write_four_region_failure_attempt(
                config=config,
                run_id=run_id,
                error=exc,
            )
        resumable = config is not None and (
            validate_resumable_collection_state(
                config=config,
                plan_path=plan_path,
                run_id=run_id,
            )
            or _completed_collection_manifest(config.project_root, run_id)
            is not None
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
            print(f"{exc.__class__.__name__}: operation checkpointed", file=sys.stderr)
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
        print(f"{exc.__class__.__name__}: operation failed", file=sys.stderr)
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
