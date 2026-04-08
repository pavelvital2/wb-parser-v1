from __future__ import annotations

from typing import Any

from .config import AppConfig
from .constants import (
    COMPONENT_FILTER,
    COMPONENT_SELLERS,
    COMPONENT_SERP,
    COMPONENT_SUGGEST,
    ERROR_CODE_COMPONENT_CRITICAL,
    ERROR_CODE_COMPONENT_NON_CRITICAL,
    ERROR_CODE_COMPONENT_UNHANDLED,
    ERROR_SEVERITY_CRITICAL,
    ERROR_SEVERITY_NON_CRITICAL,
    PIPELINE_DAILY,
    PIPELINE_MONTHLY,
    RUN_STATUS_FAILED,
    RUN_STATUS_NOT_READY,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SUCCESS,
    TASK_STATUS_FAILED,
    TASK_STATUS_NOT_READY,
    TASK_STATUS_PARTIAL,
    TASK_STATUS_SUCCESS,
)
from .error_codes import infer_error_code
from .exceptions import ComponentNotReadyError, CriticalPipelineError, NonCriticalPipelineError
from .logging_setup import get_logger
from .run_context import RunContext, build_run_id, utc_now_iso
from .run_lock import acquire_run_lock
from .run_report import write_run_report
from .state_db import StateDB

from app.filter.runner import run_filter
from app.sellers.runner import run_sellers
from app.serp.runner import run_serp
from app.suggest.runner import run_suggest


_SINGLE_COMPONENTS = {COMPONENT_SUGGEST, COMPONENT_FILTER, COMPONENT_SERP, COMPONENT_SELLERS}


def _resolve_components(target: str) -> list[str]:
    if target == PIPELINE_DAILY:
        return [COMPONENT_FILTER, COMPONENT_SERP, COMPONENT_SELLERS]
    if target == PIPELINE_MONTHLY:
        return [COMPONENT_SUGGEST, COMPONENT_FILTER]
    if target in _SINGLE_COMPONENTS:
        return [target]
    raise CriticalPipelineError(f"Unknown component target: {target}")


def _dispatch_component(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    if ctx.component == COMPONENT_SUGGEST:
        return run_suggest(config=config, db=db, ctx=ctx)
    if ctx.component == COMPONENT_FILTER:
        return run_filter(config=config, db=db, ctx=ctx)
    if ctx.component == COMPONENT_SERP:
        return run_serp(config=config, db=db, ctx=ctx)
    if ctx.component == COMPONENT_SELLERS:
        return run_sellers(config=config, db=db, ctx=ctx)
    raise CriticalPipelineError(f"Unknown component: {ctx.component}")


def _extract_result_refs(result: dict[str, int | str]) -> dict[str, str]:
    refs: dict[str, str] = {}
    for key, value in result.items():
        if key.endswith("_path") or key.endswith("_export"):
            refs[key] = str(value)
    return refs


def _build_report_payload(
    *,
    db: StateDB,
    root_ctx: RunContext,
    status: str,
    started_at_utc: str,
    finished_at_utc: str,
    total_ok: int,
    total_err: int,
    critical_count: int,
    non_critical_count: int,
    component_reports: list[dict[str, Any]],
    note: str,
    lock_path: str,
) -> dict[str, Any]:
    return {
        "run_id": root_ctx.run_id,
        "pipeline": root_ctx.pipeline,
        "job_id": root_ctx.job_id,
        "status": status,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "totals": {
            "items_ok": total_ok,
            "items_error": total_err,
            "critical_errors": critical_count,
            "non_critical_errors": non_critical_count,
        },
        "components": component_reports,
        "tasks": db.list_tasks(root_ctx.run_id),
        "errors": db.list_errors(root_ctx.run_id),
        "note": note,
        "lock_path": lock_path,
        "generated_at_utc": utc_now_iso(),
    }


def run_component(config: AppConfig, db: StateDB, target: str, job_id: str = "") -> int:
    logger = get_logger("runner")
    started_at = utc_now_iso()
    run_id = build_run_id()

    with acquire_run_lock(
        state_dir=config.paths.STATE_DIR,
        target=target,
        run_id=run_id,
        enabled=config.runtime.locking_enabled,
        stale_seconds=config.runtime.lock_stale_seconds,
    ) as lock_path:
        root_ctx = RunContext(
            run_id=run_id,
            pipeline=target,
            component="pipeline",
            started_at_utc=started_at,
            job_id=job_id,
        )

        db.create_run(run_id=root_ctx.run_id, pipeline=root_ctx.pipeline, job_id=root_ctx.job_id, created_at_utc=root_ctx.started_at_utc)
        logger.info("run_started", extra={"run_id": root_ctx.run_id, "pipeline": root_ctx.pipeline})

        total_ok = 0
        total_err = 0
        non_critical_count = 0
        critical_count = 0
        component_reports: list[dict[str, Any]] = []

        for component in _resolve_components(target):
            task_started = utc_now_iso()
            ctx = root_ctx.for_component(component)
            db.create_task(run_id=ctx.run_id, component=ctx.component, started_at_utc=task_started)
            logger.info("task_started", extra={"run_id": ctx.run_id, "pipeline": ctx.pipeline, "component": ctx.component})

            try:
                result = _dispatch_component(config=config, db=db, ctx=ctx)
                task_finished = utc_now_iso()
                items_ok = int(result.get("items_ok", 0))
                items_err = int(result.get("items_error", 0))
                total_ok += items_ok
                total_err += items_err
                db.finish_task(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    status=TASK_STATUS_SUCCESS,
                    finished_at_utc=task_finished,
                    items_ok=items_ok,
                    items_error=items_err,
                    note=str(result.get("note", "")),
                )
                component_reports.append(
                    {
                        "component": ctx.component,
                        "status": TASK_STATUS_SUCCESS,
                        "started_at_utc": task_started,
                        "finished_at_utc": task_finished,
                        "items_ok": items_ok,
                        "items_error": items_err,
                        "note": str(result.get("note", "")),
                        "error_code": "",
                        "result_refs": _extract_result_refs(result),
                    }
                )
                logger.info(
                    "task_finished",
                    extra={"run_id": ctx.run_id, "pipeline": ctx.pipeline, "component": ctx.component, "status": TASK_STATUS_SUCCESS},
                )

            except ComponentNotReadyError as exc:
                task_finished = utc_now_iso()
                err_code = infer_error_code(exc)
                db.finish_task(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    status=TASK_STATUS_NOT_READY,
                    finished_at_utc=task_finished,
                    note=str(exc),
                )
                component_reports.append(
                    {
                        "component": ctx.component,
                        "status": TASK_STATUS_NOT_READY,
                        "started_at_utc": task_started,
                        "finished_at_utc": task_finished,
                        "items_ok": 0,
                        "items_error": 0,
                        "note": str(exc),
                        "error_code": err_code,
                        "result_refs": {},
                    }
                )
                final_note = f"{ctx.component}: {exc}"
                db.finish_run(
                    run_id=ctx.run_id,
                    status=RUN_STATUS_NOT_READY,
                    finished_at_utc=task_finished,
                    items_ok=total_ok,
                    items_error=total_err,
                    critical_errors=critical_count,
                    non_critical_errors=non_critical_count,
                    note=final_note,
                )
                report_path = write_run_report(
                    state_dir=config.paths.STATE_DIR,
                    run_id=root_ctx.run_id,
                    payload=_build_report_payload(
                        db=db,
                        root_ctx=root_ctx,
                        status=RUN_STATUS_NOT_READY,
                        started_at_utc=started_at,
                        finished_at_utc=task_finished,
                        total_ok=total_ok,
                        total_err=total_err,
                        critical_count=critical_count,
                        non_critical_count=non_critical_count,
                        component_reports=component_reports,
                        note=final_note,
                        lock_path=str(lock_path or ""),
                    ),
                )
                logger.warning("run_report_written", extra={"run_id": root_ctx.run_id, "source_ref": str(report_path)})
                raise

            except NonCriticalPipelineError as exc:
                task_finished = utc_now_iso()
                err_code = infer_error_code(exc, default=ERROR_CODE_COMPONENT_NON_CRITICAL)
                non_critical_count += 1
                total_err += 1
                db.record_error(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    severity=ERROR_SEVERITY_NON_CRITICAL,
                    error_code=err_code,
                    error_class=exc.__class__.__name__,
                    error_message=str(exc),
                    source_ref="",
                    created_at_utc=task_finished,
                )
                db.finish_task(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    status=TASK_STATUS_PARTIAL,
                    finished_at_utc=task_finished,
                    items_error=1,
                    note=str(exc),
                )
                component_reports.append(
                    {
                        "component": ctx.component,
                        "status": TASK_STATUS_PARTIAL,
                        "started_at_utc": task_started,
                        "finished_at_utc": task_finished,
                        "items_ok": 0,
                        "items_error": 1,
                        "note": str(exc),
                        "error_code": err_code,
                        "result_refs": {},
                    }
                )
                logger.warning(
                    "task_non_critical",
                    extra={
                        "run_id": ctx.run_id,
                        "pipeline": ctx.pipeline,
                        "component": ctx.component,
                        "status": TASK_STATUS_PARTIAL,
                        "error_code": err_code,
                    },
                )
                continue

            except CriticalPipelineError as exc:
                task_finished = utc_now_iso()
                err_code = infer_error_code(exc, default=ERROR_CODE_COMPONENT_CRITICAL)
                critical_count += 1
                total_err += 1
                db.record_error(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    severity=ERROR_SEVERITY_CRITICAL,
                    error_code=err_code,
                    error_class=exc.__class__.__name__,
                    error_message=str(exc),
                    source_ref="",
                    created_at_utc=task_finished,
                )
                db.finish_task(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    status=TASK_STATUS_FAILED,
                    finished_at_utc=task_finished,
                    items_error=1,
                    note=str(exc),
                )
                component_reports.append(
                    {
                        "component": ctx.component,
                        "status": TASK_STATUS_FAILED,
                        "started_at_utc": task_started,
                        "finished_at_utc": task_finished,
                        "items_ok": 0,
                        "items_error": 1,
                        "note": str(exc),
                        "error_code": err_code,
                        "result_refs": {},
                    }
                )
                final_note = f"{ctx.component}: {exc}"
                db.finish_run(
                    run_id=ctx.run_id,
                    status=RUN_STATUS_FAILED,
                    finished_at_utc=task_finished,
                    items_ok=total_ok,
                    items_error=total_err,
                    critical_errors=critical_count,
                    non_critical_errors=non_critical_count,
                    note=final_note,
                )
                report_path = write_run_report(
                    state_dir=config.paths.STATE_DIR,
                    run_id=root_ctx.run_id,
                    payload=_build_report_payload(
                        db=db,
                        root_ctx=root_ctx,
                        status=RUN_STATUS_FAILED,
                        started_at_utc=started_at,
                        finished_at_utc=task_finished,
                        total_ok=total_ok,
                        total_err=total_err,
                        critical_count=critical_count,
                        non_critical_count=non_critical_count,
                        component_reports=component_reports,
                        note=final_note,
                        lock_path=str(lock_path or ""),
                    ),
                )
                logger.error(
                    "run_failed",
                    extra={
                        "run_id": ctx.run_id,
                        "pipeline": ctx.pipeline,
                        "component": ctx.component,
                        "status": RUN_STATUS_FAILED,
                        "error_code": err_code,
                        "source_ref": str(report_path),
                    },
                )
                raise

            except Exception as exc:
                task_finished = utc_now_iso()
                err_code = infer_error_code(exc, default=ERROR_CODE_COMPONENT_UNHANDLED)
                critical_count += 1
                total_err += 1
                db.record_error(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    severity=ERROR_SEVERITY_CRITICAL,
                    error_code=err_code,
                    error_class=exc.__class__.__name__,
                    error_message=str(exc),
                    source_ref="",
                    created_at_utc=task_finished,
                )
                db.finish_task(
                    run_id=ctx.run_id,
                    component=ctx.component,
                    status=TASK_STATUS_FAILED,
                    finished_at_utc=task_finished,
                    items_error=1,
                    note=f"unhandled: {exc}",
                )
                component_reports.append(
                    {
                        "component": ctx.component,
                        "status": TASK_STATUS_FAILED,
                        "started_at_utc": task_started,
                        "finished_at_utc": task_finished,
                        "items_ok": 0,
                        "items_error": 1,
                        "note": f"unhandled: {exc}",
                        "error_code": err_code,
                        "result_refs": {},
                    }
                )
                final_note = f"{ctx.component}: unhandled {exc}"
                db.finish_run(
                    run_id=ctx.run_id,
                    status=RUN_STATUS_FAILED,
                    finished_at_utc=task_finished,
                    items_ok=total_ok,
                    items_error=total_err,
                    critical_errors=critical_count,
                    non_critical_errors=non_critical_count,
                    note=final_note,
                )
                report_path = write_run_report(
                    state_dir=config.paths.STATE_DIR,
                    run_id=root_ctx.run_id,
                    payload=_build_report_payload(
                        db=db,
                        root_ctx=root_ctx,
                        status=RUN_STATUS_FAILED,
                        started_at_utc=started_at,
                        finished_at_utc=task_finished,
                        total_ok=total_ok,
                        total_err=total_err,
                        critical_count=critical_count,
                        non_critical_count=non_critical_count,
                        component_reports=component_reports,
                        note=final_note,
                        lock_path=str(lock_path or ""),
                    ),
                )
                logger.error(
                    "run_failed_unhandled",
                    extra={
                        "run_id": ctx.run_id,
                        "pipeline": ctx.pipeline,
                        "component": ctx.component,
                        "status": RUN_STATUS_FAILED,
                        "error_code": err_code,
                        "source_ref": str(report_path),
                    },
                )
                raise CriticalPipelineError(f"Unhandled error in {ctx.component}: {exc}")

        final_status = RUN_STATUS_PARTIAL if non_critical_count > 0 else RUN_STATUS_SUCCESS
        finished_at = utc_now_iso()
        db.finish_run(
            run_id=root_ctx.run_id,
            status=final_status,
            finished_at_utc=finished_at,
            items_ok=total_ok,
            items_error=total_err,
            critical_errors=critical_count,
            non_critical_errors=non_critical_count,
            note="",
        )
        report_path = write_run_report(
            state_dir=config.paths.STATE_DIR,
            run_id=root_ctx.run_id,
            payload=_build_report_payload(
                db=db,
                root_ctx=root_ctx,
                status=final_status,
                started_at_utc=started_at,
                finished_at_utc=finished_at,
                total_ok=total_ok,
                total_err=total_err,
                critical_count=critical_count,
                non_critical_count=non_critical_count,
                component_reports=component_reports,
                note="",
                lock_path=str(lock_path or ""),
            ),
        )
        logger.info(
            "run_finished",
            extra={
                "run_id": root_ctx.run_id,
                "pipeline": root_ctx.pipeline,
                "status": final_status,
                "source_ref": str(report_path),
            },
        )
        return 0
