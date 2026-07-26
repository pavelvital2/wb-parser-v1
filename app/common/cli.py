from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .cleanup import cleanup_runtime_files
from .config import AppConfig, load_config
from .constants import (
    COMPONENT_FILTER,
    COMPONENT_SELLERS,
    COMPONENT_SERP,
    COMPONENT_SUGGEST,
    PIPELINE_DAILY,
    PIPELINE_MONTHLY,
)
from .contracts import validate_csv_contract
from .exceptions import ComponentNotReadyError, CriticalPipelineError
from .logging_setup import configure_logging, get_logger
from .runner import run_component
from .state_db import StateDB


_RUN_TARGETS = [COMPONENT_SUGGEST, COMPONENT_FILTER, COMPONENT_SERP, COMPONENT_SELLERS, PIPELINE_DAILY, PIPELINE_MONTHLY]


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--job-id", default="")
    parser.add_argument("--dry-run", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wildberries unified parser CLI")
    parser.add_argument("--config", default="config/config.yaml", help="Path to runtime YAML config")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="Validate config/paths/state DB")
    sub.add_parser("validate", help="Alias for doctor")

    runs_p = sub.add_parser("runs", help="Show run history")
    runs_p.add_argument("--limit", type=int, default=20)

    cleanup_p = sub.add_parser("cleanup", help="Retention cleanup for runtime files")
    cleanup_p.add_argument("--apply", action="store_true", help="Actually delete files (default is dry-run)")

    collection_plan_p = sub.add_parser(
        "collection-plan",
        help="Run one isolated WB collection plan without publication",
    )
    collection_plan_p.add_argument("--plan-file", required=True)
    collection_plan_p.add_argument(
        "--no-publish",
        action="store_true",
        required=True,
    )
    collection_plan_p.add_argument(
        "--guarded-pilot",
        action="store_true",
        help="Run the explicit Stage 3 A-B-A pilot contract",
    )

    run_p = sub.add_parser("run", help="Run one component or pipeline")
    run_p.add_argument("target", choices=_RUN_TARGETS)
    _add_run_options(run_p)

    for target in _RUN_TARGETS:
        direct = sub.add_parser(target, help=f"Alias for 'run {target}'")
        _add_run_options(direct)

    return parser


def _resolve_path(config: AppConfig, value: str) -> Path:
    p = Path(value)
    if p.is_absolute():
        return p
    return (config.project_root / p).resolve()


def _doctor_check_latest_csv(
    config: AppConfig,
    *,
    layer: str,
    component: str,
    filename: str,
    required_columns: list[str],
    errors: list[str],
    warnings: list[str],
) -> None:
    latest = config.paths.latest_output_path(layer=layer, component=component, filename=filename)
    if latest is None:
        warnings.append(f"latest output not found: {layer}/{component}/{filename}")
        return
    try:
        validate_csv_contract(latest, required_columns=required_columns, min_rows=1)
    except CriticalPipelineError as exc:
        errors.append(f"invalid latest output {latest}: {exc}")


def _doctor_checks(config: AppConfig, db: StateDB) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    dirs = {
        "DATA_DIR": config.paths.DATA_DIR,
        "RAW_DIR": config.paths.RAW_DIR,
        "STAGING_DIR": config.paths.STAGING_DIR,
        "MARTS_DIR": config.paths.MARTS_DIR,
        "LOG_DIR": config.paths.LOG_DIR,
        "EXPORTS_DIR": config.paths.EXPORTS_DIR,
        "STATE_DIR": config.paths.STATE_DIR,
        "SQLITE_DIR": config.paths.SQLITE_DIR,
        "CHECKPOINT_DIR": config.paths.CHECKPOINT_DIR,
    }

    for name, p in dirs.items():
        if not p.exists():
            errors.append(f"missing directory {name}: {p}")
        elif not p.is_dir():
            errors.append(f"path is not directory {name}: {p}")

    db.init_schema()
    try:
        with sqlite3.connect(config.paths.SQLITE_DB) as conn:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        for table in {"runs", "tasks", "errors", "checkpoints"}:
            if table not in tables:
                errors.append(f"state_db missing table: {table}")
    except Exception as exc:
        errors.append(f"state_db is not readable: {exc}")

    suggest_cfg = config.raw.get("suggest", {})
    filter_cfg = config.raw.get("filter", {})
    serp_cfg = config.raw.get("serp", {})
    sellers_cfg = config.raw.get("sellers", {})

    prefixes = _resolve_path(config, str(suggest_cfg.get("prefixes_file", "config/prefixes.txt")))
    if not prefixes.exists():
        errors.append(f"suggest prefixes file missing: {prefixes}")

    rules_file = _resolve_path(config, str(filter_cfg.get("rules_file", "config/query_rules.yaml")))
    if not rules_file.exists():
        errors.append(f"filter rules file missing: {rules_file}")

    suggest_input = str(filter_cfg.get("input_files", {}).get("suggest_staging_csv", "")).strip()
    if suggest_input and not _resolve_path(config, suggest_input).exists():
        warnings.append(f"filter explicit suggest input not found (fallback will be used): {suggest_input}")

    serp_queries = str(serp_cfg.get("input_files", {}).get("queries_txt", "")).strip()
    if serp_queries and not _resolve_path(config, serp_queries).exists():
        warnings.append(f"serp queries file not found (fallback will be used): {serp_queries}")

    sellers_input = str(sellers_cfg.get("input_files", {}).get("products_daily_csv", "")).strip()
    if sellers_input and not _resolve_path(config, sellers_input).exists():
        warnings.append(f"sellers products input not found (fallback will be used): {sellers_input}")

    _doctor_check_latest_csv(
        config,
        layer="staging",
        component=COMPONENT_SUGGEST,
        filename="suggest_alpha_staging.csv",
        required_columns=["run_id", "typed_query", "suggestion", "status"],
        errors=errors,
        warnings=warnings,
    )
    _doctor_check_latest_csv(
        config,
        layer="marts",
        component=COMPONENT_FILTER,
        filename="top_queries.csv",
        required_columns=["run_id", "query", "rank"],
        errors=errors,
        warnings=warnings,
    )
    _doctor_check_latest_csv(
        config,
        layer="marts",
        component=COMPONENT_SERP,
        filename="products_daily.csv",
        required_columns=["run_id", "query", "nmId", "supplier_id", "status"],
        errors=errors,
        warnings=warnings,
    )
    _doctor_check_latest_csv(
        config,
        layer="marts",
        component=COMPONENT_SELLERS,
        filename="sellers_daily.csv",
        required_columns=["run_id", "supplier_id", "status", "http_status"],
        errors=errors,
        warnings=warnings,
    )

    lock_file = config.paths.STATE_DIR / "locks" / "pipeline.lock"
    if lock_file.exists():
        warnings.append(f"run lock is active: {lock_file}")

    latest_report = config.paths.STATE_DIR / "run_reports" / "latest.json"
    if latest_report.exists():
        try:
            json.loads(latest_report.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"latest run report is invalid JSON: {exc}")
    else:
        warnings.append("run report not found yet: state/run_reports/latest.json")

    return errors, warnings


def cmd_doctor(config_path: str) -> int:
    config = load_config(config_path)
    configure_logging(config)
    logger = get_logger("doctor")
    db = StateDB(config.paths.SQLITE_DB)

    errors, warnings = _doctor_checks(config=config, db=db)

    if errors:
        print("DOCTOR FAILED")
        for e in errors:
            print(f"ERROR: {e}")
        for w in warnings:
            print(f"WARN: {w}")
        logger.error("doctor_failed", extra={"config": str(config.config_file), "errors": len(errors), "warnings": len(warnings)})
        return 1

    print("DOCTOR OK")
    print(f"config: {config.config_file}")
    print(f"state_db: {config.paths.SQLITE_DB}")
    for w in warnings:
        print(f"WARN: {w}")

    logger.info("doctor_ok", extra={"config": str(config.config_file), "db": str(config.paths.SQLITE_DB), "warnings": len(warnings)})
    return 0


def cmd_runs(config_path: str, limit: int) -> int:
    config = load_config(config_path)
    configure_logging(config)
    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    rows = db.list_runs(limit=limit)
    if not rows:
        print("No runs yet")
        return 0

    for row in rows:
        print(
            f"{row['created_at_utc']} | {row['run_id']} | {row['pipeline']} | "
            f"{row['status']} | ok={row['items_ok']} err={row['items_error']}"
        )
    return 0


def cmd_cleanup(config_path: str, apply: bool) -> int:
    config = load_config(config_path)
    configure_logging(config)
    logger = get_logger("cleanup")

    result = cleanup_runtime_files(config, apply=apply)

    print("CLEANUP RESULT")
    print(f"enabled: {result['enabled']}")
    print(f"apply: {result['apply']}")
    print(f"files_scanned: {result['files_scanned']}")
    print(f"files_matched: {result['files_matched']}")
    print(f"files_deleted: {result['files_deleted']}")
    print(f"dirs_deleted: {result['dirs_deleted']}")
    if result.get("matched_paths"):
        print("sample:")
        for path in result["matched_paths"][:20]:
            print(f"- {path}")

    logger.info(
        "cleanup_done",
        extra={
            "component": "cleanup",
            "status": "done",
            "files_matched": result["files_matched"],
            "files_deleted": result["files_deleted"],
        },
    )
    return 0


def cmd_run(args: argparse.Namespace, target_override: str | None = None) -> int:
    config = load_config(args.config)
    if args.dry_run:
        config.runtime.dry_run = True

    configure_logging(config)
    logger = get_logger("cli")
    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()

    target = target_override or args.target

    try:
        return run_component(config=config, db=db, target=target, job_id=args.job_id)
    except ComponentNotReadyError as exc:
        logger.warning("component_not_ready", extra={"target": target, "error": str(exc)})
        print(str(exc))
        return 2
    except CriticalPipelineError as exc:
        logger.error("critical_error", extra={"target": target, "error": str(exc)})
        print(str(exc))
        return 1


def cmd_collection_plan(args: argparse.Namespace) -> int:
    from app.serp.collection_plan import CollectionPlanValidationError
    from app.serp.collection_plan_runner import run_collection_plan
    from app.serp.regional_pilot import run_guarded_regional_pilot

    config = load_config(args.config)
    plan_path = Path(args.plan_file)
    if not plan_path.is_absolute():
        plan_path = config.project_root / plan_path
    try:
        if args.guarded_pilot:
            manifest = run_guarded_regional_pilot(
                config=config,
                plan_path=plan_path,
                no_publish=args.no_publish,
                guarded_pilot=True,
            )
        else:
            manifest = run_collection_plan(
                config=config,
                plan_path=plan_path,
                no_publish=args.no_publish,
            )
    except (CriticalPipelineError, CollectionPlanValidationError) as exc:
        print(str(exc))
        return 1
    print(
        json.dumps(
            {
                "run_id": manifest["run_id"],
                "status": manifest["status"],
                "complete": manifest["complete"],
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command in {"doctor", "validate"}:
        return cmd_doctor(args.config)
    if args.command == "runs":
        return cmd_runs(args.config, args.limit)
    if args.command == "cleanup":
        return cmd_cleanup(args.config, args.apply)
    if args.command == "collection-plan":
        return cmd_collection_plan(args)
    if args.command == "run":
        return cmd_run(args)
    if args.command in _RUN_TARGETS:
        return cmd_run(args, target_override=args.command)

    return 0
