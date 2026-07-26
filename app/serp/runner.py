from __future__ import annotations

from pathlib import Path

from app.common.config import AppConfig
from app.common.constants import COMPONENT_SERP
from app.common.contracts import validate_csv_contract
from app.common.csv_io import read_csv_rows
from app.common.exceptions import CriticalPipelineError, NonCriticalPipelineError
from app.common.run_context import RunContext
from app.common.state_db import StateDB

from .engine import SerpEngine


def _max_error_ratio(config: AppConfig) -> float | None:
    value = config.raw.get("validation", {}).get("max_error_ratio", {}).get(COMPONENT_SERP)
    if value is None:
        return None
    return float(value)


def _errors_within_threshold(result: dict[str, int | str], max_error_ratio: float | None) -> bool:
    items_error = int(result.get("items_error", 0))
    if items_error <= 0:
        return True
    pages_done = int(result.get("pages_done", 0))
    total_pages = pages_done + items_error
    if max_error_ratio is None or total_pages <= 0:
        return False
    return (items_error / total_pages) <= max_error_ratio


def _validate_absolute_positions(path: Path, page_size: int) -> int:
    checked = 0
    for row in read_csv_rows(path):
        if (row.get("status") or "").strip().lower() != "success":
            continue
        try:
            page = int((row.get("page") or "0") or "0")
            pos = int((row.get("position_on_page") or "0") or "0")
            abs_pos = int((row.get("absolute_position") or "0") or "0")
        except ValueError as exc:
            raise CriticalPipelineError(f"Invalid numeric position values in staging: {exc}")
        if page <= 0 or pos <= 0:
            raise CriticalPipelineError("Invalid page/position_on_page for success row")
        expected = ((page - 1) * page_size) + pos
        if abs_pos != expected:
            raise CriticalPipelineError(
                f"absolute_position mismatch: expected={expected} got={abs_pos} for page={page} pos={pos}"
            )
        checked += 1
    return checked


def run_serp(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    result = SerpEngine(config=config, db=db, ctx=ctx).run()

    out_cfg = config.raw.get("serp", {}).get("output_files", {})
    raw_name = str(out_cfg.get("raw_products_csv", "products_raw.csv"))
    staging_name = str(out_cfg.get("staging_products_csv", "products_staging.csv"))
    mart_name = str(out_cfg.get("mart_products_daily_csv", "products_daily.csv"))
    pages_name = str(out_cfg.get("raw_pages_index_csv", "pages_raw_index.csv"))
    sellers_input_name = str(out_cfg.get("sellers_input_csv", "products_for_sellers.csv"))

    raw_path = config.paths.output_path(layer="raw", component=COMPONENT_SERP, run_id=ctx.run_id, filename=raw_name)
    staging_path = config.paths.output_path(layer="staging", component=COMPONENT_SERP, run_id=ctx.run_id, filename=staging_name)
    mart_path = config.paths.output_path(layer="marts", component=COMPONENT_SERP, run_id=ctx.run_id, filename=mart_name)
    pages_path = config.paths.output_path(layer="raw", component=COMPONENT_SERP, run_id=ctx.run_id, filename=pages_name)

    sellers_input_export = config.paths.EXPORTS_DIR / sellers_input_name
    preview_export = config.paths.EXPORTS_DIR / "products_daily_preview.csv"

    required = [
        "run_id",
        "component",
        "collected_at_utc",
        "source_system",
        "source_type",
        "source_ref",
        "status",
        "error_message",
        "query",
        "page",
        "position_on_page",
        "absolute_position",
        "nmId",
        "supplier_id",
        "raw_file",
    ]

    status_set = {"success", "empty", "error", "dry_run"}

    validate_csv_contract(
        raw_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses=status_set,
    )
    validate_csv_contract(
        staging_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses=status_set,
    )
    validate_csv_contract(
        mart_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses=status_set,
    )
    validate_csv_contract(
        pages_path,
        required_columns=[
            "run_id",
            "component",
            "collected_at_utc",
            "source_system",
            "source_type",
            "source_ref",
            "status",
            "error_message",
            "query",
            "page",
            "http_status",
            "products_count",
            "raw_file",
        ],
        min_rows=1,
        allowed_statuses=status_set,
    )

    outputs_published = int(result.get("outputs_published", 0)) == 1
    if outputs_published:
        validate_csv_contract(
            sellers_input_export,
            required_columns=required,
            min_rows=1,
            allowed_statuses=status_set,
        )

        validate_csv_contract(
            preview_export,
            required_columns=[
                "query",
                "page",
                "position_on_page",
                "absolute_position",
                "nmId",
                "product_name",
                "brand",
                "supplier_id",
                "supplier_name",
                "final_price",
                "price",
                "sale_price",
                "rating",
                "feedbacks",
                "raw_file",
                "run_id",
                "collected_at_utc",
            ],
            min_rows=1,
        )

    if not config.runtime.dry_run:
        checked = _validate_absolute_positions(staging_path, int(config.raw.get("serp", {}).get("page_size", 100)))
        if checked <= 0:
            raise CriticalPipelineError("No success rows found for absolute_position validation")

    if int(result.get("items_error", 0)) > 0 and not _errors_within_threshold(result, _max_error_ratio(config)):
        raise NonCriticalPipelineError(
            f"serp completed with partial errors above threshold: "
            f"items_error={result.get('items_error', 0)} pages_done={result.get('pages_done', 0)} "
            f"max_error_ratio={_max_error_ratio(config)}"
        )

    return result
