from __future__ import annotations

from app.common.config import AppConfig
from app.common.constants import COMPONENT_SELLERS
from app.common.contracts import validate_csv_contract
from app.common.exceptions import NonCriticalPipelineError
from app.common.run_context import RunContext
from app.common.state_db import StateDB

from .engine import SellersEngine


def _max_error_ratio(config: AppConfig) -> float | None:
    value = config.raw.get("validation", {}).get("max_error_ratio", {}).get(COMPONENT_SELLERS)
    if value is None:
        return None
    return float(value)


def run_sellers(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    result = SellersEngine(config=config, db=db, ctx=ctx).run()

    out_cfg = config.raw.get("sellers", {}).get("output_files", {})
    raw_name = str(out_cfg.get("raw_sellers_csv", "sellers_raw.csv"))
    staging_name = str(out_cfg.get("staging_sellers_csv", "sellers_staging.csv"))
    mart_name = str(out_cfg.get("mart_sellers_daily_csv", "sellers_daily.csv"))
    bridge_name = str(out_cfg.get("bridge_csv", "seller_query_product_bridge.csv"))

    raw_path = config.paths.output_path(layer="raw", component=COMPONENT_SELLERS, run_id=ctx.run_id, filename=raw_name)
    staging_path = config.paths.output_path(layer="staging", component=COMPONENT_SELLERS, run_id=ctx.run_id, filename=staging_name)
    mart_path = config.paths.output_path(layer="marts", component=COMPONENT_SELLERS, run_id=ctx.run_id, filename=mart_name)
    bridge_path = config.paths.output_path(layer="marts", component=COMPONENT_SELLERS, run_id=ctx.run_id, filename=bridge_name)

    required = [
        "run_id",
        "component",
        "collected_at_utc",
        "source_system",
        "source_type",
        "source_ref",
        "status",
        "error_message",
        "supplier_id",
        "supplier_name",
        "rating",
        "valuation",
        "feedbacks_count",
        "sale_item_quantity",
        "registration_date",
        "update_date",
        "delivery_duration",
        "supp_ratio",
        "ratio_mark_supp",
        "rating_is_invisible",
        "http_status",
        "raw_file",
    ]

    bridge_required = [
        "run_id",
        "component",
        "supplier_id",
        "query",
        "nmId",
        "product_run_id",
    ]

    validate_csv_contract(
        raw_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses={"success", "error", "dry_run", "empty"},
        max_error_ratio=_max_error_ratio(config),
    )
    validate_csv_contract(
        staging_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses={"success", "error", "dry_run", "empty"},
        max_error_ratio=_max_error_ratio(config),
    )
    validate_csv_contract(
        mart_path,
        required_columns=required,
        min_rows=1,
        allowed_statuses={"success", "error", "dry_run", "empty"},
        max_error_ratio=_max_error_ratio(config),
    )
    validate_csv_contract(
        bridge_path,
        required_columns=bridge_required,
        min_rows=1,
        allowed_statuses={"success", "empty", "error", "dry_run"},
    )

    if int(result.get("items_error", 0)) > 0:
        raise NonCriticalPipelineError(
            f"sellers completed with partial errors: items_error={result.get('items_error', 0)}"
        )

    return result
