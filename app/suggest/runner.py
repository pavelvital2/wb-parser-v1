from __future__ import annotations

from app.common.config import AppConfig
from app.common.constants import COMPONENT_SUGGEST
from app.common.contracts import validate_csv_contract
from app.common.exceptions import NonCriticalPipelineError
from app.common.run_context import RunContext
from app.common.state_db import StateDB

from .alpha import run_suggest_collection


def _max_error_ratio(config: AppConfig) -> float | None:
    value = config.raw.get("validation", {}).get("max_error_ratio", {}).get(COMPONENT_SUGGEST)
    if value is None:
        return None
    return float(value)


def run_suggest(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    result = run_suggest_collection(config=config, db=db, ctx=ctx)

    raw_path = config.paths.output_path(
        layer="raw",
        component=COMPONENT_SUGGEST,
        run_id=ctx.run_id,
        filename="suggest_alpha_raw.csv",
    )
    staging_path = config.paths.output_path(
        layer="staging",
        component=COMPONENT_SUGGEST,
        run_id=ctx.run_id,
        filename="suggest_alpha_staging.csv",
    )

    common_required = [
        "run_id",
        "component",
        "collected_at_utc",
        "source_system",
        "source_type",
        "source_ref",
        "status",
        "error_message",
    ]

    validate_csv_contract(
        raw_path,
        required_columns=common_required
        + [
            "base_prefix",
            "typed_query",
            "letter",
            "depth",
            "position",
            "list_size",
            "suggestion",
        ],
        min_rows=1,
        allowed_statuses={"success", "empty", "error"},
        max_error_ratio=_max_error_ratio(config),
    )
    validate_csv_contract(
        staging_path,
        required_columns=common_required + ["typed_query", "suggestion", "suggestion_lc", "is_empty_suggestion"],
        min_rows=1,
        allowed_statuses={"success", "empty", "error"},
        max_error_ratio=_max_error_ratio(config),
    )

    if int(result.get("items_error", 0)) > 0:
        raise NonCriticalPipelineError(
            f"suggest completed with partial errors: items_error={result.get('items_error', 0)}"
        )
    return result
