from __future__ import annotations

from app.common.config import AppConfig
from app.common.constants import COMPONENT_FILTER
from app.common.contracts import validate_csv_contract, validate_text_contract
from app.common.run_context import RunContext
from app.common.state_db import StateDB

from .engine import FilterEngine


def run_filter(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    result = FilterEngine(config=config, db=db, ctx=ctx).run()

    out_cfg = config.raw.get("filter", {}).get("output_files", {})
    raw_name = str(out_cfg.get("candidates_raw_csv", "filter_candidates_raw.csv"))
    debug_name = str(out_cfg.get("debug_scores_csv", "debug_scores.csv"))
    top_name = str(out_cfg.get("top_queries_csv", "top_queries.csv"))
    queries_name = str(out_cfg.get("queries_txt", "queries.txt"))

    raw_path = config.paths.output_path(layer="raw", component=COMPONENT_FILTER, run_id=ctx.run_id, filename=raw_name)
    debug_path = config.paths.output_path(layer="staging", component=COMPONENT_FILTER, run_id=ctx.run_id, filename=debug_name)
    top_path = config.paths.output_path(layer="marts", component=COMPONENT_FILTER, run_id=ctx.run_id, filename=top_name)
    run_queries_path = config.paths.output_path(layer="marts", component=COMPONENT_FILTER, run_id=ctx.run_id, filename=queries_name)
    export_queries_path = config.paths.EXPORTS_DIR / queries_name

    validate_csv_contract(
        raw_path,
        required_columns=["run_id", "component", "normalized_query", "canonical_query", "count", "wordstat_volume"],
        min_rows=1,
    )
    validate_csv_contract(
        debug_path,
        required_columns=[
            "run_id",
            "component",
            "normalized_query",
            "canonical_query",
            "source_query",
            "is_selected",
            "passes_filters",
            "parent_hops_used",
            "source_typed_queries_count",
            "hybrid_score",
        ],
        min_rows=1,
    )
    validate_csv_contract(
        top_path,
        required_columns=["run_id", "component", "rank", "query", "normalized_query", "hybrid_score"],
        min_rows=1,
    )
    validate_text_contract(run_queries_path, min_lines=1)
    validate_text_contract(export_queries_path, min_lines=1)

    return result
