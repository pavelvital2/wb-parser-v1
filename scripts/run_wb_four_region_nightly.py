#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.config import load_config
from app.common.exceptions import CriticalPipelineError
from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.collection_plan_runner import run_collection_plan
from app.serp.four_region_nightly import (
    PRE_CUTOVER_DOWNSTREAM_MODE,
    run_four_region_downstream,
    write_four_region_failure_preview,
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


def main() -> int:
    args = build_parser().parse_args()
    if args.resume_run_id and args.downstream_only_run_id:
        print(
            "--resume-run-id and --downstream-only-run-id are mutually exclusive",
            file=sys.stderr,
        )
        return 2
    config = None
    run_id = (
        args.downstream_only_run_id
        or args.resume_run_id
        or datetime.now(UTC).strftime(
        "%Y%m%d_%H%M%SZ"
        )
    )
    downstream_started = False
    try:
        config = load_config(args.config)
        plan_path = Path(args.plan_file)
        if not plan_path.is_absolute():
            plan_path = config.project_root / plan_path
        if args.downstream_only_run_id:
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
        )
    except (
        CriticalPipelineError,
        CollectionPlanValidationError,
        FileNotFoundError,
    ) as exc:
        if config is not None and not downstream_started:
            write_four_region_failure_preview(
                config=config,
                run_id=run_id,
                error=exc,
            )
        print(str(exc), file=sys.stderr)
        return 1
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
