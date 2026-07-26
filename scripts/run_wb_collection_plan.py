#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.config import load_config
from app.common.exceptions import ConfigValidationError, CriticalPipelineError
from app.serp.collection_plan import CollectionPlanValidationError
from app.serp.collection_plan_runner import run_collection_plan
from app.serp.regional_pilot import run_guarded_regional_pilot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one isolated WB collection plan without publication"
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--plan-file", required=True)
    parser.add_argument("--no-publish", action="store_true", required=True)
    parser.add_argument("--guarded-pilot", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        config = load_config(args.config)
        plan_path = Path(args.plan_file)
        if not plan_path.is_absolute():
            plan_path = config.project_root / plan_path
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
    except (
        CriticalPipelineError,
        CollectionPlanValidationError,
        ConfigValidationError,
        FileNotFoundError,
    ) as exc:
        print(str(exc), file=sys.stderr)
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


if __name__ == "__main__":
    raise SystemExit(main())
