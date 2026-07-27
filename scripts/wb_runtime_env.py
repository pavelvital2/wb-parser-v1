#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.nightly_coordinator import require_official_live_entry_lease
from app.common.runtime_env import (
    RuntimeEnvironmentError,
    load_strict_runtime_environment,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Strict WB runtime dotenv loader"
    )
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--runtime-file", required=True)
    parser.add_argument("--export0", action="store_true", required=True)
    return parser


def main() -> int:
    try:
        args = build_parser().parse_args()
        project_root = Path(args.project_root).resolve(strict=True)
        runtime_file = Path(args.runtime_file).resolve(strict=False)
        if (
            project_root != PROJECT_ROOT
            or runtime_file != project_root / "config/runtime.env"
        ):
            raise RuntimeEnvironmentError("runtime_env_path_invalid")
        require_official_live_entry_lease(environment=os.environ)
        loaded = load_strict_runtime_environment(
            project_root=project_root,
            base_environment=os.environ,
        )
        for key in loaded.exported_keys:
            value = loaded.environment[key]
            os.write(
                sys.stdout.fileno(),
                os.fsencode(f"{key}={value}") + b"\0",
            )
        return 0
    except (OSError, RuntimeError, ValueError):
        print("WB runtime environment could not be loaded", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
