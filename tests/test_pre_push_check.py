from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_pre_push_check():
    path = Path(__file__).resolve().parents[1] / "scripts" / "pre_push_check.py"
    spec = importlib.util.spec_from_file_location("pre_push_check", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_forbidden_paths_block_runtime_and_secret_files() -> None:
    check = _load_pre_push_check()

    forbidden = [
        "state/wb_warehouse/latest.json",
        "data/warehouse/wb/wb.duckdb",
        "data/logs/cron_products_sellers.log",
        "config/runtime.env.backup_20260704T060141Z",
        "config/wb_cookie.txt.candidate_20260702",
        "config/wb_request_headers.json",
        "state/browser/wb_storage_state.json",
        "docs/parser_wb_agent_handoff_20260627.md",
        "AGENTS.md.bak_summary_rule_20260628203123",
    ]

    assert all(check.forbidden_reason(path) for path in forbidden)


def test_forbidden_paths_allow_normal_source_files() -> None:
    check = _load_pre_push_check()

    allowed = [
        "scripts/wb_warehouse.py",
        "scripts/run_pre_push_check.sh",
        "tests/test_wb_warehouse.py",
        "docs/WB_WAREHOUSE.md",
        "config/config.yaml",
        ".env.example",
    ]

    assert [path for path in allowed if check.forbidden_reason(path)] == []
