from __future__ import annotations

import importlib.util
import os
import subprocess
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


def test_warehouse_validation_uses_verified_lock_v3_lease(monkeypatch) -> None:
    check = _load_pre_push_check()
    events: list[object] = []

    class Lease:
        pass_fds = (41, 42)

        def assert_held(self) -> None:
            events.append("held")

        def __exit__(self, *_args: object) -> None:
            events.append("released")

    lease = Lease()
    monkeypatch.setattr(check, "acquire_marketplace_collection_lease", lambda: lease)
    monkeypatch.setattr(
        check,
        "descendant_lease_environment",
        lambda current: {
            "PARSER_WB_LOCK_V3_CONTRACT": "test",
            "PARSER_WB_LOCK_V3_GUARD_FD": "41",
            "PARSER_WB_LOCK_V3_VALIDATION_FD": "42",
        },
    )
    monkeypatch.setattr(
        check,
        "verify_input_manifest",
        lambda _root: "a" * 64,
    )
    monkeypatch.setenv("MARKETPLACE_COORDINATOR_FORGED", "forbidden")
    monkeypatch.setenv("PARSER_WB_LOCK_V3_FORGED", "forbidden")

    def fake_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        environment = kwargs["environment"]
        assert isinstance(environment, dict)
        assert "MARKETPLACE_COORDINATOR_FORGED" not in environment
        assert "PARSER_WB_LOCK_V3_FORGED" not in environment
        assert environment["PARSER_WB_LOCK_V3_GUARD_FD"] == "41"
        assert kwargs["pass_fds"] == (41, 42)
        events.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(check, "run_command", fake_run)

    command = [os.fspath(check.PYTHON_BIN), "scripts/wb_warehouse.py", "check"]
    assert check.run_warehouse_required(command) is True
    assert events == [command, "held", "released"]


def test_doctor_validation_is_read_only(monkeypatch, tmp_path: Path) -> None:
    check = _load_pre_push_check()
    sqlite_path = tmp_path / "state.sqlite"

    class Paths:
        SQLITE_DB = sqlite_path

    class Config:
        paths = Paths()

    calls: list[object] = []
    monkeypatch.setattr(check, "load_config", lambda path: calls.append(path) or Config())
    monkeypatch.setattr(
        check,
        "StateDB",
        lambda path, *, create_parent: calls.append((path, create_parent)) or object(),
    )
    monkeypatch.setattr(
        check,
        "_doctor_checks",
        lambda config, db: ([], ["expected warning"]),
    )

    assert check.run_read_only_doctor() is True
    assert calls == [check.CONFIG_FILE, (sqlite_path, False)]
    assert not sqlite_path.exists()
