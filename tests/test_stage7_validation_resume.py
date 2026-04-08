from __future__ import annotations

import csv
from pathlib import Path

import pytest

from app.common import cli as cli_mod
from app.common import runner as runner_mod
from app.common.config import load_config
from app.common.contracts import validate_csv_contract
from app.common.exceptions import CriticalPipelineError, NonCriticalPipelineError
from app.common.retry import with_retry
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.serp.engine import SerpEngine
from app.sellers.engine import SellersEngine


def _write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _make_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-stage7
  source_system: wildberries
  timezone: Europe/Moscow

paths:
  data_raw: {str((tmp_path / 'data' / 'raw')).replace('\\', '/')}
  data_staging: {str((tmp_path / 'data' / 'staging')).replace('\\', '/')}
  data_marts: {str((tmp_path / 'data' / 'marts')).replace('\\', '/')}
  logs: {str((tmp_path / 'data' / 'logs')).replace('\\', '/')}
  exports: {str((tmp_path / 'exports')).replace('\\', '/')}
  state_sqlite: {str((tmp_path / 'state' / 'sqlite' / 'state.sqlite')).replace('\\', '/')}
  checkpoints_dir: {str((tmp_path / 'state' / 'checkpoints')).replace('\\', '/')}

runtime:
  retry_max_attempts: 5
  retry_base_delay_seconds: 0.0
  retry_max_delay_seconds: 0.0
  http_timeout_seconds: 5
  dry_run: false
  debug: false

validation:
  max_error_ratio:
    suggest: 0.95
    serp: 0.95
    sellers: 0.95

suggest:
  prefixes_file: config/prefixes.txt
  alphabet_mode: ru30

filter:
  rules_file: config/query_rules.yaml
  wordstat_csv_files: []
  wordstat_csv_glob: data/raw/wordstat/*.csv
  input_files:
    suggest_staging_csv: ""
  output_files:
    candidates_raw_csv: filter_candidates_raw.csv
    debug_scores_csv: debug_scores.csv
    top_queries_csv: top_queries.csv
    queries_txt: queries.txt

serp:
  input_files:
    queries_txt: exports/queries.txt
    top_queries_csv: data/marts/filter/latest/top_queries.csv
  pages_per_query: 1
  page_size: 100
  wb_cookie_file_env: WB_COOKIE_FILE
  wb_cookie_file: state/wb_cookie.txt
  base_url: https://example.local/search
  request_params: {{}}
  user_agent: UA
  referer_base: https://example.local/
  x_requested_with: XMLHttpRequest
  sleep_between_pages_ms: 0
  sleep_between_queries_ms: 0
  stop_on_empty_page: true
  raw_pages_subdir: pages
  output_files:
    raw_products_csv: products_raw.csv
    staging_products_csv: products_staging.csv
    mart_products_daily_csv: products_daily.csv
    raw_pages_index_csv: pages_raw_index.csv
    sellers_input_csv: products_for_sellers.csv

sellers:
  input_files:
    products_daily_csv: data/marts/serp/latest/products_daily.csv
  api_base_url: https://example.local/suppliers
  curr: RUB
  user_agent: UA
  sleep_between_sellers_ms: 0
  full_refresh_checkpoints: false
  raw_responses_subdir: responses
  request_headers:
    accept: "*/*"
    x-client-name: site
  output_files:
    raw_sellers_csv: sellers_raw.csv
    staging_sellers_csv: sellers_staging.csv
    mart_sellers_daily_csv: sellers_daily.csv
    bridge_csv: seller_query_product_bridge.csv
"""

    cfg = tmp_path / "config" / "config.yaml"
    _write_text(cfg, config_yaml)
    _write_text(tmp_path / "config" / "prefixes.txt", "шеврон\n", encoding="utf-8-sig")
    _write_text(tmp_path / "config" / "query_rules.yaml", "default:\n  top_n: 10\n")
    _write_text(tmp_path / "exports" / "queries.txt", "q1\nq2\n", encoding="utf-8-sig")
    _write_text(tmp_path / "state" / "wb_cookie.txt", "cookie=1")

    return cfg


def _load_cfg_and_db(tmp_path: Path) -> tuple:
    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()
    return cfg, db


def _write_products_daily(path: Path, rows: list[dict[str, str]]) -> None:
    _write_csv(
        path,
        ["run_id", "query", "query_group", "nmId", "supplier_id", "supplier_name", "status"],
        rows,
    )


def test_contracts_fail_on_missing_required_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    with pytest.raises(CriticalPipelineError):
        validate_csv_contract(missing, required_columns=["run_id"])


def test_contracts_fail_on_missing_required_columns(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    _write_csv(path, ["a", "b"], [{"a": "1", "b": "2"}])
    with pytest.raises(CriticalPipelineError):
        validate_csv_contract(path, required_columns=["run_id", "status"], min_rows=1)


def test_smoke_pass_on_valid_output(tmp_path: Path) -> None:
    path = tmp_path / "ok.csv"
    rows = [{"run_id": "r1", "status": "success"}, {"run_id": "r2", "status": "error"}]
    _write_csv(path, ["run_id", "status"], rows)
    info = validate_csv_contract(
        path,
        required_columns=["run_id", "status"],
        min_rows=1,
        allowed_statuses={"success", "error"},
        max_error_ratio=0.6,
    )
    assert int(info["row_count"]) == 2


def test_retry_repeats_retriable_error() -> None:
    class TemporaryError(RuntimeError):
        pass

    state = {"calls": 0}

    def fn() -> int:
        state["calls"] += 1
        if state["calls"] < 3:
            raise TemporaryError("try again")
        return 42

    result = with_retry(
        fn,
        attempts=5,
        base_delay=0.0,
        max_delay=0.0,
        jitter_ratio=0.0,
        retriable_exceptions=(TemporaryError,),
    )
    assert result == 42
    assert state["calls"] == 3


def test_retry_does_not_repeat_non_retriable_error() -> None:
    class TemporaryError(RuntimeError):
        pass

    class FatalError(RuntimeError):
        pass

    state = {"calls": 0}

    def fn() -> int:
        state["calls"] += 1
        raise FatalError("fatal")

    with pytest.raises(FatalError):
        with_retry(
            fn,
            attempts=5,
            base_delay=0.0,
            max_delay=0.0,
            jitter_ratio=0.0,
            retriable_exceptions=(RuntimeError,),
            retriable_predicate=lambda exc: isinstance(exc, TemporaryError),
        )

    assert state["calls"] == 1


def test_checkpoint_resume_for_serp(tmp_path: Path) -> None:
    cfg, db = _load_cfg_and_db(tmp_path)
    cfg.runtime.dry_run = True
    ctx = RunContext(run_id="20260307_170000Z", pipeline="serp", component="serp", started_at_utc=utc_now_iso())

    db.save_checkpoint("serp", "q1|1", f"success|{ctx.run_id}|{utc_now_iso()}", utc_now_iso())

    result = SerpEngine(config=cfg, db=db, ctx=ctx).run()
    assert int(result["pages_done"]) == 1
    assert int(result["items_ok"]) == 1


def test_checkpoint_resume_for_sellers(tmp_path: Path) -> None:
    cfg, db = _load_cfg_and_db(tmp_path)
    cfg.runtime.dry_run = True
    products = cfg.paths.MARTS_DIR / "serp" / "latest" / "products_daily.csv"
    _write_products_daily(
        products,
        [
            {"run_id": "pr1", "query": "q1", "query_group": "g1", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
            {"run_id": "pr1", "query": "q2", "query_group": "g1", "nmId": "12", "supplier_id": "2002", "supplier_name": "B", "status": "success"},
        ],
    )

    ctx = RunContext(run_id="20260307_180000Z", pipeline="sellers", component="sellers", started_at_utc=utc_now_iso())
    db.save_checkpoint("sellers", "1001", f"success|{ctx.run_id}|{utc_now_iso()}", utc_now_iso())

    result = SellersEngine(config=cfg, db=db, ctx=ctx).run()
    assert int(result["processed_sellers"]) == 1
    assert int(result["items_ok"]) == 1


def test_partial_status_set_correctly_for_non_critical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg, db = _load_cfg_and_db(tmp_path)

    monkeypatch.setattr(runner_mod, "_resolve_components", lambda target: ["serp"])

    def _raise_non_critical(config, db, ctx):
        raise NonCriticalPipelineError("partial issue")

    monkeypatch.setattr(runner_mod, "_dispatch_component", _raise_non_critical)

    code = runner_mod.run_component(cfg, db, target="serp")
    assert code == 0

    run = db.list_runs(limit=1)[0]
    assert run["status"] == "partial"
    tasks = db.list_tasks(run["run_id"])
    assert tasks[0]["status"] == "partial"


def test_failed_status_set_correctly_for_critical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg, db = _load_cfg_and_db(tmp_path)

    monkeypatch.setattr(runner_mod, "_resolve_components", lambda target: ["serp"])

    def _raise_critical(config, db, ctx):
        raise CriticalPipelineError("boom")

    monkeypatch.setattr(runner_mod, "_dispatch_component", _raise_critical)

    with pytest.raises(CriticalPipelineError):
        runner_mod.run_component(cfg, db, target="serp")

    run = db.list_runs(limit=1)[0]
    assert run["status"] == "failed"
    tasks = db.list_tasks(run["run_id"])
    assert tasks[0]["status"] == "failed"


def test_doctor_returns_ok_on_valid_project_structure(tmp_path: Path) -> None:
    cfg_path = _make_config(tmp_path)
    code = cli_mod.cmd_doctor(str(cfg_path))
    assert code == 0
