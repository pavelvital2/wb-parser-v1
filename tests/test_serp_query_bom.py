from __future__ import annotations

from pathlib import Path

from app.common.config import load_config
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.serp.engine import SerpEngine


def _write(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def _make_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-serp-queries-bom
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
  retry_max_attempts: 1
  retry_base_delay_seconds: 0.01
  retry_max_delay_seconds: 0.02
  http_timeout_seconds: 5
  dry_run: false
  debug: false

serp:
  input_files:
    queries_txt: exports/queries_bom.txt
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
"""
    path = tmp_path / "config" / "config.yaml"
    _write(path, config_yaml)
    _write(tmp_path / "state" / "wb_cookie.txt", "cookie=1")
    return path


def test_serp_queries_txt_utf8_sig_no_bom_in_first_query(tmp_path: Path) -> None:
    cfg = load_config(str(_make_config(tmp_path)))
    _write(
        tmp_path / "exports" / "queries_bom.txt",
        "кроссовки\nфутболка мужская\n",
        encoding="utf-8-sig",
    )

    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id="20260307_000001Z", pipeline="serp", component="serp", started_at_utc=utc_now_iso())
    engine = SerpEngine(config=cfg, db=db, ctx=ctx)

    tasks = engine._load_query_tasks()
    assert tasks
    assert tasks[0].query == "кроссовки"
    assert not tasks[0].query.startswith("\ufeff")
