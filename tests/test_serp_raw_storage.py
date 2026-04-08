from __future__ import annotations

from pathlib import Path

from app.common.config import load_config
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.serp.engine import SerpEngine


class _FakeResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")

    def json(self):
        raise ValueError("bad json")


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def get(self, *args, **kwargs):
        return self._response


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-serp
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
  retry_max_attempts: 2
  retry_base_delay_seconds: 0.01
  retry_max_delay_seconds: 0.02
  http_timeout_seconds: 5
  dry_run: false
  debug: false

filter:
  input_files:
    suggest_staging_csv: ""

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
  output_files:
    raw_products_csv: products_raw.csv
    staging_products_csv: products_staging.csv
    mart_products_daily_csv: products_daily.csv
    raw_pages_index_csv: pages_raw_index.csv
    sellers_input_csv: products_for_sellers.csv
"""
    path = tmp_path / "config" / "config.yaml"
    _write(path, config_yaml)
    _write(tmp_path / "state" / "wb_cookie.txt", "cookie=1")
    _write(tmp_path / "exports" / "queries.txt", "test query\n")
    return path


def _make_engine(tmp_path: Path, run_id: str = "20260307_120000Z") -> SerpEngine:
    cfg = load_config(str(_make_config(tmp_path)))
    db = StateDB(cfg.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id=run_id, pipeline="serp", component="serp", started_at_utc=utc_now_iso())
    return SerpEngine(config=cfg, db=db, ctx=ctx)


def test_write_raw_response_uses_run_and_query_slug(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)
    rel = engine._write_raw_response("шеврон мвд", 3, b'{"products":[]}')
    assert rel.startswith(f"data/raw/serp/{engine.ctx.run_id}/")
    assert rel.endswith("/page_3.json")

    abs_path = engine.config.project_root / rel
    assert abs_path.exists()
    assert abs_path.read_bytes() == b'{"products":[]}'


def test_fetch_page_saves_raw_on_invalid_json(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)
    session = _FakeSession(_FakeResponse(200, "not a json payload"))

    response, payload, error, raw_file = engine._fetch_page(session=session, query="шеврон мвд", page=2)

    assert response is not None
    assert payload is None
    assert error.startswith("json_decode_failed:")
    assert raw_file.startswith(f"data/raw/serp/{engine.ctx.run_id}/")

    abs_path = engine.config.project_root / raw_file
    assert abs_path.exists()
    assert abs_path.read_text(encoding="utf-8") == "not a json payload"
