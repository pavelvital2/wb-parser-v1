from __future__ import annotations

import socket
from pathlib import Path

import pytest
import requests

from app.common.config import load_config
from app.common.csv_io import read_csv_rows
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.serp.collection_plan import load_query_pack, normalize_query_text
from app.serp.engine import SerpEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACK_PATH = (
    PROJECT_ROOT
    / "config/wb/query_packs/shevron-core/2026-07-26.1.json"
)
LEGACY_QUERIES_PATH = PROJECT_ROOT / "exports/queries.txt"


def _legacy_queries() -> list[str]:
    return [
        normalize_query_text(line)
        for line in LEGACY_QUERIES_PATH.read_text(encoding="utf-8-sig").splitlines()
        if normalize_query_text(line)
    ]


def _write(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def _make_dry_run_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-query-pack-legacy-dry-run
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
  retry_base_delay_seconds: 0
  retry_max_delay_seconds: 0
  http_timeout_seconds: 1
  dry_run: true
  debug: false
  locking_enabled: false
  lock_stale_seconds: 60

serp:
  input_files:
    queries_txt: exports/queries.txt
    top_queries_csv: data/marts/filter/latest/top_queries.csv
  pages_per_query: 10
  page_size: 100
  wb_cookie_file_env: WB_COOKIE_FILE
  wb_cookie_file: state/not-used-cookie.txt
  base_url: https://network-is-forbidden.invalid/search
  request_params: {{}}
  user_agent: test
  referer_base: https://network-is-forbidden.invalid/
  x_requested_with: XMLHttpRequest
  output_files:
    raw_products_csv: products_raw.csv
    staging_products_csv: products_staging.csv
    mart_products_daily_csv: products_daily.csv
    raw_pages_index_csv: pages_raw_index.csv
    sellers_input_csv: products_for_sellers.csv

validation:
  max_error_ratio:
    serp: 0.05
"""
    config_path = tmp_path / "config/config.yaml"
    _write(config_path, config_yaml)
    _write(
        tmp_path / "exports/queries.txt",
        LEGACY_QUERIES_PATH.read_text(encoding="utf-8-sig"),
        encoding="utf-8-sig",
    )
    return config_path


def test_first_query_pack_matches_exact_normalized_legacy_order() -> None:
    pack = load_query_pack(PACK_PATH)
    pack_queries = [query.text for query in pack.queries]
    legacy_queries = _legacy_queries()

    assert len(legacy_queries) == 30
    assert len(set(legacy_queries)) == 30
    assert pack_queries == legacy_queries


def test_legacy_serp_dry_run_output_remains_30_queries_in_order_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_network(*args, **kwargs):
        raise AssertionError("legacy dry-run must not call the network")

    monkeypatch.setattr(socket, "socket", forbidden_network)
    monkeypatch.setattr(socket, "create_connection", forbidden_network)
    monkeypatch.setattr(requests.Session, "request", forbidden_network)

    config = load_config(str(_make_dry_run_config(tmp_path)))
    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    context = RunContext(
        run_id="20260726_120000Z",
        pipeline="serp",
        component="serp",
        started_at_utc=utc_now_iso(),
    )

    result = SerpEngine(config=config, db=db, ctx=context).run()
    rows = read_csv_rows(Path(str(result["mart_products_path"])))

    assert int(result["queries_done"]) == 30
    assert int(result["pages_done"]) == 30
    assert int(result["items_ok"]) == 30
    assert int(result["items_error"]) == 0
    assert int(result["outputs_published"]) == 1
    assert [row["query"] for row in rows] == _legacy_queries()
    assert all(row["query_group"] == "" for row in rows)
    assert all(row["status"] == "dry_run" for row in rows)
