from __future__ import annotations

from pathlib import Path

from app.common.config import load_config
from app.common.csv_io import read_csv_rows
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


class _JsonResponse:
    def __init__(self, status_code: int, payload: dict, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.content = (text or "{}").encode("utf-8")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def get(self, *args, **kwargs):
        return self._response


class _EndpointFallbackSession:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, *args, **kwargs):
        self.urls.append(url)
        if url == "https://fallback.local/search":
            return _JsonResponse(
                200,
                {
                    "products": [
                        {
                            "id": 123,
                            "name": "ok",
                            "brand": "brand",
                            "supplierId": 456,
                            "supplier": "seller",
                        }
                    ]
                },
                '{"products":[{"id":123}]}',
            )
        return _FakeResponse(498, "blocked")


class _CloseableSession:
    def __init__(self, label: int) -> None:
        self.label = label
        self.closed = False

    def close(self) -> None:
        self.closed = True


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


def test_fetch_page_falls_back_after_retryable_http_status(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)
    engine.base_urls = ["https://internal.local/search", "https://fallback.local/search"]
    engine.retry_max_attempts = 1
    session = _EndpointFallbackSession()

    response, payload, error, raw_file = engine._fetch_page(session=session, query="шеврон мвд", page=1)

    assert response is not None
    assert response.status_code == 200
    assert payload is not None
    assert error == ""
    assert raw_file.startswith(f"data/raw/serp/{engine.ctx.run_id}/")
    assert session.urls == ["https://internal.local/search", "https://fallback.local/search"]
    assert engine._active_base_url_index == 1


def test_build_session_uses_configured_proxy(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)
    engine.proxy_url = "http://proxy.local:3128"

    session = engine._build_session("cookie=1")

    try:
        assert session.proxies == {
            "http": "http://proxy.local:3128",
            "https": "http://proxy.local:3128",
        }
    finally:
        session.close()


def test_payload_anomaly_cooldown_retries_same_page_with_new_session(tmp_path: Path, monkeypatch) -> None:
    engine = _make_engine(tmp_path)
    engine.payload_anomaly_cooldown_after_consecutive = 2
    engine.payload_anomaly_cooldown_base_seconds = 0.0
    engine.payload_anomaly_cooldown_increment_seconds = 0.0
    engine.payload_anomaly_keeper_smoke_enabled = False
    engine.retry_base_delay_seconds = 0.0
    engine.retry_max_delay_seconds = 0.0

    sessions: list[_CloseableSession] = []

    def fake_build_session(cookie_value: str) -> _CloseableSession:
        session = _CloseableSession(label=len(sessions))
        sessions.append(session)
        return session

    calls: list[tuple[int, str, int, bool]] = []

    def fake_fetch_page(
        *,
        session: _CloseableSession,
        query: str,
        page: int,
        retry_payload_anomalies: bool = True,
    ):
        calls.append((session.label, query, page, retry_payload_anomalies))
        if len(calls) <= 2:
            return (
                _FakeResponse(200, "{}"),
                None,
                "retryable_payload_anomaly: nested promo products=1",
                "data/raw/serp/test/page_1.json",
            )
        return (
            _FakeResponse(200, "{}"),
            {
                "products": [
                    {
                        "id": 123,
                        "name": "ok",
                        "brand": "brand",
                        "supplierId": 456,
                        "supplier": "seller",
                    }
                ]
            },
            "",
            "data/raw/serp/test/page_1.json",
        )

    monkeypatch.setattr(engine, "_build_session", fake_build_session)
    monkeypatch.setattr(engine, "_fetch_page", fake_fetch_page)

    result = engine.run()

    assert result["items_ok"] == 1
    assert result["items_error"] == 0
    assert result["pages_done"] == 1
    assert result["payload_anomaly_cooldowns"] == 1
    assert calls == [
        (0, "test query", 1, False),
        (0, "test query", 1, False),
        (1, "test query", 1, False),
    ]
    assert [session.closed for session in sessions] == [True, True]

    page_rows = read_csv_rows(Path(result["pages_index_path"]))
    assert len(page_rows) == 1
    assert page_rows[0]["source_ref"] == "test query|page=1"
    assert page_rows[0]["status"] == "success"


def test_error_ip_rotation_retries_same_page_before_recording_error(tmp_path: Path, monkeypatch) -> None:
    engine = _make_engine(tmp_path)
    engine.deferred_retry_enabled = False
    engine.error_ip_rotation_enabled = True
    engine.error_ip_rotation_url = "https://rotate.example.test/change_ip"
    engine.error_ip_rotation_wait_seconds = 0.0
    engine.error_ip_rotation_max_attempts = 1

    sessions: list[_CloseableSession] = []

    def fake_build_session(cookie_value: str) -> _CloseableSession:
        session = _CloseableSession(label=len(sessions))
        sessions.append(session)
        return session

    fetch_calls: list[tuple[int, str, int]] = []

    def fake_fetch_page(*, session: _CloseableSession, query: str, page: int, retry_payload_anomalies: bool = True):
        fetch_calls.append((session.label, query, page))
        if len(fetch_calls) == 1:
            return (
                _FakeResponse(429, "blocked"),
                None,
                "http_429: blocked",
                "data/raw/serp/test/page_1_error.json",
            )
        return (
            _JsonResponse(200, {"products": [{"id": 123, "name": "ok", "brand": "brand"}]}, '{"products":[{"id":123}]}'),
            {"products": [{"id": 123, "name": "ok", "brand": "brand"}]},
            "",
            "data/raw/serp/test/page_1.json",
        )

    rotations: list[tuple[str, int, int, str]] = []

    def fake_rotate_ip_after_error(*, query: str, page: int, http_status: int, error_message: str) -> bool:
        rotations.append((query, page, http_status, error_message))
        return True

    monkeypatch.setattr(engine, "_build_session", fake_build_session)
    monkeypatch.setattr(engine, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(engine, "_rotate_ip_after_error", fake_rotate_ip_after_error)

    result = engine.run()

    assert result["items_ok"] == 1
    assert result["items_error"] == 0
    assert result["pages_done"] == 1
    assert result["ip_rotations"] == 1
    assert fetch_calls == [(0, "test query", 1), (1, "test query", 1)]
    assert rotations == [("test query", 1, 429, "http_429: blocked")]
    assert [session.closed for session in sessions] == [True, True]

    page_rows = read_csv_rows(Path(result["pages_index_path"]))
    assert len(page_rows) == 1
    assert page_rows[0]["source_ref"] == "test query|page=1"
    assert page_rows[0]["status"] == "success"
