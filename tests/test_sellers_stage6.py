from __future__ import annotations

import csv
from contextlib import nullcontext
from pathlib import Path

import pytest

from app.common.config import load_config
from app.common.csv_io import read_csv_rows, write_csv_rows
from app.common.exceptions import CriticalPipelineError, NonCriticalPipelineError
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.sellers.engine import SellerSeed, SellersEngine, SellersRunScope
from app.sellers.runner import run_sellers


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_products_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["run_id", "query", "query_group", "nmId", "supplier_id", "supplier_name", "status"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _make_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-sellers
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

sellers:
  input_files:
    products_daily_csv: data/marts/serp/latest/products_daily.csv
  api_base_url: https://suppliers-shipment-2.wildberries.ru/api/v1/suppliers
  curr: RUB
  user_agent: UA
  sleep_between_sellers_ms: 0
  full_refresh_checkpoints: false
  raw_responses_subdir: responses
  request_headers:
    accept: "*/*"
    accept-language: ru,en;q=0.9,en-US;q=0.8
    origin: https://www.wildberries.ru
    x-client-name: site
  output_files:
    raw_sellers_csv: sellers_raw.csv
    staging_sellers_csv: sellers_staging.csv
    mart_sellers_daily_csv: sellers_daily.csv
    bridge_csv: seller_query_product_bridge.csv
"""

    config_path = tmp_path / "config" / "config.yaml"
    _write_text(config_path, config_yaml)
    return config_path


def _setup(tmp_path: Path, rows: list[dict[str, str]], run_id: str = "20260307_120000Z") -> tuple[SellersEngine, StateDB]:
    config = load_config(str(_make_config(tmp_path)))
    products_path = config.paths.MARTS_DIR / "serp" / "latest" / "products_daily.csv"
    _write_products_csv(products_path, rows)

    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id=run_id, pipeline="sellers", component="sellers", started_at_utc=utc_now_iso())
    return SellersEngine(config=config, db=db, ctx=ctx), db


def _success_payload(seller_id: str) -> dict[str, str | int | float | bool]:
    return {
        "id": int(seller_id),
        "name": f"Seller {seller_id}",
        "rating": 4.9,
        "valuation": 1234,
        "feedbacksCount": 450,
        "saleItemQuantity": 120,
        "registrationDate": "2024-01-01",
        "updateDate": "2026-03-07",
        "deliveryDuration": 2,
        "suppRatio": 0.97,
        "ratioMarkSupp": 0.88,
        "ratingIsInvisible": False,
    }


class _FakeSellerResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = b'{"ok": true}'
        self.text = '{"ok": true}'
        self.closed = False

    def json(self) -> dict[str, object]:
        return self._payload

    def close(self) -> None:
        self.closed = True


class _FakeSellerSession:
    def __init__(self, response: _FakeSellerResponse) -> None:
        self.response = response

    def get(self, *args, **kwargs) -> _FakeSellerResponse:
        return self.response


def test_extract_unique_suppliers_from_products(tmp_path: Path) -> None:
    rows = [
        {"supplier_id": "1001", "supplier_name": "A", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "run_id": "r1"},
        {"supplier_id": "1001", "supplier_name": "A", "query": "кеды", "query_group": "обувь", "nmId": "12", "run_id": "r1"},
        {"supplier_id": "2002", "supplier_name": "B", "query": "футболка", "query_group": "одежда", "nmId": "21", "run_id": "r2"},
    ]
    engine, _ = _setup(tmp_path, rows)

    seeds = engine._extract_unique_sellers(rows)
    assert set(seeds.keys()) == {"1001", "2002"}
    assert seeds["1001"].queries == {"кроссовки", "кеды"}
    assert seeds["1001"].nm_ids == {"11", "12"}


def test_run_writes_raw_staging_mart_and_bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "12", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr2", "query": "футболка", "query_group": "одежда", "nmId": "21", "supplier_id": "2002", "supplier_name": "B", "status": "success"},
    ]
    engine, _ = _setup(tmp_path, rows)

    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))

    def _fake_fetch(self: SellersEngine, session, seller_id: str):
        return 200, _success_payload(seller_id), "", f"data/raw/sellers/{self.ctx.run_id}/responses/supplier_{seller_id}.json"

    monkeypatch.setattr(SellersEngine, "_fetch_seller", _fake_fetch)

    result = engine.run()
    assert int(result["items_ok"]) == 2

    raw_path = engine.config.paths.output_path(layer="raw", component="sellers", run_id=engine.ctx.run_id, filename="sellers_raw.csv")
    staging_path = engine.config.paths.output_path(layer="staging", component="sellers", run_id=engine.ctx.run_id, filename="sellers_staging.csv")
    mart_path = engine.config.paths.output_path(layer="marts", component="sellers", run_id=engine.ctx.run_id, filename="sellers_daily.csv")
    bridge_path = engine.config.paths.output_path(layer="marts", component="sellers", run_id=engine.ctx.run_id, filename="seller_query_product_bridge.csv")

    assert len(read_csv_rows(raw_path)) == 2
    assert len(read_csv_rows(staging_path)) == 2
    assert len(read_csv_rows(mart_path)) == 2
    assert len(read_csv_rows(bridge_path)) == 3


def test_fetch_seller_closes_response(tmp_path: Path) -> None:
    engine, _ = _setup(
        tmp_path,
        [{"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"}],
    )
    response = _FakeSellerResponse(200, _success_payload("1001"))
    session = _FakeSellerSession(response)

    status, payload, error, raw_file = engine._fetch_seller(session, "1001")

    assert status == 200
    assert payload is not None
    assert error == ""
    assert raw_file
    assert response.closed is True


def test_checkpoint_resume_by_seller_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr1", "query": "кеды", "query_group": "обувь", "nmId": "12", "supplier_id": "2002", "supplier_name": "B", "status": "success"},
    ]
    engine, db = _setup(tmp_path, rows, run_id="20260307_130000Z")

    db.save_checkpoint("sellers", "1001", f"success|{engine.ctx.run_id}|{utc_now_iso()}", utc_now_iso())

    calls: list[str] = []
    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))

    def _fake_fetch(self: SellersEngine, session, seller_id: str):
        calls.append(seller_id)
        return 200, _success_payload(seller_id), "", ""

    monkeypatch.setattr(SellersEngine, "_fetch_seller", _fake_fetch)

    result = engine.run()
    assert int(result["items_ok"]) == 1
    assert calls == ["2002"]


def test_smoke_validation_required_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
    ]
    config = load_config(str(_make_config(tmp_path)))
    products_path = config.paths.MARTS_DIR / "serp" / "latest" / "products_daily.csv"
    _write_products_csv(products_path, rows)

    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id="20260307_140000Z", pipeline="sellers", component="sellers", started_at_utc=utc_now_iso())

    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))
    monkeypatch.setattr(
        SellersEngine,
        "_fetch_seller",
        lambda self, session, seller_id: (200, _success_payload(seller_id), "", ""),
    )

    result = run_sellers(config=config, db=db, ctx=ctx)
    assert int(result["items_ok"]) == 1


def test_partial_errors_are_non_critical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr1", "query": "кеды", "query_group": "обувь", "nmId": "12", "supplier_id": "2002", "supplier_name": "B", "status": "success"},
    ]
    config = load_config(str(_make_config(tmp_path)))
    products_path = config.paths.MARTS_DIR / "serp" / "latest" / "products_daily.csv"
    _write_products_csv(products_path, rows)

    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id="20260307_150000Z", pipeline="sellers", component="sellers", started_at_utc=utc_now_iso())

    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))

    def _fake_fetch(self: SellersEngine, session, seller_id: str):
        if seller_id == "2002":
            return 503, None, "http_503: upstream unavailable", ""
        return 200, _success_payload(seller_id), "", ""

    monkeypatch.setattr(SellersEngine, "_fetch_seller", _fake_fetch)

    with pytest.raises(NonCriticalPipelineError):
        run_sellers(config=config, db=db, ctx=ctx)


def test_bridge_contains_query_product_seller_links(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "11", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr1", "query": "кроссовки", "query_group": "обувь", "nmId": "12", "supplier_id": "1001", "supplier_name": "A", "status": "success"},
        {"run_id": "pr1", "query": "футболка", "query_group": "одежда", "nmId": "21", "supplier_id": "2002", "supplier_name": "B", "status": "success"},
    ]
    engine, _ = _setup(tmp_path, rows, run_id="20260307_160000Z")

    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))
    monkeypatch.setattr(
        SellersEngine,
        "_fetch_seller",
        lambda self, session, seller_id: (200, _success_payload(seller_id), "", ""),
    )

    engine.run()

    bridge_path = engine.config.paths.output_path(layer="marts", component="sellers", run_id=engine.ctx.run_id, filename="seller_query_product_bridge.csv")
    bridge = read_csv_rows(bridge_path)

    triples = {(r.get("supplier_id", ""), r.get("query", ""), r.get("nmId", "")) for r in bridge}
    assert ("1001", "кроссовки", "11") in triples
    assert ("1001", "кроссовки", "12") in triples
    assert ("2002", "футболка", "21") in triples


def test_scoped_sellers_deduplicate_suppliers_without_global_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(str(_make_config(tmp_path)))
    input_path = tmp_path / "data/marts/wb_four_region/plan/run/products_for_sellers.csv"
    _write_products_csv(
        input_path,
        [
            {
                "run_id": "regional-run",
                "query": "q1",
                "query_group": "pack",
                "nmId": "11",
                "supplier_id": "1001",
                "supplier_name": "A",
                "status": "success",
            },
            {
                "run_id": "regional-run",
                "query": "q2",
                "query_group": "pack",
                "nmId": "12",
                "supplier_id": "1001",
                "supplier_name": "A",
                "status": "success",
            },
        ],
    )
    db = StateDB(tmp_path / "state/wb_four_region/plan/run/sellers.sqlite")
    db.init_schema()
    ctx = RunContext(
        run_id="20260726_001600Z",
        pipeline="wb_four_region_nightly",
        component="sellers_regional",
        started_at_utc=utc_now_iso(),
    )
    scope = SellersRunScope(
        input_products_path=input_path,
        raw_dir=tmp_path / "data/raw/sellers_scoped/plan/run",
        staging_dir=tmp_path / "data/staging/sellers_scoped/plan/run",
        mart_dir=tmp_path / "data/marts/sellers_scoped/plan/run",
        checkpoint_component="sellers_regional:plan",
    )
    engine = SellersEngine(config=config, db=db, ctx=ctx, run_scope=scope)
    calls: list[str] = []
    monkeypatch.setattr(SellersEngine, "_build_session", lambda self: nullcontext(object()))

    def fake_fetch(self: SellersEngine, session, seller_id: str):
        calls.append(seller_id)
        return 200, _success_payload(seller_id), "", ""

    monkeypatch.setattr(SellersEngine, "_fetch_seller", fake_fetch)
    result = engine.run()
    assert calls == ["1001"]
    assert result["status"] == "success"
    assert result["items_ok"] == 1
    assert result["processed_sellers"] == 1
    assert result["invocation_processed_sellers"] == 1
    assert result["latest_mart_sellers_path"] == ""
    assert not (tmp_path / "data/marts/sellers/latest").exists()
    assert len(read_csv_rows(Path(str(result["mart_sellers_path"])))) == 1


def test_scoped_sellers_partial_resume_reports_full_verified_mart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(str(_make_config(tmp_path)))
    input_path = (
        tmp_path
        / "data/marts/wb_four_region/plan/run/products_for_sellers.csv"
    )
    _write_products_csv(
        input_path,
        [
            {
                "run_id": "regional-run",
                "query": "q1",
                "query_group": "pack",
                "nmId": "11",
                "supplier_id": "1001",
                "supplier_name": "A",
                "status": "success",
            },
            {
                "run_id": "regional-run",
                "query": "q2",
                "query_group": "pack",
                "nmId": "22",
                "supplier_id": "2002",
                "supplier_name": "B",
                "status": "success",
            },
        ],
    )
    db = StateDB(tmp_path / "state/wb_four_region/plan/run/sellers.sqlite")
    db.init_schema()
    run_id = "20260726_001600Z"
    ctx = RunContext(
        run_id=run_id,
        pipeline="wb_four_region_nightly",
        component="sellers_regional",
        started_at_utc=utc_now_iso(),
    )
    scope = SellersRunScope(
        input_products_path=input_path,
        raw_dir=tmp_path / "data/raw/sellers_scoped/plan/run",
        staging_dir=tmp_path / "data/staging/sellers_scoped/plan/run",
        mart_dir=tmp_path / "data/marts/sellers_scoped/plan/run",
        checkpoint_component="sellers_regional:plan",
    )
    monkeypatch.setattr(
        SellersEngine,
        "_build_session",
        lambda self: nullcontext(object()),
    )
    first_calls: list[str] = []

    def first_fetch(
        self: SellersEngine,
        session: object,
        seller_id: str,
    ) -> tuple[int, dict[str, object] | None, str, str]:
        first_calls.append(seller_id)
        if seller_id == "2002":
            return 503, None, "http_503: unavailable", ""
        return 200, _success_payload(seller_id), "", ""

    monkeypatch.setattr(SellersEngine, "_fetch_seller", first_fetch)
    first = SellersEngine(
        config=config,
        db=db,
        ctx=ctx,
        run_scope=scope,
    ).run()
    assert first_calls == ["1001", "2002"]
    assert first["status"] == "partial"
    assert first["items_ok"] == 1
    assert first["items_error"] == 1
    assert first["processed_sellers"] == 2
    assert first["invocation_processed_sellers"] == 2

    resume_calls: list[str] = []

    def resumed_fetch(
        self: SellersEngine,
        session: object,
        seller_id: str,
    ) -> tuple[int, dict[str, object], str, str]:
        resume_calls.append(seller_id)
        return 200, _success_payload(seller_id), "", ""

    monkeypatch.setattr(SellersEngine, "_fetch_seller", resumed_fetch)
    resumed = SellersEngine(
        config=config,
        db=db,
        ctx=ctx,
        run_scope=scope,
    ).run()
    assert resume_calls == ["2002"]
    assert resumed["status"] == "success"
    assert resumed["items_ok"] == 2
    assert resumed["items_error"] == 0
    assert resumed["processed_sellers"] == 2
    assert resumed["invocation_processed_sellers"] == 1
    mart_rows = read_csv_rows(Path(str(resumed["mart_sellers_path"])))
    assert [row["supplier_id"] for row in mart_rows] == ["1001", "2002"]
    assert [row["status"] for row in mart_rows] == ["success", "success"]

    monkeypatch.setattr(
        SellersEngine,
        "_fetch_seller",
        lambda *_args, **_kwargs: pytest.fail(
            "fully resumed sellers must not fetch"
        ),
    )
    fully_resumed = SellersEngine(
        config=config,
        db=db,
        ctx=ctx,
        run_scope=scope,
    ).run()
    assert fully_resumed["status"] == "success"
    assert fully_resumed["items_ok"] == 2
    assert fully_resumed["items_error"] == 0
    assert fully_resumed["processed_sellers"] == 2
    assert fully_resumed["invocation_processed_sellers"] == 0


def test_scoped_sellers_rejects_error_row_behind_success_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(str(_make_config(tmp_path)))
    input_path = (
        tmp_path
        / "data/marts/wb_four_region/plan/run/products_for_sellers.csv"
    )
    _write_products_csv(
        input_path,
        [
            {
                "run_id": "regional-run",
                "query": "q1",
                "query_group": "pack",
                "nmId": "11",
                "supplier_id": "1001",
                "supplier_name": "A",
                "status": "success",
            }
        ],
    )
    db = StateDB(tmp_path / "state/wb_four_region/plan/run/sellers.sqlite")
    db.init_schema()
    ctx = RunContext(
        run_id="20260726_001600Z",
        pipeline="wb_four_region_nightly",
        component="sellers_regional",
        started_at_utc=utc_now_iso(),
    )
    scope = SellersRunScope(
        input_products_path=input_path,
        raw_dir=tmp_path / "data/raw/sellers_scoped/plan/run",
        staging_dir=tmp_path / "data/staging/sellers_scoped/plan/run",
        mart_dir=tmp_path / "data/marts/sellers_scoped/plan/run",
        checkpoint_component="sellers_regional:plan",
    )
    monkeypatch.setattr(
        SellersEngine,
        "_build_session",
        lambda self: nullcontext(object()),
    )
    monkeypatch.setattr(
        SellersEngine,
        "_fetch_seller",
        lambda self, session, seller_id: (
            200,
            _success_payload(seller_id),
            "",
            "",
        ),
    )
    first = SellersEngine(
        config=config,
        db=db,
        ctx=ctx,
        run_scope=scope,
    ).run()
    mart_path = Path(str(first["mart_sellers_path"]))
    rows = read_csv_rows(mart_path)
    rows[0]["status"] = "error"
    write_csv_rows(mart_path, rows, list(rows[0]))

    with pytest.raises(
        CriticalPipelineError,
        match="checkpoint does not match successful mart row",
    ):
        SellersEngine(
            config=config,
            db=db,
            ctx=ctx,
            run_scope=scope,
        ).run()
