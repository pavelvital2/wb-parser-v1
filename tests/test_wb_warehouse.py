from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path


def load_module(project_root: Path):
    spec = importlib.util.spec_from_file_location(
        "wb_warehouse", project_root / "scripts" / "wb_warehouse.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def install_scripts(project: Path) -> None:
    scripts_dir = project / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[1]
    for name in ("wb_warehouse.py", "run_wb_warehouse_refresh.sh"):
        source = root / "scripts" / name
        target = scripts_dir / name
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        target.chmod(0o755)


def test_wb_warehouse_builds_duckdb_and_views(tmp_path: Path):
    project = tmp_path
    install_scripts(project)

    write(
        project / "data/marts/serp/20260704_211504Z/products_daily.csv",
        "run_id;component;collected_at_utc;query;query_group;page;position_on_page;absolute_position;nmId;imtId;product_name;brand;brandId;supplier_id;supplier_name;final_price;price;sale_price;discount;rating;feedbacks;total_quantity;status\n"
        "20260704_211504Z;serp;2026-07-04T21:15:05+00:00;шеврон;;1;1;1;111;;Товар;Бренд;7;42;Продавец;100;200;100;50;4.9;10;5;success\n",
    )
    write(
        project / "data/marts/serp/latest/products_daily.csv",
        "run_id;component;collected_at_utc;query;query_group;page;position_on_page;absolute_position;nmId;imtId;product_name;brand;brandId;supplier_id;supplier_name;final_price;price;sale_price;discount;rating;feedbacks;total_quantity;status\n"
        "latest_duplicate;serp;2026-07-04T21:15:05+00:00;дубликат;;1;1;1;999;;Товар;Бренд;7;42;Продавец;100;200;100;50;4.9;10;5;success\n",
    )
    write(
        project / "data/marts/sellers/20260704_214329Z/sellers_daily.csv",
        "run_id;component;collected_at_utc;supplier_id;supplier_name;rating;valuation;feedbacks_count;sale_item_quantity;query_count;product_count;queries_ref;nm_ids_ref;source_product_run_ids;status\n"
        "20260704_214329Z;sellers;2026-07-04T21:43:30+00:00;42;Продавец;99;4.8;12;100;1;1;шеврон;111;20260704_211504Z;success\n",
    )
    write(
        project / "data/marts/sellers/20260704_214329Z/seller_query_product_bridge.csv",
        "run_id;component;collected_at_utc;supplier_id;supplier_name;query;query_group;nmId;product_run_id;status\n"
        "20260704_214329Z;sellers;2026-07-04T21:43:30+00:00;42;Продавец;шеврон;;111;20260704_211504Z;success\n",
    )
    write(
        project / "data/raw/serp/20260704_211504Z/pages_raw_index.csv",
        "run_id;component;collected_at_utc;query;query_group;page;http_status;products_count;status\n"
        "20260704_211504Z;serp;2026-07-04T21:15:05+00:00;шеврон;;1;200;100;success\n",
    )
    report = {
        "run_id": "20260704_214329Z",
        "pipeline": "sellers",
        "job_id": "job-1",
        "status": "success",
        "started_at_utc": "2026-07-04T21:43:29+00:00",
        "finished_at_utc": "2026-07-04T21:48:23+00:00",
        "duration_seconds": 294,
        "totals": {"items_ok": 1, "items_error": 0},
        "components": [{"component": "sellers", "status": "success", "items_ok": 1, "items_error": 0}],
    }
    write(project / "state/run_reports/20260704_214329Z.json", json.dumps(report, ensure_ascii=False))
    latest_report = dict(report)
    latest_report["run_id"] = "latest_duplicate"
    write(project / "state/run_reports/latest.json", json.dumps(latest_report, ensure_ascii=False))

    module = load_module(project)
    dry_run = module.build(project, dry_run=True)
    assert dry_run["files"]["product_snapshots"] == 1
    assert dry_run["files"]["run_reports"] == 1
    assert dry_run["would_write"]["manifest_path"].endswith("manifests/latest.json")

    manifest = module.build(project)
    assert manifest["rows"]["product_snapshots"] == 1
    assert manifest["rows"]["seller_snapshots"] == 1
    assert manifest["rows"]["product_seller_bridge"] == 1
    assert manifest["rows"]["serp_pages"] == 1
    assert manifest["rows"]["run_reports"] == 1
    assert manifest["rows"]["run_report_components"] == 1
    assert manifest["files"]["product_snapshots"] == 1
    assert manifest["files"]["run_reports"] == 1
    assert "Ozon" in " ".join(manifest["limitations"])
    assert (project / "data/warehouse/wb/wb.duckdb").exists()
    for table in module.WAREHOUSE_TABLES:
        assert (project / "data/warehouse/wb/parquet" / f"{table}.parquet").exists()
    assert (project / "data/warehouse/wb/manifests/latest.json").exists()

    checked = module.check(project)
    assert checked["status"] == "ok"
    assert checked["rows"]["product_snapshots"] == 1
    assert checked["rows"]["run_report_components"] == 1
    assert checked["sample"]["top_queries"] == [("шеврон", 1)]
    assert module.run_sql(project, "select count(*) from query_positions") == [(1,)]


def test_wb_warehouse_refresh_wrapper_dry_run_writes_state(tmp_path: Path):
    project = tmp_path
    install_scripts(project)
    report = {
        "run_id": "20260704_214329Z",
        "pipeline": "sellers",
        "status": "success",
        "components": [{"component": "sellers", "status": "success"}],
    }
    write(project / "state/run_reports/latest.json", json.dumps(report, ensure_ascii=False))
    env = {
        **os.environ,
        "PARSER_WB_PROJECT_DIR": str(project),
        "PARSER_WB_PYTHON_BIN": sys.executable,
    }

    result = subprocess.run(
        ["bash", str(project / "scripts/run_wb_warehouse_refresh.sh"), "--dry-run"],
        cwd=project,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    state = json.loads((project / "state/wb_warehouse/latest.json").read_text(encoding="utf-8"))
    assert state["status"] == "success"
    assert state["reason"] == "dry_run_ok"
    assert state["dry_run"] is True
    assert state["run_report"]["run_id"] == "20260704_214329Z"
    assert state["warehouse"]["database_path"].endswith("data/warehouse/wb/wb.duckdb")
    assert not (project / "data/warehouse/wb/wb.duckdb").exists()


def test_wb_warehouse_refresh_wrapper_skips_failed_latest_report(tmp_path: Path):
    project = tmp_path
    install_scripts(project)
    report = {
        "run_id": "failed_run",
        "pipeline": "sellers",
        "status": "failed",
        "components": [{"component": "sellers", "status": "failed"}],
    }
    write(project / "state/run_reports/latest.json", json.dumps(report, ensure_ascii=False))
    env = {
        **os.environ,
        "PARSER_WB_PROJECT_DIR": str(project),
        "PARSER_WB_PYTHON_BIN": sys.executable,
    }

    result = subprocess.run(
        ["bash", str(project / "scripts/run_wb_warehouse_refresh.sh"), "--dry-run"],
        cwd=project,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    state = json.loads((project / "state/wb_warehouse/latest.json").read_text(encoding="utf-8"))
    assert state["status"] == "skipped"
    assert state["reason"] == "latest_report_not_success"
    assert state["run_report"]["run_id"] == "failed_run"
    assert not (project / "data/warehouse/wb/wb.duckdb").exists()
