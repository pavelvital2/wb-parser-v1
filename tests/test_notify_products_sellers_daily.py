from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


def _load_notify():
    path = Path(__file__).resolve().parents[1] / "scripts" / "notify_products_sellers_daily.py"
    spec = importlib.util.spec_from_file_location("notify_products_sellers_daily", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def configure_paths(notify, project: Path) -> None:
    notify.PRODUCTS_FILE = project / "data/marts/serp/latest/products_daily.csv"
    notify.SERP_PAGES_FILE = project / "data/raw/serp/latest/pages_raw_index.csv"
    notify.SELLERS_FILE = project / "data/marts/sellers/latest/sellers_daily.csv"
    notify.BRIDGE_FILE = project / "data/marts/sellers/latest/seller_query_product_bridge.csv"
    notify.QUERIES_FILE = project / "exports/queries.txt"
    notify.WAREHOUSE_STATE_FILE = project / "state/wb_warehouse/latest.json"
    notify.RUN_REPORT_FILE = project / "state/run_reports/latest.json"
    notify.RUN_REPORTS_DIR = project / "state/run_reports"
    notify.KEEPER_STATE_FILE = project / "state/wb_session_keeper/latest.json"
    notify.PREFLIGHT_STATE_FILE = project / "state/wb_nightly_preflight/latest.json"
    notify.WATCHDOG_STATE_FILE = project / "state/wb_persistent_session/watchdog.json"
    notify.PERSISTENT_SESSION_STATE_FILE = project / "state/wb_persistent_session/latest.json"


def test_health_lines_summarize_local_runtime_state(tmp_path: Path) -> None:
    notify = _load_notify()
    configure_paths(notify, tmp_path)
    write(
        notify.SERP_PAGES_FILE,
        "run_id;status;query;http_status\n"
        "serp_run;success;q1;200\n"
        "serp_run;error;q1;498\n"
        "serp_run;error;q2;429\n",
    )
    write(notify.PRODUCTS_FILE, "run_id;name\nserp_run;product\n")
    write(notify.SELLERS_FILE, "run_id;supplier_id\nsellers_run;42\n")
    write(
        notify.KEEPER_STATE_FILE,
        json.dumps({"status": "ok", "checked_at_utc": "2026-07-05T00:00:00+00:00", "successes": 3, "min_successes": 2}),
    )
    write(
        notify.PREFLIGHT_STATE_FILE,
        json.dumps({"status": "ok", "checked_at_utc": "2026-07-04T21:15:02+00:00", "actions": ["current_cookie_smoke_ok", "known_good_saved"]}),
    )
    write(
        notify.RUN_REPORT_FILE,
        json.dumps({"pipeline": "sellers", "status": "success", "run_id": "sellers_run", "duration_seconds": 294}),
    )
    write(
        notify.RUN_REPORTS_DIR / "serp_run.json",
        json.dumps(
            {
                "pipeline": "serp",
                "status": "success",
                "run_id": "serp_run",
                "components": [
                    {
                        "component": "serp",
                        "note": (
                            "queries=1 pages=1 ok=1 err=0 published=1 "
                            "ip_rotations=1 ip_rotation_attempts=2 "
                            "ip_rotation_successes=1 ip_rotation_failures=1 "
                            "ip_rotation_last_change=203.0.x.x->198.51.x.x "
                            "ip_rotation_last_reason=http_status=429 "
                            "ip_rotation_last_scope=page=1"
                        ),
                    }
                ],
            }
        ),
    )
    write(
        notify.WATCHDOG_STATE_FILE,
        json.dumps({"action": "disabled", "reason": "disabled_by_env"}),
    )
    write(
        notify.PERSISTENT_SESSION_STATE_FILE,
        json.dumps({"status": "failed", "http_status": 498, "antibot": True}),
    )

    text = "\n".join(notify.health_lines())

    assert "WB API smoke" in text
    assert "3/2" in text
    assert "SERP latest" in text
    assert "pages=3" in text
    assert "429=1" in text
    assert "498=1" in text
    assert "Proxy rotation" in text
    assert "attempted=2" in text
    assert "succeeded=1" in text
    assert "failed=1" in text
    assert "203.0.x.x-&gt;198.51.x.x" in text
    assert "products_run=serp_run" in text
    assert "sellers_run=sellers_run" in text
    assert "disabled_by_env" in text


def test_proxy_rotation_uses_current_failed_serp_report_over_stale_latest_products(tmp_path: Path) -> None:
    notify = _load_notify()
    configure_paths(notify, tmp_path)
    write(notify.PRODUCTS_FILE, "run_id;product_id\nold_serp;1\n")
    write(
        notify.RUN_REPORTS_DIR / "old_serp.json",
        json.dumps(
            {
                "pipeline": "serp",
                "status": "success",
                "components": [{"component": "serp", "note": "ip_rotation_attempts=0 ip_rotation_successes=0"}],
            }
        ),
    )
    write(
        notify.RUN_REPORT_FILE,
        json.dumps(
            {
                "pipeline": "serp",
                "status": "failed",
                "run_id": "current_failed_serp",
                "components": [
                    {
                        "component": "serp",
                        "note": (
                            "ip_rotation_attempts=1 ip_rotation_successes=0 "
                            "ip_rotation_failures=1 ip_rotation_last_reason=http_status=429"
                        ),
                    }
                ],
            }
        ),
    )

    label = notify.proxy_rotation_health_label()

    assert "attempted=1" in label
    assert "succeeded=0" in label
    assert "failed=1" in label
    assert "reason=http_status=429" in label


def test_build_message_includes_health_section(tmp_path: Path) -> None:
    notify = _load_notify()
    configure_paths(notify, tmp_path)
    write(notify.PRODUCTS_FILE, "run_id;name\nserp_run;product\n")
    write(notify.SELLERS_FILE, "run_id;supplier_id\nsellers_run;42\n")
    write(notify.BRIDGE_FILE, "run_id;supplier_id\nsellers_run;42\n")
    write(notify.SERP_PAGES_FILE, "run_id;status;query;http_status\nserp_run;success;q1;200\n")
    write(notify.QUERIES_FILE, "q1\n")
    write(
        notify.WAREHOUSE_STATE_FILE,
        json.dumps({"status": "success", "reason": "refresh_ok", "warehouse": {"rows": {"product_snapshots": 10}}}),
    )
    args = argparse.Namespace(
        status=0,
        run_stamp="stamp",
        started_at="2026-07-05T00:15:00+03:00",
        finished_at="2026-07-05T00:48:00+03:00",
        log_path=str(tmp_path / "log.txt"),
        phase="run",
    )

    message = notify.build_message(args)

    assert "<b>Health</b>" in message
    assert "SERP latest" in message
    assert "Warehouse: <b>success (refresh_ok), product_snapshots=10</b>" in message
