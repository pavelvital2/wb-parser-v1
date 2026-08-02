from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.common import cli as wb_cli
from app.common.nightly_coordinator import NightlyCoordinatorContractError
from app.common.run_context import utc_now_iso
from app.common.state_db import StateDB
from app.webui.app import create_app
from app.webui import services as webui_services


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test-webui
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
  retry_base_delay_seconds: 0.0
  retry_max_delay_seconds: 0.0
  http_timeout_seconds: 5
  dry_run: false
  debug: false

suggest:
  prefixes_file: config/prefixes.txt

filter:
  rules_file: config/query_rules.yaml

webui:
  host: 127.0.0.1
  port: 8080
  admin_username: admin
  admin_password_env: WEBUI_ADMIN_PASSWORD
  secret_key_env: WEBUI_SECRET_KEY
"""
    cfg = tmp_path / "config" / "config.yaml"
    _write_text(cfg, config_yaml)
    _write_text(tmp_path / "config" / "prefixes.txt", "шеврон\n")
    _write_text(tmp_path / "config" / "query_rules.yaml", "default:\n  top_n: 10\n")
    _write_text(tmp_path / "data" / "logs" / "app.log", "line1\nline2\n")
    _write_text(tmp_path / "exports" / "sample.txt", "ok")
    return cfg


def _login(client: TestClient, username: str = "admin", password: str = "secret") -> None:
    r = client.post("/login", data={"username": username, "password": password}, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers.get("location") == "/"


def test_login_page(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")

    app = create_app(config_path=str(cfg))
    client = TestClient(app)

    r = client.get("/login")
    assert r.status_code == 200
    assert "Login" in r.text


def test_protected_page_requires_login(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")

    app = create_app(config_path=str(cfg))
    client = TestClient(app)

    r = client.get("/runs", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers.get("location") == "/login"


def test_login_success_and_runs_page(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")

    app = create_app(config_path=str(cfg))
    db = StateDB(app.state.config.paths.SQLITE_DB)
    db.init_schema()
    db.create_run("20260307_120000Z", "serp", "", utc_now_iso())
    db.finish_run("20260307_120000Z", "success", utc_now_iso(), items_ok=1, items_error=0)

    client = TestClient(app)
    _login(client)

    r = client.get("/runs")
    assert r.status_code == 200
    assert "20260307_120000Z" in r.text


def test_files_page(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")

    app = create_app(config_path=str(cfg))
    client = TestClient(app)
    _login(client)

    r = client.get("/files?root=exports")
    assert r.status_code == 200
    assert "sample.txt" in r.text


def test_config_save_prefixes(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")
    monkeypatch.setattr(
        webui_services,
        "require_official_live_entry_lease",
        lambda: 1,
    )

    app = create_app(config_path=str(cfg))
    client = TestClient(app)
    _login(client)

    r = client.post("/config/prefixes", data={"content": "патч\nнашивка\n"}, follow_redirects=False)
    assert r.status_code == 303

    saved = (tmp_path / "config" / "prefixes.txt").read_text(encoding="utf-8")
    assert "патч" in saved and "нашивка" in saved


def test_action_endpoint_starts_task(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")

    launched: dict[str, object] = {}

    class DummyProc:
        pid = 99999

    def _fake_popen(cmd, cwd=None, stdout=None, stderr=None):
        launched["cmd"] = cmd
        launched["cwd"] = cwd
        return DummyProc()

    monkeypatch.setattr("app.webui.services.subprocess.Popen", _fake_popen)

    app = create_app(config_path=str(cfg))
    client = TestClient(app)
    _login(client)

    r = client.post("/actions/run", data={"target": "serp"}, follow_redirects=False)
    assert r.status_code == 303
    assert "run" in " ".join(str(x) for x in launched.get("cmd", []))
    assert "serp" in " ".join(str(x) for x in launched.get("cmd", []))


def test_webui_mutations_require_host_lease_after_cutover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")
    app = create_app(config_path=str(cfg))
    services = app.state.webui_services
    prefixes = tmp_path / "config/prefixes.txt"
    before = prefixes.read_bytes()

    def refuse() -> None:
        raise NightlyCoordinatorContractError(
            "official_live_entry_requires_lock_v3",
            outcome="hard_failure",
        )

    monkeypatch.setattr(
        webui_services,
        "require_official_live_entry_lease",
        refuse,
    )
    with pytest.raises(NightlyCoordinatorContractError):
        services.save_text_config("prefixes", "changed\n")
    with pytest.raises(NightlyCoordinatorContractError):
        services.save_wordstat_upload("input.csv", b"changed\n")
    assert prefixes.read_bytes() == before
    assert not (tmp_path / "data/raw/wordstat/input.csv").exists()


def test_all_webui_actions_use_reviewed_live_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_config(tmp_path)
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")
    app = create_app(config_path=str(cfg))
    captured: list[list[str]] = []

    class DummyProc:
        pid = 12345

    monkeypatch.setattr(
        webui_services.subprocess,
        "Popen",
        lambda command, **_kwargs: (
            captured.append(list(command)) or DummyProc()
        ),
    )
    ok, _message = app.state.webui_services.start_action(
        target="filter",
        user="admin",
    )
    assert ok is True
    assert captured == [
        [
            str(tmp_path / "scripts/run_wb_live_component.sh"),
            "filter",
            "--job-id",
            "webui_admin",
        ]
    ]


def test_read_only_cli_never_migrates_legacy_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_config(tmp_path)
    db_path = tmp_path / "state/sqlite/state.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                component TEXT NOT NULL,
                job_id TEXT,
                status TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                items_ok INTEGER NOT NULL DEFAULT 0,
                items_error INTEGER NOT NULL DEFAULT 0,
                critical_errors INTEGER NOT NULL DEFAULT 0,
                non_critical_errors INTEGER NOT NULL DEFAULT 0,
                note TEXT
            );
            CREATE TABLE tasks (id INTEGER PRIMARY KEY);
            CREATE TABLE errors (
                id INTEGER PRIMARY KEY,
                error_class TEXT
            );
            CREATE TABLE checkpoints (
                component TEXT,
                checkpoint_key TEXT
            );
            INSERT INTO runs(
                run_id, component, status, created_at_utc
            ) VALUES(
                '20260727_000000Z', 'serp', 'success',
                '2026-07-27T00:00:00Z'
            );
            """
        )
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    monkeypatch.setenv("WEBUI_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("WEBUI_SECRET_KEY", "secret-key")
    assert wb_cli.cmd_runs(str(cfg), 5) == 0
    assert wb_cli.cmd_doctor(str(cfg)) in {0, 1}
    after = hashlib.sha256(db_path.read_bytes()).hexdigest()
    assert after == before
    with sqlite3.connect(db_path) as conn:
        run_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(runs)")
        }
        error_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(errors)")
        }
    assert "pipeline" not in run_columns
    assert "error_code" not in error_columns
