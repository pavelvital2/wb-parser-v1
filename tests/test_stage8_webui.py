from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.common.run_context import utc_now_iso
from app.common.state_db import StateDB
from app.webui.app import create_app


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
