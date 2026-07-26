from __future__ import annotations

import argparse
import importlib.util
import stat
from pathlib import Path


def _load_preflight():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wb_nightly_preflight.py"
    spec = importlib.util.spec_from_file_location("wb_nightly_preflight", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _args(tmp_path: Path, cookie_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        cookie_file=str(cookie_path),
        backup_dir=str(tmp_path / "known_good"),
        retain=5,
        query="",
        sample_count=3,
        min_successes=0,
        page=1,
        state_json=str(tmp_path / "state.json"),
        keeper_state_json=str(tmp_path / "keeper_state.json"),
        no_refresh=True,
        storage_state="",
        storage_state_out="",
        require_storage_state=False,
        browser_channel="",
        headed=False,
        no_headless=False,
        refresh_url="https://www.wildberries.ru/",
        wait_ms=1,
        timeout_ms=1,
    )


def test_backup_current_saves_known_good_cookie_with_0600_mode(tmp_path: Path, monkeypatch) -> None:
    preflight = _load_preflight()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=good\n", encoding="utf-8")
    args = _args(tmp_path, cookie_path)

    monkeypatch.setattr(preflight, "smoke_cookie", lambda config, smoke_args, path: True)

    assert preflight.backup_current({}, args) == 0
    backups = list((tmp_path / "known_good").glob("wb_cookie.known_good_*.txt"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == "cookie=good\n"
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600


def test_preflight_restores_latest_smoke_ok_known_good(tmp_path: Path, monkeypatch) -> None:
    preflight = _load_preflight()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=bad\n", encoding="utf-8")
    args = _args(tmp_path, cookie_path)
    backup_dir = tmp_path / "known_good"
    backup_dir.mkdir()
    old_backup = backup_dir / "wb_cookie.known_good_20260101T000000Z.txt"
    old_backup.write_text("cookie=old\n", encoding="utf-8")
    good_backup = backup_dir / "wb_cookie.known_good_20260102T000000Z.txt"
    good_backup.write_text("cookie=good\n", encoding="utf-8")

    def fake_smoke(config, smoke_args, path):
        return Path(path).read_text(encoding="utf-8") == "cookie=good\n"

    monkeypatch.setattr(preflight, "smoke_cookie", fake_smoke)

    assert preflight.preflight({}, args) == 0
    assert cookie_path.read_text(encoding="utf-8") == "cookie=good\n"
