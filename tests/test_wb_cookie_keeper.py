from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path


def _load_keeper():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wb_cookie_keeper.py"
    spec = importlib.util.spec_from_file_location("wb_cookie_keeper", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_nested_promo_products_is_preflight_ok() -> None:
    keeper = _load_keeper()
    assert "nested_promo_products" in keeper.OK_KINDS


def test_proxy_helpers_use_env_and_prepare_requests_and_playwright(monkeypatch) -> None:
    keeper = _load_keeper()
    monkeypatch.setenv("TEST_WB_PROXY_URL", "http://user:pa%24%24@proxy.local:3128")
    config = {"serp": {"proxy_url_env": "TEST_WB_PROXY_URL", "proxy_url": "http://ignored.local:8080"}}

    proxy_url = keeper.resolve_proxy_url(config)

    assert proxy_url == "http://user:pa%24%24@proxy.local:3128"
    assert keeper.requests_proxies(proxy_url) == {"http": proxy_url, "https": proxy_url}
    assert keeper.playwright_proxy_config(proxy_url) == {
        "server": "http://proxy.local:3128",
        "username": "user",
        "password": "pa$$",
    }


def test_ensure_keeps_existing_cookie_when_refresh_smoke_fails(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state="",
        storage_state_out="",
    )

    def fake_smoke(config, smoke_args, emit=True):
        return Path(smoke_args.cookie_file) != cookie_path and False

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[]}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(keeper, "smoke", fake_smoke)
    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: True)

    assert keeper.ensure({}, args) is False
    assert cookie_path.read_text(encoding="utf-8") == "old_cookie=1\n"


def test_renew_promotes_temp_cookie_after_smoke_success(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")
    storage_state = tmp_path / "storage_state.json"
    storage_state.write_text('{"cookies":[]}\n', encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state=str(storage_state),
        storage_state_out="",
    )

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[{"name":"ok"}]}\n', encoding="utf-8")
        return True

    def fake_smoke(config, smoke_args, emit=True):
        return Path(smoke_args.cookie_file).read_text(encoding="utf-8") == "new_cookie=1\n"

    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "smoke", fake_smoke)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: True)

    assert keeper.renew({}, args) is True
    assert cookie_path.read_text(encoding="utf-8") == "new_cookie=1\n"
    assert storage_state.read_text(encoding="utf-8") == '{"cookies":[{"name":"ok"}]}\n'


def test_renew_keeps_existing_cookie_when_html_smoke_fails(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")
    storage_state = tmp_path / "storage_state.json"
    storage_state.write_text('{"cookies":[]}\n', encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state=str(storage_state),
        storage_state_out="",
    )

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[{"name":"weak"}]}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "smoke", lambda config, smoke_args, emit=True: True)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: False)

    assert keeper.renew({}, args) is False
    assert cookie_path.read_text(encoding="utf-8") == "old_cookie=1\n"
    assert storage_state.read_text(encoding="utf-8") == '{"cookies":[]}\n'


def test_smoke_uses_fallback_urls_and_min_successes(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")
    queries_path = tmp_path / "exports" / "queries.txt"
    queries_path.parent.mkdir(parents=True)
    queries_path.write_text("q1\nq2\n", encoding="utf-8")

    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://internal.example/search",
            "fallback_base_urls": ["https://fallback.example/search"],
            "smoke_min_successes": 1,
            "proxy_url": "http://proxy.local:3128",
            "input_files": {"queries_txt": str(queries_path)},
            "request_params": {},
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        query="",
        sample_count=2,
        min_successes=0,
        page=1,
    )

    class Response:
        def __init__(self, status_code: int, payload=None, text: str = "") -> None:
            self.status_code = status_code
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    calls: list[tuple[str, str, dict[str, str] | None]] = []

    def fake_get(url, *, params, headers, timeout, proxies=None):
        calls.append((url, params["query"], proxies))
        if url == "https://fallback.example/search" and params["query"] == "q1":
            return Response(200, {"products": [{"name": "ok"}]})
        return Response(498, text="blocked")

    monkeypatch.setattr(keeper.requests, "get", fake_get)

    assert keeper.smoke(config, args, emit=False) is True
    expected_proxy = {"http": "http://proxy.local:3128", "https": "http://proxy.local:3128"}
    assert calls == [
        ("https://internal.example/search", "q1", expected_proxy),
        ("https://fallback.example/search", "q1", expected_proxy),
        ("https://internal.example/search", "q2", expected_proxy),
        ("https://fallback.example/search", "q2", expected_proxy),
    ]
