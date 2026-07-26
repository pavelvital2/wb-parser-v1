from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def _enable_proxy(monkeypatch, url: str = "http://proxy.local:3128") -> None:
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.setenv("PARSER_WB_PROXY_URL", url)


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
    _enable_proxy(monkeypatch)
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


def test_runtime_request_headers_file_merges_without_cookie(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    headers_path = tmp_path / "headers.json"
    headers_path.write_text(
        '{"authorization":"Bearer runtime","deviceid":"device-1","cookie":"stale=1"}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("TEST_WB_HEADERS_FILE", str(headers_path))
    config = {
        "serp": {
            "request_headers_file_env": "TEST_WB_HEADERS_FILE",
            "request_headers": {"x-queryid": "base"},
        }
    }

    keeper.inject_runtime_request_headers(config, tmp_path)

    assert keeper.request_headers_from_config(config) == {
        "x-queryid": "base",
        "authorization": "Bearer runtime",
        "deviceid": "device-1",
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
    _enable_proxy(monkeypatch)
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
            "request_headers": {
                "authorization": "Bearer token",
                "deviceid": "device-1",
                "cookie": "stale=1",
            },
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

    calls: list[tuple[str, str, dict[str, str] | None, dict[str, str]]] = []

    def fake_get(session, url, *, params, headers, timeout):
        calls.append((url, params["query"], dict(session.proxies), headers))
        if url == "https://fallback.example/search" and params["query"] == "q1":
            return Response(200, {"products": [{"name": "ok"}]})
        return Response(498, text="blocked")

    monkeypatch.setattr(keeper.requests.Session, "get", fake_get)

    assert keeper.smoke(config, args, emit=False) is True
    expected_proxy = {"http": "http://proxy.local:3128", "https": "http://proxy.local:3128"}
    assert [(url, query, proxy) for url, query, proxy, _headers in calls] == [
        ("https://internal.example/search", "q1", expected_proxy),
        ("https://fallback.example/search", "q1", expected_proxy),
        ("https://internal.example/search", "q2", expected_proxy),
        ("https://fallback.example/search", "q2", expected_proxy),
    ]
    assert all(headers["authorization"] == "Bearer token" for *_prefix, headers in calls)
    assert all(headers["deviceid"] == "device-1" for *_prefix, headers in calls)
    assert all(headers["cookie"] == "cookie=1" for *_prefix, headers in calls)


def test_smoke_can_check_fallback_without_cookie(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    _enable_proxy(monkeypatch)
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")

    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://fallback.example/search",
            "input_files": {"queries_txt": str(tmp_path / "missing.txt")},
            "request_params": {},
            "request_headers": {"authorization": "Bearer token"},
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        query="q1",
        sample_count=1,
        min_successes=1,
        page=1,
        without_cookie=True,
    )

    class Response:
        status_code = 200
        text = ""

        def json(self):
            return {"products": [{"name": "ok"}]}

    seen_headers: list[dict[str, str]] = []

    def fake_get(_session, url, *, params, headers, timeout):
        seen_headers.append(headers)
        return Response()

    monkeypatch.setattr(keeper.requests.Session, "get", fake_get)

    assert keeper.smoke(config, args, emit=False) is True
    assert seen_headers
    assert "cookie" not in seen_headers[0]
    assert seen_headers[0]["authorization"] == "Bearer token"


def test_smoke_without_proxy_fails_before_requests_call(
    tmp_path: Path,
    monkeypatch,
) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")
    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://search.example.test",
            "request_params": {},
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json="",
        query="q1",
        sample_count=1,
        min_successes=1,
        page=1,
    )
    calls = 0

    def forbidden_get(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("network must not be called")

    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)
    monkeypatch.setattr(keeper.requests.Session, "get", forbidden_get)

    with pytest.raises(Exception, match="marketplace_proxy_env_missing"):
        keeper.smoke(config, args, emit=False)
    assert calls == 0


def _refresh_args(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        cookie_file=str(tmp_path / "wb_cookie.txt"),
        storage_state=str(tmp_path / "missing-storage-state.json"),
        storage_state_out=str(tmp_path / "storage-state-out.json"),
        state_json="",
        browser_channel="chrome",
        no_headless=False,
        headed=False,
        require_storage_state=False,
        refresh_url="https://www.wildberries.ru/",
        timeout_ms=1000,
        wait_ms=0,
    )


def test_refresh_without_proxy_fails_before_playwright_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keeper = _load_keeper()
    calls = 0
    sync_api = ModuleType("playwright.sync_api")

    def forbidden_playwright():
        nonlocal calls
        calls += 1
        raise AssertionError("browser must not start")

    sync_api.sync_playwright = forbidden_playwright  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)

    with pytest.raises(Exception, match="marketplace_proxy_env_missing"):
        keeper.refresh(
            {"serp": {"proxy_url_env": "PARSER_WB_PROXY_URL"}},
            _refresh_args(tmp_path),
        )

    assert calls == 0


def test_refresh_passes_explicit_proxy_to_browser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keeper = _load_keeper()
    _enable_proxy(
        monkeypatch,
        "http://user:test-only@proxy.example.test:8080",
    )
    captured: dict[str, object] = {}
    sync_api = ModuleType("playwright.sync_api")

    class Chromium:
        @staticmethod
        def launch(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after launch contract check")

    class Playwright:
        chromium = Chromium()

    class ContextManager:
        def __enter__(self):
            return Playwright()

        def __exit__(self, *_args):
            return False

    sync_api.sync_playwright = lambda: ContextManager()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)

    assert (
        keeper.refresh(
            {"serp": {"proxy_url_env": "PARSER_WB_PROXY_URL"}},
            _refresh_args(tmp_path),
        )
        is False
    )
    assert captured["proxy"] == {
        "server": "http://proxy.example.test:8080",
        "username": "user",
        "password": "test-only",
    }
