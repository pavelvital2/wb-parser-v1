from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from app.common.proxy_required import (
    MarketplaceProxyError,
    build_requests_session,
    require_marketplace_proxy,
)
from app.sellers.engine import SellersEngine
from app.serp.collection_plan_runner import RequestsScopedTransport
from app.serp.engine import SerpEngine
from app.suggest import alpha as suggest_alpha


PROXY_URL = "http://user:test-only@proxy.example.test:8080"


def _enable_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.setenv("PARSER_WB_PROXY_URL", PROXY_URL)


def _raw_config() -> dict[str, Any]:
    return {
        "serp": {
            "proxy_url_env": "PARSER_WB_PROXY_URL",
            "base_url": "https://search.example.test",
            "fallback_base_urls": ["https://fallback.example.test"],
            "request_params": {},
            "cookie_required": False,
        }
    }


def _serp_engine() -> SerpEngine:
    engine = object.__new__(SerpEngine)
    engine.config = SimpleNamespace(raw=_raw_config())
    engine.user_agent = "test-agent"
    engine.x_requested_with = "XMLHttpRequest"
    engine.request_headers = {}
    engine.proxy_url = ""
    return engine


def _sellers_engine() -> SellersEngine:
    engine = object.__new__(SellersEngine)
    engine.config = SimpleNamespace(raw=_raw_config())
    engine.sellers_cfg = {"request_headers": {}}
    engine.user_agent = "test-agent"
    return engine


@pytest.mark.parametrize(
    "missing_name",
    [
        "PARSER_WB_RUNTIME_ENV_LOADED",
        "PARSER_WB_RUNTIME_ENV_SHA256",
        "PARSER_WB_PROXY_URL",
    ],
)
def test_proxy_guard_fails_closed_without_required_runtime_input(
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
) -> None:
    _enable_runtime(monkeypatch)
    monkeypatch.delenv(missing_name, raising=False)

    with pytest.raises(MarketplaceProxyError):
        require_marketplace_proxy(_raw_config())


@pytest.mark.parametrize(
    "proxy_url",
    [
        "not-a-url",
        "ftp://proxy.example.test:21",
        "http://proxy.example.test",
        "http://proxy.example.test:8080/path",
        "http://proxy.example.test:8080?secret=value",
        "http://proxy.example.test:99999",
    ],
)
def test_proxy_guard_rejects_invalid_urls_without_echoing_value(
    monkeypatch: pytest.MonkeyPatch,
    proxy_url: str,
) -> None:
    _enable_runtime(monkeypatch)
    monkeypatch.setenv("PARSER_WB_PROXY_URL", proxy_url)

    with pytest.raises(MarketplaceProxyError) as caught:
        require_marketplace_proxy(_raw_config())

    assert proxy_url not in str(caught.value)


def test_configured_requests_session_has_no_direct_environment_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_runtime(monkeypatch)
    route = require_marketplace_proxy(_raw_config())

    with build_requests_session(route) as session:
        assert session.trust_env is False
        assert session.proxies == {
            "http": PROXY_URL,
            "https": PROXY_URL,
        }
        assert getattr(session, "_wb_marketplace_proxy_sha256") == hashlib.sha256(
            PROXY_URL.encode("utf-8")
        ).hexdigest()


@pytest.mark.parametrize(
    "builder",
    [
        lambda: _serp_engine()._build_session(""),
        lambda: _sellers_engine()._build_session(),
    ],
)
def test_requests_collection_entrypoints_make_zero_session_without_proxy(
    monkeypatch: pytest.MonkeyPatch,
    builder,
) -> None:
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)
    created = 0

    def forbidden_session():
        nonlocal created
        created += 1
        raise AssertionError("Session must not be created")

    monkeypatch.setattr(requests, "Session", forbidden_session)
    with pytest.raises(MarketplaceProxyError, match="proxy_env_missing"):
        builder()
    assert created == 0


def test_serp_and_sellers_sessions_share_required_proxy_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_runtime(monkeypatch)

    sessions = [
        _serp_engine()._build_session("cookie=test"),
        _sellers_engine()._build_session(),
    ]
    try:
        assert all(session.trust_env is False for session in sessions)
        assert all(
            session.proxies == {"http": PROXY_URL, "https": PROXY_URL}
            for session in sessions
        )
    finally:
        for session in sessions:
            session.close()


class _SuggestPaths:
    def __init__(self, root: Path) -> None:
        self.root = root

    def output_path(
        self,
        *,
        layer: str,
        component: str,
        run_id: str,
        filename: str,
    ) -> Path:
        return self.root / "data" / layer / component / run_id / filename


class _SuggestDB:
    def delete_checkpoints(self, _component: str) -> None:
        pass

    def list_checkpoint_keys(self, _component: str) -> list[str]:
        return []


def _suggest_config(tmp_path: Path) -> SimpleNamespace:
    prefixes = tmp_path / "config/prefixes.txt"
    prefixes.parent.mkdir(parents=True)
    prefixes.write_text("шеврон\n", encoding="utf-8")
    return SimpleNamespace(
        raw={
            **_raw_config(),
            "project": {"source_system": "wildberries"},
            "suggest": {
                "prefixes_file": "config/prefixes.txt",
                "alphabet_mode": "ru30",
                "headless": True,
                "browser_channel": "chrome",
                "browser_profile_dir": "state/browser/test",
            },
        },
        project_root=tmp_path,
        runtime=SimpleNamespace(
            retry_max_attempts=1,
            retry_base_delay_seconds=0,
            retry_max_delay_seconds=0,
            http_timeout_seconds=1,
        ),
        paths=_SuggestPaths(tmp_path),
    )


def test_suggest_browser_fails_before_playwright_without_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _suggest_config(tmp_path)
    entered = False

    def forbidden_playwright():
        nonlocal entered
        entered = True
        raise AssertionError("Playwright must not be entered")

    monkeypatch.setattr(suggest_alpha, "sync_playwright", forbidden_playwright)
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)

    with pytest.raises(MarketplaceProxyError, match="proxy_env_missing"):
        suggest_alpha.run_suggest_collection(
            config,
            _SuggestDB(),
            SimpleNamespace(run_id="test"),
        )
    assert entered is False


def test_suggest_browser_context_receives_required_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _suggest_config(tmp_path)
    _enable_runtime(monkeypatch)
    captured: dict[str, Any] = {}

    class StopAfterLaunch(RuntimeError):
        pass

    class Chromium:
        def launch_persistent_context(self, **kwargs):
            captured.update(kwargs)
            raise StopAfterLaunch()

    class Playwright:
        chromium = Chromium()

    class Manager:
        def __enter__(self):
            return Playwright()

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(suggest_alpha, "sync_playwright", lambda: Manager())
    with pytest.raises(StopAfterLaunch):
        suggest_alpha.run_suggest_collection(
            config,
            _SuggestDB(),
            SimpleNamespace(run_id="test"),
        )

    assert captured["proxy"] == {
        "server": "http://proxy.example.test:8080",
        "username": "user",
        "password": "test-only",
    }


def test_collection_plan_transport_fails_before_session_without_proxy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)
    created = 0

    def forbidden_session():
        nonlocal created
        created += 1
        raise AssertionError("Session must not be created")

    monkeypatch.setattr(requests, "Session", forbidden_session)
    config = SimpleNamespace(
        raw=_raw_config(),
        runtime=SimpleNamespace(http_timeout_seconds=5),
        project_root=tmp_path,
    )
    with pytest.raises(MarketplaceProxyError, match="proxy_env_missing"):
        RequestsScopedTransport.from_config(config)
    assert created == 0


def test_collection_plan_resolver_search_and_egress_use_one_proxy_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _enable_runtime(monkeypatch)
    monkeypatch.setenv("PARSER_WB_COOKIE_REQUIRED", "0")
    config = SimpleNamespace(
        raw=_raw_config(),
        runtime=SimpleNamespace(http_timeout_seconds=5),
        project_root=tmp_path,
    )

    transport = RequestsScopedTransport.from_config(config)
    try:
        assert transport.egress_session is transport.session
        assert transport.session.trust_env is False
        assert transport.session.proxies == {
            "http": PROXY_URL,
            "https": PROXY_URL,
        }
    finally:
        transport.close()
