from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_session():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wb_persistent_session.py"
    spec = importlib.util.spec_from_file_location("wb_persistent_session", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_android_context_options_are_mobile_and_isolated_from_defaults() -> None:
    session = _load_session()

    options = session.android_context_options()

    assert options["is_mobile"] is True
    assert options["has_touch"] is True
    assert "Android" in options["user_agent"]
    assert options["locale"] == "ru-RU"
    assert options["timezone_id"] == "Europe/Moscow"
    assert options["extra_http_headers"]["sec-ch-ua-mobile"] == "?1"


def test_extract_public_ip_accepts_json_and_plain_text() -> None:
    session = _load_session()

    assert session._extract_public_ip('{"ip":"203.0.113.10"}') == "203.0.113.10"
    assert session._extract_public_ip("2001:db8::1\n") == "2001:db8::1"
    assert session._extract_public_ip("") == ""
    assert session._extract_public_ip("not an ip with spaces") == ""


def test_fetch_public_ip_uses_proxy_without_returning_proxy_secret(monkeypatch) -> None:
    session = _load_session()
    calls = []

    class Response:
        status_code = 200
        text = '{"ip":"203.0.113.20"}'

    def fake_get(url, *, timeout, proxies):
        calls.append({"url": url, "timeout": timeout, "proxies": proxies})
        return Response()

    monkeypatch.setattr(session.keeper.requests, "get", fake_get)
    monkeypatch.setattr(session.keeper, "resolve_proxy_url", lambda _config: "http://user:secret@example.test:8080")
    monkeypatch.setenv("PARSER_WB_PUBLIC_IP_URLS", "https://ip.example.test")

    public_ip, error = session.fetch_public_ip({})

    assert public_ip == "203.0.113.20"
    assert error == ""
    assert calls[0]["proxies"] == {
        "http": "http://user:secret@example.test:8080",
        "https": "http://user:secret@example.test:8080",
    }


def test_fetch_public_ip_falls_back_to_next_endpoint(monkeypatch) -> None:
    session = _load_session()
    calls = []

    class Response:
        status_code = 200
        text = "203.0.113.30\n"

    def fake_get(url, *, timeout, proxies):
        calls.append(url)
        if len(calls) == 1:
            raise TimeoutError("first endpoint timed out")
        return Response()

    monkeypatch.setenv("PARSER_WB_PUBLIC_IP_URLS", "https://first.example.test,https://second.example.test")
    monkeypatch.setattr(session.keeper.requests, "get", fake_get)
    monkeypatch.setattr(session.keeper, "resolve_proxy_url", lambda _config: "")

    public_ip, error = session.fetch_public_ip({})

    assert public_ip == "203.0.113.30"
    assert error == ""
    assert calls == ["https://first.example.test", "https://second.example.test"]
