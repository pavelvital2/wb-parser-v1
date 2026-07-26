from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

import requests

from .exceptions import CriticalPipelineError


RUNTIME_ENV_LOADED_ENV = "PARSER_WB_RUNTIME_ENV_LOADED"
RUNTIME_ENV_SHA256_ENV = "PARSER_WB_RUNTIME_ENV_SHA256"
PROXY_EVIDENCE_SCHEMA_VERSION = "wb_marketplace_proxy_v1"
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,100}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUESTS_SCHEMES = frozenset({"http", "https", "socks5", "socks5h"})
_BROWSER_SCHEMES = frozenset({"http", "https", "socks5"})


class MarketplaceProxyError(CriticalPipelineError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class MarketplaceProxyRoute:
    url: str = field(repr=False)
    scheme: str
    sha256: str

    @property
    def requests_proxies(self) -> dict[str, str]:
        return {"http": self.url, "https": self.url}

    def playwright_proxy(self) -> dict[str, str]:
        if self.scheme not in _BROWSER_SCHEMES:
            raise MarketplaceProxyError("marketplace_proxy_browser_scheme_unsupported")
        parsed = urlsplit(self.url)
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        server = f"{parsed.scheme}://{host}:{parsed.port}"
        result = {"server": server}
        if parsed.username is not None:
            result["username"] = unquote(parsed.username)
        if parsed.password is not None:
            result["password"] = unquote(parsed.password)
        return result

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": PROXY_EVIDENCE_SCHEMA_VERSION,
            "status": "configured",
            "runtime_env_loaded": True,
            "runtime_env_sha256_present": True,
            "proxy_configured": True,
            "proxy_valid": True,
            "proxy_route_sha256": self.sha256,
        }


def _serp_config(raw_config: Mapping[str, Any]) -> Mapping[str, Any]:
    value = raw_config.get("serp")
    return value if isinstance(value, Mapping) else {}


def require_runtime_env_loaded() -> str:
    if os.getenv(RUNTIME_ENV_LOADED_ENV, "") != "1":
        raise MarketplaceProxyError("marketplace_runtime_env_not_loaded")
    runtime_sha256 = os.getenv(RUNTIME_ENV_SHA256_ENV, "")
    if not _SHA256_RE.fullmatch(runtime_sha256):
        raise MarketplaceProxyError("marketplace_runtime_env_provenance_invalid")
    return runtime_sha256


def proxy_route_from_url(
    proxy_url: str,
    *,
    browser: bool = False,
) -> MarketplaceProxyRoute:
    if (
        not isinstance(proxy_url, str)
        or not proxy_url
        or proxy_url != proxy_url.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in proxy_url)
    ):
        raise MarketplaceProxyError("marketplace_proxy_url_invalid")
    try:
        parsed = urlsplit(proxy_url)
        port = parsed.port
    except ValueError as exc:
        raise MarketplaceProxyError("marketplace_proxy_url_invalid") from exc
    allowed_schemes = _BROWSER_SCHEMES if browser else _REQUESTS_SCHEMES
    if (
        parsed.scheme.lower() not in allowed_schemes
        or not parsed.hostname
        or port is None
        or not 1 <= port <= 65535
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        code = (
            "marketplace_proxy_browser_scheme_unsupported"
            if browser and parsed.scheme.lower() in _REQUESTS_SCHEMES
            else "marketplace_proxy_url_invalid"
        )
        raise MarketplaceProxyError(code)
    return MarketplaceProxyRoute(
        url=proxy_url,
        scheme=parsed.scheme.lower(),
        sha256=hashlib.sha256(proxy_url.encode("utf-8")).hexdigest(),
    )


def require_marketplace_proxy(
    raw_config: Mapping[str, Any],
    *,
    browser: bool = False,
) -> MarketplaceProxyRoute:
    require_runtime_env_loaded()
    serp = _serp_config(raw_config)
    env_name = str(
        serp.get("proxy_url_env") or "PARSER_WB_PROXY_URL"
    ).strip()
    if not _ENV_NAME_RE.fullmatch(env_name):
        raise MarketplaceProxyError("marketplace_proxy_env_invalid")
    if env_name not in os.environ or not os.environ[env_name].strip():
        raise MarketplaceProxyError("marketplace_proxy_env_missing")
    return proxy_route_from_url(os.environ[env_name], browser=browser)


def configure_requests_session(
    session: requests.Session,
    route: MarketplaceProxyRoute,
) -> requests.Session:
    session.trust_env = False
    session.proxies.clear()
    session.proxies.update(route.requests_proxies)
    setattr(session, "_wb_marketplace_proxy_sha256", route.sha256)
    assert_requests_session_proxy(session, route)
    return session


def assert_requests_session_proxy(
    session: requests.Session,
    route: MarketplaceProxyRoute,
) -> None:
    if session.trust_env is not False:
        raise MarketplaceProxyError("marketplace_proxy_session_trust_env_enabled")
    if dict(session.proxies) != route.requests_proxies:
        raise MarketplaceProxyError("marketplace_proxy_session_not_applied")
    if getattr(session, "_wb_marketplace_proxy_sha256", None) != route.sha256:
        raise MarketplaceProxyError("marketplace_proxy_session_provenance_mismatch")


def build_requests_session(
    route: MarketplaceProxyRoute,
) -> requests.Session:
    return configure_requests_session(requests.Session(), route)
