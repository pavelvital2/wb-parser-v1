#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.proxy_required import (
    build_requests_session,
    proxy_route_from_url,
    require_marketplace_proxy,
)


EXIT_OK = 0
EXIT_SMOKE_FAILED = 20
EXIT_REFRESH_FAILED = 21
COORDINATOR_LOCK_DIRECTORY = Path("/run/lock/parser-nightly-coordinator")

OK_KINDS = {"top_products", "nested_products", "nested_promo_products"}


def _require_host_lease_after_cutover() -> None:
    if not os.path.lexists(COORDINATOR_LOCK_DIRECTORY):
        return
    from app.common.nightly_coordinator import (
        require_official_live_entry_lease,
    )

    require_official_live_entry_lease(environment=os.environ)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_path(value: str | Path, *, root: Path = PROJECT_ROOT) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return path


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8-sig") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError("config is not a YAML object")
    inject_runtime_request_headers(data, config_path.parent.parent)
    return data


def resolve_cookie_path(config: dict[str, Any], explicit: str = "") -> Path:
    if explicit:
        return resolve_path(explicit)

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("wb_cookie_file_env") or "WB_COOKIE_FILE").strip()
    env_value = os.getenv(env_name, "").strip()
    if env_value:
        return resolve_path(env_value)

    cookie_file = str(serp.get("wb_cookie_file") or "").strip()
    if not cookie_file:
        raise RuntimeError(f"cookie file is not configured; set {env_name} or serp.wb_cookie_file")
    return resolve_path(cookie_file)


def resolve_serp_base_urls(config: dict[str, Any]) -> list[str]:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    urls: list[str] = []

    def add_url(value: Any) -> None:
        url = str(value or "").strip()
        if url and url not in urls:
            urls.append(url)

    add_url(serp.get("base_url"))
    fallback_urls = serp.get("fallback_base_urls")
    if isinstance(fallback_urls, list):
        for url in fallback_urls:
            add_url(url)

    if not urls:
        raise RuntimeError("serp.base_url is not configured")
    return urls


def resolve_proxy_url(config: dict[str, Any]) -> str:
    return require_marketplace_proxy(config).url


def requests_proxies(proxy_url: str) -> dict[str, str]:
    return proxy_route_from_url(proxy_url).requests_proxies


def _coerce_request_headers(raw_headers: Any) -> dict[str, str]:
    if not isinstance(raw_headers, dict):
        return {}
    headers: dict[str, str] = {}
    for name, value in raw_headers.items():
        header_name = str(name or "").strip()
        if not header_name or value is None:
            continue
        if header_name.lower() == "cookie":
            continue
        headers[header_name] = str(value)
    return headers


def inject_runtime_request_headers(config: dict[str, Any], project_root: Path = PROJECT_ROOT) -> None:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("request_headers_file_env") or "PARSER_WB_REQUEST_HEADERS_FILE").strip()
    headers_file = os.getenv(env_name, "").strip() if env_name else ""
    headers_file = headers_file or str(serp.get("request_headers_file") or "").strip()
    if not headers_file:
        return

    path = resolve_path(headers_file, root=project_root)
    if not path.exists():
        raise RuntimeError(f"request headers file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(payload, dict) and isinstance(payload.get("headers"), dict):
        payload = payload["headers"]
    headers = _coerce_request_headers(serp.get("request_headers"))
    headers.update(_coerce_request_headers(payload))
    serp["request_headers"] = headers


def request_headers_from_config(config: dict[str, Any]) -> dict[str, str]:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    return _coerce_request_headers(serp.get("request_headers"))


def playwright_proxy_config(proxy_url: str) -> dict[str, str]:
    return proxy_route_from_url(proxy_url, browser=True).playwright_proxy()


def marketplace_get(
    config: dict[str, Any],
    url: str,
    **kwargs: Any,
) -> requests.Response:
    with build_requests_session(require_marketplace_proxy(config)) as session:
        return session.get(url, **kwargs)


def resolve_smoke_min_successes(config: dict[str, Any], args: argparse.Namespace, total_queries: int) -> int:
    explicit = int(getattr(args, "min_successes", 0) or 0)
    if explicit > 0:
        return min(explicit, total_queries)

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    configured = serp.get("smoke_min_successes")
    if configured is not None:
        try:
            return min(max(1, int(configured)), total_queries)
        except (TypeError, ValueError):
            pass

    return total_queries


def read_cookie_value(cookie_path: Path) -> str:
    if not cookie_path.exists():
        raise RuntimeError(f"cookie file not found: {cookie_path}")
    value = cookie_path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"cookie file is empty: {cookie_path}")
    return value


def cookie_required(config: dict[str, Any]) -> bool:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("cookie_required_env") or "PARSER_WB_COOKIE_REQUIRED").strip()
    if env_name:
        env_value = os.getenv(env_name, "").strip().lower()
        if env_value:
            return env_value not in {"0", "false", "no", "off"}
    if "cookie_required" in serp:
        return bool(serp.get("cookie_required"))
    return True


def read_cookie_value_for_smoke(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path) -> str:
    if getattr(args, "without_cookie", False):
        return ""
    try:
        return read_cookie_value(cookie_path)
    except RuntimeError:
        if cookie_required(config):
            raise
        return ""


def write_cookie_value(cookie_path: Path, cookie_value: str) -> None:
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cookie_path.with_name(f"{cookie_path.name}.tmp")
    tmp_path.write_text(cookie_value.strip() + "\n", encoding="utf-8")
    tmp_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    tmp_path.replace(cookie_path)
    cookie_path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def load_queries(config: dict[str, Any], explicit_query: str, sample_count: int) -> list[str]:
    if explicit_query.strip():
        return [explicit_query.strip()]

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    input_files = serp.get("input_files") if isinstance(serp.get("input_files"), dict) else {}
    queries_txt = str(input_files.get("queries_txt") or "exports/queries.txt").strip()
    query_path = resolve_path(queries_txt)
    if not query_path.exists():
        return ["шеврон"]

    queries: list[str] = []
    for line in query_path.read_text(encoding="utf-8-sig").splitlines():
        query = " ".join(line.strip().split())
        if query:
            queries.append(query)
        if len(queries) >= sample_count:
            break
    return queries or ["шеврон"]


def classify_payload(payload: Any) -> tuple[str, int, str]:
    if not isinstance(payload, dict):
        return "json_not_object", 0, ""

    products = payload.get("products")
    if isinstance(products, list) and products:
        first = products[0] if isinstance(products[0], dict) else {}
        sample = str(first.get("name", ""))[:120] if isinstance(first, dict) else ""
        return "top_products", len(products), sample

    nested_data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    nested_products = nested_data.get("products") if isinstance(nested_data, dict) else None
    if isinstance(nested_products, list) and nested_products:
        product_dicts = [p for p in nested_products if isinstance(p, dict)]
        promo_count = 0
        for product in product_dicts:
            log = product.get("log")
            if isinstance(log, dict) and log.get("promotion") == 1:
                promo_count += 1
        sample = str(product_dicts[0].get("name", ""))[:120] if product_dicts else ""
        if product_dicts and promo_count == len(product_dicts):
            return "nested_promo_products", len(product_dicts), sample
        return "nested_products", len(product_dicts), sample

    return "empty_or_unknown", 0, ",".join(list(payload.keys())[:8])


def write_state(path_value: str, data: dict[str, Any]) -> None:
    if not path_value:
        return
    path = resolve_path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def smoke(config: dict[str, Any], args: argparse.Namespace, *, emit: bool = True) -> bool:
    # Validate the marketplace route once, outside the per-endpoint error
    # handling, so a missing proxy cannot be mistaken for a failed smoke.
    require_marketplace_proxy(config)
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    cookie_path = resolve_cookie_path(config, args.cookie_file)
    cookie_value = read_cookie_value_for_smoke(config, args, cookie_path)
    queries = load_queries(config, args.query, max(1, int(args.sample_count)))
    page = int(args.page)
    base_urls = resolve_serp_base_urls(config)
    min_successes = resolve_smoke_min_successes(config, args, len(queries))

    headers = {
        "user-agent": str(serp.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
        "x-requested-with": str(serp.get("x_requested_with") or "XMLHttpRequest"),
        "accept": "application/json, text/plain, */*",
    }
    headers.update(request_headers_from_config(config))
    if cookie_value:
        headers["cookie"] = cookie_value
    referer_base = str(serp.get("referer_base") or "https://www.wildberries.ru/catalog/0/search.aspx?search=")
    request_params = serp.get("request_params") if isinstance(serp.get("request_params"), dict) else {}
    timeout = int(config.get("runtime", {}).get("http_timeout_seconds", 45))
    results: list[dict[str, Any]] = []
    successes = 0
    for query in queries:
        params = dict(request_params)
        params["query"] = query
        params["page"] = str(page)
        req_headers = dict(headers)
        req_headers["referer"] = f"{referer_base}{quote(query)}"

        result: dict[str, Any] | None = None
        query_ok = False
        for base_url in base_urls:
            result = {
                "query": query,
                "page": page,
                "endpoint": base_url,
                "checked_at_utc": utc_now_iso(),
            }
            try:
                response = marketplace_get(
                    config,
                    base_url,
                    params=params,
                    headers=req_headers,
                    timeout=timeout,
                )
                result["http_status"] = response.status_code
                if response.status_code != 200:
                    result["kind"] = "http_error"
                    result["products_count"] = 0
                    result["sample"] = response.text[:120].replace("\n", " ")
                else:
                    try:
                        payload = response.json()
                    except Exception as exc:
                        result["kind"] = "json_decode_failed"
                        result["products_count"] = 0
                        result["sample"] = exc.__class__.__name__
                    else:
                        kind, count, sample = classify_payload(payload)
                        result["kind"] = kind
                        result["products_count"] = count
                        result["sample"] = sample
                        if kind in OK_KINDS:
                            query_ok = True
                            break
            except Exception as exc:
                result["http_status"] = 0
                result["kind"] = "request_failed"
                result["products_count"] = 0
                result["sample"] = exc.__class__.__name__

        if query_ok:
            successes += 1
        if result is None:
            result = {
                "query": query,
                "page": page,
                "endpoint": "",
                "checked_at_utc": utc_now_iso(),
                "http_status": 0,
                "kind": "request_failed",
                "products_count": 0,
                "sample": "no endpoint checked",
            }
        results.append(result)
        if emit:
            print(
                "smoke",
                f"query={query!r}",
                f"page={page}",
                f"http={result.get('http_status')}",
                f"kind={result.get('kind')}",
                f"products={result.get('products_count')}",
                f"sample={result.get('sample')}",
            )

    ok = successes >= min_successes
    write_state(
        args.state_json,
        {
            "status": "ok" if ok else "failed",
            "checked_at_utc": utc_now_iso(),
            "min_successes": min_successes,
            "successes": successes,
            "results": results,
        },
    )
    return ok


def html_access_smoke(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path, *, emit: bool = True) -> bool:
    if os.getenv("PARSER_WB_REFRESH_HTML_SMOKE_DISABLED", "").strip() == "1":
        return True

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    if serp.get("refresh_html_smoke_enabled") is False:
        return True
    require_marketplace_proxy(config)

    query = load_queries(config, getattr(args, "query", ""), 1)[0]
    url_template = str(
        serp.get("html_smoke_url") or "https://www.wildberries.ru/catalog/0/search.aspx?search={query}"
    )
    url = url_template.replace("{query}", quote(query))
    cookie_value = read_cookie_value(cookie_path)
    timeout = int(config.get("runtime", {}).get("http_timeout_seconds", 45))
    headers = {
        "user-agent": str(serp.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    headers.update(request_headers_from_config(config))
    headers["cookie"] = cookie_value
    try:
        response = marketplace_get(
            config,
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )
        text = response.text[:5000]
    except Exception as exc:
        if emit:
            print(
                f"html_smoke failed: {exc.__class__.__name__}",
                file=sys.stderr,
            )
        return False

    antibot = (
        "__wbaas/challenges/antibot" in text
        or "Почти готово" in text
        or "ÐÐ¾ÑÑÐ¸" in text
    )
    ok = response.status_code == 200 and not antibot
    if emit:
        print(f"html_smoke http={response.status_code} antibot={str(antibot).lower()} ok={str(ok).lower()}")
    return ok


def storage_state_default() -> str:
    return os.getenv("PARSER_WB_STORAGE_STATE_FILE") or os.getenv("WB_STORAGE_STATE_FILE") or "state/browser/wb_storage_state.json"


def refresh(config: dict[str, Any], args: argparse.Namespace) -> bool:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"refresh failed: playwright is unavailable in current runtime: {exc}", file=sys.stderr)
        return False

    cookie_path = resolve_cookie_path(config, args.cookie_file)
    storage_state = resolve_path(args.storage_state or storage_state_default())
    storage_state_out = resolve_path(args.storage_state_out or str(storage_state))
    suggest = config.get("suggest") if isinstance(config.get("suggest"), dict) else {}
    browser_channel = args.browser_channel or str(suggest.get("browser_channel") or "chrome")
    proxy_config = require_marketplace_proxy(
        config,
        browser=True,
    ).playwright_proxy()
    headless = not bool(args.no_headless)
    if args.headed:
        headless = False

    context_kwargs: dict[str, Any] = {}
    if storage_state.exists():
        context_kwargs["storage_state"] = str(storage_state)
    elif args.require_storage_state:
        print(f"refresh failed: storage_state not found: {storage_state}", file=sys.stderr)
        return False
    extra_headers = request_headers_from_config(config)
    if extra_headers:
        context_kwargs["extra_http_headers"] = extra_headers

    try:
        with sync_playwright() as p:
            launch_kwargs: dict[str, Any] = {"headless": headless}
            if browser_channel:
                launch_kwargs["channel"] = browser_channel
            launch_kwargs["proxy"] = proxy_config
            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.goto(args.refresh_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
            page.wait_for_timeout(args.wait_ms)
            cookies = context.cookies(["https://www.wildberries.ru", "https://search.wb.ru"])
            storage_state_out.parent.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=str(storage_state_out))
            context.close()
            browser.close()
    except Exception as exc:
        print(f"refresh failed: {exc.__class__.__name__}", file=sys.stderr)
        return False

    cookie_pairs: OrderedDict[str, str] = OrderedDict()
    allowed_domains: set[str] = set()
    for cookie in cookies:
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name:
            continue
        if "wildberries.ru" not in domain and "wb.ru" not in domain:
            continue
        allowed_domains.add(domain)
        cookie_pairs[name] = value

    if not cookie_pairs:
        print("refresh failed: browser did not return WB cookies", file=sys.stderr)
        return False

    write_cookie_value(cookie_path, "; ".join(f"{name}={value}" for name, value in cookie_pairs.items()))
    write_state(
        args.state_json,
        {
            "status": "refreshed",
            "checked_at_utc": utc_now_iso(),
            "cookie_file": str(cookie_path),
            "cookie_count": len(cookie_pairs),
            "cookie_domains": sorted(allowed_domains),
            "storage_state": str(storage_state_out),
        },
    )
    print(f"refresh ok: cookie_file={cookie_path} cookie_count={len(cookie_pairs)} storage_state={storage_state_out}")
    return True


def _namespace_copy(args: argparse.Namespace, **overrides: Any) -> argparse.Namespace:
    values = vars(args).copy()
    values.update(overrides)
    return argparse.Namespace(**values)


def _temp_path(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.name}.{suffix}.tmp")


def refresh_and_promote(config: dict[str, Any], args: argparse.Namespace) -> bool:
    cookie_path = resolve_cookie_path(config, args.cookie_file)
    temp_cookie = _temp_path(cookie_path, "refresh")
    state_json = resolve_path(args.state_json) if args.state_json else PROJECT_ROOT / "state/wb_session_keeper/latest.json"
    temp_state_json = _temp_path(state_json, "refresh")

    storage_state_target = resolve_path(args.storage_state_out or args.storage_state or storage_state_default())
    temp_storage_state = _temp_path(storage_state_target, "refresh")

    refresh_args = _namespace_copy(
        args,
        cookie_file=str(temp_cookie),
        state_json=str(temp_state_json),
        storage_state_out=str(temp_storage_state),
    )
    try:
        if not refresh(config, refresh_args):
            return False

        smoke_args = _namespace_copy(args, cookie_file=str(temp_cookie))
        if not smoke(config, smoke_args):
            print("refresh smoke failed; keeping existing cookie file unchanged", file=sys.stderr)
            return False

        if not html_access_smoke(config, smoke_args, temp_cookie):
            print("refresh html smoke failed; keeping existing cookie file unchanged", file=sys.stderr)
            return False

        write_cookie_value(cookie_path, read_cookie_value(temp_cookie))
        if temp_storage_state.exists():
            storage_state_target.parent.mkdir(parents=True, exist_ok=True)
            temp_storage_state.replace(storage_state_target)
        print(f"refresh promoted: cookie_file={cookie_path} storage_state={storage_state_target}")
        return True
    finally:
        for path in (temp_cookie, temp_state_json, temp_storage_state):
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                pass


def renew(config: dict[str, Any], args: argparse.Namespace) -> bool:
    print("proactive cookie refresh started", file=sys.stderr)
    return refresh_and_promote(config, args)


def ensure(config: dict[str, Any], args: argparse.Namespace) -> bool:
    if smoke(config, args):
        return True

    print("smoke failed; trying cookie refresh", file=sys.stderr)
    return refresh_and_promote(config, args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WB cookie/session keeper without printing secret values.")
    parser.add_argument("command", choices=["smoke", "refresh", "renew", "ensure"])
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--cookie-file", default="")
    parser.add_argument("--state-json", default="state/wb_session_keeper/latest.json")
    parser.add_argument("--query", default="")
    parser.add_argument("--sample-count", type=int, default=1)
    parser.add_argument("--min-successes", type=int, default=0)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--refresh-url", default="https://www.wildberries.ru/")
    parser.add_argument("--storage-state", default="")
    parser.add_argument("--storage-state-out", default="")
    parser.add_argument("--require-storage-state", action="store_true")
    parser.add_argument("--browser-channel", default="")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--wait-ms", type=int, default=3000)
    parser.add_argument("--timeout-ms", type=int, default=45000)
    parser.add_argument("--without-cookie", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    _require_host_lease_after_cutover()
    args = build_parser().parse_args(argv)
    config_path = resolve_path(args.config)
    config = load_config(config_path)

    try:
        if args.command == "smoke":
            return EXIT_OK if smoke(config, args) else EXIT_SMOKE_FAILED
        if args.command == "refresh":
            return EXIT_OK if refresh(config, args) else EXIT_REFRESH_FAILED
        if args.command == "renew":
            return EXIT_OK if renew(config, args) else EXIT_REFRESH_FAILED
        return EXIT_OK if ensure(config, args) else EXIT_SMOKE_FAILED
    except Exception as exc:
        print(f"{args.command} failed: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return EXIT_SMOKE_FAILED if args.command != "refresh" else EXIT_REFRESH_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
