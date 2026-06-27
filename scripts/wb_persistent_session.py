#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import signal
import stat
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEEPER_PATH = PROJECT_ROOT / "scripts" / "wb_cookie_keeper.py"
DEFAULT_URL = "https://www.wildberries.ru/catalog/0/search.aspx?search={query}"
ANDROID_CHROME_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Mobile Safari/537.36"
)


def load_keeper():
    spec = importlib.util.spec_from_file_location("wb_cookie_keeper", KEEPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load keeper module: {KEEPER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


keeper = load_keeper()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_cookie_header(cookie_header: str) -> OrderedDict[str, str]:
    pairs: OrderedDict[str, str] = OrderedDict()
    for part in cookie_header.split(";"):
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name:
            pairs[name] = value
    return pairs


def cookies_for_context(cookie_header: str) -> list[dict[str, Any]]:
    cookies: list[dict[str, Any]] = []
    for name, value in parse_cookie_header(cookie_header).items():
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": ".wildberries.ru",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax",
            }
        )
    return cookies


def cookie_header_from_context(cookies: list[dict[str, Any]]) -> str:
    pairs: OrderedDict[str, str] = OrderedDict()
    for cookie in cookies:
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name or value == "":
            continue
        if "wildberries.ru" not in domain and "wb.ru" not in domain:
            continue
        pairs[name] = value
    return "; ".join(f"{name}={value}" for name, value in pairs.items())


def detect_antibot(title: str, html: str) -> bool:
    return (
        "__wbaas/challenges/antibot" in html
        or "Почти готово" in title
        or "Почти готово" in html
        or "ÐÐ¾ÑÑÐ¸" in title
        or "ÐÐ¾ÑÑÐ¸" in html
    )


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_public_ip_seed(state_json: Path) -> tuple[str, str]:
    try:
        data = json.loads(state_json.read_text(encoding="utf-8"))
    except Exception:
        return "", ""
    if not isinstance(data, dict):
        return "", ""
    public_ip = str(
        data.get("public_ip") or data.get("last_known_public_ip") or data.get("previous_public_ip") or ""
    ).strip()
    first_seen = str(data.get("public_ip_first_seen_at_utc") or "").strip()
    return public_ip, first_seen


def parse_utc_iso(value: str) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def seconds_since(value: str) -> int | None:
    parsed = parse_utc_iso(value)
    if parsed is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))


def _extract_public_ip(response_text: str) -> str:
    text = response_text.strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        text = str(payload.get("ip") or payload.get("query") or "").strip()
    else:
        text = text.splitlines()[0].strip()
    if not text or len(text) > 80 or any(ch.isspace() for ch in text):
        return ""
    return text


def public_ip_urls() -> list[str]:
    raw_urls = os.getenv("PARSER_WB_PUBLIC_IP_URLS", "").strip()
    if not raw_urls:
        raw_urls = os.getenv("PARSER_WB_PUBLIC_IP_URL", "").strip()
    if not raw_urls:
        raw_urls = "https://api.ipify.org?format=json,https://ifconfig.me/ip,https://icanhazip.com"
    return [url.strip() for url in raw_urls.split(",") if url.strip()]


def _public_ip_error_label(url: str, error: str) -> str:
    host = urlsplit(url).netloc or "public_ip"
    return f"{host}:{error}"


def fetch_public_ip(config: dict[str, Any]) -> tuple[str, str]:
    urls = public_ip_urls()
    if not urls:
        return "", "disabled"
    timeout = int(os.getenv("PARSER_WB_PUBLIC_IP_TIMEOUT_SECONDS", "5"))
    proxies = keeper.requests_proxies(keeper.resolve_proxy_url(config))
    errors: list[str] = []
    for url in urls:
        try:
            response = keeper.requests.get(url, timeout=timeout, proxies=proxies)
            if response.status_code != 200:
                errors.append(_public_ip_error_label(url, f"http_{response.status_code}"))
                continue
            public_ip = _extract_public_ip(response.text)
            if not public_ip:
                errors.append(_public_ip_error_label(url, "parse_failed"))
                continue
            return public_ip, ""
        except Exception as exc:
            errors.append(_public_ip_error_label(url, exc.__class__.__name__))
    return "", ";".join(errors)[:240]


def promote_cookie_if_valid(
    config: dict[str, Any],
    args: argparse.Namespace,
    cookie_header: str,
    *,
    html_ok: bool,
) -> bool:
    if not cookie_header or not html_ok or args.no_promote:
        return False

    cookie_path = keeper.resolve_cookie_path(config, args.cookie_file)
    temp_cookie = cookie_path.with_name(f"{cookie_path.name}.persistent.tmp")
    try:
        keeper.write_cookie_value(temp_cookie, cookie_header)
        smoke_args = argparse.Namespace(
            cookie_file=str(temp_cookie),
            state_json=str(args.state_json),
            query=args.query,
            sample_count=args.sample_count,
            min_successes=0,
            page=args.page,
        )
        if not keeper.smoke(config, smoke_args):
            print("persistent promote skipped: serp smoke failed", file=sys.stderr)
            return False
        if not keeper.html_access_smoke(config, smoke_args, temp_cookie):
            print("persistent promote skipped: html smoke failed", file=sys.stderr)
            return False
        keeper.write_cookie_value(cookie_path, keeper.read_cookie_value(temp_cookie))
        print(f"persistent promote ok: cookie_file={cookie_path} cookie_count={len(parse_cookie_header(cookie_header))}")
        return True
    finally:
        try:
            if temp_cookie.exists():
                temp_cookie.unlink()
        except OSError:
            pass


def android_context_options() -> dict[str, Any]:
    return {
        "user_agent": ANDROID_CHROME_UA,
        "viewport": {"width": 393, "height": 873},
        "screen": {"width": 393, "height": 873},
        "device_scale_factor": 2.75,
        "is_mobile": True,
        "has_touch": True,
        "locale": "ru-RU",
        "timezone_id": "Europe/Moscow",
        "extra_http_headers": {
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    }


def install_android_fingerprint_patch(context: Any) -> None:
    context.add_init_script(
        """
(() => {
  Object.defineProperty(navigator, 'platform', {get: () => 'Linux armv8l'});
  Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 5});
  Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
})();
"""
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keep a persistent WB browser session alive without printing secrets.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--cookie-file", default="")
    parser.add_argument("--storage-state", default="state/browser/wb_storage_state.json")
    parser.add_argument("--profile-dir", default="state/browser/wb_persistent_profile")
    parser.add_argument("--state-json", default="state/wb_persistent_session/latest.json")
    parser.add_argument("--query", default="")
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--sample-count", type=int, default=3)
    parser.add_argument("--heartbeat-seconds", type=int, default=int(os.getenv("PARSER_WB_BROWSER_HEARTBEAT_SECONDS", "300")))
    parser.add_argument("--wait-ms", type=int, default=int(os.getenv("PARSER_WB_BROWSER_WAIT_MS", "5000")))
    parser.add_argument("--timeout-ms", type=int, default=int(os.getenv("PARSER_WB_BROWSER_TIMEOUT_MS", "45000")))
    parser.add_argument("--browser-channel", default="")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--oneshot", action="store_true")
    parser.add_argument("--no-promote", action="store_true")
    parser.add_argument("--no-seed-cookie", action="store_true")
    parser.add_argument("--mobile-android", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = keeper.load_config(keeper.resolve_path(args.config))
    cookie_path = keeper.resolve_cookie_path(config, args.cookie_file)
    storage_state = keeper.resolve_path(args.storage_state)
    profile_dir = keeper.resolve_path(args.profile_dir)
    state_json = keeper.resolve_path(args.state_json)
    query = args.query or keeper.load_queries(config, "", 1)[0]
    url = DEFAULT_URL.format(query=quote(query))
    proxy = keeper.playwright_proxy_config(keeper.resolve_proxy_url(config))
    suggest = config.get("suggest") if isinstance(config.get("suggest"), dict) else {}
    browser_channel = args.browser_channel or str(suggest.get("browser_channel") or "chrome")
    stop = False

    def handle_stop(_signum, _frame) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"persistent session failed: playwright unavailable: {exc}", file=sys.stderr)
        return 2

    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state.parent.mkdir(parents=True, exist_ok=True)
    state_json.parent.mkdir(parents=True, exist_ok=True)
    last_public_ip, public_ip_first_seen_at = read_public_ip_seed(state_json)

    with sync_playwright() as p:
        launch_kwargs: dict[str, Any] = {
            "headless": not args.headed,
        }
        if browser_channel:
            launch_kwargs["channel"] = browser_channel
        if proxy:
            launch_kwargs["proxy"] = proxy
        if args.mobile_android:
            launch_kwargs.update(android_context_options())
        context = p.chromium.launch_persistent_context(str(profile_dir), **launch_kwargs)
        try:
            if args.mobile_android:
                install_android_fingerprint_patch(context)

            cookie_header = ""
            if not args.no_seed_cookie:
                cookie_header = keeper.read_cookie_value(cookie_path)
                browser_cookies = cookies_for_context(cookie_header)
                if browser_cookies:
                    context.add_cookies(browser_cookies)

            page = context.pages[0] if context.pages else context.new_page()
            iteration = 0
            while not stop:
                iteration += 1
                heartbeat_started_at = utc_now_iso()
                public_ip, public_ip_error = fetch_public_ip(config)
                previous_public_ip = last_public_ip
                public_ip_changed = bool(public_ip and last_public_ip and public_ip != last_public_ip)
                if public_ip:
                    if public_ip_changed or not last_public_ip:
                        public_ip_first_seen_at = heartbeat_started_at
                    last_public_ip = public_ip
                public_ip_age_seconds = seconds_since(public_ip_first_seen_at) if public_ip_first_seen_at else None
                response_status = 0
                title = ""
                html = ""
                error = ""
                try:
                    response = page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
                    response_status = response.status if response else 0
                    page.wait_for_timeout(max(0, args.wait_ms))
                    title = page.title()[:120]
                    html = page.content()[:5000]
                except Exception as exc:
                    error = f"{exc.__class__.__name__}: {exc}"

                antibot = detect_antibot(title, html)
                html_ok = response_status == 200 and not antibot and not error
                cookies = context.cookies(["https://www.wildberries.ru", "https://search.wb.ru"])
                context.storage_state(path=str(storage_state))
                storage_state.chmod(stat.S_IRUSR | stat.S_IWUSR)
                exported_cookie = cookie_header_from_context(cookies)
                promoted = promote_cookie_if_valid(config, args, exported_cookie, html_ok=html_ok)
                status = "ok" if html_ok else "failed"
                state = {
                    "status": status,
                    "checked_at_utc": utc_now_iso(),
                    "heartbeat_started_at_utc": heartbeat_started_at,
                    "iteration": iteration,
                    "http_status": response_status,
                    "antibot": antibot,
                    "title_present": bool(title),
                    "cookie_count": len(parse_cookie_header(exported_cookie)),
                    "storage_state": str(storage_state),
                    "profile_dir": str(profile_dir),
                    "mobile_android": bool(args.mobile_android),
                    "seed_cookie": not bool(args.no_seed_cookie),
                    "promoted": promoted,
                    "public_ip": public_ip,
                    "previous_public_ip": previous_public_ip,
                    "last_known_public_ip": last_public_ip,
                    "public_ip_changed": public_ip_changed,
                    "public_ip_first_seen_at_utc": public_ip_first_seen_at,
                    "public_ip_age_seconds": public_ip_age_seconds,
                    "public_ip_error_class": public_ip_error,
                    "error_class": error.split(":", 1)[0] if error else "",
                }
                write_json(state_json, state)
                print(
                    "persistent heartbeat",
                    f"ts={state['checked_at_utc']}",
                    f"status={status}",
                    f"http={response_status}",
                    f"antibot={str(antibot).lower()}",
                    f"cookie_count={state['cookie_count']}",
                    f"promoted={str(promoted).lower()}",
                    f"public_ip={public_ip or 'unknown'}",
                    f"last_known_ip={last_public_ip or 'unknown'}",
                    f"ip_changed={str(public_ip_changed).lower()}",
                    f"ip_age={public_ip_age_seconds if public_ip_age_seconds is not None else 'unknown'}",
                    f"ip_error={public_ip_error or 'none'}",
                    f"iteration={iteration}",
                )
                if args.oneshot:
                    return 0 if html_ok else 20
                for _ in range(max(1, args.heartbeat_seconds)):
                    if stop:
                        break
                    time.sleep(1)
        finally:
            context.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
