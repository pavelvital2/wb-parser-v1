#!/usr/bin/env python3
"""Send a Telegram summary for the WB products+sellers scheduled run.

This script is intentionally standalone and stdlib-only: it reads existing
result files and never imports parser code, so notification failures cannot
affect collection logic.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("PARSER_WB_PROJECT_DIR", "/home/pavel/projects/parser_wb"))
BOT_ROOT = Path(os.environ.get("TELEGRAM_BOT_ROOT", "/home/pavel/projects/telegram-ai-agent"))
TOPIC_NAME = os.environ.get("PARSER_WB_NOTIFY_TOPIC", "parser_wb")

PRODUCTS_FILE = PROJECT_DIR / "data/marts/serp/latest/products_daily.csv"
SERP_PAGES_FILE = PROJECT_DIR / "data/raw/serp/latest/pages_raw_index.csv"
SELLERS_FILE = PROJECT_DIR / "data/marts/sellers/latest/sellers_daily.csv"
BRIDGE_FILE = PROJECT_DIR / "data/marts/sellers/latest/seller_query_product_bridge.csv"
QUERIES_FILE = PROJECT_DIR / "exports/queries.txt"
WAREHOUSE_STATE_FILE = PROJECT_DIR / "state/wb_warehouse/latest.json"
RUN_REPORT_FILE = PROJECT_DIR / "state/run_reports/latest.json"
KEEPER_STATE_FILE = PROJECT_DIR / "state/wb_session_keeper/latest.json"
PREFLIGHT_STATE_FILE = PROJECT_DIR / "state/wb_nightly_preflight/latest.json"
WATCHDOG_STATE_FILE = PROJECT_DIR / "state/wb_persistent_session/watchdog.json"
PERSISTENT_SESSION_STATE_FILE = PROJECT_DIR / "state/wb_persistent_session/latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", type=int, required=True, help="Exit status of the wrapper run")
    parser.add_argument("--run-stamp", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--finished-at", required=True)
    parser.add_argument("--log-path", default=str(PROJECT_DIR / "data/logs/cron_products_sellers.log"))
    parser.add_argument("--phase", choices=["run", "preflight"], default="run")
    parser.add_argument("--dry-run", action="store_true", help="Print message and routing without sending")
    return parser.parse_args()


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        values[key] = value
    return values


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def maybe_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def resolve_chat_id(thread_id: int | None, bot_env: dict[str, str]) -> int | None:
    direct = maybe_int(os.environ.get("PARSER_WB_NOTIFY_CHAT_ID"))
    if direct is not None:
        return direct
    env_chat = maybe_int(bot_env.get("TELEGRAM_CHAT_ID"))
    if env_chat is not None:
        return env_chat

    runtime_path = BOT_ROOT / "tmux_sessions" / TOPIC_NAME / "mcp.runtime.json"
    runtime = load_json(runtime_path)
    runtime_env = runtime.get("mcpServers", {}).get("bot", {}).get("env", {})
    runtime_chat = maybe_int(runtime_env.get("TELEGRAM_CHAT_ID"))
    if runtime_chat is not None:
        return runtime_chat

    state = load_json(BOT_ROOT / "tmux_sessions/state.json")
    if thread_id is not None:
        suffix = f":{thread_id}"
        for key, value in state.items():
            if str(key).endswith(suffix):
                chat = maybe_int(value.get("chat_id")) if isinstance(value, dict) else None
                if chat is not None:
                    return chat
    return None


def resolve_thread_id() -> int | None:
    direct = maybe_int(os.environ.get("PARSER_WB_NOTIFY_THREAD_ID"))
    if direct is not None:
        return direct

    config = load_json(BOT_ROOT / "topic_config.json")
    routing = config.get("routing", {})
    routed = maybe_int(routing.get(TOPIC_NAME))
    if routed is not None:
        return routed

    topics = config.get("topics", {})
    for key, value in topics.items():
        if isinstance(value, dict) and value.get("name") == TOPIC_NAME:
            found = maybe_int(key)
            if found is not None:
                return found
    return None


def count_csv_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=";")
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def count_queries(path: Path) -> int | None:
    if not path.exists():
        return None
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            count += 1
    return count


def first_csv_value(path: Path, column: str) -> str:
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            return str(row.get(column) or "")
    return ""


def file_label(path: Path) -> str:
    if not path.exists():
        return f"{path} (нет файла)"
    return f"{path} ({path.stat().st_size:,} bytes)".replace(",", " ")


def warehouse_status_label(path: Path | None = None) -> str:
    path = path or WAREHOUSE_STATE_FILE
    data = load_json(path)
    if not data:
        return "нет state"
    status = str(data.get("status") or "unknown")
    reason = str(data.get("reason") or "")
    rows = data.get("warehouse", {}).get("rows", {}) if isinstance(data.get("warehouse"), dict) else {}
    product_rows = rows.get("product_snapshots") if isinstance(rows, dict) else None
    if product_rows is not None:
        return f"{status} ({reason}), product_snapshots={fmt_count(maybe_int(product_rows))}"
    if reason:
        return f"{status} ({reason})"
    return status


def state_status_label(path: Path, *, success_key: str = "successes", min_key: str = "min_successes") -> str:
    data = load_json(path)
    if not data:
        return "нет state"
    status = str(data.get("status") or "unknown")
    checked_at = str(data.get("checked_at_utc") or "")
    successes = maybe_int(data.get(success_key))
    min_successes = maybe_int(data.get(min_key))
    parts = [status]
    if successes is not None and min_successes is not None:
        parts.append(f"{successes}/{min_successes}")
    if checked_at:
        parts.append(checked_at)
    return ", ".join(parts)


def preflight_label(path: Path | None = None) -> str:
    path = path or PREFLIGHT_STATE_FILE
    data = load_json(path)
    if not data:
        return "нет state"
    status = str(data.get("status") or "unknown")
    checked_at = str(data.get("checked_at_utc") or "")
    actions = data.get("actions") if isinstance(data.get("actions"), list) else []
    safe_actions = ", ".join(str(item) for item in actions[:3])
    parts = [status]
    if checked_at:
        parts.append(checked_at)
    if safe_actions:
        parts.append(safe_actions)
    return ", ".join(parts)


def run_report_label(path: Path | None = None) -> str:
    path = path or RUN_REPORT_FILE
    data = load_json(path)
    if not data:
        return "нет report"
    run_id = str(data.get("run_id") or "")
    pipeline = str(data.get("pipeline") or "")
    status = str(data.get("status") or "unknown")
    duration = maybe_int(data.get("duration_seconds"))
    parts = [item for item in (pipeline, status, run_id) if item]
    if duration is not None:
        parts.append(f"{duration}s")
    return ", ".join(parts) if parts else status


def serp_pages_health_label(path: Path | None = None) -> str:
    path = path or SERP_PAGES_FILE
    if not path.exists():
        return "нет файла"
    total = 0
    by_status: dict[str, int] = {}
    by_http: dict[str, int] = {}
    run_id = ""
    queries: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            total += 1
            run_id = run_id or str(row.get("run_id") or "")
            status = str(row.get("status") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            http_status = str(row.get("http_status") or "")
            if http_status:
                by_http[http_status] = by_http.get(http_status, 0) + 1
            query = str(row.get("query") or "")
            if query:
                queries.add(query)
    if total == 0:
        return "0 pages"
    success = by_status.get("success", 0)
    errors = by_status.get("error", 0)
    empty = by_status.get("empty", 0)
    rate_429 = by_http.get("429", 0)
    anti_498 = by_http.get("498", 0)
    return (
        f"run={run_id or 'unknown'}, pages={total}, queries={len(queries)}, "
        f"success={success}, empty={empty}, errors={errors}, 429={rate_429}, 498={anti_498}"
    )


def latest_publication_label() -> str:
    product_run = first_csv_value(PRODUCTS_FILE, "run_id")
    seller_run = first_csv_value(SELLERS_FILE, "run_id")
    if product_run and seller_run:
        return f"published, products_run={product_run}, sellers_run={seller_run}"
    if product_run:
        return f"partial, products_run={product_run}, sellers latest missing"
    if seller_run:
        return f"partial, products latest missing, sellers_run={seller_run}"
    return "нет latest"


def browser_health_label(
    watchdog_path: Path | None = None,
    session_path: Path | None = None,
) -> str:
    watchdog_path = watchdog_path or WATCHDOG_STATE_FILE
    session_path = session_path or PERSISTENT_SESSION_STATE_FILE
    watchdog = load_json(watchdog_path)
    session = load_json(session_path)
    if not watchdog and not session:
        return "нет state"
    action = str(watchdog.get("action") or "")
    reason = str(watchdog.get("reason") or "")
    session_status = str(session.get("status") or "")
    http_status = str(session.get("http_status") or "")
    antibot = session.get("antibot")
    parts = []
    if action:
        parts.append(action)
    if reason:
        parts.append(reason)
    if session_status:
        parts.append(f"session={session_status}")
    if http_status:
        parts.append(f"http={http_status}")
    if antibot is not None:
        parts.append(f"antibot={str(bool(antibot)).lower()}")
    return ", ".join(parts) if parts else "unknown"


def health_lines() -> list[str]:
    return [
        "",
        "<b>Health</b>",
        f"WB API smoke: <code>{html.escape(state_status_label(KEEPER_STATE_FILE))}</code>",
        f"Preflight: <code>{html.escape(preflight_label())}</code>",
        f"SERP latest: <code>{html.escape(serp_pages_health_label())}</code>",
        f"Latest: <code>{html.escape(latest_publication_label())}</code>",
        f"Run report: <code>{html.escape(run_report_label())}</code>",
        f"Browser channel: <code>{html.escape(browser_health_label())}</code>",
    ]


def parse_dt(value: str) -> datetime | None:
    if not value or value == "unknown":
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def duration_label(started_at: str, finished_at: str) -> str:
    started = parse_dt(started_at)
    finished = parse_dt(finished_at)
    if started is None or finished is None:
        return "unknown"
    seconds = max(0, int((finished - started).total_seconds()))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def tail_lines(path: Path, limit: int = 8) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-limit:]


def fmt_count(value: int | None) -> str:
    if value is None:
        return "нет файла"
    return f"{value:,}".replace(",", " ")


def build_message(args: argparse.Namespace) -> str:
    ok = args.status == 0
    status_label = "OK" if ok else f"ERROR exit={args.status}"
    title = "parser_wb: ночной запуск serp -> sellers"
    if args.phase == "preflight":
        title = "parser_wb: предночный preflight WB-доступа"
    products = count_csv_rows(PRODUCTS_FILE)
    serp_pages = count_csv_rows(SERP_PAGES_FILE)
    sellers = count_csv_rows(SELLERS_FILE)
    bridge = count_csv_rows(BRIDGE_FILE)
    queries = count_queries(QUERIES_FILE)

    lines = [
        f"<b>{html.escape(title)}</b>",
        f"Статус: <b>{html.escape(status_label)}</b>",
        f"Run stamp: <code>{html.escape(args.run_stamp)}</code>",
        f"Время: <code>{html.escape(args.started_at)}</code> -> <code>{html.escape(args.finished_at)}</code>",
        f"Длительность: <code>{html.escape(duration_label(args.started_at, args.finished_at))}</code>",
        "",
        f"Запросов: <b>{fmt_count(queries)}</b>",
        f"Товаров: <b>{fmt_count(products)}</b>",
        f"SERP-страниц: <b>{fmt_count(serp_pages)}</b>",
        f"Продавцов: <b>{fmt_count(sellers)}</b>",
        f"Связок товар-продавец: <b>{fmt_count(bridge)}</b>",
        f"Warehouse: <b>{html.escape(warehouse_status_label())}</b>",
        *health_lines(),
        "",
        f"Лог: <code>{html.escape(args.log_path)}</code>",
        f"Товары: <code>{html.escape(file_label(PRODUCTS_FILE))}</code>",
        f"Продавцы: <code>{html.escape(file_label(SELLERS_FILE))}</code>",
    ]

    if not ok:
        tail = tail_lines(Path(args.log_path))
        if tail:
            lines.extend(["", "Последние строки лога:", "<pre>" + html.escape("\n".join(tail)) + "</pre>"])
    return "\n".join(lines)


def send_telegram(token: str, chat_id: int, thread_id: int | None, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }
    if thread_id is not None:
        payload["message_thread_id"] = str(thread_id)
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body)
    if not parsed.get("ok"):
        raise RuntimeError(body)


def main() -> int:
    args = parse_args()
    bot_env = load_env(BOT_ROOT / ".env")
    token = os.environ.get("PARSER_WB_TELEGRAM_BOT_TOKEN") or bot_env.get("TELEGRAM_BOT_TOKEN")
    thread_id = resolve_thread_id()
    chat_id = resolve_chat_id(thread_id, bot_env)
    message = build_message(args)

    if args.dry_run:
        print(json.dumps({
            "chat_id": chat_id,
            "thread_id": thread_id,
            "topic": TOPIC_NAME,
            "has_token": bool(token),
            "message": message,
        }, ensure_ascii=False, indent=2))
        return 0

    if not token:
        print("Telegram token is not configured", file=sys.stderr)
        return 2
    if chat_id is None:
        print("Telegram chat_id is not configured", file=sys.stderr)
        return 2

    send_telegram(token, chat_id, thread_id, message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
