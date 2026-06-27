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


def file_label(path: Path) -> str:
    if not path.exists():
        return f"{path} (нет файла)"
    return f"{path} ({path.stat().st_size:,} bytes)".replace(",", " ")


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
