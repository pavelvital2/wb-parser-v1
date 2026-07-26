from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from app.common.config import AppConfig
from app.common.constants import COMPONENT_SUGGEST
from app.common.csv_io import append_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.logging_setup import get_logger
from app.common.proxy_required import require_marketplace_proxy
from app.common.retry import with_retry
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB

try:
    from playwright.sync_api import Page, sync_playwright
except Exception:  # pragma: no cover - import checked at runtime
    Page = Any  # type: ignore[misc,assignment]
    sync_playwright = None  # type: ignore[assignment]


WB_URL = "https://www.wildberries.ru"
SEARCH_INPUT_TESTID = "searchInput"
SUGGEST_ROOT_SELECTOR = "#searchBlock"

CHECKPOINT_KEY_TEMPLATE = "{prefix}|{letter}|{depth}"
CHECKPOINT_VALUE_SUCCESS = "success"
CHECKPOINT_VALUE_EMPTY = "empty"
CHECKPOINT_VALUE_ERROR = "error"


def norm_query(value: str) -> str:
    value = (value or "").strip()
    return " ".join(value.split())


def norm_lc(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("ё", "е")
    return re.sub(r"\s+", " ", value)


def build_letters(mode: str) -> list[str]:
    mode = (mode or "").strip().lower()
    if mode.startswith("custom:"):
        letters = re.sub(r"\s+", "", mode.split("custom:", 1)[1])
        if not letters:
            raise CriticalPipelineError("suggest letters custom mode is empty")
        return list(letters)

    ru = list("абвгдежзийклмнопрстуфхцчшщъыьэюя")
    if mode == "ru28":
        return [ch for ch in ru if ch not in ("ъ", "ь", "й", "э")]
    return [ch for ch in ru if ch not in ("ъ", "ь")]


def load_prefixes(path: Path) -> list[str]:
    if not path.exists():
        raise CriticalPipelineError(f"Prefixes file not found: {path}")

    prefixes = [norm_query(line) for line in path.read_text(encoding="utf-8-sig").splitlines()]
    prefixes = [item for item in prefixes if item]
    if not prefixes:
        raise CriticalPipelineError(f"Prefixes file is empty: {path}")
    return prefixes


def _extract_suggestions_narrow(page: Page, prefix: str) -> list[str]:
    root = page.locator(SUGGEST_ROOT_SELECTOR)
    root.wait_for(state="attached", timeout=10_000)
    js = """
    (root, prefix) => {
      const p = (prefix || '').trim().toLowerCase();
      if (!p) return [];

      const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
      const isVisible = (el) => {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        if (style.visibility === 'hidden' || style.display === 'none') return false;
        const r = el.getBoundingClientRect();
        return r.width >= 2 && r.height >= 2;
      };

      const selectors = [
        '[data-testid*="suggest"] a',
        '[class*="suggest"] a',
        '[class*="autocomplete"] a',
        'ul li a'
      ];

      const out = [];
      const seen = new Set();
      const all = selectors.flatMap((s) => Array.from(root.querySelectorAll(s)));

      for (const el of all) {
        if (!isVisible(el)) continue;
        const t = norm(el.innerText || el.textContent);
        if (!t) continue;
        const tl = t.toLowerCase();
        if (!tl.startsWith(p)) continue;
        if (seen.has(tl)) continue;
        seen.add(tl);
        out.push(t);
      }

      return out;
    }
    """
    return root.evaluate(js, prefix)


def _extract_suggestions_fallback(page: Page, prefix: str) -> list[str]:
    root = page.locator(SUGGEST_ROOT_SELECTOR)
    root.wait_for(state="attached", timeout=10_000)

    js = """
    (root, prefix) => {
      const p = (prefix || '').trim().toLowerCase();
      if (!p) return [];

      const isVisible = (el) => {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        if (style.visibility === 'hidden' || style.display === 'none') return false;
        const rect = el.getBoundingClientRect();
        if (rect.width < 2 || rect.height < 2) return false;
        return true;
      };

      const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();

      const candidates = Array.from(root.querySelectorAll('a,button,li,div,span'))
        .filter(isVisible);

      const hasVisibleChildCandidate = (el) => {
        for (const child of el.querySelectorAll('a,button,li,div,span')) {
          if (child === el) continue;
          if (isVisible(child)) return true;
        }
        return false;
      };

      const out = [];
      const seen = new Set();

      for (const el of candidates) {
        if (hasVisibleChildCandidate(el)) continue;

        const t = norm(el.innerText || el.textContent);
        if (t.length < 2 || t.length > 80) continue;

        const tl = t.toLowerCase();
        if (!tl.startsWith(p)) continue;

        const words = tl.split(' ').filter(Boolean).length;
        if (words > 10) continue;

        if (!seen.has(tl)) {
          seen.add(tl);
          out.push(t);
        }
      }

      return out;
    }
    """
    return root.evaluate(js, prefix)


def extract_suggestions_from_root(page: Page, prefix: str) -> list[str]:
    prefix = norm_query(prefix)
    if not prefix:
        return []

    narrow = _extract_suggestions_narrow(page, prefix)
    if narrow:
        return narrow
    return _extract_suggestions_fallback(page, prefix)


def _resolve_path(config: AppConfig, path_value: str) -> Path:
    p = Path(path_value)
    if p.is_absolute():
        return p
    return (config.project_root / p).resolve()


def _checkpoint_key(base_prefix: str, letter: str, depth: int) -> str:
    return CHECKPOINT_KEY_TEMPLATE.format(prefix=base_prefix, letter=letter, depth=depth)


def run_suggest_collection(config: AppConfig, db: StateDB, ctx: RunContext) -> dict[str, int | str]:
    logger = get_logger("suggest")
    if sync_playwright is None:
        raise CriticalPipelineError("playwright is required for suggest component")
    proxy_route = require_marketplace_proxy(config.raw, browser=True)

    settings = config.raw.get("suggest", {})
    prefixes_path = _resolve_path(config, str(settings.get("prefixes_file", "config/prefixes.txt")))
    letters = build_letters(str(settings.get("alphabet_mode", "ru30")))
    prefixes = load_prefixes(prefixes_path)

    throttle_ms = int(settings.get("throttle_ms", 900))
    type_delay_ms = int(settings.get("type_delay_ms", 50))
    headless = bool(settings.get("headless", False))
    browser_channel = str(settings.get("browser_channel", "")).strip()
    browser_executable_path = str(settings.get("browser_executable_path", "")).strip()
    max_typed = int(settings.get("max_typed_queries", 0))
    full_refresh = bool(settings.get("full_refresh_checkpoints", False)) or bool(settings.get("force_full_refresh", False))
    empty_checkpoint_policy = str(settings.get("empty_checkpoint_policy", "reprocess")).lower().strip()
    browser_profile_dir = _resolve_path(config, str(settings.get("browser_profile_dir", "state/browser/wb_profile")))
    browser_profile_dir.mkdir(parents=True, exist_ok=True)

    if full_refresh:
        db.delete_checkpoints(COMPONENT_SUGGEST)

    completed_keys: set[str] = set()
    for key in db.list_checkpoint_keys(COMPONENT_SUGGEST):
        value = db.get_checkpoint(COMPONENT_SUGGEST, key) or ""
        status_meta = value.split("|", 1)[0].strip().lower()
        if status_meta == CHECKPOINT_VALUE_SUCCESS:
            completed_keys.add(key)
        elif status_meta == CHECKPOINT_VALUE_EMPTY and empty_checkpoint_policy == "mark_done":
            completed_keys.add(key)

    raw_path = config.paths.output_path(
        layer="raw",
        component=COMPONENT_SUGGEST,
        run_id=ctx.run_id,
        filename="suggest_alpha_raw.csv",
    )
    staging_path = config.paths.output_path(
        layer="staging",
        component=COMPONENT_SUGGEST,
        run_id=ctx.run_id,
        filename="suggest_alpha_staging.csv",
    )

    raw_fields = [
        "run_id",
        "component",
        "collected_at_utc",
        "source_system",
        "source_type",
        "source_ref",
        "status",
        "error_message",
        "base_prefix",
        "typed_query",
        "letter",
        "depth",
        "position",
        "list_size",
        "suggestion",
    ]
    staging_fields = raw_fields + ["suggestion_lc", "is_empty_suggestion"]

    source_system = str(config.raw.get("project", {}).get("source_system", "wildberries"))
    source_type = "wb_suggest_dom"

    typed_total = 0
    typed_processed = 0
    typed_skipped = 0
    typed_success = 0
    typed_empty = 0
    typed_errors = 0
    rows_written = 0

    with sync_playwright() as p:
        launch_kwargs: dict[str, Any] = {
            "user_data_dir": str(browser_profile_dir),
            "headless": headless,
            "proxy": proxy_route.playwright_proxy(),
        }
        if browser_channel:
            launch_kwargs["channel"] = browser_channel
        elif browser_executable_path:
            launch_kwargs["executable_path"] = browser_executable_path

        context = with_retry(
            lambda: p.chromium.launch_persistent_context(**launch_kwargs),
            attempts=config.runtime.retry_max_attempts,
            base_delay=config.runtime.retry_base_delay_seconds,
            max_delay=config.runtime.retry_max_delay_seconds,
        )

        try:
            page = context.new_page()
            with_retry(
                lambda: page.goto(WB_URL, wait_until="domcontentloaded", timeout=config.runtime.http_timeout_seconds * 1000),
                attempts=config.runtime.retry_max_attempts,
                base_delay=config.runtime.retry_base_delay_seconds,
                max_delay=config.runtime.retry_max_delay_seconds,
            )

            search = page.get_by_test_id(SEARCH_INPUT_TESTID)
            with_retry(
                lambda: search.wait_for(state="visible", timeout=60_000),
                attempts=config.runtime.retry_max_attempts,
                base_delay=config.runtime.retry_base_delay_seconds,
                max_delay=config.runtime.retry_max_delay_seconds,
            )
            page.wait_for_timeout(1_000)

            stop = False
            for base_prefix in prefixes:
                if stop:
                    break

                typed_tasks: list[tuple[str, str, int]] = [(base_prefix, "seed", 0)]
                typed_tasks.extend((f"{base_prefix} {letter}", letter, 1) for letter in letters)

                for typed_query, letter_tag, depth in typed_tasks:
                    if max_typed and typed_total >= max_typed:
                        stop = True
                        break

                    typed_total += 1
                    key = _checkpoint_key(base_prefix=base_prefix, letter=letter_tag, depth=depth)
                    if key in completed_keys:
                        typed_skipped += 1
                        continue

                    typed_query = norm_query(typed_query)
                    if not typed_query:
                        continue

                    collected_at_utc = utc_now_iso()
                    status = CHECKPOINT_VALUE_SUCCESS
                    error_message = ""

                    try:
                        def _type_and_extract() -> list[str]:
                            search.click()
                            search.fill("")
                            search.type(typed_query, delay=type_delay_ms)
                            page.wait_for_timeout(throttle_ms)
                            return extract_suggestions_from_root(page, typed_query)

                        suggestions = with_retry(
                            _type_and_extract,
                            attempts=config.runtime.retry_max_attempts,
                            base_delay=config.runtime.retry_base_delay_seconds,
                            max_delay=config.runtime.retry_max_delay_seconds,
                        )
                    except Exception as exc:
                        status = CHECKPOINT_VALUE_ERROR
                        error_message = f"browser_failed:{exc.__class__.__name__}"
                        suggestions = []

                    list_size = len(suggestions)
                    if status == CHECKPOINT_VALUE_ERROR:
                        typed_errors += 1
                    elif list_size == 0:
                        status = CHECKPOINT_VALUE_EMPTY
                        typed_empty += 1
                    else:
                        typed_success += 1

                    if not suggestions:
                        raw_rows = [{
                            "run_id": ctx.run_id,
                            "component": COMPONENT_SUGGEST,
                            "collected_at_utc": collected_at_utc,
                            "source_system": source_system,
                            "source_type": source_type,
                            "source_ref": typed_query,
                            "status": status,
                            "error_message": error_message,
                            "base_prefix": base_prefix,
                            "typed_query": typed_query,
                            "letter": letter_tag,
                            "depth": depth,
                            "position": 0,
                            "list_size": 0,
                            "suggestion": "",
                        }]
                    else:
                        raw_rows = []
                        for pos, suggestion in enumerate(suggestions, start=1):
                            raw_rows.append(
                                {
                                    "run_id": ctx.run_id,
                                    "component": COMPONENT_SUGGEST,
                                    "collected_at_utc": collected_at_utc,
                                    "source_system": source_system,
                                    "source_type": source_type,
                                    "source_ref": typed_query,
                                    "status": status,
                                    "error_message": error_message,
                                    "base_prefix": base_prefix,
                                    "typed_query": typed_query,
                                    "letter": letter_tag,
                                    "depth": depth,
                                    "position": pos,
                                    "list_size": list_size,
                                    "suggestion": norm_query(suggestion),
                                }
                            )

                    staging_rows = []
                    for row in raw_rows:
                        suggestion = str(row["suggestion"])
                        staging_rows.append(
                            {
                                **row,
                                "suggestion_lc": norm_lc(suggestion),
                                "is_empty_suggestion": 1 if not suggestion else 0,
                            }
                        )

                    append_csv_rows(raw_path, raw_rows, raw_fields)
                    append_csv_rows(staging_path, staging_rows, staging_fields)
                    rows_written += len(raw_rows)
                    typed_processed += 1

                    # Checkpoint semantics:
                    # - success: mark as completed
                    # - empty: optional checkpoint depending on policy (default reprocess)
                    # - error: do not mark completed
                    if status == CHECKPOINT_VALUE_SUCCESS:
                        db.save_checkpoint(
                            component=COMPONENT_SUGGEST,
                            checkpoint_key=key,
                            checkpoint_value=f"{CHECKPOINT_VALUE_SUCCESS}|{typed_query}|{collected_at_utc}",
                            updated_at_utc=collected_at_utc,
                        )
                        completed_keys.add(key)
                    elif status == CHECKPOINT_VALUE_EMPTY and empty_checkpoint_policy == "mark_done":
                        db.save_checkpoint(
                            component=COMPONENT_SUGGEST,
                            checkpoint_key=key,
                            checkpoint_value=f"{CHECKPOINT_VALUE_EMPTY}|{typed_query}|{collected_at_utc}",
                            updated_at_utc=collected_at_utc,
                        )
                    else:
                        logger.warning(
                            "suggest_typed_non_success",
                            extra={
                                "run_id": ctx.run_id,
                                "pipeline": ctx.pipeline,
                                "component": COMPONENT_SUGGEST,
                                "status": status,
                            },
                        )

        finally:
            context.close()

    return {
        "items_ok": rows_written,
        "items_error": typed_errors,
        "non_critical_errors": typed_errors,
        "typed_total": typed_total,
        "typed_processed": typed_processed,
        "typed_success": typed_success,
        "typed_empty": typed_empty,
        "typed_skipped": typed_skipped,
        "note": (
            f"typed_total={typed_total}, processed={typed_processed}, success={typed_success}, "
            f"empty={typed_empty}, errors={typed_errors}, skipped={typed_skipped}, "
            f"raw={raw_path.name}, staging={staging_path.name}"
        ),
        "raw_path": str(raw_path),
        "staging_path": str(staging_path),
    }
