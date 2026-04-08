from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from app.common.config import AppConfig
from app.common.constants import COMPONENT_FILTER, COMPONENT_SERP, ERROR_CODE_NETWORK, ERROR_SEVERITY_NON_CRITICAL
from app.common.csv_io import append_csv_rows, read_csv_rows, write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.logging_setup import get_logger
from app.common.retry import with_retry
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB


@dataclass(slots=True)
class QueryTask:
    query: str
    niche: str


class RetryableHttpStatusError(requests.RequestException):
    def __init__(self, response: requests.Response) -> None:
        self.response = response
        super().__init__(f"HTTP {response.status_code}")


def _norm_query(value: str) -> str:
    return " ".join((value or "").strip().split())


def _as_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path


def _to_rub(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return round(float(value) / 100.0, 2)
    return None


def _safe_query_id(query: str) -> str:
    return hashlib.sha1(query.encode("utf-8")).hexdigest()[:12]


class SerpEngine:
    def __init__(self, config: AppConfig, db: StateDB, ctx: RunContext) -> None:
        self.config = config
        self.db = db
        self.ctx = ctx
        self.logger = get_logger("serp")

        self.serp_cfg = self.config.raw.get("serp", {})
        self.source_system = str(self.config.raw.get("project", {}).get("source_system", "wildberries"))
        self.source_type = "wb_serp_exactmatch_v18"

        self.base_url = str(self.serp_cfg.get("base_url", "https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search"))
        self.request_params = dict(self.serp_cfg.get("request_params", {}))
        self.user_agent = str(self.serp_cfg.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"))
        self.referer_base = str(self.serp_cfg.get("referer_base", "https://www.wildberries.ru/catalog/0/search.aspx?search="))
        self.x_requested_with = str(self.serp_cfg.get("x_requested_with", "XMLHttpRequest"))

        self.pages_per_query = int(self.serp_cfg.get("pages_per_query", 10))
        self.page_size = int(self.serp_cfg.get("page_size", 100))
        self.stop_on_empty_page = bool(self.serp_cfg.get("stop_on_empty_page", True))
        self.sleep_between_pages_ms = int(self.serp_cfg.get("sleep_between_pages_ms", 1300))
        self.sleep_between_queries_ms = int(self.serp_cfg.get("sleep_between_queries_ms", 2500))

        out_cfg = self.serp_cfg.get("output_files", {})
        self.raw_products_name = str(out_cfg.get("raw_products_csv", "products_raw.csv"))
        self.staging_products_name = str(out_cfg.get("staging_products_csv", "products_staging.csv"))
        self.mart_products_name = str(out_cfg.get("mart_products_daily_csv", "products_daily.csv"))
        self.pages_index_name = str(out_cfg.get("raw_pages_index_csv", "pages_raw_index.csv"))
        self.sellers_input_name = str(out_cfg.get("sellers_input_csv", "products_for_sellers.csv"))

        self.raw_run_dir = self.config.paths.layer_component_run_dir("raw", COMPONENT_SERP, self.ctx.run_id)
        self._query_slug_cache: dict[str, str] = {}
        self._used_query_slugs: set[str] = set()

    def run(self) -> dict[str, int | str]:
        tasks = self._load_query_tasks()
        if not tasks:
            raise CriticalPipelineError("SERP has no queries to process")

        dry_run = bool(self.config.runtime.dry_run)
        cookie_value = "" if dry_run else self._load_cookie_value()
        completed: set[str] = set()
        for key in self.db.list_checkpoint_keys(COMPONENT_SERP):
            value = self.db.get_checkpoint(COMPONENT_SERP, key) or ""
            if f"|{self.ctx.run_id}|" in value:
                completed.add(key)

        raw_products_path = self.config.paths.output_path(layer="raw", component=COMPONENT_SERP, run_id=self.ctx.run_id, filename=self.raw_products_name)
        staging_products_path = self.config.paths.output_path(layer="staging", component=COMPONENT_SERP, run_id=self.ctx.run_id, filename=self.staging_products_name)
        mart_products_path = self.config.paths.output_path(layer="marts", component=COMPONENT_SERP, run_id=self.ctx.run_id, filename=self.mart_products_name)
        pages_index_path = self.config.paths.output_path(layer="raw", component=COMPONENT_SERP, run_id=self.ctx.run_id, filename=self.pages_index_name)

        raw_fields, staging_fields, mart_fields, pages_index_fields = self._fields()

        items_ok = 0
        items_error = 0
        pages_done = 0
        queries_done = 0

        with self._build_session(cookie_value) as session:
            for task in tasks:
                query_done = False
                for page in range(1, self.pages_per_query + 1):
                    checkpoint_key = f"{task.query}|{page}"
                    if checkpoint_key in completed:
                        continue

                    collected_at = utc_now_iso()
                    source_ref = f"{task.query}|page={page}"
                    if dry_run:
                        dry_row = self._empty_product_row(
                            query=task.query,
                            niche=task.niche,
                            page=page,
                            collected_at_utc=collected_at,
                            status="dry_run",
                            error_message="",
                            source_ref=source_ref,
                            raw_file="",
                            page_size=self.page_size,
                        )
                        page_index_row = {
                            "run_id": self.ctx.run_id,
                            "component": COMPONENT_SERP,
                            "collected_at_utc": collected_at,
                            "source_system": self.source_system,
                            "source_type": self.source_type,
                            "source_ref": source_ref,
                            "status": "dry_run",
                            "error_message": "",
                            "query": task.query,
                            "query_group": task.niche,
                            "page": page,
                            "http_status": 0,
                            "products_count": 0,
                            "raw_file": "",
                            "raw_page_path": "",
                        }
                        append_csv_rows(raw_products_path, [dry_row], raw_fields)
                        append_csv_rows(staging_products_path, [dry_row], staging_fields)
                        append_csv_rows(mart_products_path, [{k: dry_row[k] for k in mart_fields}], mart_fields)
                        append_csv_rows(pages_index_path, [page_index_row], pages_index_fields)
                        self.db.save_checkpoint(COMPONENT_SERP, checkpoint_key, f"success|{self.ctx.run_id}|{collected_at}", collected_at)
                        completed.add(checkpoint_key)
                        items_ok += 1
                        pages_done += 1
                        query_done = True
                        break

                    response, payload, error_message, raw_file = self._fetch_page(session=session, query=task.query, page=page)
                    http_status = response.status_code if response is not None else 0
                    products: list[dict[str, Any]] = []

                    if payload is not None:
                        products = payload.get("products") or []

                    page_status = "success"
                    if error_message:
                        page_status = "error"
                    elif not products:
                        page_status = "empty"

                    page_index_row = {
                        "run_id": self.ctx.run_id,
                        "component": COMPONENT_SERP,
                        "collected_at_utc": collected_at,
                        "source_system": self.source_system,
                        "source_type": self.source_type,
                        "source_ref": source_ref,
                        "status": page_status,
                        "error_message": error_message,
                        "query": task.query,
                        "query_group": task.niche,
                        "page": page,
                        "http_status": http_status,
                        "products_count": len(products),
                        "raw_file": raw_file,
                        "raw_page_path": raw_file,
                    }
                    append_csv_rows(pages_index_path, [page_index_row], pages_index_fields)

                    if page_status == "error":
                        items_error += 1
                        self.db.record_error(
                            run_id=self.ctx.run_id,
                            component=COMPONENT_SERP,
                            severity=ERROR_SEVERITY_NON_CRITICAL,
                            error_class="SerpPageError",
                            error_message=error_message,
                            source_ref=source_ref,
                            created_at_utc=collected_at,
                            error_code=ERROR_CODE_NETWORK,
                        )
                        continue

                    if page_status == "empty":
                        empty_row = self._empty_product_row(
                            query=task.query,
                            niche=task.niche,
                            page=page,
                            collected_at_utc=collected_at,
                            status="empty",
                            error_message="",
                            source_ref=source_ref,
                            raw_file=raw_file,
                            page_size=self.page_size,
                        )
                        append_csv_rows(raw_products_path, [empty_row], raw_fields)
                        append_csv_rows(staging_products_path, [empty_row], staging_fields)
                        self.db.save_checkpoint(COMPONENT_SERP, checkpoint_key, f"empty|{self.ctx.run_id}|{collected_at}", collected_at)
                        completed.add(checkpoint_key)
                        pages_done += 1
                        if self.stop_on_empty_page:
                            query_done = True
                            break
                        continue

                    raw_rows: list[dict[str, Any]] = []
                    staging_rows: list[dict[str, Any]] = []
                    mart_rows: list[dict[str, Any]] = []

                    for idx, product in enumerate(products, start=1):
                        absolute_position = ((page - 1) * self.page_size) + idx
                        row = self._product_row(
                            product=product,
                            query=task.query,
                            niche=task.niche,
                            page=page,
                            position_on_page=idx,
                            absolute_position=absolute_position,
                            collected_at_utc=collected_at,
                            status="success",
                            error_message="",
                            source_ref=source_ref,
                            raw_file=raw_file,
                        )
                        raw_rows.append(row)
                        staging_rows.append(row)
                        mart_rows.append({k: row[k] for k in mart_fields})

                    append_csv_rows(raw_products_path, raw_rows, raw_fields)
                    append_csv_rows(staging_products_path, staging_rows, staging_fields)
                    append_csv_rows(mart_products_path, mart_rows, mart_fields)

                    self.db.save_checkpoint(COMPONENT_SERP, checkpoint_key, f"success|{self.ctx.run_id}|{collected_at}", collected_at)
                    completed.add(checkpoint_key)
                    items_ok += len(raw_rows)
                    pages_done += 1

                    if self.sleep_between_pages_ms > 0:
                        time.sleep(self.sleep_between_pages_ms / 1000.0)

                if query_done:
                    queries_done += 1
                if self.sleep_between_queries_ms > 0 and not dry_run:
                    time.sleep(self.sleep_between_queries_ms / 1000.0)

        if items_ok == 0 and not dry_run:
            raise CriticalPipelineError("SERP collected zero product rows")

        latest_raw = self.config.paths.publish_latest_output(layer="raw", component=COMPONENT_SERP, source_path=raw_products_path, filename=self.raw_products_name)
        latest_staging = self.config.paths.publish_latest_output(layer="staging", component=COMPONENT_SERP, source_path=staging_products_path, filename=self.staging_products_name)
        latest_mart = self.config.paths.publish_latest_output(layer="marts", component=COMPONENT_SERP, source_path=mart_products_path, filename=self.mart_products_name)
        latest_pages = self.config.paths.publish_latest_output(layer="raw", component=COMPONENT_SERP, source_path=pages_index_path, filename=self.pages_index_name)

        sellers_input_export = self.config.paths.EXPORTS_DIR / self.sellers_input_name
        sellers_input_export.parent.mkdir(parents=True, exist_ok=True)
        sellers_input_export.write_bytes(mart_products_path.read_bytes())

        preview_export_path = self._write_products_preview_export(mart_products_path)

        return {
            "items_ok": items_ok,
            "items_error": items_error,
            "non_critical_errors": items_error,
            "pages_done": pages_done,
            "queries_done": queries_done,
            "raw_products_path": str(raw_products_path),
            "staging_products_path": str(staging_products_path),
            "mart_products_path": str(mart_products_path),
            "pages_index_path": str(pages_index_path),
            "latest_raw_products_path": str(latest_raw),
            "latest_staging_products_path": str(latest_staging),
            "latest_mart_products_path": str(latest_mart),
            "latest_pages_index_path": str(latest_pages),
            "sellers_input_export": str(sellers_input_export),
            "products_daily_preview_export": str(preview_export_path),
            "note": f"queries={len(tasks)} pages={pages_done} ok={items_ok} err={items_error}",
        }

    def _build_session(self, cookie_value: str) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "user-agent": self.user_agent,
                "x-requested-with": self.x_requested_with,
                "accept": "application/json, text/plain, */*",
                "cookie": cookie_value,
            }
        )
        return session

    def _load_cookie_value(self) -> str:
        cookie_file = str(self.serp_cfg.get("wb_cookie_file", "")).strip()
        if not cookie_file:
            env_name = str(self.serp_cfg.get("wb_cookie_file_env", "WB_COOKIE_FILE")).strip()
            raise CriticalPipelineError(f"SERP cookie file is not configured. Set {env_name} or serp.wb_cookie_file")

        path = _as_path(self.config.project_root, cookie_file)
        if not path.exists():
            raise CriticalPipelineError(f"SERP cookie file not found: {path}")
        value = path.read_text(encoding="utf-8").strip()
        if not value:
            raise CriticalPipelineError(f"SERP cookie file is empty: {path}")
        return value

    def _resolve_top_queries_path(self) -> Path | None:
        top_path_cfg = str(self.serp_cfg.get("input_files", {}).get("top_queries_csv", "")).strip()
        if top_path_cfg:
            p = _as_path(self.config.project_root, top_path_cfg)
            if p.exists():
                return p
            if "latest" in p.parts:
                fallback = self.config.paths.latest_output_path(
                    layer="marts",
                    component=COMPONENT_FILTER,
                    filename=p.name,
                )
                if fallback is not None:
                    return fallback

        fallback = self.config.paths.latest_output_path(layer="marts", component=COMPONENT_FILTER, filename="top_queries.csv")
        return fallback

    def _resolve_queries_txt_path(self) -> Path | None:
        q_path_cfg = str(self.serp_cfg.get("input_files", {}).get("queries_txt", "")).strip()
        if q_path_cfg:
            p = _as_path(self.config.project_root, q_path_cfg)
            if p.exists():
                return p
        default_export = self.config.paths.EXPORTS_DIR / "queries.txt"
        if default_export.exists():
            return default_export
        return None

    def _load_query_tasks(self) -> list[QueryTask]:
        tasks: list[QueryTask] = []
        seen: set[str] = set()

        top_path = self._resolve_top_queries_path()
        if top_path and top_path.exists():
            for row in read_csv_rows(top_path):
                q = _norm_query((row.get("query") or row.get("normalized_query") or ""))
                if not q or q in seen:
                    continue
                niche = _norm_query((row.get("query_group") or row.get("niche") or ""))
                tasks.append(QueryTask(query=q, niche=niche))
                seen.add(q)

        txt_path = self._resolve_queries_txt_path()
        if txt_path and txt_path.exists():
            for line in txt_path.read_text(encoding="utf-8-sig").splitlines():
                q = _norm_query(line)
                if not q or q in seen:
                    continue
                tasks.append(QueryTask(query=q, niche=""))
                seen.add(q)

        return tasks

    def _query_slug(self, query: str) -> str:
        cached = self._query_slug_cache.get(query)
        if cached:
            return cached

        normalized = _norm_query(query).lower().replace("ё", "е")
        slug = re.sub(r"\s+", "-", normalized)
        slug = re.sub(r"[^\w\-]", "", slug, flags=re.UNICODE)
        slug = re.sub(r"\-+", "-", slug).strip("-_")
        if not slug:
            slug = f"query-{_safe_query_id(query)}"

        if slug in self._used_query_slugs:
            slug = f"{slug}-{_safe_query_id(query)[:6]}"

        i = 2
        base = slug
        while slug in self._used_query_slugs:
            slug = f"{base}-{i}"
            i += 1

        self._used_query_slugs.add(slug)
        self._query_slug_cache[query] = slug
        return slug

    def _write_raw_response(self, query: str, page: int, content: bytes) -> str:
        slug = self._query_slug(query)
        query_dir = self.raw_run_dir / slug
        query_dir.mkdir(parents=True, exist_ok=True)
        file_path = query_dir / f"page_{page}.json"
        file_path.write_bytes(content)
        try:
            return file_path.relative_to(self.config.project_root).as_posix()
        except ValueError:
            return str(file_path)

    def _short_http_error_text(self, text: str) -> str:
        return (text or "").strip().replace("\n", " ")[:500]

    def _fetch_page(
        self,
        session: requests.Session,
        query: str,
        page: int,
    ) -> tuple[requests.Response | None, dict[str, Any] | None, str, str]:
        referer = f"{self.referer_base}{quote(query)}"

        params = dict(self.request_params)
        params["query"] = query
        params["page"] = str(page)

        def _request() -> requests.Response:
            resp = session.get(
                self.base_url,
                params=params,
                headers={"referer": referer},
                timeout=self.config.runtime.http_timeout_seconds,
            )
            if resp.status_code >= 500:
                raise RetryableHttpStatusError(resp)
            return resp

        try:
            response = with_retry(
                _request,
                attempts=self.config.runtime.retry_max_attempts,
                base_delay=self.config.runtime.retry_base_delay_seconds,
                max_delay=self.config.runtime.retry_max_delay_seconds,
                retriable_exceptions=(requests.RequestException,),
            )
        except RetryableHttpStatusError as exc:
            response = exc.response
            raw_file = self._write_raw_response(query=query, page=page, content=response.content)
            return response, None, f"http_{response.status_code}: {self._short_http_error_text(response.text)}", raw_file
        except Exception as exc:
            return None, None, f"request_failed: {exc}", ""

        raw_file = self._write_raw_response(query=query, page=page, content=response.content)

        if response.status_code != 200:
            return response, None, f"http_{response.status_code}: {self._short_http_error_text(response.text)}", raw_file

        try:
            payload = response.json()
            if not isinstance(payload, dict):
                return response, None, "json_payload_not_object", raw_file
            return response, payload, "", raw_file
        except Exception as exc:
            return response, None, f"json_decode_failed: {exc}", raw_file

    def _extract_prices(self, product: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
        sizes = product.get("sizes") or []
        basic = None
        sale = None
        if sizes and isinstance(sizes, list):
            first = sizes[0] if sizes else {}
            price_obj = first.get("price") if isinstance(first, dict) else {}
            if isinstance(price_obj, dict):
                basic = _to_rub(price_obj.get("basic"))
                sale = _to_rub(price_obj.get("product"))
        final = sale if sale is not None else basic
        return final, basic, sale

    def _flags_badges(self, product: dict[str, Any]) -> str:
        keys = ["badges", "stickers", "log", "promoTextCard", "isBestSeller", "isNew", "flags"]
        data = {k: product.get(k) for k in keys if k in product and product.get(k) is not None}
        if not data:
            return ""
        return json.dumps(data, ensure_ascii=False)

    def _empty_product_row(
        self,
        *,
        query: str,
        niche: str,
        page: int,
        collected_at_utc: str,
        status: str,
        error_message: str,
        source_ref: str,
        raw_file: str,
        page_size: int,
    ) -> dict[str, Any]:
        return {
            "run_id": self.ctx.run_id,
            "component": COMPONENT_SERP,
            "collected_at_utc": collected_at_utc,
            "source_system": self.source_system,
            "source_type": self.source_type,
            "source_ref": source_ref,
            "status": status,
            "error_message": error_message,
            "query": query,
            "query_group": niche,
            "page": page,
            "position_on_page": 0,
            "absolute_position": ((page - 1) * page_size),
            "nmId": "",
            "imtId": "",
            "product_name": "",
            "brand": "",
            "brandId": "",
            "supplier_id": "",
            "supplier_name": "",
            "final_price": "",
            "price": "",
            "sale_price": "",
            "discount": "",
            "sale": "",
            "rating": "",
            "feedbacks": "",
            "valuation": "",
            "total_quantity": "",
            "promo_markers": "",
            "raw_json_fragment": "",
            "raw_file": raw_file,
            "raw_page_path": raw_file,
        }

    def _product_row(
        self,
        *,
        product: dict[str, Any],
        query: str,
        niche: str,
        page: int,
        position_on_page: int,
        absolute_position: int,
        collected_at_utc: str,
        status: str,
        error_message: str,
        source_ref: str,
        raw_file: str,
    ) -> dict[str, Any]:
        nm_id = product.get("id") or product.get("nmId") or ""
        imt_id = product.get("imtId") or ""
        final_price, price, sale_price = self._extract_prices(product)
        return {
            "run_id": self.ctx.run_id,
            "component": COMPONENT_SERP,
            "collected_at_utc": collected_at_utc,
            "source_system": self.source_system,
            "source_type": self.source_type,
            "source_ref": source_ref,
            "status": status,
            "error_message": error_message,
            "query": query,
            "query_group": niche,
            "page": page,
            "position_on_page": position_on_page,
            "absolute_position": absolute_position,
            "nmId": nm_id,
            "imtId": imt_id,
            "product_name": product.get("name") or "",
            "brand": product.get("brand") or "",
            "brandId": product.get("brandId") or "",
            "supplier_id": product.get("supplierId") or "",
            "supplier_name": product.get("supplier") or "",
            "final_price": final_price if final_price is not None else "",
            "price": price if price is not None else "",
            "sale_price": sale_price if sale_price is not None else "",
            "discount": product.get("discount") if product.get("discount") is not None else "",
            "sale": product.get("sale") if product.get("sale") is not None else "",
            "rating": product.get("rating") if product.get("rating") is not None else "",
            "feedbacks": product.get("feedbacks") if product.get("feedbacks") is not None else "",
            "valuation": product.get("valuation") if product.get("valuation") is not None else "",
            "total_quantity": product.get("totalQuantity") if product.get("totalQuantity") is not None else product.get("stock", ""),
            "promo_markers": self._flags_badges(product),
            "raw_json_fragment": json.dumps(product, ensure_ascii=False),
            "raw_file": raw_file,
            "raw_page_path": raw_file,
        }


    def _write_products_preview_export(self, mart_products_path: Path) -> Path:
        preview_path = self.config.paths.EXPORTS_DIR / "products_daily_preview.csv"
        preview_fields = [
            "query",
            "page",
            "position_on_page",
            "absolute_position",
            "nmId",
            "product_name",
            "brand",
            "supplier_id",
            "supplier_name",
            "final_price",
            "price",
            "sale_price",
            "rating",
            "feedbacks",
            "raw_file",
            "run_id",
            "collected_at_utc",
        ]

        source_rows = read_csv_rows(mart_products_path)
        preview_rows: list[dict[str, Any]] = []
        for row in source_rows:
            mapped: dict[str, Any] = {}
            for col in preview_fields:
                value = row.get(col, "")
                mapped[col] = "" if value is None else value
            preview_rows.append(mapped)

        write_csv_rows(preview_path, preview_rows, preview_fields)
        return preview_path
    def _fields(self) -> tuple[list[str], list[str], list[str], list[str]]:
        product_fields = [
            "run_id",
            "component",
            "collected_at_utc",
            "source_system",
            "source_type",
            "source_ref",
            "status",
            "error_message",
            "query",
            "query_group",
            "page",
            "position_on_page",
            "absolute_position",
            "nmId",
            "imtId",
            "product_name",
            "brand",
            "brandId",
            "supplier_id",
            "supplier_name",
            "final_price",
            "price",
            "sale_price",
            "discount",
            "sale",
            "rating",
            "feedbacks",
            "valuation",
            "total_quantity",
            "promo_markers",
            "raw_json_fragment",
            "raw_file",
            "raw_page_path",
        ]
        mart_fields = [f for f in product_fields if f != "raw_json_fragment"]
        pages_index_fields = [
            "run_id",
            "component",
            "collected_at_utc",
            "source_system",
            "source_type",
            "source_ref",
            "status",
            "error_message",
            "query",
            "query_group",
            "page",
            "http_status",
            "products_count",
            "raw_file",
            "raw_page_path",
        ]
        return product_fields, product_fields, mart_fields, pages_index_fields







