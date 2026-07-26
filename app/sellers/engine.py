from __future__ import annotations

import json
import os
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests

from app.common.config import AppConfig
from app.common.constants import COMPONENT_SELLERS, COMPONENT_SERP, ERROR_CODE_NETWORK, ERROR_SEVERITY_NON_CRITICAL
from app.common.csv_io import append_csv_rows, read_csv_rows, write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.logging_setup import get_logger
from app.common.proxy_required import build_requests_session, require_marketplace_proxy
from app.common.retry import with_retry
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB


@dataclass(slots=True)
class SellerSeed:
    supplier_id: str
    supplier_name: str = ""
    product_run_ids: set[str] = field(default_factory=set)
    queries: set[str] = field(default_factory=set)
    query_groups: set[str] = field(default_factory=set)
    nm_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class SellersRunScope:
    input_products_path: Path
    raw_dir: Path
    staging_dir: Path
    mart_dir: Path
    checkpoint_component: str
    component: str = "sellers_regional"
    publish_latest: bool = False
    request_timeout_provider: Callable[[float], float] | None = None


class RetryableHttpStatusError(requests.RequestException):
    def __init__(self, response: requests.Response) -> None:
        self.response = response
        super().__init__(f"HTTP {response.status_code}", response=response)


def _as_path(project_root: Path, value: str) -> Path:
    p = Path(value)
    if not p.is_absolute():
        p = project_root / p
    return p


def _norm(value: str) -> str:
    return " ".join((value or "").strip().split())


def _safe_supplier_id(value: str) -> str:
    v = "".join(ch for ch in (value or "") if ch.isdigit())
    return v.strip()


class SellersEngine:
    def __init__(
        self,
        config: AppConfig,
        db: StateDB,
        ctx: RunContext,
        *,
        run_scope: SellersRunScope | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.ctx = ctx
        self.run_scope = run_scope
        self.component = run_scope.component if run_scope else COMPONENT_SELLERS
        self.checkpoint_component = (
            run_scope.checkpoint_component if run_scope else COMPONENT_SELLERS
        )
        self.logger = get_logger("sellers")

        self.sellers_cfg = self.config.raw.get("sellers", {})
        self.source_system = str(self.config.raw.get("project", {}).get("source_system", "wildberries"))
        self.source_type = "wb_suppliers_shipment_api_v1"

        self.base_url = str(self.sellers_cfg.get("api_base_url", "https://suppliers-shipment-2.wildberries.ru/api/v1/suppliers")).rstrip("/")
        self.curr = str(self.sellers_cfg.get("curr", "RUB"))
        self.user_agent = str(self.sellers_cfg.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"))
        self.sleep_between_sellers_ms = int(self.sellers_cfg.get("sleep_between_sellers_ms", 250))

        out_cfg = self.sellers_cfg.get("output_files", {})
        self.raw_name = str(out_cfg.get("raw_sellers_csv", "sellers_raw.csv"))
        self.staging_name = str(out_cfg.get("staging_sellers_csv", "sellers_staging.csv"))
        self.mart_name = str(out_cfg.get("mart_sellers_daily_csv", "sellers_daily.csv"))
        self.bridge_name = str(out_cfg.get("bridge_csv", "seller_query_product_bridge.csv"))

        if run_scope is None:
            self.raw_dir = self.config.paths.layer_component_run_dir(
                "raw",
                COMPONENT_SELLERS,
                self.ctx.run_id,
            )
            self.staging_dir = self.config.paths.layer_component_run_dir(
                "staging",
                COMPONENT_SELLERS,
                self.ctx.run_id,
            )
            self.mart_dir = self.config.paths.layer_component_run_dir(
                "marts",
                COMPONENT_SELLERS,
                self.ctx.run_id,
            )
        else:
            self._validate_run_scope(run_scope)
            self.raw_dir = run_scope.raw_dir
            self.staging_dir = run_scope.staging_dir
            self.mart_dir = run_scope.mart_dir
            for directory in (self.raw_dir, self.staging_dir, self.mart_dir):
                directory.mkdir(parents=True, exist_ok=True)
        raw_subdir = str(self.sellers_cfg.get("raw_responses_subdir", "responses"))
        self.raw_responses_dir = self.raw_dir / raw_subdir
        self.raw_responses_dir.mkdir(parents=True, exist_ok=True)

    def _validate_run_scope(self, scope: SellersRunScope) -> None:
        root = self.config.project_root.resolve()
        for field_name, path in (
            ("input_products_path", scope.input_products_path),
            ("raw_dir", scope.raw_dir),
            ("staging_dir", scope.staging_dir),
            ("mart_dir", scope.mart_dir),
        ):
            candidate = Path(os.path.abspath(path))
            try:
                relative = candidate.relative_to(root)
            except ValueError as exc:
                raise CriticalPipelineError(
                    f"scoped sellers {field_name} must be inside project root"
                ) from exc
            current = root
            for part in relative.parts:
                current /= part
                if current.is_symlink():
                    raise CriticalPipelineError(
                        f"scoped sellers {field_name} must not use symlinks"
                    )
        if scope.publish_latest:
            raise CriticalPipelineError("scoped sellers cannot publish global latest")
        if not scope.checkpoint_component.startswith("sellers_regional:"):
            raise CriticalPipelineError(
                "scoped sellers checkpoint component is invalid"
            )

    def run(self) -> dict[str, int | str]:
        full_refresh = bool(self.sellers_cfg.get("full_refresh_checkpoints", False))
        if full_refresh:
            self.db.delete_checkpoints(self.checkpoint_component)

        products_path = self._resolve_products_input_path()
        product_rows = self._load_products_rows(products_path)
        seeds = self._extract_unique_sellers(product_rows)
        bridge_rows = self._build_bridge_rows(product_rows)

        if not seeds:
            raise CriticalPipelineError("Sellers has no supplier_id values in products input")

        raw_path = self.raw_dir / self.raw_name
        staging_path = self.staging_dir / self.staging_name
        mart_path = self.mart_dir / self.mart_name
        bridge_path = self.mart_dir / self.bridge_name

        raw_fields, staging_fields, mart_fields, bridge_fields = self._fields()

        completed: set[str] = set()
        for key in self.db.list_checkpoint_keys(self.checkpoint_component):
            value = self.db.get_checkpoint(self.checkpoint_component, key) or ""
            if value.startswith("success|") and f"|{self.ctx.run_id}|" in value:
                completed.add(key)

        items_ok = 0
        items_error = 0
        processed = 0
        dry_run = bool(self.config.runtime.dry_run)

        session_cm = nullcontext(None) if dry_run else self._build_session()
        with session_cm as session:
            for seller_id, seed in seeds.items():
                if seller_id in completed:
                    continue

                collected_at = utc_now_iso()
                source_ref = f"seller:{seller_id}"

                if dry_run:
                    status = "dry_run"
                    error_message = ""
                    http_status = 0
                    payload: dict[str, Any] | None = None
                    raw_file = ""
                    raw_json_fragment = ""
                else:
                    http_status, payload, error_message, raw_file = self._fetch_seller(session, seller_id)
                    status = "success" if payload is not None and http_status == 200 and not error_message else "error"
                    raw_json_fragment = json.dumps(payload, ensure_ascii=False) if payload is not None else ""

                row = self._seller_row(
                    seed=seed,
                    seller_id=seller_id,
                    payload=payload,
                    status=status,
                    error_message=error_message,
                    http_status=http_status,
                    source_ref=source_ref,
                    collected_at_utc=collected_at,
                    raw_file=raw_file,
                    raw_json_fragment=raw_json_fragment,
                )

                append_csv_rows(raw_path, [row], raw_fields)
                append_csv_rows(staging_path, [row], staging_fields)
                append_csv_rows(mart_path, [{k: row.get(k, "") for k in mart_fields}], mart_fields)

                processed += 1
                if status == "success" or status == "dry_run":
                    items_ok += 1
                    self.db.save_checkpoint(
                        component=self.checkpoint_component,
                        checkpoint_key=seller_id,
                        checkpoint_value=f"success|{self.ctx.run_id}|{collected_at}",
                        updated_at_utc=collected_at,
                    )
                else:
                    items_error += 1
                    self.db.record_error(
                        run_id=self.ctx.run_id,
                        component=self.component,
                        severity=ERROR_SEVERITY_NON_CRITICAL,
                        error_class="SellerFetchError",
                        error_message=error_message,
                        source_ref=source_ref,
                        created_at_utc=collected_at,
                        error_code=ERROR_CODE_NETWORK,
                    )

                if self.sleep_between_sellers_ms > 0 and not dry_run:
                    time.sleep(self.sleep_between_sellers_ms / 1000.0)

        invocation_processed = processed
        write_csv_rows(bridge_path, bridge_rows, bridge_fields)

        if self.run_scope is not None and not dry_run:
            items_ok, items_error, processed = self._canonicalize_scoped_mart(
                mart_path=mart_path,
                mart_fields=mart_fields,
                seeds=seeds,
                completed=completed,
            )
        if items_ok == 0 and not dry_run:
            raise CriticalPipelineError("Sellers collected zero successful rows")

        latest_raw = ""
        latest_staging = ""
        latest_mart = ""
        latest_bridge = ""
        if self.run_scope is None:
            latest_raw = str(
                self.config.paths.publish_latest_output(
                    layer="raw",
                    component=COMPONENT_SELLERS,
                    source_path=raw_path,
                    filename=self.raw_name,
                )
            )
            latest_staging = str(
                self.config.paths.publish_latest_output(
                    layer="staging",
                    component=COMPONENT_SELLERS,
                    source_path=staging_path,
                    filename=self.staging_name,
                )
            )
            latest_mart = str(
                self.config.paths.publish_latest_output(
                    layer="marts",
                    component=COMPONENT_SELLERS,
                    source_path=mart_path,
                    filename=self.mart_name,
                )
            )
            latest_bridge = str(
                self.config.paths.publish_latest_output(
                    layer="marts",
                    component=COMPONENT_SELLERS,
                    source_path=bridge_path,
                    filename=self.bridge_name,
                )
            )

        run_status = (
            "dry_run"
            if dry_run
            else (
                "success"
                if items_ok == len(seeds) and items_error == 0
                else "partial"
            )
        )
        return {
            "status": run_status,
            "items_ok": items_ok,
            "items_error": items_error,
            "non_critical_errors": items_error,
            "processed_sellers": processed,
            "invocation_processed_sellers": invocation_processed,
            "input_products_path": str(products_path),
            "raw_sellers_path": str(raw_path),
            "staging_sellers_path": str(staging_path),
            "mart_sellers_path": str(mart_path),
            "bridge_path": str(bridge_path),
            "latest_raw_sellers_path": latest_raw,
            "latest_staging_sellers_path": latest_staging,
            "latest_mart_sellers_path": latest_mart,
            "latest_bridge_path": latest_bridge,
            "note": (
                f"sellers={len(seeds)} processed={processed} "
                f"invocation_processed={invocation_processed} "
                f"ok={items_ok} err={items_error}"
            ),
        }

    def _canonicalize_scoped_mart(
        self,
        *,
        mart_path: Path,
        mart_fields: list[str],
        seeds: dict[str, SellerSeed],
        completed: set[str],
    ) -> tuple[int, int, int]:
        if not mart_path.is_file() or mart_path.is_symlink():
            raise CriticalPipelineError("Scoped sellers mart is unavailable")
        if not completed.issubset(seeds):
            raise CriticalPipelineError(
                "Scoped sellers checkpoints contain an unknown seller"
            )
        latest_by_seller: dict[str, dict[str, str]] = {}
        for row in read_csv_rows(mart_path):
            seller_id = _safe_supplier_id(row.get("supplier_id", ""))
            if not seller_id or seller_id not in seeds:
                raise CriticalPipelineError(
                    "Scoped sellers mart contains an unknown seller"
                )
            latest_by_seller[seller_id] = row
        if set(latest_by_seller) != set(seeds):
            raise CriticalPipelineError("Scoped sellers mart is incomplete")
        for seller_id in completed:
            if latest_by_seller[seller_id].get("status") != "success":
                raise CriticalPipelineError(
                    "Scoped sellers checkpoint does not match successful mart row"
                )

        canonical_rows = [
            {
                field: latest_by_seller[seller_id].get(field, "")
                for field in mart_fields
            }
            for seller_id in seeds
        ]
        write_csv_rows(mart_path, canonical_rows, mart_fields)
        verified_rows = read_csv_rows(mart_path)
        verified_ids = [
            _safe_supplier_id(row.get("supplier_id", ""))
            for row in verified_rows
        ]
        if (
            verified_ids != list(seeds)
            or len(set(verified_ids)) != len(verified_ids)
        ):
            raise CriticalPipelineError("Scoped sellers mart is not canonical")
        items_ok = sum(
            row.get("status") == "success" for row in verified_rows
        )
        items_error = len(verified_rows) - items_ok
        return items_ok, items_error, len(verified_rows)

    def _resolve_products_input_path(self) -> Path:
        if self.run_scope is not None:
            path = self.run_scope.input_products_path
            if not path.is_file() or path.is_symlink():
                raise CriticalPipelineError(
                    "Scoped sellers input products file is unavailable"
                )
            return path
        cfg_path = str(self.sellers_cfg.get("input_files", {}).get("products_daily_csv", "")).strip()
        if cfg_path:
            p = _as_path(self.config.project_root, cfg_path)
            if p.exists():
                return p
            if "latest" in p.parts:
                fallback = self.config.paths.latest_output_path(layer="marts", component=COMPONENT_SERP, filename=p.name)
                if fallback is not None:
                    return fallback

        fallback = self.config.paths.latest_output_path(layer="marts", component=COMPONENT_SERP, filename="products_daily.csv")
        if fallback is not None:
            return fallback

        raise CriticalPipelineError("Sellers input products_daily.csv not found")

    def _load_products_rows(self, path: Path) -> list[dict[str, str]]:
        rows = read_csv_rows(path)
        if not rows:
            raise CriticalPipelineError(f"Sellers input products file is empty: {path}")
        return rows

    def _extract_unique_sellers(self, product_rows: list[dict[str, str]]) -> dict[str, SellerSeed]:
        sellers: dict[str, SellerSeed] = {}

        for row in product_rows:
            seller_id = _safe_supplier_id(str(row.get("supplier_id") or row.get("supplierId") or ""))
            if not seller_id:
                continue

            seed = sellers.get(seller_id)
            if seed is None:
                seed = SellerSeed(supplier_id=seller_id)
                sellers[seller_id] = seed

            supplier_name = _norm(str(row.get("supplier_name") or row.get("supplier") or ""))
            if supplier_name and not seed.supplier_name:
                seed.supplier_name = supplier_name

            product_run_id = _norm(str(row.get("run_id") or ""))
            if product_run_id:
                seed.product_run_ids.add(product_run_id)

            q = _norm(str(row.get("query") or ""))
            if q:
                seed.queries.add(q)

            qg = _norm(str(row.get("query_group") or row.get("niche") or ""))
            if qg:
                seed.query_groups.add(qg)

            nmid = _norm(str(row.get("nmId") or row.get("nm_id") or ""))
            if nmid:
                seed.nm_ids.add(nmid)

        return sellers

    def _build_bridge_rows(self, product_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
        collected_at = utc_now_iso()
        bridge_fields = self._fields()[3]
        seen: set[tuple[str, str, str, str, str]] = set()
        rows: list[dict[str, Any]] = []

        for row in product_rows:
            seller_id = _safe_supplier_id(str(row.get("supplier_id") or row.get("supplierId") or ""))
            if not seller_id:
                continue

            query = _norm(str(row.get("query") or ""))
            query_group = _norm(str(row.get("query_group") or row.get("niche") or ""))
            nmid = _norm(str(row.get("nmId") or row.get("nm_id") or ""))
            product_run_id = _norm(str(row.get("run_id") or ""))
            supplier_name = _norm(str(row.get("supplier_name") or row.get("supplier") or ""))

            key = (seller_id, query, query_group, nmid, product_run_id)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "run_id": self.ctx.run_id,
                    "component": self.component,
                    "collected_at_utc": collected_at,
                    "source_system": self.source_system,
                    "source_type": "seller_query_product_bridge",
                    "source_ref": f"{seller_id}|{nmid}|{query}",
                    "status": "success",
                    "error_message": "",
                    "supplier_id": seller_id,
                    "supplier_name": supplier_name,
                    "query": query,
                    "query_group": query_group,
                    "nmId": nmid,
                    "product_run_id": product_run_id,
                }
            )

        if not rows:
            rows.append({k: "" for k in bridge_fields})
            rows[0]["run_id"] = self.ctx.run_id
            rows[0]["component"] = self.component
            rows[0]["collected_at_utc"] = collected_at
            rows[0]["source_system"] = self.source_system
            rows[0]["source_type"] = "seller_query_product_bridge"
            rows[0]["status"] = "empty"
            rows[0]["error_message"] = "no_links"

        return rows

    def _build_session(self) -> requests.Session:
        session = build_requests_session(
            require_marketplace_proxy(self.config.raw)
        )
        headers_cfg = self.sellers_cfg.get("request_headers", {})
        base_headers = {
            "accept": "*/*",
            "accept-language": "ru,en;q=0.9,en-US;q=0.8",
            "origin": "https://www.wildberries.ru",
            "x-client-name": "site",
            "user-agent": self.user_agent,
        }
        for k, v in headers_cfg.items():
            if isinstance(k, str):
                base_headers[k] = str(v)
        session.headers.update(base_headers)
        return session

    def _write_raw_response(self, seller_id: str, content: bytes) -> str:
        file_path = self.raw_responses_dir / f"supplier_{seller_id}.json"
        file_path.write_bytes(content)
        try:
            return file_path.relative_to(self.config.project_root).as_posix()
        except ValueError:
            return str(file_path)

    def _close_retry_response(self, attempt: int, delay: float, exc: Exception) -> None:
        response = getattr(exc, "response", None)
        if response is not None:
            response.close()
        self.logger.warning(
            "retry_scheduled",
            extra={
                "attempt": attempt,
                "max_attempts": self.config.runtime.retry_max_attempts,
                "delay_seconds": round(delay, 3),
                "error_class": exc.__class__.__name__,
                "error_message": "retryable_request_failed",
            },
        )

    def _fetch_seller(
        self,
        session: requests.Session | None,
        seller_id: str,
    ) -> tuple[int, dict[str, Any] | None, str, str]:
        if session is None:
            return 0, None, "session_missing", ""

        url = f"{self.base_url}/{quote(seller_id)}?curr={quote(self.curr)}"
        referer = f"https://www.wildberries.ru/seller/{quote(seller_id)}"

        def _request() -> requests.Response:
            timeout = float(self.config.runtime.http_timeout_seconds)
            if (
                self.run_scope is not None
                and self.run_scope.request_timeout_provider is not None
            ):
                timeout = self.run_scope.request_timeout_provider(timeout)
            resp = session.get(
                url,
                headers={"referer": referer},
                timeout=timeout,
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
                on_retry=self._close_retry_response,
            )
        except RetryableHttpStatusError as exc:
            response = exc.response
            try:
                raw_file = self._write_raw_response(seller_id=seller_id, content=response.content)
                return response.status_code, None, f"http_{response.status_code}: {(response.text or '').strip()[:500]}", raw_file
            finally:
                response.close()
        except CriticalPipelineError:
            raise
        except Exception as exc:
            return 0, None, f"request_failed:{exc.__class__.__name__}", ""

        try:
            raw_file = self._write_raw_response(seller_id=seller_id, content=response.content)

            if response.status_code != 200:
                return response.status_code, None, f"http_{response.status_code}: {(response.text or '').strip()[:500]}", raw_file

            try:
                payload = response.json()
                if not isinstance(payload, dict):
                    return response.status_code, None, "json_payload_not_object", raw_file
                return response.status_code, payload, "", raw_file
            except Exception as exc:
                return (
                    response.status_code,
                    None,
                    f"json_decode_failed:{exc.__class__.__name__}",
                    raw_file,
                )
        finally:
            response.close()

    def _seller_row(
        self,
        *,
        seed: SellerSeed,
        seller_id: str,
        payload: dict[str, Any] | None,
        status: str,
        error_message: str,
        http_status: int,
        source_ref: str,
        collected_at_utc: str,
        raw_file: str,
        raw_json_fragment: str,
    ) -> dict[str, Any]:
        payload = payload or {}
        supplier_name = (
            _norm(str(payload.get("name") or payload.get("supplierName") or ""))
            or seed.supplier_name
        )
        return {
            "run_id": self.ctx.run_id,
            "component": self.component,
            "collected_at_utc": collected_at_utc,
            "source_system": self.source_system,
            "source_type": self.source_type,
            "source_ref": source_ref,
            "status": status,
            "error_message": error_message,
            "supplier_id": seller_id,
            "supplier_name": supplier_name,
            "rating": payload.get("rating", ""),
            "valuation": payload.get("valuation", ""),
            "feedbacks_count": payload.get("feedbacksCount", ""),
            "sale_item_quantity": payload.get("saleItemQuantity", ""),
            "registration_date": payload.get("registrationDate", ""),
            "update_date": payload.get("updateDate", ""),
            "delivery_duration": payload.get("deliveryDuration", ""),
            "supp_ratio": payload.get("suppRatio", ""),
            "ratio_mark_supp": payload.get("ratioMarkSupp", ""),
            "rating_is_invisible": payload.get("ratingIsInvisible", ""),
            "http_status": http_status,
            "query_count": len(seed.queries),
            "product_count": len(seed.nm_ids),
            "queries_ref": "|".join(sorted(seed.queries)),
            "query_groups_ref": "|".join(sorted(seed.query_groups)),
            "nm_ids_ref": "|".join(sorted(seed.nm_ids)),
            "source_product_run_ids": "|".join(sorted(seed.product_run_ids)),
            "raw_file": raw_file,
            "raw_json_fragment": raw_json_fragment,
        }

    def _fields(self) -> tuple[list[str], list[str], list[str], list[str]]:
        base_fields = [
            "run_id",
            "component",
            "collected_at_utc",
            "source_system",
            "source_type",
            "source_ref",
            "status",
            "error_message",
            "supplier_id",
            "supplier_name",
            "rating",
            "valuation",
            "feedbacks_count",
            "sale_item_quantity",
            "registration_date",
            "update_date",
            "delivery_duration",
            "supp_ratio",
            "ratio_mark_supp",
            "rating_is_invisible",
            "http_status",
            "query_count",
            "product_count",
            "queries_ref",
            "query_groups_ref",
            "nm_ids_ref",
            "source_product_run_ids",
            "raw_file",
            "raw_json_fragment",
        ]
        mart_fields = [f for f in base_fields if f != "raw_json_fragment"]
        bridge_fields = [
            "run_id",
            "component",
            "collected_at_utc",
            "source_system",
            "source_type",
            "source_ref",
            "status",
            "error_message",
            "supplier_id",
            "supplier_name",
            "query",
            "query_group",
            "nmId",
            "product_run_id",
        ]
        return base_fields, base_fields, mart_fields, bridge_fields
