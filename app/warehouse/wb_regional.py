from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import stat
import tempfile
import time
import uuid
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import duckdb

from app.common.exceptions import CriticalPipelineError
from app.common.run_lock import acquire_run_lock
from app.serp.collection_plan_runner import acquire_advisory_lock


REGIONAL_WAREHOUSE_SCHEMA = "wb_regional_warehouse_v3"
LEGACY_REGION_ID = "yaroslavl"
LEGACY_REGION_NAME = "Ярославль"
LEGACY_REGION_PROVENANCE = "legacy_global_assigned_yaroslavl"
COLLECTED_REGION_PROVENANCE = "scoped_collection_plan"
DUCKDB_MEMORY_LIMIT = "1GiB"
DUCKDB_MEMORY_LIMIT_SETTING = "1.0 GiB"
DUCKDB_MAX_THREADS = 2
STALE_TEMP_SECONDS = 24 * 60 * 60
LEGACY_MIGRATION_LOCK_TARGET = "wb-legacy-yaroslavl-migration"
LEGACY_MIGRATION_STALE_SECONDS = 6 * 60 * 60
REGIONAL_RUN_QUALITY_HASH_FIELDS = (
    "run_id",
    "run_date",
    "region_id",
    "region_provenance",
    "collection_plan_id",
    "query_pack_id",
    "query_pack_version",
    "status",
    "complete",
    "started_at_utc",
    "finished_at_utc",
    "deadline_utc",
    "duration_seconds",
    "items_ok",
    "items_error",
    "components_count",
    "queries_expected",
    "queries_ok",
    "pages_max",
    "pages_ok",
    "positions_max",
    "positions_ok",
    "duplicate_product_positions",
    "endpoint_usage_json",
    "source_manifest_sha256",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _regional_run_quality_sha256(row: dict[str, Any]) -> str:
    if set(row) != set(REGIONAL_RUN_QUALITY_HASH_FIELDS):
        raise CriticalPipelineError(
            "regional run quality hash payload does not match retained fields"
        )
    payload = {
        field: row[field]
        for field in REGIONAL_RUN_QUALITY_HASH_FIELDS
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _read_semicolon_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter=";")]


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise CriticalPipelineError("regional collection manifest is unavailable")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CriticalPipelineError("regional collection manifest is invalid") from exc
    if not isinstance(payload, dict):
        raise CriticalPipelineError("regional collection manifest is invalid")
    return payload


def _run_date(run_id: str) -> str:
    if len(run_id) < 8 or not run_id[:8].isdigit():
        return ""
    value = run_id[:8]
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def _safe_temp_root(project_root: Path) -> Path:
    root = project_root / "data/warehouse/wb_regional/tmp"
    current = project_root
    for part in root.relative_to(project_root).parts:
        current /= part
        if current.is_symlink():
            raise CriticalPipelineError(
                "regional warehouse temp path contains a symlink"
            )
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    if not root.is_dir() or root.is_symlink():
        raise CriticalPipelineError("regional warehouse temp path is unsafe")
    return root


def _cleanup_stale_temp_sessions(root: Path) -> None:
    cutoff = time.time() - STALE_TEMP_SECONDS
    for candidate in root.glob("session-*"):
        try:
            stat = candidate.lstat()
        except FileNotFoundError:
            continue
        if (
            candidate.is_symlink()
            or not candidate.is_dir()
            or stat.st_mtime >= cutoff
        ):
            continue
        shutil.rmtree(candidate)


@contextmanager
def bounded_regional_connection(
    *,
    project_root: Path,
    database_path: Path,
):
    temp_root = _safe_temp_root(project_root)
    _cleanup_stale_temp_sessions(temp_root)
    temp_dir = Path(tempfile.mkdtemp(prefix="session-", dir=temp_root))
    os.chmod(temp_dir, 0o700)
    connection = duckdb.connect(str(database_path))
    escaped_temp = str(temp_dir).replace("'", "''")
    try:
        connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        connection.execute(f"SET threads = {DUCKDB_MAX_THREADS}")
        connection.execute(f"SET temp_directory = '{escaped_temp}'")
        connection.execute(
            "CREATE TEMP TABLE wb_regional_runtime_contract AS "
            "SELECT ?::VARCHAR AS memory_limit, ?::INTEGER AS threads, "
            "?::VARCHAR AS temp_directory",
            [DUCKDB_MEMORY_LIMIT, DUCKDB_MAX_THREADS, str(temp_dir)],
        )
        yield connection
    finally:
        connection.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def _create_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_query_positions (
            marketplace VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            run_date VARCHAR NOT NULL,
            collected_at_utc VARCHAR,
            region_id VARCHAR NOT NULL,
            region_name VARCHAR NOT NULL,
            displayed_region VARCHAR NOT NULL,
            region_provenance VARCHAR NOT NULL,
            collection_plan_id VARCHAR NOT NULL,
            query_pack_id VARCHAR NOT NULL,
            query_pack_version VARCHAR NOT NULL,
            query_id VARCHAR NOT NULL,
            query VARCHAR NOT NULL,
            query_group VARCHAR NOT NULL,
            page INTEGER NOT NULL,
            position_on_page INTEGER NOT NULL,
            absolute_position INTEGER NOT NULL,
            product_id VARCHAR NOT NULL,
            imt_id VARCHAR,
            product_name VARCHAR,
            brand VARCHAR,
            brand_id VARCHAR,
            supplier_id VARCHAR,
            supplier_name VARCHAR,
            final_price DOUBLE,
            price DOUBLE,
            sale_price DOUBLE,
            discount DOUBLE,
            rating DOUBLE,
            feedbacks BIGINT,
            total_quantity BIGINT,
            endpoint_id VARCHAR,
            status VARCHAR,
            warehouse_source_path VARCHAR,
            source_sha256 VARCHAR NOT NULL,
            source_row_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (run_id, region_id, query_id, absolute_position)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_query_generations (
            marketplace VARCHAR NOT NULL,
            run_date VARCHAR NOT NULL,
            query_pack_id VARCHAR NOT NULL,
            query_pack_version VARCHAR NOT NULL,
            region_id VARCHAR NOT NULL,
            query_id VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            collection_plan_id VARCHAR NOT NULL,
            source_manifest_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (
                marketplace,
                run_date,
                query_pack_id,
                query_pack_version,
                region_id,
                query_id
            )
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_seller_snapshots (
            run_id VARCHAR NOT NULL,
            run_date VARCHAR NOT NULL,
            collected_at_utc VARCHAR,
            region_id VARCHAR NOT NULL,
            region_name VARCHAR NOT NULL,
            region_provenance VARCHAR NOT NULL,
            supplier_id VARCHAR NOT NULL,
            supplier_name VARCHAR,
            rating DOUBLE,
            valuation DOUBLE,
            feedbacks_count BIGINT,
            sale_item_quantity BIGINT,
            query_count BIGINT,
            product_count BIGINT,
            queries_ref VARCHAR,
            nm_ids_ref VARCHAR,
            source_product_run_ids VARCHAR,
            status VARCHAR,
            warehouse_source_path VARCHAR,
            source_sha256 VARCHAR NOT NULL,
            source_row_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (run_id, region_id, supplier_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_ingestions (
            run_id VARCHAR NOT NULL,
            collection_plan_id VARCHAR NOT NULL,
            bridge_sha256 VARCHAR NOT NULL,
            sellers_sha256 VARCHAR NOT NULL,
            collection_manifest_sha256 VARCHAR NOT NULL,
            positions_count BIGINT NOT NULL,
            sellers_count BIGINT NOT NULL,
            duplicate_product_positions BIGINT NOT NULL,
            ingested_at_utc VARCHAR NOT NULL,
            schema_version VARCHAR NOT NULL,
            PRIMARY KEY (run_id, collection_plan_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_run_quality (
            run_id VARCHAR NOT NULL,
            run_date VARCHAR NOT NULL,
            region_id VARCHAR NOT NULL,
            region_provenance VARCHAR NOT NULL,
            collection_plan_id VARCHAR NOT NULL,
            query_pack_id VARCHAR,
            query_pack_version VARCHAR,
            status VARCHAR NOT NULL,
            complete BOOLEAN NOT NULL,
            started_at_utc VARCHAR,
            finished_at_utc VARCHAR,
            deadline_utc VARCHAR,
            duration_seconds DOUBLE,
            items_ok BIGINT,
            items_error BIGINT,
            components_count BIGINT,
            queries_expected INTEGER NOT NULL,
            queries_ok INTEGER NOT NULL,
            pages_max INTEGER NOT NULL,
            pages_ok INTEGER NOT NULL,
            positions_max INTEGER NOT NULL,
            positions_ok INTEGER NOT NULL,
            duplicate_product_positions BIGINT NOT NULL,
            endpoint_usage_json VARCHAR,
            source_manifest_sha256 VARCHAR NOT NULL,
            source_row_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (run_id, region_id, collection_plan_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_query_quality (
            run_id VARCHAR NOT NULL,
            run_date VARCHAR NOT NULL,
            region_id VARCHAR NOT NULL,
            collection_plan_id VARCHAR NOT NULL,
            query_pack_id VARCHAR NOT NULL,
            query_pack_version VARCHAR NOT NULL,
            query_id VARCHAR NOT NULL,
            query VARCHAR NOT NULL,
            payload_total BIGINT NOT NULL,
            capped_total INTEGER NOT NULL,
            pages_count INTEGER NOT NULL,
            positions_count INTEGER NOT NULL,
            terminal_page INTEGER NOT NULL,
            terminal_reason VARCHAR NOT NULL,
            duplicate_product_positions INTEGER NOT NULL,
            egress_verification_status VARCHAR NOT NULL,
            segment_sha256 VARCHAR NOT NULL,
            complete BOOLEAN NOT NULL,
            PRIMARY KEY (run_id, region_id, collection_plan_id, query_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS legacy_sync_revisions (
            revision_id VARCHAR PRIMARY KEY,
            source_id VARCHAR NOT NULL,
            source_positions_count BIGINT NOT NULL,
            source_sellers_count BIGINT NOT NULL,
            source_run_quality_count BIGINT NOT NULL,
            inserted_positions_count BIGINT NOT NULL,
            inserted_sellers_count BIGINT NOT NULL,
            inserted_run_quality_count BIGINT NOT NULL,
            synced_at_utc VARCHAR NOT NULL
        )
        """
    )


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    try:
        return int(float(value)) if str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _attach_read_only(
    connection: duckdb.DuckDBPyConnection,
    *,
    path: Path,
) -> None:
    escaped = str(path).replace("'", "''")
    connection.execute(f"ATTACH '{escaped}' AS legacy_source (READ_ONLY)")


def _legacy_source_tables(connection: duckdb.DuckDBPyConnection) -> None:
    required_positions = {
        "run_id",
        "run_date",
        "collected_at_utc",
        "query",
        "page",
        "position_on_page",
        "absolute_position",
        "product_id",
        "imt_id",
        "product_name",
        "brand",
        "brand_id",
        "supplier_id",
        "supplier_name",
        "final_price",
        "price",
        "sale_price",
        "discount",
        "rating",
        "feedbacks",
        "total_quantity",
        "status",
        "warehouse_source_path",
    }
    required_sellers = {
        "run_id",
        "run_date",
        "collected_at_utc",
        "supplier_id",
        "supplier_name",
        "rating",
        "valuation",
        "feedbacks_count",
        "sale_item_quantity",
        "query_count",
        "product_count",
        "queries_ref",
        "nm_ids_ref",
        "source_product_run_ids",
        "status",
        "warehouse_source_path",
    }
    required_run_quality = {
        "run_id",
        "pipeline",
        "status",
        "started_at_utc",
        "finished_at_utc",
        "duration_seconds",
        "items_ok",
        "items_error",
        "components_count",
        "warehouse_source_path",
    }
    position_columns = {
        row[0]
        for row in connection.execute(
            "DESCRIBE SELECT * FROM legacy_source.query_positions"
        ).fetchall()
    }
    seller_columns = {
        row[0]
        for row in connection.execute(
            "DESCRIBE SELECT * FROM legacy_source.seller_daily_metrics"
        ).fetchall()
    }
    run_quality_columns = {
        row[0]
        for row in connection.execute(
            "DESCRIBE SELECT * FROM legacy_source.daily_run_quality"
        ).fetchall()
    }
    if not required_positions.issubset(position_columns):
        raise CriticalPipelineError("legacy WB query_positions schema is incompatible")
    if not required_sellers.issubset(seller_columns):
        raise CriticalPipelineError(
            "legacy WB seller_daily_metrics schema is incompatible"
        )
    if not required_run_quality.issubset(run_quality_columns):
        raise CriticalPipelineError(
            "legacy WB daily_run_quality schema is incompatible"
        )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE legacy_position_source AS
        WITH normalized AS (
            SELECT
                coalesce(run_id, '')::VARCHAR AS run_id,
                coalesce(run_date, '')::VARCHAR AS run_date,
                coalesce(collected_at_utc, '')::VARCHAR AS collected_at_utc,
                coalesce(query, '')::VARCHAR AS query,
                coalesce(page, 0)::INTEGER AS page,
                coalesce(position_on_page, 0)::INTEGER AS position_on_page,
                coalesce(absolute_position, 0)::INTEGER AS absolute_position,
                coalesce(product_id, '')::VARCHAR AS product_id,
                coalesce(imt_id, '')::VARCHAR AS imt_id,
                coalesce(product_name, '')::VARCHAR AS product_name,
                coalesce(brand, '')::VARCHAR AS brand,
                coalesce(brand_id, '')::VARCHAR AS brand_id,
                coalesce(supplier_id, '')::VARCHAR AS supplier_id,
                coalesce(supplier_name, '')::VARCHAR AS supplier_name,
                final_price::DOUBLE AS final_price,
                price::DOUBLE AS price,
                sale_price::DOUBLE AS sale_price,
                discount::DOUBLE AS discount,
                rating::DOUBLE AS rating,
                feedbacks::BIGINT AS feedbacks,
                total_quantity::BIGINT AS total_quantity,
                coalesce(status, '')::VARCHAR AS status,
                coalesce(warehouse_source_path, '')::VARCHAR
                    AS warehouse_source_path
            FROM legacy_source.query_positions
        )
        SELECT
            *,
            'legacy-' || substr(sha256(query), 1, 24) AS query_id,
            sha256(to_json(struct_pack(
                run_id := run_id,
                query := query,
                absolute_position := absolute_position
            ))) AS source_key_sha256,
            sha256(to_json(struct_pack(
                marketplace := 'wb',
                run_id := run_id,
                run_date := run_date,
                collected_at_utc := collected_at_utc,
                region_id := 'yaroslavl',
                region_name := 'Ярославль',
                displayed_region := 'Ярославль',
                region_provenance := 'legacy_global_assigned_yaroslavl',
                query := query,
                page := page,
                position_on_page := position_on_page,
                absolute_position := absolute_position,
                product_id := product_id,
                imt_id := imt_id,
                product_name := product_name,
                brand := brand,
                brand_id := brand_id,
                supplier_id := supplier_id,
                supplier_name := supplier_name,
                final_price := final_price,
                price := price,
                sale_price := sale_price,
                discount := discount,
                rating := rating,
                feedbacks := feedbacks,
                total_quantity := total_quantity,
                status := status,
                warehouse_source_path := warehouse_source_path
            ))) AS source_row_sha256
        FROM normalized
        WHERE run_id <> '' AND query <> '' AND product_id <> ''
          AND absolute_position > 0
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE legacy_run_quality_source AS
        WITH position_counts AS (
            SELECT
                run_id,
                count(DISTINCT query_id)::INTEGER AS queries_observed,
                count(DISTINCT (query_id, page))::INTEGER AS pages_observed,
                count(*)::INTEGER AS positions_observed,
                (
                    count(*) - count(DISTINCT (query_id, product_id))
                )::BIGINT AS duplicate_product_positions
            FROM legacy_position_source
            GROUP BY run_id
        ),
        normalized AS (
            SELECT
                coalesce(quality.run_id, '')::VARCHAR AS run_id,
                coalesce(quality.pipeline, '')::VARCHAR AS pipeline,
                coalesce(quality.status, '')::VARCHAR AS status,
                coalesce(quality.started_at_utc, '')::VARCHAR AS started_at_utc,
                coalesce(quality.finished_at_utc, '')::VARCHAR AS finished_at_utc,
                CASE
                    WHEN regexp_full_match(
                        coalesce(quality.started_at_utc, ''),
                        '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|\\+00:00)$'
                    )
                    AND regexp_full_match(
                        coalesce(quality.finished_at_utc, ''),
                        '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|\\+00:00)$'
                    )
                    AND try_cast(
                        quality.started_at_utc AS TIMESTAMPTZ
                    ) IS NOT NULL
                    AND try_cast(
                        quality.finished_at_utc AS TIMESTAMPTZ
                    ) >= try_cast(
                        quality.started_at_utc AS TIMESTAMPTZ
                    )
                    THEN date_diff(
                        'millisecond',
                        try_cast(quality.started_at_utc AS TIMESTAMPTZ),
                        try_cast(quality.finished_at_utc AS TIMESTAMPTZ)
                    ) / 1000.0
                    ELSE NULL
                END::DOUBLE AS duration_seconds,
                quality.items_ok::BIGINT AS items_ok,
                quality.items_error::BIGINT AS items_error,
                quality.components_count::BIGINT AS components_count,
                coalesce(counts.queries_observed, 0)::INTEGER
                    AS queries_observed,
                coalesce(counts.pages_observed, 0)::INTEGER
                    AS pages_observed,
                coalesce(counts.positions_observed, 0)::INTEGER
                    AS positions_observed,
                coalesce(counts.duplicate_product_positions, 0)::BIGINT
                    AS duplicate_product_positions,
                coalesce(quality.warehouse_source_path, '')::VARCHAR
                    AS warehouse_source_path
            FROM legacy_source.daily_run_quality quality
            LEFT JOIN position_counts counts USING (run_id)
        )
        SELECT
            *,
            sha256(to_json(struct_pack(run_id := run_id))) AS source_key_sha256,
            sha256(to_json(struct_pack(
                run_id := run_id,
                pipeline := pipeline,
                status := status,
                started_at_utc := started_at_utc,
                finished_at_utc := finished_at_utc,
                duration_seconds := duration_seconds,
                items_ok := items_ok,
                items_error := items_error,
                components_count := components_count,
                queries_observed := queries_observed,
                pages_observed := pages_observed,
                positions_observed := positions_observed,
                duplicate_product_positions := duplicate_product_positions,
                warehouse_source_path := warehouse_source_path
            ))) AS source_row_sha256
        FROM normalized
        WHERE run_id <> ''
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE legacy_seller_source AS
        WITH normalized AS (
            SELECT
                coalesce(run_id, '')::VARCHAR AS run_id,
                coalesce(run_date, '')::VARCHAR AS run_date,
                coalesce(collected_at_utc, '')::VARCHAR AS collected_at_utc,
                coalesce(supplier_id, '')::VARCHAR AS supplier_id,
                coalesce(supplier_name, '')::VARCHAR AS supplier_name,
                rating::DOUBLE AS rating,
                valuation::DOUBLE AS valuation,
                feedbacks_count::BIGINT AS feedbacks_count,
                sale_item_quantity::BIGINT AS sale_item_quantity,
                query_count::BIGINT AS query_count,
                product_count::BIGINT AS product_count,
                coalesce(queries_ref, '')::VARCHAR AS queries_ref,
                coalesce(nm_ids_ref, '')::VARCHAR AS nm_ids_ref,
                coalesce(source_product_run_ids, '')::VARCHAR
                    AS source_product_run_ids,
                coalesce(status, '')::VARCHAR AS status,
                coalesce(warehouse_source_path, '')::VARCHAR
                    AS warehouse_source_path
            FROM legacy_source.seller_daily_metrics
        )
        SELECT
            *,
            sha256(to_json(struct_pack(
                run_id := run_id,
                supplier_id := supplier_id
            ))) AS source_key_sha256,
            sha256(to_json(struct_pack(
                run_id := run_id,
                run_date := run_date,
                collected_at_utc := collected_at_utc,
                supplier_id := supplier_id,
                supplier_name := supplier_name,
                rating := rating,
                valuation := valuation,
                feedbacks_count := feedbacks_count,
                sale_item_quantity := sale_item_quantity,
                query_count := query_count,
                product_count := product_count,
                queries_ref := queries_ref,
                nm_ids_ref := nm_ids_ref,
                source_product_run_ids := source_product_run_ids,
                status := status,
                warehouse_source_path := warehouse_source_path
            ))) AS source_row_sha256
        FROM normalized
        WHERE run_id <> '' AND supplier_id <> ''
        """
    )
    duplicate_positions = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT run_id, query_id, absolute_position
            FROM legacy_position_source
            GROUP BY ALL HAVING count(*) <> 1
        )
        """
    ).fetchone()[0]
    duplicate_sellers = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT run_id, supplier_id
            FROM legacy_seller_source
            GROUP BY ALL HAVING count(*) <> 1
        )
        """
    ).fetchone()[0]
    duplicate_run_quality = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT run_id
            FROM legacy_run_quality_source
            GROUP BY ALL HAVING count(*) <> 1
        )
        """
    ).fetchone()[0]
    if duplicate_positions or duplicate_sellers or duplicate_run_quality:
        raise CriticalPipelineError("legacy WB source contains duplicate stable keys")


def migrate_legacy_yaroslavl(
    *,
    project_root: Path,
    connection: duckdb.DuckDBPyConnection,
    integrity_gate: Callable[[], None] = lambda: None,
) -> dict[str, int | str]:
    try:
        runtime_contract = connection.execute(
            "SELECT current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('temp_directory')"
        ).fetchone()
    except duckdb.Error as exc:
        raise CriticalPipelineError(
            "regional warehouse requires bounded DuckDB runtime"
        ) from exc
    expected_temp_root = project_root / "data/warehouse/wb_regional/tmp"
    if (
        runtime_contract is None
        or runtime_contract[0] != DUCKDB_MEMORY_LIMIT_SETTING
        or type(runtime_contract[1]) is not int
        or runtime_contract[1] != DUCKDB_MAX_THREADS
        or not isinstance(runtime_contract[2], str)
    ):
        raise CriticalPipelineError(
            "regional warehouse bounded DuckDB runtime is invalid"
        )
    actual_temp_directory = Path(runtime_contract[2])
    if (
        not actual_temp_directory.is_relative_to(expected_temp_root)
        or not actual_temp_directory.is_dir()
        or actual_temp_directory.is_symlink()
        or actual_temp_directory.stat().st_mode & 0o777 != 0o700
    ):
        raise CriticalPipelineError(
            "regional warehouse bounded DuckDB runtime is invalid"
        )
    global_db = project_root / "data/warehouse/wb/wb.duckdb"
    if not global_db.is_file() or global_db.is_symlink():
        return {"status": "source_absent", "positions": 0, "sellers": 0}
    source_id = "global-wb-as-yaroslavl-v2"
    _attach_read_only(connection, path=global_db)
    try:
        _legacy_source_tables(connection)
        source_positions = int(
            connection.execute(
                "SELECT count(*) FROM legacy_position_source"
            ).fetchone()[0]
        )
        source_sellers = int(
            connection.execute(
                "SELECT count(*) FROM legacy_seller_source"
            ).fetchone()[0]
        )
        source_run_quality = int(
            connection.execute(
                "SELECT count(*) FROM legacy_run_quality_source"
            ).fetchone()[0]
        )
        connection.execute("BEGIN TRANSACTION")
        try:
            changed_positions = connection.execute(
                """
                SELECT count(*)
                FROM regional_query_positions target
                JOIN legacy_position_source source
                  ON target.run_id = source.run_id
                 AND target.region_id = ?
                 AND target.query_id = source.query_id
                 AND target.absolute_position = source.absolute_position
                WHERE target.region_provenance = ?
                  AND target.source_row_sha256 <> source.source_row_sha256
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            missing_positions = connection.execute(
                """
                SELECT count(*)
                FROM regional_query_positions target
                LEFT JOIN legacy_position_source source
                  ON target.run_id = source.run_id
                 AND target.query_id = source.query_id
                 AND target.absolute_position = source.absolute_position
                WHERE target.region_id = ?
                  AND target.region_provenance = ?
                  AND source.run_id IS NULL
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            changed_sellers = connection.execute(
                """
                SELECT count(*)
                FROM regional_seller_snapshots target
                JOIN legacy_seller_source source
                  ON target.run_id = source.run_id
                 AND target.region_id = ?
                 AND target.supplier_id = source.supplier_id
                WHERE target.region_provenance = ?
                  AND target.source_row_sha256 <> source.source_row_sha256
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            missing_sellers = connection.execute(
                """
                SELECT count(*)
                FROM regional_seller_snapshots target
                LEFT JOIN legacy_seller_source source
                  ON target.run_id = source.run_id
                 AND target.supplier_id = source.supplier_id
                WHERE target.region_id = ?
                  AND target.region_provenance = ?
                  AND source.run_id IS NULL
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            changed_run_quality = connection.execute(
                """
                SELECT count(*)
                FROM regional_run_quality target
                JOIN legacy_run_quality_source source
                  ON target.run_id = source.run_id
                 AND target.region_id = ?
                 AND target.collection_plan_id = 'legacy-global'
                WHERE target.region_provenance = ?
                  AND target.source_row_sha256 <> source.source_row_sha256
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            missing_run_quality = connection.execute(
                """
                SELECT count(*)
                FROM regional_run_quality target
                LEFT JOIN legacy_run_quality_source source
                  ON target.run_id = source.run_id
                WHERE target.region_id = ?
                  AND target.collection_plan_id = 'legacy-global'
                  AND target.region_provenance = ?
                  AND source.run_id IS NULL
                """,
                [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
            if missing_positions or missing_sellers or missing_run_quality:
                raise CriticalPipelineError(
                    "legacy WB source removed an already imported row"
                )
            if changed_positions or changed_sellers or changed_run_quality:
                raise CriticalPipelineError(
                    "legacy WB source changed an already imported row"
                )
            before_positions = int(
                connection.execute(
                    "SELECT count(*) FROM regional_query_positions "
                    "WHERE region_id = ? AND region_provenance = ?",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            before_sellers = int(
                connection.execute(
                    "SELECT count(*) FROM regional_seller_snapshots "
                    "WHERE region_id = ? AND region_provenance = ?",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            before_run_quality = int(
                connection.execute(
                    "SELECT count(*) FROM regional_run_quality "
                    "WHERE region_id = ? AND region_provenance = ? "
                    "AND collection_plan_id = 'legacy-global'",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO regional_query_positions
                SELECT
                    'wb',
                    source.run_id,
                    source.run_date,
                    source.collected_at_utc,
                    ?,
                    ?,
                    ?,
                    ?,
                    'legacy-global',
                    'legacy-global',
                    'legacy',
                    source.query_id,
                    source.query,
                    'legacy-global',
                    source.page,
                    source.position_on_page,
                    source.absolute_position,
                    source.product_id,
                    source.imt_id,
                    source.product_name,
                    source.brand,
                    source.brand_id,
                    source.supplier_id,
                    source.supplier_name,
                    source.final_price,
                    source.price,
                    source.sale_price,
                    source.discount,
                    source.rating,
                    source.feedbacks,
                    source.total_quantity,
                    'legacy-global',
                    source.status,
                    source.warehouse_source_path,
                    source.source_key_sha256,
                    source.source_row_sha256
                FROM legacy_position_source source
                WHERE NOT EXISTS (
                    SELECT 1 FROM regional_query_positions target
                    WHERE target.run_id = source.run_id
                      AND target.region_id = ?
                      AND target.query_id = source.query_id
                      AND target.absolute_position = source.absolute_position
                )
                """,
                [
                    LEGACY_REGION_ID,
                    LEGACY_REGION_NAME,
                    LEGACY_REGION_NAME,
                    LEGACY_REGION_PROVENANCE,
                    LEGACY_REGION_ID,
                ],
            )
            connection.execute(
                """
                INSERT INTO regional_run_quality
                SELECT
                    source.run_id,
                    substr(source.run_id, 1, 4) || '-' ||
                        substr(source.run_id, 5, 2) || '-' ||
                        substr(source.run_id, 7, 2),
                    ?,
                    ?,
                    'legacy-global',
                    'legacy-global',
                    'legacy',
                    source.status,
                    source.status = 'success',
                    source.started_at_utc,
                    source.finished_at_utc,
                    '',
                    source.duration_seconds,
                    source.items_ok,
                    source.items_error,
                    source.components_count,
                    source.queries_observed,
                    source.queries_observed,
                    source.pages_observed,
                    source.pages_observed,
                    source.positions_observed,
                    source.positions_observed,
                    source.duplicate_product_positions,
                    '{}',
                    source.source_key_sha256,
                    source.source_row_sha256
                FROM legacy_run_quality_source source
                WHERE NOT EXISTS (
                    SELECT 1 FROM regional_run_quality target
                    WHERE target.run_id = source.run_id
                      AND target.region_id = ?
                      AND target.collection_plan_id = 'legacy-global'
                )
                """,
                [
                    LEGACY_REGION_ID,
                    LEGACY_REGION_PROVENANCE,
                    LEGACY_REGION_ID,
                ],
            )
            connection.execute(
                """
                INSERT INTO regional_seller_snapshots
                SELECT
                    source.run_id,
                    source.run_date,
                    source.collected_at_utc,
                    ?,
                    ?,
                    ?,
                    source.supplier_id,
                    source.supplier_name,
                    source.rating,
                    source.valuation,
                    source.feedbacks_count,
                    source.sale_item_quantity,
                    source.query_count,
                    source.product_count,
                    source.queries_ref,
                    source.nm_ids_ref,
                    source.source_product_run_ids,
                    source.status,
                    source.warehouse_source_path,
                    source.source_key_sha256,
                    source.source_row_sha256
                FROM legacy_seller_source source
                WHERE NOT EXISTS (
                    SELECT 1 FROM regional_seller_snapshots target
                    WHERE target.run_id = source.run_id
                      AND target.region_id = ?
                      AND target.supplier_id = source.supplier_id
                )
                """,
                [
                    LEGACY_REGION_ID,
                    LEGACY_REGION_NAME,
                    LEGACY_REGION_PROVENANCE,
                    LEGACY_REGION_ID,
                ],
            )
            positions_count = int(
                connection.execute(
                    "SELECT count(*) FROM regional_query_positions "
                    "WHERE region_id = ? AND region_provenance = ?",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            sellers_count = int(
                connection.execute(
                    "SELECT count(*) FROM regional_seller_snapshots "
                    "WHERE region_id = ? AND region_provenance = ?",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            run_quality_count = int(
                connection.execute(
                    "SELECT count(*) FROM regional_run_quality "
                    "WHERE region_id = ? AND region_provenance = ? "
                    "AND collection_plan_id = 'legacy-global'",
                    [LEGACY_REGION_ID, LEGACY_REGION_PROVENANCE],
                ).fetchone()[0]
            )
            inserted_positions = positions_count - before_positions
            inserted_sellers = sellers_count - before_sellers
            inserted_run_quality = run_quality_count - before_run_quality
            revision_seed = (
                f"{source_id}|{source_positions}|{source_sellers}|"
                f"{source_run_quality}|{positions_count}|{sellers_count}|"
                f"{run_quality_count}"
            )
            revision_id = hashlib.sha256(revision_seed.encode("utf-8")).hexdigest()
            connection.execute(
                """
                INSERT INTO legacy_sync_revisions
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM legacy_sync_revisions WHERE revision_id = ?
                )
                """,
                [
                    revision_id,
                    source_id,
                    source_positions,
                    source_sellers,
                    source_run_quality,
                    inserted_positions,
                    inserted_sellers,
                    inserted_run_quality,
                    datetime.now(UTC).replace(microsecond=0).isoformat(),
                    revision_id,
                ],
            )
            integrity_gate()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.execute("DETACH legacy_source")
    return {
        "status": (
            "updated"
            if inserted_positions or inserted_sellers or inserted_run_quality
            else "no_changes"
        ),
        "positions": positions_count,
        "sellers": sellers_count,
        "inserted_positions": inserted_positions,
        "inserted_sellers": inserted_sellers,
        "run_quality": run_quality_count,
        "inserted_run_quality": inserted_run_quality,
        "revision_id": revision_id,
    }


def _safe_regular_snapshot(path: Path) -> dict[str, int | str]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CriticalPipelineError(
            "legacy migration database path is unsafe"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_mode & 0o002
            or before.st_nlink != 1
        ):
            raise CriticalPipelineError(
                "legacy migration database metadata is unsafe"
            )
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(descriptor)
        identity = (
            "device",
            "inode",
            "owner_uid",
            "mode",
            "size",
            "mtime_ns",
            "ctime_ns",
        )
        before_values = (
            before.st_dev,
            before.st_ino,
            before.st_uid,
            stat.S_IMODE(before.st_mode),
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_values = (
            after.st_dev,
            after.st_ino,
            after.st_uid,
            stat.S_IMODE(after.st_mode),
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if before_values != after_values or size != before.st_size:
            raise CriticalPipelineError(
                "legacy migration database changed while hashing"
            )
        return {
            **dict(zip(identity, before_values, strict=True)),
            "sha256": digest.hexdigest(),
        }
    finally:
        os.close(descriptor)


def _same_snapshot(
    left: dict[str, int | str],
    right: dict[str, int | str],
) -> bool:
    return left == right


def _safe_regional_warehouse_directory(
    project_root: Path,
    *,
    create: bool = True,
) -> Path:
    project_root = project_root.resolve(strict=True)
    warehouse_root = project_root / "data/warehouse"
    target = warehouse_root / "wb_regional"
    current = project_root
    for part in target.relative_to(project_root).parts:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            if not create:
                raise CriticalPipelineError(
                    "regional warehouse directory is unavailable"
                ) from None
            current.mkdir(mode=0o755)
            _fsync_directory(current.parent)
            info = current.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o002
        ):
            raise CriticalPipelineError(
                "regional warehouse directory is unsafe"
            )
    return target


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_database(source: Path, target: Path) -> None:
    source_snapshot = _safe_regular_snapshot(source)
    source_descriptor = os.open(
        source,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    target_descriptor = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        while True:
            chunk = os.read(source_descriptor, 1024 * 1024)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:
                written = os.write(target_descriptor, view)
                if written <= 0:
                    raise CriticalPipelineError(
                        "regional warehouse staging copy failed"
                    )
                view = view[written:]
        os.fsync(target_descriptor)
    finally:
        os.close(target_descriptor)
        os.close(source_descriptor)
    if not _same_snapshot(source_snapshot, _safe_regular_snapshot(source)):
        raise CriticalPipelineError(
            "regional warehouse source changed during staging copy"
        )


def _legacy_database_check(
    database_path: Path,
) -> dict[str, Any]:
    if not database_path.is_file() or database_path.is_symlink():
        raise CriticalPipelineError(
            "regional warehouse database is unavailable"
        )
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        required_tables = {
            "regional_query_positions",
            "regional_seller_snapshots",
            "regional_run_quality",
            "regional_query_quality",
            "regional_query_generations",
            "regional_ingestions",
            "legacy_sync_revisions",
        }
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        if not required_tables.issubset(tables):
            raise CriticalPipelineError(
                "regional warehouse schema is incomplete"
            )
        positions = int(
            connection.execute(
                "SELECT count(*) FROM regional_query_positions "
                "WHERE region_provenance = ?",
                [LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
        )
        sellers = int(
            connection.execute(
                "SELECT count(*) FROM regional_seller_snapshots "
                "WHERE region_provenance = ?",
                [LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
        )
        run_quality = int(
            connection.execute(
                "SELECT count(*) FROM regional_run_quality "
                "WHERE region_provenance = ?",
                [LEGACY_REGION_PROVENANCE],
            ).fetchone()[0]
        )
        invalid_regions = int(
            connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM regional_query_positions
                     WHERE region_id IS NULL OR trim(region_id) = ''
                        OR region_name IS NULL OR trim(region_name) = ''
                        OR displayed_region IS NULL
                        OR trim(displayed_region) = '')
                  + (SELECT count(*) FROM regional_seller_snapshots
                     WHERE region_id IS NULL OR trim(region_id) = ''
                        OR region_name IS NULL OR trim(region_name) = '')
                  + (SELECT count(*) FROM regional_run_quality
                     WHERE region_id IS NULL OR trim(region_id) = '')
                """
            ).fetchone()[0]
        )
        legacy_dimensions = connection.execute(
            """
            SELECT DISTINCT marketplace, region_id, region_name,
                            displayed_region, region_provenance,
                            collection_plan_id, query_pack_id,
                            query_pack_version, query_group
            FROM regional_query_positions
            WHERE region_provenance = ?
            ORDER BY ALL
            """,
            [LEGACY_REGION_PROVENANCE],
        ).fetchall()
        seller_legacy_dimensions = connection.execute(
            """
            SELECT DISTINCT region_id, region_name, region_provenance
            FROM regional_seller_snapshots
            WHERE region_provenance = ?
            ORDER BY ALL
            """,
            [LEGACY_REGION_PROVENANCE],
        ).fetchall()
        run_quality_legacy_dimensions = connection.execute(
            """
            SELECT DISTINCT region_id, region_provenance,
                            collection_plan_id, query_pack_id,
                            query_pack_version
            FROM regional_run_quality
            WHERE region_provenance = ?
            ORDER BY ALL
            """,
            [LEGACY_REGION_PROVENANCE],
        ).fetchall()
        expected_dimensions = [
            (
                "wb",
                LEGACY_REGION_ID,
                LEGACY_REGION_NAME,
                LEGACY_REGION_NAME,
                LEGACY_REGION_PROVENANCE,
                "legacy-global",
                "legacy-global",
                "legacy",
                "legacy-global",
            )
        ]
        expected_seller_dimensions = [
            (
                LEGACY_REGION_ID,
                LEGACY_REGION_NAME,
                LEGACY_REGION_PROVENANCE,
            )
        ]
        expected_run_quality_dimensions = [
            (
                LEGACY_REGION_ID,
                LEGACY_REGION_PROVENANCE,
                "legacy-global",
                "legacy-global",
                "legacy",
            )
        ]
        if (
            invalid_regions
            or (positions > 0 and legacy_dimensions != expected_dimensions)
            or (
                sellers > 0
                and seller_legacy_dimensions
                != expected_seller_dimensions
            )
            or (
                run_quality > 0
                and run_quality_legacy_dimensions
                != expected_run_quality_dimensions
            )
        ):
            raise CriticalPipelineError(
                "regional warehouse legacy dimensions are invalid"
            )
        return {
            "status": "ok",
            "database_path": str(database_path),
            "positions": positions,
            "sellers": sellers,
            "run_quality": run_quality,
            "invalid_region_rows": invalid_regions,
            "legacy_dimensions": [
                {
                    "marketplace": row[0],
                    "region_id": row[1],
                    "region_name": row[2],
                    "displayed_region": row[3],
                    "region_provenance": row[4],
                    "collection_plan_id": row[5],
                    "query_pack_id": row[6],
                    "query_pack_version": row[7],
                    "query_group": row[8],
                }
                for row in legacy_dimensions
            ],
            "seller_legacy_dimensions": [
                {
                    "region_id": row[0],
                    "region_name": row[1],
                    "region_provenance": row[2],
                }
                for row in seller_legacy_dimensions
            ],
            "run_quality_legacy_dimensions": [
                {
                    "region_id": row[0],
                    "region_provenance": row[1],
                    "collection_plan_id": row[2],
                    "query_pack_id": row[3],
                    "query_pack_version": row[4],
                }
                for row in run_quality_legacy_dimensions
            ],
            "api_source_schema_compatible": True,
        }
    finally:
        connection.close()


def check_legacy_yaroslavl_database(
    *,
    project_root: Path,
) -> dict[str, Any]:
    project_root = project_root.resolve(strict=True)
    warehouse_dir = _safe_regional_warehouse_directory(
        project_root,
        create=False,
    )
    database_path = warehouse_dir / "wb_regional.duckdb"
    _safe_regular_snapshot(database_path)
    return _legacy_database_check(database_path)


@contextmanager
def acquire_legacy_yaroslavl_migration_locks(
    *,
    project_root: Path,
    run_id: str,
    stale_seconds: int = LEGACY_MIGRATION_STALE_SECONDS,
):
    lock_dir = project_root / "state/locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        stack.enter_context(
            acquire_advisory_lock(
                lock_dir / "products_sellers_daily.flock"
            )
        )
        stack.enter_context(
            acquire_run_lock(
                state_dir=project_root / "state",
                target=LEGACY_MIGRATION_LOCK_TARGET,
                run_id=run_id,
                enabled=True,
                stale_seconds=stale_seconds,
                guard_blocking=False,
            )
        )
        stack.enter_context(
            acquire_advisory_lock(
                lock_dir / "wb_warehouse_refresh.flock"
            )
        )
        stack.enter_context(
            acquire_advisory_lock(
                lock_dir / "wb_collection_plan.flock"
            )
        )
        yield


def migrate_legacy_yaroslavl_database(
    *,
    project_root: Path,
    apply: bool,
    run_id: str,
    stale_seconds: int = LEGACY_MIGRATION_STALE_SECONDS,
    integrity_gate: Callable[[], None] = lambda: None,
    event_hook: Callable[[str, Path], None] | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve(strict=True)
    source_path = project_root / "data/warehouse/wb/wb.duckdb"
    if not source_path.is_file() or source_path.is_symlink():
        raise CriticalPipelineError(
            "legacy WB warehouse source is unavailable"
        )
    with acquire_legacy_yaroslavl_migration_locks(
        project_root=project_root,
        run_id=run_id,
        stale_seconds=stale_seconds,
    ):
        source_snapshot = _safe_regular_snapshot(source_path)
        warehouse_dir = _safe_regional_warehouse_directory(project_root)
        target_path = warehouse_dir / "wb_regional.duckdb"
        target_exists = os.path.lexists(target_path)
        target_snapshot = (
            _safe_regular_snapshot(target_path)
            if target_exists
            else None
        )
        candidate_path = (
            warehouse_dir
            / f".wb-regional-yaroslavl-{uuid.uuid4().hex}.duckdb"
        )

        def verify_source() -> None:
            integrity_gate()
            if not _same_snapshot(
                source_snapshot,
                _safe_regular_snapshot(source_path),
            ):
                raise CriticalPipelineError(
                    "legacy WB warehouse source changed during migration"
                )

        try:
            if target_snapshot is not None:
                _copy_database(target_path, candidate_path)
            with bounded_regional_connection(
                project_root=project_root,
                database_path=candidate_path,
            ) as connection:
                _create_schema(connection)
                migration = migrate_legacy_yaroslavl(
                    project_root=project_root,
                    connection=connection,
                    integrity_gate=verify_source,
                )
            os.chmod(candidate_path, 0o644)
            candidate_descriptor = os.open(
                candidate_path,
                os.O_RDONLY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                os.fsync(candidate_descriptor)
            finally:
                os.close(candidate_descriptor)
            candidate_snapshot = _safe_regular_snapshot(candidate_path)
            check = _legacy_database_check(candidate_path)
            if (
                check["positions"] != migration["positions"]
                or check["sellers"] != migration["sellers"]
                or check["run_quality"] != migration["run_quality"]
            ):
                raise CriticalPipelineError(
                    "regional warehouse migration verification mismatch"
                )
            result = {
                "schema_version": "wb_legacy_yaroslavl_migration_v1",
                "mode": "apply" if apply else "dry_run",
                "status": migration["status"],
                "source": {
                    "sha256": source_snapshot["sha256"],
                    "size": source_snapshot["size"],
                },
                "target": {
                    "sha256": candidate_snapshot["sha256"],
                    "size": candidate_snapshot["size"],
                },
                "migration": migration,
                "check": {
                    key: value
                    for key, value in check.items()
                    if key != "database_path"
                },
                "publication": "not_requested",
            }
            if not apply:
                return result
            if target_snapshot is not None and migration["status"] == "no_changes":
                result["target"] = {
                    "sha256": target_snapshot["sha256"],
                    "size": target_snapshot["size"],
                }
                result["publication"] = "no_changes"
                return result
            verify_source()
            current_target_exists = os.path.lexists(target_path)
            current_target = (
                _safe_regular_snapshot(target_path)
                if current_target_exists
                else None
            )
            if current_target != target_snapshot:
                raise CriticalPipelineError(
                    "regional warehouse target changed during migration"
                )
            if event_hook is not None:
                event_hook("before_publish", target_path)
            os.replace(candidate_path, target_path)
            _fsync_directory(warehouse_dir)
            published = _safe_regular_snapshot(target_path)
            if (
                published["sha256"] != candidate_snapshot["sha256"]
                or published["size"] != candidate_snapshot["size"]
            ):
                raise CriticalPipelineError(
                    "regional warehouse atomic publication verification failed"
                )
            if event_hook is not None:
                event_hook("after_publish", target_path)
            result["publication"] = "published"
            return result
        finally:
            try:
                candidate_path.unlink()
            except FileNotFoundError:
                pass


def ingest_regional_run(
    *,
    project_root: Path,
    run_id: str,
    collection_plan_id: str,
    bridge_path: Path,
    sellers_path: Path,
    collection_manifest_path: Path | None = None,
    integrity_gate: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    if collection_manifest_path is None:
        collection_manifest_path = bridge_path.parent / "collection_manifest.json"
    bridge_sha256 = _sha256(bridge_path)
    sellers_sha256 = _sha256(sellers_path)
    collection_manifest_sha256 = _sha256(collection_manifest_path)
    collection_manifest = _read_json_object(collection_manifest_path)
    if (
        collection_manifest.get("run_id") != run_id
        or collection_manifest.get("collection_plan_id") != collection_plan_id
        or collection_manifest.get("status") != "success"
        or collection_manifest.get("complete") is not True
    ):
        raise CriticalPipelineError(
            "regional collection manifest is not complete for ingestion"
        )
    resume = collection_manifest.get("resume")
    segment_refs = resume.get("segments") if isinstance(resume, dict) else None
    region_manifests = collection_manifest.get("regions")
    if not isinstance(segment_refs, list) or not isinstance(region_manifests, list):
        raise CriticalPipelineError(
            "regional collection quality evidence is incomplete"
        )
    positions = _read_semicolon_csv(bridge_path)
    sellers = _read_semicolon_csv(sellers_path)
    warehouse_dir = project_root / "data/warehouse/wb_regional"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    database_path = warehouse_dir / "wb_regional.duckdb"
    with bounded_regional_connection(
        project_root=project_root,
        database_path=database_path,
    ) as connection:
        _create_schema(connection)
        legacy = migrate_legacy_yaroslavl(
            project_root=project_root,
            connection=connection,
            integrity_gate=integrity_gate,
        )
        prior = connection.execute(
            "SELECT bridge_sha256, sellers_sha256, collection_manifest_sha256, "
            "positions_count, sellers_count, "
            "duplicate_product_positions FROM regional_ingestions "
            "WHERE run_id = ? AND collection_plan_id = ?",
            [run_id, collection_plan_id],
        ).fetchone()
        if prior is not None:
            if (
                prior[0] != bridge_sha256
                or prior[1] != sellers_sha256
                or prior[2] != collection_manifest_sha256
            ):
                raise CriticalPipelineError(
                    "regional warehouse source hash mismatch for existing run"
                )
            return {
                "status": "already_ingested",
                "database_path": str(database_path),
                "positions_count": int(prior[3]),
                "sellers_count": int(prior[4]),
                "duplicate_product_positions": int(prior[5]),
                "legacy": legacy,
            }

        region_names = {
            row["region_id"]: row["region_name"]
            for row in positions
            if row.get("region_id")
        }
        seller_by_id = {
            row.get("supplier_id", ""): row
            for row in sellers
            if row.get("supplier_id")
        }
        suppliers_by_region: dict[str, set[str]] = {}
        position_keys: set[tuple[str, str, int]] = set()
        positions_by_scope: dict[tuple[str, str], list[int]] = {}
        product_occurrences: dict[tuple[str, str, str], int] = {}
        connection.execute("BEGIN TRANSACTION")
        try:
            for row in positions:
                region_id = row.get("region_id", "")
                query_id = row.get("query_id", "")
                product_id = row.get("nmId", "")
                absolute_position = _optional_int(row.get("absolute_position")) or 0
                supplier_id = row.get("supplier_id", "")
                if (
                    not region_id
                    or not query_id
                    or not product_id
                    or absolute_position <= 0
                ):
                    raise CriticalPipelineError(
                        "regional warehouse position identity is incomplete"
                    )
                position_key = (region_id, query_id, absolute_position)
                if position_key in position_keys:
                    raise CriticalPipelineError(
                        "regional warehouse position key is duplicated"
                    )
                position_keys.add(position_key)
                positions_by_scope.setdefault((region_id, query_id), []).append(
                    absolute_position
                )
                product_key = (region_id, query_id, product_id)
                product_occurrences[product_key] = (
                    product_occurrences.get(product_key, 0) + 1
                )
                if supplier_id:
                    suppliers_by_region.setdefault(region_id, set()).add(supplier_id)
                source_row_sha256 = hashlib.sha256(
                    repr(
                        tuple(row.get(field, "") for field in sorted(row))
                    ).encode("utf-8")
                ).hexdigest()
                connection.execute(
                    """
                        INSERT INTO regional_query_positions VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        "wb",
                        run_id,
                        _run_date(run_id),
                        row.get("collected_at_utc", ""),
                        region_id,
                        row.get("region_name", ""),
                        row.get("displayed_region")
                        or row.get("region_name", ""),
                        COLLECTED_REGION_PROVENANCE,
                        collection_plan_id,
                        row.get("query_pack_id", ""),
                        row.get("query_pack_version", ""),
                        query_id,
                        row.get("query", ""),
                        row.get("query_group", ""),
                        _optional_int(row.get("page")) or 0,
                        _optional_int(row.get("position_on_page")) or 0,
                        absolute_position,
                        product_id,
                        row.get("imtId", ""),
                        row.get("product_name", ""),
                        row.get("brand", ""),
                        row.get("brandId", ""),
                        supplier_id,
                        row.get("supplier_name", ""),
                        _optional_float(row.get("final_price")),
                        _optional_float(row.get("price")),
                        _optional_float(row.get("sale_price")),
                        _optional_float(row.get("discount")),
                        _optional_float(row.get("rating")),
                        _optional_int(row.get("feedbacks")),
                        _optional_int(row.get("total_quantity")),
                        row.get("endpoint_id", ""),
                        row.get("status", ""),
                        str(bridge_path),
                        bridge_sha256,
                        source_row_sha256,
                    ],
                )
            for scope_positions in positions_by_scope.values():
                ordered = sorted(scope_positions)
                if (
                    ordered != list(range(1, len(ordered) + 1))
                    or len(ordered) > 1000
                ):
                    raise CriticalPipelineError(
                        "regional warehouse position sequence is incomplete"
                    )
            seller_rows_written = 0
            for region_id, supplier_ids in suppliers_by_region.items():
                for supplier_id in sorted(supplier_ids):
                    seller = seller_by_id.get(supplier_id)
                    if seller is None:
                        raise CriticalPipelineError(
                            "regional seller output misses a referenced supplier"
                        )
                    source_row_sha256 = hashlib.sha256(
                        repr(
                            tuple(seller.get(field, "") for field in sorted(seller))
                        ).encode("utf-8")
                    ).hexdigest()
                    connection.execute(
                        """
                        INSERT INTO regional_seller_snapshots VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?
                        )
                        """,
                        [
                            run_id,
                            _run_date(run_id),
                            seller.get("collected_at_utc", ""),
                            region_id,
                            region_names.get(region_id, ""),
                            COLLECTED_REGION_PROVENANCE,
                            supplier_id,
                            seller.get("supplier_name", ""),
                            _optional_float(seller.get("rating")),
                            _optional_float(seller.get("valuation")),
                            _optional_int(seller.get("feedbacks_count")),
                            _optional_int(seller.get("sale_item_quantity")),
                            _optional_int(seller.get("query_count")),
                            _optional_int(seller.get("product_count")),
                            seller.get("queries_ref", ""),
                            seller.get("nm_ids_ref", ""),
                            seller.get("source_product_run_ids", ""),
                            seller.get("status", ""),
                            str(sellers_path),
                            sellers_sha256,
                            source_row_sha256,
                        ],
                    )
                    seller_rows_written += 1
            duplicate_product_positions = sum(
                count - 1 for count in product_occurrences.values() if count > 1
            )
            query_text_by_id = {
                row["query_id"]: row.get("query", "")
                for row in positions
                if row.get("query_id")
            }
            region_quality = {
                item.get("region_id"): item
                for item in region_manifests
                if isinstance(item, dict)
            }
            query_quality_rows: list[dict[str, Any]] = []
            for ref in segment_refs:
                if not isinstance(ref, dict):
                    raise CriticalPipelineError(
                        "regional query quality reference is invalid"
                    )
                completion = ref.get("completion")
                egress = ref.get("egress")
                region_id = ref.get("region_id")
                query_id = ref.get("query_id")
                if (
                    not isinstance(completion, dict)
                    or not isinstance(egress, dict)
                    or region_id not in region_quality
                    or query_id not in query_text_by_id
                    or completion.get("complete") is not True
                ):
                    raise CriticalPipelineError(
                        "regional query quality evidence is invalid"
                    )
                query_quality_rows.append(
                    {
                        "region_id": region_id,
                        "query_id": query_id,
                        "query": query_text_by_id[query_id],
                        "payload_total": completion.get("payload_total"),
                        "capped_total": completion.get("capped_total"),
                        "pages_count": completion.get("pages_count"),
                        "products_count": completion.get("products_count"),
                        "terminal_page": completion.get("terminal_page"),
                        "terminal_reason": completion.get("terminal_reason"),
                        "duplicate_product_positions": completion.get(
                            "duplicate_product_positions"
                        ),
                        "egress_verification_status": egress.get(
                            "verification_status"
                        ),
                        "segment_sha256": ref.get("sha256"),
                    }
                )
            if len(query_quality_rows) != len(segment_refs):
                raise CriticalPipelineError(
                    "regional query quality evidence is incomplete"
                )
            query_pack_id = str(
                collection_manifest.get("query_pack_id", "")
            )
            query_pack_version = str(
                collection_manifest.get("query_pack_version", "")
            )
            if not query_pack_id or not query_pack_version:
                raise CriticalPipelineError(
                    "regional query pack identity is incomplete"
                )
            generation_scopes = {
                (row["region_id"], row["query_id"])
                for row in query_quality_rows
            }
            for region_id, query_id in sorted(generation_scopes):
                prior_generation = connection.execute(
                    """
                    SELECT run_id
                    FROM regional_query_generations
                    WHERE marketplace = 'wb'
                      AND run_date = ?
                      AND query_pack_id = ?
                      AND query_pack_version = ?
                      AND region_id = ?
                      AND query_id = ?
                    """,
                    [
                        _run_date(run_id),
                        query_pack_id,
                        query_pack_version,
                        region_id,
                        query_id,
                    ],
                ).fetchone()
                if prior_generation is not None:
                    raise CriticalPipelineError(
                        "regional query generation already exists for date"
                    )
            for row in query_quality_rows:
                scope = (row["region_id"], row["query_id"])
                scope_positions = positions_by_scope.get(scope, [])
                scope_duplicate_positions = sum(
                    count - 1
                    for (region_id, query_id, _product_id), count
                    in product_occurrences.items()
                    if (region_id, query_id) == scope and count > 1
                )
                if (
                    len(scope_positions) != row["products_count"]
                    or scope_duplicate_positions
                    != row["duplicate_product_positions"]
                ):
                    raise CriticalPipelineError(
                        "regional query quality does not match position facts"
                    )
            for region_id, region in region_quality.items():
                if region_id not in region_names:
                    raise CriticalPipelineError(
                        "regional run quality references unknown region"
                    )
                query_rows = [
                    row
                    for row in query_quality_rows
                    if row["region_id"] == region_id
                ]
                run_quality_payload = {
                    "run_id": run_id,
                    "run_date": _run_date(run_id),
                    "region_id": region_id,
                    "region_provenance": COLLECTED_REGION_PROVENANCE,
                    "collection_plan_id": collection_plan_id,
                    "query_pack_id": collection_manifest.get(
                        "query_pack_id",
                        "",
                    ),
                    "query_pack_version": collection_manifest.get(
                        "query_pack_version",
                        "",
                    ),
                    "status": region.get("status", ""),
                    "complete": region.get("complete") is True,
                    "started_at_utc": collection_manifest.get(
                        "started_at_utc",
                        "",
                    ),
                    "finished_at_utc": collection_manifest.get(
                        "finished_at_utc",
                        "",
                    ),
                    "deadline_utc": collection_manifest.get(
                        "deadline_utc",
                        "",
                    ),
                    "duration_seconds": None,
                    "items_ok": int(region.get("products_ok", 0)),
                    "items_error": 0,
                    "components_count": 3,
                    "queries_expected": len(query_rows),
                    "queries_ok": int(region.get("queries_ok", 0)),
                    "pages_max": len(query_rows) * 10,
                    "pages_ok": int(region.get("pages_ok", 0)),
                    "positions_max": len(query_rows) * 1000,
                    "positions_ok": int(region.get("products_ok", 0)),
                    "duplicate_product_positions": int(
                        region.get(
                            "duplicate_product_positions",
                            0,
                        )
                    ),
                    "endpoint_usage_json": json.dumps(
                        collection_manifest.get("endpoint_usage", {}),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "source_manifest_sha256": collection_manifest_sha256,
                }
                source_row_sha256 = _regional_run_quality_sha256(
                    run_quality_payload
                )
                connection.execute(
                    """
                    INSERT INTO regional_run_quality VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        *[
                            run_quality_payload[field]
                            for field in REGIONAL_RUN_QUALITY_HASH_FIELDS
                        ],
                        source_row_sha256,
                    ],
                )
            for region_id, query_id in sorted(generation_scopes):
                connection.execute(
                    """
                    INSERT INTO regional_query_generations VALUES (
                        'wb', ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        _run_date(run_id),
                        query_pack_id,
                        query_pack_version,
                        region_id,
                        query_id,
                        run_id,
                        collection_plan_id,
                        collection_manifest_sha256,
                    ],
                )
            for row in query_quality_rows:
                values = [
                    row["payload_total"],
                    row["capped_total"],
                    row["pages_count"],
                    row["products_count"],
                    row["terminal_page"],
                    row["duplicate_product_positions"],
                ]
                if any(type(value) is not int or value < 0 for value in values):
                    raise CriticalPipelineError(
                        "regional query quality counters are invalid"
                    )
                connection.execute(
                    """
                    INSERT INTO regional_query_quality VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        run_id,
                        _run_date(run_id),
                        row["region_id"],
                        collection_plan_id,
                        collection_manifest.get("query_pack_id", ""),
                        collection_manifest.get("query_pack_version", ""),
                        row["query_id"],
                        row["query"],
                        row["payload_total"],
                        row["capped_total"],
                        row["pages_count"],
                        row["products_count"],
                        row["terminal_page"],
                        row["terminal_reason"],
                        row["duplicate_product_positions"],
                        row["egress_verification_status"],
                        row["segment_sha256"],
                        True,
                    ],
                )
            connection.execute(
                "INSERT INTO regional_ingestions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    collection_plan_id,
                    bridge_sha256,
                    sellers_sha256,
                    collection_manifest_sha256,
                    len(positions),
                    seller_rows_written,
                    duplicate_product_positions,
                    datetime.now(UTC).replace(microsecond=0).isoformat(),
                    REGIONAL_WAREHOUSE_SCHEMA,
                ],
            )
            integrity_gate()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return {
        "status": "success",
        "database_path": str(database_path),
        "positions_count": len(positions),
        "sellers_count": seller_rows_written,
        "duplicate_product_positions": duplicate_product_positions,
        "legacy": legacy,
    }
