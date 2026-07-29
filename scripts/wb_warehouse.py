#!/usr/bin/env python3
"""Build a read-only WB analytics warehouse from existing parser marts.

This script does not run the parser and does not modify raw/staging/marts/latest.
It rebuilds the MVP warehouse from stable historical CSV/JSON outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COORDINATOR_LOCK_DIRECTORY = Path("/run/lock/parser-nightly-coordinator")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.config import load_config
from app.common.durable_atomic import durable_atomic_replace
from app.common.exceptions import CriticalPipelineError
from app.common.nightly_attestation import integrity_gate
from app.warehouse.wb_regional import (
    check_legacy_yaroslavl_database,
    migrate_legacy_yaroslavl_database,
)


def _require_host_lease_after_cutover() -> None:
    if not os.path.lexists(COORDINATOR_LOCK_DIRECTORY):
        return
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from app.common.nightly_coordinator import (
        require_official_live_entry_lease,
    )

    require_official_live_entry_lease(environment=os.environ)

try:
    import duckdb
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime only
    raise SystemExit(
        "Missing dependency: duckdb. Install it in the shared parser_wb runtime "
        "(/home/Codex/agent-tools/parser_wb-python), not in the project tree."
    ) from exc

TABLE_SPECS = {
    "product_snapshots": "data/marts/serp/*/products_daily.csv",
    "seller_snapshots": "data/marts/sellers/*/sellers_daily.csv",
    "product_seller_bridge": "data/marts/sellers/*/seller_query_product_bridge.csv",
    "serp_pages": "data/raw/serp/*/pages_raw_index.csv",
}
WAREHOUSE_TABLES = [
    "product_snapshots",
    "seller_snapshots",
    "product_seller_bridge",
    "serp_pages",
    "run_reports",
    "run_report_components",
]
MVP_LIMITATIONS = [
    "MVP rebuild from existing WB parser marts/raw indexes/run reports.",
    "Does not delete or modify raw/staging/marts/latest.",
    "Ignores latest directories to avoid duplicate historical rows.",
    "Ozon, cron, proxy, cookies, runtime.env and request headers are out of scope.",
    "No retention or scheduled append is enabled in this stage.",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_path_list(paths: Iterable[Path]) -> str:
    return "[" + ", ".join(sql_quote(str(path)) for path in paths) + "]"


def list_csv_files(project_root: Path, pattern: str) -> list[Path]:
    files = []
    for path in project_root.glob(pattern):
        if not path.is_file():
            continue
        if path.parent.name == "latest":
            continue
        files.append(path)
    return sorted(files)


def extract_run_date(run_id: str | None) -> str:
    if not run_id:
        return ""
    match = re.match(r"^(\d{8})", str(run_id))
    if not match:
        return ""
    value = match.group(1)
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def report_files(project_root: Path) -> list[Path]:
    files = sorted((project_root / "state" / "run_reports").glob("*.json"))
    concrete = [path for path in files if path.name != "latest.json"]
    if concrete:
        return concrete
    return files


def discover_sources(project_root: Path) -> dict[str, list[Path]]:
    sources = {name: list_csv_files(project_root, pattern) for name, pattern in TABLE_SPECS.items()}
    sources["run_reports"] = report_files(project_root)
    return sources


def load_run_reports(con: duckdb.DuckDBPyConnection, project_root: Path) -> tuple[int, int]:
    summary_rows: list[tuple[Any, ...]] = []
    component_rows: list[tuple[Any, ...]] = []
    loaded_at = utc_now_iso()

    for path in report_files(project_root):
        try:
            data = read_json(path)
        except Exception as exc:
            summary_rows.append(
                (
                    "",
                    "",
                    "",
                    "parse_error",
                    "",
                    "",
                    None,
                    None,
                    None,
                    0,
                    str(path),
                    str(exc),
                    loaded_at,
                )
            )
            continue
        totals = data.get("totals") or {}
        run_id = str(data.get("run_id") or "")
        summary_rows.append(
            (
                run_id,
                str(data.get("pipeline") or ""),
                str(data.get("job_id") or ""),
                str(data.get("status") or ""),
                str(data.get("started_at_utc") or ""),
                str(data.get("finished_at_utc") or ""),
                data.get("duration_seconds"),
                totals.get("items_ok"),
                totals.get("items_error"),
                len(data.get("components") or []),
                str(path),
                str(data.get("note") or ""),
                loaded_at,
            )
        )
        for component in data.get("components") or []:
            refs = component.get("result_refs") or {}
            component_rows.append(
                (
                    run_id,
                    str(component.get("component") or ""),
                    str(component.get("status") or ""),
                    str(component.get("started_at_utc") or ""),
                    str(component.get("finished_at_utc") or ""),
                    component.get("items_ok"),
                    component.get("items_error"),
                    str(component.get("error_code") or ""),
                    str(component.get("note") or ""),
                    json.dumps(refs, ensure_ascii=False, sort_keys=True),
                    str(path),
                    loaded_at,
                )
            )

    con.execute(
        """
        CREATE OR REPLACE TABLE run_reports (
            run_id VARCHAR,
            pipeline VARCHAR,
            job_id VARCHAR,
            status VARCHAR,
            started_at_utc VARCHAR,
            finished_at_utc VARCHAR,
            duration_seconds DOUBLE,
            items_ok BIGINT,
            items_error BIGINT,
            components_count BIGINT,
            warehouse_source_path VARCHAR,
            note VARCHAR,
            warehouse_loaded_at VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE run_report_components (
            run_id VARCHAR,
            component VARCHAR,
            status VARCHAR,
            started_at_utc VARCHAR,
            finished_at_utc VARCHAR,
            items_ok BIGINT,
            items_error BIGINT,
            error_code VARCHAR,
            note VARCHAR,
            result_refs_json VARCHAR,
            warehouse_source_path VARCHAR,
            warehouse_loaded_at VARCHAR
        )
        """
    )
    if summary_rows:
        con.executemany("INSERT INTO run_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", summary_rows)
    if component_rows:
        con.executemany(
            "INSERT INTO run_report_components VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            component_rows,
        )
    return len(summary_rows), len(component_rows)


def create_csv_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    paths: list[Path],
    loaded_at: str,
) -> int:
    if not paths:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT NULL::VARCHAR AS warehouse_loaded_at WHERE false")
        return 0
    file_list = sql_path_list(paths)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            regexp_extract(COALESCE(run_id, ''), '^(\\d{{8}})', 1) AS warehouse_run_day_raw,
            CASE
                WHEN regexp_extract(COALESCE(run_id, ''), '^(\\d{{8}})', 1) != ''
                THEN substr(regexp_extract(COALESCE(run_id, ''), '^(\\d{{8}})', 1), 1, 4)
                     || '-' || substr(regexp_extract(COALESCE(run_id, ''), '^(\\d{{8}})', 1), 5, 2)
                     || '-' || substr(regexp_extract(COALESCE(run_id, ''), '^(\\d{{8}})', 1), 7, 2)
                ELSE ''
            END AS warehouse_run_date,
            filename AS warehouse_source_path,
            {sql_quote(loaded_at)} AS warehouse_loaded_at
        FROM read_csv_auto(
            {file_list},
            delim=';',
            header=true,
            all_varchar=true,
            union_by_name=true,
            filename=true
        )
        """
    )
    return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE VIEW query_positions AS
        SELECT
            run_id,
            warehouse_run_date AS run_date,
            collected_at_utc,
            query,
            query_group,
            try_cast(page AS INTEGER) AS page,
            try_cast(position_on_page AS INTEGER) AS position_on_page,
            try_cast(absolute_position AS INTEGER) AS absolute_position,
            nmId AS product_id,
            imtId AS imt_id,
            product_name,
            brand,
            brandId AS brand_id,
            supplier_id,
            supplier_name,
            try_cast(final_price AS DOUBLE) AS final_price,
            try_cast(price AS DOUBLE) AS price,
            try_cast(sale_price AS DOUBLE) AS sale_price,
            try_cast(discount AS DOUBLE) AS discount,
            try_cast(rating AS DOUBLE) AS rating,
            try_cast(feedbacks AS BIGINT) AS feedbacks,
            try_cast(total_quantity AS BIGINT) AS total_quantity,
            status,
            warehouse_source_path
        FROM product_snapshots
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW seller_daily_metrics AS
        SELECT
            run_id,
            warehouse_run_date AS run_date,
            collected_at_utc,
            supplier_id,
            supplier_name,
            try_cast(rating AS DOUBLE) AS rating,
            try_cast(valuation AS DOUBLE) AS valuation,
            try_cast(feedbacks_count AS BIGINT) AS feedbacks_count,
            try_cast(sale_item_quantity AS BIGINT) AS sale_item_quantity,
            try_cast(query_count AS BIGINT) AS query_count,
            try_cast(product_count AS BIGINT) AS product_count,
            queries_ref,
            nm_ids_ref,
            source_product_run_ids,
            status,
            warehouse_source_path
        FROM seller_snapshots
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_run_quality AS
        SELECT
            run_id,
            pipeline,
            status,
            started_at_utc,
            finished_at_utc,
            duration_seconds,
            items_ok,
            items_error,
            components_count,
            warehouse_source_path
        FROM run_reports
        """
    )


def export_parquet(con: duckdb.DuckDBPyConnection, warehouse_dir: Path) -> dict[str, str]:
    parquet_dir = warehouse_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    exported: dict[str, str] = {}
    for table in WAREHOUSE_TABLES:
        target = parquet_dir / f"{table}.parquet"
        if target.exists():
            target.unlink()
        con.execute(f"COPY {table} TO {sql_quote(str(target))} (FORMAT PARQUET)")
        exported[table] = str(target)
    return exported


def write_manifest(
    project_root: Path,
    warehouse_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    manifests_dir = warehouse_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    latest = manifests_dir / "latest.json"
    stamp = manifest["built_at_utc"].replace(":", "").replace("-", "")
    stamped = manifests_dir / f"warehouse_build_{stamp}.json"
    payload = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    encoded = payload.encode("utf-8")
    gate = integrity_gate(project_root)
    durable_atomic_replace(
        stamped.absolute(),
        encoded,
        mode=0o644,
        integrity_gate=gate,
    )
    durable_atomic_replace(
        latest.absolute(),
        encoded,
        mode=0o644,
        integrity_gate=gate,
    )
    return latest


def build(project_root: Path, dry_run: bool = False) -> dict[str, Any]:
    project_root = project_root.resolve()
    data_dir = project_root / "data"
    warehouse_dir = data_dir / "warehouse" / "wb"
    loaded_at = utc_now_iso()
    sources = discover_sources(project_root)

    if dry_run:
        return {
            "status": "dry_run",
            "built_at_utc": loaded_at,
            "project_root": str(project_root),
            "files": {name: len(paths) for name, paths in sources.items()},
            "would_write": {
                "database_path": str(warehouse_dir / "wb.duckdb"),
                "parquet_dir": str(warehouse_dir / "parquet"),
                "manifest_path": str(warehouse_dir / "manifests" / "latest.json"),
            },
            "limitations": MVP_LIMITATIONS,
        }

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    db_path = warehouse_dir / "wb.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        counts: dict[str, int] = {}
        for table in TABLE_SPECS:
            paths = sources[table]
            counts[table] = create_csv_table(con, table, paths, loaded_at)
        report_count, report_component_count = load_run_reports(con, project_root)
        counts["run_reports"] = report_count
        counts["run_report_components"] = report_component_count
        create_views(con)
        exported = export_parquet(con, warehouse_dir)
        manifest = {
            "status": "success",
            "built_at_utc": loaded_at,
            "project_root": str(project_root),
            "database_path": str(db_path),
            "parquet_exports": exported,
            "files": {name: len(paths) for name, paths in sources.items()},
            "rows": counts,
            "source_patterns": TABLE_SPECS,
            "tables": WAREHOUSE_TABLES,
            "views": ["query_positions", "seller_daily_metrics", "daily_run_quality"],
            "limitations": MVP_LIMITATIONS,
        }
        manifest_path = write_manifest(project_root, warehouse_dir, manifest)
        manifest["manifest_path"] = str(manifest_path)
        return manifest
    finally:
        con.close()


def check(project_root: Path) -> dict[str, Any]:
    project_root = project_root.resolve()
    warehouse_dir = project_root / "data" / "warehouse" / "wb"
    db_path = warehouse_dir / "wb.duckdb"
    manifest_path = warehouse_dir / "manifests" / "latest.json"
    if not db_path.exists():
        raise SystemExit(f"Warehouse database not found: {db_path}")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = {table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in WAREHOUSE_TABLES}
        sample = {
            "top_queries": con.execute(
                """
                SELECT query, count(*) AS rows
                FROM query_positions
                GROUP BY query
                ORDER BY rows DESC, query
                LIMIT 10
                """
            ).fetchall(),
            "latest_product_runs": con.execute(
                """
                SELECT run_id, count(*) AS rows
                FROM product_snapshots
                GROUP BY run_id
                ORDER BY run_id DESC
                LIMIT 5
                """
            ).fetchall(),
        }
        manifest = read_json(manifest_path) if manifest_path.exists() else None
        return {
            "status": "ok",
            "database_path": str(db_path),
            "manifest_path": str(manifest_path) if manifest_path.exists() else "",
            "rows": rows,
            "sample": sample,
            "manifest_built_at_utc": manifest.get("built_at_utc") if manifest else None,
        }
    finally:
        con.close()


def run_sql(project_root: Path, query: str) -> list[tuple[Any, ...]]:
    db_path = project_root.resolve() / "data" / "warehouse" / "wb" / "wb.duckdb"
    if not db_path.exists():
        raise SystemExit(f"Warehouse database not found: {db_path}")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(query).fetchall()
    finally:
        con.close()


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def main(argv: list[str] | None = None) -> int:
    _require_host_lease_after_cutover()
    parser = argparse.ArgumentParser(description="WB parser analytics warehouse MVP")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="rebuild warehouse from existing WB outputs")
    build_parser.add_argument("--dry-run", action="store_true")

    subparsers.add_parser("check", help="check warehouse row counts and sample queries")

    sql_parser = subparsers.add_parser("sql", help="run a read-only SQL query against warehouse")
    sql_parser.add_argument("query")

    migration_parser = subparsers.add_parser(
        "migrate-legacy-yaroslavl",
        help="safely migrate global WB warehouse history into regional storage",
    )
    migration_mode = migration_parser.add_mutually_exclusive_group(required=True)
    migration_mode.add_argument("--dry-run", action="store_true")
    migration_mode.add_argument("--apply", action="store_true")

    subparsers.add_parser(
        "check-legacy-yaroslavl",
        help="read-only validation of the regional legacy migration",
    )

    args = parser.parse_args(argv)
    if args.command == "build":
        print_json(build(args.project_root, dry_run=args.dry_run))
        return 0
    if args.command == "check":
        print_json(check(args.project_root))
        return 0
    if args.command == "sql":
        print_json(run_sql(args.project_root, args.query))
        return 0
    if args.command == "migrate-legacy-yaroslavl":
        project_root = args.project_root.resolve(strict=True)
        config = load_config(project_root / "config/config.yaml")
        run_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%SZ")
        try:
            result = migrate_legacy_yaroslavl_database(
                project_root=project_root,
                apply=args.apply,
                run_id=run_id,
                stale_seconds=config.runtime.lock_stale_seconds,
                integrity_gate=integrity_gate(project_root),
            )
        except CriticalPipelineError:
            print_json(
                {
                    "schema_version": "wb_legacy_yaroslavl_migration_v1",
                    "status": "failed",
                    "reason_code": "legacy_yaroslavl_migration_failed",
                }
            )
            return 2
        print_json(result)
        return 0
    if args.command == "check-legacy-yaroslavl":
        try:
            result = check_legacy_yaroslavl_database(
                project_root=args.project_root,
            )
        except CriticalPipelineError:
            print_json(
                {
                    "schema_version": "wb_legacy_yaroslavl_migration_v1",
                    "status": "failed",
                    "reason_code": "legacy_yaroslavl_check_failed",
                }
            )
            return 2
        print_json(result)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
