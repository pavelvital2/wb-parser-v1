from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from app.common.csv_io import write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.warehouse.wb_regional import (
    DUCKDB_MAX_THREADS,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_MEMORY_LIMIT_SETTING,
    REGIONAL_RUN_QUALITY_HASH_FIELDS,
    _create_schema,
    _regional_run_quality_sha256,
    bounded_regional_connection,
    ingest_regional_run,
    migrate_legacy_yaroslavl,
)


BRIDGE_FIELDS = [
    "run_id",
    "collection_plan_id",
    "query_pack_id",
    "query_pack_version",
    "query_id",
    "query",
    "query_group",
    "region_id",
    "region_name",
    "page",
    "position_on_page",
    "absolute_position",
    "nmId",
    "product_name",
    "brand",
    "supplier_id",
    "rating",
    "feedbacks",
    "total_quantity",
    "endpoint_id",
]
SELLER_FIELDS = [
    "supplier_id",
    "supplier_name",
    "rating",
    "valuation",
    "feedbacks_count",
    "sale_item_quantity",
    "status",
]


def _legacy_global_database(project: Path) -> None:
    path = project / "data/warehouse/wb/wb.duckdb"
    path.parent.mkdir(parents=True)
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE query_positions AS SELECT
                '20260701_000000Z'::VARCHAR AS run_id,
                '2026-07-01'::VARCHAR AS run_date,
                '2026-07-01T00:00:00+00:00'::VARCHAR AS collected_at_utc,
                'legacy-query'::VARCHAR AS query,
                1::INTEGER AS page,
                1::INTEGER AS position_on_page,
                1::INTEGER AS absolute_position,
                '9001'::VARCHAR AS product_id,
                '8001'::VARCHAR AS imt_id,
                'legacy product'::VARCHAR AS product_name,
                'legacy brand'::VARCHAR AS brand,
                '6001'::VARCHAR AS brand_id,
                '7001'::VARCHAR AS supplier_id,
                'legacy seller'::VARCHAR AS supplier_name,
                99.0::DOUBLE AS final_price,
                120.0::DOUBLE AS price,
                99.0::DOUBLE AS sale_price,
                17.5::DOUBLE AS discount,
                4.5::DOUBLE AS rating,
                10::BIGINT AS feedbacks,
                5::BIGINT AS total_quantity,
                'success'::VARCHAR AS status,
                'data/marts/serp/history.csv'::VARCHAR AS warehouse_source_path
            """
        )
        connection.execute(
            """
            CREATE TABLE daily_run_quality AS SELECT
                '20260701_000000Z'::VARCHAR AS run_id,
                'products_sellers_daily'::VARCHAR AS pipeline,
                'success'::VARCHAR AS status,
                '2026-07-01T00:00:00+00:00'::VARCHAR AS started_at_utc,
                '2026-07-01T00:10:00+00:00'::VARCHAR AS finished_at_utc,
                999.0::DOUBLE AS duration_seconds,
                77::BIGINT AS items_ok,
                0::BIGINT AS items_error,
                2::BIGINT AS components_count,
                'state/run_reports/legacy.json'::VARCHAR
                    AS warehouse_source_path
            """
        )
        connection.execute(
            """
            CREATE TABLE seller_daily_metrics AS SELECT
                '20260701_000000Z'::VARCHAR AS run_id,
                '2026-07-01'::VARCHAR AS run_date,
                '2026-07-01T00:00:00+00:00'::VARCHAR AS collected_at_utc,
                '7001'::VARCHAR AS supplier_id,
                'legacy seller'::VARCHAR AS supplier_name,
                4.8::DOUBLE AS rating,
                100::DOUBLE AS valuation,
                20::BIGINT AS feedbacks_count,
                30::BIGINT AS sale_item_quantity,
                1::BIGINT AS query_count,
                1::BIGINT AS product_count,
                'legacy-query'::VARCHAR AS queries_ref,
                '9001'::VARCHAR AS nm_ids_ref,
                '20260701_000000Z'::VARCHAR AS source_product_run_ids,
                'success'::VARCHAR AS status,
                'data/marts/sellers/history.csv'::VARCHAR AS warehouse_source_path
            """
        )
    finally:
        connection.close()


def _legacy_execute(project: Path, sql: str) -> None:
    path = project / "data/warehouse/wb/wb.duckdb"
    connection = duckdb.connect(str(path))
    try:
        connection.execute(sql)
    finally:
        connection.close()


def _sources(project: Path) -> tuple[Path, Path]:
    bridge_path = project / "scoped/bridge.csv"
    sellers_path = project / "scoped/sellers.csv"
    write_csv_rows(
        bridge_path,
        [
            {
                "run_id": "20260726_001600Z",
                "collection_plan_id": "shevron-four-regions-top1000-v2",
                "query_pack_id": "shevron-core",
                "query_pack_version": "2026-07-26.1",
                "query_id": "shevron",
                "query": "шеврон",
                "query_group": "shevron",
                "region_id": "moscow",
                "region_name": "Москва",
                "page": "1",
                "position_on_page": "1",
                "absolute_position": "1",
                "nmId": "1001",
                "product_name": "new product",
                "brand": "brand",
                "supplier_id": "2001",
                "rating": "4.9",
                "feedbacks": "50",
                "total_quantity": "10",
                "endpoint_id": "primary",
            }
        ],
        BRIDGE_FIELDS,
    )
    write_csv_rows(
        sellers_path,
        [
            {
                "supplier_id": "2001",
                "supplier_name": "new seller",
                "rating": "4.9",
                "valuation": "200",
                "feedbacks_count": "40",
                "sale_item_quantity": "60",
                "status": "success",
            }
        ],
        SELLER_FIELDS,
    )
    manifest_path = bridge_path.parent / "collection_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": "20260726_001600Z",
                "collection_plan_id": "shevron-four-regions-top1000-v2",
                "query_pack_id": "shevron-core",
                "query_pack_version": "2026-07-26.1",
                "status": "success",
                "complete": True,
                "started_at_utc": "2026-07-26T00:16:00+00:00",
                "finished_at_utc": "2026-07-26T00:20:00+00:00",
                "deadline_utc": "2026-07-26T06:16:00+00:00",
                "endpoint_usage": {
                    "primary": {"attempts": 1, "pages_ok": 1}
                },
                "regions": [
                    {
                        "region_id": "moscow",
                        "status": "success",
                        "complete": True,
                        "queries_ok": 1,
                        "pages_ok": 1,
                        "products_ok": 1,
                        "duplicate_product_positions": 0,
                    }
                ],
                "resume": {
                    "segments": [
                        {
                            "region_id": "moscow",
                            "query_id": "shevron",
                            "sha256": "a" * 64,
                            "egress": {
                                "verification_status": "verified_constant"
                            },
                            "completion": {
                                "payload_total": 1,
                                "capped_total": 1,
                                "pages_count": 1,
                                "products_count": 1,
                                "terminal_page": 1,
                                "terminal_reason": "payload_total_reached",
                                "complete": True,
                                "duplicate_product_positions": 0,
                            },
                        }
                    ]
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return bridge_path, sellers_path


def test_regional_warehouse_is_idempotent_and_assigns_legacy_to_yaroslavl(
    tmp_path: Path,
) -> None:
    _legacy_global_database(tmp_path)
    bridge_path, sellers_path = _sources(tmp_path)
    first = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    second = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    assert first["status"] == "success"
    assert second["status"] == "already_ingested"
    assert first["legacy"]["positions"] == 1
    database = duckdb.connect(
        str(tmp_path / "data/warehouse/wb_regional/wb_regional.duckdb"),
        read_only=True,
    )
    try:
        rows = database.execute(
            "SELECT region_id, region_provenance, count(*) "
            "FROM regional_query_positions GROUP BY ALL ORDER BY region_id"
        ).fetchall()
        legacy_fields = database.execute(
            """
            SELECT imt_id, brand_id, supplier_name, final_price, price,
                   sale_price, discount, status, collected_at_utc
            FROM regional_query_positions
            WHERE region_id = 'yaroslavl'
            """
        ).fetchone()
        quality_counts = database.execute(
            """
            SELECT
                (SELECT count(*) FROM regional_run_quality),
                (SELECT count(*) FROM regional_query_quality)
            """
        ).fetchone()
        legacy_quality = database.execute(
            """
            SELECT duration_seconds, items_ok,
                   queries_expected, queries_ok,
                   pages_max, pages_ok,
                   positions_max, positions_ok
            FROM regional_run_quality
            WHERE region_id = 'yaroslavl'
            """
        ).fetchone()
        position_columns = {
            row[0]
            for row in database.execute(
                "DESCRIBE regional_query_positions"
            ).fetchall()
        }
        run_quality_columns = {
            row[0]
            for row in database.execute(
                "DESCRIBE regional_run_quality"
            ).fetchall()
        }
    finally:
        database.close()
    assert rows == [
        ("moscow", "scoped_collection_plan", 1),
        ("yaroslavl", "legacy_global_assigned_yaroslavl", 1),
    ]
    assert legacy_fields == (
        "8001",
        "6001",
        "legacy seller",
        99.0,
        120.0,
        99.0,
        17.5,
        "success",
        "2026-07-01T00:00:00+00:00",
    )
    assert quality_counts == (2, 1)
    assert legacy_quality == (600.0, 77, 1, 1, 1, 1, 1, 1)
    assert {
        "collected_at_utc",
        "imt_id",
        "brand_id",
        "supplier_name",
        "final_price",
        "price",
        "sale_price",
        "discount",
        "status",
    }.issubset(position_columns)
    assert {
        "queries_expected",
        "queries_ok",
        "pages_max",
        "pages_ok",
        "positions_max",
        "positions_ok",
        "duplicate_product_positions",
        "endpoint_usage_json",
        "source_manifest_sha256",
    }.issubset(run_quality_columns)


def test_regional_warehouse_rechecks_integrity_inside_transaction(
    tmp_path: Path,
) -> None:
    bridge_path, sellers_path = _sources(tmp_path)

    def reject_drift() -> None:
        raise CriticalPipelineError("input attestation changed")

    with pytest.raises(CriticalPipelineError, match="input attestation changed"):
        ingest_regional_run(
            project_root=tmp_path,
            run_id="20260726_001600Z",
            collection_plan_id="shevron-four-regions-top1000-v2",
            bridge_path=bridge_path,
            sellers_path=sellers_path,
            integrity_gate=reject_drift,
        )

    database = duckdb.connect(
        str(tmp_path / "data/warehouse/wb_regional/wb_regional.duckdb"),
        read_only=True,
    )
    try:
        assert database.execute(
            "SELECT count(*) FROM regional_ingestions"
        ).fetchone()[0] == 0
        assert database.execute(
            "SELECT count(*) FROM regional_query_positions"
        ).fetchone()[0] == 0
    finally:
        database.close()


def test_regional_run_quality_hash_covers_every_retained_field(
    tmp_path: Path,
) -> None:
    bridge_path, sellers_path = _sources(tmp_path)
    ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    database = duckdb.connect(
        str(tmp_path / "data/warehouse/wb_regional/wb_regional.duckdb"),
        read_only=True,
    )
    try:
        description = database.execute(
            "DESCRIBE regional_run_quality"
        ).fetchall()
        columns = [row[0] for row in description]
        values = database.execute(
            "SELECT * FROM regional_run_quality "
            "WHERE region_id = 'moscow'"
        ).fetchone()
    finally:
        database.close()
    assert values is not None
    stored = dict(zip(columns, values, strict=True))
    stored_hash = stored.pop("source_row_sha256")
    assert tuple(stored) == REGIONAL_RUN_QUALITY_HASH_FIELDS
    assert _regional_run_quality_sha256(stored) == stored_hash

    for field in REGIONAL_RUN_QUALITY_HASH_FIELDS:
        mutated = dict(stored)
        value = mutated[field]
        if value is None:
            mutated[field] = 1.0
        elif type(value) is bool:
            mutated[field] = not value
        elif isinstance(value, int | float):
            mutated[field] = value + 1
        else:
            mutated[field] = f"{value}-changed"
        assert _regional_run_quality_sha256(mutated) != stored_hash, field


def test_regional_warehouse_rejects_changed_source_for_same_run(
    tmp_path: Path,
) -> None:
    bridge_path, sellers_path = _sources(tmp_path)
    ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    bridge_path.write_text(
        bridge_path.read_text(encoding="utf-8-sig") + "\n",
        encoding="utf-8-sig",
    )
    with pytest.raises(CriticalPipelineError, match="source hash mismatch"):
        ingest_regional_run(
            project_root=tmp_path,
            run_id="20260726_001600Z",
            collection_plan_id="shevron-four-regions-top1000-v2",
            bridge_path=bridge_path,
            sellers_path=sellers_path,
        )


def test_regional_duckdb_runtime_is_bounded_and_temp_session_is_cleaned(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "data/warehouse/wb_regional/test.duckdb"
    database_path.parent.mkdir(parents=True)
    with bounded_regional_connection(
        project_root=tmp_path,
        database_path=database_path,
    ) as connection:
        contract = connection.execute(
            "SELECT memory_limit, threads, temp_directory "
            "FROM wb_regional_runtime_contract"
        ).fetchone()
        actual = connection.execute(
            "SELECT current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('temp_directory')"
        ).fetchone()
        assert contract[0] == DUCKDB_MEMORY_LIMIT
        assert contract[1] == DUCKDB_MAX_THREADS
        assert actual == (
            DUCKDB_MEMORY_LIMIT_SETTING,
            DUCKDB_MAX_THREADS,
            contract[2],
        )
        temp_dir = Path(contract[2])
        assert temp_dir.is_dir()
        assert temp_dir.stat().st_mode & 0o777 == 0o700
    assert not list(
        (tmp_path / "data/warehouse/wb_regional/tmp").glob("session-*")
    )


def test_legacy_sync_rejects_unbounded_duckdb_connection(
    tmp_path: Path,
) -> None:
    _legacy_global_database(tmp_path)
    database_path = tmp_path / "unbounded.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        _create_schema(connection)
        with pytest.raises(CriticalPipelineError, match="bounded DuckDB runtime"):
            migrate_legacy_yaroslavl(
                project_root=tmp_path,
                connection=connection,
            )
    finally:
        connection.close()


@pytest.mark.parametrize("setting", ["memory_limit", "threads", "temp_directory"])
def test_legacy_sync_rejects_tampered_actual_duckdb_settings(
    tmp_path: Path,
    setting: str,
) -> None:
    _legacy_global_database(tmp_path)
    database_path = tmp_path / "data/warehouse/wb_regional/test.duckdb"
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with bounded_regional_connection(
        project_root=tmp_path,
        database_path=database_path,
    ) as connection:
        if setting == "memory_limit":
            connection.execute("SET memory_limit = '2GiB'")
        elif setting == "threads":
            connection.execute("SET threads = 1")
        else:
            wrong_temp = tmp_path / "wrong-temp"
            wrong_temp.mkdir(mode=0o700)
            connection.execute(
                "SET temp_directory = ?",
                [str(wrong_temp)],
            )
        with pytest.raises(
            CriticalPipelineError,
            match="bounded DuckDB runtime is invalid",
        ):
            migrate_legacy_yaroslavl(
                project_root=tmp_path,
                connection=connection,
            )


def test_legacy_run_quality_uses_observed_position_counts(
    tmp_path: Path,
) -> None:
    _legacy_global_database(tmp_path)
    _legacy_execute(
        tmp_path,
        """
        INSERT INTO query_positions SELECT
            '20260701_000000Z', '2026-07-01',
            '2026-07-01T00:00:01+00:00',
            'legacy-query', 2, 1, 101, '9001', '8001', 'legacy product',
            'legacy brand', '6001', '7001', 'legacy seller',
            99.0, 120.0, 99.0, 17.5, 4.5, 10, 5, 'success',
            'data/marts/serp/history.csv'
        UNION ALL SELECT
            '20260701_000000Z', '2026-07-01',
            '2026-07-01T00:00:02+00:00',
            'second-query', 1, 1, 1, '9002', '8002', 'second product',
            'legacy brand', '6001', '7001', 'legacy seller',
            100.0, 121.0, 100.0, 17.4, 4.6, 11, 6, 'success',
            'data/marts/serp/history.csv'
        """,
    )
    bridge_path, sellers_path = _sources(tmp_path)
    result = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    database = duckdb.connect(result["database_path"], read_only=True)
    try:
        quality = database.execute(
            """
            SELECT items_ok, queries_expected, queries_ok,
                   pages_max, pages_ok, positions_max, positions_ok,
                   duplicate_product_positions, duration_seconds
            FROM regional_run_quality
            WHERE region_id = 'yaroslavl'
            """
        ).fetchone()
    finally:
        database.close()
    assert quality == (77, 2, 2, 3, 3, 3, 3, 1, 600.0)


def test_legacy_run_quality_without_positions_uses_zero_and_null_duration(
    tmp_path: Path,
) -> None:
    _legacy_global_database(tmp_path)
    _legacy_execute(
        tmp_path,
        """
        INSERT INTO daily_run_quality SELECT
            '20260702_000000Z', 'products_sellers_daily', 'success',
            'not-a-timestamp', '2026-07-02T00:10:00+00:00',
            600.0, 55, 2, 3, 'state/run_reports/no-positions.json'
        """,
    )
    bridge_path, sellers_path = _sources(tmp_path)
    result = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    database = duckdb.connect(result["database_path"], read_only=True)
    try:
        quality = database.execute(
            """
            SELECT duration_seconds, items_ok,
                   queries_expected, queries_ok,
                   pages_max, pages_ok, positions_max, positions_ok
            FROM regional_run_quality
            WHERE run_id = '20260702_000000Z'
              AND region_id = 'yaroslavl'
            """
        ).fetchone()
    finally:
        database.close()
    assert quality == (None, 55, 0, 0, 0, 0, 0, 0)


def test_legacy_sync_appends_new_run_without_rewriting_prior_rows(
    tmp_path: Path,
) -> None:
    _legacy_global_database(tmp_path)
    bridge_path, sellers_path = _sources(tmp_path)
    first = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    _legacy_execute(
        tmp_path,
        """
        INSERT INTO query_positions SELECT
            '20260702_000000Z', '2026-07-02', '2026-07-02T00:00:00+00:00',
            'legacy-query', 1, 1, 1, '9002', '8002', 'new legacy product',
            'legacy brand', '6001', '7002', 'new legacy seller',
            100.0, 121.0, 100.0, 17.4, 4.6, 11, 6, 'success',
            'data/marts/serp/new.csv'
        """,
    )
    _legacy_execute(
        tmp_path,
        """
        INSERT INTO seller_daily_metrics SELECT
            '20260702_000000Z', '2026-07-02', '2026-07-02T00:00:00+00:00',
            '7002', 'new legacy seller', 4.9, 101, 21, 31, 1, 1,
            'legacy-query', '9002', '20260702_000000Z', 'success',
            'data/marts/sellers/new.csv'
        """,
    )
    second = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    assert first["legacy"]["inserted_positions"] == 1
    assert second["legacy"]["positions"] == 2
    assert second["legacy"]["inserted_positions"] == 1
    assert second["legacy"]["inserted_sellers"] == 1


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            "UPDATE query_positions SET product_name = 'tampered' "
            "WHERE run_id = '20260701_000000Z'",
            "changed an already imported row",
        ),
        (
            "DELETE FROM query_positions WHERE run_id = '20260701_000000Z'",
            "removed an already imported row",
        ),
        (
            "UPDATE daily_run_quality SET items_ok = 2 "
            "WHERE run_id = '20260701_000000Z'",
            "changed an already imported row",
        ),
        (
            "DELETE FROM seller_daily_metrics "
            "WHERE run_id = '20260701_000000Z'",
            "removed an already imported row",
        ),
    ],
)
def test_legacy_sync_rejects_changed_or_missing_imported_fact_transactionally(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    _legacy_global_database(tmp_path)
    bridge_path, sellers_path = _sources(tmp_path)
    ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    _legacy_execute(
        tmp_path,
        """
        INSERT INTO query_positions SELECT
            '20260702_000000Z', '2026-07-02', '2026-07-02T00:00:00+00:00',
            'legacy-query', 1, 1, 1, '9002', '8002', 'new legacy product',
            'legacy brand', '6001', '7002', 'new legacy seller',
            100.0, 121.0, 100.0, 17.4, 4.6, 11, 6, 'success',
            'data/marts/serp/new.csv'
        """,
    )
    _legacy_execute(tmp_path, mutation)
    with pytest.raises(CriticalPipelineError, match=message):
        ingest_regional_run(
            project_root=tmp_path,
            run_id="20260726_001600Z",
            collection_plan_id="shevron-four-regions-top1000-v2",
            bridge_path=bridge_path,
            sellers_path=sellers_path,
        )
    database = duckdb.connect(
        str(tmp_path / "data/warehouse/wb_regional/wb_regional.duckdb"),
        read_only=True,
    )
    try:
        imported_new = database.execute(
            "SELECT count(*) FROM regional_query_positions "
            "WHERE run_id = '20260702_000000Z'"
        ).fetchone()[0]
    finally:
        database.close()
    assert imported_new == 0


def test_position_fact_key_preserves_repeated_product_at_different_positions(
    tmp_path: Path,
) -> None:
    bridge_path, sellers_path = _sources(tmp_path)
    rows = list(csv.DictReader(
        bridge_path.open("r", encoding="utf-8-sig", newline=""),
        delimiter=";",
    ))
    repeated = dict(rows[0])
    repeated.update(
        {
            "page": "1",
            "position_on_page": "2",
            "absolute_position": "2",
        }
    )
    write_csv_rows(bridge_path, [rows[0], repeated], BRIDGE_FIELDS)
    manifest_path = bridge_path.parent / "collection_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    completion = manifest["resume"]["segments"][0]["completion"]
    completion.update(
        {
            "payload_total": 2,
            "capped_total": 2,
            "products_count": 2,
            "duplicate_product_positions": 1,
        }
    )
    manifest["regions"][0].update(
        {
            "products_ok": 2,
            "duplicate_product_positions": 1,
        }
    )
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result = ingest_regional_run(
        project_root=tmp_path,
        run_id="20260726_001600Z",
        collection_plan_id="shevron-four-regions-top1000-v2",
        bridge_path=bridge_path,
        sellers_path=sellers_path,
    )
    assert result["positions_count"] == 2
    assert result["duplicate_product_positions"] == 1
    database = duckdb.connect(result["database_path"], read_only=True)
    try:
        facts = database.execute(
            "SELECT product_id, absolute_position "
            "FROM regional_query_positions WHERE region_id = 'moscow' "
            "ORDER BY absolute_position"
        ).fetchall()
    finally:
        database.close()
    assert facts == [("1001", 1), ("1001", 2)]
