from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class StateDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline TEXT NOT NULL,
                    job_id TEXT,
                    status TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    started_at_utc TEXT,
                    finished_at_utc TEXT,
                    items_ok INTEGER NOT NULL DEFAULT 0,
                    items_error INTEGER NOT NULL DEFAULT 0,
                    critical_errors INTEGER NOT NULL DEFAULT 0,
                    non_critical_errors INTEGER NOT NULL DEFAULT 0,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    component TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at_utc TEXT,
                    finished_at_utc TEXT,
                    items_ok INTEGER NOT NULL DEFAULT 0,
                    items_error INTEGER NOT NULL DEFAULT 0,
                    note TEXT,
                    UNIQUE(run_id, component)
                );

                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    component TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    error_code TEXT,
                    error_class TEXT,
                    error_message TEXT,
                    source_ref TEXT,
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    component TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    checkpoint_value TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (component, checkpoint_key)
                );
                """
            )
            self._migrate_runs_table(conn)
            self._migrate_errors_table(conn)

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    def _runs_columns(self, conn: sqlite3.Connection) -> set[str]:
        return self._table_columns(conn, "runs")

    def _migrate_runs_table(self, conn: sqlite3.Connection) -> None:
        cols = self._runs_columns(conn)
        if "pipeline" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN pipeline TEXT")
            if "component" in cols:
                conn.execute("UPDATE runs SET pipeline = component WHERE pipeline IS NULL OR pipeline = ''")
            else:
                conn.execute("UPDATE runs SET pipeline = 'unknown' WHERE pipeline IS NULL OR pipeline = ''")

        cols = self._runs_columns(conn)
        if "component" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN component TEXT")
            conn.execute("UPDATE runs SET component = pipeline WHERE component IS NULL OR component = ''")

    def _migrate_errors_table(self, conn: sqlite3.Connection) -> None:
        cols = self._table_columns(conn, "errors")
        if "error_code" not in cols:
            conn.execute("ALTER TABLE errors ADD COLUMN error_code TEXT")
            conn.execute("UPDATE errors SET error_code = '' WHERE error_code IS NULL")

    def create_run(self, run_id: str, pipeline: str, job_id: str, created_at_utc: str) -> None:
        with self._connect() as conn:
            cols = self._runs_columns(conn)
            if "component" in cols:
                conn.execute(
                    """
                    INSERT INTO runs(run_id, component, pipeline, job_id, status, created_at_utc, started_at_utc)
                    VALUES(?, ?, ?, ?, 'running', ?, ?)
                    """,
                    (run_id, pipeline, pipeline, job_id, created_at_utc, created_at_utc),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO runs(run_id, pipeline, job_id, status, created_at_utc, started_at_utc)
                    VALUES(?, ?, ?, 'running', ?, ?)
                    """,
                    (run_id, pipeline, job_id, created_at_utc, created_at_utc),
                )

    def finish_run(
        self,
        run_id: str,
        status: str,
        finished_at_utc: str,
        items_ok: int = 0,
        items_error: int = 0,
        critical_errors: int = 0,
        non_critical_errors: int = 0,
        note: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = ?,
                    finished_at_utc = ?,
                    items_ok = ?,
                    items_error = ?,
                    critical_errors = ?,
                    non_critical_errors = ?,
                    note = ?
                WHERE run_id = ?
                """,
                (status, finished_at_utc, items_ok, items_error, critical_errors, non_critical_errors, note, run_id),
            )

    def create_task(self, run_id: str, component: str, started_at_utc: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks(run_id, component, status, started_at_utc)
                VALUES(?, ?, 'running', ?)
                ON CONFLICT(run_id, component) DO UPDATE SET
                    status='running',
                    started_at_utc=excluded.started_at_utc,
                    finished_at_utc=NULL,
                    items_ok=0,
                    items_error=0,
                    note=''
                """,
                (run_id, component, started_at_utc),
            )

    def finish_task(self, run_id: str, component: str, status: str, finished_at_utc: str, items_ok: int = 0, items_error: int = 0, note: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status=?, finished_at_utc=?, items_ok=?, items_error=?, note=?
                WHERE run_id=? AND component=?
                """,
                (status, finished_at_utc, items_ok, items_error, note, run_id, component),
            )

    def save_checkpoint(self, component: str, checkpoint_key: str, checkpoint_value: str, updated_at_utc: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints(component, checkpoint_key, checkpoint_value, updated_at_utc)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(component, checkpoint_key)
                DO UPDATE SET checkpoint_value = excluded.checkpoint_value,
                              updated_at_utc = excluded.updated_at_utc
                """,
                (component, checkpoint_key, checkpoint_value, updated_at_utc),
            )

    def delete_checkpoints(self, component: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM checkpoints WHERE component = ?", (component,))

    def list_checkpoint_keys(self, component: str) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT checkpoint_key FROM checkpoints WHERE component = ?",
                (component,),
            ).fetchall()
            return {str(row["checkpoint_key"]) for row in rows}

    def get_checkpoint(self, component: str, checkpoint_key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT checkpoint_value FROM checkpoints WHERE component = ? AND checkpoint_key = ?",
                (component, checkpoint_key),
            ).fetchone()
            return None if row is None else str(row["checkpoint_value"])

    def record_error(
        self,
        run_id: str,
        component: str,
        severity: str,
        error_class: str,
        error_message: str,
        source_ref: str,
        created_at_utc: str,
        error_code: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO errors(run_id, component, severity, error_code, error_class, error_message, source_ref, created_at_utc)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, component, severity, error_code, error_class, error_message[:2000], source_ref, created_at_utc),
            )

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            cols = self._runs_columns(conn)
            pipeline_expr = "COALESCE(pipeline, component)" if "component" in cols else "pipeline"
            rows = conn.execute(
                """
                SELECT run_id, """ + pipeline_expr + """ AS pipeline, status, created_at_utc, items_ok, items_error
                FROM runs
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            cols = self._runs_columns(conn)
            pipeline_expr = "COALESCE(pipeline, component)" if "component" in cols else "pipeline"
            row = conn.execute(
                """
                SELECT run_id, """ + pipeline_expr + """ AS pipeline, status, created_at_utc,
                       started_at_utc, finished_at_utc, items_ok, items_error,
                       critical_errors, non_critical_errors, note
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            return None if row is None else dict(row)

    def list_tasks(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, component, status, started_at_utc, finished_at_utc,
                       items_ok, items_error, note
                FROM tasks
                WHERE run_id = ?
                ORDER BY component ASC
                """,
                (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_errors(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, component, severity, error_code, error_class, error_message, source_ref, created_at_utc
                FROM errors
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]
