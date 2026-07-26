from __future__ import annotations

import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from app.common.config import AppConfig
from app.common.logging_setup import get_logger


ALLOWED_ACTIONS = {"suggest", "filter", "serp", "sellers", "monthly", "daily"}


@dataclass(slots=True)
class WebUIServices:
    config: AppConfig
    logger: Any = field(init=False)

    def __post_init__(self) -> None:
        self.logger = get_logger("webui")

    @property
    def db_path(self) -> Path:
        return self.config.paths.SQLITE_DB

    def log_ui_action(self, user: str, action: str, details: str = "") -> None:
        self.logger.info(
            f"UI_ACTION user={user} action={action} details={details}",
            extra={"component": "webui", "status": "info"},
        )

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    r.run_id,
                    COALESCE(r.pipeline, r.component, '') AS component,
                    r.status,
                    r.started_at_utc,
                    r.finished_at_utc,
                    COALESCE((
                        SELECT e.error_message
                        FROM errors e
                        WHERE e.run_id = r.run_id
                        ORDER BY e.id DESC
                        LIMIT 1
                    ), '') AS error_message
                FROM runs r
                ORDER BY r.created_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def latest_outputs(self) -> list[dict[str, str]]:
        targets = [
            ("suggest", "staging", "suggest_alpha_staging.csv"),
            ("filter", "marts", "top_queries.csv"),
            ("serp", "marts", "products_daily.csv"),
            ("sellers", "marts", "sellers_daily.csv"),
        ]
        out: list[dict[str, str]] = []
        for component, layer, filename in targets:
            p = self.config.paths.latest_output_path(layer=layer, component=component, filename=filename)
            if p is None:
                out.append(
                    {
                        "component": component,
                        "layer": layer,
                        "filename": filename,
                        "path": "",
                        "size": "",
                        "modified_at": "",
                        "status": "missing",
                    }
                )
                continue

            stat = p.stat()
            out.append(
                {
                    "component": component,
                    "layer": layer,
                    "filename": filename,
                    "path": self._as_project_relative(p),
                    "size": str(stat.st_size),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "status": "ok",
                }
            )
        return out

    def list_log_files(self) -> list[str]:
        if not self.config.paths.LOG_DIR.exists():
            return []
        return sorted([p.name for p in self.config.paths.LOG_DIR.glob("*.log") if p.is_file()])

    def tail_log(self, filename: str, lines: int = 200) -> list[str]:
        safe_name = Path(filename).name
        target = (self.config.paths.LOG_DIR / safe_name).resolve()
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Log file not found: {safe_name}")
        if not target.is_relative_to(self.config.paths.LOG_DIR.resolve()):
            raise ValueError("Invalid log file path")

        content = target.read_text(encoding="utf-8", errors="replace").splitlines()
        return content[-max(1, min(lines, 5000)) :]

    def list_roots(self) -> dict[str, Path]:
        return {
            "raw": self.config.paths.RAW_DIR,
            "staging": self.config.paths.STAGING_DIR,
            "marts": self.config.paths.MARTS_DIR,
            "exports": self.config.paths.EXPORTS_DIR,
        }

    def resolve_file_path(self, root_key: str, relative_path: str) -> Path:
        roots = self.list_roots()
        if root_key not in roots:
            raise ValueError("Unknown root")
        root = roots[root_key].resolve()
        candidate = (root / relative_path).resolve()
        if not candidate.is_relative_to(root):
            raise ValueError("Path traversal blocked")
        if not candidate.exists() or not candidate.is_file():
            raise FileNotFoundError("File not found")
        return candidate

    def list_files(self, root_key: str, subdir: str = "") -> list[dict[str, str]]:
        roots = self.list_roots()
        if root_key not in roots:
            raise ValueError("Unknown root")

        root = roots[root_key].resolve()
        start = (root / subdir).resolve() if subdir else root
        if not start.is_relative_to(root):
            raise ValueError("Path traversal blocked")
        if not start.exists() or not start.is_dir():
            return []

        rows: list[dict[str, str]] = []
        for p in start.rglob("*"):
            if not p.is_file():
                continue
            stat = p.stat()
            rows.append(
                {
                    "relative_path": p.relative_to(root).as_posix(),
                    "absolute_path": str(p),
                    "size": str(stat.st_size),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                }
            )

        rows.sort(key=lambda x: x["modified_at"], reverse=True)
        return rows[:2000]

    def config_file_path(self, name: str) -> Path:
        mapping = {
            "prefixes": self.config.project_root / "config" / "prefixes.txt",
            "query_rules": self.config.project_root / "config" / "query_rules.yaml",
            "main": self.config.config_file,
        }
        if name not in mapping:
            raise ValueError("Unknown config file")
        return mapping[name]

    def load_text_config(self, name: str) -> str:
        p = self.config_file_path(name)
        if not p.exists():
            return ""
        return p.read_text(encoding="utf-8")

    def save_text_config(self, name: str, content: str) -> None:
        p = self.config_file_path(name)
        p.parent.mkdir(parents=True, exist_ok=True)

        if name in {"query_rules", "main"}:
            yaml.safe_load(content or "")

        if name == "prefixes":
            content = (content or "").replace("\r\n", "\n")
            if content and not content.endswith("\n"):
                content += "\n"

        p.write_text(content, encoding="utf-8")

    def save_wordstat_upload(self, filename: str, payload: bytes) -> Path:
        clean_name = Path(filename or "wordstat_upload.csv").name
        if not clean_name.lower().endswith(".csv"):
            clean_name = f"{clean_name}.csv"

        target_dir = self.config.paths.RAW_DIR / "wordstat"
        target_dir.mkdir(parents=True, exist_ok=True)

        target = target_dir / clean_name
        if target.exists():
            stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            target = target_dir / f"{target.stem}_{stamp}{target.suffix}"

        target.write_bytes(payload)
        return target

    def start_action(self, target: str, user: str) -> tuple[bool, str]:
        action = target.strip().lower()
        if action not in ALLOWED_ACTIONS:
            return False, f"Unsupported action: {target}"

        if action == "filter":
            cmd = [
                sys.executable,
                str(self.config.project_root / "main.py"),
                "--config",
                str(self.config.config_file),
                "run",
                action,
                "--job-id",
                f"webui_{user}",
            ]
        else:
            cmd = [
                str(
                    self.config.project_root
                    / "scripts"
                    / "run_wb_live_component.sh"
                ),
                action,
                "--job-id",
                f"webui_{user}",
            ]

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(self.config.project_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:
            return False, f"Failed to start process: {exc}"

        self.log_ui_action(user=user, action=f"run_{action}", details=f"pid={proc.pid}")
        return True, f"Task started: {action} (pid={proc.pid})"

    def _as_project_relative(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.config.project_root).as_posix()
        except ValueError:
            return str(path)
