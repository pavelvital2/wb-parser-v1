from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .exceptions import CriticalPipelineError


DEFAULT_SERVICE_FIELDS = [
    "run_id",
    "component",
    "collected_at_utc",
    "source_system",
    "source_type",
    "source_ref",
    "status",
    "error_message",
]


def ensure_file(path: Path, *, non_empty: bool = True, label: str = "file") -> None:
    if not path.exists():
        raise CriticalPipelineError(f"Required {label} does not exist: {path}")
    if non_empty and path.stat().st_size == 0:
        raise CriticalPipelineError(f"Required {label} is empty: {path}")


def validate_text_contract(path: Path, *, min_lines: int = 1, encoding: str = "utf-8-sig") -> dict[str, float | int]:
    ensure_file(path, non_empty=True, label="text file")
    lines = [line.strip() for line in path.read_text(encoding=encoding).splitlines() if line.strip()]
    if len(lines) < min_lines:
        raise CriticalPipelineError(
            f"Text row count below minimum ({len(lines)} < {min_lines}): {path}"
        )
    return {"row_count": len(lines), "error_ratio": 0.0, "error_count": 0}


def validate_csv_contract(
    path: Path,
    *,
    required_columns: list[str],
    min_rows: int = 1,
    required_service_fields: bool = False,
    allowed_statuses: set[str] | None = None,
    max_error_ratio: float | None = None,
    status_column: str = "status",
    error_statuses: Iterable[str] = ("error",),
) -> dict[str, float | int]:
    ensure_file(path, non_empty=True, label="output file")

    required = list(required_columns)
    if required_service_fields:
        for c in DEFAULT_SERVICE_FIELDS:
            if c not in required:
                required.append(c)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames:
            raise CriticalPipelineError(f"CSV has no header: {path}")

        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise CriticalPipelineError(f"CSV missing columns {missing}: {path}")

        row_count = 0
        error_count = 0
        invalid_statuses: set[str] = set()
        allowed = {s.lower() for s in (allowed_statuses or set())}
        error_status = {s.lower() for s in error_statuses}

        for row in reader:
            row_count += 1
            raw_status = str(row.get(status_column, "") or "").strip().lower()

            if allowed and raw_status and raw_status not in allowed:
                invalid_statuses.add(raw_status)

            if raw_status in error_status:
                error_count += 1

        if row_count < min_rows:
            raise CriticalPipelineError(
                f"CSV row count below minimum ({row_count} < {min_rows}): {path}"
            )

        if invalid_statuses:
            raise CriticalPipelineError(
                f"CSV has unsupported statuses {sorted(invalid_statuses)} in '{status_column}': {path}"
            )

        error_ratio = (error_count / row_count) if row_count > 0 else 0.0
        if max_error_ratio is not None and row_count > 0 and error_ratio > max_error_ratio:
            raise CriticalPipelineError(
                f"CSV error ratio above threshold ({error_ratio:.4f} > {max_error_ratio:.4f}): {path}"
            )

    return {
        "row_count": row_count,
        "error_count": error_count,
        "error_ratio": round(error_ratio, 6),
    }


def smoke_validate_csv(path: Path, required_columns: list[str], min_rows: int = 1) -> None:
    validate_csv_contract(path, required_columns=required_columns, min_rows=min_rows)
