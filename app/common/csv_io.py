from __future__ import annotations

import csv
from pathlib import Path


CSV_ENCODING = "utf-8-sig"
CSV_DELIMITER = ";"
CSV_NEWLINE = ""


def write_csv_rows(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=CSV_ENCODING, newline=CSV_NEWLINE) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=CSV_DELIMITER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def append_csv_rows(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", encoding=CSV_ENCODING, newline=CSV_NEWLINE) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=CSV_DELIMITER)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_csv_rows(path: Path, encoding: str = "utf-8-sig") -> list[dict[str, str]]:
    with path.open("r", encoding=encoding, newline=CSV_NEWLINE) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        return [dict(row) for row in reader]

