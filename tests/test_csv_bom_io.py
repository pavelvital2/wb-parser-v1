from __future__ import annotations

from app.common.csv_io import write_csv_rows


def test_write_csv_rows_writes_utf8_bom(tmp_path):
    path = tmp_path / "sample.csv"
    write_csv_rows(path, [{"name": "Товар"}], ["name"])
    raw = path.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
