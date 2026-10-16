"""Every file in the loaded GTFS zip has the same number of rows in its raw table."""

import csv
import io
import zipfile
from pathlib import Path

import pytest
from psycopg import sql

from quebec_train_to_trail.ingest.via_gtfs import RAW_DIR

ZIP_PATH = RAW_DIR / "viarail.zip"  # the zip the flow loaded


def gtfs_files() -> list[str]:
    """Names of the .txt files in the zip."""
    with zipfile.ZipFile(ZIP_PATH) as zf:
        return [n for n in zf.namelist() if n.endswith(".txt")]


def count_rows(file_name: str) -> int:
    """Data rows in a CSV file in the zip, header excluded."""
    with zipfile.ZipFile(ZIP_PATH) as zf, zf.open(file_name) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        next(reader)  # header
        return sum(1 for _ in reader)


@pytest.mark.parametrize("file_name", gtfs_files())
def test_raw_table_matches_file(conn, file_name):
    table = sql.Identifier("raw", Path(file_name).stem)
    (rows,) = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(table)).fetchone()
    assert rows == count_rows(file_name)
