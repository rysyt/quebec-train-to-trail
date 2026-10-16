"""Run each SQL check in tests/sql/. A check returns the rows that break it; none = pass."""

from pathlib import Path

import pytest

SQL_DIR = Path(__file__).parent / "sql"


@pytest.mark.parametrize("path", sorted(SQL_DIR.glob("*.sql")), ids=lambda p: p.stem)
def test_sql_check(conn, path):
    bad_rows = conn.execute(path.read_text()).fetchall()
    assert bad_rows == [], f"{len(bad_rows)} bad rows, first ones: {bad_rows[:10]}"
