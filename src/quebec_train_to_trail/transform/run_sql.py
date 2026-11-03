"""Run the transform SQL files into the staging schema.

Spec:
- Run every db/transform/NN_<result>.sql in number order (ADR 0013).
- Each file is one Prefect task with its own connection and transaction: if a file fails,
  its changes roll back and the flow stops there.
- Log the rows each file wrote.

Run from the repo root, after the GTFS flow:
    uv run --env-file .env python -m quebec_train_to_trail.transform.run_sql
"""

import re
from pathlib import Path

from prefect import flow, get_run_logger, task

from quebec_train_to_trail.ingest.via_gtfs import connect

SQL_DIR = Path("db/transform")
FILE_NAME = re.compile(r"\d{2}_\w+\.sql")


@task
def list_sql_files(sql_dir: Path) -> list[Path]:
    """Return the SQL files in run order. Fail on a file that breaks the naming rule."""
    paths = sorted(sql_dir.glob("*.sql"))
    bad = [p.name for p in paths if not FILE_NAME.fullmatch(p.name)]
    if bad:
        raise ValueError(f"Not named NN_<result>.sql (ADR 0013): {bad}")
    return paths


@task(task_run_name="{path.name}")
def run_sql_file(path: Path) -> int:
    """Run one file in a single transaction. Return the rows its last INSERT wrote."""
    with connect() as conn, conn.cursor() as cur:
        # One execute call runs every statement in the file; leaving the block commits,
        # and an error rolls the whole file back.
        cur.execute(path.read_text())
        # There is one result per statement. Keep the count of the last one that wrote
        # rows (an INSERT); DROP, CREATE and CREATE INDEX report -1.
        rows = cur.rowcount
        while cur.nextset():
            if cur.rowcount >= 0:
                rows = cur.rowcount
        return rows


@flow
def transform() -> None:
    """Run each transform SQL file in order."""
    logger = get_run_logger()
    for path in list_sql_files(SQL_DIR):
        rows = run_sql_file(path)
        logger.info("%s: %d rows", path.name, rows)


if __name__ == "__main__":
    transform()
