"""Run the whole pipeline: ingest, transform, then a data-quality gate.

Spec:
- Ingest the VIA GTFS feed and OpenStreetMap into raw, then run the transform SQL into
  staging (subflows).
- Gate: run every check in tests/sql/ (ADR 0015) as its own task. A check that returns rows
  fails its task and logs the first rows. Every check runs, then the pipeline fails if any did,
  so bad data never moves on to analytics.

Run from the repo root:
    uv run --env-file .env python -m quebec_train_to_trail.pipeline
"""

from pathlib import Path

from prefect import flow, get_run_logger, task

from quebec_train_to_trail.ingest.osm import ingest_osm
from quebec_train_to_trail.ingest.via_gtfs import connect, ingest_via_gtfs
from quebec_train_to_trail.transform.run_sql import transform

CHECK_DIR = Path("tests/sql")


class BadRowsError(Exception):
    """A data check returned rows that break its rule."""


@task(task_run_name="{path.stem}")
def run_check(path: Path) -> None:
    """Run one check. Raise if it returns any rows."""
    with connect() as conn:
        bad_rows = conn.execute(path.read_text()).fetchall()
    if bad_rows:
        raise BadRowsError(f"{len(bad_rows)} bad rows, first ones: {bad_rows[:10]}")


@flow
def quality_gate() -> None:
    """Run every check, then fail if any check failed."""
    logger = get_run_logger()
    states = {
        path.stem: run_check(path, return_state=True)
        for path in sorted(CHECK_DIR.glob("*.sql"))
    }
    failed = [name for name, state in states.items() if state.is_failed()]
    if failed:
        raise BadRowsError(f"Checks failed: {failed}")
    logger.info("All %d checks passed", len(states))


@flow
def pipeline() -> None:
    """Ingest GTFS and OSM, transform, then gate."""
    ingest_via_gtfs()
    ingest_osm()
    transform()
    quality_gate()


if __name__ == "__main__":
    pipeline()
