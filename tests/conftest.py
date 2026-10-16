"""Shared fixtures. The tests read the database that the GTFS flow loaded.

Run from the repo root after the flow:
    uv run --env-file .env pytest
"""

import pytest

from quebec_train_to_trail.ingest.via_gtfs import connect


@pytest.fixture(scope="session")
def conn():
    """One connection for the whole test run."""
    with connect() as conn:
        yield conn
