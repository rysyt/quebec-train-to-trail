"""Spark's GeoParquet output has valid GeoParquet metadata and matches the raw tables."""

import json

import pyarrow.parquet as pq
import pytest

from quebec_train_to_trail.ingest.osm import PARQUET_DIR

GEO_FILES = {"ways": "LineString", "places": "Point"}


@pytest.mark.parametrize(("name", "geometry_type"), GEO_FILES.items())
def test_geoparquet_metadata(name: str, geometry_type: str) -> None:
    """Every file has a WKB geom column of one type, in lon/lat (no CRS means OGC:CRS84)."""
    files = sorted((PARQUET_DIR / name).glob("part-*.parquet"))
    assert files, f"no Parquet files in {PARQUET_DIR / name}"
    for path in files:
        geo = json.loads(pq.read_schema(path).metadata[b"geo"])
        column = geo["columns"][geo["primary_column"]]
        assert geo["primary_column"] == "geom"
        assert column["encoding"] == "WKB"
        assert column["geometry_types"] in ([], [geometry_type])
        assert "crs" not in column


@pytest.mark.parametrize(
    ("name", "table"),
    [("ways", "osm_ways"), ("places", "osm_places"), ("routes", "osm_hiking_routes")],
)
def test_parquet_rows_match_raw(conn, name: str, table: str) -> None:
    """The raw table has as many rows as its Parquet directory."""
    files = (PARQUET_DIR / name).glob("part-*.parquet")
    in_parquet = sum(pq.read_metadata(path).num_rows for path in files)
    in_table = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    assert in_table == in_parquet
