# ADR 0016: Spark writes GeoParquet before the load

Status: Accepted (2026-11-05)

Riley's words, 2026-11-06:
> GeoParquet makes the most sense here. We'll have Spark write its output to GeoParquet and load PostGIS from those files

## Context
In slice 5 the Spark job (ADR 0014) streamed its rows straight into PostGIS with `COPY`, so
nothing was kept between the 10-minute Spark step and the load. A failed load meant running Spark
again, and the filtered data existed only in the database.

Parquet is the usual file format for Spark output: column by column, compressed, typed.
GeoParquet is Parquet plus a `geo` metadata block naming the geometry column, its encoding and
its coordinate system, so tools such as QGIS, GDAL and DuckDB read the geometry directly. Sedona
writes it with `format("geoparquet")`.

ADR 0014 listed "PySpark with Sedona, writing Parquet or Apache Iceberg tables that PostGIS then
loads" as an option and did not choose it.

## Considered options
- Spark writes GeoParquet to `raw/osm/`; a separate task loads it with pyarrow (chosen)
- Spark reads the Parquet back for the load: no new dependency, but every load starts Java
- Load the GeoParquet with `ogr2ogr`: needs GDAL built with Parquet support
- Delta Lake: Parquet plus a transaction log, for updates and history; one job rewrites
  everything here, so its features would go unused
- Keep streaming into PostGIS with no files in between

## Decision
`ingest_osm` has two tasks for the OpenStreetMap data:
1. `spark_to_parquet` writes `raw/osm/ways` and `raw/osm/places` as GeoParquet (WKB, CRS left
   out, which GeoParquet reads as OGC:CRS84, the same lon/lat as EPSG:4326) and
   `raw/osm/routes` as Parquet. It writes `raw/osm/manifest.json` last, with the extract's MD5
   and hashes of the corridor and of the filter code. When the manifest matches, Spark is
   skipped.
2. `parquet_to_raw` reads the files with pyarrow and copies them into the same three `raw`
   tables as before.

`raw/osm/` stays local and is not committed (ADR 0007).

## Consequences
+ A failed or repeated load takes seconds instead of rerunning Spark.
+ The filtered OpenStreetMap data is kept as standard files that QGIS opens without PostGIS.
+ The `raw` tables and everything downstream are unchanged.
- New dependency: pyarrow.
- Any edit to `filter_osm`, even reformatting, reruns Spark once.
- Touches ADR 0014 (takes up its Parquet option, without Iceberg) and ADR 0007 (`raw/osm/` is
  local data).
