# ADR 0014: Spark for the OpenStreetMap trails step

Status: Accepted (2026-11-03)

Riley's words, 2026-11-03:
> We'll use spark to process the openstreetmap trails extract. It's the largest input in the pipeline so using spark for batch processing at that scale makes the most sense.

## Context
Apache Spark splits batch processing of large data across many workers, written in Python
(PySpark) or SQL. Apache Kafka carries live event streams. Apache Sedona adds geometry types and
spatial functions to Spark, much as PostGIS does for PostgreSQL.

The VIA Rail GTFS feed is a few small files; PostGIS handles it easily, and Spark there would be
overkill. The OpenStreetMap trails step (slice 5, `feat/ingest-osm-trails`) is the largest input:
a regional extract (for example from Geofabrik) has to be parsed and filtered down to trail
segments before loading. Its size has not been measured yet.

VIA publishes no GTFS-realtime feed (no live positions or delays), so there is no event stream in
this project for Kafka to carry.

## Considered options
- PySpark with Sedona for the OpenStreetMap step only: read the extract, keep trail ways, write
  them to `raw` (proposed)
- PySpark with Sedona, writing Parquet or Apache Iceberg tables that PostGIS then loads
- No Spark: load OpenStreetMap with the usual tools (`osm2pgsql`, `ogr2ogr` or Python)
- Kafka: not a fit; nothing in the sources streams. A live feed would need another source, such
  as a transit agency's GTFS-realtime, in a separate project

## Decision
None yet. Spark is proposed for the OpenStreetMap step only; every other step stays as in ADRs
0002, 0005 and 0010. Kafka is not proposed.

## Consequences
If accepted:
+ The largest input is processed with a tool built for large batch data.
+ Spatial SQL in Sedona is close to PostGIS, so the hand-written SQL style carries over.
- Spark adds Java and a heavy dependency to the `ingest` part; the pipeline image (D19) grows.
- At this data size a single machine may be faster than Spark; measure the extract first.
- Touches ADR 0005 (Prefect would run the Spark job as a task) and ADR 0010 (`ingest` gains a
  Spark job; storage stays in `raw`).
