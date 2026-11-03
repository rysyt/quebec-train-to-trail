-- staging.stop: each stop on the two train lines, once.
-- staging.stop_line: which line or lines each stop is on (Montréal to Hervey
-- is on both). A line's stops are the stops of the trips that use its shape.

DROP TABLE IF EXISTS staging.stop_line;
DROP TABLE IF EXISTS staging.stop;

CREATE TABLE staging.stop (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT NOT NULL,
    geom GEOMETRY (POINT, 32198) NOT NULL,
    CONSTRAINT stop_geom_valid CHECK (ST_ISVALID(geom))
);

CREATE TABLE staging.stop_line (
    stop_id TEXT NOT NULL REFERENCES staging.stop (stop_id),
    line_id TEXT NOT NULL REFERENCES staging.rail_line (line_id),
    PRIMARY KEY (stop_id, line_id)
);

CREATE TEMPORARY TABLE line_stops ON COMMIT DROP AS
SELECT DISTINCT
    stop_times.stop_id,
    rail_line.line_id
FROM staging.rail_line AS rail_line
INNER JOIN raw.trips AS trips ON rail_line.source_shape_id = trips.shape_id
INNER JOIN raw.stop_times AS stop_times
    ON trips.trip_id = stop_times.trip_id;

-- Stops first, so the pairs below can point at them.
INSERT INTO staging.stop (stop_id, stop_name, geom)
SELECT
    stops.stop_id,
    stops.stop_name,
    ST_TRANSFORM(
        ST_SETSRID(
            ST_MAKEPOINT(
                stops.stop_lon::DOUBLE PRECISION,
                stops.stop_lat::DOUBLE PRECISION
            ),
            4326
        ),
        32198
    ) AS geom
FROM raw.stops AS stops
WHERE stops.stop_id IN (SELECT line_stops.stop_id FROM line_stops);

INSERT INTO staging.stop_line (stop_id, line_id)
SELECT
    line_stops.stop_id,
    line_stops.line_id
FROM line_stops;

-- GiST index for distance queries.
CREATE INDEX stop_geom_gist ON staging.stop USING gist (geom);
