-- Every stop on a train line is within 100 m of that line (slice 4 done).
-- A line's stops are the stops of the trips that use its shape.
-- Returns each stop farther than 100 m, with its distance in metres.
WITH line_stops AS (
    SELECT DISTINCT
        rail_line.line_id,
        stops.stop_id,
        stops.stop_name,
        ST_DISTANCE(
            rail_line.geom,
            ST_TRANSFORM(
                ST_SETSRID(
                    ST_MAKEPOINT(
                        stops.stop_lon::DOUBLE PRECISION,
                        stops.stop_lat::DOUBLE PRECISION
                    ),
                    4326
                ),
                32198
            )
        ) AS metres
    FROM staging.rail_line AS rail_line
    INNER JOIN raw.trips AS trips ON rail_line.source_shape_id = trips.shape_id
    INNER JOIN raw.stop_times AS stop_times
        ON trips.trip_id = stop_times.trip_id
    INNER JOIN raw.stops AS stops ON stop_times.stop_id = stops.stop_id
)

SELECT
    line_id,
    stop_id,
    stop_name,
    ROUND(metres::NUMERIC, 1) AS metres
FROM line_stops
WHERE metres > 100
ORDER BY line_id ASC, metres DESC;
