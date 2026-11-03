-- Every stop on a train line is within 100 m of that line (slice 4 done).
-- Returns each stop farther than 100 m, with its distance in metres.
WITH line_stops AS (
    SELECT
        stop_line.line_id,
        stops.stop_id,
        stops.stop_name,
        ST_DISTANCE(rail_line.geom, stops.geom) AS metres
    FROM staging.stop_line AS stop_line
    INNER JOIN staging.stop AS stops ON stop_line.stop_id = stops.stop_id
    INNER JOIN staging.rail_line AS rail_line
        ON stop_line.line_id = rail_line.line_id
)

SELECT
    line_id,
    stop_id,
    stop_name,
    ROUND(metres::NUMERIC, 1) AS metres
FROM line_stops
WHERE metres > 100
ORDER BY line_id ASC, metres DESC;
