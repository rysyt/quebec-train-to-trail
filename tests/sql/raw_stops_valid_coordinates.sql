-- Every stop has a latitude and longitude that are numbers in range.
-- CASE, not OR: SQL may evaluate OR in any order, so the casts only run once both are valid.
-- A missing value makes pg_input_is_valid return null, which falls to ELSE and is reported.
SELECT stop_id, stop_lat, stop_lon
FROM raw.stops
WHERE CASE
    WHEN pg_input_is_valid(stop_lat, 'numeric') AND pg_input_is_valid(stop_lon, 'numeric')
        THEN stop_lat::numeric NOT BETWEEN -90 AND 90
            OR stop_lon::numeric NOT BETWEEN -180 AND 180
    ELSE true
END;
