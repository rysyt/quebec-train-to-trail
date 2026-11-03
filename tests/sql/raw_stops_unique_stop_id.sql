-- Each stop_id appears once in raw.stops.
SELECT
    stop_id,
    COUNT(*) AS copies
FROM raw.stops
GROUP BY stop_id
HAVING COUNT(*) > 1;
