-- Each stop_id appears once in raw.stops.
SELECT stop_id, count(*) AS copies
FROM raw.stops
GROUP BY stop_id
HAVING count(*) > 1;
