-- Every stop_id in raw.stop_times exists in raw.stops.
SELECT DISTINCT st.stop_id
FROM raw.stop_times AS st
LEFT JOIN raw.stops AS s ON st.stop_id = s.stop_id
WHERE s.stop_id IS NULL;
