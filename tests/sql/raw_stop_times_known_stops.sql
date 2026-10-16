-- Every stop_id in raw.stop_times exists in raw.stops.
SELECT DISTINCT st.stop_id
FROM raw.stop_times AS st
LEFT JOIN raw.stops AS s ON s.stop_id = st.stop_id
WHERE s.stop_id IS NULL;
