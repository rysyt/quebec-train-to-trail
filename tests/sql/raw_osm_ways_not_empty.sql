-- The OpenStreetMap step loaded ways. Returns one row if raw.osm_ways is empty.
SELECT 'raw.osm_ways is empty' AS problem
WHERE NOT EXISTS (SELECT 1 FROM raw.osm_ways);
