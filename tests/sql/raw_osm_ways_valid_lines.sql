-- Every OpenStreetMap way is a valid line with at least two points and a
-- highway tag.
SELECT
    way_id,
    tags ->> 'highway' AS highway,
    ST_NPOINTS(geom) AS points
FROM raw.osm_ways
WHERE
    geom IS NULL
    OR NOT ST_ISVALID(geom)
    OR ST_NPOINTS(geom) < 2
    OR tags ->> 'highway' IS NULL;
