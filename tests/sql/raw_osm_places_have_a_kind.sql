-- Every OpenStreetMap place is a point tagged as lodging, food, a shelter or
-- toilets, or a settlement.
SELECT
    osm_type,
    osm_id
FROM raw.osm_places
WHERE
    geom IS NULL
    OR COALESCE(
        tags ->> 'tourism', tags ->> 'amenity', tags ->> 'place'
    ) IS NULL;
