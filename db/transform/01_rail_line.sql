-- staging.rail_line: one line per VIA train line, built from the GTFS shapes.
-- shape 535 = Montréal to Jonquière, shape 536 = Montréal to Senneterre.
-- raw stores every column as text, so the casts happen here.
-- Reprojected to EPSG:32198 (Québec Lambert, metres) as ADR 0010 says.

DROP TABLE IF EXISTS staging.rail_line CASCADE;

CREATE TABLE staging.rail_line (
    line_id TEXT PRIMARY KEY,
    line_name TEXT NOT NULL,
    source_shape_id TEXT NOT NULL,
    geom GEOMETRY (LINESTRING, 32198) NOT NULL,
    CONSTRAINT rail_line_geom_valid CHECK (ST_ISVALID(geom))
);

INSERT INTO staging.rail_line (line_id, line_name, source_shape_id, geom)
SELECT
    chosen_lines.line_id,
    chosen_lines.line_name,
    chosen_lines.shape_id,
    ST_TRANSFORM(
        ST_MAKELINE(
            ST_SETSRID(
                ST_MAKEPOINT(
                    shapes.shape_pt_lon::DOUBLE PRECISION,
                    shapes.shape_pt_lat::DOUBLE PRECISION
                ),
                4326
            )
            ORDER BY shapes.shape_pt_sequence::INTEGER
        ),
        32198
    ) AS geom
FROM raw.shapes AS shapes
INNER JOIN (
    VALUES
    ('J', 'Jonquière', '535'),
    ('S', 'Senneterre', '536')
) AS chosen_lines (line_id, line_name, shape_id)
    ON shapes.shape_id = chosen_lines.shape_id
GROUP BY
    chosen_lines.line_id,
    chosen_lines.line_name,
    chosen_lines.shape_id;

-- GiST index for distance queries.
CREATE INDEX rail_line_geom_gist ON staging.rail_line USING gist (geom);
