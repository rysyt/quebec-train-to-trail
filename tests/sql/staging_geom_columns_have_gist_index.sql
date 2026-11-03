-- Every geometry column in staging has a GiST index. Returns each column
-- without one.
SELECT
    geom_columns.table_name,
    geom_columns.column_name
FROM information_schema.columns AS geom_columns
WHERE
    geom_columns.table_schema = 'staging'
    AND geom_columns.udt_name = 'geometry'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_indexes AS gist_indexes
        WHERE
            gist_indexes.schemaname = 'staging'
            AND gist_indexes.tablename = geom_columns.table_name
            AND gist_indexes.indexdef ILIKE '%USING gist ('
            || geom_columns.column_name
            || ')%'
    );
