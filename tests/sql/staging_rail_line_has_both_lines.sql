-- Both train lines were built. Returns any expected line that is missing
-- (for example if VIA renumbers its shapes).
SELECT expected.line_id
FROM (VALUES ('J'), ('S')) AS expected (line_id)
LEFT JOIN staging.rail_line AS rail_line ON expected.line_id = rail_line.line_id
WHERE rail_line.line_id IS NULL;
