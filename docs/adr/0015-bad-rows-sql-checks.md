# ADR 0015: Data checks are bad-rows SQL files

Status: Accepted (2026-11-03)

Riley's words, 2026-11-03:
> What would be the professional / production answer for question 1? If it's A let's go with that.

## Context
Slice 3 added SQL checks for the `raw` schema (`tests/sql/`, run on every push by
`tests/test_sql_checks.py`). Slices 4 onward add `staging` and `analytics` tables that need
checks too. A check can be a database constraint, a SQL query, or a Python test.

## Considered options
- Bad-rows SQL checks: each file selects the rows that break one rule; no rows means pass (chosen)
- Database constraints only: cannot express rules such as "every stop is within 100 m of its line"
- Python tests that query the database: a second language for rules SQL already expresses

## Decision
Each data rule that a constraint cannot express is a file in `tests/sql/` that returns the rows
breaking it. It is named for the table and the rule (`staging_rail_line_stops_within_100m.sql`)
and run by the existing test runner. Constraints on tables are decided separately.

## Consequences
+ One convention for every schema, run in CI already.
+ A failure shows the offending rows.
- A rule needs a query that can find its own violations; some statistical rules are awkward.
