# ADR 0006: Data tests in GitHub Actions

Status: Accepted (2026-09-27). Replaces ADR 0003.

Riley's words, 2026-09-27:
> Also I think I mentioned that data quality SQL code and Python code will be implemented (or I
> want it to be implemented) in GitHub Actions, so that tests run on push.

## Context
ADR 0003 covered SQL tests on push and left the tool open. This adds Python tests and names the
tool.

## Considered options
- GitHub Actions running SQL and Python tests on push (chosen)
- SQL tests only (ADR 0003)

## Decision
On every push, GitHub Actions runs the data-quality tests: SQL checks (row counts between steps,
valid geometry, no duplicates) and Python tests (pytest).

## Consequences
+ Lost or broken data and broken code are caught before they reach `main`.
- The workflow needs a PostGIS database and test data small enough to publish (ADR 0007).
