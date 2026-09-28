# ADR 0003: SQL data tests on push

Status: Superseded by ADR 0006

Riley's words, 2026-09-27:
> I also want there to be tests when pushing to git that use SQL to make sure that data is clean,
> nothing is lost, etc.

## Context
Each step moves data between schemas (`raw`, `staging`, `analytics`). Rows can be dropped or
geometry broken without any error.

## Considered options
- SQL checks that run automatically on push (chosen)
- Checks run by hand only

## Decision
Data-quality tests are written in SQL (for example: row counts match between steps, geometry is
valid, no duplicates) and run automatically when pushing to GitHub.

## Consequences
+ Lost or broken data is caught before it reaches `main`.
- The push check needs a PostGIS database and the data to test against.
- Open: the tool that runs them (GitHub Actions is the likely one).
