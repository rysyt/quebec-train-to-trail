# ADR 0002: Hand-written SQL in small files

Status: Accepted (2026-09-27)

Riley's words, 2026-09-27:
> On the SQL, I'd want to write by hand for more control. I'd do that in small files I run myself
> (or run automatically after making changes to something).

## Context
The `transform` and `analytics` steps are SQL. Writing the queries by hand gives full control
over each step.

## Considered options
- Plain `.sql` files written by hand, run in order (chosen)
- dbt (a tool that runs SQL models and tests them)

## Decision
Transformations and analysis are hand-written SQL, one small file per step, run by Riley.

## Consequences
+ Every query is Riley's own, and each file is small enough to read and test on its own.
- The run order must be recorded: numbered file names, or files named by their output plus a
  YAML or text file listing the order.
- Open: whether files also run automatically after a change, and what triggers that.
