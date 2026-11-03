# ADR 0013: Number and name SQL files

Status: Accepted (2026-10-23)

Riley's words, 2026-10-23:
> b let's do mix like 01_{result}, prefect running in order

## Context
ADR 0002 left the run order of the hand-written SQL files open: numbered file names, or files
named by their output plus a file listing the order. ADR 0005 has Prefect run the SQL files in
order. Slice 4 writes the first ones.

## Considered options
- Both: a number, then the table the file builds, for example `01_stops.sql` (chosen)
- Numbers only, for example `01.sql`
- Output names only, plus a YAML or text file listing the order

## Decision
Each SQL file is named `NN_<result>.sql`: a two-digit number giving its place in the run order,
then the name of what it builds. The Prefect flow runs the files in number order.

## Consequences
+ A directory listing shows both the order and what each file makes.
+ No separate order file to keep in step.
- Putting a file between two others means renumbering, or leaving gaps in the numbers.
