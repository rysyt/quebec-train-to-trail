# ADR 0004: Commit public source data

Status: Superseded by ADR 0007

Riley's words, 2026-09-27:
> and I don't mind storing this data on git (like the data that's public anyway with trails and
> whatnot)

## Context
The tests in ADR 0003 need data to run against. The sources (VIA Rail GTFS, OpenStreetMap) are
public. `.gitignore` currently excludes `raw/` and `*.tif`.

## Considered options
- Commit the public data (chosen)
- Download it fresh on every run

## Decision
Public source data may be committed to the repo.

## Consequences
+ Tests and reruns work offline and on exactly the same data.
- GitHub rejects files over 100 MB, so a full Québec extract likely needs trimming to the study
  area first; `.gitignore` changes once the files are known.
