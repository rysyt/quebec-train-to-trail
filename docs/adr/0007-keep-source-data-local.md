# ADR 0007: Keep source data local

Status: Accepted (2026-09-27). Replaces ADR 0004.

Riley's words, 2026-09-27:
> On public source data I'll keep that local and just publish whatever data is small enough, I
> don't really care enough about this specifically, I just want to give viewers as much context
> as possible.

## Context
ADR 0004 allowed committing all public source data. GitHub rejects files over 100 MB, and a
Québec OpenStreetMap extract is likely larger.

## Considered options
- Keep downloads local; commit only small files (chosen)
- Commit all public source data (ADR 0004)

## Decision
Downloads stay local (`raw/` stays in `.gitignore`). Small public files, such as test data, may
be committed.

## Consequences
+ The repo stays small, and the current `.gitignore` already fits.
- Anyone cloning the repo runs the download step first.
