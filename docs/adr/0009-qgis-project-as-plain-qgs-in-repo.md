# ADR 0009: QGIS project as a plain .qgs file in this repo

Status: Accepted (2026-09-27)

Riley's words, 2026-09-27:
> I wanted it explicit that we use the non-compressed format for the QGIS project and that it be
> placed in this repo so that files are openable AND it can be pushed to git.

## Context
QGIS saves projects as `.qgz` (a zip) or `.qgs` (plain XML text). The `map` part reads the
`analytics` schema.

## Considered options
- `.qgs` committed in this repo (chosen)
- `.qgz` committed in this repo
- Project kept outside the repo

## Decision
The QGIS project is a `.qgs` file in this repo, committed with the code.

## Consequences
+ The file opens in any text editor, and git shows line-by-line changes to it.
- Layer paths must be saved as relative (QGIS: Project Properties, General, Save paths) so the
  project opens on another machine.
- The database password must not be saved in the project; connect through a PostgreSQL service
  file or QGIS's password store instead. Check the `.qgs` before each commit.
