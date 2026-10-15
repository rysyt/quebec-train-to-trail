# ADR 0012: Pre-commit hooks

Status: Accepted (2026-10-15)

Riley's words, 2026-10-15:
> yes let's do this except mypy for now

## Context
Formatting and simple mistakes should be caught before they are committed, not in review. The
repo now has Python (ADR 0011), YAML (`compose.yaml`) and TOML (`pyproject.toml`); hand-written
SQL starts in slice 4.

## Considered options
- The pre-commit framework, installed as a dev dependency with uv, running general file checks,
  Ruff and uv's lock check (chosen)
- The same, with pre-commit installed once per machine (`uv tool install`), not recorded in the
  repo
- SQLFluff (SQL linter) now; deferred to slice 4, when there is SQL to check
- mypy (type checking); not for now

## Decision
`.pre-commit-config.yaml` runs, on every commit:
- from pre-commit-hooks: `end-of-file-fixer`, `trailing-whitespace`, `check-yaml`, `check-toml`,
  `check-added-large-files`, `detect-private-key`, `check-merge-conflict`; the large-file limit
  is 500 KB for most files and 5 MB for images, such as the map renders shown in the README
- from Ruff: `ruff-check` (lint, with simple fixes) and `ruff-format`
- from uv: `uv-lock`, which keeps `uv.lock` in step with `pyproject.toml`

pre-commit is a dev dependency, so `uv sync` installs it; each clone runs
`uv run pre-commit install` once.

## Consequences
+ Every commit is checked the same way, on every machine.
+ Hook versions are pinned in the config file.
- A hook that fixes a file stops the commit; the fix must be staged and committed again.
- `check-added-large-files` will need a higher limit for the test data committed in slice 3.
