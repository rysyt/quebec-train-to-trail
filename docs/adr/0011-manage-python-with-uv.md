# ADR 0011: Manage Python with uv

Status: Accepted (2026-10-08)

Riley's words, 2026-10-08:
> I'll use UV, it'll make github actions easier at the end of the slice.

## Context
The `ingest` flows (ADR 0005) are the repo's first Python. The Python version and library versions
must be recorded so the flows run the same locally, in GitHub Actions (ADR 0006) and for anyone
who clones the repo.

## Considered options
- uv: installs Python, creates `.venv/`, records dependencies in `pyproject.toml` and pins them in
  `uv.lock` (chosen)
- pip with venv: built into Python; no lock file by default, Python version managed by hand
- conda: suited to data science and non-Python libraries; heavier than this project needs
- Poetry: similar to uv, slower, does not install Python

## Decision
uv manages the Python version, the virtual environment and the dependencies. `pyproject.toml`
and `uv.lock` are committed; `.venv/` is not.

## Consequences
+ One command (`uv sync`) rebuilds the same environment anywhere.
+ GitHub Actions can set it up with uv's official action.
- Contributors need uv installed.
