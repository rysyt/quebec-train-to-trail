# ADR 0005: Orchestrate with Prefect

Status: Accepted (2026-09-27)

Riley's words, 2026-09-27:
> Okay, keep the decision as Prefect and put it into the proposal, though also make sure in the
> decision file that the other ones are listed in case I want to attempt using them.

## Context
The pipeline has steps that must run in order: download, load, then the SQL files from ADR 0002.
An orchestrator runs those steps, logs them and retries failures. A 2026-09-26 chat compared the
options below.

## Considered options
- Prefect: plain Python functions marked as tasks and flows (chosen for now)
- Dagster: organized around the datasets the pipeline produces and shows how each was made;
  still open to try later
- Airflow: the industry standard, built around DAGs (task graphs); heavy for one person; still
  open to try later
- dbt: runs SQL files as models and tests them; overlaps with ADR 0002; still open to try later

## Decision
Prefect runs the pipeline for now; Riley is leaning towards it and may switch to one of the
others. Each step is a Python function; the SQL steps run Riley's hand-written files in order.

## Consequences
+ Settles the run order left open in ADR 0002.
+ Swapping to Dagster later means rewrapping the same functions, not rewriting them.
- One more tool to install.
- Locally, steps are run by hand; tests run on push (ADR 0006).
