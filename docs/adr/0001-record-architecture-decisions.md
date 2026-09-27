# ADR 0001: Record architecture decisions

Status: Accepted (2026-09-26)

Riley's words, 2026-09-26:
> On the ADR though I want to have it mark my exact words AND ALSO have this sort of format

> It should create the docs/adr at setup and have the first entry be a boilerplate using ADR (with
> the date marked as today since I've decided every future repo uses it)

## Context
Decisions made in conversation get lost or drift when they are paraphrased or copied.

## Considered options
- ADRs in Michael Nygard's format, plus considered options and Riley's exact words (chosen)
- A single running log with no structure

## Decision
Big design decisions in this repo are recorded in `docs/adr/`, one file each, titled `ADR NNNN:
Title`. Other decisions go in `DECISIONS.md` as D1, D2, ..., in the same format. Every entry quotes
Riley's exact words. An accepted entry is not rewritten; a changed decision is a new entry, and the
old one's Status says what replaced it.

## Consequences
+ Every decision can be traced to Riley's own words.
- Each decision takes a few minutes to record.
