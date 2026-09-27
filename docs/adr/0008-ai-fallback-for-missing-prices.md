# ADR 0008: AI fallback for missing prices, flagged in a column

Status: Accepted (2026-09-27)

Riley's words, 2026-09-27:
> I'd also try my best to get price data from that. Also, I had the idea that, in the pipeline, if no
> sources for something come up price-wise, having Claude try to find it by searching and whether
> it finds or not, marking in another column in the table that Claude was used (so it's not
> reliable but still shows me using AI to fill in gaps and marking to not trust it completely).

## Context
Places to stay (v2, D5) need prices. Many remote places have no price in any open source.

## Considered options
- Sources first; if none, Claude searches the web; a column marks every row Claude touched,
  found or not (chosen)
- Leave missing prices empty

## Decision
The price step tries real sources first. For a place with no price, Claude searches for one. The
row records that Claude was used, whether or not it found a price, so those values are treated
as unverified.

## Consequences
+ Gaps get filled, and every AI-filled value can be told apart from sourced ones.
- Needs a Claude API key in `.env` and costs a little per lookup.
- Queries that need trusted prices must filter on that column.
