# ADR 0010: Pipeline parts

Status: Accepted (2026-09-28)

Riley's words, 2026-09-28:
> They're accepted as long as storage is part of either db or ingest (or equally after transform)

## Context
Branches are named `type/<part>-<slice>`, so the project needs named parts. Each step of the
pipeline extracts, transforms or saves data, and every step's output must be stored.

## Considered options
- Six parts, split by pipeline step (chosen)
- One part per data source (VIA, OpenStreetMap, elevation)

## Decision
| Part | What it does | Where it stores |
|---|---|---|
| `db` | PostGIS in Docker Compose; schemas `raw`, `staging`, `analytics`; credentials from `.env` | the database itself |
| `ingest` | Python flows (ADR 0005) that download each source and load it | `raw/` on disk, schema `raw`; elevation as a GeoTIFF, reprojected only |
| `transform` | Hand-written SQL (ADR 0002): reproject to EPSG:32198, fix invalid geometry, remove duplicates | schema `staging` |
| `analytics` | SQL answering the hike questions | schema `analytics` |
| `map` | QGIS project as a plain `.qgs` (ADR 0009) reading `analytics` | the `.qgs` in this repo |
| `ci` | GitHub Actions tests on push (ADR 0006) | none |

## Consequences
+ Every step saves its output, so any step can be rerun or checked on its own.
+ Each part has at least one branch in v1.
- A part can span several pull requests (for example `ingest` has three in v1).
