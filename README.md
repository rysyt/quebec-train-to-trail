# quebec-train-to-trail

A tool for planning hikes in northern Québec that start from a VIA Rail stop. It will answer questions like "which trails of at least 10 km start within 3 km of a stop?" (arbitrary numbers for now) and illustrate the answers with a map.

**Status:** in progress. The database runs (slice 1 of 9); no data is loaded yet.

## Run it locally

Needs [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```
cp .env.example .env     # then set your own password in .env
docker compose up -d     # start PostGIS in the background
docker compose exec db psql -U train_to_trail -d train_to_trail
```

In `psql`, `SELECT postgis_full_version();` should answer. The database listens on `localhost:5432` only. Stop it with `docker compose down`; your data is kept.

## How it will work

Pipelines will collect train stops, trails and elevation from public sources, load them into PostGIS, clean them up with SQL, and map the results in QGIS.

```
VIA GTFS, OSM, MRDEM -> Prefect -> raw -> SQL -> staging -> analytics -> QGIS
```

## Sources

- **VIA Rail GTFS**: schedules, stops and track shapes for the Montréal to Senneterre and Montréal to Jonquière lines (Open Government Licence - Canada).
- **OpenStreetMap**: trails, and later places to stay.
- **Natural Resources Canada MRDEM-30**: a 30 m elevation model.

## Steps

1. **Collect.** Python flows run by Prefect download each source into `raw/`. Downloads stay on the local machine and are not committed. A rerun reuses them instead of downloading again.
2. **Load.** PostGIS runs in Docker Compose, with three schemas: `raw`, `staging` and `analytics`.
3. **Clean.** Hand-written SQL files run in order. They reproject everything to EPSG:32198 (NAD83 / Québec Lambert), fix invalid geometry, remove duplicates, and join trail segments into whole trails with their lengths. Elevation is only cut to a corridor around the two lines and reprojected.
4. **Ask.** SQL queries in `analytics` answer the hike questions, for example trails of at least a given length within a given distance of a stop. Later: remote, hilly trails with somewhere to sleep and eat nearby.
5. **Map.** A QGIS project, saved as a plain `.qgs` file in the repo, shows the results on a 2D map and the train's route in 3D on the elevation data.

## Checks

On every push, GitHub Actions runs pytest and SQL data checks: row counts between steps, valid geometry, and no duplicates.

## Roadmap

- **v1**: everything above, end to end, with a 3D render of the route.
- **v2**: places to stay with prices, then terrain analysis (ruggedness).

## Design decisions

See [docs/adr/](docs/adr/).
