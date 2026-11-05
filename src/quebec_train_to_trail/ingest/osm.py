"""Load OpenStreetMap ways and places near the two train lines into the raw schema.

Spec:
- Download Geofabrik's Québec extract to raw/quebec-latest.osm.pbf (gitignored). Skip the
  download if the file is already there (ADR 0007). Check it against Geofabrik's MD5 sum.
- Build a 20 km corridor around the two lines from raw.shapes (shapes 535 and 536, as in
  db/transform/01_rail_line.sql), cut into small pieces so Spark can index them.
- Filter with Spark and Sedona, filter first and join second:
  1. keep the points inside the corridor,
  2. keep the ways (any highway, or a place to sleep or eat) that touch one of those points,
  3. join only those ways to their points and build their lines.
- Load raw.osm_ways (highway lines), raw.osm_places (lodging, food, towns as points) and
  raw.osm_hiking_routes (hiking route relations and their member ways). Tags stay as JSON;
  what counts as a trail is decided in transform.
- Log the number of footpaths in the corridor.

Run from the repo root (Java 17 needed for Spark):
    uv run --env-file .env python -m quebec_train_to_trail.ingest.osm
"""

import hashlib
import os
from pathlib import Path

import httpx
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NONE
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from quebec_train_to_trail.ingest.via_gtfs import connect

PBF_URL = "https://download.geofabrik.de/north-america/canada/quebec-latest.osm.pbf"
RAW_DIR = Path("raw")
PBF_PATH = RAW_DIR / "quebec-latest.osm.pbf"
CORRIDOR_M = 20_000

# Sedona's Spark jars, matched to PySpark 4.1 and apache-sedona 1.9.1 in pyproject.toml.
MAVEN = "https://repo1.maven.org/maven2"
SEDONA_JARS = [
    "org/apache/sedona/sedona-spark-shaded-4.1_2.13/1.9.1/sedona-spark-shaded-4.1_2.13-1.9.1.jar",
    "org/datasyslab/geotools-wrapper/1.9.1-33.5/geotools-wrapper-1.9.1-33.5.jar",
]
JARS_DIR = Path(
    os.environ.get(
        "SEDONA_JARS_DIR", Path.home() / ".cache" / "quebec-train-to-trail" / "jars"
    )
)

SLEEP = ["hotel", "motel", "guest_house", "hostel", "chalet", "camp_site",
         "caravan_site", "wilderness_hut", "alpine_hut", "apartment"]  # fmt: skip
AMENITY = ["restaurant", "cafe", "fast_food", "pub", "bar", "shelter", "toilets",
           "drinking_water"]  # fmt: skip
SETTLEMENT = ["city", "town", "village", "hamlet"]
FOOTPATHS = ["path", "footway", "track", "bridleway", "steps"]


def in_list(key: str, values: list[str]) -> str:
    """SQL test that tag `key` has one of `values`."""
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"tags['{key}'] IN ({quoted})"


PLACE_TEST = " OR ".join(
    [
        in_list("tourism", SLEEP),
        in_list("amenity", AMENITY),
        in_list("place", SETTLEMENT),
    ]
)
WAY_TEST = f"map_contains_key(tags, 'highway') OR {PLACE_TEST}"

# The 20 km corridor around both lines, simplified to 100 m and cut into pieces of at most
# 64 vertices, in lon/lat to match OSM.
CORRIDOR_SQL = """
WITH lines AS (
    SELECT ST_MakeLine(
        ST_SetSRID(ST_MakePoint(shape_pt_lon::float8, shape_pt_lat::float8), 4326)
        ORDER BY shape_pt_sequence::integer
    ) AS geom
    FROM raw.shapes
    WHERE shape_id IN ('535', '536')
    GROUP BY shape_id
),
corridor AS (
    SELECT ST_Simplify(ST_Buffer(ST_Transform(ST_Union(geom), 32198), %(metres)s), 100)
        AS geom
    FROM lines
)
SELECT ST_AsText(ST_Transform(ST_Subdivide(geom, 64), 4326)) FROM corridor
"""


@task(retries=2, retry_delay_seconds=30)
def download_pbf(url: str, dest: Path) -> Path:
    """Download the extract to dest unless it's already there, then check its MD5 sum."""
    logger = get_run_logger()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file():
        logger.info("%s already downloaded, skipping", dest)
        return dest

    # Download to a temporary name, so a cut-off download never looks finished.
    partial = dest.with_suffix(".partial")
    md5 = hashlib.md5()
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as response:
        response.raise_for_status()
        logger.info(
            "Downloading %s (%s bytes)",
            response.url,
            response.headers.get("content-length"),
        )
        with partial.open("wb") as f:
            for chunk in response.iter_bytes(1 << 20):
                f.write(chunk)
                md5.update(chunk)
        # Geofabrik publishes the sum next to the dated file it redirects to.
        expected = httpx.get(f"{response.url}.md5", follow_redirects=True).text.split()[
            0
        ]

    if md5.hexdigest() != expected:
        partial.unlink()
        raise ValueError(f"MD5 mismatch: got {md5.hexdigest()}, expected {expected}")
    partial.rename(dest)
    logger.info("Saved %s, MD5 checked", dest)
    return dest


@task
def corridor_pieces(metres: int) -> list[str]:
    """Return the corridor around both lines as small WKT polygons."""
    with connect() as conn:
        rows = conn.execute(CORRIDOR_SQL, {"metres": metres}).fetchall()
    if not rows:
        raise ValueError("No corridor: raw.shapes has no shapes 535 and 536")
    return [r[0] for r in rows]


def sedona_jars() -> list[Path]:
    """Download Sedona's jars once into JARS_DIR, checking Maven's SHA-1. Return their paths."""
    JARS_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for jar in SEDONA_JARS:
        path = JARS_DIR / Path(jar).name
        if not path.is_file():
            data = httpx.get(
                f"{MAVEN}/{jar}", follow_redirects=True, timeout=120
            ).content
            expected = httpx.get(
                f"{MAVEN}/{jar}.sha1", follow_redirects=True
            ).text.split()[0]
            if hashlib.sha1(data).hexdigest() != expected:
                raise ValueError(f"SHA-1 mismatch for {jar}")
            path.write_bytes(data)
        paths.append(path)
    return paths


def spark_session():
    """A local Spark session with Sedona's spatial functions."""
    from sedona.spark import SedonaContext

    # The jars go on the driver's own classpath at start-up. Loaded later (spark.jars.packages),
    # they sit in a separate class loader from the JTS copy bundled with Spark 4.1, and Java
    # blocks Sedona's index code from reaching into it (IllegalAccessError).
    jars = [str(p) for p in sedona_jars()]
    builder = (
        SedonaContext.builder()
        .master("local[*]")
        .appName("osm-corridor")
        .config("spark.jars", ",".join(jars))
        .config("spark.driver.extraClassPath", os.pathsep.join(jars))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return SedonaContext.create(builder.getOrCreate())


def filter_osm(spark, pbf: Path, pieces: list[str]) -> dict[str, DataFrame]:
    """Filter first, join second. Return the ways, places and routes as DataFrames."""
    osm = spark.read.format("osmpbf").load(str(pbf))
    nodes = osm.filter("kind = 'node'").select(
        "id",
        "tags",
        F.col("location.longitude").alias("lon"),
        F.col("location.latitude").alias("lat"),
    )
    ways = osm.filter("kind = 'way'").select("id", "tags", "refs")
    relations = osm.filter("kind = 'relation'").select(
        "id", "tags", "refs", "ref_types"
    )

    # 1. Points in the corridor: a cheap box test, then the exact test against the pieces.
    corridor = spark.createDataFrame([(p,) for p in pieces], ["wkt"]).select(
        F.expr("ST_GeomFromWKT(wkt)").alias("piece")
    )
    box = corridor.agg(F.expr("ST_Envelope_Aggr(piece)").alias("box")).first()["box"]
    xmin, ymin, xmax, ymax = box.bounds
    in_corridor = (
        nodes.filter(
            F.col("lon").between(xmin, xmax) & F.col("lat").between(ymin, ymax)
        )
        .withColumn("geom", F.expr("ST_Point(lon, lat)"))
        .join(F.broadcast(corridor), F.expr("ST_Intersects(piece, geom)"), "left_semi")
        .persist()
    )

    # 2. Ways that touch one of those points.
    candidates = ways.filter(WAY_TEST)
    touching = (
        candidates.select("id", F.explode("refs").alias("node_id"))
        .join(in_corridor.select(F.col("id").alias("node_id")), "node_id", "left_semi")
        .select("id")
        .distinct()
    )
    kept = candidates.join(touching, "id", "left_semi")

    # 3. Only those ways' points, joined back in order.
    coords = (
        kept.select(
            F.col("id").alias("way_id"), F.posexplode("refs").alias("pos", "node_id")
        )
        .join(nodes.select(F.col("id").alias("node_id"), "lon", "lat"), "node_id")
        .groupBy("way_id")
        .agg(
            F.array_sort(F.collect_list(F.struct("pos", "lon", "lat"))).alias("pts"),
            F.avg("lon").alias("avg_lon"),
            F.avg("lat").alias("avg_lat"),
        )
    )
    built = (
        kept.join(coords, kept.id == coords.way_id)
        .filter(F.size("pts") == F.size("refs"))  # every point found
        .filter(F.size("pts") >= 2)
    )
    wkt = F.concat(
        F.lit("LINESTRING("),
        F.concat_ws(",", F.transform("pts", lambda p: F.concat_ws(" ", p.lon, p.lat))),
        F.lit(")"),
    )

    out_ways = built.filter("map_contains_key(tags, 'highway')").select(
        "id", F.to_json("tags").alias("tags"), wkt.alias("wkt")
    )
    place_nodes = in_corridor.filter(PLACE_TEST).select(
        F.lit("node").alias("osm_type"),
        "id",
        F.to_json("tags").alias("tags"),
        F.expr("ST_AsText(geom)").alias("wkt"),
    )
    # A hotel drawn as a building outline becomes a point at the average of its corners.
    place_ways = built.filter(
        f"NOT map_contains_key(tags, 'highway') AND ({PLACE_TEST})"
    ).select(
        F.lit("way").alias("osm_type"),
        "id",
        F.to_json("tags").alias("tags"),
        F.format_string("POINT(%s %s)", "avg_lon", "avg_lat").alias("wkt"),
    )
    member_ways = (
        relations.filter(in_list("route", ["hiking", "foot"]))
        .select("id", "tags", F.arrays_zip("refs", "ref_types").alias("members"))
        .withColumn(
            "way_ids",
            F.expr(
                "transform(filter(members, m -> lower(m.ref_types) = 'way'), m -> m.refs)"
            ),
        )
    )
    out_routes = (
        member_ways.select("id", "tags", F.explode("way_ids").alias("way_id"))
        .join(out_ways.select(F.col("id").alias("way_id")), "way_id", "left_semi")
        .select("id")
        .distinct()
        .join(member_ways, "id")
        .select("id", F.to_json("tags").alias("tags"), "way_ids")
    )
    return {
        "ways": out_ways,
        "places": place_nodes.unionByName(place_ways),
        "routes": out_routes,
    }


TABLES = {
    "ways": """
        CREATE TABLE raw.osm_ways (
            way_id bigint PRIMARY KEY, tags jsonb NOT NULL, geom geometry(LineString, 4326)
        )""",
    "places": """
        CREATE TABLE raw.osm_places (
            osm_type text NOT NULL, osm_id bigint NOT NULL, tags jsonb NOT NULL,
            geom geometry(Point, 4326), PRIMARY KEY (osm_type, osm_id)
        )""",
    "routes": """
        CREATE TABLE raw.osm_hiking_routes (
            relation_id bigint PRIMARY KEY, tags jsonb NOT NULL, way_ids bigint[] NOT NULL
        )""",
}
COPY = {
    "ways": "COPY raw.osm_ways (way_id, tags, geom) FROM STDIN",
    "places": "COPY raw.osm_places (osm_type, osm_id, tags, geom) FROM STDIN",
    "routes": "COPY raw.osm_hiking_routes (relation_id, tags, way_ids) FROM STDIN",
}


def as_row(name: str, row) -> tuple:
    """Spark row to a COPY row; geometry goes in as EWKT text."""
    if name == "ways":
        return (row["id"], row["tags"], f"SRID=4326;{row['wkt']}")
    if name == "places":
        return (row["osm_type"], row["id"], row["tags"], f"SRID=4326;{row['wkt']}")
    return (row["id"], row["tags"], list(row["way_ids"]))


@task(cache_policy=NONE)
def spark_to_raw(pbf: Path, pieces: list[str]) -> dict[str, int]:
    """Run the Spark filter and load its three outputs into raw. Return the row counts."""
    spark = spark_session()
    try:
        frames = filter_osm(spark, pbf, pieces)
        counts = {}
        with connect() as conn:
            for name, df in frames.items():
                table = {"ways": "osm_ways", "places": "osm_places"}.get(
                    name, "osm_hiking_routes"
                )
                conn.execute(f"DROP TABLE IF EXISTS raw.{table}")
                conn.execute(TABLES[name])
                with conn.cursor() as cur, cur.copy(COPY[name]) as copy:
                    n = 0
                    for row in df.toLocalIterator():
                        copy.write_row(as_row(name, row))
                        n += 1
                counts[name] = n
            conn.execute("CREATE INDEX ON raw.osm_ways USING gist (geom)")
            conn.execute("CREATE INDEX ON raw.osm_places USING gist (geom)")
            counts["footpaths"] = conn.execute(
                "SELECT count(*) FROM raw.osm_ways WHERE tags->>'highway' = ANY(%s)",
                (FOOTPATHS,),
            ).fetchone()[0]
        return counts
    finally:
        spark.stop()


@flow
def ingest_osm() -> None:
    """Download the extract, then filter it with Spark into raw."""
    logger = get_run_logger()
    pbf = download_pbf(PBF_URL, PBF_PATH)
    pieces = corridor_pieces(CORRIDOR_M)
    logger.info("Corridor: %d km, %d pieces", CORRIDOR_M // 1000, len(pieces))
    counts = spark_to_raw(pbf, pieces)
    logger.info(
        "raw.osm_ways: %d (footpaths in the corridor: %d), raw.osm_places: %d, "
        "raw.osm_hiking_routes: %d",
        counts["ways"],
        counts["footpaths"],
        counts["places"],
        counts["routes"],
    )


if __name__ == "__main__":
    ingest_osm()
