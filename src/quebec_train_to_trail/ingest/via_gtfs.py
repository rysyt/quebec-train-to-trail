"""Load VIA Rail's GTFS feed into the raw schema.

Spec:
- Download https://www.viarail.ca/sites/all/files/gtfs/viarail.zip to raw/ (gitignored).
  Skip the download if the file is already there (ADR 0007).
- Load every .txt file in the zip into schema raw, one table per file (stops.txt -> raw.stops).
- A Prefect flow made of tasks (ADR 0005), run by hand.
- Database credentials come from the environment (POSTGRES_USER, POSTGRES_PASSWORD,
  POSTGRES_DB, loaded from .env); host localhost, port 5432. Never hard-coded.
- The database runs in Docker and can't see files on this machine: load with
  COPY ... FROM STDIN, not COPY ... FROM '<path>'.

Done:
- raw.stops row count matches stops.txt.
- A rerun doesn't download again and doesn't duplicate rows.

Run from the repo root:
    uv run --env-file .env python -m quebec_train_to_trail.ingest.via_gtfs
"""

import csv
import io
import httpx
import zipfile
import psycopg
import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NONE
from psycopg import sql

GTFS_URL = "https://www.viarail.ca/sites/all/files/gtfs/viarail.zip"
RAW_DIR = Path("raw")


def connect():
    """Open a connection to PostGIS using the credentials in the environment."""
    return psycopg.connect(
        host="localhost",                       # compose publishes the db on 127.0.0.1
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    


@task(retries=2, retry_delay_seconds=10)
def download_gtfs(url: str, dest: Path) -> Path:
    """Download the zip to dest unless it's already there. Return its path."""
    #Checking directory exists
    if not RAW_DIR.exists():
        RAW_DIR.mkdir(parents=True)
    
    #Checking the file does NOT exist
    if dest.is_file():
        return dest
    
    #Downloading the zip
    with httpx.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()
        with dest.open("wb") as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
    return dest
        


@task
def read_headers(zip_path: Path) -> list[tuple[str, list[str]]]:
    """Pair each .txt file in the zip with its column names."""
    headers = []
    with zipfile.ZipFile(zip_path) as zf:
        for file_name in zf.namelist():
            if not file_name.endswith(".txt"):
                continue
            with zf.open(file_name) as f:
                columns = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig")))
            headers.append((file_name, columns))
    return headers


@task(cache_policy=NONE)
def create_raw_table(conn: psycopg.Connection, name: str, columns: list[str]) -> None:
    """Drop and recreate raw.<name> with the given columns, all text."""
    table = sql.Identifier("raw", name)
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
    conn.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            table,
            sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns),
        )
    )


@task(cache_policy=NONE)
def load_table(conn: psycopg.Connection, zip_path: Path, name: str) -> int:
    """COPY <name>.txt from the zip into raw.<name>. Return the row count."""
    copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier("raw", name)
    )
    with zipfile.ZipFile(zip_path) as zf, zf.open(f"{name}.txt") as f:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while chunk := f.read(65536):
                    copy.write(chunk)
            return cur.rowcount


@flow
def ingest_via_gtfs() -> None:
    """Download the feed, then create and load each table."""
    logger = get_run_logger()
    gtfs_path = download_gtfs(GTFS_URL, RAW_DIR / "viarail.zip")
    with connect() as conn:
        for file_name, columns in read_headers(gtfs_path):
            name = Path(file_name).stem #Getting name "stops" from "stops.txt"
            create_raw_table(conn, name, columns)
            rows = load_table(conn, gtfs_path, name)
            logger.info("raw.%s: %d rows", name, rows)
    


if __name__ == "__main__":
    ingest_via_gtfs()
