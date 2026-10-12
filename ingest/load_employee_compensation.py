"""Loads the Employee Compensation dataset into Snowflake. Runs once a day.

What one run does:
  1. Asks the source when it last published and how many rows it has.
  2. If that publish is already loaded, records "nothing new" and stops.
  3. Downloads every row, 50,000 at a time, into one compressed file.
  4. Uploads the file to Snowflake and copies it into the raw table.
  5. Checks the new batch, and keeps it only if the checks pass.

Every run writes one row to LOAD_AUDIT, so a failed or skipped run is
visible in Snowflake, not only in the scheduler's logs.

I load the full dataset each time because the source replaces every row on
each publish and has no stable row key, so there is nothing to load
incrementally.

Usage:
    python ingest/load_employee_compensation.py           # normal daily run
    python ingest/load_employee_compensation.py --force   # reload even if unchanged
"""

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()  # reads settings from .env on a laptop; GitHub Actions sets them directly
log = logging.getLogger("loader")

SOURCE_URL = os.environ.get("SOURCE_URL", "https://data.sf.gov")
DATASET_ID = "88g8-5mnd"
PAGE_SIZE = 50_000

# Columns the staging model reads. If one disappears, fail before downloading.
EXPECTED_COLUMNS = {
    "year_type", "year", "organization_group_code", "organization_group",
    "department_code", "department", "job_family_code", "job_family",
    "job_code", "job", "union_code", "union", "employee_identifier",
    "employment_type", "salaries", "overtime", "other_salaries",
    "total_salary", "retirement", "health_and_dental", "other_benefits",
    "total_benefits", "total_compensation", "hours", "data_as_of",
}


class LoadError(Exception):
    """A check failed. The message is saved in LOAD_AUDIT."""


# ---------------------------------------------------------------- source

def make_session():
    """HTTP session that retries on throttling and server errors."""
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    # The exercise treats the data as private, so sign in when a key is set.
    if os.environ.get("SOCRATA_KEY_ID"):
        session.auth = (os.environ["SOCRATA_KEY_ID"], os.environ["SOCRATA_KEY_SECRET"])
    return session


def get_source_state(session):
    """Return when the source last published, its columns, and its row count."""
    meta = session.get(f"{SOURCE_URL}/api/views/{DATASET_ID}.json", timeout=60)
    meta.raise_for_status()
    meta = meta.json()
    published_at = datetime.fromtimestamp(meta["rowsUpdatedAt"], tz=timezone.utc)
    columns = {c["fieldName"] for c in meta["columns"]}

    count = session.get(f"{SOURCE_URL}/resource/{DATASET_ID}.json",
                        params={"$select": "count(*) as n"}, timeout=120)
    count.raise_for_status()
    return published_at, columns, int(count.json()[0]["n"])


def download(session, path):
    """Write every row to a gzipped file, one JSON record per line."""
    rows = 0
    with gzip.open(path, "wt", encoding="utf-8") as out:
        while True:
            page = session.get(
                f"{SOURCE_URL}/resource/{DATASET_ID}.json",
                params={"$order": ":id", "$limit": PAGE_SIZE, "$offset": rows},
                timeout=300,
            )
            page.raise_for_status()
            records = page.json()
            if not records:
                return rows
            for record in records:
                out.write(json.dumps(record) + "\n")
            rows += len(records)
            log.info("downloaded %s rows", f"{rows:,}")


# ------------------------------------------------------------- snowflake

def connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="SVC_LOADER",
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE", "snowflake_key.p8"),
        role="LOADER",
        warehouse="LOADING_WH",
        database="RAW",
        schema="CONTROLLER",
    )


def write_audit(cur, batch_id, status, published_at=None, rows=None, error=None):
    """Record how this run ended."""
    cur.execute(
        "insert into load_audit (batch_id, started_at, finished_at, status, "
        "  source_published_at, rows_loaded, error_message) "
        "select %(batch_id)s, %(started_at)s::timestamp_tz, current_timestamp(), "
        "  %(status)s, %(published_at)s::timestamp_tz, %(rows)s, %(error)s",
        {
            "batch_id": batch_id,
            "started_at": datetime.strptime(batch_id, "%Y%m%dT%H%M%SZ")
                                  .replace(tzinfo=timezone.utc).isoformat(),
            "status": status,
            "published_at": published_at.isoformat() if published_at else None,
            "rows": rows,
            "error": error[:2000] if error else None,
        },
    )


def last_loaded_publish(cur):
    """When the source had last published, as of the last successful load."""
    cur.execute("select max(source_published_at) from load_audit where status = 'SUCCESS'")
    return cur.fetchone()[0]


def load_batch(cur, batch_id, published_at, local_file, source_rows):
    """Copy the file into the raw table and check it, all or nothing.

    The copy and the checks run in one transaction. If a check fails, the
    batch is rolled back and staging keeps reading the previous good batch.
    """
    stage_path = f"@employee_compensation_stage/{batch_id[:6]}/"
    cur.execute(f"put 'file://{local_file.as_posix()}' {stage_path} "
                "auto_compress = false overwrite = true")

    cur.execute("begin")
    try:
        cur.execute(
            "copy into employee_compensation "
            "  (_batch_id, _source_published_at, _source_file, record) "
            "from (select %(batch_id)s, %(published_at)s::timestamp_tz, metadata$filename, $1 "
            f"      from {stage_path}{local_file.name}) "
            "file_format = (type = json) on_error = abort_statement",
            {"batch_id": batch_id, "published_at": published_at.isoformat()},
        )
        cur.execute(
            "select count(*), count(distinct record:year_type::string) "
            "from employee_compensation where _batch_id = %(batch_id)s",
            {"batch_id": batch_id},
        )
        rows_loaded, year_types = cur.fetchone()

        # Check 1: every row the source has made it into Snowflake.
        if rows_loaded != source_rows:
            raise LoadError(f"Loaded {rows_loaded:,} rows; the source has {source_rows:,}.")
        # Check 2: both calendar-year and fiscal-year rows are present.
        if year_types != 2:
            raise LoadError(f"Expected 2 year types, found {year_types}.")
    except Exception:
        cur.execute("rollback")
        raise
    cur.execute("commit")


# ------------------------------------------------------------------ main

def run(force=False):
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session = make_session()
    conn = connect()
    cur = conn.cursor()
    published_at = None

    try:
        published_at, columns, source_rows = get_source_state(session)
        log.info("source published %s with %s rows", published_at.isoformat(), f"{source_rows:,}")

        missing = EXPECTED_COLUMNS - columns
        if missing:
            raise LoadError(f"Source is missing columns: {sorted(missing)}")

        if last_loaded_publish(cur) == published_at and not force:
            log.info("this publish is already loaded; nothing to do")
            write_audit(cur, batch_id, "SKIPPED_NO_NEW_DATA", published_at)
            return "SKIPPED_NO_NEW_DATA"

        with tempfile.TemporaryDirectory() as tmp:
            local_file = Path(tmp) / f"employee_compensation_{batch_id}.json.gz"
            if download(session, local_file) != source_rows:
                raise LoadError("Download returned a different row count than the source reported.")
            load_batch(cur, batch_id, published_at, local_file, source_rows)

        write_audit(cur, batch_id, "SUCCESS", published_at, source_rows)
        log.info("loaded batch %s", batch_id)
        return "SUCCESS"

    except Exception as exc:
        log.exception("load failed")
        write_audit(cur, batch_id, "FAILED", published_at, error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load Employee Compensation into Snowflake.")
    parser.add_argument("--force", action="store_true", help="load even if already loaded")
    try:
        run(force=parser.parse_args().force)
    except Exception:
        sys.exit(1)  # a non-zero exit is what the scheduler alerts on
