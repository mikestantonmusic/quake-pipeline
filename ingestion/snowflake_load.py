"""Load: copy raw GeoJSON files from the S3 stage into Snowflake.

Each file becomes one row in raw.feed_files holding the whole GeoJSON document (a VARIANT);
dbt flattens it into one row per event version. COPY INTO remembers which files it has
already loaded (for 64 days), so running it again skips them: loading is idempotent without
any key checks of our own. A file that is rewritten with new content is loaded again, which
is harmless because dbt keeps one row per (event_id, updated_ms).

Each COPY's per-file results are recorded in raw.load_audit for the dashboard.

Connection settings come from the environment (see .env.example): SNOWFLAKE_ACCOUNT,
SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE,
SNOWFLAKE_DATABASE.

CLI:
    python -m ingestion.snowflake_load
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass

log = logging.getLogger(__name__)

STAGE = "raw.s3_raw"

DDL = [
    """
    create table if not exists raw.feed_files (
        source_file        string        not null,  -- S3 key, e.g. raw/feed/all_day/...
        source             string        not null,  -- 'feed' or 'fdsn'
        file_last_modified timestamp_ltz,
        payload            variant       not null,  -- the whole GeoJSON FeatureCollection
        loaded_at          timestamp_ltz not null default current_timestamp()
    )
    """,
    """
    create table if not exists raw.load_audit (
        source_file  string        not null,
        status       string        not null,        -- LOADED, LOAD_FAILED, PARTIALLY_LOADED
        rows_loaded  integer,
        errors_seen  integer,
        first_error  string,
        loaded_at    timestamp_ltz not null default current_timestamp()
    )
    """,
]

# metadata$filename is the file's full S3 key (raw/feed/... or raw/fdsn/...), the same key
# the extract step wrote, so its second folder is the source.
COPY_FILES = f"""
copy into raw.feed_files (source_file, source, file_last_modified, payload)
from (
    select
        metadata$filename,
        split_part(metadata$filename, '/', 2),
        metadata$file_last_modified,
        $1
    from @{STAGE}
)
pattern = '.*[.]geojson'
on_error = continue
"""

INSERT_AUDIT = """
insert into raw.load_audit (source_file, status, rows_loaded, errors_seen, first_error)
values (%s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class CopyResult:
    file: str
    status: str
    rows_loaded: int
    errors_seen: int
    first_error: str | None

    @property
    def ok(self) -> bool:
        return self.status == "LOADED"


def ensure_tables(conn) -> None:
    cur = conn.cursor()
    for statement in DDL:
        cur.execute(statement)


def copy_new_files(conn) -> list[CopyResult]:
    """COPY every not-yet-loaded file from the stage and record the outcome per file."""
    cur = conn.cursor()
    cur.execute(COPY_FILES)
    results = parse_copy_results(cur.fetchall(), [d[0] for d in cur.description])
    if results:
        cur.executemany(
            INSERT_AUDIT,
            [(r.file, r.status, r.rows_loaded, r.errors_seen, r.first_error) for r in results],
        )
    for r in results:
        if not r.ok:
            log.warning("%s: %s (%s)", r.file, r.status, r.first_error)
    log.info(
        "COPY: %d file(s) processed, %d loaded, %d failed",
        len(results), sum(r.ok for r in results), sum(not r.ok for r in results),
    )
    return results


def parse_copy_results(rows: list[tuple], columns: list[str]) -> list[CopyResult]:
    """Turn COPY INTO's result set into CopyResults.

    With nothing new to load, COPY returns a single 'status' column saying
    "Copy executed with 0 files processed." instead of one row per file.
    """
    names = [c.lower() for c in columns]
    if "file" not in names:
        return []
    results = []
    for row in rows:
        r = dict(zip(names, row, strict=True))
        results.append(
            CopyResult(
                file=object_key(r["file"]),
                status=r["status"],
                rows_loaded=int(r["rows_loaded"] or 0),
                errors_seen=int(r["errors_seen"] or 0),
                first_error=r.get("first_error"),
            )
        )
    return results


def object_key(file: str) -> str:
    """s3://<bucket>/raw/feed/... -> raw/feed/..., matching source_file in raw.feed_files."""
    if file.startswith("s3://"):
        return file.removeprefix("s3://").partition("/")[2]
    return file


def connect_from_env():
    """Connect as the pipeline user with key-pair authentication."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing Snowflake settings: {', '.join(missing)} (see .env.example)")

    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "PIPELINE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "QUAKE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "QUAKE"),
        schema="RAW",
        session_parameters={"QUERY_TAG": "quake-pipeline:load", "TIMEZONE": "UTC"},
    )


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="COPY new raw files from S3 into Snowflake.").parse_args(
        argv
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = connect_from_env()
    try:
        ensure_tables(conn)
        results = copy_new_files(conn)
    finally:
        conn.close()
    if any(not r.ok for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
