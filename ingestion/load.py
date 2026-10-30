"""Load: insert raw GeoJSON files into the warehouse's raw layer.

raw.earthquake_events keeps one row per event *version*, keyed on (event_id, updated_ms).
USGS bumps `updated` whenever it revises an event, so a revision becomes a new row, while
the same version seen again (the hourly all_day feed overlaps itself ~24 times) is skipped.
That makes loading idempotent: re-loading any file inserts nothing new.

Every load is recorded in raw.load_audit for observability.

CLI:
    python -m ingestion.load                       # everything under data/raw
    python -m ingestion.load data/raw/fdsn/2026/10  # specific files or folders
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json, execute_values

from ingestion.extract import default_data_dir
from ingestion.schemas import validate_features

log = logging.getLogger(__name__)

DEFAULT_DSN = "postgresql://quake:quake@localhost:5432/warehouse"

DDL = """
create schema if not exists raw;

create table if not exists raw.earthquake_events (
    event_id    text        not null,
    updated_ms  bigint      not null,
    payload     jsonb       not null,
    source      text        not null,
    source_file text        not null,
    loaded_at   timestamptz not null default now(),
    primary key (event_id, updated_ms)
);

create table if not exists raw.load_audit (
    load_id     bigserial   primary key,
    source_file text        not null,
    source      text        not null,
    n_features  integer     not null,
    n_invalid   integer     not null,
    n_inserted  integer     not null,
    started_at  timestamptz not null,
    finished_at timestamptz not null
);
"""

INSERT_EVENTS = """
insert into raw.earthquake_events (event_id, updated_ms, payload, source, source_file)
values %s
on conflict (event_id, updated_ms) do nothing
returning 1
"""

INSERT_AUDIT = """
insert into raw.load_audit
    (source_file, source, n_features, n_invalid, n_inserted, started_at, finished_at)
values (%s, %s, %s, %s, %s, %s, now())
"""


@dataclass(frozen=True)
class LoadResult:
    source_file: str
    n_features: int
    n_invalid: int
    n_inserted: int


def ensure_tables(conn: Connection) -> None:
    """Create the raw tables if they don't exist (safe to run every time)."""
    with conn, conn.cursor() as cur:
        cur.execute(DDL)


def to_rows(features: list[dict], source: str, source_file: str) -> list[tuple]:
    return [
        (f["id"], f["properties"]["updated"], Json(f), source, source_file) for f in features
    ]


def load_file(conn: Connection, path: Path, data_dir: Path) -> LoadResult:
    """Load one raw file. Events and its audit row are written in a single transaction."""
    started_at = datetime.now(UTC)
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    validation = validate_features(payload.get("features", []))
    for event_id, error in validation.invalid:
        log.warning("Skipping invalid feature %s: %s", event_id, error)

    source_file = relative_source_file(Path(path), Path(data_dir))
    source = source_of(source_file)
    rows = to_rows(validation.valid, source, source_file)

    # `with conn` commits on success and rolls back on any error, so a failed load leaves
    # neither partial events nor an audit row behind.
    with conn, conn.cursor() as cur:
        inserted = execute_values(cur, INSERT_EVENTS, rows, fetch=True) if rows else []
        n_features = len(validation.valid) + len(validation.invalid)
        cur.execute(
            INSERT_AUDIT,
            (source_file, source, n_features, len(validation.invalid), len(inserted), started_at),
        )

    result = LoadResult(source_file, n_features, len(validation.invalid), len(inserted))
    log.info(
        "Loaded %s: %d features, %d new versions, %d invalid",
        source_file, result.n_features, result.n_inserted, result.n_invalid,
    )
    return result


def load_paths(conn: Connection, paths: Iterable[Path], data_dir: Path) -> list[LoadResult]:
    """Load files, expanding folders to every .geojson inside them, in path order."""
    ensure_tables(conn)
    return [load_file(conn, f, data_dir) for f in expand_paths(paths)]


def expand_paths(paths: Iterable[Path]) -> list[Path]:
    files: set[Path] = set()
    for p in map(Path, paths):
        files.update(p.rglob("*.geojson") if p.is_dir() else [p])
    return sorted(files)


def relative_source_file(path: Path, data_dir: Path) -> str:
    """Path relative to the data dir, with forward slashes.

    Stored instead of the absolute path, which differs between your PC and the containers.
    """
    try:
        return path.resolve().relative_to(data_dir.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def source_of(source_file: str) -> str:
    """'raw/feed/...' -> 'feed', 'raw/fdsn/...' -> 'fdsn'."""
    parts = source_file.split("/")
    if "raw" in parts and parts.index("raw") + 1 < len(parts) - 1:
        return parts[parts.index("raw") + 1]
    return "unknown"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load raw GeoJSON files into Postgres.")
    parser.add_argument("paths", nargs="*", type=Path, help="files or folders (default: data/raw)")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    parser.add_argument("--dsn", default=os.environ.get("WAREHOUSE_DSN", DEFAULT_DSN))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = psycopg2.connect(args.dsn)
    try:
        results = load_paths(conn, args.paths or [args.data_dir / "raw"], args.data_dir)
    finally:
        conn.close()
    log.info(
        "Done: %d file(s), %d features, %d new versions",
        len(results), sum(r.n_features for r in results), sum(r.n_inserted for r in results),
    )


if __name__ == "__main__":
    main()
