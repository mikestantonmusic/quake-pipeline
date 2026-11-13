import json
from pathlib import Path

import psycopg2
import pytest

from ingestion.load import (
    expand_paths,
    load_file,
    load_paths,
    relative_source_file,
    source_of,
    to_rows,
)

# --- pure helpers ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_file, expected",
    [
        ("raw/feed/all_day/2026/10/28/x.geojson", "feed"),
        ("raw/fdsn/2026/10/01/x.geojson", "fdsn"),
        ("somewhere/else.geojson", "unknown"),
        ("raw/x.geojson", "unknown"),
    ],
)
def test_source_of(source_file, expected):
    assert source_of(source_file) == expected


def test_relative_source_file_uses_forward_slashes(tmp_path):
    path = tmp_path / "raw" / "fdsn" / "f.geojson"

    assert relative_source_file(path, tmp_path) == "raw/fdsn/f.geojson"


def test_to_rows_keys_on_id_and_updated(valid_feature):
    [row] = to_rows([valid_feature], "feed", "raw/feed/f.geojson")

    assert row[:2] == ("ci41568504", valid_feature["properties"]["updated"])
    assert row[3:] == ("feed", "raw/feed/f.geojson")


def test_expand_paths_finds_files_in_folders_sorted(tmp_path):
    for name in ["b.geojson", "a.geojson", "notes.txt"]:
        (tmp_path / "raw" / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "raw" / name).write_text("{}")

    assert [p.name for p in expand_paths([tmp_path / "raw"])] == ["a.geojson", "b.geojson"]


# --- database tests (skipped when Postgres isn't reachable) -------------------------------


@pytest.fixture
def conn(test_dsn):
    conn = psycopg2.connect(test_dsn, connect_timeout=3)
    with conn, conn.cursor() as cur:
        cur.execute("drop schema if exists raw cascade")
    yield conn
    conn.close()


def write_file(data_dir: Path, rel: str, features: list[dict]) -> Path:
    path = data_dir / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return path


def query(conn, statement: str) -> list[tuple]:
    with conn, conn.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall()


def test_load_inserts_events_and_audit_row(conn, feed_sample, tmp_path):
    path = write_file(tmp_path, "raw/feed/all_day/a.geojson", feed_sample["features"])

    [result] = load_paths(conn, [path], tmp_path)

    assert (result.n_features, result.n_inserted, result.n_invalid) == (3, 3, 0)
    assert query(conn, "select count(*), min(source) from raw.earthquake_events") == [(3, "feed")]
    assert query(conn, "select source_file, n_inserted from raw.load_audit") == [
        ("raw/feed/all_day/a.geojson", 3)
    ]


def test_payload_is_stored_as_queryable_json(conn, feed_sample, tmp_path):
    path = write_file(tmp_path, "raw/feed/a.geojson", feed_sample["features"][:1])

    load_paths(conn, [path], tmp_path)

    assert query(conn, "select payload->'properties'->>'place' from raw.earthquake_events") == [
        (feed_sample["features"][0]["properties"]["place"],)
    ]


def test_reloading_and_overlapping_files_insert_nothing_new(conn, feed_sample, tmp_path):
    first = write_file(tmp_path, "raw/feed/a.geojson", feed_sample["features"])
    overlap = write_file(tmp_path, "raw/feed/b.geojson", feed_sample["features"][1:])

    results = load_paths(conn, [first, first, overlap], tmp_path)

    # The duplicate path is loaded once; the overlapping file only has known versions.
    assert [r.n_inserted for r in results] == [3, 0]
    load_file(conn, first, tmp_path)
    assert query(conn, "select count(*) from raw.earthquake_events") == [(3,)]
    assert query(conn, "select n_inserted from raw.load_audit order by load_id") == [
        (3,), (0,), (0,)
    ]


def test_revised_event_is_kept_as_new_version(conn, valid_feature, tmp_path):
    revised = json.loads(json.dumps(valid_feature))
    revised["properties"]["updated"] += 60_000
    revised["properties"]["mag"] = 1.4
    write_file(tmp_path, "raw/feed/a.geojson", [valid_feature])
    write_file(tmp_path, "raw/feed/b.geojson", [revised])

    load_paths(conn, [tmp_path / "raw"], tmp_path)

    rows = query(
        conn,
        "select payload->'properties'->>'mag' from raw.earthquake_events order by updated_ms",
    )
    assert rows == [(str(valid_feature["properties"]["mag"]),), ("1.4",)]


def test_invalid_features_are_skipped_but_counted(conn, feed_sample, tmp_path):
    feed_sample["features"][0]["geometry"] = None
    path = write_file(tmp_path, "raw/fdsn/a.geojson", feed_sample["features"])

    [result] = load_paths(conn, [path], tmp_path)

    assert (result.n_features, result.n_inserted, result.n_invalid) == (3, 2, 1)
    assert query(conn, "select n_features, n_invalid from raw.load_audit") == [(3, 1)]


def test_empty_file_records_audit_row(conn, tmp_path):
    path = write_file(tmp_path, "raw/fdsn/empty.geojson", [])

    [result] = load_paths(conn, [path], tmp_path)

    assert result.n_inserted == 0
    assert query(conn, "select count(*) from raw.load_audit") == [(1,)]
