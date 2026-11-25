"""Snowflake loader tests with a fake connection: no Snowflake account needed."""

import pytest

from ingestion import snowflake_load
from ingestion.snowflake_load import (
    CopyResult,
    connect_from_env,
    copy_new_files,
    ensure_tables,
    object_key,
    parse_copy_results,
)

COPY_COLUMNS = [
    "file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen",
    "first_error", "first_error_line", "first_error_character", "first_error_column_name",
]
LOADED = (
    "s3://bucket/raw/feed/all_day/2026/11/25/20261125T230000Z.geojson",
    "LOADED", 1, 1, 1, 0, None, None, None, None,
)
FAILED = (
    "s3://bucket/raw/fdsn/2026/11/01/20261101T000000Z_20261102T000000Z.geojson",
    "LOAD_FAILED", 1, 0, 1, 1, "Error parsing JSON", 1, 1, None,
)


class FakeCursor:
    def __init__(self, rows, columns):
        self.rows, self.description = rows, [(c,) for c in columns]
        self.executed, self.many = [], []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        return self

    def executemany(self, sql, seq):
        self.many.append((sql, list(seq)))

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_parse_copy_results_one_row_per_file():
    results = parse_copy_results([LOADED, FAILED], [c.upper() for c in COPY_COLUMNS])

    assert results == [
        CopyResult("raw/feed/all_day/2026/11/25/20261125T230000Z.geojson", "LOADED", 1, 0, None),
        CopyResult(
            "raw/fdsn/2026/11/01/20261101T000000Z_20261102T000000Z.geojson",
            "LOAD_FAILED", 0, 1, "Error parsing JSON",
        ),
    ]
    assert [r.ok for r in results] == [True, False]


def test_parse_copy_results_nothing_new():
    rows = [("Copy executed with 0 files processed.",)]

    assert parse_copy_results(rows, ["status"]) == []


@pytest.mark.parametrize(
    ("file", "expected"),
    [
        ("s3://bucket/raw/feed/all_day/x.geojson", "raw/feed/all_day/x.geojson"),
        ("raw/feed/all_day/x.geojson", "raw/feed/all_day/x.geojson"),
    ],
)
def test_object_key(file, expected):
    assert object_key(file) == expected


def test_copy_new_files_records_each_file_in_audit():
    cur = FakeCursor([LOADED, FAILED], COPY_COLUMNS)

    results = copy_new_files(FakeConn(cur))

    assert "copy into raw.feed_files" in cur.executed[0]
    sql, rows = cur.many[0]
    assert "insert into raw.load_audit" in sql
    expected = [(r.file, r.status, r.rows_loaded, r.errors_seen, r.first_error) for r in results]
    assert rows == expected


def test_copy_new_files_with_nothing_new_writes_no_audit_rows():
    cur = FakeCursor([("Copy executed with 0 files processed.",)], ["status"])

    assert copy_new_files(FakeConn(cur)) == []
    assert cur.many == []


def test_ensure_tables_creates_both_tables():
    cur = FakeCursor([], [])

    ensure_tables(FakeConn(cur))

    assert len(cur.executed) == 2
    assert "raw.feed_files" in cur.executed[0] and "raw.load_audit" in cur.executed[1]


def test_connect_from_env_names_missing_settings(monkeypatch):
    for name in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ORG-ACCT")

    with pytest.raises(RuntimeError, match="SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH"):
        connect_from_env()


def test_main_exits_nonzero_when_a_file_fails(monkeypatch):
    cur = FakeCursor([LOADED, FAILED], COPY_COLUMNS)
    monkeypatch.setattr(snowflake_load, "connect_from_env", lambda: FakeConnClosable(cur))

    with pytest.raises(SystemExit) as exc:
        snowflake_load.main([])

    assert exc.value.code == 1


class FakeConnClosable(FakeConn):
    def close(self):
        pass
