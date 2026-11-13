import copy
import json
import os
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql
from tenacity import wait_none

from ingestion.usgs_client import USGSClient

FIXTURES = Path(__file__).parent / "fixtures"
TEST_DSN = os.environ.get(
    "TEST_WAREHOUSE_DSN", "postgresql://quake:quake@localhost:5432/warehouse_test"
)


@pytest.fixture(scope="session")
def test_dsn() -> str:
    """DSN of the warehouse_test database, created if missing. Skips if Postgres is down."""
    try:
        params = psycopg2.extensions.parse_dsn(TEST_DSN)
        admin = psycopg2.connect(**{**params, "dbname": "postgres"}, connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not available: {exc}")
    admin.autocommit = True  # CREATE DATABASE can't run inside a transaction
    with admin.cursor() as cur:
        cur.execute("select 1 from pg_database where datname = %s", (params["dbname"],))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("create database {}").format(sql.Identifier(params["dbname"])))
    admin.close()
    return TEST_DSN


@pytest.fixture
def feed_sample() -> dict:
    """A real all_hour feed response trimmed to three earthquakes."""
    return json.loads((FIXTURES / "feed_sample.geojson").read_text(encoding="utf-8"))


@pytest.fixture
def valid_feature(feed_sample) -> dict:
    return copy.deepcopy(feed_sample["features"][0])


@pytest.fixture
def client() -> USGSClient:
    """Client that retries without sleeping, so retry tests run instantly."""
    return USGSClient(max_attempts=3, wait=wait_none())
