import copy
import json
from pathlib import Path

import pytest
from tenacity import wait_none

from ingestion.usgs_client import USGSClient

FIXTURES = Path(__file__).parent / "fixtures"


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
