import copy
import json
from pathlib import Path

import pytest
from tenacity import wait_none

from ingestion.storage import S3Store
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


TEST_BUCKET = "quake-test-bucket"


@pytest.fixture
def s3(monkeypatch):
    """A fake S3 (moto) with an empty test bucket. Nothing reaches AWS.

    Fake credentials are set first so boto3 can never pick up real ones from .env,
    the environment or ~/.aws.
    """
    from moto import mock_aws

    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-2",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    with mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-2")
        client.create_bucket(
            Bucket=TEST_BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"}
        )
        yield client


@pytest.fixture
def s3_store(s3) -> S3Store:
    return S3Store(TEST_BUCKET, client=s3)
