import json
from datetime import UTC, datetime

import responses

from ingestion.extract import extract_feed, extract_range, main, utc_days
from ingestion.storage import LocalStore
from ingestion.usgs_client import FDSN_URL

FEED_ALL_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
RUN_TS = datetime(2026, 10, 28, 14, 0, tzinfo=UTC)


@responses.activate
def test_extract_feed_saves_payload_unchanged(client, feed_sample, tmp_path):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    result = extract_feed(client, tmp_path, RUN_TS)

    expected = tmp_path / "raw/feed/all_day/2026/10/28/20261028T140000Z.geojson"
    assert result.path == expected
    assert json.loads(expected.read_text(encoding="utf-8")) == feed_sample
    assert (result.n_features, result.n_invalid) == (3, 0)


@responses.activate
def test_rerunning_extract_overwrites_instead_of_duplicating(client, feed_sample, tmp_path):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    extract_feed(client, tmp_path, RUN_TS)
    extract_feed(client, tmp_path, RUN_TS)

    assert len(list(tmp_path.rglob("*.geojson"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))


@responses.activate
def test_extract_keeps_invalid_features_in_raw_file(client, feed_sample, tmp_path):
    feed_sample["features"][0]["geometry"] = None
    responses.get(FEED_ALL_DAY, json=feed_sample)

    result = extract_feed(client, tmp_path, RUN_TS)

    assert (result.n_features, result.n_invalid) == (3, 1)
    assert len(json.loads(result.path.read_text(encoding="utf-8"))["features"]) == 3


@responses.activate
def test_extract_range_writes_one_file_per_window(client, tmp_path):
    # Whole day is over the limit, each half fits -> two windows, two files.
    responses.get(f"{FDSN_URL}/count", json={"count": 30000, "maxAllowed": 20000})
    responses.get(f"{FDSN_URL}/count", json={"count": 15000, "maxAllowed": 20000})
    responses.get(f"{FDSN_URL}/query", json={"type": "FeatureCollection", "features": []})
    responses.get(f"{FDSN_URL}/count", json={"count": 15000, "maxAllowed": 20000})
    responses.get(f"{FDSN_URL}/query", json={"type": "FeatureCollection", "features": []})

    results = extract_range(client, tmp_path, datetime(2026, 10, 1), datetime(2026, 10, 2))

    assert [r.path.name for r in results] == [
        "20261001T000000Z_20261001T120000Z.geojson",
        "20261001T120000Z_20261002T000000Z.geojson",
    ]
    assert all(r.path.parent == tmp_path / "raw/fdsn/2026/10/01" for r in results)


@responses.activate
def test_cli_feed_command(feed_sample, tmp_path):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    main(["--data-dir", str(tmp_path), "feed"])

    assert len(list((tmp_path / "raw/feed/all_day").rglob("*.geojson"))) == 1


@responses.activate
def test_extract_writes_same_file_locally_and_to_s3(client, feed_sample, tmp_path, s3, s3_store):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    result = extract_feed(client, [LocalStore(tmp_path), s3_store], RUN_TS)

    key = "raw/feed/all_day/2026/10/28/20261028T140000Z.geojson"
    assert result.key == key
    assert result.locations == (str(tmp_path / key), f"s3://{s3_store.bucket}/{key}")
    assert result.path == tmp_path / key
    uploaded = s3.get_object(Bucket=s3_store.bucket, Key=key)["Body"].read()
    assert uploaded == (tmp_path / key).read_bytes()
    assert json.loads(uploaded) == feed_sample


@responses.activate
def test_extract_to_s3_only_has_no_local_path(client, feed_sample, s3_store):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    result = extract_feed(client, [s3_store], RUN_TS)

    assert result.path is None
    assert result.locations[0].startswith("s3://")


@responses.activate
def test_cli_uploads_when_raw_storage_includes_s3(
    feed_sample, tmp_path, s3, s3_store, monkeypatch
):
    responses.get(FEED_ALL_DAY, json=feed_sample)
    monkeypatch.setenv("RAW_STORAGE", "local,s3")
    monkeypatch.setenv("RAW_BUCKET", s3_store.bucket)

    main(["--data-dir", str(tmp_path), "feed"])

    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=s3_store.bucket)["Contents"]]
    assert len(keys) == 1 and keys[0].startswith("raw/feed/all_day/")
    assert len(list((tmp_path / "raw/feed/all_day").rglob("*.geojson"))) == 1


@responses.activate
def test_extract_range_saves_one_file_per_utc_day(client, tmp_path):
    for _ in range(3):
        responses.get(f"{FDSN_URL}/count", json={"count": 10, "maxAllowed": 20000})
        responses.get(f"{FDSN_URL}/query", json={"type": "FeatureCollection", "features": []})

    results = extract_range(client, tmp_path, datetime(2026, 10, 1), datetime(2026, 10, 4))

    assert [r.key for r in results] == [
        "raw/fdsn/2026/10/01/20261001T000000Z_20261002T000000Z.geojson",
        "raw/fdsn/2026/10/02/20261002T000000Z_20261003T000000Z.geojson",
        "raw/fdsn/2026/10/03/20261003T000000Z_20261004T000000Z.geojson",
    ]


def test_utc_days_keeps_partial_days_at_the_edges():
    start = datetime(2026, 10, 1, 18, tzinfo=UTC)
    end = datetime(2026, 10, 3, 6, tzinfo=UTC)

    assert list(utc_days(start, end)) == [
        (start, datetime(2026, 10, 2, tzinfo=UTC)),
        (datetime(2026, 10, 2, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC)),
        (datetime(2026, 10, 3, tzinfo=UTC), end),
    ]


def test_utc_days_empty_when_end_not_after_start():
    t = datetime(2026, 10, 1, tzinfo=UTC)

    assert list(utc_days(t, t)) == []
