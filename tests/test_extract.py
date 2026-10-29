import json
from datetime import UTC, datetime

import responses

from ingestion.extract import extract_feed, extract_range, main
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
