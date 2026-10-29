from datetime import UTC, datetime, timedelta, timezone

import pytest
import requests
import responses
from responses import matchers

from ingestion.usgs_client import (
    FDSN_URL,
    MIN_WINDOW,
    RetryableHTTPError,
    TooManyEventsError,
    Window,
    split_window,
)

FEED_ALL_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
DAY = Window(
    datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC)
)


# --- fetch_feed --------------------------------------------------------------------------


@responses.activate
def test_fetch_feed_returns_payload(client, feed_sample):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    payload = client.fetch_feed("all", "day")

    assert [f["id"] for f in payload["features"]] == ["ci41568504", "ci41568496", "ci41568488"]


@responses.activate
def test_fetch_feed_sends_user_agent(client, feed_sample):
    responses.get(FEED_ALL_DAY, json=feed_sample)

    client.fetch_feed()

    assert responses.calls[0].request.headers["User-Agent"].startswith("quake-pipeline/")


@pytest.mark.parametrize("level, period", [("3.0", "day"), ("all", "year")])
def test_fetch_feed_rejects_unknown_feed(client, level, period):
    with pytest.raises(ValueError):
        client.fetch_feed(level, period)


# --- retries -----------------------------------------------------------------------------


@responses.activate
def test_retries_server_errors_then_succeeds(client, feed_sample):
    responses.get(FEED_ALL_DAY, status=503)
    responses.get(FEED_ALL_DAY, status=429)
    responses.get(FEED_ALL_DAY, json=feed_sample)

    payload = client.fetch_feed()

    assert len(payload["features"]) == 3
    assert len(responses.calls) == 3


@responses.activate
def test_gives_up_after_max_attempts(client):
    responses.get(FEED_ALL_DAY, status=500)

    with pytest.raises(RetryableHTTPError):
        client.fetch_feed()
    assert len(responses.calls) == 3


@responses.activate
def test_retries_connection_errors(client, feed_sample):
    responses.get(FEED_ALL_DAY, body=requests.ConnectionError("reset"))
    responses.get(FEED_ALL_DAY, json=feed_sample)

    assert len(client.fetch_feed()["features"]) == 3


@responses.activate
def test_does_not_retry_client_errors(client):
    responses.get(f"{FDSN_URL}/query", status=400, body="exceeds search limit of 20000")

    with pytest.raises(requests.HTTPError):
        client.query(DAY)
    assert len(responses.calls) == 1


# --- FDSN count / query ------------------------------------------------------------------


@responses.activate
def test_count_sends_window_as_utc(client):
    responses.get(
        f"{FDSN_URL}/count",
        json={"count": 415, "maxAllowed": 20000},
        match=[
            matchers.query_param_matcher(
                {"format": "geojson", "starttime": "2026-10-01T00:00:00",
                 "endtime": "2026-10-02T00:00:00"}
            )
        ],
    )

    assert client.count(DAY) == (415, 20000)


@responses.activate
def test_fetch_range_converts_naive_and_offset_times_to_utc(client):
    responses.get(f"{FDSN_URL}/count", json={"count": 1, "maxAllowed": 20000})
    responses.get(f"{FDSN_URL}/query", json={"type": "FeatureCollection", "features": []})
    plus_two = timezone(timedelta(hours=2))

    [(window, _)] = client.fetch_range(
        datetime(2026, 10, 1, 2, tzinfo=plus_two), datetime(2026, 10, 2)
    )

    assert window == DAY
    assert "starttime=2026-10-01T00%3A00%3A00" in responses.calls[1].request.url


# --- split_window ------------------------------------------------------------------------


def counter(events_per_hour: int, max_allowed: int = 100):
    """Fake count function: events spread evenly over time."""
    def count(w: Window) -> tuple[int, int]:
        hours = (w.end - w.start).total_seconds() / 3600
        return int(hours * events_per_hour), max_allowed
    return count


def test_split_window_keeps_small_window_whole():
    assert split_window(DAY, counter(events_per_hour=1)) == [DAY]


def test_split_window_splits_until_each_part_fits():
    count = counter(events_per_hour=10)  # 240 events/day, limit 100 -> needs quarters

    windows = split_window(DAY, count)

    assert len(windows) == 4
    assert all(count(w)[0] <= 100 for w in windows)


def test_split_windows_are_contiguous_and_cover_the_range():
    windows = split_window(DAY, counter(events_per_hour=1000))

    assert windows[0].start == DAY.start
    assert windows[-1].end == DAY.end
    assert all(a.end == b.start for a, b in zip(windows, windows[1:], strict=False))
    assert all(w.start.microsecond == 0 for w in windows)


def test_split_window_stops_at_minimum_size():
    with pytest.raises(TooManyEventsError):
        split_window(Window(DAY.start, DAY.start + MIN_WINDOW), lambda w: (500, 100))
