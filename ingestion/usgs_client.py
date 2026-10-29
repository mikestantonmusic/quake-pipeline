"""HTTP client for the USGS earthquake APIs.

Two sources:
- Summary feeds: pre-built GeoJSON files refreshed every minute (used for incremental loads).
- FDSN Event API: arbitrary time-range queries (used for backfills). A single query may
  return at most `maxAllowed` events (20,000), so large ranges are split into windows.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity.wait import wait_base

log = logging.getLogger(__name__)

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{level}_{period}.geojson"
FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1"

FEED_LEVELS = ("significant", "4.5", "2.5", "1.0", "all")
FEED_PERIODS = ("hour", "day", "week", "month")

DEFAULT_USER_AGENT = "quake-pipeline/0.1 (portfolio data pipeline)"
# Smallest window split_window will produce; guards against endless splitting.
MIN_WINDOW = timedelta(minutes=1)


class RetryableHTTPError(Exception):
    """A response worth retrying: rate limiting (429) or a server error (5xx)."""


class TooManyEventsError(Exception):
    """A time window still exceeds the API's per-query limit at the minimum window size."""


@dataclass(frozen=True)
class Window:
    """A half-open time range [start, end) in UTC."""

    start: datetime
    end: datetime


class USGSClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: float = 30.0,
        max_attempts: int = 5,
        wait: wait_base | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.session = session or requests.Session()
        # Assign rather than setdefault: requests sessions already carry a default User-Agent.
        self.session.headers["User-Agent"] = user_agent
        self.timeout = timeout
        self._retrying = Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait or wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception_type(
                (RetryableHTTPError, requests.ConnectionError, requests.Timeout)
            ),
            before_sleep=lambda rs: log.warning(
                "USGS request failed (attempt %d): %s", rs.attempt_number, rs.outcome.exception()
            ),
            reraise=True,
        )

    def fetch_feed(self, level: str = "all", period: str = "day") -> dict:
        """Download a summary feed, e.g. ("all", "day") for every event in the past 24 hours."""
        if level not in FEED_LEVELS:
            raise ValueError(f"level must be one of {FEED_LEVELS}, got {level!r}")
        if period not in FEED_PERIODS:
            raise ValueError(f"period must be one of {FEED_PERIODS}, got {period!r}")
        return self._get_json(FEED_URL.format(level=level, period=period))

    def count(self, window: Window) -> tuple[int, int]:
        """Return (matching event count, max events allowed per query) for a window."""
        body = self._get_json(f"{FDSN_URL}/count", params=_fdsn_params(window))
        return body["count"], body["maxAllowed"]

    def query(self, window: Window) -> dict:
        """Fetch all events in a window as a GeoJSON FeatureCollection (one request)."""
        params = {**_fdsn_params(window), "orderby": "time-asc"}
        return self._get_json(f"{FDSN_URL}/query", params=params)

    def fetch_range(self, start: datetime, end: datetime) -> Iterator[tuple[Window, dict]]:
        """Yield (window, FeatureCollection) for [start, end), split to stay under the limit."""
        for window in split_window(Window(to_utc(start), to_utc(end)), self.count):
            yield window, self.query(window)

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        return self._retrying(self._get_json_once, url, params)

    def _get_json_once(self, url: str, params: dict | None) -> dict:
        resp = self.session.get(url, params=params, timeout=self.timeout)
        if resp.status_code == 429 or resp.status_code >= 500:
            raise RetryableHTTPError(f"HTTP {resp.status_code} from {resp.url}")
        # Other 4xx errors (bad parameters, too many events) won't succeed on retry.
        resp.raise_for_status()
        return resp.json()


def split_window(window: Window, count: Callable[[Window], tuple[int, int]]) -> list[Window]:
    """Split a window into consecutive sub-windows that each fit within the per-query limit.

    `count` returns (events in window, max allowed per query). Windows over the limit are
    halved recursively until they fit.
    """
    n, max_allowed = count(window)
    if n <= max_allowed:
        return [window]
    if window.end - window.start <= MIN_WINDOW:
        raise TooManyEventsError(
            f"{n} events between {window.start} and {window.end} exceeds limit {max_allowed}"
        )
    # Whole seconds, so window boundaries survive the seconds-precision timestamps sent to the API.
    mid = window.start + timedelta(seconds=(window.end - window.start).total_seconds() // 2)
    log.info("Splitting %s..%s (%d events > %d)", window.start, window.end, n, max_allowed)
    return split_window(Window(window.start, mid), count) + split_window(
        Window(mid, window.end), count
    )


def _fdsn_params(window: Window) -> dict:
    return {
        "format": "geojson",
        "starttime": _iso(window.start),
        "endtime": _iso(window.end),
    }


def to_utc(dt: datetime) -> datetime:
    """Treat naive datetimes as UTC; convert aware ones to UTC."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _iso(dt: datetime) -> str:
    # FDSN expects UTC without an offset suffix.
    return to_utc(dt).strftime("%Y-%m-%dT%H:%M:%S")
