"""Extract: download USGS data and save it unchanged to the raw landing zone.

Files are named deterministically from the window they cover, so re-running the same
extract overwrites the same file instead of creating duplicates. The same key is used in
every store (see ingestion.storage): under data/ locally and in the S3 bucket.

    raw/feed/<level>_<period>/YYYY/MM/DD/<run_ts>.geojson
    raw/fdsn/YYYY/MM/DD/<start>_<end>.geojson

CLI (RAW_STORAGE=local,s3 also uploads to S3):
    python -m ingestion.extract feed --level all --period day
    python -m ingestion.extract range --start 2026-10-01 --end 2026-10-02
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.schemas import validate_features
from ingestion.storage import LocalStore, RawStore, stores_from_env
from ingestion.usgs_client import USGSClient, Window, to_utc

log = logging.getLogger(__name__)

TS_FORMAT = "%Y%m%dT%H%M%SZ"


# Either the stores to write to, or a data directory (shorthand for one LocalStore).
Destination = Path | str | Sequence[RawStore]


@dataclass(frozen=True)
class ExtractResult:
    key: str
    locations: tuple[str, ...]  # one per store: a local path or an s3:// URI
    n_features: int
    n_invalid: int

    @property
    def path(self) -> Path | None:
        """The local copy, if one of the stores was local (the Postgres loader reads it)."""
        local = [loc for loc in self.locations if not loc.startswith("s3://")]
        return Path(local[0]) if local else None


def default_data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "data"))


def extract_feed(
    client: USGSClient,
    dest: Destination,
    run_ts: datetime,
    level: str = "all",
    period: str = "day",
) -> ExtractResult:
    """Download a summary feed and save it under the run timestamp."""
    run_ts = to_utc(run_ts)
    payload = client.fetch_feed(level, period)
    key = (
        f"raw/feed/{level}_{period}/{run_ts.strftime('%Y/%m/%d')}/"
        f"{run_ts.strftime(TS_FORMAT)}.geojson"
    )
    return _save(payload, key, _stores(dest))


def extract_range(
    client: USGSClient, dest: Destination, start: datetime, end: datetime
) -> list[ExtractResult]:
    """Download [start, end) from the FDSN API, saving one file per UTC day.

    Splitting by day keeps files small (one VARIANT row each in Snowflake) and makes a
    day's file name independent of the range requested, so re-running any range that
    covers a day overwrites that day's file. A day over the API's 20,000-event limit is
    split further into smaller windows, one file each.
    """
    stores = _stores(dest)
    return [
        _save(payload, _window_key(window), stores)
        for day_start, day_end in utc_days(to_utc(start), to_utc(end))
        for window, payload in client.fetch_range(day_start, day_end)
    ]


def utc_days(start: datetime, end: datetime) -> Iterator[tuple[datetime, datetime]]:
    """Split [start, end) at UTC midnights. Partial days at either end are kept as-is."""
    while start < end:
        next_midnight = datetime.combine(start.date() + timedelta(days=1), datetime.min.time(), UTC)
        yield start, min(next_midnight, end)
        start = next_midnight


def _stores(dest: Destination) -> list[RawStore]:
    if isinstance(dest, Path | str):
        return [LocalStore(dest)]
    return list(dest)


def _window_key(window: Window) -> str:
    name = f"{window.start.strftime(TS_FORMAT)}_{window.end.strftime(TS_FORMAT)}.geojson"
    return f"raw/fdsn/{window.start.strftime('%Y/%m/%d')}/{name}"


def _save(payload: dict, key: str, stores: Sequence[RawStore]) -> ExtractResult:
    features = payload.get("features", [])
    validation = validate_features(features)
    for event_id, error in validation.invalid:
        log.warning("Invalid feature %s: %s", event_id, error)

    body = json.dumps(payload).encode("utf-8")
    locations = tuple(store.write(key, body) for store in stores)

    result = ExtractResult(key, locations, len(features), len(validation.invalid))
    log.info(
        "Saved %d features (%d invalid) to %s",
        result.n_features, result.n_invalid, ", ".join(locations),
    )
    return result


def _parse_dt(value: str) -> datetime:
    return to_utc(datetime.fromisoformat(value))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Download USGS earthquake data to data/raw (and S3 if RAW_STORAGE says so)."
    )
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    sub = parser.add_subparsers(dest="command", required=True)

    feed = sub.add_parser("feed", help="download a summary feed")
    feed.add_argument("--level", default="all")
    feed.add_argument("--period", default="day")

    rng = sub.add_parser("range", help="download a time range from the FDSN API")
    rng.add_argument("--start", type=_parse_dt, required=True, help="ISO date/time, UTC")
    rng.add_argument("--end", type=_parse_dt, required=True, help="ISO date/time, UTC (exclusive)")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = USGSClient()
    stores = stores_from_env(args.data_dir)

    if args.command == "feed":
        extract_feed(client, stores, datetime.now(UTC), args.level, args.period)
    else:
        results = extract_range(client, stores, args.start, args.end)
        total = sum(r.n_features for r in results)
        log.info("Done: %d features in %d file(s)", total, len(results))


if __name__ == "__main__":
    main()
