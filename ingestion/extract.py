"""Extract: download USGS data and save it unchanged to the raw landing zone.

Files are named deterministically from the window they cover, so re-running the same
extract overwrites the same file instead of creating duplicates.

    data/raw/feed/<level>_<period>/YYYY/MM/DD/<run_ts>.geojson
    data/raw/fdsn/YYYY/MM/DD/<start>_<end>.geojson

CLI:
    python -m ingestion.extract feed --level all --period day
    python -m ingestion.extract range --start 2026-10-01 --end 2026-10-02
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ingestion.schemas import validate_features
from ingestion.usgs_client import USGSClient, Window, to_utc

log = logging.getLogger(__name__)

TS_FORMAT = "%Y%m%dT%H%M%SZ"


@dataclass(frozen=True)
class ExtractResult:
    path: Path
    n_features: int
    n_invalid: int


def default_data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "data"))


def extract_feed(
    client: USGSClient,
    data_dir: Path,
    run_ts: datetime,
    level: str = "all",
    period: str = "day",
) -> ExtractResult:
    """Download a summary feed and save it under the run timestamp."""
    run_ts = to_utc(run_ts)
    payload = client.fetch_feed(level, period)
    path = (
        Path(data_dir) / "raw" / "feed" / f"{level}_{period}"
        / run_ts.strftime("%Y/%m/%d") / f"{run_ts.strftime(TS_FORMAT)}.geojson"
    )
    return _save(payload, path)


def extract_range(
    client: USGSClient, data_dir: Path, start: datetime, end: datetime
) -> list[ExtractResult]:
    """Download [start, end) from the FDSN API, saving one file per query window."""
    return [
        _save(payload, _window_path(Path(data_dir), window))
        for window, payload in client.fetch_range(start, end)
    ]


def _window_path(data_dir: Path, window: Window) -> Path:
    name = f"{window.start.strftime(TS_FORMAT)}_{window.end.strftime(TS_FORMAT)}.geojson"
    return data_dir / "raw" / "fdsn" / window.start.strftime("%Y/%m/%d") / name


def _save(payload: dict, path: Path) -> ExtractResult:
    features = payload.get("features", [])
    validation = validate_features(features)
    for event_id, error in validation.invalid:
        log.warning("Invalid feature %s: %s", event_id, error)

    # Write to a temp file then rename, so a crash never leaves a half-written file behind.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, path)

    result = ExtractResult(path, len(features), len(validation.invalid))
    log.info("Saved %d features (%d invalid) to %s", result.n_features, result.n_invalid, path)
    return result


def _parse_dt(value: str) -> datetime:
    return to_utc(datetime.fromisoformat(value))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download USGS earthquake data to data/raw.")
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

    if args.command == "feed":
        extract_feed(client, args.data_dir, datetime.now(UTC), args.level, args.period)
    else:
        results = extract_range(client, args.data_dir, args.start, args.end)
        total = sum(r.n_features for r in results)
        log.info("Done: %d features in %d file(s)", total, len(results))


if __name__ == "__main__":
    main()
