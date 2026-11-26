"""Calculations on query results: pure pandas, no database or Streamlit, easy to unit test."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd

# Loads run once a day. Matches the dbt source freshness rule on raw.feed_files.
FRESH_WARN_AFTER = timedelta(hours=26)
FRESH_ERROR_AFTER = timedelta(hours=50)


@dataclass(frozen=True)
class Filters:
    start: date
    end: date  # inclusive
    min_mag: float | None = None  # None = no magnitude filter (keeps events without one)
    regions: tuple[str, ...] = ()  # empty = all regions


def filter_events(events: pd.DataFrame, f: Filters) -> pd.DataFrame:
    """The earthquakes matching the filters."""
    keep = events["occurred_date"].between(f.start, f.end)
    if f.min_mag is not None:
        keep &= events["mag"] >= f.min_mag
    if f.regions:
        keep &= events["region"].isin(f.regions)
    return events[keep]


def date_bounds(events: pd.DataFrame) -> tuple[date, date] | None:
    """First and last day with data, or None if there are no earthquakes."""
    if events.empty:
        return None
    return events["occurred_date"].min(), events["occurred_date"].max()


def region_options(events: pd.DataFrame) -> list[str]:
    """All regions, busiest first (for the region filter)."""
    counts = events["region"].value_counts()
    return sorted(counts.index, key=lambda r: (-counts[r], r))


def kpis(events: pd.DataFrame) -> dict:
    """Headline numbers for the stat tiles."""
    with_mag = events.dropna(subset=["mag"])
    strongest = with_mag.loc[with_mag["mag"].idxmax()] if not with_mag.empty else None
    return {
        "count": len(events),
        "strongest_mag": None if strongest is None else float(strongest["mag"]),
        "strongest_place": None if strongest is None else strongest["place"],
        "significant": int(events["is_significant"].fillna(False).sum()),
        "latest_at": events["occurred_at"].max() if not events.empty else None,
    }


def daily_counts(events: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Earthquakes per day, including days with none, so gaps show as zero, not a jump."""
    days = pd.date_range(start, end, freq="D")
    counts = pd.to_datetime(events["occurred_date"]).value_counts()
    return pd.DataFrame(
        {"date": days, "earthquakes": counts.reindex(days, fill_value=0).to_numpy()}
    )


def magnitude_histogram(events: pd.DataFrame, width: float = 0.5) -> pd.DataFrame:
    """Earthquake counts per magnitude band of `width`, e.g. 2.0–2.5. Unknown magnitudes skipped."""
    mags = events["mag"].dropna().astype(float)
    if mags.empty:
        return pd.DataFrame(columns=["bin_start", "band", "earthquakes"])
    starts = (mags // width) * width
    counts = starts.value_counts().sort_index()
    return pd.DataFrame(
        {
            "bin_start": counts.index,
            "band": [f"{s:.1f}–{s + width:.1f}" for s in counts.index],
            "earthquakes": counts.to_numpy(),
        }
    ).reset_index(drop=True)


def top_regions(events: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    counts = events["region"].value_counts().head(n)
    return pd.DataFrame({"region": counts.index, "earthquakes": counts.to_numpy()})


def strongest(events: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    cols = ["occurred_at", "mag", "place", "depth_km", "is_significant", "url"]
    return events.dropna(subset=["mag"]).nlargest(n, "mag")[cols].reset_index(drop=True)


def freshness(age: timedelta | None) -> tuple[str, str]:
    """(status, label) for how old the newest load is: good, warning or critical."""
    if age is None:
        return "critical", "No loads recorded"
    if age <= FRESH_WARN_AFTER:
        return "good", f"Fresh: last load into Snowflake {humanize(age)} ago"
    if age <= FRESH_ERROR_AFTER:
        return "warning", f"Behind: last load {humanize(age)} ago (expected daily)"
    return "critical", f"Stale: last load {humanize(age)} ago"


def last_load_at(files: pd.DataFrame) -> datetime | None:
    """When a file was last loaded successfully."""
    loaded = files.loc[files["status"] == "LOADED", "loaded_at"]
    return None if loaded.empty else loaded.max()


def hourly_coverage(files: pd.DataFrame, until: datetime, hours: int = 24) -> int:
    """How many of the `hours` clock hours before `until` produced a feed file in S3.

    Measured up to the last load, since files written after it aren't in Snowflake yet.
    """
    feed = files[(files["source"] == "feed") & (files["status"] == "LOADED")]
    written = feed["file_last_modified"]
    recent = written[(written > until - timedelta(hours=hours)) & (written <= until)]
    return recent.dt.floor("h").nunique()


def files_per_day(files: pd.DataFrame, now: datetime, days: int = 14) -> pd.DataFrame:
    """Files loaded per UTC day over the last `days` days, including days with none."""
    end = pd.Timestamp(now).tz_convert("UTC").normalize()
    day_index = pd.date_range(end - pd.Timedelta(days=days - 1), end, freq="D")
    loaded = files.loc[files["status"] == "LOADED", "loaded_at"].dt.floor("D")
    counts = loaded.value_counts().reindex(day_index, fill_value=0)
    return pd.DataFrame({"date": day_index, "files": counts.to_numpy()})


def humanize(delta: timedelta) -> str:
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return f"{seconds} s"
    if seconds < 3600:
        return f"{seconds // 60} min"
    if seconds < 86400:
        return f"{seconds // 3600} h {seconds % 3600 // 60} min"
    return f"{seconds // 86400} d {seconds % 86400 // 3600} h"
