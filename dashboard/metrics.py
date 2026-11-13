"""Calculations on query results: pure pandas, no database or Streamlit, easy to unit test."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

# Matches the dbt source freshness rule on raw.earthquake_events.
FRESH_WARN_AFTER = timedelta(hours=2)
FRESH_ERROR_AFTER = timedelta(hours=26)


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
        return "good", f"Fresh: last load {humanize(age)} ago"
    if age <= FRESH_ERROR_AFTER:
        return "warning", f"Behind: last load {humanize(age)} ago (expected hourly)"
    return "critical", f"Stale: last load {humanize(age)} ago"


def hourly_coverage(loads: pd.DataFrame, now: datetime, hours: int = 24) -> int:
    """How many of the last `hours` clock hours had at least one feed load."""
    feed = loads[loads["source"] == "feed"]
    recent = feed[feed["finished_at"] > now - timedelta(hours=hours)]
    return recent["finished_at"].dt.floor("h").nunique()


def humanize(delta: timedelta) -> str:
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return f"{seconds} s"
    if seconds < 3600:
        return f"{seconds // 60} min"
    if seconds < 86400:
        return f"{seconds // 3600} h {seconds % 3600 // 60} min"
    return f"{seconds // 86400} d {seconds % 86400 // 3600} h"
