"""SQL behind the dashboard.

Each function takes `run`, a function that executes SQL and returns a DataFrame (db.run in
the app, a fake in tests), so there's no Streamlit or Snowflake in this module.

The queries are fixed text with no parameters. That's deliberate: Snowflake reuses a
query's result (its result cache, kept 24 hours) when the same text runs again and the
tables haven't changed, without starting the warehouse. Filtering is done in pandas.
At ~10k earthquakes a month the full fact table stays small; add a date window here if it
ever gets too big to load in one go.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

Run = Callable[[str], pd.DataFrame]

# Casts: NUMBER columns would otherwise arrive as Python Decimals.
EVENTS_SQL = """
select
    event_id, occurred_at, occurred_date, mag::float as mag, mag_class,
    depth_km::float as depth_km, depth_class, latitude::float as latitude,
    longitude::float as longitude, place, region, is_significant, has_tsunami_flag, url
from marts.fct_earthquakes
order by occurred_at
"""

PIPELINE_FILES_SQL = """
select source_file, source, status, n_features, errors_seen, first_error,
       file_last_modified, loaded_at
from marts.pipeline_files
order by loaded_at desc, source_file
"""


def _utc(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Timestamp columns as pandas UTC datetimes (also right when the frame is empty)."""
    for col in columns:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def events(run: Run) -> pd.DataFrame:
    """Every earthquake in marts.fct_earthquakes, oldest first."""
    return _utc(run(EVENTS_SQL), ["occurred_at"])


def pipeline_files(run: Run) -> pd.DataFrame:
    """Every file COPY has processed (marts.pipeline_files), newest load first."""
    df = _utc(run(PIPELINE_FILES_SQL), ["file_last_modified", "loaded_at"])
    for col in ("n_features", "errors_seen"):
        df[col] = pd.to_numeric(df[col]).astype("Int64")  # nullable: failed files have none
    return df
