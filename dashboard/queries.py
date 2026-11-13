"""SQL behind the dashboard.

Plain functions that take a SQLAlchemy engine and return DataFrames. There's no Streamlit
in this module, so it can be tested directly against the warehouse_test database. The
first parameter is named `_engine` so Streamlit's cache (see db.py) skips hashing it.

Filtering happens in SQL with bound parameters: Postgres returns only the rows needed,
and filter values can never be interpreted as SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class Filters:
    start: date
    end: date  # inclusive
    min_mag: float | None = None  # None = no magnitude filter (keeps events without one)
    regions: tuple[str, ...] = ()  # empty = all regions

    def params(self) -> dict:
        return {
            "start": self.start,
            "end": self.end,
            "min_mag": self.min_mag,
            "regions": list(self.regions),
        }


EVENTS_SQL = text("""
    select
        event_id, occurred_at, occurred_date, mag, mag_class, depth_km, depth_class,
        latitude, longitude, place, region, is_significant, has_tsunami_flag, url
    from marts.fct_earthquakes
    where occurred_date between :start and :end
      and (cast(:min_mag as numeric) is null or mag >= cast(:min_mag as numeric))
      and (cardinality(cast(:regions as text[])) = 0 or region = any(cast(:regions as text[])))
    order by occurred_at
""")


def events(_engine: Engine, filters: Filters) -> pd.DataFrame:
    """Every earthquake matching the filters."""
    with _engine.connect() as conn:
        return pd.read_sql(EVENTS_SQL, conn, params=filters.params())


def date_bounds(_engine: Engine) -> tuple[date, date] | None:
    """First and last day with data, or None if the fact table is empty."""
    with _engine.connect() as conn:
        first, last = conn.execute(
            text("select min(occurred_date), max(occurred_date) from marts.fct_earthquakes")
        ).one()
    return (first, last) if first else None


def regions(_engine: Engine) -> list[str]:
    """All regions, busiest first (for the region filter)."""
    with _engine.connect() as conn:
        rows = conn.execute(
            text("select region from marts.dim_region order by n_earthquakes desc, region")
        )
        return [r.region for r in rows]


def load_history(_engine: Engine, hours: int = 72) -> pd.DataFrame:
    """Loads recorded in raw.load_audit over the last `hours`, newest first."""
    sql = text("""
        select load_id, source, source_file, n_features, n_invalid, n_inserted,
               started_at, finished_at
        from raw.load_audit
        where finished_at >= now() - make_interval(hours => :hours)
        order by finished_at desc
    """)
    with _engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"hours": hours})


def pipeline_status(_engine: Engine) -> dict:
    """Freshness timestamps and warehouse totals for the health page."""
    sql = text("""
        select
            now() as checked_at,
            (select max(finished_at) from raw.load_audit) as last_load_at,
            (select max(occurred_at) from marts.fct_earthquakes) as newest_event_at,
            (select count(*) from raw.earthquake_events) as raw_versions,
            (select count(distinct event_id) from raw.earthquake_events) as raw_events,
            (select count(*) from marts.fct_earthquakes) as earthquakes
    """)
    with _engine.connect() as conn:
        return dict(conn.execute(sql).one()._mapping)
