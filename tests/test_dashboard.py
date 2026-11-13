from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from sqlalchemy import create_engine
from streamlit.testing.v1 import AppTest

import metrics
import queries
from ingestion.load import ensure_tables
from queries import Filters

ALL_DAYS = Filters(date(2026, 11, 1), date(2026, 11, 3))
APP = Path(__file__).parent.parent / "dashboard" / "app.py"


# --- metrics (pure pandas) ---------------------------------------------------------------


def events_frame(rows: list[dict]) -> pd.DataFrame:
    defaults = {"place": "x", "region": "x", "is_significant": False, "depth_class": "shallow"}
    df = pd.DataFrame([{**defaults, **r} for r in rows])
    df["occurred_at"] = pd.to_datetime(df["occurred_date"]).dt.tz_localize("UTC")
    return df


def test_kpis_pick_strongest_and_ignore_missing_magnitudes():
    df = events_frame(
        [
            {"occurred_date": "2026-11-01", "mag": 2.0, "place": "a"},
            {"occurred_date": "2026-11-02", "mag": 5.5, "place": "b", "is_significant": True},
            {"occurred_date": "2026-11-03", "mag": None, "place": "c"},
        ]
    )

    k = metrics.kpis(df)

    assert (k["count"], k["strongest_mag"], k["strongest_place"], k["significant"]) == (
        3,
        5.5,
        "b",
        1,
    )


def test_kpis_on_empty_frame():
    k = metrics.kpis(events_frame([{"occurred_date": "2026-11-01", "mag": 1.0}]).iloc[0:0])

    assert (k["count"], k["strongest_mag"], k["latest_at"]) == (0, None, None)


def test_daily_counts_fill_quiet_days_with_zero():
    df = events_frame(
        [{"occurred_date": "2026-11-01", "mag": 1}, {"occurred_date": "2026-11-03", "mag": 1}]
    )

    daily = metrics.daily_counts(df, date(2026, 11, 1), date(2026, 11, 4))

    assert daily["earthquakes"].tolist() == [1, 0, 1, 0]


def test_magnitude_histogram_bins_by_half_magnitude():
    df = events_frame(
        [{"occurred_date": "2026-11-01", "mag": m} for m in [2.1, 2.4, 2.6, None, -0.3]]
    )

    hist = metrics.magnitude_histogram(df)

    assert hist[["band", "earthquakes"]].values.tolist() == [
        ["-0.5–0.0", 1],
        ["2.0–2.5", 2],
        ["2.5–3.0", 1],
    ]


@pytest.mark.parametrize(
    "age, expected",
    [
        (None, "critical"),
        (timedelta(minutes=30), "good"),
        (timedelta(hours=5), "warning"),
        (timedelta(days=2), "critical"),
    ],
)
def test_freshness_thresholds_match_dbt(age, expected):
    assert metrics.freshness(age)[0] == expected


def test_hourly_coverage_counts_distinct_feed_hours():
    now = datetime(2026, 11, 12, 18, 30, tzinfo=UTC)
    loads = pd.DataFrame(
        {
            "source": ["feed", "feed", "feed", "fdsn", "feed"],
            "finished_at": [now - timedelta(minutes=m) for m in (5, 20, 65, 10, 60 * 30)],
        }
    )

    # 18:25 and 18:10 share an hour; 17:25 is another; fdsn and the 30-hour-old load don't count.
    assert metrics.hourly_coverage(loads, now) == 2


@pytest.mark.parametrize(
    "seconds, text",
    [(30, "30 s"), (300, "5 min"), (3 * 3600 + 120, "3 h 2 min"), (90000, "1 d 1 h")],
)
def test_humanize(seconds, text):
    assert metrics.humanize(timedelta(seconds=seconds)) == text


# --- queries and pages against warehouse_test ---------------------------------------------

FCT_ROWS = [
    # event_id, occurred_at, mag, depth_km, depth_class, region, significant
    ("e1", "2026-11-01 10:00+00", 2.0, 10, "shallow", "Alaska", False),
    ("e2", "2026-11-02 11:00+00", 4.5, 120, "intermediate", "Japan", False),
    ("e3", "2026-11-03 12:00+00", None, 5, "shallow", "California", False),
    ("e4", "2026-11-03 13:00+00", 6.1, 400, "deep", "Japan", True),
]


@pytest.fixture
def warehouse(test_dsn):
    """warehouse_test with small marts and raw tables in the shape the dashboard reads."""
    conn = psycopg2.connect(test_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("drop schema if exists marts cascade; drop schema if exists raw cascade;")
    ensure_tables(conn)
    with conn, conn.cursor() as cur:
        cur.execute("""
            create schema marts;
            create table marts.fct_earthquakes (
                event_id text, occurred_at timestamptz, occurred_date date, mag numeric,
                mag_class text, depth_km numeric, depth_class text, latitude numeric,
                longitude numeric, place text, region text, is_significant boolean,
                has_tsunami_flag boolean, url text
            );
            create table marts.dim_region as
                select 'Japan' as region, 2 as n_earthquakes
                union all select 'Alaska', 1 union all select 'California', 1;
        """)
        for event_id, at, mag, depth, depth_class, region, sig in FCT_ROWS:
            cur.execute(
                """insert into marts.fct_earthquakes values
                   (%s, %s, (%s::timestamptz at time zone 'UTC')::date, %s, 'x', %s, %s,
                    10, 20, %s, %s, %s, false, 'https://example.org')""",
                (event_id, at, at, mag, depth, depth_class, f"near {region}", region, sig),
            )
        cur.execute("""
            insert into raw.load_audit
                (source_file, source, n_features, n_invalid, n_inserted, started_at, finished_at)
            values ('raw/feed/a.geojson', 'feed', 4, 0, 4, now() - interval '10 minutes',
                    now() - interval '9 minutes')
        """)
    conn.close()
    yield create_engine(test_dsn.replace("postgresql://", "postgresql+psycopg2://", 1))


def ids(df: pd.DataFrame) -> list[str]:
    return sorted(df["event_id"])


def test_events_without_filters_include_missing_magnitudes(warehouse):
    assert ids(queries.events(warehouse, ALL_DAYS)) == ["e1", "e2", "e3", "e4"]


def test_events_date_range_is_inclusive(warehouse):
    f = Filters(date(2026, 11, 2), date(2026, 11, 3))

    assert ids(queries.events(warehouse, f)) == ["e2", "e3", "e4"]


def test_events_min_magnitude_filter(warehouse):
    f = Filters(ALL_DAYS.start, ALL_DAYS.end, min_mag=4.0)

    assert ids(queries.events(warehouse, f)) == ["e2", "e4"]


def test_events_region_filter(warehouse):
    f = Filters(ALL_DAYS.start, ALL_DAYS.end, regions=("Japan", "Alaska"))

    assert ids(queries.events(warehouse, f)) == ["e1", "e2", "e4"]


def test_regions_and_bounds(warehouse):
    assert queries.regions(warehouse) == ["Japan", "Alaska", "California"]
    assert queries.date_bounds(warehouse) == (date(2026, 11, 1), date(2026, 11, 3))


def test_pipeline_status(warehouse):
    status = queries.pipeline_status(warehouse)

    assert status["earthquakes"] == 4
    assert (
        timedelta(minutes=8) < status["checked_at"] - status["last_load_at"] < timedelta(minutes=10)
    )


@pytest.fixture
def app(warehouse, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_URL", warehouse.url.render_as_string(hide_password=False))
    return AppTest.from_file(str(APP), default_timeout=30)


def test_earthquakes_page_renders(app):
    app.run()

    assert not app.exception
    assert [m.value for m in app.metric][:3] == ["4", "M 6.1", "1"]


def test_pipeline_health_page_renders(app):
    app.run()
    app.switch_page("views/pipeline_health.py")
    app.run()

    assert not app.exception
    assert "Fresh" in app.success[0].value
