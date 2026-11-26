from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

import db
import metrics
import queries
from metrics import Filters

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
        (timedelta(hours=20), "good"),
        (timedelta(hours=30), "warning"),
        (timedelta(hours=60), "critical"),
    ],
)
def test_freshness_thresholds_match_dbt(age, expected):
    assert metrics.freshness(age)[0] == expected


NOW = datetime(2026, 11, 12, 18, 30, tzinfo=UTC)


def files_frame(rows: list[tuple]) -> pd.DataFrame:
    """pipeline_files rows: (source, status, minutes before NOW written, minutes before loaded)."""
    return pd.DataFrame(
        [
            {
                "source_file": f"raw/{source}/f{i}.geojson",
                "source": source,
                "status": status,
                "file_last_modified": NOW - timedelta(minutes=written),
                "loaded_at": NOW - timedelta(minutes=loaded),
            }
            for i, (source, status, written, loaded) in enumerate(rows)
        ]
    )


def test_hourly_coverage_counts_distinct_feed_hours_before_the_load():
    files = files_frame(
        [
            ("feed", "LOADED", 125, 5),  # 16:25
            ("feed", "LOADED", 140, 5),  # 16:10, same hour
            ("feed", "LOADED", 185, 5),  # 15:25
            ("fdsn", "LOADED", 150, 5),  # backfill files don't count
            ("feed", "LOAD_FAILED", 250, 5),  # not in Snowflake
            ("feed", "LOADED", 60 * 30, 5),  # more than 24 h before the load
        ]
    )

    assert metrics.hourly_coverage(files, until=NOW - timedelta(minutes=5)) == 2


def test_last_load_ignores_failed_files():
    files = files_frame([("feed", "LOADED", 100, 90), ("feed", "LOAD_FAILED", 50, 10)])

    assert metrics.last_load_at(files) == NOW - timedelta(minutes=90)
    assert metrics.last_load_at(files.iloc[0:0]) is None


def test_files_per_day_fills_days_without_loads():
    files = files_frame(
        [("feed", "LOADED", 0, 0), ("feed", "LOADED", 0, 1), ("feed", "LOADED", 0, 60 * 48)]
    )

    daily = metrics.files_per_day(files, NOW, days=3)

    assert daily["files"].tolist() == [1, 0, 2]


@pytest.mark.parametrize(
    "seconds, text",
    [(30, "30 s"), (300, "5 min"), (3 * 3600 + 120, "3 h 2 min"), (90000, "1 d 1 h")],
)
def test_humanize(seconds, text):
    assert metrics.humanize(timedelta(seconds=seconds)) == text


# --- filters ----------------------------------------------------------------------------

FCT_ROWS = [
    # event_id, occurred_at (UTC), mag, depth_km, depth_class, region, significant
    ("e1", "2026-11-01 10:00", 2.0, 10.0, "shallow", "Alaska", False),
    ("e2", "2026-11-02 11:00", 4.5, 120.0, "intermediate", "Japan", False),
    ("e3", "2026-11-03 12:00", None, 5.0, "shallow", "California", False),
    ("e4", "2026-11-03 13:00", 6.1, 400.0, "deep", "Japan", True),
]


def fct_rows(sql: str = "") -> pd.DataFrame:
    """Rows shaped like queries.EVENTS_SQL returns them from Snowflake."""
    rows = []
    for event_id, at, mag, depth, depth_class, region, sig in FCT_ROWS:
        occurred = datetime.fromisoformat(at).replace(tzinfo=UTC)
        rows.append(
            {
                "event_id": event_id,
                "occurred_at": occurred,
                "occurred_date": occurred.date(),
                "mag": mag,
                "mag_class": "x",
                "depth_km": depth,
                "depth_class": depth_class,
                "latitude": 10.0,
                "longitude": 20.0,
                "place": f"near {region}",
                "region": region,
                "is_significant": sig,
                "has_tsunami_flag": False,
                "url": "https://example.org",
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture
def events() -> pd.DataFrame:
    return queries.events(fct_rows)


def ids(df: pd.DataFrame) -> list[str]:
    return sorted(df["event_id"])


def test_events_without_filters_include_missing_magnitudes(events):
    assert ids(metrics.filter_events(events, ALL_DAYS)) == ["e1", "e2", "e3", "e4"]


def test_events_date_range_is_inclusive(events):
    f = Filters(date(2026, 11, 2), date(2026, 11, 3))

    assert ids(metrics.filter_events(events, f)) == ["e2", "e3", "e4"]


def test_events_min_magnitude_filter(events):
    f = Filters(ALL_DAYS.start, ALL_DAYS.end, min_mag=4.0)

    assert ids(metrics.filter_events(events, f)) == ["e2", "e4"]


def test_events_region_filter(events):
    f = Filters(ALL_DAYS.start, ALL_DAYS.end, regions=("Japan", "Alaska"))

    assert ids(metrics.filter_events(events, f)) == ["e1", "e2", "e4"]


def test_regions_and_bounds(events):
    assert metrics.region_options(events) == ["Japan", "Alaska", "California"]
    assert metrics.date_bounds(events) == (date(2026, 11, 1), date(2026, 11, 3))
    assert metrics.date_bounds(events.iloc[0:0]) is None


def test_queries_return_utc_timestamps(events):
    assert str(events["occurred_at"].dt.tz) == "UTC"


# --- connection settings ------------------------------------------------------------------


def test_settings_from_environment(monkeypatch):
    monkeypatch.setenv("DASHBOARD_SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("DASHBOARD_SNOWFLAKE_USER", "DASHBOARD_USER")

    assert db.settings() == {"account": "acct", "user": "DASHBOARD_USER"}


def test_private_key_from_pem_text_or_file(tmp_path):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    (tmp_path / "k.p8").write_bytes(pem)

    from_text = db.private_key_der({"private_key": pem.decode()})
    from_file = db.private_key_der({"private_key_file": str(tmp_path / "k.p8")})

    assert from_text == from_file
    assert serialization.load_der_private_key(from_text, None).key_size == 2048


# --- pages, with Snowflake replaced by canned query results --------------------------------


@pytest.fixture
def app(monkeypatch):
    now = datetime.now(UTC)
    files = pd.DataFrame(
        {
            "source_file": ["raw/feed/a.geojson"],
            "source": ["feed"],
            "status": ["LOADED"],
            "n_features": [4],
            "errors_seen": [0],
            "first_error": [None],
            "file_last_modified": [now - timedelta(hours=2)],
            "loaded_at": [now - timedelta(hours=1)],
        }
    )

    def fake_run(sql: str) -> pd.DataFrame:
        return files.copy() if "pipeline_files" in sql else fct_rows()

    monkeypatch.setattr(db, "run", fake_run)
    st.cache_data.clear()
    yield AppTest.from_file(str(APP), default_timeout=30)
    st.cache_data.clear()


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
    assert [m.value for m in app.metric][2:4] == ["1", "0"]
