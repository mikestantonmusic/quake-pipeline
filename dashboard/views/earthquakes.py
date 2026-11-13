"""Earthquakes page: one row of filters, headline numbers, map, trend, magnitudes, regions."""

from datetime import UTC, datetime, timedelta

import streamlit as st

import charts
import db
import metrics
from queries import Filters

MAG_OPTIONS = ["All", 1, 2, 3, 4, 5, 6]
mode = st.context.theme.type  # "light", "dark", or None before the browser reports it

st.title("Earthquakes")

try:
    bounds = db.date_bounds(db.engine())
except Exception as exc:  # show connection problems on the page instead of a traceback
    st.error(f"Can't reach the warehouse: {exc}", icon=":material/error:")
    st.stop()
if bounds is None:
    st.info("No earthquakes in the warehouse yet. Run the quake_incremental or quake_backfill DAG.")
    st.stop()
first, last = bounds

# One filter row scopes everything below it.
f_dates, f_mag, f_regions = st.columns([2, 2, 3])
picked = f_dates.date_input(
    "Dates (UTC)",
    value=(max(first, last - timedelta(days=29)), last),
    min_value=first,
    max_value=last,
)
min_mag = f_mag.select_slider("Minimum magnitude", options=MAG_OPTIONS, value="All")
chosen = f_regions.multiselect("Regions", db.regions(db.engine()), placeholder="All regions")

if not isinstance(picked, tuple) or len(picked) != 2:
    st.info("Pick an end date to finish the range.")
    st.stop()

start, end = picked
filters = Filters(start, end, None if min_mag == "All" else float(min_mag), tuple(chosen))
events = db.events(db.engine(), filters)

k = metrics.kpis(events)
t1, t2, t3, t4 = st.columns(4)
t1.metric("Earthquakes", f"{k['count']:,}")
t2.metric(
    "Strongest",
    "–" if k["strongest_mag"] is None else f"M {k['strongest_mag']:.1f}",
    help=k["strongest_place"],
)
t3.metric("Significant", f"{k['significant']:,}", help="USGS significance score of 600 or more")
t4.metric(
    "Latest",
    "–"
    if k["latest_at"] is None
    else f"{metrics.humanize(datetime.now(UTC) - k['latest_at'])} ago",
    help=None if k["latest_at"] is None else k["latest_at"].strftime("%d %b %Y %H:%M UTC"),
)

if events.empty:
    st.info("No earthquakes match these filters.")
    st.stop()

st.subheader("Where they happened")
st.pydeck_chart(charts.quake_map(events, mode), height=460)
depth_counts = events["depth_class"].value_counts()
swatches = "".join(
    f'<span style="margin-right:1.25rem;white-space:nowrap">'
    f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
    f'background:{charts.depth_color(key, mode)};margin-right:6px"></span>'
    f"{label} · {depth_counts.get(key, 0):,}</span>"
    for key, label in charts.DEPTH_LABELS.items()
)
st.markdown(
    f'<div style="font-size:0.875rem">Depth: {swatches} Dot size shows magnitude.</div>',
    unsafe_allow_html=True,
)

left, right = st.columns(2)
with left:
    st.subheader("Earthquakes per day")
    daily = metrics.daily_counts(events, start, end)
    st.altair_chart(charts.daily_line(daily, mode), theme=None, width="stretch")
    with st.expander("Table view"):
        st.dataframe(daily, hide_index=True, width="stretch")
with right:
    st.subheader("Magnitude distribution")
    hist = metrics.magnitude_histogram(events)
    st.altair_chart(charts.magnitude_bars(hist, mode), theme=None, width="stretch")
    with st.expander("Table view"):
        st.dataframe(hist[["band", "earthquakes"]], hide_index=True, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Busiest regions")
    top = metrics.top_regions(events)
    st.altair_chart(charts.region_bars(top, mode), theme=None, width="stretch")
    with st.expander("Table view"):
        st.dataframe(top, hide_index=True, width="stretch")
with right:
    st.subheader("Strongest earthquakes")
    st.dataframe(
        metrics.strongest(events),
        hide_index=True,
        width="stretch",
        column_config={
            "occurred_at": st.column_config.DatetimeColumn(
                "When (UTC)", format="D MMM YYYY, HH:mm"
            ),
            "mag": st.column_config.NumberColumn("Magnitude", format="%.1f"),
            "place": "Place",
            "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.0f"),
            "is_significant": st.column_config.CheckboxColumn("Significant"),
            "url": st.column_config.LinkColumn("Details", display_text="USGS page"),
        },
    )

st.caption("Source: USGS earthquake feeds, refreshed hourly. Data is cached for 5 minutes.")
