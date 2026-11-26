"""Pipeline health page: are files arriving, how fresh is Snowflake, and did any loads fail?"""

from datetime import UTC, datetime, timedelta

import streamlit as st

import charts
import db
import metrics

STATUS_BOX = {
    "good": (st.success, ":material/check_circle:"),
    "warning": (st.warning, ":material/warning:"),
    "critical": (st.error, ":material/error:"),
}
mode = st.context.theme.type

st.title("Pipeline health")
st.caption(
    "Airflow downloads the USGS feed to S3 every hour; once a day (06:00 UTC) it copies the "
    "new files into Snowflake and rebuilds the dbt models this dashboard reads."
)
if st.button("Refresh", icon=":material/refresh:"):
    st.cache_data.clear()

try:
    files = db.pipeline_files()
    events = db.events()
except Exception as exc:
    st.error(f"Can't reach the warehouse: {exc}", icon=":material/error:")
    st.stop()

now = datetime.now(UTC)
last_load = metrics.last_load_at(files)
level, label = metrics.freshness(None if last_load is None else now - last_load)
box, icon = STATUS_BOX[level]
box(label, icon=icon)

week = files[files["loaded_at"] > now - timedelta(days=7)]
failed = week[week["status"] != "LOADED"]
t1, t2, t3, t4 = st.columns(4)
t1.metric(
    "Hourly files before last load",
    "–" if last_load is None else f"{metrics.hourly_coverage(files, last_load)} / 24",
    help="Clock hours in the 24 h before the last load with a feed file in S3. Below 24 "
    "means Airflow wasn't running for some hours (e.g. the laptop was asleep). No data is "
    "lost unless it's off for more than 24 h: each file holds the past day of earthquakes.",
)
newest = events["occurred_at"].max() if not events.empty else None
t2.metric(
    "Newest earthquake",
    "–" if newest is None else f"{metrics.humanize(now - newest)} ago",
    help="Most recent occurred_at in marts.fct_earthquakes",
)
t3.metric("Files loaded, last 7 days", f"{int((week['status'] == 'LOADED').sum()):,}")
t4.metric(
    "Failed files, last 7 days",
    f"{len(failed):,}",
    help="Files COPY INTO couldn't load. Should be 0.",
)

if not failed.empty:
    st.error("Some files failed to load:", icon=":material/error:")
    st.dataframe(
        failed[["source_file", "status", "errors_seen", "first_error", "loaded_at"]],
        hide_index=True,
        width="stretch",
    )

st.subheader("Files loaded per day")
if files.empty:
    st.info("No files loaded yet. Is the quake_transform DAG unpaused?")
else:
    st.altair_chart(
        charts.files_bars(metrics.files_per_day(files, now), mode), theme=None, width="stretch"
    )
    with st.expander("Recent files", expanded=False):
        st.dataframe(
            files.head(200),
            hide_index=True,
            width="stretch",
            column_config={
                "file_last_modified": st.column_config.DatetimeColumn(
                    "Written to S3 (UTC)", format="D MMM, HH:mm"
                ),
                "loaded_at": st.column_config.DatetimeColumn("Loaded (UTC)", format="D MMM, HH:mm"),
                "n_features": st.column_config.NumberColumn("Events in file"),
                "first_error": None,
            },
        )

st.subheader("Warehouse totals")
w1, w2, w3 = st.columns(3)
w1.metric("Earthquakes in marts", f"{len(events):,}", help="Excludes blasts and deleted events")
w2.metric("Files in Snowflake", f"{int((files['status'] == 'LOADED').sum()):,}")
bounds = metrics.date_bounds(events)
w3.metric(
    "Days covered",
    "–" if bounds is None else f"{(bounds[1] - bounds[0]).days + 1:,}",
    help=None if bounds is None else f"{bounds[0]:%d %b %Y} to {bounds[1]:%d %b %Y}",
)

st.caption("Task-level results (retries, failures, logs) are in the Airflow UI.")
