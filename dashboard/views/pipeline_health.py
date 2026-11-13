"""Pipeline health page: is data arriving, how fresh is it, and what did recent loads do?"""

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
if st.button("Refresh", icon=":material/refresh:"):
    st.cache_data.clear()

try:
    status = db.pipeline_status(db.engine())
    loads = db.load_history(db.engine(), hours=72)
except Exception as exc:
    st.error(f"Can't reach the warehouse: {exc}", icon=":material/error:")
    st.stop()

now = datetime.now(UTC)
last_load = status["last_load_at"]
level, label = metrics.freshness(None if last_load is None else now - last_load)
box, icon = STATUS_BOX[level]
box(label, icon=icon)

recent = loads[loads["finished_at"] > now - timedelta(hours=24)]
t1, t2, t3, t4 = st.columns(4)
t1.metric(
    "Hourly loads, last 24 h",
    f"{metrics.hourly_coverage(loads, now)} / 24",
    help="Clock hours with at least one feed load. Below 24 means missed runs.",
)
newest = status["newest_event_at"]
t2.metric(
    "Newest earthquake",
    "–" if newest is None else f"{metrics.humanize(now - newest)} ago",
    help="Most recent occurred_at in marts.fct_earthquakes",
)
t3.metric("New versions, last 24 h", f"{int(recent['n_inserted'].sum()):,}")
t4.metric(
    "Invalid events, last 24 h",
    f"{int(recent['n_invalid'].sum()):,}",
    help="Events that failed validation and were skipped. Should be 0.",
)

st.subheader("Loads in the last 72 hours")
if loads.empty:
    st.info("No loads in the last 72 hours. Is the quake_incremental DAG unpaused?")
else:
    st.altair_chart(charts.loads_bars(loads, mode), theme=None, width="stretch")
    with st.expander("Table view", expanded=False):
        st.dataframe(
            loads,
            hide_index=True,
            width="stretch",
            column_config={
                "finished_at": st.column_config.DatetimeColumn(
                    "Finished (UTC)", format="D MMM, HH:mm"
                ),
                "started_at": None,
                "load_id": None,
            },
        )

st.subheader("Warehouse totals")
w1, w2, w3, w4 = st.columns(4)
w1.metric("Event versions in raw", f"{status['raw_versions']:,}")
w2.metric("Distinct events in raw", f"{status['raw_events']:,}")
w3.metric(
    "Revisions",
    f"{status['raw_versions'] - status['raw_events']:,}",
    help="Extra versions stored because USGS revised an event after first publishing it",
)
w4.metric(
    "Earthquakes in marts", f"{status['earthquakes']:,}", help="Excludes blasts and deleted events"
)

st.caption(
    "Task-level run results (retries, failures, logs) are in the Airflow UI at "
    "[localhost:8080](http://localhost:8080)."
)
