"""Dashboard entry point: page setup and navigation. Run with `streamlit run app.py`."""

import streamlit as st

st.set_page_config(page_title="USGS earthquakes", page_icon=":material/public:", layout="wide")

page = st.navigation(
    [
        st.Page(
            "views/earthquakes.py", title="Earthquakes", icon=":material/public:", default=True
        ),
        st.Page(
            "views/pipeline_health.py", title="Pipeline health", icon=":material/monitor_heart:"
        ),
    ]
)
page.run()
