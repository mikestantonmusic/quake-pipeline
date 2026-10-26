"""Placeholder dashboard: shows warehouse connectivity until the marts exist (Step 6)."""

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Earthquake Pipeline", layout="wide")
st.title("USGS Earthquake Pipeline")


@st.cache_resource
def get_engine():
    return create_engine(os.environ["WAREHOUSE_URL"])


try:
    tables = pd.read_sql(
        """
        select table_schema, table_name
        from information_schema.tables
        where table_schema in ('raw', 'staging', 'marts')
        order by 1, 2
        """,
        get_engine(),
    )
    st.success("Connected to the warehouse.")
    if tables.empty:
        st.info("No tables yet. Run the ingestion DAG to populate the warehouse.")
    else:
        st.dataframe(tables, hide_index=True)
except Exception as exc:  # surface connection problems in the UI instead of a stack trace
    st.error(f"Could not connect to the warehouse: {exc}")
