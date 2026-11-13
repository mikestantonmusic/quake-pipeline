"""Database connection and cached query wrappers for the Streamlit pages."""

import os

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import queries

# Data changes hourly, so five minutes of caching costs nothing in freshness.
CACHE_SECONDS = 300


@st.cache_resource
def engine() -> Engine:
    return create_engine(os.environ["WAREHOUSE_URL"], pool_pre_ping=True)


def _cached(fn):
    return st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)(fn)


events = _cached(queries.events)
date_bounds = _cached(queries.date_bounds)
regions = _cached(queries.regions)
load_history = _cached(queries.load_history)
pipeline_status = _cached(queries.pipeline_status)
