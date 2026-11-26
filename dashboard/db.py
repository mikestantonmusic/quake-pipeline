"""Snowflake connection and cached query wrappers for the Streamlit pages.

Settings come from Streamlit secrets (the [snowflake] table; Community Cloud and
.streamlit/secrets.toml) or, failing that, DASHBOARD_SNOWFLAKE_* environment variables
(docker compose). See .streamlit/secrets.toml.example.

Cost: Snowflake bills the warehouse per second, with a 60-second minimum each time it
resumes. The dashboard runs only two fixed queries, so between dbt runs Snowflake answers
repeats from its result cache without starting the warehouse, and Streamlit caches the
results for an hour on top of that. Filters are applied in pandas, not in SQL.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

import queries

# The marts change once a day (quake_transform at 06:00 UTC), so an hour is plenty fresh.
CACHE_SECONDS = 3600

ENV_PREFIX = "DASHBOARD_SNOWFLAKE_"
SETTINGS = ["account", "user", "role", "warehouse", "database", "private_key_file"]


def settings() -> dict:
    """Connection settings: the [snowflake] secrets table if present, else the environment."""
    try:
        if "snowflake" in st.secrets:
            return dict(st.secrets["snowflake"])
    except FileNotFoundError:  # no secrets.toml at all
        pass
    env = {k: os.environ.get(ENV_PREFIX + k.upper()) for k in SETTINGS}
    return {k: v for k, v in env.items() if v}


def private_key_der(cfg: dict) -> bytes:
    """The private key as DER bytes, from PEM text (private_key) or a file (private_key_file).

    Community Cloud has no files besides the repo, so there the key is pasted into secrets.
    """
    from cryptography.hazmat.primitives import serialization

    if cfg.get("private_key"):
        pem = cfg["private_key"].encode()
    elif cfg.get("private_key_file"):
        with open(cfg["private_key_file"], "rb") as f:
            pem = f.read()
    else:
        raise RuntimeError("No Snowflake private key: set private_key or private_key_file")
    key = serialization.load_pem_private_key(pem, password=None)
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


@st.cache_resource
def connection():
    import snowflake.connector

    cfg = settings()
    missing = [k for k in ("account", "user") if not cfg.get(k)]
    if missing:
        raise RuntimeError(f"Missing Snowflake settings: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        authenticator="SNOWFLAKE_JWT",
        private_key=private_key_der(cfg),
        role=cfg.get("role", "REPORTER_ROLE"),
        warehouse=cfg.get("warehouse", "QUAKE_WH"),
        database=cfg.get("database", "QUAKE"),
        schema="MARTS",
        client_session_keep_alive=True,
        session_parameters={"QUERY_TAG": "quake-pipeline:dashboard", "TIMEZONE": "UTC"},
    )


def run(sql: str) -> pd.DataFrame:
    """Run a query and return its rows as a DataFrame with lower-case column names."""
    cur = connection().cursor()
    try:
        cur.execute(sql)
        columns = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()


@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Loading earthquakes…")
def events() -> pd.DataFrame:
    return queries.events(run)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def pipeline_files() -> pd.DataFrame:
    return queries.pipeline_files(run)
