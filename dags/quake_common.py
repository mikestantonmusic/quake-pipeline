"""Shared settings and helpers for the quake pipeline DAGs (not a DAG itself)."""

import os
from datetime import timedelta
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import TestIndirectSelection

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/quake"))
DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"
# One slot, created by airflow-init: dbt tasks from different DAG runs never overlap.
DBT_POOL = "dbt"

DEFAULT_ARGS = {
    "owner": "quake-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def raw_stores() -> list:
    """Where extract tasks save raw files, from RAW_STORAGE (set in docker-compose.yml)."""
    from ingestion.storage import stores_from_env

    return stores_from_env(DATA_DIR)


def copy_into_snowflake() -> dict:
    """COPY new raw files from S3 into Snowflake; return totals (small enough for XCom).

    Fails the task if any file failed to load, after recording every file in
    raw.load_audit. COPY skips files it already loaded, so a retry only redoes the failures.
    """
    from ingestion.snowflake_load import connect_from_env, copy_new_files, ensure_tables

    conn = connect_from_env()
    try:
        ensure_tables(conn)
        results = copy_new_files(conn)
    finally:
        conn.close()
    failed = [r.file for r in results if not r.ok]
    if failed:
        raise RuntimeError(f"{len(failed)} file(s) failed to load: {failed[:5]}")
    return {"files_loaded": len(results)}


def dbt_task_group(group_id: str = "transform") -> DbtTaskGroup:
    """One Airflow task per dbt model and seed, each followed by its tests."""
    return DbtTaskGroup(
        group_id=group_id,
        # Packages are installed in the image at build time (see the Dockerfile), so neither
        # parsing nor each task needs to run `dbt deps`.
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=False),
        profile_config=ProfileConfig(
            profile_name="quake",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            # Run a test only once every model it reads is built. The default (EAGER) would
            # run assert_daily_agg_matches_fct right after fct_earthquakes, before the
            # aggregate is rebuilt, and fail on stale totals.
            test_indirect_selection=TestIndirectSelection.BUILDABLE,
        ),
        operator_args={"pool": DBT_POOL},
    )
