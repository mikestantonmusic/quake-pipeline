"""Shared settings and helpers for the quake pipeline DAGs (not a DAG itself)."""

import os
from datetime import timedelta
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import TestIndirectSelection

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/quake"))
DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"
WAREHOUSE_CONN_ID = "warehouse"
# One slot, created by airflow-init: dbt tasks from different DAG runs never overlap.
DBT_POOL = "dbt"

DEFAULT_ARGS = {
    "owner": "quake-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_files(paths: list[str]) -> dict:
    """Load raw files into the warehouse and return totals (small enough for XCom)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from ingestion.load import load_paths

    conn = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()
    try:
        results = load_paths(conn, [Path(p) for p in paths], DATA_DIR)
    finally:
        conn.close()
    return {
        "files": len(results),
        "features": sum(r.n_features for r in results),
        "new_versions": sum(r.n_inserted for r in results),
        "invalid": sum(r.n_invalid for r in results),
    }


def dbt_task_group(group_id: str = "transform") -> DbtTaskGroup:
    """One Airflow task per dbt model and seed, each followed by its tests."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(DBT_PROJECT_DIR),
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
