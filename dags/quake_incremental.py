"""Hourly pipeline: download the past day of earthquakes, load new versions, rebuild the marts.

The all_day feed overlaps the previous run on purpose, so revisions USGS makes after an
event first appears are picked up. Loading is idempotent, so the overlap creates no duplicates.
"""

from datetime import datetime

from airflow.sdk import dag, get_current_context, task
from quake_common import DATA_DIR, DEFAULT_ARGS, dbt_task_group, load_files


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["usgs", "ingestion"],
    doc_md=__doc__,
)
def quake_incremental():
    @task
    def extract() -> str:
        from ingestion.extract import extract_feed
        from ingestion.usgs_client import USGSClient

        # run_after is fixed for a given run, so a retried task overwrites the same file.
        run_after = get_current_context()["dag_run"].run_after
        result = extract_feed(USGSClient(), DATA_DIR, run_after, level="all", period="day")
        return str(result.path)

    @task
    def load(path: str) -> dict:
        return load_files([path])

    load(extract()) >> dbt_task_group()


quake_incremental()
