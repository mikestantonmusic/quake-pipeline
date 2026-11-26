"""Hourly: download the past day of earthquakes to S3.

Only extracts; quake_transform loads and transforms once a day, so the Snowflake warehouse
starts once a day instead of every hour. The all_day feed overlaps the previous run on
purpose, so revisions USGS makes after an event first appears are picked up. Each run writes
its own file, and dbt keeps one row per event version, so the overlap creates no duplicates.
"""

from datetime import datetime

from airflow.sdk import dag, get_current_context, task
from quake_common import DEFAULT_ARGS, extract_latest_feed


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["usgs", "ingestion"],
    doc_md=__doc__,
)
def quake_extract():
    @task
    def extract() -> str:
        # run_after is fixed for a given run, so a retried task overwrites the same file.
        return extract_latest_feed(get_current_context()["dag_run"].run_after)

    extract()


quake_extract()
