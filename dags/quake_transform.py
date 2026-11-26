"""Daily: load new raw files from S3 into Snowflake, then rebuild and test the dbt models.

COPY INTO skips files it has already loaded, so a run loads only what arrived since the
last one, whether from the hourly extract or a backfill. Also triggered by quake_backfill.
"""

from datetime import datetime

from airflow.sdk import dag, task
from quake_common import DEFAULT_ARGS, copy_into_snowflake, dbt_task_group


@dag(
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "dbt"],
    doc_md=__doc__,
)
def quake_transform():
    @task
    def load() -> dict:
        return copy_into_snowflake()

    load() >> dbt_task_group()


quake_transform()
