"""Daily: download the latest feed, load new raw files into Snowflake, rebuild the dbt models.

The download runs first, so every load includes data from moments earlier, even when the
laptop was asleep at 06:00 and this run starts late, at the same time as quake_extract.

COPY INTO skips files it has already loaded, so a run loads only what arrived since the
last one, whether from the hourly extract, this run's download or a backfill. Also
triggered by quake_backfill.
"""

from datetime import UTC, datetime

from airflow.sdk import dag, task
from quake_common import DEFAULT_ARGS, copy_into_snowflake, dbt_task_group, extract_latest_feed


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
    def extract() -> str:
        # The actual time, not the scheduled 06:00: a late run's file is named for when the
        # data was downloaded. A retry writes a second file, which is harmless.
        return extract_latest_feed(datetime.now(UTC))

    # all_done: if USGS is down, still load the files already in S3. The failed download
    # still shows red in Airflow.
    @task(trigger_rule="all_done")
    def load() -> dict:
        return copy_into_snowflake()

    extract() >> load() >> dbt_task_group()


quake_transform()
