"""Backfill: load a historical date range from the FDSN API, then rebuild the marts.

Trigger manually with a start and end date (UTC, end exclusive). Ranges over the API's
20,000-event limit are split automatically. Re-running a range is safe: files are
overwritten and already-loaded versions are skipped.
"""

from datetime import date, datetime, time

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task
from quake_common import DATA_DIR, DEFAULT_ARGS, dbt_task_group, load_files


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["usgs", "backfill"],
    doc_md=__doc__,
    params={
        "start": Param("2026-10-01", type="string", format="date", description="First day (UTC)"),
        "end": Param(
            "2026-10-08", type="string", format="date", description="Day after the last (UTC)"
        ),
    },
)
def quake_backfill():
    @task
    def extract() -> list[str]:
        from ingestion.extract import extract_range
        from ingestion.usgs_client import USGSClient

        params = get_current_context()["params"]
        start = datetime.combine(date.fromisoformat(params["start"]), time())
        end = datetime.combine(date.fromisoformat(params["end"]), time())
        if end <= start:
            # AirflowFailException fails the task straight away: retrying bad input can't help.
            raise AirflowFailException(
                f"end ({params['end']}) must be after start ({params['start']})"
            )
        return [str(r.path) for r in extract_range(USGSClient(), DATA_DIR, start, end)]

    @task
    def load(paths: list[str]) -> dict:
        return load_files(paths)

    load(extract()) >> dbt_task_group()


quake_backfill()
