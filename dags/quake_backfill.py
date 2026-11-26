"""Backfill: download a historical date range from the FDSN API to S3, then run quake_transform.

Trigger manually with a start and end date (UTC, end exclusive). Each UTC day is saved as its
own file, so re-running a range overwrites the same files. Loading and dbt are left to
quake_transform, triggered at the end, so they are defined in one place.
"""

from datetime import date, datetime, time

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from quake_common import DEFAULT_ARGS, raw_stores


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
    def extract() -> int:
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
        return len(extract_range(USGSClient(), raw_stores(), start, end))

    # Starts a quake_transform run (queued behind any running one, as it allows one at a
    # time) and finishes without waiting for it.
    transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="quake_transform",
        fail_when_dag_is_paused=True,
    )

    extract() >> transform


quake_backfill()
